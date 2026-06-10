from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from src.alerts.discord_notifier import DiscordNotifier
from src.config import Settings
from src.execution.paper_executor import (
    PENDING_ORDERS_STATE_KEY,
    PaperExecutor,
)
from src.main import (
    HIGH_WATER_STATE_KEY,
    TraderComponentStaleError,
    TraderRuntime,
)
from src.models import BTC_PERP, OrderType, Side, Signal
from src.risk.circuit_breakers import CircuitBreakerManager


@dataclass
class FakeRepository:
    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    state: dict[str, dict[str, Any]] = field(default_factory=dict)
    upsert_state_calls: int = 0
    heartbeats: list[dict[str, Any]] = field(default_factory=list)
    inserts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.inserts.setdefault(table, []).append(payload)
        return payload

    async def update(self, table: str, row_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return payload | {"id": row_id}

    async def delete(self, table: str, row_id: str) -> None:
        return None

    async def select_where(
        self,
        table: str,
        filters: dict[str, Any],
        limit: int = 1000,
        order_column: str = "timestamp",
        desc: bool = True,
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.rows.get(table, [])
            if all(row.get(column) == value for column, value in filters.items())
        ]
        rows.sort(key=lambda row: row.get(order_column) or 0, reverse=desc)
        return rows[:limit]

    async def get_state(self, key: str) -> dict[str, Any] | None:
        return self.state.get(key)

    async def upsert_state(self, key: str, value: dict[str, Any]) -> None:
        self.state[key] = value
        self.upsert_state_calls += 1

    async def upsert_heartbeat(
        self,
        component: str,
        last_ok_at: datetime,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.heartbeats.append(
            {"component": component, "last_ok_at": last_ok_at, "detail": detail or {}}
        )


class DummySms:
    def __init__(self) -> None:
        self.errors: list[Exception] = []

    def send_error(self, exc: Exception) -> None:
        self.errors.append(exc)


def _settings(**overrides: Any) -> Settings:
    params: dict[str, Any] = {
        "execution_mode": "paper_sim",
        "supabase_url": "https://example.supabase.co",
        "supabase_service_key": "service-key",
        "starting_balance_usd": 1000.0,
    }
    params.update(overrides)
    return Settings(**params)


def _runtime(
    repository: FakeRepository | None = None,
    settings: Settings | None = None,
) -> tuple[TraderRuntime, FakeRepository]:
    runtime = object.__new__(TraderRuntime)
    runtime.settings = settings or _settings()
    repo = repository or FakeRepository()
    runtime.repository = repo
    runtime.stop_event = asyncio.Event()
    runtime.high_water_equity = runtime.settings.starting_balance_usd
    runtime.sms = DummySms()
    runtime._throttled_log_at = {}
    components = ["market", "strategy", "equity"]
    if runtime.settings.market_data_writer:
        components.append("market_data")
    runtime._heartbeat_components = tuple(components)
    now_mono = time.monotonic()
    now_wall = datetime.now(tz=UTC)
    runtime._last_success = {component: now_mono for component in components}
    runtime._last_success_wall = {component: now_wall for component in components}
    runtime._heartbeat_detail = {component: {} for component in components}
    return runtime, repo


# ---------------------------------------------------------------------------
# High-water mark restore + persistence (drawdown breaker survives restarts)
# ---------------------------------------------------------------------------


async def test_high_water_rebuilt_from_equity_snapshot_history() -> None:
    repo = FakeRepository(
        rows={
            "equity_snapshots": [
                {"equity_usd": 1500.0, "execution_mode": "paper_sim"},
                {"equity_usd": 1800.0, "execution_mode": "paper_sim"},
                {"equity_usd": 1200.0, "execution_mode": "paper_sim"},
            ]
        }
    )
    runtime, _ = _runtime(repository=repo)

    await runtime._load_high_water_equity()

    assert runtime.high_water_equity == pytest.approx(1800.0)


async def test_high_water_prefers_persisted_state_when_higher() -> None:
    repo = FakeRepository(
        rows={"equity_snapshots": [{"equity_usd": 1800.0, "execution_mode": "paper_sim"}]},
        state={HIGH_WATER_STATE_KEY: {"value": 2500.0}},
    )
    runtime, _ = _runtime(repository=repo)

    await runtime._load_high_water_equity()

    assert runtime.high_water_equity == pytest.approx(2500.0)


async def test_high_water_defaults_to_starting_balance_without_history() -> None:
    runtime, _ = _runtime()

    await runtime._load_high_water_equity()

    assert runtime.high_water_equity == pytest.approx(1000.0)


async def test_high_water_persists_only_on_new_peak() -> None:
    runtime, repo = _runtime()

    await runtime._update_high_water_equity(1200.0)

    assert runtime.high_water_equity == pytest.approx(1200.0)
    assert repo.state[HIGH_WATER_STATE_KEY]["value"] == pytest.approx(1200.0)
    assert repo.upsert_state_calls == 1

    await runtime._update_high_water_equity(1100.0)

    assert runtime.high_water_equity == pytest.approx(1200.0)
    assert repo.upsert_state_calls == 1


# ---------------------------------------------------------------------------
# Fail-loud task supervisor + work-gated watchdog
# ---------------------------------------------------------------------------


async def test_supervisor_raises_when_a_task_dies() -> None:
    runtime, _ = _runtime()

    async def dying_loop() -> None:
        raise ValueError("strategy loop crashed")

    task = asyncio.create_task(dying_loop(), name="strategy-loop")
    with pytest.raises(ValueError, match="strategy loop crashed"):
        await runtime._supervise_tasks([task])


async def test_supervisor_raises_when_a_task_exits_silently() -> None:
    runtime, _ = _runtime()

    async def returning_loop() -> None:
        return None

    task = asyncio.create_task(returning_loop(), name="market-loop")
    with pytest.raises(RuntimeError, match="exited unexpectedly"):
        await runtime._supervise_tasks([task])


async def test_supervisor_returns_cleanly_on_shutdown_request() -> None:
    runtime, _ = _runtime()

    async def healthy_loop() -> None:
        while True:
            await asyncio.sleep(3600)

    task = asyncio.create_task(healthy_loop(), name="market-loop")
    runtime.stop_event.set()
    try:
        await asyncio.wait_for(runtime._supervise_tasks([task]), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_watchdog_raises_when_component_goes_stale() -> None:
    runtime, repo = _runtime()
    runtime._last_success["strategy"] = (
        time.monotonic() - runtime.settings.stale_limit_seconds - 5
    )

    with pytest.raises(TraderComponentStaleError, match="strategy"):
        await runtime._watchdog_check_once()

    components = {heartbeat["component"] for heartbeat in repo.heartbeats}
    assert "strategy" in components
    sms: Any = runtime.sms
    assert len(sms.errors) == 1


async def test_watchdog_passes_and_upserts_heartbeats_when_fresh() -> None:
    runtime, repo = _runtime()
    for component in runtime._heartbeat_components:
        runtime._mark_success(component, detail={"state": "ok"})

    await runtime._watchdog_check_once()

    components = [heartbeat["component"] for heartbeat in repo.heartbeats]
    assert components == list(runtime._heartbeat_components)


# ---------------------------------------------------------------------------
# Pending paper limit orders survive restart
# ---------------------------------------------------------------------------


def _paper_executor(repository: FakeRepository, settings: Settings) -> PaperExecutor:
    discord = DiscordNotifier(None)
    breakers = CircuitBreakerManager(settings, None, discord)
    return PaperExecutor(
        exchange=None,
        repository=repository,  # type: ignore[arg-type]
        circuit_breakers=breakers,
        discord=discord,
        sms=None,
        settings=settings,
    )


def _entry_signal() -> Signal:
    return Signal(
        side=Side.LONG,
        symbol=BTC_PERP,
        size_pct_equity=0.1,
        entry_price=100.0,
        stop_loss=99.0,
        take_profit=102.0,
        reasoning="test entry",
        strategy_name="rsi_mean_reversion",
        confidence=0.6,
        features={"rsi": 18.5},
    )


async def test_pending_limit_orders_persist_and_restore_across_restart() -> None:
    settings = _settings()
    repo = FakeRepository()
    executor = _paper_executor(repo, settings)

    result = await executor.place_order(
        symbol=BTC_PERP,
        side=Side.BUY,
        size=0.01,
        price=95.0,
        order_type=OrderType.LIMIT,
        signal=_entry_signal(),
    )

    assert result["status"] == "pending"
    persisted = repo.state[PENDING_ORDERS_STATE_KEY]
    assert len(persisted["orders"]) == 1
    assert persisted["orders"][0]["price"] == pytest.approx(95.0)

    restarted = _paper_executor(repo, settings)
    await restarted.restore_state()

    assert len(restarted.pending_orders) == 1
    restored = next(iter(restarted.pending_orders.values()))
    assert restored.oid == result["oid"]
    assert restored.symbol == BTC_PERP
    assert restored.side == Side.BUY
    assert restored.price == pytest.approx(95.0)
    assert restored.signal is not None
    assert restored.signal.strategy_name == "rsi_mean_reversion"
    assert restored.signal.stop_loss == pytest.approx(99.0)
    assert restarted._next_oid > restored.oid


async def test_restored_pending_order_dropped_when_position_already_open() -> None:
    settings = _settings()
    timestamp = datetime(2026, 6, 9, 12, 0, tzinfo=UTC).isoformat()
    trade_id = "33333333-3333-4333-8333-333333333333"
    repo = FakeRepository(
        rows={
            "trades": [
                {
                    "id": trade_id,
                    "account_id": settings.account_id,
                    "timestamp": timestamp,
                    "strategy_name": "rsi_mean_reversion",
                    "symbol": BTC_PERP,
                    "side": "long",
                    "size": 0.01,
                    "entry_price": 100.0,
                    "fees_usd": 0.05,
                    "reason_entry": "test",
                    "status": "open",
                    "execution_mode": "paper_sim",
                }
            ],
            "positions": [
                {
                    "id": trade_id,
                    "account_id": settings.account_id,
                    "timestamp": timestamp,
                    "symbol": BTC_PERP,
                    "side": "long",
                    "size": 0.01,
                    "entry_price": 100.0,
                    "unrealized_pnl": 0,
                    "leverage": 10,
                    "strategy_name": "rsi_mean_reversion",
                    "stop_loss": 99.0,
                    "take_profit": 105.0,
                    "execution_mode": "paper_sim",
                }
            ],
        },
        state={
            PENDING_ORDERS_STATE_KEY: {
                "next_oid": 8,
                "orders": [
                    {
                        "oid": 7,
                        "symbol": BTC_PERP,
                        "side": "buy",
                        "size": 0.01,
                        "price": 95.0,
                        "created_at": timestamp,
                        "signal": None,
                    }
                ],
            }
        },
    )

    executor = _paper_executor(repo, settings)
    await executor.restore_state()

    assert BTC_PERP in executor.open_positions
    assert executor.pending_orders == {}
    # The filtered (empty) set is re-persisted so bot_state stays honest.
    assert repo.state[PENDING_ORDERS_STATE_KEY]["orders"] == []
    assert executor._next_oid == 8


async def test_malformed_persisted_order_is_skipped_not_fatal() -> None:
    settings = _settings()
    repo = FakeRepository(
        state={
            PENDING_ORDERS_STATE_KEY: {
                "next_oid": 3,
                "orders": [
                    {"oid": "not-an-int", "symbol": BTC_PERP},
                    {
                        "oid": 2,
                        "symbol": BTC_PERP,
                        "side": "buy",
                        "size": 0.01,
                        "price": 95.0,
                        "created_at": "2026-06-09T12:00:00+00:00",
                        # Signal payload missing required fields: the order is
                        # dropped rather than restored without its stop loss.
                        "signal": {"side": "long"},
                    },
                ],
            }
        }
    )

    executor = _paper_executor(repo, settings)
    await executor.restore_state()

    assert executor.pending_orders == {}
    assert executor._next_oid == 3
