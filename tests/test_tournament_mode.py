from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import random
from typing import Any

import pytest

from src.config import Settings
from src.db.supabase_client import SupabaseRepository
from src.execution.paper_executor import PaperExecutor
from src.models import BTC_PERP, ETH_PERP, Position, Side, Signal
from src.risk.circuit_breakers import CircuitBreakerManager
from src.strategies.base import Strategy
from src.strategies.chaos_wrapper import ChaosStrategyWrapper


@dataclass
class MiniRepository:
    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    state: dict[str, dict[str, Any]] = field(default_factory=dict)

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.rows.setdefault(table, []).append(payload)
        return payload

    async def get_state(self, key: str) -> dict[str, Any] | None:
        return self.state.get(key)

    async def upsert_state(self, key: str, value: dict[str, Any]) -> None:
        self.state[key] = value

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
        rows.sort(key=lambda row: str(row.get(order_column, "")), reverse=desc)
        return rows[:limit]


@dataclass
class DummyDiscord:
    async def send(self, _: str, **__: object) -> None:
        return None


class AlwaysSignalStrategy(Strategy):
    name = "always_signal"
    symbols = [BTC_PERP]

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        return Signal(
            side=Side.LONG,
            symbol=BTC_PERP,
            size_pct_equity=0.10,
            entry_price=100,
            stop_loss=99,
            take_profit=102,
            reasoning="test signal",
            strategy_name=self.name,
        )

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        return None

    async def should_exit(self, position: Position) -> bool:
        return False


class FixedRng:
    def random(self) -> float:
        return 0.99

    def uniform(self, a: float, b: float) -> float:
        return 1.25


def _settings(account_id: str) -> Settings:
    return Settings(
        account_id=account_id,
        personality=account_id,
        execution_mode="paper_sim",
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
    )


def _executor(account_id: str, repository: MiniRepository) -> PaperExecutor:
    settings = _settings(account_id)
    discord = DummyDiscord()
    breakers = CircuitBreakerManager(settings, None, discord)  # type: ignore[arg-type]
    return PaperExecutor(
        exchange=None,
        repository=repository,  # type: ignore[arg-type]
        circuit_breakers=breakers,
        discord=discord,  # type: ignore[arg-type]
        sms=None,
        settings=settings,
    )


def _trade_row(trade_id: str, account_id: str, symbol: str) -> dict[str, Any]:
    timestamp = datetime(2026, 5, 31, 12, 0, tzinfo=UTC).isoformat()
    return {
        "id": trade_id,
        "account_id": account_id,
        "timestamp": timestamp,
        "strategy_name": "rsi_mean_reversion",
        "symbol": symbol,
        "side": "long",
        "size": 1,
        "entry_price": 100,
        "exit_price": None,
        "pnl_usd": None,
        "pnl_pct": None,
        "fees_usd": 0.05,
        "hold_duration_sec": None,
        "reason_entry": "test restore",
        "reason_exit": None,
        "regime": None,
        "status": "open",
        "execution_mode": "paper_sim",
        "created_at": timestamp,
    }


def _position_row(trade_id: str, account_id: str, symbol: str) -> dict[str, Any]:
    timestamp = datetime(2026, 5, 31, 12, 0, tzinfo=UTC).isoformat()
    return {
        "id": trade_id,
        "account_id": account_id,
        "timestamp": timestamp,
        "symbol": symbol,
        "side": "long",
        "size": 1,
        "entry_price": 100,
        "liquidation_price": None,
        "unrealized_pnl": 0,
        "leverage": 10,
        "strategy_name": "rsi_mean_reversion",
        "stop_loss": 99,
        "take_profit": 105,
        "execution_mode": "paper_sim",
    }


@pytest.mark.asyncio
async def test_tournament_account_isolation() -> None:
    balanced_trade_id = "11111111-1111-4111-8111-111111111111"
    aggressive_trade_id = "22222222-2222-4222-8222-222222222222"
    repository = MiniRepository(
        rows={
            "trades": [
                _trade_row(balanced_trade_id, "balanced", BTC_PERP),
                _trade_row(aggressive_trade_id, "aggressive", ETH_PERP),
            ],
            "positions": [
                _position_row(balanced_trade_id, "balanced", BTC_PERP),
                _position_row(aggressive_trade_id, "aggressive", ETH_PERP),
            ],
            "equity_snapshots": [
                {
                    "account_id": "balanced",
                    "timestamp": datetime(2026, 5, 31, 12, 0, tzinfo=UTC).isoformat(),
                    "balance_usd": 999.95,
                    "equity_usd": 999.95,
                    "execution_mode": "paper_sim",
                },
                {
                    "account_id": "aggressive",
                    "timestamp": datetime(2026, 5, 31, 12, 0, tzinfo=UTC).isoformat(),
                    "balance_usd": 888.88,
                    "equity_usd": 888.88,
                    "execution_mode": "paper_sim",
                },
            ],
        }
    )

    balanced = _executor("balanced", repository)
    aggressive = _executor("aggressive", repository)
    await balanced.restore_state()
    await aggressive.restore_state()

    assert list(balanced.open_positions) == [BTC_PERP]
    assert list(aggressive.open_positions) == [ETH_PERP]
    assert balanced.paper_balance_usd == pytest.approx(999.95)
    assert aggressive.paper_balance_usd == pytest.approx(888.88)


@pytest.mark.asyncio
async def test_chaos_wrapper_randomizes_size() -> None:
    wrapper = ChaosStrategyWrapper(AlwaysSignalStrategy(), rng=FixedRng())

    signal = await wrapper.on_tick({})

    assert signal is not None
    assert signal.size_pct_equity == pytest.approx(0.125)
    assert signal.features["chaos_size_multiplier"] == pytest.approx(1.25)


@pytest.mark.asyncio
async def test_chaos_wrapper_skip_rate() -> None:
    wrapper = ChaosStrategyWrapper(AlwaysSignalStrategy(), rng=random.Random(7))
    skipped = 0

    for _ in range(1000):
        if await wrapper.on_tick({}) is None:
            skipped += 1

    assert 250 <= skipped <= 350


def test_account_id_required_on_writes() -> None:
    repository = object.__new__(SupabaseRepository)
    repository._account_id = "balanced"

    with pytest.raises(ValueError, match="account_id"):
        repository._validate_account_payload("trades", {"symbol": BTC_PERP})

    payload = repository._with_account("trades", {"symbol": BTC_PERP})
    repository._validate_account_payload("trades", payload)
    assert payload["account_id"] == "balanced"
