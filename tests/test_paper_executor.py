from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.alerts.discord_notifier import DiscordNotifier
from src.config import Settings
from src.execution.paper_executor import PaperExecutor
from src.models import BTC_PERP, EquitySnapshot, MarketState, OrderType, Side, Signal
from src.risk.circuit_breakers import CircuitBreakerManager


@dataclass
class DummyRepository:
    inserts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    updates: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    deletes: list[tuple[str, str]] = field(default_factory=list)
    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.inserts.setdefault(table, []).append(payload)
        return payload

    async def update(self, table: str, row_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.updates.setdefault(table, []).append(payload | {"id": row_id})
        return payload

    async def delete(self, table: str, row_id: str) -> None:
        self.deletes.append((table, row_id))

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
    messages: list[str] = field(default_factory=list)

    async def send(self, content: str, **_: object) -> None:
        self.messages.append(content)


def _settings(starting_balance: float = 1000) -> Settings:
    return Settings(
        execution_mode="paper_sim",
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
        starting_balance_usd=starting_balance,
        paper_slippage_bps=5,
        paper_max_pending_orders=10,
    )


def _executor(
    settings: Settings | None = None,
    repository: DummyRepository | None = None,
) -> PaperExecutor:
    settings = settings or _settings()
    repository = repository or DummyRepository()
    discord = DummyDiscord()
    breakers = CircuitBreakerManager(
        settings,
        None,
        discord,  # type: ignore[arg-type]
    )
    return PaperExecutor(
        exchange=None,
        repository=repository,  # type: ignore[arg-type]
        circuit_breakers=breakers,
        discord=discord,  # type: ignore[arg-type]
        sms=None,
        settings=settings,
    )


def _market(
    bid: float,
    ask: float,
    last: float,
    symbol: str = BTC_PERP,
    timestamp: datetime | None = None,
) -> MarketState:
    return MarketState(
        symbol=symbol,
        timestamp=timestamp or datetime.now(tz=UTC),
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2,
        last_trade_price=last,
    )


def _signal(
    side: Side,
    entry_price: float,
    stop_loss: float | None = None,
    take_profit: float | None = None,
    strategy_name: str = "test_strategy",
) -> Signal:
    return Signal(
        side=side,
        symbol=BTC_PERP,
        size_pct_equity=0.1,
        entry_price=entry_price,
        stop_loss=stop_loss or entry_price * 0.99,
        take_profit=take_profit,
        reasoning="paper executor test signal",
        strategy_name=strategy_name,
        confidence=0.6,
    )


@pytest.mark.asyncio
async def test_paper_market_buy_fills_at_ask_plus_slippage() -> None:
    executor = _executor()
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))

    result = await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=1,
        price=0,
        order_type=OrderType.MARKET,
    )

    assert result["status"] == "filled"
    assert result["fill_price"] == pytest.approx(100.05)
    assert executor.open_positions[BTC_PERP].entry_price == pytest.approx(100.05)


@pytest.mark.asyncio
async def test_paper_market_sell_fills_at_bid_minus_slippage() -> None:
    executor = _executor()
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))

    result = await executor.place_order(
        BTC_PERP,
        Side.SELL,
        size=1,
        price=0,
        order_type=OrderType.MARKET,
    )

    assert result["status"] == "filled"
    assert result["fill_price"] == pytest.approx(98.95)
    assert executor.open_positions[BTC_PERP].side == Side.SHORT


@pytest.mark.asyncio
async def test_paper_limit_order_pending_until_crossed() -> None:
    executor = _executor()
    await executor.update_market_state(_market(bid=100, ask=101, last=100.5))

    result = await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=1,
        price=99,
        order_type=OrderType.LIMIT,
    )
    await executor.update_market_state(_market(bid=99.8, ask=100, last=99.5))

    assert result["status"] == "pending"
    assert executor.pending_orders
    assert BTC_PERP not in executor.open_positions

    await executor.update_market_state(_market(bid=98.8, ask=99, last=98.9))

    assert not executor.pending_orders
    assert executor.open_positions[BTC_PERP].entry_price == pytest.approx(99)


@pytest.mark.asyncio
async def test_paper_stop_loss_triggers_correctly() -> None:
    repository = DummyRepository()
    executor = _executor(repository=repository)
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))
    await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=1,
        price=0,
        order_type=OrderType.MARKET,
        signal=_signal(Side.LONG, entry_price=100, stop_loss=99),
    )

    await executor.update_market_state(_market(bid=98.5, ask=98.7, last=98.5))

    assert BTC_PERP not in executor.open_positions
    assert repository.updates["trades"][-1]["status"] == "closed"
    assert repository.updates["trades"][-1]["exit_price"] == pytest.approx(98.95)


@pytest.mark.asyncio
async def test_paper_take_profit_triggers_correctly() -> None:
    repository = DummyRepository()
    executor = _executor(repository=repository)
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))
    await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=1,
        price=0,
        order_type=OrderType.MARKET,
        signal=_signal(Side.LONG, entry_price=100, take_profit=102),
    )

    await executor.update_market_state(_market(bid=102.2, ask=102.4, last=102.2))

    assert BTC_PERP not in executor.open_positions
    assert repository.updates["trades"][-1]["reason_exit"] == "paper take profit triggered"
    assert repository.updates["trades"][-1]["pnl_usd"] > 0


@pytest.mark.asyncio
async def test_paper_fees_deducted_correctly() -> None:
    executor = _executor()
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))

    await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=10,
        price=0,
        order_type=OrderType.MARKET,
    )

    assert executor.paper_balance_usd == pytest.approx(999.55)
    assert executor.open_positions[BTC_PERP].fees_paid == pytest.approx(0.45)


@pytest.mark.asyncio
async def test_paper_pnl_calculation_long_and_short() -> None:
    long_executor = _executor()
    await long_executor.update_market_state(_market(bid=99, ask=100, last=99.5))
    await long_executor.place_order(BTC_PERP, Side.BUY, 1, 0, OrderType.MARKET)
    long_trade = await long_executor.close_position(BTC_PERP, 110, "manual close")

    assert long_trade is not None
    assert long_trade.pnl_usd is not None
    assert long_trade.pnl_usd > 0

    short_executor = _executor()
    await short_executor.update_market_state(_market(bid=100, ask=101, last=100.5))
    await short_executor.place_order(BTC_PERP, Side.SELL, 1, 0, OrderType.MARKET)
    short_trade = await short_executor.close_position(BTC_PERP, 90, "manual close")

    assert short_trade is not None
    assert short_trade.pnl_usd is not None
    assert short_trade.pnl_usd > 0


@pytest.mark.asyncio
async def test_paper_balance_never_goes_negative_or_circuit_breaker_trips() -> None:
    settings = _settings(starting_balance=1000)
    discord = DummyDiscord()
    breaker = CircuitBreakerManager(settings, None, discord)  # type: ignore[arg-type]
    executor = PaperExecutor(
        exchange=None,
        repository=None,
        circuit_breakers=breaker,
        discord=discord,  # type: ignore[arg-type]
        sms=None,
        settings=settings,
    )
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))
    await executor.place_order(BTC_PERP, Side.BUY, 10, 0, OrderType.MARKET)
    await executor.close_position(BTC_PERP, 0.01, "catastrophic paper loss")

    snapshot = EquitySnapshot(
        execution_mode="paper_sim",
        balance_usd=executor.paper_balance_usd,
        equity_usd=executor.paper_equity_usd,
        margin_used_usd=executor.paper_margin_used_usd,
        open_position_count=0,
        daily_pnl=executor.paper_equity_usd - settings.starting_balance_usd,
        weekly_pnl=executor.paper_equity_usd - settings.starting_balance_usd,
        mdd_pct=1.0,
    )
    event = await breaker.evaluate(snapshot)

    assert executor.paper_balance_usd >= 0
    assert event is not None
    assert event.breaker_type == "max_drawdown"


@pytest.mark.asyncio
async def test_paper_restore_state_rehydrates_open_trade_and_position() -> None:
    trade_id = "11111111-1111-4111-8111-111111111111"
    timestamp = datetime(2026, 5, 26, 12, 0, tzinfo=UTC).isoformat()
    repository = DummyRepository(
        rows={
            "trades": [
                {
                    "id": trade_id,
                    "timestamp": timestamp,
                    "strategy_name": "rsi_mean_reversion",
                    "symbol": BTC_PERP,
                    "side": "long",
                    "size": 1,
                    "entry_price": 100,
                    "exit_price": None,
                    "pnl_usd": None,
                    "pnl_pct": None,
                    "fees_usd": 0.05,
                    "hold_duration_sec": None,
                    "reason_entry": "restored test trade",
                    "reason_exit": None,
                    "regime": None,
                    "status": "open",
                    "execution_mode": "paper_sim",
                    "created_at": timestamp,
                }
            ],
            "positions": [
                {
                    "id": trade_id,
                    "timestamp": timestamp,
                    "symbol": BTC_PERP,
                    "side": "long",
                    "size": 1,
                    "entry_price": 100,
                    "liquidation_price": None,
                    "unrealized_pnl": 2.5,
                    "leverage": 10,
                    "strategy_name": "rsi_mean_reversion",
                    "stop_loss": 99,
                    "take_profit": 105,
                    "execution_mode": "paper_sim",
                }
            ],
            "equity_snapshots": [
                {
                    "timestamp": timestamp,
                    "balance_usd": 999.95,
                    "equity_usd": 1002.45,
                    "execution_mode": "paper_sim",
                }
            ],
        }
    )
    executor = _executor(repository=repository)

    await executor.restore_state()

    assert executor.paper_balance_usd == pytest.approx(999.95)
    assert executor._trade_ids_by_symbol[BTC_PERP] == trade_id
    assert str(next(iter(executor._open_trades))) == trade_id
    restored_position = executor.open_positions[BTC_PERP]
    assert restored_position.strategy_name == "rsi_mean_reversion"
    assert restored_position.stop_loss == pytest.approx(99)
    assert restored_position.take_profit == pytest.approx(105)
    assert restored_position.current_unrealized_pnl == pytest.approx(2.5)

    await executor.update_market_state(_market(bid=98.5, ask=98.7, last=98.5))

    assert BTC_PERP not in executor.open_positions
    assert repository.updates["trades"][-1]["status"] == "closed"


@pytest.mark.asyncio
async def test_scalp_position_closes_after_max_hold() -> None:
    settings = _settings()
    settings.scalp_max_hold_minutes = 1
    repository = DummyRepository()
    executor = _executor(settings=settings, repository=repository)
    await executor.update_market_state(_market(bid=99, ask=100, last=99.5))
    await executor.place_order(
        BTC_PERP,
        Side.BUY,
        size=1,
        price=0,
        order_type=OrderType.MARKET,
        signal=_signal(
            Side.LONG,
            entry_price=100,
            stop_loss=50,
            take_profit=200,
            strategy_name="micro_rsi_scalp",
        ),
    )

    assert executor.open_positions[BTC_PERP].max_hold_minutes == 1
    await executor.update_market_state(
        _market(
            bid=100,
            ask=100.2,
            last=100,
            timestamp=datetime.now(tz=UTC) + timedelta(minutes=2),
        )
    )

    assert BTC_PERP not in executor.open_positions
    assert repository.updates["trades"][-1]["reason_exit"] == "scalp max hold time reached"
