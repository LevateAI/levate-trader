"""Core domain models used across strategies, risk, and execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


BTC_PERP = "BTC-PERP"
ETH_PERP = "ETH-PERP"
SUPPORTED_SYMBOLS = {BTC_PERP, ETH_PERP}


class Side(StrEnum):
    """Order and position direction."""

    BUY = "buy"
    SELL = "sell"
    LONG = "long"
    SHORT = "short"


class OrderType(StrEnum):
    """Supported Hyperliquid order types for v1."""

    LIMIT = "limit"
    MARKET = "market"


class TradeStatus(StrEnum):
    """Trade lifecycle status."""

    OPEN = "open"
    CLOSED = "closed"


class SignalType(StrEnum):
    """Strategy signal direction."""

    ENTRY = "entry"
    EXIT = "exit"


def utc_now() -> datetime:
    """Return timezone-aware UTC now."""
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class Signal:
    """Actionable strategy signal."""

    side: Side
    symbol: str
    size_pct_equity: float
    entry_price: float
    stop_loss: float
    take_profit: float | None
    reasoning: str
    strategy_name: str
    signal_strength: float = 1.0
    confidence: float = 0.5
    reduce_only: bool = False
    features: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    @property
    def stop_distance_pct(self) -> float:
        """Return absolute stop distance as a fraction of entry price."""
        if self.entry_price <= 0:
            return 0.0
        return abs(self.entry_price - self.stop_loss) / self.entry_price


class Position(BaseModel):
    """Open position record."""

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=utc_now)
    symbol: str
    side: Side
    size: float
    entry_price: float
    liquidation_price: float | None = None
    unrealized_pnl: float = 0.0
    leverage: float = 1.0
    strategy_name: str
    stop_loss: float | None = None
    take_profit: float | None = None


class Trade(BaseModel):
    """Trade row model."""

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=utc_now)
    strategy_name: str
    symbol: str
    side: Side
    size: float
    entry_price: float
    exit_price: float | None = None
    pnl_usd: float | None = None
    pnl_pct: float | None = None
    fees_usd: float = 0.0
    hold_duration_sec: int | None = None
    reason_entry: str
    reason_exit: str | None = None
    regime: str | None = None
    status: TradeStatus = TradeStatus.OPEN


class EquitySnapshot(BaseModel):
    """Account equity snapshot."""

    timestamp: datetime = Field(default_factory=utc_now)
    execution_mode: str = "paper_sim"
    balance_usd: float
    equity_usd: float
    margin_used_usd: float
    open_position_count: int
    daily_pnl: float
    weekly_pnl: float
    mdd_pct: float


class CircuitBreakerEvent(BaseModel):
    """Persisted circuit breaker event."""

    timestamp: datetime = Field(default_factory=utc_now)
    breaker_type: str
    threshold_value: float
    observed_value: float
    action: str


class MarketState(BaseModel):
    """Market data update passed into strategies."""

    symbol: str
    timestamp: datetime = Field(default_factory=utc_now)
    bid: float
    ask: float
    mid: float
    last_trade_price: float
    volume_24h: float | None = None
    funding_rate: float | None = None
    open_interest: float | None = None
    bars_5m: list[dict[str, Any]] = Field(default_factory=list)
    trade_events: list[dict[str, Any]] = Field(default_factory=list)
    bid_levels: list[dict[str, Any]] | None = None
    ask_levels: list[dict[str, Any]] | None = None
    book_bids: list[dict[str, Any]] = Field(default_factory=list)
    book_asks: list[dict[str, Any]] = Field(default_factory=list)
    equity_usd: float | None = None
    open_positions: list[Position] = Field(default_factory=list)
