"""Domain models for Polymarket paper trading."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4


class PolymarketSide(StrEnum):
    """Binary prediction-market side."""

    YES = "YES"
    NO = "NO"


class PolymarketPositionStatus(StrEnum):
    """Paper position lifecycle states."""

    OPEN = "open"
    CLOSED = "closed"
    RESOLVED = "resolved"


class PolymarketTradeStatus(StrEnum):
    """Paper trade lifecycle states."""

    OPEN = "open"
    CLOSED = "closed"
    RESOLVED = "resolved"


@dataclass(slots=True)
class PolymarketBookLevel:
    """One CLOB book level in shares at a probability price."""

    price: float
    size: float


@dataclass(slots=True)
class PolymarketOrderBook:
    """Read-only CLOB order book for a single outcome token."""

    token_id: str
    market_id: str
    side: PolymarketSide
    timestamp: datetime
    bids: list[PolymarketBookLevel] = field(default_factory=list)
    asks: list[PolymarketBookLevel] = field(default_factory=list)

    @property
    def best_bid(self) -> float | None:
        """Return the highest bid price."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        """Return the lowest ask price."""
        return self.asks[0].price if self.asks else None

    @property
    def ask_depth(self) -> float:
        """Return total buyable ask-side depth in shares."""
        return sum(level.size for level in self.asks)

    @property
    def bid_depth(self) -> float:
        """Return total sellable bid-side depth in shares."""
        return sum(level.size for level in self.bids)


@dataclass(slots=True)
class PolymarketMarket:
    """A binary crypto market mapped to YES/NO CLOB token IDs."""

    market_id: str
    condition_id: str
    slug: str | None
    question: str
    yes_token_id: str
    no_token_id: str
    asset_symbol: str
    horizon: str
    window_seconds: int
    window_open_time: datetime
    resolution_time: datetime | None
    reference_price: float | None = None
    fees_enabled: bool = True
    taker_fee_rate: float = 0.07


@dataclass(slots=True)
class CoinbaseSpotPrice:
    """Coinbase reference price for one crypto asset."""

    product_id: str
    asset_symbol: str
    price: float
    timestamp: datetime


@dataclass(slots=True)
class PolymarketMarketSnapshot:
    """Synchronized Polymarket CLOB and Coinbase spot snapshot."""

    market_id: str
    market_question: str
    yes_price: float
    no_price: float
    yes_book_depth: float
    no_book_depth: float
    coinbase_ref_price: float
    implied_gap: float
    resolution_time: datetime | None
    horizon: str
    window_seconds: int
    seconds_to_resolution: int
    price_to_beat: float | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    def to_payload(self) -> dict[str, object]:
        """Return a Supabase row payload."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "market_id": self.market_id,
            "market_question": self.market_question,
            "yes_price": round(self.yes_price, 4),
            "no_price": round(self.no_price, 4),
            "yes_book_depth": round(self.yes_book_depth, 4),
            "no_book_depth": round(self.no_book_depth, 4),
            "coinbase_ref_price": round(self.coinbase_ref_price, 2),
            "implied_gap": round(self.implied_gap, 4),
            "horizon": self.horizon,
            "window_seconds": self.window_seconds,
            "seconds_to_resolution": self.seconds_to_resolution,
            "price_to_beat": (
                round(self.price_to_beat, 2) if self.price_to_beat is not None else None
            ),
            "resolution_time": (
                self.resolution_time.isoformat() if self.resolution_time is not None else None
            ),
        }


@dataclass(slots=True)
class PolymarketMarketContext:
    """Live strategy context with snapshot plus full CLOB books."""

    market: PolymarketMarket
    snapshot: PolymarketMarketSnapshot
    yes_book: PolymarketOrderBook
    no_book: PolymarketOrderBook


@dataclass(slots=True)
class PolymarketPosition:
    """In-memory paper position for prediction shares."""

    id: UUID
    account_id: str
    timestamp: datetime
    market_id: str
    horizon: str
    window_seconds: int
    side: PolymarketSide
    shares: float
    avg_entry_price: float
    current_price: float
    unrealized_pnl: float
    status: PolymarketPositionStatus
    resolution_outcome: PolymarketSide | None = None
    fees_paid: float = 0.0

    @classmethod
    def open(
        cls,
        account_id: str,
        market_id: str,
        horizon: str,
        window_seconds: int,
        side: PolymarketSide,
        shares: float,
        avg_entry_price: float,
        fees_paid: float,
    ) -> "PolymarketPosition":
        """Create an open paper position."""
        return cls(
            id=uuid4(),
            account_id=account_id,
            timestamp=datetime.now(tz=UTC),
            market_id=market_id,
            horizon=horizon,
            window_seconds=window_seconds,
            side=side,
            shares=shares,
            avg_entry_price=avg_entry_price,
            current_price=avg_entry_price,
            unrealized_pnl=0.0,
            status=PolymarketPositionStatus.OPEN,
            fees_paid=fees_paid,
        )


@dataclass(slots=True)
class PolymarketTrade:
    """Paper trade row for prediction shares."""

    id: UUID
    account_id: str
    timestamp: datetime
    market_id: str
    horizon: str
    window_seconds: int
    strategy_name: str
    side: PolymarketSide
    shares: float
    entry_price: float
    exit_price: float | None
    pnl_usd: float | None
    status: PolymarketTradeStatus
    reason_entry: str
    reason_exit: str | None = None
    p_model: float | None = None
    edge_at_entry: float | None = None
    fee_paid: float | None = None
    entry_reason_code: str | None = None

    def to_payload(self) -> dict[str, object]:
        """Return a Supabase row payload."""
        return {
            "id": str(self.id),
            "account_id": self.account_id,
            "timestamp": self.timestamp.isoformat(),
            "market_id": self.market_id,
            "horizon": self.horizon,
            "window_seconds": self.window_seconds,
            "strategy_name": self.strategy_name,
            "side": self.side.value,
            "shares": round(self.shares, 6),
            "entry_price": round(self.entry_price, 4),
            "exit_price": round(self.exit_price, 4) if self.exit_price is not None else None,
            "pnl_usd": round(self.pnl_usd, 2) if self.pnl_usd is not None else None,
            "status": self.status.value,
            "reason_entry": self.reason_entry,
            "reason_exit": self.reason_exit,
            "p_model": round(self.p_model, 6) if self.p_model is not None else None,
            "edge_at_entry": (
                round(self.edge_at_entry, 6) if self.edge_at_entry is not None else None
            ),
            "fee_paid": round(self.fee_paid, 6) if self.fee_paid is not None else None,
            "entry_reason_code": self.entry_reason_code,
        }


def compute_implied_gap(
    yes_price: float,
    no_price: float,
    coinbase_ref_price: float,
    market_reference_price: float | None,
) -> float:
    """Compute a conservative gap metric for the synchronized snapshot."""
    if market_reference_price is None:
        return yes_price + no_price - 1.0
    reference_yes_price = 1.0 if coinbase_ref_price >= market_reference_price else 0.0
    return yes_price - reference_yes_price


def fee_for_trade(shares: float, avg_price: float, taker_fee_rate: float) -> float:
    """Return the Polymarket taker fee in USDC."""
    if shares <= 0 or avg_price <= 0 or taker_fee_rate <= 0:
        return 0.0
    return shares * taker_fee_rate * avg_price * (1 - avg_price)
