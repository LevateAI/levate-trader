"""Strategy signal models for Polymarket paper execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.polymarket.models import PolymarketOrderBook, PolymarketSide


@dataclass(slots=True)
class PolymarketSignalLeg:
    """One buy leg for a Polymarket paper signal."""

    side: PolymarketSide
    shares: float
    order_book: PolymarketOrderBook
    expected_avg_price: float


@dataclass(slots=True)
class PolymarketSignal:
    """Actionable Polymarket paper signal."""

    strategy_name: str
    market_id: str
    horizon: str
    window_seconds: int
    reason_entry: str
    risk_profile: str
    legs: list[PolymarketSignalLeg]
    features: dict[str, Any] = field(default_factory=dict)
    max_stake_usd: float | None = None
