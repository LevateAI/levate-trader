"""Polymarket paper-trading strategies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import structlog

from src.polymarket.models import (
    PolymarketBookLevel,
    PolymarketMarketContext,
    PolymarketOrderBook,
    PolymarketSide,
    fee_for_trade,
)
from src.polymarket.signals import PolymarketSignal, PolymarketSignalLeg
from src.polymarket.volatility import CoinbaseVolatilityTracker

logger = structlog.get_logger(__name__)


class PolymarketStrategy(Protocol):
    """Protocol for Polymarket snapshot strategies."""

    name: str

    async def on_context(
        self,
        context: PolymarketMarketContext,
        account_equity: float,
        volatility_tracker: CoinbaseVolatilityTracker,
        now: datetime | None = None,
    ) -> PolymarketSignal | None:
        """Evaluate one synchronized market context."""


@dataclass(slots=True)
class FillSimulation:
    """Simulated book walk for a single side."""

    shares: float
    notional: float
    avg_price: float


class MultiOutcomeSumArbitrageStrategy:
    """Buy YES and NO when their filled cost is below guaranteed $1 payout."""

    name = "multi_outcome_sum_arb"

    def __init__(
        self,
        threshold: float = 0.02,
        max_account_pct: float = 0.10,
        cooldown_seconds: int = 300,
    ) -> None:
        self._threshold = threshold
        self._max_account_pct = max_account_pct
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._last_signal_at: dict[str, datetime] = {}

    async def on_context(
        self,
        context: PolymarketMarketContext,
        account_equity: float,
        volatility_tracker: CoinbaseVolatilityTracker,
        now: datetime | None = None,
    ) -> PolymarketSignal | None:
        """Evaluate a binary sum-arbitrage opportunity."""
        del volatility_tracker
        current_time = now or datetime.now(tz=UTC)
        if self._in_cooldown(context.market.market_id, current_time):
            return None
        yes_ask = context.yes_book.best_ask
        no_ask = context.no_book.best_ask
        if yes_ask is None or no_ask is None:
            return None

        best_fee = _fee_per_share(yes_ask, context.market.taker_fee_rate) + _fee_per_share(
            no_ask,
            context.market.taker_fee_rate,
        )
        best_edge = 1.0 - yes_ask - no_ask - best_fee
        if best_edge <= self._threshold:
            return None

        max_notional = max(account_equity * self._max_account_pct, 0.0)
        target_shares = _shares_for_pair_notional(
            context.yes_book.asks,
            context.no_book.asks,
            max_notional,
            context.market.taker_fee_rate,
        )
        target_shares = min(
            target_shares,
            context.yes_book.ask_depth,
            context.no_book.ask_depth,
        )
        if target_shares <= 0:
            return None

        yes_fill = _simulate_fill(context.yes_book.asks, target_shares)
        no_fill = _simulate_fill(context.no_book.asks, target_shares)
        if yes_fill is None or no_fill is None or yes_fill.shares != no_fill.shares:
            return None

        total_fee = fee_for_trade(
            yes_fill.shares,
            yes_fill.avg_price,
            context.market.taker_fee_rate,
        ) + fee_for_trade(
            no_fill.shares,
            no_fill.avg_price,
            context.market.taker_fee_rate,
        )
        edge_after_fees = 1.0 - ((yes_fill.notional + no_fill.notional + total_fee) / yes_fill.shares)
        if edge_after_fees <= self._threshold:
            logger.info(
                "arb_evaporated_on_second_leg",
                strategy_name=self.name,
                market_id=context.market.market_id,
                best_edge=best_edge,
                edge_after_fees=edge_after_fees,
                shares=yes_fill.shares,
            )
            return None

        self._last_signal_at[context.market.market_id] = current_time
        reason = (
            f"YES+NO filled cost captures {edge_after_fees:.4f} guaranteed edge/share "
            f"after fees; buying {yes_fill.shares:.4f} shares of both outcomes."
        )
        logger.info(
            "polymarket_sum_arb_signal",
            market_id=context.market.market_id,
            shares=yes_fill.shares,
            edge_after_fees=edge_after_fees,
        )
        return PolymarketSignal(
            strategy_name=self.name,
            market_id=context.market.market_id,
            reason_entry=reason,
            risk_profile="guaranteed_if_both_legs_fill",
            legs=[
                PolymarketSignalLeg(
                    side=PolymarketSide.YES,
                    shares=yes_fill.shares,
                    order_book=context.yes_book,
                    expected_avg_price=yes_fill.avg_price,
                ),
                PolymarketSignalLeg(
                    side=PolymarketSide.NO,
                    shares=no_fill.shares,
                    order_book=context.no_book,
                    expected_avg_price=no_fill.avg_price,
                ),
            ],
            features={
                "best_edge_after_fees": best_edge,
                "edge_after_fees": edge_after_fees,
                "yes_avg_price": yes_fill.avg_price,
                "no_avg_price": no_fill.avg_price,
                "threshold": self._threshold,
            },
        )

    def _in_cooldown(self, market_id: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(market_id)
        return last_signal_at is not None and now - last_signal_at < self._cooldown


class LatencyArbStrategy:
    """Buy a stale underpriced side using Coinbase-derived fair probability."""

    name = "latency_arb"

    def __init__(
        self,
        edge_threshold: float = 0.05,
        max_account_pct: float = 0.05,
        cooldown_seconds: int = 300,
    ) -> None:
        self._edge_threshold = edge_threshold
        self._max_account_pct = max_account_pct
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._last_signal_at: dict[str, datetime] = {}

    async def on_context(
        self,
        context: PolymarketMarketContext,
        account_equity: float,
        volatility_tracker: CoinbaseVolatilityTracker,
        now: datetime | None = None,
    ) -> PolymarketSignal | None:
        """Evaluate a probabilistic latency-arb opportunity."""
        current_time = now or datetime.now(tz=UTC)
        if self._in_cooldown(context.market.market_id, current_time):
            return None
        if context.market.reference_price is None or context.market.resolution_time is None:
            return None
        seconds_to_resolution = (
            context.market.resolution_time - current_time
        ).total_seconds()
        fair_yes = volatility_tracker.fair_yes_probability(
            asset_symbol=context.market.asset_symbol,
            spot_price=context.snapshot.coinbase_ref_price,
            reference_price=context.market.reference_price,
            seconds_to_resolution=seconds_to_resolution,
        )
        if fair_yes is None:
            return None

        yes_ask = context.yes_book.best_ask
        no_ask = context.no_book.best_ask
        candidates: list[tuple[PolymarketSide, float, float, PolymarketOrderBook]] = []
        if yes_ask is not None:
            candidates.append((PolymarketSide.YES, fair_yes - yes_ask, yes_ask, context.yes_book))
        if no_ask is not None:
            candidates.append((PolymarketSide.NO, (1.0 - fair_yes) - no_ask, no_ask, context.no_book))
        if not candidates:
            return None

        side, edge, ask_price, book = max(candidates, key=lambda candidate: candidate[1])
        if edge <= self._edge_threshold:
            return None

        max_notional = account_equity * self._max_account_pct
        shares = _shares_for_notional(book.asks, max_notional, context.market.taker_fee_rate)
        if shares <= 0:
            return None
        fill = _simulate_fill(book.asks, shares)
        if fill is None:
            return None

        self._last_signal_at[context.market.market_id] = current_time
        reason = (
            f"Coinbase-derived fair {side.value} probability shows {edge:.4f} edge over "
            f"Polymarket ask {ask_price:.4f}; probabilistic latency arb, not guaranteed."
        )
        logger.info(
            "polymarket_latency_arb_signal",
            market_id=context.market.market_id,
            side=side.value,
            fair_yes_probability=fair_yes,
            edge=edge,
            shares=fill.shares,
        )
        return PolymarketSignal(
            strategy_name=self.name,
            market_id=context.market.market_id,
            reason_entry=reason,
            risk_profile="probabilistic_latency_edge",
            legs=[
                PolymarketSignalLeg(
                    side=side,
                    shares=fill.shares,
                    order_book=book,
                    expected_avg_price=fill.avg_price,
                )
            ],
            features={
                "fair_yes_probability": fair_yes,
                "edge": edge,
                "ask_price": ask_price,
                "reference_price": context.market.reference_price,
                "coinbase_ref_price": context.snapshot.coinbase_ref_price,
                "seconds_to_resolution": max(seconds_to_resolution, 0.0),
                "threshold": self._edge_threshold,
            },
        )

    def _in_cooldown(self, market_id: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(market_id)
        return last_signal_at is not None and now - last_signal_at < self._cooldown


def _simulate_fill(
    asks: list[PolymarketBookLevel],
    shares: float,
) -> FillSimulation | None:
    remaining = shares
    filled = 0.0
    notional = 0.0
    for level in asks:
        if remaining <= 0:
            break
        fill_size = min(remaining, level.size)
        filled += fill_size
        notional += fill_size * level.price
        remaining -= fill_size
    if filled <= 0 or remaining > 1e-9:
        return None
    return FillSimulation(shares=filled, notional=notional, avg_price=notional / filled)


def _shares_for_pair_notional(
    yes_asks: list[PolymarketBookLevel],
    no_asks: list[PolymarketBookLevel],
    max_notional: float,
    fee_rate: float,
) -> float:
    available_depth = min(
        sum(level.size for level in yes_asks),
        sum(level.size for level in no_asks),
    )
    if available_depth <= 0:
        return 0.0
    low = 0.0
    high = available_depth
    for _ in range(30):
        mid = (low + high) / 2
        yes_fill = _simulate_fill(yes_asks, mid)
        no_fill = _simulate_fill(no_asks, mid)
        if yes_fill is None or no_fill is None:
            high = mid
            continue
        total_cost = (
            yes_fill.notional
            + no_fill.notional
            + fee_for_trade(mid, yes_fill.avg_price, fee_rate)
            + fee_for_trade(mid, no_fill.avg_price, fee_rate)
        )
        if total_cost <= max_notional:
            low = mid
        else:
            high = mid
    return low


def _shares_for_notional(
    asks: list[PolymarketBookLevel],
    max_notional: float,
    fee_rate: float,
) -> float:
    available_depth = sum(level.size for level in asks)
    low = 0.0
    high = available_depth
    for _ in range(30):
        mid = (low + high) / 2
        fill = _simulate_fill(asks, mid)
        if fill is None:
            high = mid
            continue
        total_cost = fill.notional + fee_for_trade(mid, fill.avg_price, fee_rate)
        if total_cost <= max_notional:
            low = mid
        else:
            high = mid
    return low


def _fee_per_share(price: float, fee_rate: float) -> float:
    return fee_for_trade(1.0, price, fee_rate)


STRATEGY_REGISTRY: dict[str, type[MultiOutcomeSumArbitrageStrategy] | type[LatencyArbStrategy]] = {
    MultiOutcomeSumArbitrageStrategy.name: MultiOutcomeSumArbitrageStrategy,
    LatencyArbStrategy.name: LatencyArbStrategy,
}
