"""Polymarket paper-trading strategies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import structlog

from src.polymarket.ev_model import (
    EwmaLogVarianceTracker,
    student_t_digital_yes_probability,
)
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

MIN_LATENCY_ENTRY_PRICE = 0.05
MAX_LATENCY_ENTRY_PRICE = 0.95


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


@dataclass(slots=True)
class EvCandidate:
    """One eligible EV-gated entry candidate."""

    side: PolymarketSide
    p_model: float
    ask_price: float
    spread_half: float
    fee_per_share: float
    raw_edge: float
    net_ev: float
    order_book: PolymarketOrderBook
    entry_reason_code: str


class MultiOutcomeSumArbitrageStrategy:
    """Buy YES and NO when their filled cost is below guaranteed $1 payout."""

    name = "multi_outcome_sum_arb"

    def __init__(
        self,
        threshold: float = 0.02,
        max_account_pct: float = 0.10,
        max_stake_usd: float = 50.0,
        cooldown_seconds: int = 300,
    ) -> None:
        self._threshold = threshold
        self._max_account_pct = max_account_pct
        self._max_stake_usd = max_stake_usd
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

        max_notional = _capped_notional(
            account_equity=account_equity,
            max_account_pct=self._max_account_pct,
            max_stake_usd=self._max_stake_usd,
        )
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
            horizon=context.market.horizon,
            window_seconds=context.market.window_seconds,
            reason_entry=reason,
            risk_profile="guaranteed_if_both_legs_fill",
            max_stake_usd=max_notional,
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
        max_stake_usd: float = 25.0,
        cooldown_seconds: int = 300,
    ) -> None:
        self._edge_threshold = edge_threshold
        self._max_account_pct = max_account_pct
        self._max_stake_usd = max_stake_usd
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
        if yes_ask is not None and _is_latency_price_tradable(yes_ask):
            candidates.append((PolymarketSide.YES, fair_yes - yes_ask, yes_ask, context.yes_book))
        if no_ask is not None and _is_latency_price_tradable(no_ask):
            candidates.append((PolymarketSide.NO, (1.0 - fair_yes) - no_ask, no_ask, context.no_book))
        if not candidates:
            return None

        side, edge, ask_price, book = max(candidates, key=lambda candidate: candidate[1])
        if edge <= self._edge_threshold:
            return None

        max_notional = _capped_notional(
            account_equity=account_equity,
            max_account_pct=self._max_account_pct,
            max_stake_usd=self._max_stake_usd,
        )
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
            horizon=context.market.horizon,
            window_seconds=context.market.window_seconds,
            reason_entry=reason,
            risk_profile="probabilistic_latency_edge",
            max_stake_usd=max_notional,
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


class EvGatedStrategy:
    """Buy only when a fee-adjusted digital-option model clears a hard EV gate."""

    name = "ev_gated"

    def __init__(
        self,
        min_edge: float = 0.04,
        stake_usd: float = 30.0,
        fee_band_low: float = 0.45,
        fee_band_high: float = 0.55,
        fee_rate: float = 0.072,
        vol_lambda: float = 0.97,
        vol_nu: float = 4.0,
        vol_sample_sec: float = 2.0,
        cooldown_seconds: int = 300,
    ) -> None:
        self._min_edge = min_edge
        self._stake_usd = stake_usd
        self._fee_band_low = fee_band_low
        self._fee_band_high = fee_band_high
        self._fee_rate = fee_rate
        self._vol_nu = vol_nu
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._last_signal_at: dict[str, datetime] = {}
        self._volatility = EwmaLogVarianceTracker(
            lambda_=vol_lambda,
            sample_interval_sec=vol_sample_sec,
        )
        self._vol_lambda = vol_lambda
        self._vol_sample_sec = vol_sample_sec

    async def on_context(
        self,
        context: PolymarketMarketContext,
        account_equity: float,
        volatility_tracker: CoinbaseVolatilityTracker,
        now: datetime | None = None,
    ) -> PolymarketSignal | None:
        """Evaluate one short-horizon market through the EV entry gate."""
        del volatility_tracker
        current_time = now or datetime.now(tz=UTC)
        self._volatility.record_price(
            asset_symbol=context.market.asset_symbol,
            price=context.snapshot.coinbase_ref_price,
            timestamp=context.snapshot.timestamp,
        )
        if self._in_cooldown(context.market.market_id, current_time):
            return None
        price_to_beat = context.snapshot.price_to_beat
        if price_to_beat is None or price_to_beat <= 0:
            logger.info(
                "polymarket_ev_gated_skipped_missing_strike",
                market_id=context.market.market_id,
                asset_symbol=context.market.asset_symbol,
            )
            return None
        variance_rate = self._volatility.variance_rate(context.market.asset_symbol)
        if variance_rate is None:
            return None
        seconds_to_resolution = max(float(context.snapshot.seconds_to_resolution), 0.0)
        if seconds_to_resolution <= 0:
            return None

        fair_yes = student_t_digital_yes_probability(
            spot_price=context.snapshot.coinbase_ref_price,
            price_to_beat=price_to_beat,
            seconds_to_resolution=seconds_to_resolution,
            variance_rate=variance_rate,
            degrees_of_freedom=self._vol_nu,
        )
        candidate = evaluate_ev_entry_gate(
            fair_yes_probability=fair_yes,
            yes_book=context.yes_book,
            no_book=context.no_book,
            fee_rate=self._fee_rate,
            min_edge=self._min_edge,
            fee_band_low=self._fee_band_low,
            fee_band_high=self._fee_band_high,
        )
        if candidate is None:
            return None

        max_notional = min(max(account_equity, 0.0), self._stake_usd)
        shares = _shares_for_notional(
            candidate.order_book.asks,
            max_notional,
            self._fee_rate,
        )
        if shares <= 0:
            return None
        fill = _simulate_fill(candidate.order_book.asks, shares)
        if fill is None:
            return None

        self._last_signal_at[context.market.market_id] = current_time
        if candidate.entry_reason_code == "expensive_side_ev":
            logger.info(
                "polymarket_ev_gated_expensive_side_accepted",
                market_id=context.market.market_id,
                side=candidate.side.value,
                p_model=candidate.p_model,
                ask_price=candidate.ask_price,
                net_ev=candidate.net_ev,
            )
        reason = (
            f"EV gate accepted {candidate.side.value}: p_model={candidate.p_model:.4f}, "
            f"ask={candidate.ask_price:.4f}, net_ev={candidate.net_ev:.4f}, "
            f"strike={price_to_beat:.2f}, spot={context.snapshot.coinbase_ref_price:.2f}."
        )
        logger.info(
            "polymarket_ev_gated_signal",
            market_id=context.market.market_id,
            asset_symbol=context.market.asset_symbol,
            side=candidate.side.value,
            p_model=candidate.p_model,
            edge_at_entry=candidate.net_ev,
            shares=fill.shares,
            entry_reason_code=candidate.entry_reason_code,
        )
        return PolymarketSignal(
            strategy_name=self.name,
            market_id=context.market.market_id,
            horizon=context.market.horizon,
            window_seconds=context.market.window_seconds,
            reason_entry=reason,
            risk_profile="probabilistic_ev_gated_digital_model",
            max_stake_usd=max_notional,
            legs=[
                PolymarketSignalLeg(
                    side=candidate.side,
                    shares=fill.shares,
                    order_book=candidate.order_book,
                    expected_avg_price=fill.avg_price,
                    p_model=candidate.p_model,
                    edge_at_entry=candidate.net_ev,
                    entry_reason_code=candidate.entry_reason_code,
                )
            ],
            features={
                "fair_yes_probability": fair_yes,
                "p_model": candidate.p_model,
                "raw_edge": candidate.raw_edge,
                "edge_at_entry": candidate.net_ev,
                "fee_per_share": candidate.fee_per_share,
                "spread_half": candidate.spread_half,
                "ask_price": candidate.ask_price,
                "price_to_beat": price_to_beat,
                "coinbase_ref_price": context.snapshot.coinbase_ref_price,
                "seconds_to_resolution": seconds_to_resolution,
                "variance_rate": variance_rate,
                "vol_lambda": self._vol_lambda,
                "vol_nu": self._vol_nu,
                "vol_sample_sec": self._vol_sample_sec,
                "entry_reason_code": candidate.entry_reason_code,
                "min_edge": self._min_edge,
            },
        )

    def _in_cooldown(self, market_id: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(market_id)
        return last_signal_at is not None and now - last_signal_at < self._cooldown


def evaluate_ev_entry_gate(
    *,
    fair_yes_probability: float,
    yes_book: PolymarketOrderBook,
    no_book: PolymarketOrderBook,
    fee_rate: float,
    min_edge: float,
    fee_band_low: float,
    fee_band_high: float,
) -> EvCandidate | None:
    """Return the preferred EV candidate when the hard entry gate clears."""
    candidates = [
        _ev_candidate_for_side(
            side=PolymarketSide.YES,
            p_model=fair_yes_probability,
            order_book=yes_book,
            other_book=no_book,
            fee_rate=fee_rate,
            min_edge=min_edge,
            fee_band_low=fee_band_low,
            fee_band_high=fee_band_high,
        ),
        _ev_candidate_for_side(
            side=PolymarketSide.NO,
            p_model=1.0 - fair_yes_probability,
            order_book=no_book,
            other_book=yes_book,
            fee_rate=fee_rate,
            min_edge=min_edge,
            fee_band_low=fee_band_low,
            fee_band_high=fee_band_high,
        ),
    ]
    eligible = [candidate for candidate in candidates if candidate is not None]
    if not eligible:
        return None
    return min(eligible, key=lambda candidate: (candidate.ask_price, -candidate.net_ev))


def _ev_candidate_for_side(
    *,
    side: PolymarketSide,
    p_model: float,
    order_book: PolymarketOrderBook,
    other_book: PolymarketOrderBook,
    fee_rate: float,
    min_edge: float,
    fee_band_low: float,
    fee_band_high: float,
) -> EvCandidate | None:
    ask_price = order_book.best_ask
    bid_price = order_book.best_bid
    if ask_price is None or bid_price is None:
        return None
    if fee_band_low <= ask_price <= fee_band_high:
        return None
    spread_half = max(ask_price - bid_price, 0.0) / 2.0
    fee_per_share = _fee_per_share(ask_price, fee_rate)
    raw_edge = p_model - ask_price
    net_ev = raw_edge - fee_per_share - spread_half
    if net_ev < min_edge:
        return None
    other_ask = other_book.best_ask
    reason_code = (
        "expensive_side_ev"
        if other_ask is not None and ask_price > other_ask
        else "cheap_side_ev"
    )
    return EvCandidate(
        side=side,
        p_model=p_model,
        ask_price=ask_price,
        spread_half=spread_half,
        fee_per_share=fee_per_share,
        raw_edge=raw_edge,
        net_ev=net_ev,
        order_book=order_book,
        entry_reason_code=reason_code,
    )


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


def _capped_notional(
    account_equity: float,
    max_account_pct: float,
    max_stake_usd: float,
) -> float:
    return max(min(account_equity * max_account_pct, max_stake_usd), 0.0)


def _is_latency_price_tradable(price: float | None) -> bool:
    return (
        price is not None
        and MIN_LATENCY_ENTRY_PRICE <= price <= MAX_LATENCY_ENTRY_PRICE
    )


STRATEGY_REGISTRY: dict[
    str,
    type[MultiOutcomeSumArbitrageStrategy] | type[LatencyArbStrategy] | type[EvGatedStrategy],
] = {
    MultiOutcomeSumArbitrageStrategy.name: MultiOutcomeSumArbitrageStrategy,
    LatencyArbStrategy.name: LatencyArbStrategy,
    EvGatedStrategy.name: EvGatedStrategy,
}
