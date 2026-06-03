"""Coinbase-derived realized volatility tracker for Polymarket fair value."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(slots=True)
class PricePoint:
    """One observed Coinbase spot price."""

    timestamp: datetime
    price: float


class CoinbaseVolatilityTracker:
    """Track recent Coinbase spot and estimate log-price variance per second."""

    def __init__(self, window_sec: int = 900) -> None:
        self._window = timedelta(seconds=window_sec)
        self._prices: dict[str, deque[PricePoint]] = {}

    def record_price(self, asset_symbol: str, price: float, timestamp: datetime) -> None:
        """Record one spot observation."""
        if price <= 0:
            return
        symbol = asset_symbol.upper()
        points = self._prices.setdefault(symbol, deque())
        points.append(PricePoint(timestamp=timestamp, price=price))
        cutoff = timestamp - self._window
        while points and points[0].timestamp < cutoff:
            points.popleft()

    def variance_rate(self, asset_symbol: str) -> float | None:
        """Return realized log-return variance per second."""
        points = list(self._prices.get(asset_symbol.upper()) or [])
        if len(points) < 3:
            return None
        squared_returns = 0.0
        elapsed_sec = 0.0
        for previous, current in zip(points, points[1:], strict=False):
            dt = (current.timestamp - previous.timestamp).total_seconds()
            if dt <= 0 or previous.price <= 0 or current.price <= 0:
                continue
            squared_returns += math.log(current.price / previous.price) ** 2
            elapsed_sec += dt
        if elapsed_sec <= 0 or squared_returns <= 0:
            return None
        return squared_returns / elapsed_sec

    def fair_yes_probability(
        self,
        asset_symbol: str,
        spot_price: float,
        reference_price: float,
        seconds_to_resolution: float,
    ) -> float | None:
        """Estimate P(spot at resolution > reference).

        Assumptions: zero drift, log-price normality, and realized Coinbase
        variance over the recent rolling window as the forward variance proxy.
        This is a probabilistic edge estimate, not a guaranteed arbitrage.
        """
        if spot_price <= 0 or reference_price <= 0:
            return None
        if seconds_to_resolution <= 0:
            return 1.0 if spot_price >= reference_price else 0.0
        variance_rate = self.variance_rate(asset_symbol)
        if variance_rate is None:
            return None
        resolution_std = math.sqrt(max(variance_rate * seconds_to_resolution, 0.0))
        if resolution_std <= 1e-12:
            return 1.0 if spot_price >= reference_price else 0.0
        z_score = math.log(spot_price / reference_price) / resolution_std
        return _normal_cdf(z_score)


def _normal_cdf(value: float) -> float:
    """Return the standard normal CDF without scipy."""
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))

