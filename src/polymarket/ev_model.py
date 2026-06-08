"""Digital-option fair value helpers for short-horizon Polymarket markets."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(slots=True)
class EvPricePoint:
    """One sampled Coinbase spot price used for EV-gated volatility."""

    timestamp: datetime
    price: float


class EwmaLogVarianceTracker:
    """EWMA log-return variance sampler for short-horizon crypto spots."""

    def __init__(self, lambda_: float = 0.97, sample_interval_sec: float = 2.0) -> None:
        if not 0 < lambda_ < 1:
            raise ValueError("EWMA lambda must be between 0 and 1")
        if sample_interval_sec <= 0:
            raise ValueError("sample interval must be positive")
        self._lambda = lambda_
        self._sample_interval = timedelta(seconds=sample_interval_sec)
        self._last_sample: dict[str, EvPricePoint] = {}
        self._variance_rate: dict[str, float] = {}

    def record_price(self, asset_symbol: str, price: float, timestamp: datetime) -> None:
        """Record a sampled spot tick when enough time has elapsed."""
        if price <= 0:
            return
        symbol = asset_symbol.upper()
        previous = self._last_sample.get(symbol)
        if previous is None:
            self._last_sample[symbol] = EvPricePoint(timestamp=timestamp, price=price)
            return
        elapsed_sec = (timestamp - previous.timestamp).total_seconds()
        if elapsed_sec <= 0:
            return
        if timestamp - previous.timestamp < self._sample_interval:
            return
        sample_variance_rate = math.log(price / previous.price) ** 2 / elapsed_sec
        previous_variance = self._variance_rate.get(symbol)
        if previous_variance is None:
            self._variance_rate[symbol] = sample_variance_rate
        else:
            self._variance_rate[symbol] = (
                self._lambda * previous_variance
                + (1.0 - self._lambda) * sample_variance_rate
            )
        self._last_sample[symbol] = EvPricePoint(timestamp=timestamp, price=price)

    def variance_rate(self, asset_symbol: str) -> float | None:
        """Return EWMA log-return variance per second for an asset."""
        variance = self._variance_rate.get(asset_symbol.upper())
        if variance is None or variance <= 0:
            return None
        return variance


def student_t_digital_yes_probability(
    *,
    spot_price: float,
    price_to_beat: float,
    seconds_to_resolution: float,
    variance_rate: float,
    degrees_of_freedom: float = 4.0,
) -> float:
    """Return P(spot at resolution > strike) using a Student-t digital model."""
    if spot_price <= 0 or price_to_beat <= 0:
        raise ValueError("spot and strike must be positive")
    if seconds_to_resolution <= 0:
        return 1.0 if spot_price >= price_to_beat else 0.0
    if variance_rate <= 0:
        return 1.0 if spot_price >= price_to_beat else 0.0
    resolution_std = math.sqrt(max(variance_rate * seconds_to_resolution, 0.0))
    if resolution_std <= 1e-12:
        return 1.0 if spot_price >= price_to_beat else 0.0
    z_score = math.log(spot_price / price_to_beat) / resolution_std
    return student_t_cdf(z_score, degrees_of_freedom)


def normal_digital_yes_probability(
    *,
    spot_price: float,
    price_to_beat: float,
    seconds_to_resolution: float,
    variance_rate: float,
) -> float:
    """Return the normal-CDF version of the digital model for calibration tests."""
    if spot_price <= 0 or price_to_beat <= 0:
        raise ValueError("spot and strike must be positive")
    if seconds_to_resolution <= 0 or variance_rate <= 0:
        return 1.0 if spot_price >= price_to_beat else 0.0
    resolution_std = math.sqrt(max(variance_rate * seconds_to_resolution, 0.0))
    if resolution_std <= 1e-12:
        return 1.0 if spot_price >= price_to_beat else 0.0
    z_score = math.log(spot_price / price_to_beat) / resolution_std
    return normal_cdf(z_score)


def student_t_cdf(value: float, degrees_of_freedom: float = 4.0) -> float:
    """Return Student-t CDF without scipy."""
    if degrees_of_freedom <= 0:
        raise ValueError("degrees of freedom must be positive")
    if math.isclose(degrees_of_freedom, 4.0):
        return _student_t_cdf_nu4(value)
    return _student_t_cdf_numeric(value, degrees_of_freedom)


def normal_cdf(value: float) -> float:
    """Return standard normal CDF."""
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _student_t_cdf_nu4(value: float) -> float:
    abs_value = abs(value)
    root = math.sqrt(abs_value * abs_value + 4.0)
    integral = (3.0 * abs_value) / (4.0 * root) - (
        abs_value**3 / (4.0 * root**3)
    )
    probability = 0.5 + integral if value >= 0 else 0.5 - integral
    return min(max(probability, 0.0), 1.0)


def _student_t_cdf_numeric(value: float, degrees_of_freedom: float) -> float:
    if value == 0:
        return 0.5
    upper = min(abs(value), 20.0)
    steps = 240
    if steps % 2:
        steps += 1
    h = upper / steps
    total = _student_t_pdf(0.0, degrees_of_freedom) + _student_t_pdf(
        upper,
        degrees_of_freedom,
    )
    for index in range(1, steps):
        weight = 4 if index % 2 else 2
        total += weight * _student_t_pdf(index * h, degrees_of_freedom)
    integral = total * h / 3.0
    probability = 0.5 + integral if value > 0 else 0.5 - integral
    return min(max(probability, 0.0), 1.0)


def _student_t_pdf(value: float, degrees_of_freedom: float) -> float:
    coefficient = float(math.gamma((degrees_of_freedom + 1.0) / 2.0)) / (
        math.sqrt(degrees_of_freedom * math.pi)
        * float(math.gamma(degrees_of_freedom / 2.0))
    )
    return float(coefficient * (1.0 + value * value / degrees_of_freedom) ** (
        -(degrees_of_freedom + 1.0) / 2.0
    ))
