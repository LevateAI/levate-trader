"""Position sizing utilities."""

from __future__ import annotations

import math


def calculate_position_size(
    equity: float,
    signal_confidence: float,
    asset_realized_vol: float,
    leverage_cap: float = 10,
    edge: float | None = None,
    variance: float | None = None,
    stop_distance: float = 0.01,
    target_daily_vol: float = 0.01,
) -> float:
    """Calculate USD notional using quarter-Kelly, vol target, and risk caps.

    Percent inputs are fractions, so `0.02` means 2%.
    """
    if equity <= 0:
        return 0.0
    if leverage_cap <= 0:
        return 0.0

    normalized_confidence = min(max(signal_confidence, 0.0), 1.0)
    implied_edge = edge if edge is not None else max(normalized_confidence - 0.5, 0.0)
    implied_variance = variance if variance is not None else max(asset_realized_vol**2, 1e-8)

    if implied_edge <= 0 or implied_variance <= 0:
        kelly_size = 0.0
    else:
        kelly_fraction = 0.25 * (implied_edge / implied_variance)
        kelly_size = max(kelly_fraction, 0.0) * equity

    if asset_realized_vol <= 0 or math.isnan(asset_realized_vol):
        vol_target_size = leverage_cap * equity
    else:
        vol_target_size = (target_daily_vol * equity) / asset_realized_vol

    safe_stop_distance = max(stop_distance, 1e-6)
    risk_cap_size = (0.02 * equity) / safe_stop_distance
    leverage_cap_size = leverage_cap * equity

    return max(0.0, min(kelly_size, vol_target_size, risk_cap_size, leverage_cap_size))
