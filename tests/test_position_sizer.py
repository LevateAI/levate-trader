from __future__ import annotations

from src.risk.position_sizer import calculate_position_size


def test_quarter_kelly_math_is_capped_by_requested_limits() -> None:
    size = calculate_position_size(
        equity=1000,
        signal_confidence=0.6,
        asset_realized_vol=0.02,
        edge=0.01,
        variance=0.04,
        stop_distance=0.01,
        leverage_cap=10,
    )

    assert size == 62.5


def test_zero_vol_uses_other_caps_without_crashing() -> None:
    size = calculate_position_size(
        equity=1000,
        signal_confidence=0.6,
        asset_realized_vol=0,
        edge=0.02,
        variance=0.01,
        stop_distance=0.02,
        leverage_cap=10,
    )

    assert size == 500


def test_negative_edge_returns_zero() -> None:
    size = calculate_position_size(
        equity=1000,
        signal_confidence=0.4,
        asset_realized_vol=0.02,
        edge=-0.01,
        variance=0.04,
        stop_distance=0.01,
    )

    assert size == 0
