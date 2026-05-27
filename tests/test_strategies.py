from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.models import BTC_PERP, ETH_PERP, Position, Side
from src.strategies.cme_gap_fill import CmeGapFillStrategy
from src.strategies.rsi_mean_reversion import RsiMeanReversionStrategy


class FixedCmeFetcher:
    async def fetch_friday_settlement(self) -> float:
        return 100_000


@pytest.mark.asyncio
async def test_cme_gap_strategy_fires_on_sunday_open_gap() -> None:
    strategy = CmeGapFillStrategy(reference_fetcher=FixedCmeFetcher())  # type: ignore[arg-type]

    signal = await strategy.on_tick(
        {
            "symbol": BTC_PERP,
            "timestamp": datetime(2026, 5, 24, 22, 0, tzinfo=UTC),
            "mid": 100_500,
        }
    )

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.take_profit == 100_000
    assert "77% within 30 days" in signal.reasoning


@pytest.mark.asyncio
async def test_rsi_strategy_fires_long_on_oversold() -> None:
    strategy = RsiMeanReversionStrategy()
    closes = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91]
    bars = [{"c": close} for close in closes]

    signal = await strategy.on_tick(
        {
            "symbol": ETH_PERP,
            "bars_5m": bars,
            "bid": 90.9,
            "ask": 91.1,
            "open_positions": [],
        }
    )

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.stop_loss == pytest.approx(90.9 * 0.992)


@pytest.mark.asyncio
async def test_rsi_strategy_fires_short_on_overbought() -> None:
    strategy = RsiMeanReversionStrategy()
    closes = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    bars = [{"c": close} for close in closes]

    signal = await strategy.on_tick(
        {
            "symbol": BTC_PERP,
            "bars_5m": bars,
            "bid": 108.9,
            "ask": 109.1,
            "open_positions": [],
        }
    )

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.stop_loss == pytest.approx(109.1 * 1.008)


@pytest.mark.asyncio
async def test_rsi_strategy_does_not_close_other_strategy_position() -> None:
    strategy = RsiMeanReversionStrategy()
    closes = [100, 99, 98, 99, 100, 101, 100, 99, 100, 101]
    bars = [{"c": close} for close in closes]
    cme_position = Position(
        symbol=BTC_PERP,
        side=Side.LONG,
        size=1,
        entry_price=100,
        strategy_name="cme_gap_fill",
    )

    signal = await strategy.on_tick(
        {
            "symbol": BTC_PERP,
            "bars_5m": bars,
            "bid": 100.9,
            "ask": 101.1,
            "open_positions": [cme_position],
        }
    )

    assert signal is None or not signal.reduce_only
