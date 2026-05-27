from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.models import BTC_PERP, Side
from src.strategies.book_imbalance import BookImbalanceStrategy
from src.strategies.micro_rsi_scalp import MicroRsiScalpStrategy
from src.strategies.volume_fade import VolumeFadeStrategy


def _bars_1m(
    closes: list[float],
    volumes: list[float] | None = None,
    start: datetime | None = None,
) -> list[dict[str, Any]]:
    start = start or datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    volumes = volumes or [1.0] * len(closes)
    bars: list[dict[str, Any]] = []
    previous_close = closes[0]
    for index, close in enumerate(closes):
        open_price = previous_close if index else close
        bars.append(
            {
                "timestamp": (start + timedelta(minutes=index)).isoformat(),
                "o": open_price,
                "h": max(open_price, close),
                "l": min(open_price, close),
                "c": close,
                "v": volumes[index],
            }
        )
        previous_close = close
    return bars


def _market_state(
    *,
    bars_1m: list[dict[str, Any]] | None = None,
    bid: float = 99.9,
    ask: float = 100.1,
    timestamp: datetime | None = None,
    open_positions: list[Any] | None = None,
) -> dict[str, Any]:
    return {
        "symbol": BTC_PERP,
        "timestamp": timestamp or datetime(2026, 5, 26, 12, 30, tzinfo=UTC),
        "bid": bid,
        "ask": ask,
        "mid": (bid + ask) / 2,
        "last_trade_price": (bid + ask) / 2,
        "bars_1m": bars_1m or [],
        "open_positions": open_positions or [],
    }


def _book_state(
    bid_size: float,
    ask_size: float,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    bids = [{"px": "100", "sz": str(bid_size)} for _ in range(5)]
    asks = [{"px": "101", "sz": str(ask_size)} for _ in range(5)]
    return _market_state(timestamp=timestamp) | {
        "bid": 100,
        "ask": 101,
        "book_bids": bids,
        "book_asks": asks,
    }


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_on_extreme_low() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m([100, 99, 98, 97])))

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.entry_price == pytest.approx(100.1)
    assert signal.stop_loss == pytest.approx(100.1 * 0.997)


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_on_extreme_high() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m([100, 101, 102, 103])))

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.entry_price == pytest.approx(99.9)
    assert signal.take_profit == pytest.approx(99.9 * 0.994)


@pytest.mark.asyncio
async def test_micro_rsi_scalp_respects_cooldown() -> None:
    strategy = MicroRsiScalpStrategy(cooldown_seconds=600)
    bars = _bars_1m([100, 99, 98, 97])
    timestamp = datetime(2026, 5, 26, 12, 30, tzinfo=UTC)

    first = await strategy.on_tick(_market_state(bars_1m=bars, timestamp=timestamp))
    second = await strategy.on_tick(
        _market_state(bars_1m=bars, timestamp=timestamp + timedelta(seconds=60))
    )

    assert first is not None
    assert second is None


@pytest.mark.asyncio
async def test_micro_rsi_scalp_no_signal_in_neutral_zone() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m([100, 101, 100, 101, 100])))

    assert signal is None


@pytest.mark.asyncio
async def test_book_imbalance_fires_on_bid_heavy() -> None:
    strategy = BookImbalanceStrategy()

    assert await strategy.on_tick(_book_state(10, 1)) is None
    assert await strategy.on_tick(_book_state(10, 1)) is None
    signal = await strategy.on_tick(_book_state(10, 1))

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.entry_price == pytest.approx(101)


@pytest.mark.asyncio
async def test_book_imbalance_fires_on_ask_heavy() -> None:
    strategy = BookImbalanceStrategy()

    await strategy.on_tick(_book_state(1, 10))
    await strategy.on_tick(_book_state(1, 10))
    signal = await strategy.on_tick(_book_state(1, 10))

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.entry_price == pytest.approx(100)


@pytest.mark.asyncio
async def test_book_imbalance_requires_persistence() -> None:
    strategy = BookImbalanceStrategy()

    assert await strategy.on_tick(_book_state(10, 1)) is None
    assert await strategy.on_tick(_book_state(10, 1)) is None


@pytest.mark.asyncio
async def test_book_imbalance_respects_cooldown() -> None:
    strategy = BookImbalanceStrategy(cooldown_seconds=600)
    timestamp = datetime(2026, 5, 26, 12, 30, tzinfo=UTC)

    for offset in range(3):
        signal = await strategy.on_tick(
            _book_state(10, 1, timestamp=timestamp + timedelta(seconds=offset))
        )
    assert signal is not None
    cooldown_signal = await strategy.on_tick(
        _book_state(10, 1, timestamp=timestamp + timedelta(seconds=60))
    )

    assert cooldown_signal is None


@pytest.mark.asyncio
async def test_volume_fade_fires_on_volume_dump() -> None:
    strategy = VolumeFadeStrategy()
    bars = _bars_1m([100] * 20 + [99.6], [10] * 20 + [40])

    signal = await strategy.on_tick(_market_state(bars_1m=bars, bid=99.5, ask=99.7))

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.entry_price == pytest.approx(99.7)


@pytest.mark.asyncio
async def test_volume_fade_fires_on_volume_pump() -> None:
    strategy = VolumeFadeStrategy()
    bars = _bars_1m([100] * 20 + [100.4], [10] * 20 + [40])

    signal = await strategy.on_tick(_market_state(bars_1m=bars, bid=100.3, ask=100.5))

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.entry_price == pytest.approx(100.3)


@pytest.mark.asyncio
async def test_volume_fade_no_fire_without_volume() -> None:
    strategy = VolumeFadeStrategy()
    bars = _bars_1m([100] * 20 + [99.6], [10] * 20 + [20])

    signal = await strategy.on_tick(_market_state(bars_1m=bars, bid=99.5, ask=99.7))

    assert signal is None


@pytest.mark.asyncio
async def test_volume_fade_respects_cooldown() -> None:
    strategy = VolumeFadeStrategy(cooldown_seconds=600)
    bars = _bars_1m([100] * 20 + [99.6], [10] * 20 + [40])
    timestamp = datetime(2026, 5, 26, 12, 30, tzinfo=UTC)

    first = await strategy.on_tick(_market_state(bars_1m=bars, timestamp=timestamp))
    second = await strategy.on_tick(
        _market_state(bars_1m=bars, timestamp=timestamp + timedelta(seconds=60))
    )

    assert first is not None
    assert second is None
