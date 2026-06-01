from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.models import BTC_PERP, Position, Side
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
    levels: int = 5,
) -> dict[str, Any]:
    bids = [{"px": "100", "sz": str(bid_size)} for _ in range(levels)]
    asks = [{"px": "101", "sz": str(ask_size)} for _ in range(levels)]
    return _market_state(timestamp=timestamp) | {
        "bid": 100,
        "ask": 101,
        "bid_levels": bids,
        "ask_levels": asks,
    }


def _book_position() -> Position:
    return Position(
        symbol=BTC_PERP,
        side=Side.LONG,
        size=1.0,
        entry_price=100.0,
        strategy_name="book_imbalance",
    )


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_on_extreme_low() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m(list(range(130, 100, -1)))))

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.entry_price == pytest.approx(100.1)
    assert signal.stop_loss == pytest.approx(100.1 * 0.997)


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_with_foreign_strategy_position() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(
        _market_state(
            bars_1m=_bars_1m(list(range(130, 100, -1))),
            open_positions=[_book_position()],
        )
    )

    assert signal is not None
    assert signal.strategy_name == "micro_rsi_scalp"
    assert signal.side == Side.LONG


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_from_trade_events_after_warmup() -> None:
    strategy = MicroRsiScalpStrategy(cooldown_seconds=0)
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    signal = None

    for index, price in enumerate(range(130, 99, -1)):
        timestamp = start + timedelta(minutes=index)
        signal = await strategy.on_tick(
            {
                "symbol": BTC_PERP,
                "timestamp": timestamp,
                "bid": price - 0.1,
                "ask": price + 0.1,
                "trade_events": [
                    {"px": str(price), "sz": "1", "time": int(timestamp.timestamp() * 1000)}
                ],
                "open_positions": [],
            }
        )

    assert signal is not None
    assert signal.side == Side.LONG


@pytest.mark.asyncio
async def test_micro_rsi_scalp_fires_on_extreme_high() -> None:
    strategy = MicroRsiScalpStrategy()

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m(list(range(100, 130)))))

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.entry_price == pytest.approx(99.9)
    assert signal.take_profit == pytest.approx(99.9 * 0.994)


@pytest.mark.asyncio
async def test_micro_rsi_scalp_respects_cooldown() -> None:
    strategy = MicroRsiScalpStrategy(cooldown_seconds=600)
    bars = _bars_1m(list(range(130, 100, -1)))
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

    signal = await strategy.on_tick(_market_state(bars_1m=_bars_1m([100, 101] * 15)))

    assert signal is None


@pytest.mark.asyncio
async def test_rsi_scalp_warmup_required() -> None:
    strategy = MicroRsiScalpStrategy()
    timestamp = datetime(2026, 5, 26, 12, 29, 59, tzinfo=UTC)

    signal = await strategy.on_tick(
        _market_state(bars_1m=_bars_1m(list(range(130, 100, -1))), timestamp=timestamp)
    )

    assert signal is None


@pytest.mark.asyncio
async def test_book_imbalance_fires_on_bid_heavy() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    assert await strategy.on_tick(_book_state(1000, 100, timestamp=start)) is None
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=61)))
        is None
    )
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=62)))
        is None
    )
    signal = await strategy.on_tick(
        _book_state(1000, 100, timestamp=start + timedelta(seconds=63))
    )

    assert signal is not None
    assert signal.side == Side.LONG
    assert signal.entry_price == pytest.approx(101)


@pytest.mark.asyncio
async def test_book_imbalance_fires_on_ask_heavy() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    await strategy.on_tick(_book_state(100, 1000, timestamp=start))
    await strategy.on_tick(_book_state(100, 1000, timestamp=start + timedelta(seconds=61)))
    await strategy.on_tick(_book_state(100, 1000, timestamp=start + timedelta(seconds=62)))
    signal = await strategy.on_tick(_book_state(100, 1000, timestamp=start + timedelta(seconds=63)))

    assert signal is not None
    assert signal.side == Side.SHORT
    assert signal.entry_price == pytest.approx(100)


@pytest.mark.asyncio
async def test_book_imbalance_requires_persistence() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    assert await strategy.on_tick(_book_state(1000, 100, timestamp=start)) is None
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=61)))
        is None
    )
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=62)))
        is None
    )


@pytest.mark.asyncio
async def test_book_imbalance_warmup_required() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    assert await strategy.on_tick(_book_state(1000, 100, timestamp=start)) is None
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=1)))
        is None
    )
    assert (
        await strategy.on_tick(_book_state(1000, 100, timestamp=start + timedelta(seconds=2)))
        is None
    )


@pytest.mark.asyncio
async def test_book_imbalance_rejects_thin_book() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    await strategy.on_tick(_book_state(1000, 100, timestamp=start, levels=4))
    await strategy.on_tick(
        _book_state(1000, 100, timestamp=start + timedelta(seconds=61), levels=4)
    )
    await strategy.on_tick(
        _book_state(1000, 100, timestamp=start + timedelta(seconds=62), levels=4)
    )
    signal = await strategy.on_tick(
        _book_state(1000, 100, timestamp=start + timedelta(seconds=63), levels=4)
    )

    assert signal is None


@pytest.mark.asyncio
async def test_book_imbalance_rejects_anomaly_ratio() -> None:
    strategy = BookImbalanceStrategy()
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)

    await strategy.on_tick(_book_state(500, 20, timestamp=start))
    await strategy.on_tick(_book_state(500, 20, timestamp=start + timedelta(seconds=61)))
    await strategy.on_tick(_book_state(500, 20, timestamp=start + timedelta(seconds=62)))
    signal = await strategy.on_tick(_book_state(500, 20, timestamp=start + timedelta(seconds=63)))

    assert signal is None


@pytest.mark.asyncio
async def test_book_imbalance_respects_cooldown() -> None:
    strategy = BookImbalanceStrategy(cooldown_seconds=600)
    timestamp = datetime(2026, 5, 26, 12, 30, tzinfo=UTC)

    assert await strategy.on_tick(_book_state(1000, 100, timestamp=timestamp)) is None
    for offset in range(61, 64):
        signal = await strategy.on_tick(
            _book_state(1000, 100, timestamp=timestamp + timedelta(seconds=offset))
        )
    assert signal is not None
    cooldown_signal = await strategy.on_tick(
        _book_state(1000, 100, timestamp=timestamp + timedelta(seconds=64))
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
async def test_volume_fade_fires_with_foreign_strategy_position() -> None:
    strategy = VolumeFadeStrategy()
    bars = _bars_1m([100] * 20 + [99.6], [10] * 20 + [40])

    signal = await strategy.on_tick(
        _market_state(
            bars_1m=bars,
            bid=99.5,
            ask=99.7,
            open_positions=[_book_position()],
        )
    )

    assert signal is not None
    assert signal.strategy_name == "volume_fade"
    assert signal.side == Side.LONG


@pytest.mark.asyncio
async def test_volume_fade_fires_from_trade_events_after_warmup() -> None:
    strategy = VolumeFadeStrategy(cooldown_seconds=0)
    start = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    signal = None

    for index in range(20):
        timestamp = start + timedelta(minutes=index)
        signal = await strategy.on_tick(
            {
                "symbol": BTC_PERP,
                "timestamp": timestamp,
                "bid": 99.9,
                "ask": 100.1,
                "trade_events": [
                    {"px": "100", "sz": "10", "time": int(timestamp.timestamp() * 1000)}
                ],
                "open_positions": [],
            }
        )
    timestamp = start + timedelta(minutes=20)
    signal = await strategy.on_tick(
        {
            "symbol": BTC_PERP,
            "timestamp": timestamp,
            "bid": 99.5,
            "ask": 99.7,
            "trade_events": [
                {"px": "100", "sz": "20", "time": int(timestamp.timestamp() * 1000)},
                {"px": "99.6", "sz": "20", "time": int(timestamp.timestamp() * 1000) + 30_000},
            ],
            "open_positions": [],
        }
    )

    assert signal is not None
    assert signal.side == Side.LONG


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


@pytest.mark.asyncio
async def test_volume_fade_warmup_required() -> None:
    strategy = VolumeFadeStrategy()
    bars = _bars_1m([100] * 20 + [99.6], [10] * 20 + [40])
    timestamp = datetime(2026, 5, 26, 12, 19, 59, tzinfo=UTC)

    signal = await strategy.on_tick(
        _market_state(bars_1m=bars, bid=99.5, ask=99.7, timestamp=timestamp)
    )

    assert signal is None
