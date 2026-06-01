"""Shared helpers for high-frequency paper scalp strategies."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.models import Position

SCALP_STRATEGY_NAMES = frozenset(
    {"micro_rsi_scalp", "book_imbalance", "volume_fade"}
)


@dataclass(slots=True)
class MinuteBar:
    """Mutable one-minute OHLCV bar built from trade events."""

    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def update(self, price: float, size: float) -> None:
        """Fold a trade into this minute bar."""
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size

    def as_dict(self) -> dict[str, Any]:
        """Return a pandas/test-friendly bar payload."""
        return {
            "timestamp": self.minute.isoformat(),
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
        }


class MinuteBarBuffer:
    """Rolling per-symbol minute bar store."""

    def __init__(self, max_bars: int) -> None:
        self._max_bars = max_bars
        self._bars: dict[str, deque[MinuteBar]] = {}

    def update_from_market_state(
        self,
        symbol: str,
        market_state: dict[str, Any],
    ) -> list[MinuteBar]:
        """Update bars from market-state trade events and return current bars."""
        if market_state.get("bars_1m"):
            self._load_supplied_bars(symbol, list(market_state["bars_1m"]))
            return self.bars(symbol)

        fallback_timestamp = coerce_datetime(market_state.get("timestamp"))
        for trade in list(market_state.get("trade_events") or []):
            price = trade_price(trade)
            if price is None:
                continue
            size = trade_size(trade)
            timestamp = coerce_datetime(
                trade.get("timestamp") or trade.get("time"),
                default=fallback_timestamp,
            )
            self._append_trade(symbol, timestamp, price, size)
        return self.bars(symbol)

    def bars(self, symbol: str) -> list[MinuteBar]:
        """Return rolling bars for a symbol."""
        return list(self._bars.get(symbol, deque()))

    def _append_trade(self, symbol: str, timestamp: datetime, price: float, size: float) -> None:
        minute = timestamp.astimezone(UTC).replace(second=0, microsecond=0)
        bars = self._bars.setdefault(symbol, deque(maxlen=self._max_bars))
        if bars and bars[-1].minute == minute:
            bars[-1].update(price, size)
            return
        if bars and minute < bars[-1].minute:
            return
        bars.append(MinuteBar(minute, price, price, price, price, size))

    def _load_supplied_bars(self, symbol: str, bars_payload: list[dict[str, Any]]) -> None:
        bars = deque(maxlen=self._max_bars)
        for item in bars_payload[-self._max_bars :]:
            close = float(item.get("c") or item.get("close"))
            open_price = float(item.get("o") or item.get("open") or close)
            high = float(item.get("h") or item.get("high") or max(open_price, close))
            low = float(item.get("l") or item.get("low") or min(open_price, close))
            volume = float(item.get("v") or item.get("volume") or 1.0)
            bars.append(
                MinuteBar(
                    minute=coerce_datetime(item.get("timestamp") or item.get("t")),
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            )
        self._bars[symbol] = bars


def is_scalp_strategy_name(strategy_name: str) -> bool:
    """Return whether a strategy should use scalp-specific executor behavior."""
    return strategy_name in SCALP_STRATEGY_NAMES


def has_open_position(
    positions: list[Any],
    symbol: str,
    strategy_name: str | None = None,
) -> bool:
    """Return whether an open position exists for the symbol."""
    for position in positions:
        if isinstance(position, Position):
            if (
                position.symbol == symbol
                and (strategy_name is None or position.strategy_name == strategy_name)
            ):
                return True
            continue
        if (
            position.get("symbol") == symbol
            and (strategy_name is None or position.get("strategy_name") == strategy_name)
        ):
            return True
    return False


def rsi(closes: list[float], period: int) -> float:
    """Compute the latest RSI value for a close series."""
    close = pd.Series(closes, dtype="float64")
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    values = 100 - (100 / (1 + rs))
    values = values.mask((avg_loss == 0) & (avg_gain > 0), 100)
    values = values.mask((avg_gain == 0) & (avg_loss > 0), 0)
    return float(values.fillna(50).iloc[-1])


def trade_price(trade: dict[str, Any]) -> float | None:
    """Extract a trade price if present."""
    value = trade.get("px") or trade.get("price")
    return float(value) if value is not None else None


def trade_size(trade: dict[str, Any]) -> float:
    """Extract a trade size, defaulting to one unit for sparse test payloads."""
    value = trade.get("sz") or trade.get("size") or 1.0
    return max(float(value), 0.0)


def coerce_datetime(value: Any, default: datetime | None = None) -> datetime:
    """Convert common websocket/test timestamp shapes to UTC datetimes."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, int | float):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return default or datetime.now(tz=UTC)
