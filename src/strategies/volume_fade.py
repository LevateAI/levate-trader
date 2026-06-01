"""One-minute volume spike fade strategy."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import structlog

from src.models import BTC_PERP, ETH_PERP, Position, Side, Signal
from src.strategies.base import Strategy
from src.strategies.scalp_common import (
    MinuteBarBuffer,
    coerce_datetime,
    has_open_position,
)

logger = structlog.get_logger(__name__)

VOLUME_MULTIPLIER = 3.0
MIN_MOVE_PCT = 0.003
WARMUP_SECONDS = 20 * 60
REQUIRED_BARS = 21


class VolumeFadeStrategy(Strategy):
    """Fade sharp one-minute moves when volume spikes above the rolling average."""

    name = "volume_fade"
    symbols = [BTC_PERP, ETH_PERP]

    def __init__(
        self,
        scalp_mode_enabled: bool = True,
        cooldown_seconds: int = 600,
    ) -> None:
        self._enabled = scalp_mode_enabled
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._bars = MinuteBarBuffer(max_bars=30)
        self._last_signal_at: dict[str, datetime] = {}
        self._first_tick_at: dict[str, datetime] = {}
        self._warmup_logged: set[str] = set()

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate a one-minute volume spike."""
        if not self._enabled:
            return None
        symbol = str(market_state.get("symbol", ""))
        if symbol not in self.symbols:
            return None
        if has_open_position(
            list(market_state.get("open_positions") or []),
            symbol,
            strategy_name=self.name,
        ):
            return None
        if not market_state.get("trade_events") and not market_state.get("bars_1m"):
            return None

        bars = self._bars.update_from_market_state(symbol, market_state)
        if not bars:
            return None
        now = coerce_datetime(market_state.get("timestamp"))
        self._first_tick_at.setdefault(symbol, bars[0].minute)
        if not self._warmup_complete(symbol, now, len(bars)):
            return None
        if self._in_cooldown(symbol, now):
            return None

        current = bars[-1]
        previous = bars[-21:-1]
        avg_volume = sum(bar.volume for bar in previous) / len(previous)
        if avg_volume <= 0 or current.volume <= avg_volume * VOLUME_MULTIPLIER:
            return None
        move_pct = (current.close - current.open) / current.open if current.open else 0.0
        if abs(move_pct) <= MIN_MOVE_PCT:
            return None

        bid = float(market_state["bid"])
        ask = float(market_state["ask"])
        if move_pct > 0:
            side = Side.SHORT
            entry_price = bid
            stop_loss = bid * 1.003
            take_profit = bid * 0.994
            direction = "pump"
        else:
            side = Side.LONG
            entry_price = ask
            stop_loss = ask * 0.997
            take_profit = ask * 1.006
            direction = "dump"

        self._last_signal_at[symbol] = now
        logger.info(
            "volume_fade_signal",
            symbol=symbol,
            side=side.value,
            current_volume=current.volume,
            avg_volume=avg_volume,
            move_pct=move_pct,
        )
        return Signal(
            side=side,
            symbol=symbol,
            size_pct_equity=0.08,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=(
                f"{symbol} printed a 1-minute volume {direction}: "
                f"{current.volume:.4f} vs {avg_volume:.4f} average and "
                f"{move_pct:.2%} price move."
            ),
            strategy_name=self.name,
            signal_strength=min(current.volume / max(avg_volume * VOLUME_MULTIPLIER, 1e-9), 1.0),
            confidence=0.52,
            features={
                "current_volume": current.volume,
                "avg_volume_20m": avg_volume,
                "move_pct": move_pct,
                "volume_multiplier": VOLUME_MULTIPLIER,
            },
        )

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Log fills for observability."""
        logger.info("volume_fade_fill_received", oid=fill_event.get("oid"))

    async def should_exit(self, position: Position) -> bool:
        """Stops, targets, and max-hold exits are executor-owned."""
        return False

    def _in_cooldown(self, symbol: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(symbol)
        if last_signal_at is None:
            return False
        if now - last_signal_at < self._cooldown:
            logger.info("volume_fade_cooldown", symbol=symbol)
            return True
        return False

    def _warmup_complete(self, symbol: str, now: datetime, bar_count: int) -> bool:
        first_tick_at = self._first_tick_at[symbol]
        elapsed_sec = (now - first_tick_at).total_seconds()
        if elapsed_sec >= WARMUP_SECONDS and bar_count >= REQUIRED_BARS:
            return True
        if symbol not in self._warmup_logged:
            logger.info(
                "strategy_warmup_pending",
                strategy_name=self.name,
                symbol=symbol,
                elapsed_sec=round(elapsed_sec, 2),
                required_sec=WARMUP_SECONDS,
            )
            self._warmup_logged.add(symbol)
        return False
