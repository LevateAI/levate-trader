"""Micro RSI scalp strategy built from one-minute trade buckets."""

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
    rsi,
)

logger = structlog.get_logger(__name__)


class MicroRsiScalpStrategy(Strategy):
    """Trade extreme RSI(3) moves on locally aggregated one-minute bars."""

    name = "micro_rsi_scalp"
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

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate an incoming trade update."""
        if not self._enabled:
            return None
        symbol = str(market_state.get("symbol", ""))
        if symbol not in self.symbols:
            return None
        if has_open_position(list(market_state.get("open_positions") or []), symbol):
            return None
        if not market_state.get("trade_events") and not market_state.get("bars_1m"):
            return None

        bars = self._bars.update_from_market_state(symbol, market_state)
        if len(bars) < 4:
            return None
        now = coerce_datetime(market_state.get("timestamp"))
        if self._in_cooldown(symbol, now):
            return None

        current_rsi = rsi([bar.close for bar in bars], period=3)
        ask = float(market_state["ask"])
        bid = float(market_state["bid"])
        if current_rsi < 15:
            self._last_signal_at[symbol] = now
            logger.info(
                "micro_rsi_scalp_signal",
                symbol=symbol,
                side=Side.LONG.value,
                rsi_3=current_rsi,
            )
            return Signal(
                side=Side.LONG,
                symbol=symbol,
                size_pct_equity=0.08,
                entry_price=ask,
                stop_loss=ask * 0.997,
                take_profit=ask * 1.006,
                reasoning=f"RSI(3) on 1-minute {symbol} bars is {current_rsi:.2f}, below 15.",
                strategy_name=self.name,
                signal_strength=min((15 - current_rsi) / 15, 1.0),
                confidence=0.52,
                features={"rsi_3": current_rsi, "timeframe": "1m"},
            )
        if current_rsi > 85:
            self._last_signal_at[symbol] = now
            logger.info(
                "micro_rsi_scalp_signal",
                symbol=symbol,
                side=Side.SHORT.value,
                rsi_3=current_rsi,
            )
            return Signal(
                side=Side.SHORT,
                symbol=symbol,
                size_pct_equity=0.08,
                entry_price=bid,
                stop_loss=bid * 1.003,
                take_profit=bid * 0.994,
                reasoning=f"RSI(3) on 1-minute {symbol} bars is {current_rsi:.2f}, above 85.",
                strategy_name=self.name,
                signal_strength=min((current_rsi - 85) / 15, 1.0),
                confidence=0.52,
                features={"rsi_3": current_rsi, "timeframe": "1m"},
            )
        return None

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Log fills for observability."""
        logger.info("micro_rsi_scalp_fill_received", oid=fill_event.get("oid"))

    async def should_exit(self, position: Position) -> bool:
        """Stops, targets, and max-hold exits are executor-owned."""
        return False

    def _in_cooldown(self, symbol: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(symbol)
        if last_signal_at is None:
            return False
        if now - last_signal_at < self._cooldown:
            logger.info("micro_rsi_scalp_cooldown", symbol=symbol)
            return True
        return False
