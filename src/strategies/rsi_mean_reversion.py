"""RSI(5) mean-reversion strategy on five-minute BTC/ETH bars."""

from __future__ import annotations

from typing import Any

import pandas as pd
import numpy as np
import structlog

from src.models import BTC_PERP, ETH_PERP, Position, Side, Signal
from src.strategies.base import Strategy

logger = structlog.get_logger(__name__)


class RsiMeanReversionStrategy(Strategy):
    """Trade short-term RSI extremes back toward neutral."""

    name = "rsi_mean_reversion"
    symbols = [BTC_PERP, ETH_PERP]

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate a five-minute candle close."""
        symbol = str(market_state.get("symbol", ""))
        if symbol not in self.symbols:
            return None
        bars = list(market_state.get("bars_5m") or [])
        if len(bars) < 6:
            return None

        close = pd.Series([float(bar["c"]) for bar in bars], dtype="float64")
        rsi = _rsi(close, period=5)
        current_rsi = float(rsi.iloc[-1])
        open_positions = list(market_state.get("open_positions") or [])
        bid = float(market_state["bid"])
        ask = float(market_state["ask"])

        long_position = _has_position(open_positions, symbol, Side.LONG, strategy_name=self.name)
        short_position = _has_position(open_positions, symbol, Side.SHORT, strategy_name=self.name)
        if long_position and current_rsi > 50:
            return Signal(
                side=Side.SHORT,
                symbol=symbol,
                size_pct_equity=0.0,
                entry_price=bid,
                stop_loss=bid,
                take_profit=None,
                reasoning=f"RSI(5) crossed above 50 at {current_rsi:.2f}; closing long.",
                strategy_name=self.name,
                signal_strength=1.0,
                confidence=1.0,
                reduce_only=True,
                features={"rsi_5": current_rsi, "exit_rule": "long_rsi_cross_above_50"},
            )
        if short_position and current_rsi < 50:
            return Signal(
                side=Side.LONG,
                symbol=symbol,
                size_pct_equity=0.0,
                entry_price=ask,
                stop_loss=ask,
                take_profit=None,
                reasoning=f"RSI(5) crossed below 50 at {current_rsi:.2f}; closing short.",
                strategy_name=self.name,
                signal_strength=1.0,
                confidence=1.0,
                reduce_only=True,
                features={"rsi_5": current_rsi, "exit_rule": "short_rsi_cross_below_50"},
            )

        if current_rsi < 20 and not long_position:
            reasoning = (
                f"RSI(5) on {symbol} 5-minute bars is {current_rsi:.2f}, below 20. "
                f"Placing long limit at current bid ${bid:.2f}; stop is 0.8% below "
                "entry and exit target is RSI crossing above 50."
            )
            return Signal(
                side=Side.LONG,
                symbol=symbol,
                size_pct_equity=0.03,
                entry_price=bid,
                stop_loss=bid * 0.992,
                take_profit=None,
                reasoning=reasoning,
                strategy_name=self.name,
                signal_strength=min((20 - current_rsi) / 20, 1.0),
                confidence=0.5,
                features={"rsi_5": current_rsi, "timeframe": "5m"},
            )

        if current_rsi > 80 and not short_position:
            reasoning = (
                f"RSI(5) on {symbol} 5-minute bars is {current_rsi:.2f}, above 80. "
                f"Placing short limit at current ask ${ask:.2f}; stop is 0.8% above "
                "entry and exit target is RSI crossing below 50."
            )
            return Signal(
                side=Side.SHORT,
                symbol=symbol,
                size_pct_equity=0.03,
                entry_price=ask,
                stop_loss=ask * 1.008,
                take_profit=None,
                reasoning=reasoning,
                strategy_name=self.name,
                signal_strength=min((current_rsi - 80) / 20, 1.0),
                confidence=0.5,
                features={"rsi_5": current_rsi, "timeframe": "5m"},
            )

        return None

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """No-op hook for v1."""
        logger.info("rsi_fill_received", oid=fill_event.get("oid"))

    async def should_exit(self, position: Position) -> bool:
        """RSI exits need fresh market state, so executor handles stops/targets for v1."""
        return False


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss > 0), 0)
    return rsi.fillna(50)


def _has_position(
    positions: list[Any],
    symbol: str,
    side: Side,
    strategy_name: str | None = None,
) -> bool:
    for position in positions:
        if isinstance(position, Position):
            if (
                position.symbol == symbol
                and position.side == side
                and (strategy_name is None or position.strategy_name == strategy_name)
            ):
                return True
            continue
        if (
            position.get("symbol") == symbol
            and position.get("side") == side.value
            and (strategy_name is None or position.get("strategy_name") == strategy_name)
        ):
            return True
    return False
