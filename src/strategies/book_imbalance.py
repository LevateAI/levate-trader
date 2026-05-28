"""Top-of-book imbalance scalp strategy."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import structlog

from src.models import BTC_PERP, ETH_PERP, Position, Side, Signal
from src.strategies.base import Strategy
from src.strategies.scalp_common import coerce_datetime, has_open_position

logger = structlog.get_logger(__name__)

IMBALANCE_THRESHOLD = 5.0
IMBALANCE_ANOMALY_RATIO = 20.0
MIN_SIDE_DEPTH_USD = 10_000.0
PERSISTENCE_TICKS = 3
WARMUP_SECONDS = 60


class BookImbalanceStrategy(Strategy):
    """Scalp when top-five L2 depth is persistently one-sided."""

    name = "book_imbalance"
    symbols = [BTC_PERP, ETH_PERP]

    def __init__(
        self,
        scalp_mode_enabled: bool = True,
        cooldown_seconds: int = 600,
    ) -> None:
        self._enabled = scalp_mode_enabled
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._last_signal_at: dict[str, datetime] = {}
        self._persistent_side: dict[str, Side | None] = {}
        self._persistent_count: dict[str, int] = {}
        self._first_book_received_at: dict[str, datetime] = {}
        self._warmup_logged: set[str] = set()

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate an L2 book update."""
        if not self._enabled:
            return None
        symbol = str(market_state.get("symbol", ""))
        if symbol not in self.symbols:
            return None
        if has_open_position(list(market_state.get("open_positions") or []), symbol):
            return None

        now = coerce_datetime(market_state.get("timestamp"))
        bid_levels = _book_side(market_state, "bid")
        ask_levels = _book_side(market_state, "ask")
        if not bid_levels or not ask_levels:
            return None
        if symbol not in self._first_book_received_at:
            self._first_book_received_at[symbol] = now
        if not self._warmup_complete(symbol, now):
            self._reset(symbol)
            return None
        if len(bid_levels) < 5 or len(ask_levels) < 5:
            self._reset(symbol)
            return None

        bid_depth = _depth(bid_levels)
        ask_depth = _depth(ask_levels)
        if bid_depth < MIN_SIDE_DEPTH_USD or ask_depth < MIN_SIDE_DEPTH_USD:
            self._reset(symbol)
            return None

        side: Side | None = None
        ratio = 0.0
        if bid_depth / ask_depth > IMBALANCE_THRESHOLD:
            side = Side.LONG
            ratio = bid_depth / ask_depth
        elif ask_depth / bid_depth > IMBALANCE_THRESHOLD:
            side = Side.SHORT
            ratio = ask_depth / bid_depth
        else:
            self._reset(symbol)
            return None

        if ratio > IMBALANCE_ANOMALY_RATIO:
            logger.warning(
                "book_imbalance_anomaly_rejected",
                symbol=symbol,
                bid_depth=bid_depth,
                ask_depth=ask_depth,
                ratio=ratio,
            )
            self._reset(symbol)
            return None

        self._record_persistence(symbol, side)
        if self._persistent_count.get(symbol, 0) < PERSISTENCE_TICKS:
            return None

        if self._in_cooldown(symbol, now):
            return None

        bid = float(market_state["bid"])
        ask = float(market_state["ask"])
        self._last_signal_at[symbol] = now
        logger.info(
            "book_imbalance_signal",
            symbol=symbol,
            side=side.value,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            ratio=ratio,
        )
        if side == Side.LONG:
            return Signal(
                side=Side.LONG,
                symbol=symbol,
                size_pct_equity=0.08,
                entry_price=ask,
                stop_loss=ask * 0.997,
                take_profit=ask * 1.006,
                reasoning=f"Top-five bid depth is {ratio:.2f}x ask depth on {symbol}.",
                strategy_name=self.name,
                signal_strength=min(ratio / 5, 1.0),
                confidence=0.52,
                features={"bid_depth": bid_depth, "ask_depth": ask_depth, "ratio": ratio},
            )
        return Signal(
            side=Side.SHORT,
            symbol=symbol,
            size_pct_equity=0.08,
            entry_price=bid,
            stop_loss=bid * 1.003,
            take_profit=bid * 0.994,
            reasoning=f"Top-five ask depth is {ratio:.2f}x bid depth on {symbol}.",
            strategy_name=self.name,
            signal_strength=min(ratio / 5, 1.0),
            confidence=0.52,
            features={"bid_depth": bid_depth, "ask_depth": ask_depth, "ratio": ratio},
        )

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Log fills for observability."""
        logger.info("book_imbalance_fill_received", oid=fill_event.get("oid"))

    async def should_exit(self, position: Position) -> bool:
        """Stops, targets, and max-hold exits are executor-owned."""
        return False

    def _record_persistence(self, symbol: str, side: Side) -> None:
        if self._persistent_side.get(symbol) == side:
            self._persistent_count[symbol] = self._persistent_count.get(symbol, 0) + 1
            return
        self._persistent_side[symbol] = side
        self._persistent_count[symbol] = 1

    def _reset(self, symbol: str) -> None:
        self._persistent_side[symbol] = None
        self._persistent_count[symbol] = 0

    def _warmup_complete(self, symbol: str, now: datetime) -> bool:
        first_book_at = self._first_book_received_at[symbol]
        elapsed_sec = (now - first_book_at).total_seconds()
        if elapsed_sec >= WARMUP_SECONDS:
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

    def _in_cooldown(self, symbol: str, now: datetime) -> bool:
        last_signal_at = self._last_signal_at.get(symbol)
        if last_signal_at is None:
            return False
        if now - last_signal_at < self._cooldown:
            logger.info("book_imbalance_cooldown", symbol=symbol)
            return True
        return False


def _book_side(market_state: dict[str, Any], side: str) -> list[dict[str, Any]]:
    direct_key = "bid_levels" if side == "bid" else "ask_levels"
    if market_state.get(direct_key):
        return list(market_state[direct_key])
    legacy_key = "book_bids" if side == "bid" else "book_asks"
    if market_state.get(legacy_key):
        return list(market_state[legacy_key])
    levels = market_state.get("book_levels") or {}
    if isinstance(levels, dict):
        key = "bids" if side == "bid" else "asks"
        return list(levels.get(key) or [])
    if isinstance(levels, list) and len(levels) >= 2:
        return list(levels[0] if side == "bid" else levels[1])
    return []


def _depth(levels: list[dict[str, Any]]) -> float:
    total = 0.0
    for level in levels[:5]:
        price = float(level.get("px") or level.get("price"))
        size = float(level.get("sz") or level.get("size"))
        total += price * size
    return total
