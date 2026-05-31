"""Chaos-mode signal wrapper for tournament experiments."""

from __future__ import annotations

from dataclasses import replace
import random
from typing import Any, Protocol

import structlog

from src.models import Position, Signal
from src.strategies.base import Strategy

logger = structlog.get_logger(__name__)


class RandomLike(Protocol):
    """Small random interface used by the chaos wrapper."""

    def random(self) -> float:
        """Return a random float in [0.0, 1.0)."""

    def uniform(self, a: float, b: float) -> float:
        """Return a random float in [a, b]."""


class ChaosStrategyWrapper(Strategy):
    """Randomly skip and resize signals from an underlying strategy."""

    def __init__(
        self,
        wrapped: Strategy,
        rng: RandomLike | None = None,
        skip_probability: float = 0.30,
        min_size_multiplier: float = 0.50,
        max_size_multiplier: float = 1.50,
    ) -> None:
        self._wrapped = wrapped
        self._rng = rng or random.Random()
        self._skip_probability = skip_probability
        self._min_size_multiplier = min_size_multiplier
        self._max_size_multiplier = max_size_multiplier
        self.name = wrapped.name
        self.symbols = wrapped.symbols

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate the wrapped strategy, then apply chaos-mode sampling."""
        signal = await self._wrapped.on_tick(market_state)
        if signal is None:
            return None
        if self._rng.random() < self._skip_probability:
            logger.info(
                "chaos_signal_skipped",
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
            )
            return None
        multiplier = self._rng.uniform(self._min_size_multiplier, self._max_size_multiplier)
        randomized = replace(
            signal,
            size_pct_equity=signal.size_pct_equity * multiplier,
            features=signal.features | {"chaos_size_multiplier": multiplier},
        )
        logger.info(
            "chaos_signal_resized",
            strategy_name=randomized.strategy_name,
            symbol=randomized.symbol,
            multiplier=multiplier,
            original_size_pct_equity=signal.size_pct_equity,
            randomized_size_pct_equity=randomized.size_pct_equity,
        )
        return randomized

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Forward fill events to the wrapped strategy."""
        await self._wrapped.on_fill(fill_event)

    async def should_exit(self, position: Position) -> bool:
        """Forward exit checks to the wrapped strategy."""
        return await self._wrapped.should_exit(position)
