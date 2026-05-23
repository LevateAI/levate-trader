"""Strategy base contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.models import Position, Signal


class Strategy(ABC):
    """Abstract trading strategy interface."""

    name: str
    symbols: list[str]

    @abstractmethod
    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate a market update and optionally return a signal."""

    @abstractmethod
    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Handle a fill event."""

    @abstractmethod
    async def should_exit(self, position: Position) -> bool:
        """Return whether a tracked position should exit."""
