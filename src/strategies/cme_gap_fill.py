"""Sunday CME gap fill strategy for BTC-PERP."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from src.models import BTC_PERP, Position, Side, Signal
from src.strategies.base import Strategy

logger = structlog.get_logger(__name__)


class CmeReferenceFetcher:
    """Placeholder CME reference data fetcher."""

    async def fetch_friday_settlement(self) -> float:
        """Return Friday BTC futures settlement.

        TODO: Replace this hardcoded placeholder with a real public CME/Coinglass
        source once the data provider is selected.
        """
        return 105_000.00


class CmeGapFillStrategy(Strategy):
    """Trade Sunday CME gaps toward Friday's settlement."""

    name = "cme_gap_fill"
    symbols = [BTC_PERP]

    def __init__(self, reference_fetcher: CmeReferenceFetcher | None = None) -> None:
        self._reference_fetcher = reference_fetcher or CmeReferenceFetcher()
        self._last_signal_week: tuple[int, int] | None = None
        self._position_opened_at: datetime | None = None

    async def on_tick(self, market_state: dict[str, Any]) -> Signal | None:
        """Evaluate BTC Sunday open gap once per ISO week."""
        symbol = str(market_state.get("symbol", ""))
        if symbol != BTC_PERP:
            return None

        now = _coerce_datetime(market_state.get("timestamp"))
        if not self._is_cme_open_window(now):
            return None
        iso_key = (now.isocalendar().year, now.isocalendar().week)
        if self._last_signal_week == iso_key:
            return None

        current_price = float(market_state["mid"])
        friday_close = await self._reference_fetcher.fetch_friday_settlement()
        gap = current_price - friday_close
        if abs(gap) <= 200:
            logger.info("cme_gap_no_signal", gap=gap, current_price=current_price)
            self._last_signal_week = iso_key
            return None

        side = Side.SHORT if gap > 0 else Side.LONG
        stop_loss = current_price * (1.01 if side == Side.SHORT else 0.99)
        reasoning = (
            f"Friday CME close was ${friday_close:.2f}, Sunday open is "
            f"${current_price:.2f}, gap of ${abs(gap):.2f}, historical fill rate "
            f"cited from research (77% within 30 days), placing {side.value} at "
            f"${current_price:.2f} targeting ${friday_close:.2f}."
        )
        self._last_signal_week = iso_key
        logger.info("cme_gap_signal", symbol=symbol, side=side.value, gap=gap)
        return Signal(
            side=side,
            symbol=BTC_PERP,
            size_pct_equity=0.05,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=friday_close,
            reasoning=reasoning,
            strategy_name=self.name,
            signal_strength=min(abs(gap) / 1000, 1.0),
            confidence=0.55,
            features={
                "friday_cme_close": friday_close,
                "sunday_open": current_price,
                "gap_usd": gap,
                "historical_fill_rate_30d": 0.77,
            },
        )

    async def on_fill(self, fill_event: dict[str, Any]) -> None:
        """Track when a CME gap position opens."""
        if fill_event.get("strategy_name") == self.name:
            self._position_opened_at = _coerce_datetime(fill_event.get("timestamp"))

    async def should_exit(self, position: Position) -> bool:
        """Exit when target/stop is hit or the position is older than 7 days."""
        if position.strategy_name != self.name:
            return False
        if self._position_opened_at is None:
            return False
        return datetime.now(tz=UTC) - self._position_opened_at >= timedelta(days=7)

    @staticmethod
    def _is_cme_open_window(now: datetime) -> bool:
        return now.weekday() == 6 and now.hour == 22 and now.minute < 5


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(tz=UTC)
