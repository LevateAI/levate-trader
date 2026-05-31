"""Persistent loss circuit breakers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from src.alerts.discord_notifier import DiscordNotifier
from src.alerts.twilio_notifier import TwilioNotifier
from src.config import Settings
from src.db.supabase_client import SupabaseRepository
from src.models import CircuitBreakerEvent, EquitySnapshot

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class BreakerState:
    """Runtime breaker pause state."""

    paused_until: datetime | None = None
    paused_indefinitely: bool = False
    reason: str | None = None

    @property
    def new_entries_paused(self) -> bool:
        """Return true when new entries are currently paused."""
        if self.paused_indefinitely:
            return True
        if self.paused_until is None:
            return False
        return datetime.now(tz=UTC) < self.paused_until


class CircuitBreakerManager:
    """Evaluate and persist risk circuit breaker state."""

    STATE_KEY = "circuit_breaker_state"

    def __init__(
        self,
        settings: Settings,
        repository: SupabaseRepository | None,
        discord: DiscordNotifier,
        sms: TwilioNotifier | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._discord = discord
        self._sms = sms
        self.state = BreakerState()

    async def load_state(self) -> None:
        """Load persisted breaker state."""
        if self._repository is None:
            return
        raw = await self._repository.get_state(self.STATE_KEY)
        if not raw:
            return
        paused_until_raw = raw.get("paused_until")
        self.state = BreakerState(
            paused_until=(
                datetime.fromisoformat(paused_until_raw)
                if isinstance(paused_until_raw, str) and paused_until_raw
                else None
            ),
            paused_indefinitely=bool(raw.get("paused_indefinitely", False)),
            reason=raw.get("reason"),
        )
        logger.info("circuit_breaker_state_loaded", state=raw)

    async def persist_state(self) -> None:
        """Persist breaker state."""
        if self._repository is None:
            return
        await self._repository.upsert_state(
            self.STATE_KEY,
            {
                "paused_until": (
                    self.state.paused_until.isoformat() if self.state.paused_until else None
                ),
                "paused_indefinitely": self.state.paused_indefinitely,
                "reason": self.state.reason,
            },
        )

    async def evaluate(self, snapshot: EquitySnapshot) -> CircuitBreakerEvent | None:
        """Evaluate hard loss rules and return a newly triggered event."""
        daily_loss = -snapshot.daily_pnl / snapshot.equity_usd if snapshot.equity_usd else 0.0
        weekly_loss = -snapshot.weekly_pnl / snapshot.equity_usd if snapshot.equity_usd else 0.0
        drawdown = snapshot.mdd_pct

        if drawdown >= self._settings.drawdown_fraction:
            return await self._trip(
                "max_drawdown",
                self._settings.drawdown_fraction,
                drawdown,
                "flat all positions and pause indefinitely until manual reset",
                paused_until=None,
                paused_indefinitely=True,
            )
        if weekly_loss >= self._settings.weekly_loss_fraction:
            return await self._trip(
                "weekly_loss",
                self._settings.weekly_loss_fraction,
                weekly_loss,
                "flat all positions and pause new entries for 24 hours",
                paused_until=datetime.now(tz=UTC) + timedelta(hours=24),
                paused_indefinitely=False,
            )
        if daily_loss >= self._settings.daily_loss_fraction:
            return await self._trip(
                "daily_loss",
                self._settings.daily_loss_fraction,
                daily_loss,
                "pause new entries for 4 hours",
                paused_until=datetime.now(tz=UTC) + timedelta(hours=4),
                paused_indefinitely=False,
            )
        return None

    def can_open_new_entries(self) -> bool:
        """Return whether the executor may open fresh positions."""
        return not self.state.new_entries_paused

    async def _trip(
        self,
        breaker_type: str,
        threshold_value: float,
        observed_value: float,
        action: str,
        paused_until: datetime | None,
        paused_indefinitely: bool,
    ) -> CircuitBreakerEvent | None:
        if self.state.new_entries_paused and self.state.reason == breaker_type:
            return None

        self.state = BreakerState(
            paused_until=paused_until,
            paused_indefinitely=paused_indefinitely,
            reason=breaker_type,
        )
        event = CircuitBreakerEvent(
            account_id=self._settings.account_id,
            breaker_type=breaker_type,
            threshold_value=threshold_value,
            observed_value=observed_value,
            action=action,
        )
        payload = _model_dump(event)
        logger.warning("circuit_breaker_tripped", **payload)
        if self._repository is not None:
            await self._repository.insert("circuit_breaker_events", payload)
            await self.persist_state()
        await self._discord.send(
            (
                f"BREAKER {breaker_type} TRIPPED | threshold {threshold_value:.2%} "
                f"hit at {observed_value:.2%} | action: {action}"
            ),
            breaker_type=breaker_type,
        )
        if self._sms is not None:
            self._sms.send_circuit_breaker(payload)
        return event


def _model_dump(model: CircuitBreakerEvent) -> dict[str, Any]:
    payload = model.model_dump(mode="json")
    payload["threshold_value"] = round(float(payload["threshold_value"]), 6)
    payload["observed_value"] = round(float(payload["observed_value"]), 6)
    return payload
