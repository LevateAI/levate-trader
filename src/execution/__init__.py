"""Execution factory."""

from __future__ import annotations

from src.alerts.discord_notifier import DiscordNotifier
from src.alerts.twilio_notifier import TwilioNotifier
from src.config import Settings
from src.db.supabase_client import SupabaseRepository
from src.exchange.hyperliquid_client import HyperliquidClient
from src.execution.executor import Executor as RealExecutor
from src.execution.paper_executor import PaperExecutor
from src.risk.circuit_breakers import CircuitBreakerManager


def get_executor(
    settings: Settings,
    exchange: HyperliquidClient,
    repository: SupabaseRepository | None,
    circuit_breakers: CircuitBreakerManager,
    discord: DiscordNotifier,
    sms: TwilioNotifier | None = None,
) -> PaperExecutor | RealExecutor:
    """Return the execution engine for the configured execution mode."""
    if settings.execution_mode == "paper_sim":
        return PaperExecutor(
            exchange=exchange,
            repository=repository,
            circuit_breakers=circuit_breakers,
            discord=discord,
            sms=sms,
            settings=settings,
        )
    return RealExecutor(
        exchange=exchange,
        repository=repository,
        circuit_breakers=circuit_breakers,
        discord=discord,
        sms=sms,
        settings=settings,
    )


__all__ = ["PaperExecutor", "RealExecutor", "get_executor"]
