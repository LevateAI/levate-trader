from __future__ import annotations

from dataclasses import dataclass

import pytest

from src.config import Settings
from src.models import EquitySnapshot
from src.risk.circuit_breakers import CircuitBreakerManager


@dataclass
class DummyDiscord:
    messages: list[str]

    async def send(self, content: str, **_: object) -> None:
        self.messages.append(content)


def _settings() -> Settings:
    return Settings(
        hyperliquid_private_key="0x" + "1" * 64,
        hyperliquid_account_address="0x" + "2" * 40,
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
        max_daily_loss_pct=5.0,
        max_weekly_loss_pct=10.0,
        max_drawdown_pct=20.0,
    )


@pytest.mark.asyncio
async def test_daily_breaker_triggers_at_threshold() -> None:
    discord = DummyDiscord([])
    manager = CircuitBreakerManager(_settings(), None, discord)  # type: ignore[arg-type]
    snapshot = EquitySnapshot(
        balance_usd=950,
        equity_usd=1000,
        margin_used_usd=0,
        open_position_count=0,
        daily_pnl=-50,
        weekly_pnl=-50,
        mdd_pct=0.05,
    )

    event = await manager.evaluate(snapshot)

    assert event is not None
    assert event.breaker_type == "daily_loss"
    assert manager.state.new_entries_paused


@pytest.mark.asyncio
async def test_weekly_breaker_triggers_at_threshold() -> None:
    discord = DummyDiscord([])
    manager = CircuitBreakerManager(_settings(), None, discord)  # type: ignore[arg-type]
    snapshot = EquitySnapshot(
        balance_usd=900,
        equity_usd=1000,
        margin_used_usd=0,
        open_position_count=0,
        daily_pnl=-20,
        weekly_pnl=-100,
        mdd_pct=0.1,
    )

    event = await manager.evaluate(snapshot)

    assert event is not None
    assert event.breaker_type == "weekly_loss"


@pytest.mark.asyncio
async def test_drawdown_breaker_triggers_at_threshold() -> None:
    discord = DummyDiscord([])
    manager = CircuitBreakerManager(_settings(), None, discord)  # type: ignore[arg-type]
    snapshot = EquitySnapshot(
        balance_usd=800,
        equity_usd=1000,
        margin_used_usd=0,
        open_position_count=0,
        daily_pnl=0,
        weekly_pnl=0,
        mdd_pct=0.2,
    )

    event = await manager.evaluate(snapshot)

    assert event is not None
    assert event.breaker_type == "max_drawdown"
    assert manager.state.paused_indefinitely
