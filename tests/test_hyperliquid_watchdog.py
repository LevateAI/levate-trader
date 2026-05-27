from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from src.exchange.hyperliquid_client import HyperliquidClient


@pytest.mark.asyncio
async def test_watchdog_reconnects_once_and_keeps_running() -> None:
    client = object.__new__(HyperliquidClient)
    client._last_event_at = datetime.now(tz=UTC) - timedelta(seconds=35)
    reconnect_calls = 0
    reconnected = asyncio.Event()

    async def fake_reconnect() -> None:
        nonlocal reconnect_calls
        reconnect_calls += 1
        client._last_event_at = datetime.now(tz=UTC)
        reconnected.set()

    client.reconnect_with_backoff = fake_reconnect  # type: ignore[method-assign]

    task = asyncio.create_task(
        client.watchdog_loop(stale_threshold_sec=30, check_interval_sec=0.01)
    )
    try:
        await asyncio.wait_for(reconnected.wait(), timeout=1)
        await asyncio.sleep(0.03)

        assert reconnect_calls == 1
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
