from __future__ import annotations

import asyncio
from collections import deque
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


def _queue_full_client() -> HyperliquidClient:
    client = object.__new__(HyperliquidClient)
    client.market_events = asyncio.Queue(maxsize=1)
    client.market_events.put_nowait({"channel": "book", "symbol": "BTC-PERP", "payload": {}})
    client._last_event_at = datetime.now(tz=UTC)
    client.dropped_event_counter = 0
    client._drop_timestamps = deque()
    client._force_reconnect_due_to_drops = False
    return client


def test_market_queue_drop_counter_increments() -> None:
    client = _queue_full_client()

    client._safe_queue_event({"channel": "book", "symbol": "BTC-PERP", "payload": {}})

    assert client.dropped_event_counter == 1
    assert len(client._drop_timestamps) == 1


@pytest.mark.asyncio
async def test_market_queue_high_drops_triggers_reconnect() -> None:
    client = _queue_full_client()
    reconnect_calls = 0
    reconnected = asyncio.Event()

    async def fake_reconnect() -> None:
        nonlocal reconnect_calls
        reconnect_calls += 1
        reconnected.set()

    client.reconnect_with_backoff = fake_reconnect  # type: ignore[method-assign]
    for _ in range(1001):
        client._safe_queue_event({"channel": "book", "symbol": "BTC-PERP", "payload": {}})

    assert client._force_reconnect_due_to_drops is True

    task = asyncio.create_task(
        client.watchdog_loop(stale_threshold_sec=999, check_interval_sec=0.01)
    )
    try:
        await asyncio.wait_for(reconnected.wait(), timeout=1)

        assert reconnect_calls == 1
        assert client._force_reconnect_due_to_drops is False
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
