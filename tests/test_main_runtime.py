from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.config import Settings
from src.main import TraderRuntime, _parse_market_event
from src.models import BTC_PERP


@dataclass
class DummyExecutor:
    paper_equity_usd: float = 1000.0
    update_market_state_calls: int = 0

    async def get_open_positions(self) -> list[Any]:
        return []

    async def update_market_state(self, _: Any) -> None:
        self.update_market_state_calls += 1


class DummyExchange:
    def __init__(self) -> None:
        self.get_candles_calls = 0
        self.market_events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def get_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> list[dict[str, Any]]:
        self.get_candles_calls += 1
        return [{"c": 100 + index} for index in range(60)]


def _runtime() -> tuple[TraderRuntime, DummyExchange]:
    runtime = object.__new__(TraderRuntime)
    exchange = DummyExchange()
    runtime.settings = Settings(
        execution_mode="paper_sim",
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
    )
    runtime.exchange = exchange
    runtime.executor = DummyExecutor()
    runtime._bars_5m = {}
    runtime._bars_last_fetch = {}
    runtime._last_market_state = {}
    runtime._latest_market = runtime._last_market_state
    runtime._pending_trade_events = {}
    runtime._http_calls_this_minute = 0
    return runtime, exchange


@pytest.mark.asyncio
async def test_build_market_state_does_not_refetch_bars_per_ws_event() -> None:
    runtime, exchange = _runtime()
    fetch_calls = 0
    original_fetch = runtime._fetch_recent_bars

    async def counting_fetch(symbol: str, count: int) -> list[dict[str, Any]]:
        nonlocal fetch_calls
        fetch_calls += 1
        return await original_fetch(symbol, count)

    runtime._fetch_recent_bars = counting_fetch  # type: ignore[method-assign]
    event = {
        "channel": "book",
        "symbol": BTC_PERP,
        "payload": {
            "data": {
                "levels": [
                    [{"px": "99999"}],
                    [{"px": "100001"}],
                ]
            }
        },
    }

    for _ in range(1000):
        await runtime._build_market_state(BTC_PERP, event)

    assert fetch_calls <= 2
    assert exchange.get_candles_calls <= 2


@pytest.mark.asyncio
async def test_build_market_state_refreshes_stale_bars_cache() -> None:
    runtime, exchange = _runtime()
    runtime._bars_5m[BTC_PERP] = [{"c": 1}]
    runtime._bars_last_fetch[BTC_PERP] = datetime.now(tz=UTC) - timedelta(seconds=61)
    event = {
        "channel": "trades",
        "symbol": BTC_PERP,
        "payload": {"data": [{"px": "100000"}]},
    }

    await runtime._build_market_state(BTC_PERP, event)

    assert exchange.get_candles_calls == 1


def test_parse_market_event_preserves_full_l2_levels() -> None:
    event = {
        "channel": "book",
        "symbol": BTC_PERP,
        "payload": {
            "data": {
                "levels": [
                    [{"px": str(100 - index), "sz": "1"} for index in range(6)],
                    [{"px": str(101 + index), "sz": "2"} for index in range(6)],
                ]
            }
        },
    }

    parsed = _parse_market_event(event)

    assert parsed["bid"] == 100
    assert parsed["ask"] == 101
    assert len(parsed["bid_levels"]) == 6
    assert len(parsed["ask_levels"]) == 6


@pytest.mark.asyncio
async def test_market_loop_sms_failure_does_not_crash_loop() -> None:
    runtime, exchange = _runtime()
    send_error_calls = 0

    class ExplodingSms:
        def send_error(self, _: Exception) -> None:
            nonlocal send_error_calls
            send_error_calls += 1
            raise RuntimeError("sms queue corrupted")

    runtime.sms = ExplodingSms()
    await exchange.market_events.put(
        {"channel": "book", "symbol": BTC_PERP, "payload": {"data": {}}}
    )

    task = asyncio.create_task(runtime._market_loop([]))
    try:
        await asyncio.sleep(0.05)

        assert send_error_calls == 1
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_queue_decouple_consumer() -> None:
    runtime, exchange = _runtime()
    event = {
        "channel": "book",
        "symbol": BTC_PERP,
        "payload": {
            "data": {
                "levels": [
                    [{"px": "99999", "sz": "1"}],
                    [{"px": "100001", "sz": "1"}],
                ]
            }
        },
    }

    task = asyncio.create_task(runtime._market_loop([]))
    try:
        for _ in range(5000):
            await exchange.market_events.put(event)

        assert not exchange.market_events.full()
        await asyncio.wait_for(exchange.market_events.join(), timeout=1)

        assert exchange.get_candles_calls == 0
        assert runtime._http_calls_this_minute == 0
        assert runtime._latest_market[BTC_PERP].mid == pytest.approx(100000)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
