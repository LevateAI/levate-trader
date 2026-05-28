from __future__ import annotations

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

    async def get_open_positions(self) -> list[Any]:
        return []


class DummyExchange:
    def __init__(self) -> None:
        self.get_candles_calls = 0

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
