from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from src.data.cme_data import CmeDataUnavailableError, CmeReferenceFetcher


@pytest.mark.asyncio
async def test_fetcher_returns_friday_close(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_download(*args: Any, **kwargs: Any) -> pd.DataFrame:
        calls.append({"args": args, "kwargs": kwargs})
        return pd.DataFrame(
            {"Close": [101_234.56]},
            index=pd.DatetimeIndex(["2026-05-22"]),
        )

    monkeypatch.setattr("src.data.cme_data.yf.download", fake_download)
    fetcher = CmeReferenceFetcher(now_fn=lambda: datetime(2026, 5, 24, 22, 0, tzinfo=UTC))

    close_price = await fetcher.fetch_friday_settlement()

    assert close_price == pytest.approx(101_234.56)
    assert calls[0]["args"] == ("BTC=F",)
    assert calls[0]["kwargs"]["start"] == "2026-05-22"
    assert calls[0]["kwargs"]["end"] == "2026-05-23"
    assert calls[0]["kwargs"]["interval"] == "1d"


@pytest.mark.asyncio
async def test_cache_prevents_refetch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    now = datetime(2026, 5, 24, 22, 0, tzinfo=UTC)

    def fake_download(*_: Any, **__: Any) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame({"Close": [100_000.0]})

    monkeypatch.setattr("src.data.cme_data.yf.download", fake_download)
    fetcher = CmeReferenceFetcher(now_fn=lambda: now)

    first = await fetcher.fetch_friday_settlement()
    second = await fetcher.fetch_friday_settlement()

    assert first == pytest.approx(100_000)
    assert second == pytest.approx(100_000)
    assert calls == 1


@pytest.mark.asyncio
async def test_cache_expires_after_6_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    current_now = datetime(2026, 5, 24, 22, 0, tzinfo=UTC)

    def fake_download(*_: Any, **__: Any) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame({"Close": [100_000.0 + calls]})

    def now_fn() -> datetime:
        return current_now

    monkeypatch.setattr("src.data.cme_data.yf.download", fake_download)
    fetcher = CmeReferenceFetcher(now_fn=now_fn)

    first = await fetcher.fetch_friday_settlement()
    current_now += timedelta(hours=6, seconds=1)
    second = await fetcher.fetch_friday_settlement()

    assert first == pytest.approx(100_001)
    assert second == pytest.approx(100_002)
    assert calls == 2


@pytest.mark.asyncio
async def test_raises_when_yfinance_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(*_: Any, **__: Any) -> pd.DataFrame:
        raise RuntimeError("yfinance exploded")

    monkeypatch.setattr("src.data.cme_data.yf.download", fake_download)
    fetcher = CmeReferenceFetcher(now_fn=lambda: datetime(2026, 5, 24, 22, 0, tzinfo=UTC))

    with pytest.raises(CmeDataUnavailableError):
        await fetcher.fetch_friday_settlement()
