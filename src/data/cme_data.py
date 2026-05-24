"""CME Bitcoin futures reference data via yfinance."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import structlog
import yfinance as yf
from requests import RequestException

logger = structlog.get_logger(__name__)

EASTERN_TZ = ZoneInfo("America/New_York")
FRIDAY = 4
CME_FRIDAY_CLOSE_ET = time(hour=16)


class CmeDataUnavailableError(RuntimeError):
    """Raised when CME Bitcoin futures settlement data cannot be loaded."""


class CmeReferenceFetcher:
    """Fetch and cache the most recent Friday CME Bitcoin futures close."""

    def __init__(
        self,
        ticker: str = "BTC=F",
        cache_ttl: timedelta = timedelta(hours=6),
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._ticker = ticker
        self._cache_ttl = cache_ttl
        self._now_fn = now_fn or (lambda: datetime.now(tz=UTC))
        self._cache: dict[str, tuple[datetime, float]] = {}

    async def fetch_friday_settlement(self) -> float:
        """Return the latest available Friday daily close for BTC CME futures."""
        now_utc = self._utc_now()
        friday_date = self._most_recent_friday(now_utc)
        friday_date_iso = friday_date.isoformat()
        cached = self._cache.get(friday_date_iso)
        if cached is not None:
            fetched_at, close_price = cached
            if now_utc - fetched_at < self._cache_ttl:
                logger.info(
                    "cme_friday_close_cache_hit",
                    close_price=close_price,
                    friday_date_iso=friday_date_iso,
                )
                return close_price

        try:
            close_price = await asyncio.to_thread(self._fetch_close_sync, friday_date)
        except CmeDataUnavailableError as exc:
            logger.error(
                "cme_fetch_failed",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        except (
            IndexError,
            KeyError,
            OSError,
            RequestException,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            logger.error(
                "cme_fetch_failed",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise CmeDataUnavailableError(
                f"failed to fetch CME Bitcoin futures close for {friday_date_iso}"
            ) from exc

        self._cache[friday_date_iso] = (now_utc, close_price)
        logger.info(
            "cme_friday_close_fetched",
            close_price=close_price,
            friday_date_iso=friday_date_iso,
            source="yfinance",
        )
        return close_price

    def _fetch_close_sync(self, friday_date: date) -> float:
        end_date = friday_date + timedelta(days=1)
        data = yf.download(
            self._ticker,
            start=friday_date.isoformat(),
            end=end_date.isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            multi_level_index=False,
        )
        return self._extract_close(data, friday_date)

    def _extract_close(self, data: pd.DataFrame | None, friday_date: date) -> float:
        friday_date_iso = friday_date.isoformat()
        if data is None or data.empty:
            raise CmeDataUnavailableError(
                f"no CME Bitcoin futures data returned for {friday_date_iso}"
            )

        close_series: pd.Series | None = None
        if "Close" in data.columns:
            maybe_series = data["Close"]
            if isinstance(maybe_series, pd.DataFrame):
                close_series = maybe_series.iloc[:, 0]
            else:
                close_series = maybe_series
        elif isinstance(data.columns, pd.MultiIndex):
            close_columns = [column for column in data.columns if column[0] == "Close"]
            if close_columns:
                close_series = data[close_columns[0]]

        if close_series is None:
            raise CmeDataUnavailableError(
                f"CME Bitcoin futures close column missing for {friday_date_iso}"
            )

        close_series = close_series.dropna()
        if close_series.empty:
            raise CmeDataUnavailableError(
                f"CME Bitcoin futures close is empty for {friday_date_iso}"
            )
        return float(close_series.iloc[-1])

    def _most_recent_friday(self, now: datetime) -> date:
        now_et = now.astimezone(EASTERN_TZ)
        days_since_friday = (now_et.weekday() - FRIDAY) % 7
        target_friday = now_et.date() - timedelta(days=days_since_friday)
        if now_et.weekday() == FRIDAY and now_et.time() < CME_FRIDAY_CLOSE_ET:
            target_friday -= timedelta(days=7)
        return target_friday

    def _utc_now(self) -> datetime:
        now = self._now_fn()
        if now.tzinfo is None:
            return now.replace(tzinfo=UTC)
        return now.astimezone(UTC)
