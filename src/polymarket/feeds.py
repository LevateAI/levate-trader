"""Read-only Polymarket and Coinbase feed clients."""

from __future__ import annotations

import asyncio
import html
import json
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import structlog

from src.polymarket.models import (
    CoinbaseSpotPrice,
    PolymarketBookLevel,
    PolymarketMarket,
    PolymarketOrderBook,
    PolymarketSide,
)

logger = structlog.get_logger(__name__)

COINBASE_PRODUCTS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}
DEFAULT_CRYPTO_TAKER_FEE_RATE = 0.07
EASTERN_TZ = ZoneInfo("America/New_York")
SHORT_HORIZON_SECONDS = {"5m": 300, "15m": 900}
MONTH_NUMBERS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
SUPPORTED_ASSET_ALIASES = {
    "BTC": ("btc", "bitcoin"),
    "ETH": ("eth", "ethereum"),
    "SOL": ("sol", "solana"),
    "XRP": ("xrp", "ripple"),
}
UPDOWN_SLUG_PREFIXES = {
    "BTC": "btc",
    "ETH": "eth",
    "SOL": "sol",
    "XRP": "xrp",
}
UPDOWN_SEARCH_NAMES = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana",
    "XRP": "XRP",
}
WINDOW_RE = re.compile(
    r"(?:(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?|tember)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)\s+(?P<day>\d{1,2}),?\s+)?"
    r"(?P<start>\d{1,2}:\d{2}\s*(?:am|pm)?)\s*-\s*"
    r"(?P<end>\d{1,2}:\d{2}\s*(?:am|pm)?)\s*et\b",
    re.IGNORECASE,
)


@dataclass(slots=True)
class _DiscoveryDiagnostics:
    """One raw Gamma market payload's filter diagnostics."""

    asset_matched: bool = False
    keyword_matched: bool = False
    title_matched: bool = False
    active: bool = False
    order_book_enabled: bool = False
    horizon_derived: bool = False
    kept: bool = False
    reject_reason: str | None = None
    asset_symbol: str | None = None
    horizon: str | None = None
    window_seconds: int | None = None
    window_open_source: str | None = None
    window_open_time: datetime | None = None
    resolution_time: datetime | None = None
    derived_window_seconds: int | None = None


class FeedUnavailableError(RuntimeError):
    """Raised when a public market data feed cannot return a valid message."""


class FeedWatchdog:
    """Independent stale-feed watchdog with reconnect callback support."""

    def __init__(
        self,
        feed_name: str,
        stale_threshold_sec: int = 20,
        check_interval_sec: int = 5,
    ) -> None:
        self.feed_name = feed_name
        self.stale_threshold_sec = stale_threshold_sec
        self.check_interval_sec = check_interval_sec
        self.last_message_at = time.monotonic()

    def mark_message_received(self) -> None:
        """Record that the feed delivered a usable message."""
        self.last_message_at = time.monotonic()

    async def watchdog_loop(self, reconnect: Callable[[], Awaitable[None]]) -> None:
        """Loop forever and reconnect when the feed goes stale."""
        while True:
            await asyncio.sleep(self.check_interval_sec)
            await self.check_once(reconnect)

    async def check_once(self, reconnect: Callable[[], Awaitable[None]]) -> bool:
        """Check staleness once and return whether reconnect was called."""
        silent_for_sec = time.monotonic() - self.last_message_at
        if silent_for_sec <= self.stale_threshold_sec:
            return False
        logger.warning(
            "ws_stale_detected",
            feed_name=self.feed_name,
            silent_for_sec=round(silent_for_sec, 2),
            stale_threshold_sec=self.stale_threshold_sec,
        )
        logger.warning("ws_reconnecting", feed_name=self.feed_name)
        await reconnect()
        self.mark_message_received()
        logger.info("ws_reconnected", feed_name=self.feed_name)
        return True


class PolymarketClobClient:
    """Public read-only Polymarket Gamma/CLOB REST client."""

    def __init__(
        self,
        gamma_url: str = "https://gamma-api.polymarket.com",
        clob_url: str = "https://clob.polymarket.com",
        polymarket_web_url: str = "https://polymarket.com",
        fee_rate_crypto: float = DEFAULT_CRYPTO_TAKER_FEE_RATE,
        stale_threshold_sec: int = 20,
        timeout_sec: float = 10.0,
    ) -> None:
        self._gamma_url = gamma_url.rstrip("/")
        self._clob_url = clob_url.rstrip("/")
        self._polymarket_web_url = polymarket_web_url.rstrip("/")
        self._fee_rate_crypto = fee_rate_crypto
        self._timeout_sec = timeout_sec
        self._client = httpx.AsyncClient(timeout=timeout_sec)
        self.watchdog = FeedWatchdog("polymarket_clob", stale_threshold_sec=stale_threshold_sec)
        self._price_to_beat_cache: dict[str, float] = {}

    async def reconnect(self) -> None:
        """Recreate the HTTP client used by the polling feed."""
        await self.close()
        self._client = httpx.AsyncClient(timeout=self._timeout_sec)

    async def close(self) -> None:
        """Close network resources."""
        await self._client.aclose()

    async def fetch_crypto_markets(
        self,
        keywords: list[str],
        limit: int = 10,
    ) -> list[PolymarketMarket]:
        """Fetch active CLOB-enabled 5-minute BTC/ETH/SOL/XRP markets from Gamma."""
        try:
            payloads = await self._fetch_market_payloads(keywords, limit)
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning(
                "polymarket_markets_fetch_failed",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise FeedUnavailableError("Polymarket market discovery failed") from exc

        self.watchdog.mark_message_received()
        markets: list[PolymarketMarket] = []
        seen_market_ids: set[str] = set()
        rejection_counts: dict[str, int] = {}
        title_matched_count = 0
        horizon_derived_count = 0
        for item in payloads:
            diagnostics = _market_payload_diagnostics(item, keywords)
            if diagnostics.title_matched:
                title_matched_count += 1
            if diagnostics.horizon_derived:
                horizon_derived_count += 1
            market = _market_from_payload(item, keywords, self._fee_rate_crypto)
            if market is not None and market.market_id not in seen_market_ids:
                seen_market_ids.add(market.market_id)
                markets.append(market)
            elif market is None:
                reject_reason = diagnostics.reject_reason or "unknown"
                rejection_counts[reject_reason] = rejection_counts.get(reject_reason, 0) + 1
                logger.debug(
                    "polymarket_market_filter_rejected",
                    reason=reject_reason,
                    market_id=item.get("id") or item.get("conditionId"),
                    slug=item.get("slug") or item.get("_event_slug"),
                    question=item.get("question") or item.get("_event_title"),
                    asset_symbol=diagnostics.asset_symbol,
                    title_matched=diagnostics.title_matched,
                    resolution_time=(
                        diagnostics.resolution_time.isoformat()
                        if diagnostics.resolution_time is not None
                        else None
                    ),
                    window_open_time=(
                        diagnostics.window_open_time.isoformat()
                        if diagnostics.window_open_time is not None
                        else None
                    ),
                    window_open_source=diagnostics.window_open_source,
                    derived_window_seconds=diagnostics.derived_window_seconds,
                )
        logger.info(
            "polymarket_market_discovery_filter_counts",
            fetched=len(payloads),
            title_matched=title_matched_count,
            horizon_derived=horizon_derived_count,
            kept=len(markets),
            rejection_counts=rejection_counts,
        )
        logger.info("polymarket_crypto_markets_loaded", count=len(markets))
        return markets

    async def _fetch_market_payloads(self, keywords: list[str], limit: int) -> list[dict[str, Any]]:
        """Fetch raw market payloads from events first, then legacy fallbacks."""
        current_payloads = await self._fetch_current_updown_payloads()
        search_payloads = await self._fetch_public_search_payloads(keywords, limit)
        payloads = [*current_payloads, *search_payloads]
        if not payloads:
            payloads = await self._fetch_global_event_payloads(limit)
        logger.info(
            "polymarket_market_fetch_source_counts",
            current_slug_count=len(current_payloads),
            public_search_count=len(search_payloads),
            fallback_event_count=max(
                len(payloads) - len(current_payloads) - len(search_payloads),
                0,
            ),
            total_count=len(payloads),
        )
        raw_log_limit = min(len(payloads), 50)
        for sample_index, item in enumerate(payloads[:raw_log_limit]):
            logger.info(
                "polymarket_raw_market_payload",
                sample_index=sample_index,
                sample_limit=raw_log_limit,
                payload=item,
            )
        return _dedupe_payloads(payloads)

    async def _fetch_current_updown_payloads(self) -> list[dict[str, Any]]:
        """Fetch deterministic current/next short-window markets by slug."""
        slugs = _candidate_updown_slugs(datetime.now(tz=UTC))
        tasks = [self._fetch_markets_by_slug(slug) for slug in slugs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        payloads: list[dict[str, Any]] = []
        for slug, result in zip(slugs, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "polymarket_current_slug_fetch_failed",
                    slug=slug,
                    error_type=type(result).__name__,
                    error_message=str(result),
                )
                continue
            for payload in result:
                payload["_discovery_source"] = "current_slug"
                payloads.append(payload)
        return payloads

    async def _fetch_markets_by_slug(self, slug: str) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"{self._gamma_url}/markets",
            params={"slug": slug},
        )
        response.raise_for_status()
        data = response.json()
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    async def _fetch_public_search_payloads(
        self,
        keywords: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for query in _updown_search_queries(keywords):
            response = await self._client.get(
                f"{self._gamma_url}/public-search",
                params={"q": query, "limit": max(limit, 10)},
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                continue
            for event in data.get("events") or []:
                if not isinstance(event, dict):
                    continue
                for market in event.get("markets") or []:
                    if isinstance(market, dict):
                        payload = dict(market)
                        payload["_event_slug"] = str(event.get("slug") or "")
                        payload["_event_title"] = str(
                            event.get("title") or event.get("question") or ""
                        )
                        payload["_discovery_source"] = "public_search"
                        payload["_search_query"] = query
                        payloads.append(payload)
        return payloads

    async def _fetch_global_event_payloads(self, limit: int) -> list[dict[str, Any]]:
        """Fetch raw market payloads from global events as a last-resort fallback."""
        payloads: list[dict[str, Any]] = []
        response = await self._client.get(
            f"{self._gamma_url}/events",
            params={
                "active": "true",
                "closed": "false",
                "archived": "false",
                "order": "volume_24hr",
                "ascending": "false",
                "limit": max(limit * 25, 100),
            },
        )
        response.raise_for_status()
        data = response.json()
        events = (
            data
            if isinstance(data, list)
            else data.get("events")
            if isinstance(data, dict)
            else []
        )
        for event in events or []:
            if not isinstance(event, dict):
                continue
            event_slug = str(event.get("slug") or "")
            event_title = str(event.get("title") or event.get("question") or "")
            for market in event.get("markets") or []:
                if isinstance(market, dict):
                    payload = dict(market)
                    payload["_event_slug"] = event_slug
                    payload["_event_title"] = event_title
                    payload["_discovery_source"] = "global_events"
                    payloads.append(payload)
        return payloads

    async def fetch_order_book(
        self,
        token_id: str,
        market_id: str,
        side: PolymarketSide,
    ) -> PolymarketOrderBook:
        """Fetch the public CLOB order book for one outcome token."""
        try:
            response = await self._client.get(
                f"{self._clob_url}/book",
                params={"token_id": token_id},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning(
                "polymarket_order_book_fetch_failed",
                token_id=token_id,
                market_id=market_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise FeedUnavailableError("Polymarket order book fetch failed") from exc

        self.watchdog.mark_message_received()
        return PolymarketOrderBook(
            token_id=token_id,
            market_id=market_id,
            side=side,
            timestamp=_parse_timestamp(payload.get("timestamp")),
            bids=_levels(payload.get("bids"), descending=True),
            asks=_levels(payload.get("asks"), descending=False),
        )

    async def fetch_best_price(self, token_id: str, side: str) -> float | None:
        """Fetch live CLOB best price for a token side; BUY is ask, SELL is bid."""
        normalized_side = side.upper()
        try:
            response = await self._client.get(
                f"{self._clob_url}/price",
                params={"token_id": token_id, "side": normalized_side},
            )
            response.raise_for_status()
            payload = response.json()
            price = _price_from_payload(payload)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            logger.warning(
                "polymarket_best_price_fetch_failed",
                token_id=token_id,
                side=normalized_side,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return None
        self.watchdog.mark_message_received()
        return price

    async def fetch_price_to_beat(self, market: PolymarketMarket) -> float | None:
        """Fetch the official per-window price-to-beat, falling back to parsed strike."""
        if not market.slug:
            return market.reference_price
        cached_price_to_beat = self._price_to_beat_cache.get(market.slug)
        if cached_price_to_beat is not None:
            logger.info(
                "polymarket_price_to_beat_cache_hit",
                market_id=market.market_id,
                slug=market.slug,
                price_to_beat=cached_price_to_beat,
            )
            return cached_price_to_beat
        price_to_beat: float | None = None
        try:
            response = await self._client.get(
                f"{self._polymarket_web_url}/api/equity/price-to-beat/{market.slug}"
            )
            response.raise_for_status()
            payload = response.json()
            price_to_beat = _price_to_beat_from_payload(payload)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            logger.warning(
                "polymarket_price_to_beat_fetch_failed",
                market_id=market.market_id,
                slug=market.slug,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        else:
            if price_to_beat is not None:
                self.watchdog.mark_message_received()
                self._price_to_beat_cache[market.slug] = price_to_beat
                logger.info(
                    "polymarket_price_to_beat_fetched",
                    market_id=market.market_id,
                    slug=market.slug,
                    price_to_beat=price_to_beat,
                    source="price_to_beat_api",
                )
                return price_to_beat

        page_price_to_beat = await self._fetch_price_to_beat_from_event_page(market)
        if page_price_to_beat is not None:
            self._price_to_beat_cache[market.slug] = page_price_to_beat
            logger.info(
                "polymarket_price_to_beat_fetched",
                market_id=market.market_id,
                slug=market.slug,
                price_to_beat=page_price_to_beat,
                source="event_page_crypto_prices",
            )
            return page_price_to_beat
        if price_to_beat is None:
            return market.reference_price
        return price_to_beat

    async def _fetch_price_to_beat_from_event_page(
        self,
        market: PolymarketMarket,
    ) -> float | None:
        if not market.slug:
            return None
        try:
            response = await self._client.get(f"{self._polymarket_web_url}/event/{market.slug}")
            response.raise_for_status()
            price_to_beat = _price_to_beat_from_event_page_html(response.text, market)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            logger.warning(
                "polymarket_price_to_beat_page_fetch_failed",
                market_id=market.market_id,
                slug=market.slug,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return None
        if price_to_beat is None:
            logger.warning(
                "polymarket_price_to_beat_page_missing",
                market_id=market.market_id,
                slug=market.slug,
                window_open_time=market.window_open_time.isoformat(),
                resolution_time=market.resolution_time.isoformat()
                if market.resolution_time is not None
                else None,
            )
            return None
        self.watchdog.mark_message_received()
        return price_to_beat

    async def fetch_books_for_market(
        self,
        market: PolymarketMarket,
    ) -> tuple[PolymarketOrderBook, PolymarketOrderBook]:
        """Fetch YES and NO books concurrently for a binary market."""
        yes_book, no_book = await asyncio.gather(
            self.fetch_order_book(market.yes_token_id, market.market_id, PolymarketSide.YES),
            self.fetch_order_book(market.no_token_id, market.market_id, PolymarketSide.NO),
        )
        if _book_pair_needs_price_fallback(yes_book, no_book):
            logger.warning(
                "polymarket_book_pair_sanity_failed",
                market_id=market.market_id,
                yes_bid=yes_book.best_bid,
                yes_ask=yes_book.best_ask,
                no_bid=no_book.best_bid,
                no_ask=no_book.best_ask,
            )
            yes_buy, yes_sell, no_buy, no_sell = await asyncio.gather(
                self.fetch_best_price(market.yes_token_id, "BUY"),
                self.fetch_best_price(market.yes_token_id, "SELL"),
                self.fetch_best_price(market.no_token_id, "BUY"),
                self.fetch_best_price(market.no_token_id, "SELL"),
            )
            yes_book = _book_with_live_top_prices(yes_book, buy_price=yes_buy, sell_price=yes_sell)
            no_book = _book_with_live_top_prices(no_book, buy_price=no_buy, sell_price=no_sell)
        return yes_book, no_book


class CoinbaseSpotClient:
    """Public Coinbase Exchange REST spot-price client."""

    def __init__(
        self,
        base_url: str = "https://api.exchange.coinbase.com",
        stale_threshold_sec: int = 20,
        timeout_sec: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._client = httpx.AsyncClient(timeout=timeout_sec)
        self.watchdog = FeedWatchdog("coinbase_spot", stale_threshold_sec=stale_threshold_sec)

    async def reconnect(self) -> None:
        """Recreate the HTTP client used by the polling feed."""
        await self.close()
        self._client = httpx.AsyncClient(timeout=self._timeout_sec)

    async def close(self) -> None:
        """Close network resources."""
        await self._client.aclose()

    async def fetch_spot(self, asset_symbol: str) -> CoinbaseSpotPrice:
        """Fetch the latest public Coinbase ticker price for a supported crypto asset."""
        normalized = asset_symbol.upper()
        product_id = COINBASE_PRODUCTS.get(normalized)
        if product_id is None:
            raise ValueError(f"unsupported Coinbase asset symbol: {asset_symbol}")
        try:
            response = await self._client.get(f"{self._base_url}/products/{product_id}/ticker")
            response.raise_for_status()
            payload = response.json()
            price = float(payload["price"])
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "coinbase_spot_fetch_failed",
                product_id=product_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise FeedUnavailableError("Coinbase spot fetch failed") from exc

        self.watchdog.mark_message_received()
        return CoinbaseSpotPrice(
            product_id=product_id,
            asset_symbol=normalized,
            price=price,
            timestamp=_parse_timestamp(payload.get("time")),
        )


def _candidate_updown_slugs(now: datetime) -> list[str]:
    """Return deterministic current and near-future recurring Up/Down market slugs."""
    timestamp = int(now.timestamp())
    slugs: list[str] = []
    for horizon, window_seconds in SHORT_HORIZON_SECONDS.items():
        current_window_start = timestamp // window_seconds * window_seconds
        for offset in range(3):
            window_start = current_window_start + offset * window_seconds
            for prefix in UPDOWN_SLUG_PREFIXES.values():
                slugs.append(f"{prefix}-updown-{horizon}-{window_start}")
    return slugs


def _updown_search_queries(keywords: list[str]) -> list[str]:
    """Return targeted public-search queries for enabled crypto Up/Down assets."""
    normalized_keywords = {keyword.lower() for keyword in keywords}
    queries: list[str] = []
    for symbol, aliases in SUPPORTED_ASSET_ALIASES.items():
        if normalized_keywords and not any(alias in normalized_keywords for alias in aliases):
            continue
        name = UPDOWN_SEARCH_NAMES[symbol]
        prefix = UPDOWN_SLUG_PREFIXES[symbol]
        queries.extend(
            [
                f"{name} Up or Down",
                f"{prefix}-updown-5m",
                f"{prefix}-updown-15m",
            ]
        )
    return queries


def _dedupe_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedupe Gamma payloads while preserving source order."""
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for payload in payloads:
        key = str(payload.get("conditionId") or payload.get("id") or payload.get("slug") or "")
        if not key:
            key = json.dumps(payload, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def _payload_searchable_text(item: dict[str, Any]) -> str:
    """Return normalized text used by discovery filters."""
    return " ".join(
        str(item.get(key) or "")
        for key in (
            "question",
            "slug",
            "category",
            "description",
            "groupItemTitle",
            "_event_slug",
            "_event_title",
        )
    ).lower()


def _market_payload_diagnostics(
    item: dict[str, Any],
    keywords: list[str],
) -> _DiscoveryDiagnostics:
    """Explain which discovery stage a raw Gamma market payload reached."""
    diagnostics = _DiscoveryDiagnostics()
    searchable = _payload_searchable_text(item)
    diagnostics.asset_symbol = _asset_symbol_from_text(searchable)
    diagnostics.asset_matched = diagnostics.asset_symbol is not None
    if not diagnostics.asset_matched:
        diagnostics.reject_reason = "asset_not_supported"
        return diagnostics

    normalized_keywords = [keyword.lower() for keyword in keywords]
    diagnostics.keyword_matched = not normalized_keywords or any(
        keyword in searchable for keyword in normalized_keywords
    )
    if not diagnostics.keyword_matched:
        diagnostics.reject_reason = "keyword_not_matched"
        return diagnostics

    diagnostics.title_matched = _looks_like_up_down_market(searchable)
    if not diagnostics.title_matched:
        diagnostics.reject_reason = "not_up_down_title"
        return diagnostics

    diagnostics.order_book_enabled = item.get("enableOrderBook") is not False
    if not diagnostics.order_book_enabled:
        diagnostics.reject_reason = "order_book_disabled"
        return diagnostics

    diagnostics.active = not (
        item.get("closed") or item.get("archived") or item.get("active") is False
    )
    if not diagnostics.active:
        diagnostics.reject_reason = "inactive_or_closed"
        return diagnostics

    diagnostics.resolution_time = _maybe_timestamp(
        item.get("endDate") or item.get("endDateIso") or item.get("umaEndDate")
    )
    if diagnostics.resolution_time is None:
        diagnostics.reject_reason = "missing_resolution_time"
        return diagnostics

    window_open_meta = _window_open_meta_from_payload(
        item,
        searchable,
        diagnostics.resolution_time,
    )
    if window_open_meta is None:
        diagnostics.reject_reason = "missing_window_open"
        return diagnostics
    diagnostics.window_open_time, diagnostics.window_open_source = window_open_meta
    diagnostics.derived_window_seconds = int(
        round((diagnostics.resolution_time - diagnostics.window_open_time).total_seconds())
    )
    horizon_meta = _derive_short_horizon(
        diagnostics.window_open_time,
        diagnostics.resolution_time,
    )
    if horizon_meta is None:
        diagnostics.reject_reason = "unsupported_window_length"
        return diagnostics
    diagnostics.horizon, diagnostics.window_seconds = horizon_meta
    diagnostics.horizon_derived = True

    token_ids = _json_list(item.get("clobTokenIds") or item.get("tokenIds"))
    if len(token_ids) < 2:
        diagnostics.reject_reason = "missing_token_ids"
        return diagnostics
    if item.get("id") is None and item.get("conditionId") is None:
        diagnostics.reject_reason = "missing_market_id"
        return diagnostics
    diagnostics.kept = True
    return diagnostics


def _market_from_payload(
    item: dict[str, Any],
    keywords: list[str],
    fee_rate_crypto: float,
) -> PolymarketMarket | None:
    question = str(item.get("question") or "")
    slug = str(item.get("slug") or item.get("_event_slug") or "")
    searchable = _payload_searchable_text(item)
    asset_symbol = _asset_symbol_from_text(searchable)
    if asset_symbol is None:
        return None
    normalized_keywords = [keyword.lower() for keyword in keywords]
    if normalized_keywords and not any(keyword in searchable for keyword in normalized_keywords):
        return None
    if not _looks_like_up_down_market(searchable):
        return None
    if item.get("enableOrderBook") is False:
        return None
    if item.get("closed") or item.get("archived") or item.get("active") is False:
        return None

    resolution_time = _maybe_timestamp(
        item.get("endDate") or item.get("endDateIso") or item.get("umaEndDate")
    )
    if resolution_time is None:
        return None
    window_open_meta = _window_open_meta_from_payload(item, searchable, resolution_time)
    if window_open_meta is None:
        return None
    window_open_time, window_open_source = window_open_meta
    horizon_meta = _derive_short_horizon(window_open_time, resolution_time)
    if horizon_meta is None:
        return None
    horizon, window_seconds = horizon_meta
    logger.info(
        "polymarket_market_window_derived",
        market_id=item.get("id") or item.get("conditionId"),
        slug=slug or item.get("_event_slug"),
        question=question or item.get("_event_title"),
        horizon=horizon,
        window_seconds=window_seconds,
        window_open_time=window_open_time.isoformat(),
        window_open_source=window_open_source,
        resolution_time=resolution_time.isoformat(),
        derived_window_seconds=int(
            round((resolution_time - window_open_time).total_seconds())
        ),
    )

    token_ids = _json_list(item.get("clobTokenIds") or item.get("tokenIds"))
    if len(token_ids) < 2:
        return None
    fees_enabled = bool(item.get("feesEnabled", True))
    market_id = item.get("id") or item.get("conditionId")
    condition_id = item.get("conditionId") or item.get("id")
    if market_id is None or condition_id is None:
        return None
    return PolymarketMarket(
        market_id=str(market_id),
        condition_id=str(condition_id),
        slug=slug or None,
        question=question,
        yes_token_id=str(token_ids[0]),
        no_token_id=str(token_ids[1]),
        asset_symbol=asset_symbol,
        horizon=horizon,
        window_seconds=window_seconds,
        window_open_time=window_open_time,
        resolution_time=resolution_time,
        reference_price=_extract_reference_price(question),
        fees_enabled=fees_enabled,
        taker_fee_rate=_fee_rate_from_payload(item, fee_rate_crypto) if fees_enabled else 0.0,
    )


def _asset_symbol_from_text(searchable: str) -> str | None:
    for symbol, aliases in SUPPORTED_ASSET_ALIASES.items():
        if any(re.search(rf"\b{re.escape(alias)}\b", searchable) for alias in aliases):
            return symbol
    return None


def _looks_like_up_down_market(searchable: str) -> bool:
    return (
        "up or down" in searchable
        or "updown" in searchable
        or re.search(r"\bup\s*-\s*down\b", searchable) is not None
    )


def _window_open_from_payload(
    item: dict[str, Any],
    searchable: str,
    resolution_time: datetime,
) -> datetime | None:
    meta = _window_open_meta_from_payload(item, searchable, resolution_time)
    return meta[0] if meta is not None else None


def _window_open_meta_from_payload(
    item: dict[str, Any],
    searchable: str,
    resolution_time: datetime,
) -> tuple[datetime, str] | None:
    text_open = _window_open_from_text(searchable, resolution_time)
    if text_open is not None:
        return text_open, "title_window"
    for key in ("startDate", "startDateIso", "startTime", "openTime", "windowStart"):
        parsed = _maybe_timestamp(item.get(key))
        if parsed is not None:
            return parsed, key
    return None


def _window_open_from_text(text: str, resolution_time: datetime) -> datetime | None:
    match = WINDOW_RE.search(text)
    if match is None:
        return None
    local_resolution = resolution_time.astimezone(EASTERN_TZ)
    month = match.group("month")
    day = match.group("day")
    if month is not None and day is not None:
        month_number = MONTH_NUMBERS.get(month.lower())
        if month_number is None:
            return None
        local_date = local_resolution.replace(
            month=month_number,
            day=int(day),
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    else:
        local_date = local_resolution.replace(hour=0, minute=0, second=0, microsecond=0)
    end_meridiem = _meridiem(match.group("end"))
    start_meridiem = _meridiem(match.group("start")) or end_meridiem
    local_open = _parse_local_time(local_date, match.group("start"), start_meridiem)
    return local_open.astimezone(UTC) if local_open is not None else None


def _parse_local_time(
    local_date: datetime,
    raw_time: str,
    fallback_meridiem: str | None,
) -> datetime | None:
    normalized = raw_time.lower().replace(" ", "")
    if _meridiem(normalized) is None and fallback_meridiem is not None:
        normalized = f"{normalized}{fallback_meridiem}"
    try:
        parsed_time = datetime.strptime(normalized, "%I:%M%p").time()
    except ValueError:
        return None
    return local_date.replace(hour=parsed_time.hour, minute=parsed_time.minute)


def _meridiem(raw_time: str) -> str | None:
    normalized = raw_time.lower().replace(" ", "")
    if normalized.endswith("am"):
        return "am"
    if normalized.endswith("pm"):
        return "pm"
    return None


def _derive_short_horizon(
    window_open_time: datetime,
    resolution_time: datetime,
) -> tuple[str, int] | None:
    window_seconds = int(round((resolution_time - window_open_time).total_seconds()))
    for horizon, canonical_seconds in SHORT_HORIZON_SECONDS.items():
        if abs(window_seconds - canonical_seconds) <= 30:
            return horizon, canonical_seconds
    return None


def _levels(payload: Any, *, descending: bool) -> list[PolymarketBookLevel]:
    levels: list[PolymarketBookLevel] = []
    for item in payload or []:
        try:
            price = float(item["price"])
            size = float(item["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if 0 < price < 1 and size > 0:
            levels.append(PolymarketBookLevel(price=price, size=size))
    return sorted(levels, key=lambda level: level.price, reverse=descending)


def _price_from_payload(payload: Any) -> float | None:
    if isinstance(payload, int | float):
        price = float(payload)
        return price if 0 < price < 1 else None
    if isinstance(payload, str):
        price = float(payload)
        return price if 0 < price < 1 else None
    if not isinstance(payload, dict):
        return None
    for key in ("price", "bestPrice", "value"):
        raw_value = payload.get(key)
        if raw_value is None:
            continue
        price = float(raw_value)
        return price if 0 < price < 1 else None
    return None


def _price_to_beat_from_payload(payload: Any) -> float | None:
    if isinstance(payload, int | float):
        return float(payload)
    if isinstance(payload, str):
        return float(payload.replace(",", ""))
    if not isinstance(payload, dict):
        return None
    for key in ("priceToBeat", "price_to_beat", "price", "value", "strike"):
        raw_value = payload.get(key)
        if raw_value is None:
            continue
        if isinstance(raw_value, str):
            return float(raw_value.replace(",", ""))
        return float(raw_value)
    return None


def _price_to_beat_from_event_page_html(
    page_html: str,
    market: PolymarketMarket,
) -> float | None:
    """Extract the window open price from Polymarket's hydrated event page data."""
    match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        page_html,
        re.DOTALL,
    )
    if match is None:
        return None
    next_data = json.loads(html.unescape(match.group(1)))
    return _price_to_beat_from_next_data(next_data, market)


def _price_to_beat_from_next_data(
    next_data: Any,
    market: PolymarketMarket,
) -> float | None:
    """Extract price-to-beat from Next query cache data for a specific market."""
    exact_event_metadata = _event_metadata_price_to_beat(next_data, market.slug)
    if exact_event_metadata is not None:
        return exact_event_metadata

    open_time = market.window_open_time
    resolution_time = market.resolution_time
    if resolution_time is None:
        return None
    for query in _iter_dicts(next_data):
        query_key = query.get("queryKey")
        state = query.get("state")
        if not isinstance(query_key, list) or not isinstance(state, dict):
            continue
        if len(query_key) < 6 or query_key[0:2] != ["crypto-prices", "price"]:
            continue
        symbol = str(query_key[2]).upper()
        query_open_time = _maybe_timestamp(query_key[3])
        query_close_time = _maybe_timestamp(query_key[5])
        if symbol != market.asset_symbol.upper():
            continue
        if query_open_time != open_time or query_close_time != resolution_time:
            continue
        data = state.get("data")
        if not isinstance(data, dict):
            continue
        raw_open_price = data.get("openPrice")
        if raw_open_price is None:
            continue
        return float(raw_open_price)
    return None


def _event_metadata_price_to_beat(next_data: Any, slug: str | None) -> float | None:
    if not slug:
        return None
    for item in _iter_dicts(next_data):
        item_slug = item.get("slug") or item.get("ticker")
        if item_slug != slug:
            continue
        event_metadata = item.get("eventMetadata")
        if not isinstance(event_metadata, dict):
            continue
        raw_price_to_beat = event_metadata.get("priceToBeat")
        if raw_price_to_beat is not None:
            return float(raw_price_to_beat)
    return None


def _iter_dicts(value: Any) -> list[dict[str, Any]]:
    dicts: list[dict[str, Any]] = []
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            dicts.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return dicts


def _book_pair_needs_price_fallback(
    yes_book: PolymarketOrderBook,
    no_book: PolymarketOrderBook,
) -> bool:
    yes_mid = _book_mid(yes_book)
    no_mid = _book_mid(no_book)
    if yes_mid is not None and no_mid is not None:
        mid_sum = yes_mid + no_mid
        if mid_sum < 0.97 or mid_sum > 1.03:
            return True
    if yes_book.best_ask is not None and no_book.best_ask is not None:
        if yes_book.best_ask >= 0.95 and no_book.best_ask >= 0.95:
            return True
    return False


def _book_mid(book: PolymarketOrderBook) -> float | None:
    if book.best_bid is None or book.best_ask is None:
        return None
    return (book.best_bid + book.best_ask) / 2


def _book_with_live_top_prices(
    book: PolymarketOrderBook,
    *,
    buy_price: float | None,
    sell_price: float | None,
) -> PolymarketOrderBook:
    return PolymarketOrderBook(
        token_id=book.token_id,
        market_id=book.market_id,
        side=book.side,
        timestamp=datetime.now(tz=UTC),
        bids=_replace_top_price(book.bids, sell_price, descending=True),
        asks=_replace_top_price(book.asks, buy_price, descending=False),
    )


def _replace_top_price(
    levels: list[PolymarketBookLevel],
    price: float | None,
    *,
    descending: bool,
) -> list[PolymarketBookLevel]:
    if price is None or not levels:
        return levels
    updated = [PolymarketBookLevel(price=price, size=levels[0].size), *levels[1:]]
    return sorted(updated, key=lambda level: level.price, reverse=descending)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _parse_timestamp(value: Any) -> datetime:
    parsed = _maybe_timestamp(value)
    return parsed or datetime.now(tz=UTC)


def _maybe_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, int | float):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)
    if isinstance(value, str):
        if value.isdigit():
            return _maybe_timestamp(float(value))
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _extract_reference_price(question: str) -> float | None:
    match = re.search(r"\$?\s*([0-9]{2,3}(?:,[0-9]{3})+(?:\.\d+)?)", question)
    if match is None:
        match = re.search(r"\$?\s*([0-9]{4,}(?:\.\d+)?)", question)
    if match is None:
        return None
    return float(match.group(1).replace(",", ""))


def _fee_rate_from_payload(item: dict[str, Any], default_crypto_rate: float) -> float:
    fee_schedule = item.get("feeSchedule")
    if isinstance(fee_schedule, dict):
        for key in ("takerFeeRate", "taker_fee_rate", "feeRate", "rate", "r"):
            if key in fee_schedule and fee_schedule[key] is not None:
                try:
                    return float(fee_schedule[key])
                except (TypeError, ValueError):
                    break
    raw_fee = item.get("fee")
    if isinstance(raw_fee, (int, float)):
        return float(raw_fee)
    if isinstance(raw_fee, str) and raw_fee:
        try:
            return float(raw_fee)
        except ValueError:
            pass
    return default_crypto_rate
