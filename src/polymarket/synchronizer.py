"""Two-feed Polymarket/Coinbase snapshot synchronizer."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import structlog

from src.polymarket.feeds import CoinbaseSpotClient, PolymarketClobClient
from src.polymarket.models import (
    PolymarketMarket,
    PolymarketMarketContext,
    PolymarketMarketSnapshot,
    PolymarketOrderBook,
    compute_implied_gap,
)

logger = structlog.get_logger(__name__)


class PolymarketDataSynchronizer:
    """Join Polymarket CLOB books with Coinbase spot truth prices."""

    def __init__(
        self,
        clob_client: PolymarketClobClient,
        coinbase_client: CoinbaseSpotClient,
        keywords: list[str],
        max_markets: int = 10,
        market_refresh_sec: int = 300,
        horizon: str | None = None,
    ) -> None:
        self._clob = clob_client
        self._coinbase = coinbase_client
        self._keywords = keywords
        self._max_markets = max_markets
        self._horizon = horizon
        self._market_refresh = timedelta(seconds=market_refresh_sec)
        self._markets: list[PolymarketMarket] = []
        self._markets_loaded_at: datetime | None = None

    @property
    def markets(self) -> list[PolymarketMarket]:
        """Return currently tracked markets."""
        return list(self._markets)

    async def refresh_markets_if_needed(self) -> None:
        """Refresh the crypto market universe on a fixed cadence."""
        now = datetime.now(tz=UTC)
        if (
            self._markets_loaded_at is not None
            and now - self._markets_loaded_at < self._market_refresh
        ):
            return
        markets = await self._clob.fetch_crypto_markets(
            keywords=self._keywords,
            limit=self._max_markets,
        )
        self._markets = [
            market for market in markets if self._horizon is None or market.horizon == self._horizon
        ][: self._max_markets]
        logger.info(
            "polymarket_markets_refreshed",
            fetched_count=len(markets),
            tracked_count=len(self._markets),
            horizon=self._horizon,
            market_ids=[market.market_id for market in self._markets],
            slugs=[market.slug for market in self._markets],
        )
        self._markets_loaded_at = now

    async def build_context(self, market: PolymarketMarket) -> PolymarketMarketContext | None:
        """Build one synchronized strategy context."""
        books, spot, live_price_to_beat = await asyncio.gather(
            self._clob.fetch_books_for_market(market),
            self._coinbase.fetch_spot(market.asset_symbol),
            self._clob.fetch_price_to_beat(market),
        )
        yes_book, no_book = books
        yes_price = _snapshot_price(yes_book)
        no_price = _snapshot_price(no_book)
        if yes_price is None or no_price is None:
            logger.info(
                "polymarket_snapshot_skipped_empty_book",
                market_id=market.market_id,
                yes_has_price=yes_price is not None,
                no_has_price=no_price is not None,
            )
            return None
        price_to_beat = (
            live_price_to_beat if live_price_to_beat is not None else market.reference_price
        )
        market.reference_price = price_to_beat
        timestamp = max(yes_book.timestamp, no_book.timestamp, spot.timestamp)
        snapshot = PolymarketMarketSnapshot(
            market_id=market.market_id,
            market_question=market.question,
            yes_price=yes_price,
            no_price=no_price,
            yes_book_depth=_snapshot_depth(yes_book),
            no_book_depth=_snapshot_depth(no_book),
            coinbase_ref_price=spot.price,
            implied_gap=compute_implied_gap(
                yes_price=yes_price,
                no_price=no_price,
                coinbase_ref_price=spot.price,
                market_reference_price=price_to_beat,
            ),
            resolution_time=market.resolution_time,
            horizon=market.horizon,
            window_seconds=market.window_seconds,
            seconds_to_resolution=_seconds_to_resolution(market.resolution_time, timestamp),
            price_to_beat=price_to_beat,
            timestamp=timestamp,
        )
        return PolymarketMarketContext(
            market=market,
            snapshot=snapshot,
            yes_book=yes_book,
            no_book=no_book,
        )

    async def build_snapshot(self, market: PolymarketMarket) -> PolymarketMarketSnapshot | None:
        """Build one synchronized CLOB/spot snapshot."""
        context = await self.build_context(market)
        return context.snapshot if context is not None else None

    async def build_contexts(self) -> list[PolymarketMarketContext]:
        """Build strategy contexts for the tracked market universe."""
        await self.refresh_markets_if_needed()
        contexts: list[PolymarketMarketContext] = []
        for market in self._markets:
            context = await self.build_context(market)
            if context is not None:
                contexts.append(context)
        logger.info("polymarket_contexts_built", count=len(contexts))
        return contexts

    async def build_snapshots(self) -> list[PolymarketMarketSnapshot]:
        """Build snapshots for the tracked market universe."""
        contexts = await self.build_contexts()
        snapshots = [context.snapshot for context in contexts]
        logger.info("polymarket_snapshots_built", count=len(snapshots))
        return snapshots


def _snapshot_price(book: PolymarketOrderBook) -> float | None:
    """Return a usable live price from the book for snapshot purposes."""
    return book.best_ask if book.best_ask is not None else book.best_bid


def _snapshot_depth(book: PolymarketOrderBook) -> float:
    """Return buyable depth when available, otherwise visible bid depth."""
    return book.ask_depth if book.ask_depth > 0 else book.bid_depth


def _seconds_to_resolution(resolution_time: datetime | None, timestamp: datetime) -> int:
    if resolution_time is None:
        return 0
    return max(int(round((resolution_time - timestamp).total_seconds())), 0)
