"""Two-feed Polymarket/Coinbase snapshot synchronizer."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import structlog

from src.polymarket.feeds import CoinbaseSpotClient, PolymarketClobClient
from src.polymarket.models import (
    PolymarketMarket,
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
    ) -> None:
        self._clob = clob_client
        self._coinbase = coinbase_client
        self._keywords = keywords
        self._max_markets = max_markets
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
        self._markets = await self._clob.fetch_crypto_markets(
            keywords=self._keywords,
            limit=self._max_markets,
        )
        self._markets_loaded_at = now

    async def build_snapshot(self, market: PolymarketMarket) -> PolymarketMarketSnapshot | None:
        """Build one synchronized CLOB/spot snapshot."""
        books, spot = await asyncio.gather(
            self._clob.fetch_books_for_market(market),
            self._coinbase.fetch_spot(market.asset_symbol),
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
        timestamp = max(yes_book.timestamp, no_book.timestamp, spot.timestamp)
        return PolymarketMarketSnapshot(
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
                market_reference_price=market.reference_price,
            ),
            resolution_time=market.resolution_time,
            timestamp=timestamp,
        )

    async def build_snapshots(self) -> list[PolymarketMarketSnapshot]:
        """Build snapshots for the tracked market universe."""
        await self.refresh_markets_if_needed()
        snapshots: list[PolymarketMarketSnapshot] = []
        for market in self._markets:
            snapshot = await self.build_snapshot(market)
            if snapshot is not None:
                snapshots.append(snapshot)
        logger.info("polymarket_snapshots_built", count=len(snapshots))
        return snapshots


def _snapshot_price(book: PolymarketOrderBook) -> float | None:
    """Return a usable live price from the book for snapshot purposes."""
    return book.best_ask if book.best_ask is not None else book.best_bid


def _snapshot_depth(book: PolymarketOrderBook) -> float:
    """Return buyable depth when available, otherwise visible bid depth."""
    return book.ask_depth if book.ask_depth > 0 else book.bid_depth
