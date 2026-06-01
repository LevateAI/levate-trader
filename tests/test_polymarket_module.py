from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from src.polymarket.feeds import FeedWatchdog
from src.polymarket.models import (
    CoinbaseSpotPrice,
    PolymarketBookLevel,
    PolymarketMarket,
    PolymarketOrderBook,
    PolymarketSide,
    fee_for_trade,
)
from src.polymarket.paper_executor import PolymarketPaperExecutor
from src.polymarket.synchronizer import PolymarketDataSynchronizer


def _market() -> PolymarketMarket:
    return PolymarketMarket(
        market_id="market-1",
        condition_id="condition-1",
        question="Will Bitcoin be above $100,000 today?",
        yes_token_id="yes-token",
        no_token_id="no-token",
        asset_symbol="BTC",
        resolution_time=datetime(2026, 6, 1, 23, 59, tzinfo=UTC),
        reference_price=100_000,
        fees_enabled=True,
        taker_fee_rate=0.07,
    )


def _book(
    side: PolymarketSide,
    bids: list[tuple[float, float]] | None = None,
    asks: list[tuple[float, float]] | None = None,
) -> PolymarketOrderBook:
    return PolymarketOrderBook(
        token_id=f"{side.value.lower()}-token",
        market_id="market-1",
        side=side,
        timestamp=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        bids=[PolymarketBookLevel(price=price, size=size) for price, size in (bids or [])],
        asks=[PolymarketBookLevel(price=price, size=size) for price, size in (asks or [])],
    )


class FakeClob:
    def __init__(self) -> None:
        self.market = _market()
        self.yes_book = _book(PolymarketSide.YES, asks=[(0.62, 100)])
        self.no_book = _book(PolymarketSide.NO, asks=[(0.4, 100)])

    async def fetch_crypto_markets(self, keywords: list[str], limit: int) -> list[PolymarketMarket]:
        return [self.market]

    async def fetch_books_for_market(
        self,
        market: PolymarketMarket,
    ) -> tuple[PolymarketOrderBook, PolymarketOrderBook]:
        return self.yes_book, self.no_book


class FakeCoinbase:
    async def fetch_spot(self, asset_symbol: str) -> CoinbaseSpotPrice:
        return CoinbaseSpotPrice(
            product_id="BTC-USD",
            asset_symbol=asset_symbol,
            price=101_000,
            timestamp=datetime(2026, 6, 1, 12, 0, 1, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_two_feed_synchronizer_produces_joined_snapshot() -> None:
    synchronizer = PolymarketDataSynchronizer(
        clob_client=FakeClob(),  # type: ignore[arg-type]
        coinbase_client=FakeCoinbase(),  # type: ignore[arg-type]
        keywords=["bitcoin"],
        max_markets=1,
    )

    snapshots = await synchronizer.build_snapshots()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.market_id == "market-1"
    assert snapshot.yes_price == pytest.approx(0.62)
    assert snapshot.no_price == pytest.approx(0.4)
    assert snapshot.coinbase_ref_price == pytest.approx(101_000)
    assert snapshot.implied_gap == pytest.approx(-0.38)


@pytest.mark.asyncio
async def test_paper_buying_shares_respects_book_depth() -> None:
    executor = PolymarketPaperExecutor(
        account_id="poly-test",
        starting_balance_usd=500,
        fees_enabled=False,
    )
    book = _book(PolymarketSide.YES, asks=[(0.62, 10), (0.63, 5)])

    trade = await executor.open_position(
        market_id="market-1",
        side=PolymarketSide.YES,
        requested_shares=20,
        order_book=book,
        strategy_name="test",
        reason_entry="depth test",
    )

    position = executor.positions[("market-1", PolymarketSide.YES)]
    assert trade.shares == pytest.approx(15)
    assert position.shares == pytest.approx(15)
    assert position.avg_entry_price == pytest.approx((0.62 * 10 + 0.63 * 5) / 15)


@pytest.mark.asyncio
async def test_yes_resolution_pnl_is_share_payout_minus_entry_cost() -> None:
    executor = PolymarketPaperExecutor("poly-test", fees_enabled=False)
    await executor.open_position(
        "market-1",
        PolymarketSide.YES,
        10,
        _book(PolymarketSide.YES, asks=[(0.62, 10)]),
        "test",
        "yes resolution test",
    )

    trade = await executor.settle_position("market-1", PolymarketSide.YES, PolymarketSide.YES)

    assert trade.pnl_usd == pytest.approx(10 * (1 - 0.62))


@pytest.mark.asyncio
async def test_no_resolution_pnl_is_negative_entry_cost_for_yes_position() -> None:
    executor = PolymarketPaperExecutor("poly-test", fees_enabled=False)
    await executor.open_position(
        "market-1",
        PolymarketSide.YES,
        10,
        _book(PolymarketSide.YES, asks=[(0.62, 10)]),
        "test",
        "no resolution test",
    )

    trade = await executor.settle_position("market-1", PolymarketSide.YES, PolymarketSide.NO)

    assert trade.pnl_usd == pytest.approx(-(0.62 * 10))


@pytest.mark.asyncio
async def test_mark_to_market_updates_unrealized_pnl_from_bid_book() -> None:
    executor = PolymarketPaperExecutor("poly-test", fees_enabled=False)
    await executor.open_position(
        "market-1",
        PolymarketSide.YES,
        10,
        _book(PolymarketSide.YES, asks=[(0.5, 10)]),
        "test",
        "mark test",
    )

    await executor.mark_to_market(
        "market-1",
        PolymarketSide.YES,
        _book(PolymarketSide.YES, bids=[(0.65, 100)]),
    )

    position = executor.positions[("market-1", PolymarketSide.YES)]
    assert position.current_price == pytest.approx(0.65)
    assert position.unrealized_pnl == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_feed_watchdog_fires_on_stale_feed() -> None:
    watchdog = FeedWatchdog("test_feed", stale_threshold_sec=20)
    watchdog.last_message_at = time.monotonic() - 21
    reconnect_calls = 0

    async def reconnect() -> None:
        nonlocal reconnect_calls
        reconnect_calls += 1

    reconnected = await watchdog.check_once(reconnect)

    assert reconnected is True
    assert reconnect_calls == 1


def test_polymarket_crypto_fee_formula() -> None:
    assert fee_for_trade(shares=100, avg_price=0.5, taker_fee_rate=0.07) == pytest.approx(1.75)

