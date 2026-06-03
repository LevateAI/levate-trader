from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from src.polymarket.feeds import FeedWatchdog
from src.polymarket.main import PolymarketRuntime
from src.polymarket.models import (
    CoinbaseSpotPrice,
    PolymarketBookLevel,
    PolymarketMarketContext,
    PolymarketMarketSnapshot,
    PolymarketMarket,
    PolymarketOrderBook,
    PolymarketSide,
    fee_for_trade,
)
from src.polymarket.paper_executor import PolymarketPaperExecutor
from src.polymarket.strategies import LatencyArbStrategy, MultiOutcomeSumArbitrageStrategy
from src.polymarket.synchronizer import PolymarketDataSynchronizer
from src.polymarket.volatility import CoinbaseVolatilityTracker


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


def _context(
    *,
    market: PolymarketMarket | None = None,
    yes_book: PolymarketOrderBook | None = None,
    no_book: PolymarketOrderBook | None = None,
    spot: float = 101_000,
) -> PolymarketMarketContext:
    market = market or _market()
    yes_book = yes_book or _book(PolymarketSide.YES, asks=[(0.62, 100)])
    no_book = no_book or _book(PolymarketSide.NO, asks=[(0.4, 100)])
    return PolymarketMarketContext(
        market=market,
        yes_book=yes_book,
        no_book=no_book,
        snapshot=PolymarketMarketSnapshot(
            market_id=market.market_id,
            market_question=market.question,
            yes_price=yes_book.best_ask or yes_book.best_bid or 0,
            no_price=no_book.best_ask or no_book.best_bid or 0,
            yes_book_depth=yes_book.ask_depth,
            no_book_depth=no_book.ask_depth,
            coinbase_ref_price=spot,
            implied_gap=0,
            resolution_time=market.resolution_time,
            timestamp=datetime.now(tz=UTC),
        ),
    )


@dataclass
class FakeRepository:
    trades: list[Any] = field(default_factory=list)
    positions: list[Any] = field(default_factory=list)

    async def insert_trade(self, trade: Any) -> None:
        self.trades.append(trade)

    async def insert_position(self, position: Any) -> None:
        self.positions.append(position)


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


@pytest.mark.asyncio
async def test_sum_arb_detects_and_sizes_within_book_depth() -> None:
    strategy = MultiOutcomeSumArbitrageStrategy(threshold=0.02, max_account_pct=0.10)
    context = _context(
        yes_book=_book(PolymarketSide.YES, asks=[(0.45, 30)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.45, 30)]),
    )

    signal = await strategy.on_context(
        context,
        account_equity=500,
        volatility_tracker=CoinbaseVolatilityTracker(),
    )

    assert signal is not None
    assert signal.strategy_name == "multi_outcome_sum_arb"
    assert len(signal.legs) == 2
    assert signal.legs[0].shares == pytest.approx(signal.legs[1].shares)
    assert signal.legs[0].shares <= 30
    assert signal.features["edge_after_fees"] > 0.02


@pytest.mark.asyncio
async def test_sum_arb_rejects_when_second_leg_erases_edge() -> None:
    strategy = MultiOutcomeSumArbitrageStrategy(threshold=0.02, max_account_pct=0.10)
    context = _context(
        yes_book=_book(PolymarketSide.YES, asks=[(0.45, 10)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.45, 1), (0.75, 9)]),
    )

    signal = await strategy.on_context(
        context,
        account_equity=500,
        volatility_tracker=CoinbaseVolatilityTracker(),
    )

    assert signal is None


def test_latency_fair_value_model_produces_sane_probability() -> None:
    tracker = CoinbaseVolatilityTracker(window_sec=3600)
    now = datetime.now(tz=UTC)
    tracker.record_price("BTC", 104_900, now - timedelta(seconds=120))
    tracker.record_price("BTC", 104_950, now - timedelta(seconds=60))
    tracker.record_price("BTC", 105_000, now)

    probability = tracker.fair_yes_probability("BTC", 105_000, 100_000, 3600)

    assert probability is not None
    assert 0.5 < probability <= 1.0


@pytest.mark.asyncio
async def test_latency_arb_emits_signal_only_when_edge_exceeds_threshold() -> None:
    strategy = LatencyArbStrategy(edge_threshold=0.05, max_account_pct=0.05)
    now = datetime.now(tz=UTC)
    tracker = CoinbaseVolatilityTracker(window_sec=3600)
    tracker.record_price("BTC", 104_900, now - timedelta(seconds=120))
    tracker.record_price("BTC", 104_950, now - timedelta(seconds=60))
    tracker.record_price("BTC", 105_000, now)
    market = _market()
    market.resolution_time = now + timedelta(hours=1)
    cheap_context = _context(
        market=market,
        spot=105_000,
        yes_book=_book(PolymarketSide.YES, asks=[(0.80, 100)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.30, 100)]),
    )
    rich_context = _context(
        market=market,
        spot=105_000,
        yes_book=_book(PolymarketSide.YES, asks=[(0.99, 100)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.30, 100)]),
    )

    signal = await strategy.on_context(cheap_context, 500, tracker, now=now)
    no_signal = await LatencyArbStrategy(edge_threshold=0.05).on_context(
        rich_context,
        500,
        tracker,
        now=now,
    )

    assert signal is not None
    assert signal.strategy_name == "latency_arb"
    assert signal.legs[0].side == PolymarketSide.YES
    assert signal.features["fair_yes_probability"] > signal.legs[0].expected_avg_price
    assert no_signal is None


@pytest.mark.asyncio
async def test_polymarket_runtime_halts_strategies_when_feed_is_stale() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.settings = SimpleNamespace(polymarket_stale_threshold_sec=20)
    runtime.clob = SimpleNamespace(
        watchdog=SimpleNamespace(last_message_at=time.monotonic() - 21)
    )
    runtime.coinbase = SimpleNamespace(
        watchdog=SimpleNamespace(last_message_at=time.monotonic())
    )
    runtime.executor = PolymarketPaperExecutor("poly-test", fees_enabled=False)
    runtime.repository = FakeRepository()
    runtime.volatility_tracker = CoinbaseVolatilityTracker()
    runtime.strategies = [MultiOutcomeSumArbitrageStrategy(threshold=0.02, max_account_pct=0.10)]
    context = _context(
        yes_book=_book(PolymarketSide.YES, asks=[(0.45, 30)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.45, 30)]),
    )

    await runtime._run_strategies_for_context(context)

    assert not runtime.executor.trades
    assert not runtime.repository.trades


@pytest.mark.asyncio
async def test_runtime_settlement_writes_resolved_trade_and_position() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.executor = PolymarketPaperExecutor("poly-test", fees_enabled=False)
    runtime.repository = FakeRepository()
    market = _market()
    market.resolution_time = datetime.now(tz=UTC) - timedelta(seconds=1)
    await runtime.executor.open_position(
        market.market_id,
        PolymarketSide.YES,
        10,
        _book(PolymarketSide.YES, asks=[(0.62, 10)]),
        "test",
        "runtime settlement test",
    )
    context = _context(market=market, spot=101_000)

    settled = await runtime._settle_if_due(context)

    assert settled is True
    assert runtime.repository.trades[-1].pnl_usd == pytest.approx(10 * (1 - 0.62))
    assert runtime.repository.positions[-1].status == "resolved"
