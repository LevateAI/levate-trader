from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from src.polymarket.feeds import FeedWatchdog, PolymarketClobClient, _levels, _market_from_payload
from src.polymarket.main import PolymarketComponentStaleError, PolymarketRuntime
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
        slug="btc-updown-5m-1780000000",
        question="Will Bitcoin be above $100,000 today?",
        yes_token_id="yes-token",
        no_token_id="no-token",
        asset_symbol="BTC",
        horizon="5m",
        window_seconds=300,
        window_open_time=datetime(2026, 6, 1, 23, 54, tzinfo=UTC),
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
            horizon=market.horizon,
            window_seconds=market.window_seconds,
            seconds_to_resolution=300,
            timestamp=datetime.now(tz=UTC),
        ),
    )


@dataclass
class FakeRepository:
    snapshots: list[Any] = field(default_factory=list)
    trades: list[Any] = field(default_factory=list)
    positions: list[Any] = field(default_factory=list)
    equity_snapshots: list[dict[str, Any]] = field(default_factory=list)
    heartbeats: list[dict[str, Any]] = field(default_factory=list)
    fail_equity: bool = False

    async def insert_snapshot(self, snapshot: Any) -> None:
        self.snapshots.append(snapshot)

    async def insert_trade(self, trade: Any) -> None:
        self.trades.append(trade)

    async def insert_position(self, position: Any) -> None:
        self.positions.append(position)

    async def insert_equity_snapshot(
        self,
        balance_usd: float,
        equity_usd: float,
        open_position_count: int,
    ) -> None:
        if self.fail_equity:
            raise RuntimeError("equity insert failed")
        self.equity_snapshots.append(
            {
                "balance_usd": balance_usd,
                "equity_usd": equity_usd,
                "open_position_count": open_position_count,
            }
        )

    async def upsert_heartbeat(
        self,
        component: str,
        last_ok_at: datetime,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.heartbeats.append(
            {
                "component": component,
                "last_ok_at": last_ok_at,
                "detail": detail or {},
            }
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

    async def fetch_price_to_beat(self, market: PolymarketMarket) -> float | None:
        return market.reference_price


class FakeCoinbase:
    async def fetch_spot(self, asset_symbol: str) -> CoinbaseSpotPrice:
        return CoinbaseSpotPrice(
            product_id="BTC-USD",
            asset_symbol=asset_symbol,
            price=101_000,
            timestamp=datetime(2026, 6, 1, 12, 0, 1, tzinfo=UTC),
        )


class MultiHorizonClob(FakeClob):
    async def fetch_crypto_markets(self, keywords: list[str], limit: int) -> list[PolymarketMarket]:
        five_min = _market()
        fifteen_min = _market()
        fifteen_min.market_id = "market-15m"
        fifteen_min.condition_id = "condition-15m"
        fifteen_min.horizon = "15m"
        fifteen_min.window_seconds = 900
        fifteen_min.window_open_time = datetime(2026, 6, 1, 23, 44, tzinfo=UTC)
        return [five_min, fifteen_min]


class FallbackPriceClob(PolymarketClobClient):
    def __init__(self) -> None:
        self.watchdog = FeedWatchdog("fallback-test")
        self.best_price_calls: list[tuple[str, str]] = []

    async def fetch_order_book(
        self,
        token_id: str,
        market_id: str,
        side: PolymarketSide,
    ) -> PolymarketOrderBook:
        return PolymarketOrderBook(
            token_id=token_id,
            market_id=market_id,
            side=side,
            timestamp=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
            bids=[PolymarketBookLevel(price=0.01, size=100)],
            asks=[PolymarketBookLevel(price=0.99, size=100)],
        )

    async def fetch_best_price(self, token_id: str, side: str) -> float | None:
        self.best_price_calls.append((token_id, side))
        prices = {
            ("yes-token", "BUY"): 0.54,
            ("yes-token", "SELL"): 0.53,
            ("no-token", "BUY"): 0.47,
            ("no-token", "SELL"): 0.46,
        }
        return prices[(token_id, side)]


def _window_payload(
    asset: str,
    alias: str,
    start: str,
    end: str,
    resolution_time: str,
) -> dict[str, Any]:
    return {
        "id": f"{asset.lower()}-market",
        "conditionId": f"{asset.lower()}-condition",
        "slug": f"{asset.lower()}-updown-1780000000",
        "question": f"{alias} Up or Down - June 4, {start}-{end} ET",
        "clobTokenIds": '["yes-token","no-token"]',
        "enableOrderBook": True,
        "active": True,
        "closed": False,
        "archived": False,
        "endDate": resolution_time,
    }


def test_market_discovery_accepts_all_four_5m_crypto_assets() -> None:
    payloads = {
        "BTC": _window_payload("btc", "Bitcoin", "12:40PM", "12:45PM", "2026-06-04T16:45:00Z"),
        "ETH": _window_payload("eth", "Ethereum", "12:40PM", "12:45PM", "2026-06-04T16:45:00Z"),
        "SOL": _window_payload("sol", "Solana", "12:40PM", "12:45PM", "2026-06-04T16:45:00Z"),
        "XRP": _window_payload("xrp", "XRP", "12:40PM", "12:45PM", "2026-06-04T16:45:00Z"),
    }

    markets = [
        _market_from_payload(
            payload,
            ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "ripple"],
            0.07,
        )
        for payload in payloads.values()
    ]

    assert [market.asset_symbol for market in markets if market is not None] == [
        "BTC",
        "ETH",
        "SOL",
        "XRP",
    ]
    assert all(market is not None and market.yes_token_id == "yes-token" for market in markets)
    assert all(market is not None and market.no_token_id == "no-token" for market in markets)
    assert all(market is not None and market.horizon == "5m" for market in markets)
    assert all(market is not None and market.window_seconds == 300 for market in markets)


def test_market_discovery_accepts_15m_crypto_windows() -> None:
    market = _market_from_payload(
        _window_payload("btc", "Bitcoin", "12:40PM", "12:55PM", "2026-06-04T16:55:00Z"),
        ["bitcoin", "btc"],
        0.07,
    )

    assert market is not None
    assert market.horizon == "15m"
    assert market.window_seconds == 900


def test_polymarket_account_ids_are_coin_by_horizon_books() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.settings = SimpleNamespace(polymarket_horizon="15m")

    assert [runtime._account_id_for_asset(asset) for asset in ("BTC", "ETH", "SOL", "XRP")] == [
        "btc_15m",
        "eth_15m",
        "sol_15m",
        "xrp_15m",
    ]


def test_horizon_is_derived_from_window_length_not_title_label() -> None:
    payload = _window_payload(
        "eth",
        "Ethereum",
        "12:40PM",
        "12:55PM",
        "2026-06-04T16:55:00Z",
    )
    payload["slug"] = "ethereum-short-duration-window-without-horizon-label"

    market = _market_from_payload(payload, ["ethereum", "eth"], 0.07)

    assert market is not None
    assert market.horizon == "15m"
    assert market.window_seconds == 900


def test_snapshot_yes_no_prices_preserve_binary_probability_pair() -> None:
    context = _context(
        yes_book=_book(PolymarketSide.YES, asks=[(0.49, 100)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.51, 100)]),
    )

    assert context.snapshot.yes_price + context.snapshot.no_price == pytest.approx(1.0)


def test_market_discovery_rejects_long_dated_milestone_markets() -> None:
    payload = {
        "id": "milestone-market",
        "conditionId": "milestone-condition",
        "slug": "will-bitcoin-hit-100k-in-june",
        "question": "Will Bitcoin hit $100,000 in June?",
        "clobTokenIds": '["yes-token","no-token"]',
        "enableOrderBook": True,
        "active": True,
        "closed": False,
        "archived": False,
        "endDate": "2026-06-30T23:59:59Z",
    }

    market = _market_from_payload(payload, ["bitcoin", "btc"], 0.07)

    assert market is None


def test_book_levels_are_sorted_by_side() -> None:
    payload = [
        {"price": "0.52", "size": "5"},
        {"price": "0.49", "size": "10"},
        {"price": "0.51", "size": "7"},
    ]

    bids = _levels(payload, descending=True)
    asks = _levels(payload, descending=False)

    assert [level.price for level in bids] == [0.52, 0.51, 0.49]
    assert [level.price for level in asks] == [0.49, 0.51, 0.52]


@pytest.mark.asyncio
async def test_book_pair_falls_back_to_live_prices_when_sanity_fails() -> None:
    clob = FallbackPriceClob()

    yes_book, no_book = await clob.fetch_books_for_market(_market())

    assert yes_book.best_ask == pytest.approx(0.54)
    assert yes_book.best_bid == pytest.approx(0.53)
    assert no_book.best_ask == pytest.approx(0.47)
    assert no_book.best_bid == pytest.approx(0.46)
    assert yes_book.best_ask + no_book.best_ask == pytest.approx(1.01)
    assert len(clob.best_price_calls) == 4


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
    assert snapshot.price_to_beat == pytest.approx(100_000)
    assert snapshot.horizon == "5m"
    assert snapshot.window_seconds == 300
    assert snapshot.seconds_to_resolution > 0
    assert snapshot.implied_gap == pytest.approx(-0.38)
    payload = snapshot.to_payload()
    assert payload["horizon"] == "5m"
    assert payload["window_seconds"] == 300
    assert payload["seconds_to_resolution"] > 0
    assert payload["price_to_beat"] == pytest.approx(100_000)


@pytest.mark.asyncio
async def test_synchronizer_filters_markets_by_configured_horizon() -> None:
    synchronizer = PolymarketDataSynchronizer(
        clob_client=MultiHorizonClob(),  # type: ignore[arg-type]
        coinbase_client=FakeCoinbase(),  # type: ignore[arg-type]
        keywords=["bitcoin"],
        max_markets=2,
        horizon="15m",
    )

    await synchronizer.refresh_markets_if_needed()

    assert [market.horizon for market in synchronizer.markets] == ["15m"]
    assert [market.window_seconds for market in synchronizer.markets] == [900]


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
    assert trade.to_payload()["horizon"] == "5m"
    assert trade.to_payload()["window_seconds"] == 300
    assert position.horizon == "5m"
    assert position.window_seconds == 300


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
    runtime.executors = {"BTC": runtime.executor}
    runtime.repositories = {"BTC": runtime.repository}
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
async def test_equity_success_is_marked_only_after_db_write_succeeds() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.settings = SimpleNamespace(polymarket_horizon="5m")
    repository = FakeRepository()
    executor = PolymarketPaperExecutor("btc_5m", fees_enabled=False)
    runtime._asset_symbols = ("BTC",)
    runtime.executors = {"BTC": executor}
    runtime.repositories = {"BTC": repository}
    runtime._component_name = {("BTC", "equity"): "5m_btc_equity"}
    runtime._last_success = {"5m_btc_equity": 1.0}
    runtime._last_success_wall = {"5m_btc_equity": datetime(2026, 6, 1, tzinfo=UTC)}
    runtime._heartbeat_detail = {"5m_btc_equity": {"state": "old"}}

    await runtime._equity_iteration()

    assert repository.equity_snapshots
    assert runtime._last_success["5m_btc_equity"] > 1.0
    assert runtime._heartbeat_detail["5m_btc_equity"]["open_position_count"] == 0

    last_success = runtime._last_success["5m_btc_equity"]
    repository.fail_equity = True

    with pytest.raises(RuntimeError, match="equity insert failed"):
        await runtime._equity_iteration()

    assert runtime._last_success["5m_btc_equity"] == last_success


@pytest.mark.asyncio
async def test_polymarket_watchdog_upserts_and_raises_on_stale_component() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.settings = SimpleNamespace(stale_limit_seconds=1)
    repository = FakeRepository()
    runtime._heartbeat_repository = repository
    runtime._heartbeat_components = ("5m_btc_snapshot", "5m_btc_equity")
    runtime._last_success = {
        "5m_btc_snapshot": time.monotonic() - 2,
        "5m_btc_equity": time.monotonic(),
    }
    runtime._last_success_wall = {
        "5m_btc_snapshot": datetime.now(tz=UTC) - timedelta(seconds=2),
        "5m_btc_equity": datetime.now(tz=UTC),
    }
    runtime._heartbeat_detail = {
        "5m_btc_snapshot": {"context_count": 1},
        "5m_btc_equity": {"open_position_count": 0},
    }

    with pytest.raises(PolymarketComponentStaleError, match="5m_btc_snapshot"):
        await runtime._watchdog_check_once()

    assert [row["component"] for row in repository.heartbeats] == [
        "5m_btc_snapshot",
        "5m_btc_equity",
    ]
    assert repository.heartbeats[0]["detail"]["age_sec"] >= 1


@pytest.mark.asyncio
async def test_polymarket_task_supervisor_propagates_worker_failure() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.stop_event = asyncio.Event()

    async def dead_worker() -> None:
        raise RuntimeError("worker died")

    task = asyncio.create_task(dead_worker(), name="dead-worker")

    with pytest.raises(RuntimeError, match="worker died"):
        await runtime._supervise_tasks([task])


@pytest.mark.asyncio
async def test_runtime_settlement_writes_resolved_trade_and_position() -> None:
    runtime = object.__new__(PolymarketRuntime)
    repository = FakeRepository()
    executor = PolymarketPaperExecutor("btc_5m", fees_enabled=False)
    runtime.executors = {"BTC": executor}
    runtime.repositories = {"BTC": repository}
    market = _market()
    market.resolution_time = datetime.now(tz=UTC) - timedelta(seconds=1)
    await executor.open_position(
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
    assert repository.trades[-1].pnl_usd == pytest.approx(10 * (1 - 0.62))
    assert repository.positions[-1].status == "resolved"


@pytest.mark.asyncio
async def test_runtime_routes_trades_to_coin_horizon_book() -> None:
    runtime = object.__new__(PolymarketRuntime)
    runtime.settings = SimpleNamespace(polymarket_stale_threshold_sec=20)
    runtime.clob = SimpleNamespace(watchdog=SimpleNamespace(last_message_at=time.monotonic()))
    runtime.coinbase = SimpleNamespace(watchdog=SimpleNamespace(last_message_at=time.monotonic()))
    btc_repository = FakeRepository()
    eth_repository = FakeRepository()
    runtime.executors = {
        "BTC": PolymarketPaperExecutor("btc_5m", fees_enabled=False),
        "ETH": PolymarketPaperExecutor("eth_5m", fees_enabled=False),
    }
    runtime.repositories = {"BTC": btc_repository, "ETH": eth_repository}
    runtime.volatility_tracker = CoinbaseVolatilityTracker()
    runtime.strategies = [MultiOutcomeSumArbitrageStrategy(threshold=0.02, max_account_pct=0.10)]
    context = _context(
        yes_book=_book(PolymarketSide.YES, asks=[(0.45, 30)]),
        no_book=_book(PolymarketSide.NO, asks=[(0.45, 30)]),
    )

    executed = await runtime._run_strategies_for_context(context)

    assert executed is True
    assert btc_repository.trades
    assert not eth_repository.trades
    assert {trade.account_id for trade in btc_repository.trades} == {"btc_5m"}
    assert all(trade.horizon == "5m" for trade in btc_repository.trades)
    assert all(trade.window_seconds == 300 for trade in btc_repository.trades)
