"""Standalone Polymarket paper bot runtime.

Part 1 intentionally writes read-only market snapshots and account equity only.
Trading strategies are reserved for part 2.
"""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import UTC, datetime

import structlog

from src.logging import configure_logging
from src.polymarket.config import PolymarketSettings
from src.polymarket.feeds import CoinbaseSpotClient, PolymarketClobClient
from src.polymarket.models import PolymarketMarketContext, PolymarketSide
from src.polymarket.paper_executor import PolymarketPaperExecutor
from src.polymarket.repository import PolymarketRepository
from src.polymarket.signals import PolymarketSignal
from src.polymarket.strategies import (
    STRATEGY_REGISTRY,
    LatencyArbStrategy,
    MultiOutcomeSumArbitrageStrategy,
    PolymarketStrategy,
)
from src.polymarket.synchronizer import PolymarketDataSynchronizer
from src.polymarket.volatility import CoinbaseVolatilityTracker

logger = structlog.get_logger(__name__)


class PolymarketRuntime:
    """Coordinates two-feed snapshots and paper account bookkeeping."""

    def __init__(self, settings: PolymarketSettings | None = None) -> None:
        self.settings = settings or PolymarketSettings()
        configure_logging(self.settings.log_level)
        self.stop_event = asyncio.Event()
        self.clob = PolymarketClobClient(
            gamma_url=self.settings.polymarket_gamma_url,
            clob_url=self.settings.polymarket_clob_url,
            fee_rate_crypto=self.settings.polymarket_fee_rate_crypto,
            stale_threshold_sec=self.settings.polymarket_stale_threshold_sec,
        )
        self.coinbase = CoinbaseSpotClient(
            base_url=self.settings.coinbase_exchange_url,
            stale_threshold_sec=self.settings.polymarket_stale_threshold_sec,
        )
        self.synchronizer = PolymarketDataSynchronizer(
            clob_client=self.clob,
            coinbase_client=self.coinbase,
            keywords=self.settings.market_keywords,
            max_markets=self.settings.polymarket_max_markets,
            market_refresh_sec=self.settings.polymarket_market_refresh_sec,
        )
        self.repository = PolymarketRepository(self.settings)
        self.executor = PolymarketPaperExecutor(
            account_id=self.settings.polymarket_account_id,
            starting_balance_usd=self.settings.polymarket_starting_balance_usd,
            taker_fee_rate=self.settings.polymarket_fee_rate_crypto,
            fees_enabled=True,
        )
        self.volatility_tracker = CoinbaseVolatilityTracker(
            window_sec=self.settings.polymarket_vol_window_sec,
        )
        self.strategies = self._load_strategies()

    async def run(self) -> None:
        """Run the Polymarket data layer until shutdown."""
        logger.warning(
            "polymarket_runtime_starting",
            account_id=self.settings.polymarket_account_id,
            paper_balance_usd=self.settings.polymarket_starting_balance_usd,
            real_money_enabled=False,
        )
        await self.repository.ensure_account(
            display_name=self.settings.polymarket_display_name,
            starting_balance_usd=self.settings.polymarket_starting_balance_usd,
        )
        tasks = [
            asyncio.create_task(self._snapshot_loop(), name="polymarket-snapshot-loop"),
            asyncio.create_task(self._equity_loop(), name="polymarket-equity-loop"),
            asyncio.create_task(
                self.clob.watchdog.watchdog_loop(self.clob.reconnect),
                name="polymarket-clob-watchdog",
            ),
            asyncio.create_task(
                self.coinbase.watchdog.watchdog_loop(self.coinbase.reconnect),
                name="coinbase-spot-watchdog",
            ),
        ]
        await self.stop_event.wait()
        logger.info("polymarket_runtime_shutdown_started")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.clob.close()
        await self.coinbase.close()
        logger.info("polymarket_runtime_shutdown_complete")

    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self.stop_event.set()

    async def _snapshot_loop(self) -> None:
        while True:
            try:
                contexts = await self.synchronizer.build_contexts()
                for context in contexts:
                    await self.repository.insert_snapshot(context.snapshot)
                    self.volatility_tracker.record_price(
                        asset_symbol=context.market.asset_symbol,
                        price=context.snapshot.coinbase_ref_price,
                        timestamp=context.snapshot.timestamp,
                    )
                    await self._mark_open_positions(context)
                    if not self._feeds_fresh():
                        continue
                    if await self._settle_if_due(context):
                        continue
                    await self._run_strategies_for_context(context)
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error(
                    "polymarket_snapshot_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            await asyncio.sleep(self.settings.polymarket_poll_interval_sec)

    async def _equity_loop(self) -> None:
        while True:
            try:
                await self.repository.insert_equity_snapshot(
                    balance_usd=self.executor.balance_usd,
                    equity_usd=self.executor.equity_usd,
                    open_position_count=len(self.executor.positions),
                )
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error(
                    "polymarket_equity_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            await asyncio.sleep(60)

    def _load_strategies(self) -> list[PolymarketStrategy]:
        strategies: list[PolymarketStrategy] = []
        for name in self.settings.enabled_strategy_names:
            strategy_cls = STRATEGY_REGISTRY.get(name)
            if strategy_cls is None:
                logger.warning("polymarket_strategy_unknown", strategy_name=name)
                continue
            if strategy_cls is MultiOutcomeSumArbitrageStrategy:
                strategy = strategy_cls(
                    threshold=self.settings.polymarket_sum_arb_threshold,
                    max_account_pct=self.settings.polymarket_sum_arb_max_account_pct,
                    cooldown_seconds=self.settings.polymarket_strategy_cooldown_sec,
                )
            elif strategy_cls is LatencyArbStrategy:
                strategy = strategy_cls(
                    edge_threshold=self.settings.polymarket_latency_edge_threshold,
                    max_account_pct=self.settings.polymarket_latency_max_account_pct,
                    cooldown_seconds=self.settings.polymarket_strategy_cooldown_sec,
                )
            else:
                strategy = strategy_cls()
            strategies.append(strategy)
            logger.info("polymarket_strategy_loaded", strategy_name=name)
        return strategies

    async def _run_strategies_for_context(self, context: PolymarketMarketContext) -> None:
        if not self._feeds_fresh():
            return
        if self._market_has_open_position(context.market.market_id):
            return
        for strategy in self.strategies:
            signal_result = await strategy.on_context(
                context=context,
                account_equity=self.executor.equity_usd,
                volatility_tracker=self.volatility_tracker,
            )
            if signal_result is None:
                continue
            await self._execute_signal(signal_result)
            break

    async def _execute_signal(self, signal: PolymarketSignal) -> None:
        trades = await self.executor.execute_signal(signal)
        for trade in trades:
            await self.repository.insert_trade(trade)
        for leg in signal.legs:
            position = self.executor.positions.get((signal.market_id, leg.side))
            if position is not None:
                await self.repository.insert_position(position)

    async def _mark_open_positions(self, context: PolymarketMarketContext) -> None:
        for side, book in (
            (PolymarketSide.YES, context.yes_book),
            (PolymarketSide.NO, context.no_book),
        ):
            await self.executor.mark_to_market(context.market.market_id, side, book)
            position = self.executor.positions.get((context.market.market_id, side))
            if position is not None:
                await self.repository.insert_position(position)

    async def _settle_if_due(self, context: PolymarketMarketContext) -> bool:
        resolution_time = context.market.resolution_time
        reference_price = context.market.reference_price
        if resolution_time is None or reference_price is None:
            return False
        now = datetime.now(tz=UTC)
        if resolution_time > now:
            return False
        outcome = (
            PolymarketSide.YES
            if context.snapshot.coinbase_ref_price >= reference_price
            else PolymarketSide.NO
        )
        settled_any = False
        for side in (PolymarketSide.YES, PolymarketSide.NO):
            if (context.market.market_id, side) not in self.executor.positions:
                continue
            trade = await self.executor.settle_position(context.market.market_id, side, outcome)
            await self.repository.insert_trade(trade)
            if self.executor.closed_positions:
                await self.repository.insert_position(self.executor.closed_positions[-1])
            settled_any = True
        return settled_any

    def _market_has_open_position(self, market_id: str) -> bool:
        return any(position_market_id == market_id for position_market_id, _ in self.executor.positions)

    def _feeds_fresh(self) -> bool:
        clob_age = time.monotonic() - self.clob.watchdog.last_message_at
        coinbase_age = time.monotonic() - self.coinbase.watchdog.last_message_at
        stale_threshold = self.settings.polymarket_stale_threshold_sec
        if clob_age <= stale_threshold and coinbase_age <= stale_threshold:
            return True
        logger.warning(
            "trading_halted_stale_feed",
            polymarket_clob_age_sec=round(clob_age, 2),
            coinbase_spot_age_sec=round(coinbase_age, 2),
            stale_threshold_sec=stale_threshold,
        )
        return False


async def main() -> None:
    """Async entrypoint for systemd."""
    runtime = PolymarketRuntime()
    loop = asyncio.get_running_loop()
    for signame in ("SIGTERM", "SIGINT"):
        loop.add_signal_handler(getattr(signal, signame), runtime.request_shutdown)
    await runtime.run()


if __name__ == "__main__":
    asyncio.run(main())
