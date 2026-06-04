"""Standalone Polymarket paper bot runtime.

Part 1 intentionally writes read-only market snapshots and account equity only.
Trading strategies are reserved for part 2.
"""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import UTC, datetime
from typing import Any

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

HEARTBEAT_COMPONENTS: tuple[str, ...] = ("snapshot", "strategy", "settle", "equity")
LOOP_ERROR_BACKOFF_SEC = 5.0


class PolymarketComponentStaleError(RuntimeError):
    """Raised when a Polymarket runtime component has stopped doing useful work."""


class PolymarketRuntime:
    """Coordinates two-feed snapshots and paper account bookkeeping."""

    def __init__(self, settings: PolymarketSettings | None = None) -> None:
        self.settings = settings or PolymarketSettings()  # type: ignore[call-arg]
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
        started_at_mono = time.monotonic()
        started_at_wall = datetime.now(tz=UTC)
        self._heartbeat_components = HEARTBEAT_COMPONENTS
        self._last_success: dict[str, float] = {
            component: started_at_mono for component in self._heartbeat_components
        }
        self._last_success_wall: dict[str, datetime] = {
            component: started_at_wall for component in self._heartbeat_components
        }
        self._heartbeat_detail: dict[str, dict[str, Any]] = {
            component: {"state": "initializing"} for component in self._heartbeat_components
        }
        self._latest_contexts: dict[str, PolymarketMarketContext] = {}

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
            asyncio.create_task(self._strategy_loop(), name="polymarket-strategy-loop"),
            asyncio.create_task(self._settlement_loop(), name="polymarket-settlement-loop"),
            asyncio.create_task(self._equity_loop(), name="polymarket-equity-loop"),
            asyncio.create_task(self._watchdog_loop(), name="polymarket-runtime-watchdog"),
            asyncio.create_task(
                self.clob.watchdog.watchdog_loop(self.clob.reconnect),
                name="polymarket-clob-watchdog",
            ),
            asyncio.create_task(
                self.coinbase.watchdog.watchdog_loop(self.coinbase.reconnect),
                name="coinbase-spot-watchdog",
            ),
        ]
        try:
            await self._supervise_tasks(tasks)
        finally:
            logger.info("polymarket_runtime_shutdown_started")
            await self._cancel_tasks(tasks)
            await self.clob.close()
            await self.coinbase.close()
            logger.info("polymarket_runtime_shutdown_complete")

    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self.stop_event.set()

    async def _supervise_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Wait for shutdown or fail loudly when any worker task exits."""
        stop_task = asyncio.create_task(self.stop_event.wait(), name="polymarket-stop-wait")
        try:
            done, _ = await asyncio.wait(
                [*tasks, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done and self.stop_event.is_set():
                return
            for task in done:
                if task is stop_task:
                    continue
                task_name = task.get_name()
                if task.cancelled():
                    logger.critical("polymarket_task_cancelled", task_name=task_name)
                    raise RuntimeError(f"Polymarket task cancelled unexpectedly: {task_name}")
                exc = task.exception()
                if exc is not None:
                    logger.critical(
                        "polymarket_task_failed",
                        task_name=task_name,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    raise exc
                logger.critical("polymarket_task_exited", task_name=task_name)
                raise RuntimeError(f"Polymarket task exited unexpectedly: {task_name}")
        finally:
            stop_task.cancel()
            try:
                await stop_task
            except asyncio.CancelledError:
                pass

    async def _cancel_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Cancel worker tasks during shutdown without hiding live supervisor failures."""
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error(
                    "polymarket_task_cleanup_error",
                    task_name=task.get_name(),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

    async def _snapshot_loop(self) -> None:
        while True:
            try:
                await self._snapshot_iteration()
            except Exception as exc:
                logger.error(
                    "polymarket_snapshot_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(LOOP_ERROR_BACKOFF_SEC)
                continue
            await asyncio.sleep(self.settings.polymarket_poll_interval_sec)

    async def _strategy_loop(self) -> None:
        while True:
            try:
                await self._strategy_iteration()
            except Exception as exc:
                logger.error(
                    "polymarket_strategy_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(LOOP_ERROR_BACKOFF_SEC)
                continue
            await asyncio.sleep(self.settings.polymarket_poll_interval_sec)

    async def _settlement_loop(self) -> None:
        while True:
            try:
                await self._settlement_iteration()
            except Exception as exc:
                logger.error(
                    "polymarket_settlement_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(LOOP_ERROR_BACKOFF_SEC)
                continue
            await asyncio.sleep(self.settings.polymarket_poll_interval_sec)

    async def _equity_loop(self) -> None:
        while True:
            try:
                await self._equity_iteration()
            except Exception as exc:
                logger.error(
                    "polymarket_equity_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(LOOP_ERROR_BACKOFF_SEC)
                continue
            await asyncio.sleep(60)

    async def _watchdog_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.watchdog_interval_seconds)
            try:
                await self._watchdog_check_once()
            except PolymarketComponentStaleError:
                raise
            except Exception as exc:
                logger.error(
                    "polymarket_watchdog_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(LOOP_ERROR_BACKOFF_SEC)

    async def _snapshot_iteration(self) -> None:
        contexts = await self.synchronizer.build_contexts()
        if not contexts:
            return
        for context in contexts:
            await self.repository.insert_snapshot(context.snapshot)
            self._latest_contexts[context.market.market_id] = context
            self.volatility_tracker.record_price(
                asset_symbol=context.market.asset_symbol,
                price=context.snapshot.coinbase_ref_price,
                timestamp=context.snapshot.timestamp,
            )
            await self._mark_open_positions(context)
        self._mark_success(
            "snapshot",
            {
                "context_count": len(contexts),
                "open_position_count": len(self.executor.positions),
            },
        )

    async def _strategy_iteration(self) -> None:
        contexts = list(self._latest_contexts.values())
        if not contexts:
            return
        evaluated_count = 0
        executed_count = 0
        feeds_fresh = self._feeds_fresh()
        if not feeds_fresh:
            return
        if feeds_fresh:
            for context in contexts:
                evaluated_count += 1
                if await self._run_strategies_for_context(context):
                    executed_count += 1
        self._mark_success(
            "strategy",
            {
                "context_count": len(contexts),
                "evaluated_count": evaluated_count,
                "executed_count": executed_count,
                "feeds_fresh": feeds_fresh,
            },
        )

    async def _settlement_iteration(self) -> None:
        contexts = list(self._latest_contexts.values())
        if not contexts:
            return
        settled_count = 0
        feeds_fresh = self._feeds_fresh()
        if not feeds_fresh:
            return
        if feeds_fresh:
            for context in contexts:
                if await self._settle_if_due(context):
                    settled_count += 1
        self._mark_success(
            "settle",
            {
                "context_count": len(contexts),
                "settled_count": settled_count,
                "feeds_fresh": feeds_fresh,
            },
        )

    async def _equity_iteration(self) -> None:
        await self.repository.insert_equity_snapshot(
            balance_usd=self.executor.balance_usd,
            equity_usd=self.executor.equity_usd,
            open_position_count=len(self.executor.positions),
        )
        self._mark_success(
            "equity",
            {
                "balance_usd": self.executor.balance_usd,
                "equity_usd": self.executor.equity_usd,
                "open_position_count": len(self.executor.positions),
            },
        )

    async def _watchdog_check_once(self) -> None:
        now_mono = time.monotonic()
        stale_component: str | None = None
        stale_age = 0.0
        for component in self._heartbeat_components:
            last_success_mono = self._last_success[component]
            last_success_wall = self._last_success_wall[component]
            age_sec = now_mono - last_success_mono
            detail = {
                **self._heartbeat_detail.get(component, {}),
                "age_sec": round(age_sec, 2),
                "stale_limit_seconds": self.settings.stale_limit_seconds,
            }
            await self.repository.upsert_heartbeat(
                component=component,
                last_ok_at=last_success_wall,
                detail=detail,
            )
            if age_sec > self.settings.stale_limit_seconds and stale_component is None:
                stale_component = component
                stale_age = age_sec
        if stale_component is not None:
            logger.critical(
                "polymarket_component_stale",
                component=stale_component,
                age_sec=round(stale_age, 2),
                stale_limit_seconds=self.settings.stale_limit_seconds,
            )
            raise PolymarketComponentStaleError(
                f"Polymarket component stale: {stale_component} age={stale_age:.2f}s"
            )

    def _mark_success(self, component: str, detail: dict[str, Any] | None = None) -> None:
        self._last_success[component] = time.monotonic()
        self._last_success_wall[component] = datetime.now(tz=UTC)
        self._heartbeat_detail[component] = detail or {}

    def _load_strategies(self) -> list[PolymarketStrategy]:
        strategies: list[PolymarketStrategy] = []
        for name in self.settings.enabled_strategy_names:
            strategy_cls = STRATEGY_REGISTRY.get(name)
            if strategy_cls is None:
                logger.warning("polymarket_strategy_unknown", strategy_name=name)
                continue
            strategy: PolymarketStrategy
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

    async def _run_strategies_for_context(self, context: PolymarketMarketContext) -> bool:
        if not self._feeds_fresh():
            return False
        if self._market_has_open_position(context.market.market_id):
            return False
        for strategy in self.strategies:
            signal_result = await strategy.on_context(
                context=context,
                account_equity=self.executor.equity_usd,
                volatility_tracker=self.volatility_tracker,
            )
            if signal_result is None:
                continue
            await self._execute_signal(signal_result)
            return True
        return False

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
        return any(
            position_market_id == market_id
            for position_market_id, _ in self.executor.positions
        )

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
