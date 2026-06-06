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
        self._asset_symbols = self.settings.enabled_asset_symbols
        self.clob = PolymarketClobClient(
            gamma_url=self.settings.polymarket_gamma_url,
            clob_url=self.settings.polymarket_clob_url,
            polymarket_web_url=self.settings.polymarket_web_url,
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
            horizon=self.settings.polymarket_horizon,
        )
        self.repositories = {
            asset_symbol: PolymarketRepository(
                self.settings,
                account_id=self._account_id_for_asset(asset_symbol),
            )
            for asset_symbol in self._asset_symbols
        }
        self.executors = {
            asset_symbol: PolymarketPaperExecutor(
                account_id=self._account_id_for_asset(asset_symbol),
                starting_balance_usd=self.settings.polymarket_starting_balance_usd,
                taker_fee_rate=self.settings.polymarket_fee_rate_crypto,
                fees_enabled=True,
            )
            for asset_symbol in self._asset_symbols
        }
        self._heartbeat_repository = self.repositories[self._asset_symbols[0]]
        self._snapshot_repository = self.repositories[self._asset_symbols[0]]
        self.repository = self._heartbeat_repository
        self.executor = self.executors[self._asset_symbols[0]]
        self.volatility_tracker = CoinbaseVolatilityTracker(
            window_sec=self.settings.polymarket_vol_window_sec,
        )
        self.strategies = self._load_strategies()
        started_at_mono = time.monotonic()
        started_at_wall = datetime.now(tz=UTC)
        self._component_name = {
            (asset_symbol, component): (
                f"{self.settings.polymarket_horizon}_{asset_symbol.lower()}_{component}"
            )
            for asset_symbol in self._asset_symbols
            for component in HEARTBEAT_COMPONENTS
        }
        self._heartbeat_components = tuple(self._component_name.values())
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

    def _account_id_for_asset(self, asset_symbol: str) -> str:
        """Return the tournament book id for an asset in this horizon process."""
        return f"{asset_symbol.lower()}_{self.settings.polymarket_horizon}"

    def _display_name_for_asset(self, asset_symbol: str) -> str:
        """Return a human-friendly account display name."""
        return f"Polymarket {asset_symbol.upper()} {self.settings.polymarket_horizon}"

    async def run(self) -> None:
        """Run the Polymarket data layer until shutdown."""
        logger.warning(
            "polymarket_runtime_starting",
            horizon=self.settings.polymarket_horizon,
            account_ids=[
                self._account_id_for_asset(asset_symbol) for asset_symbol in self._asset_symbols
            ],
            paper_balance_usd_per_asset=self.settings.polymarket_starting_balance_usd,
            real_money_enabled=False,
        )
        for asset_symbol, repository in self.repositories.items():
            await repository.ensure_account(
                display_name=self._display_name_for_asset(asset_symbol),
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
        contexts_by_asset: dict[str, int] = {asset_symbol: 0 for asset_symbol in self._asset_symbols}
        for context in contexts:
            asset_symbol = context.market.asset_symbol
            if asset_symbol not in self.executors:
                logger.warning(
                    "polymarket_context_asset_not_enabled",
                    asset_symbol=asset_symbol,
                    market_id=context.market.market_id,
                    horizon=context.market.horizon,
                )
                continue
            await self._snapshot_repository.insert_snapshot(context.snapshot)
            self._latest_contexts[context.market.market_id] = context
            self.volatility_tracker.record_price(
                asset_symbol=asset_symbol,
                price=context.snapshot.coinbase_ref_price,
                timestamp=context.snapshot.timestamp,
            )
            await self._mark_open_positions(context)
            contexts_by_asset[asset_symbol] = contexts_by_asset.get(asset_symbol, 0) + 1
        for asset_symbol, context_count in contexts_by_asset.items():
            if context_count <= 0:
                continue
            executor = self.executors[asset_symbol]
            self._mark_success(
                "snapshot",
                asset_symbol=asset_symbol,
                detail={
                    "context_count": context_count,
                    "open_position_count": len(executor.positions),
                },
            )

    async def _strategy_iteration(self) -> None:
        contexts = list(self._latest_contexts.values())
        if not contexts:
            return
        stats_by_asset: dict[str, dict[str, int]] = {
            asset_symbol: {"context_count": 0, "evaluated_count": 0, "executed_count": 0}
            for asset_symbol in self._asset_symbols
        }
        feeds_fresh = self._feeds_fresh()
        if not feeds_fresh:
            return
        if feeds_fresh:
            for context in contexts:
                asset_symbol = context.market.asset_symbol
                if asset_symbol not in self.executors:
                    continue
                stats = stats_by_asset[asset_symbol]
                stats["context_count"] += 1
                stats["evaluated_count"] += 1
                if await self._run_strategies_for_context(context):
                    stats["executed_count"] += 1
        for asset_symbol, stats in stats_by_asset.items():
            if stats["context_count"] <= 0:
                continue
            self._mark_success(
                "strategy",
                asset_symbol=asset_symbol,
                detail={
                    **stats,
                    "feeds_fresh": feeds_fresh,
                },
            )

    async def _settlement_iteration(self) -> None:
        contexts = list(self._latest_contexts.values())
        if not contexts:
            return
        stats_by_asset: dict[str, dict[str, int]] = {
            asset_symbol: {"context_count": 0, "settled_count": 0}
            for asset_symbol in self._asset_symbols
        }
        feeds_fresh = self._feeds_fresh()
        if not feeds_fresh:
            return
        if feeds_fresh:
            for context in contexts:
                asset_symbol = context.market.asset_symbol
                if asset_symbol not in self.executors:
                    continue
                stats = stats_by_asset[asset_symbol]
                stats["context_count"] += 1
                if await self._settle_if_due(context):
                    stats["settled_count"] += 1
        for asset_symbol, stats in stats_by_asset.items():
            if stats["context_count"] <= 0:
                continue
            self._mark_success(
                "settle",
                asset_symbol=asset_symbol,
                detail={
                    **stats,
                    "feeds_fresh": feeds_fresh,
                },
            )

    async def _equity_iteration(self) -> None:
        for asset_symbol in self._asset_symbols:
            repository = self.repositories[asset_symbol]
            executor = self.executors[asset_symbol]
            await repository.insert_equity_snapshot(
                balance_usd=executor.balance_usd,
                equity_usd=executor.equity_usd,
                open_position_count=len(executor.positions),
            )
            self._mark_success(
                "equity",
                asset_symbol=asset_symbol,
                detail={
                    "balance_usd": executor.balance_usd,
                    "equity_usd": executor.equity_usd,
                    "open_position_count": len(executor.positions),
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
            await self._heartbeat_repository.upsert_heartbeat(
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

    def _mark_success(
        self,
        component: str,
        detail: dict[str, Any] | None = None,
        asset_symbol: str | None = None,
    ) -> None:
        component_map = getattr(self, "_component_name", {})
        if asset_symbol is None:
            component_name = component_map.get(component, component)
        else:
            component_name = component_map.get(
                (asset_symbol.upper(), component),
                f"{getattr(self.settings, 'polymarket_horizon', '5m')}_{asset_symbol.lower()}_{component}",
            )
        self._last_success[component_name] = time.monotonic()
        self._last_success_wall[component_name] = datetime.now(tz=UTC)
        self._heartbeat_detail[component_name] = detail or {}

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
                    max_stake_usd=self.settings.polymarket_sum_arb_max_stake_usd,
                    cooldown_seconds=self.settings.polymarket_strategy_cooldown_sec,
                )
            elif strategy_cls is LatencyArbStrategy:
                strategy = strategy_cls(
                    edge_threshold=self.settings.polymarket_latency_edge_threshold,
                    max_account_pct=self.settings.polymarket_latency_max_account_pct,
                    max_stake_usd=self.settings.polymarket_latency_max_stake_usd,
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
        asset_symbol = context.market.asset_symbol
        if asset_symbol not in self.executors:
            return False
        executor = self.executors[asset_symbol]
        if self._market_has_open_position(context.market.market_id, asset_symbol):
            return False
        for strategy in self.strategies:
            signal_result = await strategy.on_context(
                context=context,
                account_equity=executor.equity_usd,
                volatility_tracker=self.volatility_tracker,
            )
            if signal_result is None:
                continue
            await self._execute_signal(signal_result, asset_symbol)
            return True
        return False

    async def _execute_signal(self, signal: PolymarketSignal, asset_symbol: str) -> None:
        executor = self.executors[asset_symbol]
        repository = self.repositories[asset_symbol]
        trades = await executor.execute_signal(signal)
        for trade in trades:
            await repository.insert_trade(trade)
        for leg in signal.legs:
            position = executor.positions.get((signal.market_id, leg.side))
            if position is not None:
                await repository.insert_position(position)

    async def _mark_open_positions(self, context: PolymarketMarketContext) -> None:
        asset_symbol = context.market.asset_symbol
        if asset_symbol not in self.executors:
            return
        executor = self.executors[asset_symbol]
        repository = self.repositories[asset_symbol]
        for side, book in (
            (PolymarketSide.YES, context.yes_book),
            (PolymarketSide.NO, context.no_book),
        ):
            await executor.mark_to_market(context.market.market_id, side, book)
            position = executor.positions.get((context.market.market_id, side))
            if position is not None:
                await repository.insert_position(position)

    async def _settle_if_due(self, context: PolymarketMarketContext) -> bool:
        asset_symbol = context.market.asset_symbol
        if asset_symbol not in self.executors:
            return False
        executor = self.executors[asset_symbol]
        repository = self.repositories[asset_symbol]
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
            if (context.market.market_id, side) not in executor.positions:
                continue
            trade = await executor.settle_position(context.market.market_id, side, outcome)
            await repository.insert_trade(trade)
            if executor.closed_positions:
                await repository.insert_position(executor.closed_positions[-1])
            settled_any = True
        return settled_any

    def _market_has_open_position(self, market_id: str, asset_symbol: str) -> bool:
        executor = self.executors[asset_symbol]
        return any(
            position_market_id == market_id
            for position_market_id, _ in executor.positions
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
