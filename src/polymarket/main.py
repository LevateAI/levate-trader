"""Standalone Polymarket paper bot runtime.

Part 1 intentionally writes read-only market snapshots and account equity only.
Trading strategies are reserved for part 2.
"""

from __future__ import annotations

import asyncio
import signal

import structlog

from src.logging import configure_logging
from src.polymarket.config import PolymarketSettings
from src.polymarket.feeds import CoinbaseSpotClient, PolymarketClobClient
from src.polymarket.paper_executor import PolymarketPaperExecutor
from src.polymarket.repository import PolymarketRepository
from src.polymarket.synchronizer import PolymarketDataSynchronizer

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
                snapshots = await self.synchronizer.build_snapshots()
                for snapshot in snapshots:
                    await self.repository.insert_snapshot(snapshot)
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


async def main() -> None:
    """Async entrypoint for systemd."""
    runtime = PolymarketRuntime()
    loop = asyncio.get_running_loop()
    for signame in ("SIGTERM", "SIGINT"):
        loop.add_signal_handler(getattr(signal, signame), runtime.request_shutdown)
    await runtime.run()


if __name__ == "__main__":
    asyncio.run(main())
