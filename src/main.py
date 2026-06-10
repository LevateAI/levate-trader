"""Main trading loop."""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import structlog

from src.alerts.discord_notifier import DiscordNotifier
from src.alerts.twilio_notifier import TwilioNotifier
from src.config import get_settings
from src.db.supabase_client import SupabaseRepository
from src.exchange.hyperliquid_client import HyperliquidClient
from src.execution import get_executor
from src.logging import configure_logging
from src.models import BTC_PERP, ETH_PERP, EquitySnapshot, MarketState
from src.risk.circuit_breakers import CircuitBreakerManager
from src.strategies import STRATEGY_REGISTRY, Strategy
from src.strategies.chaos_wrapper import ChaosStrategyWrapper
from src.strategies.scalp_common import SCALP_STRATEGY_NAMES

logger = structlog.get_logger(__name__)
STALE_LOG_THROTTLE_SEC = 30
HIGH_WATER_STATE_KEY = "high_water_equity"
HEARTBEAT_BASE_COMPONENTS: tuple[str, ...] = ("market", "strategy", "equity")


class TraderComponentStaleError(RuntimeError):
    """Raised when a perp runtime loop has stopped doing useful work."""


class TraderRuntime:
    """Coordinates exchange data, strategies, risk checks, and persistence."""

    def __init__(self) -> None:
        self.settings = get_settings()
        configure_logging(self.settings.log_level)
        self._log_startup_banner()
        self.repository = SupabaseRepository(self.settings)
        self.exchange = HyperliquidClient(self.settings)
        self.discord = DiscordNotifier(self.settings.discord_webhook_url)
        self.sms = TwilioNotifier(self.settings)
        self.circuit_breakers = CircuitBreakerManager(
            self.settings,
            self.repository,
            self.discord,
            self.sms,
        )
        self.executor = get_executor(
            settings=self.settings,
            exchange=self.exchange,
            repository=self.repository,
            circuit_breakers=self.circuit_breakers,
            discord=self.discord,
            sms=self.sms,
        )
        self.stop_event = asyncio.Event()
        self.high_water_equity = self.settings.starting_balance_usd
        self._bars_5m: dict[str, list[dict[str, Any]]] = {}
        self._bars_last_fetch: dict[str, datetime] = {}
        self._latest_market: dict[str, MarketState] = {}
        self._last_market_state = self._latest_market
        self._pending_trade_events: dict[str, list[dict[str, Any]]] = {}
        self._last_market_write_source_timestamp: dict[str, datetime] = {}
        self._throttled_log_at: dict[str, float] = {}
        self._http_calls_this_minute = 0
        components = list(HEARTBEAT_BASE_COMPONENTS)
        if self.settings.market_data_writer:
            components.append("market_data")
        self._heartbeat_components: tuple[str, ...] = tuple(components)
        started_at_mono = time.monotonic()
        started_at_wall = datetime.now(tz=UTC)
        self._last_success: dict[str, float] = {
            component: started_at_mono for component in self._heartbeat_components
        }
        self._last_success_wall: dict[str, datetime] = {
            component: started_at_wall for component in self._heartbeat_components
        }
        self._heartbeat_detail: dict[str, dict[str, Any]] = {
            component: {"state": "initializing"} for component in self._heartbeat_components
        }

    async def run(self) -> None:
        """Run until shutdown, failing loudly when any worker task dies."""
        logger.info("runtime_starting")
        await self.sms.start()
        await self.circuit_breakers.load_state()
        await self._load_high_water_equity()
        if self.settings.execution_mode == "paper_sim":
            await self.executor.restore_state()
        await self.exchange.connect_ws()
        strategies = self._load_strategies()
        symbols = sorted({symbol for strategy in strategies for symbol in strategy.symbols})
        for symbol in symbols:
            await self.exchange.subscribe_to_book(symbol)
            await self.exchange.subscribe_to_trades(symbol)
        await self.exchange.subscribe_to_user_fills()

        tasks = [
            asyncio.create_task(self._market_loop(strategies), name="market-loop"),
            asyncio.create_task(self._strategy_loop(strategies), name="strategy-loop"),
            asyncio.create_task(self._market_snapshot_loop(), name="market-snapshot-loop"),
            asyncio.create_task(self._snapshot_loop(), name="snapshot-loop"),
            asyncio.create_task(self._market_data_prune_loop(), name="market-data-prune-loop"),
            asyncio.create_task(self._runtime_watchdog_loop(), name="runtime-watchdog"),
            asyncio.create_task(
                self.exchange.watchdog_loop(
                    stale_threshold_sec=self.settings.stale_threshold_sec,
                    check_interval_sec=5,
                ),
                name="hyperliquid-ws-watchdog",
            ),
        ]
        try:
            await self._supervise_tasks(tasks)
        finally:
            logger.info("runtime_shutdown_started")
            await self._cancel_tasks(tasks)
            await self._shutdown()

    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self.stop_event.set()

    async def _supervise_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Wait for shutdown or fail loudly when any worker task exits."""
        stop_task = asyncio.create_task(self.stop_event.wait(), name="runtime-stop-wait")
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
                    logger.critical("runtime_task_cancelled", task_name=task_name)
                    raise RuntimeError(f"perp task cancelled unexpectedly: {task_name}")
                exc = task.exception()
                if exc is not None:
                    logger.critical(
                        "runtime_task_failed",
                        task_name=task_name,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    raise exc
                logger.critical("runtime_task_exited", task_name=task_name)
                raise RuntimeError(f"perp task exited unexpectedly: {task_name}")
        finally:
            stop_task.cancel()
            try:
                await stop_task
            except asyncio.CancelledError:
                pass

    async def _cancel_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Cancel worker tasks during shutdown without hiding live failures."""
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error(
                    "runtime_task_cleanup_error",
                    task_name=task.get_name(),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

    async def _runtime_watchdog_loop(self) -> None:
        """Upsert loop heartbeats and exit the process when a loop goes stale."""
        while True:
            await asyncio.sleep(self.settings.watchdog_interval_seconds)
            try:
                await self._watchdog_check_once()
            except TraderComponentStaleError:
                raise
            except Exception as exc:
                logger.error(
                    "runtime_watchdog_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

    async def _watchdog_check_once(self) -> None:
        now_mono = time.monotonic()
        stale_component: str | None = None
        stale_age = 0.0
        for component in self._heartbeat_components:
            age_sec = now_mono - self._last_success[component]
            detail = {
                **self._heartbeat_detail.get(component, {}),
                "age_sec": round(age_sec, 2),
                "stale_limit_seconds": self.settings.stale_limit_seconds,
            }
            await self.repository.upsert_heartbeat(
                component=component,
                last_ok_at=self._last_success_wall[component],
                detail=detail,
            )
            if age_sec > self.settings.stale_limit_seconds and stale_component is None:
                stale_component = component
                stale_age = age_sec
        if stale_component is not None:
            logger.critical(
                "perp_component_stale",
                component=stale_component,
                age_sec=round(stale_age, 2),
                stale_limit_seconds=self.settings.stale_limit_seconds,
                account_id=self.settings.account_id,
            )
            error = TraderComponentStaleError(
                f"perp component stale: {stale_component} age={stale_age:.2f}s"
            )
            self._safe_send_sms_error(error)
            raise error

    def _mark_success(self, component: str, detail: dict[str, Any] | None = None) -> None:
        """Record that one runtime loop provably completed an iteration."""
        last_success = getattr(self, "_last_success", None)
        if last_success is None or component not in last_success:
            return
        last_success[component] = time.monotonic()
        self._last_success_wall[component] = datetime.now(tz=UTC)
        self._heartbeat_detail[component] = detail or {}

    async def _market_loop(self, strategies: list[Strategy]) -> None:
        while True:
            event: dict[str, Any] | None = None
            try:
                event = await self.exchange.market_events.get()
                if event["channel"] == "user_fills":
                    logger.info("fill_event_received", payload=event["payload"])
                    self._mark_success("market", detail={"event": "user_fills"})
                    continue
                symbol = event.get("symbol")
                if symbol not in {BTC_PERP, ETH_PERP}:
                    continue
                market_state = self._ingest_market_event(symbol, event)
                if self.settings.execution_mode == "paper_sim":
                    await self.executor.update_market_state(market_state)
                self._mark_success("market", detail={"symbol": symbol})
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error("market_loop_error", error=str(exc), error_type=type(exc).__name__)
                self._safe_send_sms_error(exc)
                await asyncio.sleep(1)
            finally:
                if event is not None:
                    self.exchange.market_events.task_done()

    async def _strategy_loop(self, strategies: list[Strategy]) -> None:
        symbols = sorted({symbol for strategy in strategies for symbol in strategy.symbols})
        while True:
            try:
                for symbol in symbols:
                    latest_market = self._market_cache().get(symbol)
                    if latest_market is None:
                        continue
                    if not self._price_is_fresh(latest_market):
                        continue
                    market_state = await self._build_strategy_market_state(symbol, latest_market)
                    if self.settings.execution_mode == "paper_sim":
                        await self.executor.update_market_state(market_state)
                    await self._run_strategies_for_symbol(symbol, market_state, strategies)
                    self._pending_trade_events[symbol] = []
                self._mark_success("strategy", detail={"symbols": symbols})
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error("strategy_loop_error", error=str(exc), error_type=type(exc).__name__)
                self._safe_send_sms_error(exc)
            await asyncio.sleep(1)

    async def _market_snapshot_loop(self) -> None:
        while True:
            try:
                if self.settings.market_data_writer:
                    market_states = list(self._market_cache().values())
                    for market_state in market_states:
                        await self._write_market_snapshot(market_state)
                    self._mark_success(
                        "market_data",
                        detail={"snapshot_count": len(market_states)},
                    )
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error(
                    "market_snapshot_loop_error",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                self._safe_send_sms_error(exc)
            await asyncio.sleep(5)

    async def _market_data_prune_loop(self) -> None:
        while True:
            try:
                if self.settings.market_data_writer:
                    cutoff = datetime.now(tz=UTC) - timedelta(hours=24)
                    deleted_count = await self.repository.delete_older_than(
                        "market_data_snapshots",
                        "created_at",
                        cutoff.isoformat(),
                    )
                    logger.info(
                        "market_data_snapshots_pruned",
                        deleted_count=deleted_count,
                        cutoff_iso=cutoff.isoformat(),
                    )
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error(
                    "market_data_prune_loop_error",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                self._safe_send_sms_error(exc)
            await asyncio.sleep(6 * 60 * 60)

    async def _run_strategies_for_symbol(
        self,
        symbol: str,
        market_state: MarketState,
        strategies: list[Strategy],
    ) -> None:
        if not self._price_is_fresh(market_state):
            return
        for strategy in strategies:
            if symbol not in strategy.symbols:
                continue
            signal_result = await strategy.on_tick(market_state.model_dump())
            if signal_result is None:
                continue
            equity = (
                self.executor.paper_equity_usd
                if self.settings.execution_mode == "paper_sim"
                else market_state.equity_usd or self.settings.starting_balance_usd
            )
            vol = await self._realized_daily_vol(symbol)
            await self.executor.execute_signal(signal_result, equity, vol)

    async def _snapshot_loop(self) -> None:
        while True:
            try:
                if not self._all_prices_fresh():
                    self._mark_success("equity", detail={"skipped": "stale_prices"})
                    await asyncio.sleep(60)
                    continue
                snapshot = await self._build_equity_snapshot()
                await self.repository.insert(
                    "equity_snapshots",
                    snapshot.model_dump(mode="json") | {"account_id": self.settings.account_id},
                )
                breaker_event = await self.circuit_breakers.evaluate(snapshot)
                if breaker_event and breaker_event.breaker_type in {"weekly_loss", "max_drawdown"}:
                    if self.settings.execution_mode == "paper_sim":
                        await self.executor.close_all_positions("paper circuit breaker")
                    else:
                        await self.exchange.close_all_positions()
                self._mark_success(
                    "equity",
                    detail={
                        "equity_usd": snapshot.equity_usd,
                        "mdd_pct": round(snapshot.mdd_pct, 6),
                        "high_water_equity": self.high_water_equity,
                    },
                )
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                logger.error("snapshot_loop_error", error=str(exc), error_type=type(exc).__name__)
                self._safe_send_sms_error(exc)
            self._log_http_calls_per_minute()
            await asyncio.sleep(60)

    async def _build_market_state(self, symbol: str, event: dict[str, Any]) -> MarketState:
        parsed = _parse_market_event(event)
        if self.settings.execution_mode == "paper_sim":
            equity = self.executor.paper_equity_usd
            positions = await self.executor.get_open_positions()
        else:
            self._record_http_call(2)
            account_state = await self.exchange.get_account_state()
            equity = float(account_state.get("marginSummary", {}).get("accountValue") or 0)
            positions = await self.exchange.get_open_positions()
        bars = await self._get_bars_5m(symbol, count=60)
        market_state = self._market_state_from_parsed(
            symbol=symbol,
            parsed=parsed,
            bars_5m=bars,
            trade_events=list(parsed.get("trade_events") or []),
            equity=equity,
            positions=positions,
        )
        self._market_cache()[symbol] = market_state
        return market_state

    def _ingest_market_event(self, symbol: str, event: dict[str, Any]) -> MarketState:
        """Parse a websocket event into memory without Supabase or HTTP calls."""
        parsed = _parse_market_event(event)
        trade_events = list(parsed.get("trade_events") or [])
        if trade_events:
            self._pending_trade_events.setdefault(symbol, []).extend(trade_events)
        market_state = self._market_state_from_parsed(
            symbol=symbol,
            parsed=parsed,
            bars_5m=[],
            trade_events=[],
            equity=None,
            positions=[],
        )
        self._market_cache()[symbol] = market_state
        return market_state

    async def _build_strategy_market_state(
        self,
        symbol: str,
        latest_market: MarketState,
    ) -> MarketState:
        """Enrich latest in-memory market data for the slower strategy loop."""
        if self.settings.execution_mode == "paper_sim":
            equity = self.executor.paper_equity_usd
            positions = await self.executor.get_open_positions()
        else:
            self._record_http_call(2)
            account_state = await self.exchange.get_account_state()
            equity = float(account_state.get("marginSummary", {}).get("accountValue") or 0)
            positions = await self.exchange.get_open_positions()
        bars = await self._get_bars_5m(symbol, count=60)
        market_state = MarketState(
            symbol=symbol,
            timestamp=latest_market.timestamp,
            bid=latest_market.bid,
            ask=latest_market.ask,
            mid=latest_market.mid,
            last_trade_price=latest_market.last_trade_price,
            volume_24h=latest_market.volume_24h,
            funding_rate=latest_market.funding_rate,
            open_interest=latest_market.open_interest,
            bars_5m=bars,
            trade_events=list(self._pending_trade_events.get(symbol) or []),
            bid_levels=latest_market.bid_levels,
            ask_levels=latest_market.ask_levels,
            book_bids=list(latest_market.bid_levels or []),
            book_asks=list(latest_market.ask_levels or []),
            equity_usd=equity,
            open_positions=positions,
        )
        self._market_cache()[symbol] = market_state
        return market_state

    def _market_state_from_parsed(
        self,
        symbol: str,
        parsed: dict[str, Any],
        bars_5m: list[dict[str, Any]],
        trade_events: list[dict[str, Any]],
        equity: float | None,
        positions: list[Any],
    ) -> MarketState:
        previous = self._market_cache().get(symbol)
        mid = parsed.get("mid") or parsed.get("last_trade_price")
        if mid is None and previous is not None:
            mid = previous.mid
        if mid is None:
            raise ValueError(f"market event missing price and no cached market state for {symbol}")

        bid = parsed.get("bid") or (previous.bid if previous is not None else None)
        ask = parsed.get("ask") or (previous.ask if previous is not None else None)
        if bid is None or ask is None:
            spread = mid * 0.0002
            bid = mid - spread / 2
            ask = mid + spread / 2

        bid_levels = parsed.get("bid_levels")
        ask_levels = parsed.get("ask_levels")
        if bid_levels is None and previous is not None:
            bid_levels = previous.bid_levels
        if ask_levels is None and previous is not None:
            ask_levels = previous.ask_levels
        return MarketState(
            symbol=symbol,
            bid=float(bid),
            ask=float(ask),
            mid=float(mid),
            last_trade_price=float(parsed.get("last_trade_price") or mid),
            bars_5m=bars_5m,
            trade_events=trade_events,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            book_bids=list(bid_levels or []),
            book_asks=list(ask_levels or []),
            equity_usd=equity,
            open_positions=positions,
        )

    def _market_cache(self) -> dict[str, MarketState]:
        """Return the latest-market cache, including manually constructed test runtimes."""
        if not hasattr(self, "_latest_market"):
            self._latest_market = getattr(self, "_last_market_state", {})
            self._last_market_state = self._latest_market
        return self._latest_market

    async def _write_market_snapshot(self, market_state: MarketState) -> None:
        last_written = self._last_market_write_source_timestamp.get(market_state.symbol)
        if last_written == market_state.timestamp:
            self._log_throttled(
                f"market_write_skipped_stale:{market_state.symbol}",
                "market_write_skipped_stale",
                symbol=market_state.symbol,
            )
            return
        payload = {
            "account_id": self.settings.account_id,
            "timestamp": market_state.timestamp.isoformat(),
            "symbol": market_state.symbol,
            "bid": round(market_state.bid, 2),
            "ask": round(market_state.ask, 2),
            "mid": round(market_state.mid, 2),
            "last_trade_price": round(market_state.last_trade_price, 2),
            "volume_24h": (
                round(market_state.volume_24h, 2)
                if market_state.volume_24h is not None
                else None
            ),
            "funding_rate": market_state.funding_rate,
            "open_interest": (
                round(market_state.open_interest, 2)
                if market_state.open_interest is not None
                else None
            ),
        }
        await self.repository.insert("market_data_snapshots", payload)
        self._last_market_write_source_timestamp[market_state.symbol] = market_state.timestamp

    async def _load_high_water_equity(self) -> None:
        """Rebuild the all-time equity peak so the drawdown breaker survives restarts."""
        high_water = self.settings.starting_balance_usd
        state = await self.repository.get_state(HIGH_WATER_STATE_KEY)
        if state is not None:
            raw_value = state.get("value")
            if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
                high_water = max(high_water, float(raw_value))
        rows = await self.repository.select_where(
            "equity_snapshots",
            filters={"execution_mode": self.settings.execution_mode},
            limit=1,
            order_column="equity_usd",
            desc=True,
        )
        if rows:
            raw_equity = rows[0].get("equity_usd")
            if raw_equity is not None:
                try:
                    high_water = max(high_water, float(raw_equity))
                except (TypeError, ValueError):
                    logger.warning(
                        "high_water_snapshot_value_invalid",
                        raw_equity=raw_equity,
                    )
        self.high_water_equity = high_water
        logger.info(
            "high_water_equity_loaded",
            high_water_equity=high_water,
            account_id=self.settings.account_id,
            execution_mode=self.settings.execution_mode,
        )

    async def _update_high_water_equity(self, equity: float) -> None:
        """Track and persist a new all-time equity peak."""
        if equity <= self.high_water_equity:
            return
        self.high_water_equity = equity
        await self.repository.upsert_state(
            HIGH_WATER_STATE_KEY,
            {
                "value": round(equity, 2),
                "execution_mode": self.settings.execution_mode,
                "updated_at": datetime.now(tz=UTC).isoformat(),
            },
        )
        logger.info(
            "high_water_equity_updated",
            high_water_equity=self.high_water_equity,
            account_id=self.settings.account_id,
        )

    async def _build_equity_snapshot(self) -> EquitySnapshot:
        if self.settings.execution_mode == "paper_sim":
            equity = self.executor.paper_equity_usd
            daily_pnl = await self._pnl_since(equity, datetime.now(tz=UTC) - timedelta(days=1))
            weekly_pnl = await self._pnl_since(equity, datetime.now(tz=UTC) - timedelta(days=7))
            await self._update_high_water_equity(equity)
            mdd = (
                (self.high_water_equity - equity) / self.high_water_equity
                if self.high_water_equity > 0
                else 0
            )
            return EquitySnapshot(
                account_id=self.settings.account_id,
                execution_mode=self.settings.execution_mode,
                balance_usd=self.executor.paper_balance_usd,
                equity_usd=equity,
                margin_used_usd=self.executor.paper_margin_used_usd,
                open_position_count=len(self.executor.open_positions),
                daily_pnl=daily_pnl,
                weekly_pnl=weekly_pnl,
                mdd_pct=mdd,
            )

        account_state = await self.exchange.get_account_state()
        self._record_http_call(2)
        margin_summary = account_state.get("marginSummary", {})
        equity = float(margin_summary.get("accountValue") or self.settings.starting_balance_usd)
        margin_used = float(margin_summary.get("totalMarginUsed") or 0)
        withdrawable = float(account_state.get("withdrawable") or equity)
        positions = await self.exchange.get_open_positions()
        await self._update_high_water_equity(equity)
        mdd = (
            (self.high_water_equity - equity) / self.high_water_equity
            if self.high_water_equity > 0
            else 0
        )
        daily_pnl = await self._pnl_since(equity, datetime.now(tz=UTC) - timedelta(days=1))
        weekly_pnl = await self._pnl_since(equity, datetime.now(tz=UTC) - timedelta(days=7))
        return EquitySnapshot(
            account_id=self.settings.account_id,
            execution_mode=self.settings.execution_mode,
            balance_usd=withdrawable,
            equity_usd=equity,
            margin_used_usd=margin_used,
            open_position_count=len(positions),
            daily_pnl=daily_pnl,
            weekly_pnl=weekly_pnl,
            mdd_pct=mdd,
        )

    async def _pnl_since(self, current_equity: float, since: datetime) -> float:
        rows = await self.repository.select_since(
            "equity_snapshots",
            since.isoformat(),
            limit=1,
            filters={
                "execution_mode": self.settings.execution_mode,
                "account_id": self.settings.account_id,
            },
        )
        if not rows:
            return current_equity - self.settings.starting_balance_usd
        start_equity = float(rows[0].get("equity_usd") or self.settings.starting_balance_usd)
        return current_equity - start_equity

    async def _fetch_recent_bars(self, symbol: str, count: int) -> list[dict[str, Any]]:
        if self._bars_cache_is_fresh(symbol):
            return self._bars_5m[symbol]

        end = datetime.now(tz=UTC)
        start = end - timedelta(minutes=5 * count)
        self._record_http_call()
        bars = await self.exchange.get_candles(
            symbol=symbol,
            interval="5m",
            start_ms=int(start.timestamp() * 1000),
            end_ms=int(end.timestamp() * 1000),
        )
        self._bars_5m[symbol] = bars
        self._bars_last_fetch[symbol] = end
        return bars

    async def _get_bars_5m(self, symbol: str, count: int) -> list[dict[str, Any]]:
        if self._bars_cache_is_fresh(symbol):
            return self._bars_5m[symbol]
        return await self._fetch_recent_bars(symbol, count)

    def _bars_cache_is_fresh(self, symbol: str) -> bool:
        last_fetch = self._bars_last_fetch.get(symbol)
        return (
            symbol in self._bars_5m
            and last_fetch is not None
            and (datetime.now(tz=UTC) - last_fetch).total_seconds() < 60
        )

    def _record_http_call(self, count: int = 1) -> None:
        self._http_calls_this_minute += count

    def _log_http_calls_per_minute(self) -> None:
        logger.info("http_calls_per_minute", count=self._http_calls_this_minute)
        self._http_calls_this_minute = 0

    def _price_is_fresh(self, market_state: MarketState) -> bool:
        age_sec = self._market_age_sec(market_state)
        if age_sec <= self.settings.stale_threshold_sec:
            return True
        self._log_throttled(
            f"trading_halted_stale_price:{market_state.symbol}",
            "trading_halted_stale_price",
            symbol=market_state.symbol,
            age_sec=round(age_sec, 2),
            stale_threshold_sec=self.settings.stale_threshold_sec,
        )
        return False

    def _all_prices_fresh(self) -> bool:
        market_states = list(self._market_cache().values())
        if not market_states:
            self._log_throttled(
                "trading_halted_stale_price:no_market_data",
                "trading_halted_stale_price",
                symbol="all",
                age_sec=None,
                stale_threshold_sec=self.settings.stale_threshold_sec,
            )
            return False
        return all(self._price_is_fresh(market_state) for market_state in market_states)

    def _market_age_sec(self, market_state: MarketState) -> float:
        timestamp = (
            market_state.timestamp
            if market_state.timestamp.tzinfo is not None
            else market_state.timestamp.replace(tzinfo=UTC)
        )
        return max(0.0, (datetime.now(tz=UTC) - timestamp).total_seconds())

    def _log_throttled(self, key: str, event: str, **fields: Any) -> None:
        now = time.monotonic()
        last_logged_at = self._throttled_log_at.get(key, 0.0)
        if now - last_logged_at < STALE_LOG_THROTTLE_SEC:
            return
        logger.warning(event, **fields)
        self._throttled_log_at[key] = now

    async def _realized_daily_vol(self, symbol: str) -> float:
        bars = await self._fetch_recent_bars(symbol, count=288)
        closes = np.array([float(bar["c"]) for bar in bars], dtype=float)
        if closes.size < 2:
            return 0.02
        returns = np.diff(np.log(closes))
        return float(np.std(returns) * np.sqrt(288))

    def _load_strategies(self) -> list[Strategy]:
        strategies: list[Strategy] = []
        for name in self.settings.enabled_strategy_names:
            strategy_cls = STRATEGY_REGISTRY.get(name)
            if strategy_cls is None:
                logger.warning("strategy_unknown", strategy_name=name)
                continue
            if name in SCALP_STRATEGY_NAMES:
                strategy = strategy_cls(  # type: ignore[call-arg]
                    scalp_mode_enabled=self.settings.scalp_mode_enabled,
                    cooldown_seconds=self.settings.scalp_cooldown_seconds,
                )
            else:
                strategy = strategy_cls()
            if self.settings.chaos_mode:
                strategy = ChaosStrategyWrapper(strategy)
                logger.info(
                    "strategy_chaos_wrapped",
                    strategy_name=name,
                    account_id=self.settings.account_id,
                )
            strategies.append(strategy)
            logger.info("strategy_loaded", strategy_name=name)
        return strategies

    async def _shutdown(self) -> None:
        self.exchange.disconnect()
        try:
            await self.sms.stop()
        except Exception as exc:
            logger.warning(
                "sms_stop_failed",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        logger.info(
            "runtime_shutdown_complete",
            close_positions_on_shutdown=self.settings.close_positions_on_shutdown,
        )

    def _safe_send_sms_error(self, exc: Exception) -> None:
        try:
            self.sms.send_error(exc)
        except Exception as sms_exc:
            logger.warning(
                "sms_error_notification_failed",
                original_error_type=type(exc).__name__,
                error_type=type(sms_exc).__name__,
                error_message=str(sms_exc),
            )

    def _log_startup_banner(self) -> None:
        if self.settings.execution_mode == "paper_sim":
            market_data = "Hyperliquid MAINNET (read-only)"
            fills = "Simulated locally"
            balance = f"${self.settings.starting_balance_usd:.2f} USD (paper)"
        else:
            market_data = "Hyperliquid TESTNET"
            fills = "Real orders on Hyperliquid testnet"
            balance = "Hyperliquid testnet account"
        logger.warning(
            "execution_mode_banner",
            banner=(
                "\n=======================================\n"
                f" EXECUTION MODE: {self.settings.execution_mode.upper()}\n"
                f" Market data: {market_data}\n"
                f" Fills: {fills}\n"
                f" Starting balance: {balance}\n"
                "======================================="
            ),
            execution_mode=self.settings.execution_mode,
        )
        logger.info(
            "tournament_account_loaded",
            account_id=self.settings.account_id,
            personality=self.settings.personality,
        )


async def main() -> None:
    """Async entrypoint."""
    runtime = TraderRuntime()
    loop = asyncio.get_running_loop()
    for signame in ("SIGTERM", "SIGINT"):
        loop.add_signal_handler(getattr(signal, signame), runtime.request_shutdown)
    await runtime.run()


def _parse_market_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict) and "levels" in data:
        levels = data.get("levels") or []
        if len(levels) >= 2 and levels[0] and levels[1]:
            bid = float(levels[0][0]["px"])
            ask = float(levels[1][0]["px"])
            return {
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) / 2,
                "bid_levels": list(levels[0]),
                "ask_levels": list(levels[1]),
            }
    if isinstance(data, list) and data:
        last = data[-1]
        if isinstance(last, dict) and "px" in last:
            return {"last_trade_price": float(last["px"]), "trade_events": data}
    if isinstance(data, dict) and "px" in data:
        return {"last_trade_price": float(data["px"]), "trade_events": [data]}
    return {}


if __name__ == "__main__":
    asyncio.run(main())
