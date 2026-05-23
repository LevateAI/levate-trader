"""Async wrapper around the official Hyperliquid Python SDK."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import eth_account
import structlog
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from src.config import Settings
from src.models import OrderType, Position, Side

logger = structlog.get_logger(__name__)

SYMBOL_TO_COIN = {"BTC-PERP": "BTC", "ETH-PERP": "ETH"}
COIN_TO_SYMBOL = {value: key for key, value in SYMBOL_TO_COIN.items()}


class HyperliquidClient:
    """Async facade for Hyperliquid testnet trading and market data."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base_url = (
            constants.TESTNET_API_URL
            if settings.hyperliquid_testnet
            else constants.MAINNET_API_URL
        )
        if not settings.hyperliquid_testnet:
            raise ValueError("This bot is paper-trading only and refuses mainnet endpoints")

        self._wallet = eth_account.Account.from_key(settings.hyperliquid_private_key)
        self._info: Info | None = None
        self._exchange: Exchange | None = None
        self._subscriptions: list[tuple[dict[str, Any], Callable[[Any], None]]] = []
        self.market_events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=10_000)

    async def connect_ws(self) -> None:
        """Create SDK clients with websocket support and configure account leverage."""
        logger.info("hyperliquid_connecting", base_url=self._base_url)
        await asyncio.to_thread(self._connect_sync)
        for coin in SYMBOL_TO_COIN.values():
            await asyncio.to_thread(self._exchange_required().update_leverage, 10, coin, True)
        logger.info("hyperliquid_connected")

    async def reconnect_with_backoff(self) -> None:
        """Reconnect websocket subscriptions with exponential backoff."""
        delay = 1.0
        while True:
            try:
                self.disconnect()
                await self.connect_ws()
                for subscription, callback in list(self._subscriptions):
                    await asyncio.to_thread(self._info_required().subscribe, subscription, callback)
                logger.info("hyperliquid_reconnected")
                return
            except (OSError, RuntimeError, ValueError) as exc:
                logger.error("hyperliquid_reconnect_failed", error=str(exc), delay=delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def subscribe_to_book(self, symbol: str) -> None:
        """Subscribe to L2 book updates."""
        coin = self._coin(symbol)
        subscription = {"type": "l2Book", "coin": coin}
        callback = self._queue_callback("book", symbol)
        self._subscriptions.append((subscription, callback))
        await asyncio.to_thread(self._info_required().subscribe, subscription, callback)
        logger.info("hyperliquid_subscribed_book", symbol=symbol)

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trades."""
        coin = self._coin(symbol)
        subscription = {"type": "trades", "coin": coin}
        callback = self._queue_callback("trades", symbol)
        self._subscriptions.append((subscription, callback))
        await asyncio.to_thread(self._info_required().subscribe, subscription, callback)
        logger.info("hyperliquid_subscribed_trades", symbol=symbol)

    async def subscribe_to_user_fills(self) -> None:
        """Subscribe to user fill events."""
        subscription = {"type": "userFills", "user": self._settings.hyperliquid_account_address}
        callback = self._queue_callback("user_fills", None)
        self._subscriptions.append((subscription, callback))
        await asyncio.to_thread(self._info_required().subscribe, subscription, callback)
        logger.info("hyperliquid_subscribed_user_fills")

    async def get_account_state(self) -> dict[str, Any]:
        """Fetch account clearinghouse state."""
        logger.info("hyperliquid_get_account_state")
        return await asyncio.to_thread(
            self._info_required().user_state,
            self._settings.hyperliquid_account_address,
        )

    async def get_open_positions(self) -> list[Position]:
        """Return normalized open perp positions."""
        state = await self.get_account_state()
        positions: list[Position] = []
        for item in state.get("assetPositions", []):
            raw_position = item.get("position", {})
            size = float(raw_position.get("szi") or 0)
            if size == 0:
                continue
            coin = raw_position.get("coin", "")
            symbol = COIN_TO_SYMBOL.get(coin, f"{coin}-PERP")
            positions.append(
                Position(
                    symbol=symbol,
                    side=Side.LONG if size > 0 else Side.SHORT,
                    size=abs(size),
                    entry_price=float(raw_position.get("entryPx") or 0),
                    liquidation_price=_maybe_float(raw_position.get("liquidationPx")),
                    unrealized_pnl=float(raw_position.get("unrealizedPnl") or 0),
                    leverage=float(raw_position.get("leverage", {}).get("value") or 1),
                    strategy_name="external",
                )
            )
        logger.info("hyperliquid_open_positions_loaded", count=len(positions))
        return positions

    async def place_order(
        self,
        symbol: str,
        side: Side,
        size: float,
        price: float,
        order_type: OrderType,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        """Place an order through Hyperliquid."""
        coin = self._coin(symbol)
        is_buy = side in {Side.BUY, Side.LONG}
        hl_order_type = (
            {"limit": {"tif": "Gtc"}}
            if order_type == OrderType.LIMIT
            else {"limit": {"tif": "Ioc"}}
        )
        logger.info(
            "hyperliquid_place_order",
            symbol=symbol,
            side=str(side),
            size=size,
            price=price,
            order_type=str(order_type),
            reduce_only=reduce_only,
        )
        response = await asyncio.to_thread(
            self._exchange_required().order,
            coin,
            is_buy,
            size,
            price,
            hl_order_type,
            reduce_only,
        )
        return dict(response)

    async def cancel_order(self, symbol: str, oid: int) -> dict[str, Any]:
        """Cancel an open order."""
        logger.info("hyperliquid_cancel_order", symbol=symbol, oid=oid)
        response = await asyncio.to_thread(self._exchange_required().cancel, self._coin(symbol), oid)
        return dict(response)

    async def close_position(self, symbol: str) -> dict[str, Any] | None:
        """Submit a reduce-only market close for a symbol if a position exists."""
        logger.warning("hyperliquid_close_position", symbol=symbol)
        response = await asyncio.to_thread(self._exchange_required().market_close, self._coin(symbol))
        return dict(response) if response is not None else None

    async def close_all_positions(self) -> list[dict[str, Any]]:
        """Close all currently open supported positions."""
        responses: list[dict[str, Any]] = []
        for position in await self.get_open_positions():
            if position.symbol not in SYMBOL_TO_COIN:
                logger.warning("hyperliquid_skip_unsupported_close", symbol=position.symbol)
                continue
            response = await self.close_position(position.symbol)
            if response is not None:
                responses.append(response)
        logger.warning("hyperliquid_close_all_positions_complete", count=len(responses))
        return responses

    async def get_mid_price(self, symbol: str) -> float:
        """Fetch the current mid price for a symbol."""
        coin = self._coin(symbol)
        mids = await asyncio.to_thread(self._info_required().all_mids)
        return float(mids[coin])

    async def get_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> list[dict[str, Any]]:
        """Fetch historical candles."""
        candles = await asyncio.to_thread(
            self._info_required().candles_snapshot,
            self._coin(symbol),
            interval,
            start_ms,
            end_ms,
        )
        return list(candles)

    def disconnect(self) -> None:
        """Disconnect websocket resources."""
        if self._info is not None:
            try:
                self._info.disconnect_websocket()
            except RuntimeError as exc:
                logger.warning("hyperliquid_disconnect_failed", error=str(exc))

    def _connect_sync(self) -> None:
        self._info = Info(self._base_url, skip_ws=False)
        self._exchange = Exchange(
            self._wallet,
            self._base_url,
            account_address=self._settings.hyperliquid_account_address,
        )

    def _queue_callback(self, channel: str, symbol: str | None) -> Callable[[Any], None]:
        loop = asyncio.get_running_loop()

        def callback(message: Any) -> None:
            event = {"channel": channel, "symbol": symbol, "payload": message}
            loop.call_soon_threadsafe(self._safe_queue_event, event)

        return callback

    def _safe_queue_event(self, event: dict[str, Any]) -> None:
        try:
            self.market_events.put_nowait(event)
        except asyncio.QueueFull:
            logger.error(
                "hyperliquid_market_queue_full",
                channel=event.get("channel"),
                symbol=event.get("symbol"),
            )

    def _info_required(self) -> Info:
        if self._info is None:
            raise RuntimeError("Hyperliquid websocket client is not connected")
        return self._info

    def _exchange_required(self) -> Exchange:
        if self._exchange is None:
            raise RuntimeError("Hyperliquid exchange client is not connected")
        return self._exchange

    @staticmethod
    def _coin(symbol: str) -> str:
        if symbol not in SYMBOL_TO_COIN:
            raise ValueError(f"unsupported symbol: {symbol}")
        return SYMBOL_TO_COIN[symbol]


def _maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
