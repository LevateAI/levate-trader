"""Local paper-simulation executor using live read-only market data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import structlog

from src.alerts.discord_notifier import DiscordNotifier
from src.alerts.twilio_notifier import TwilioNotifier
from src.config import Settings
from src.db.supabase_client import SupabaseRepository
from src.exchange.hyperliquid_client import HyperliquidClient
from src.models import MarketState, OrderType, Position, Side, Signal, Trade, TradeStatus
from src.risk.circuit_breakers import CircuitBreakerManager
from src.risk.position_sizer import calculate_position_size
from src.strategies.scalp_common import is_scalp_strategy_name

logger = structlog.get_logger(__name__)

TAKER_FEE_RATE = 0.00045
MAKER_FEE_RATE = 0.00015
MAX_FILL_MARKET_AGE_SECONDS = 10
MAX_FILL_SPREAD_PCT = 0.05


@dataclass(slots=True)
class PaperPosition:
    """In-memory paper position state."""

    symbol: str
    side: Side
    size: float
    entry_price: float
    entry_time: datetime
    leverage: float
    stop_loss: float | None
    take_profit: float | None
    strategy_name: str
    fees_paid: float
    current_unrealized_pnl: float
    max_hold_minutes: int | None = None


@dataclass(slots=True)
class PendingPaperOrder:
    """Pending paper limit order."""

    oid: int
    symbol: str
    side: Side
    size: float
    price: float
    signal: Signal | None
    created_at: datetime


class PaperExecutor:
    """Simulate fills locally while consuming Hyperliquid mainnet market data."""

    def __init__(
        self,
        exchange: HyperliquidClient | None,
        repository: SupabaseRepository | None,
        circuit_breakers: CircuitBreakerManager,
        discord: DiscordNotifier,
        sms: TwilioNotifier | None,
        settings: Settings,
    ) -> None:
        self._exchange = exchange
        self._repository = repository
        self._circuit_breakers = circuit_breakers
        self._discord = discord
        self._sms = sms
        self._settings = settings
        self.paper_balance_usd = _money(settings.starting_balance_usd)
        self.open_positions: dict[str, PaperPosition] = {}
        self.pending_orders: dict[int, PendingPaperOrder] = {}
        self._latest_market: dict[str, MarketState] = {}
        self._open_trades: dict[str, Trade] = {}
        self._trade_ids_by_symbol: dict[str, str] = {}
        self._last_position_row_update: dict[str, datetime] = {}
        self._next_oid = 1

    @property
    def paper_equity_usd(self) -> float:
        """Return paper balance plus live unrealized PnL."""
        return _money(
            self.paper_balance_usd
            + sum(position.current_unrealized_pnl for position in self.open_positions.values())
        )

    @property
    def paper_margin_used_usd(self) -> float:
        """Return rough paper margin usage."""
        return _money(
            sum(
                (position.entry_price * position.size) / max(position.leverage, 1)
                for position in self.open_positions.values()
            )
        )

    async def restore_state(self) -> None:
        """Restore open paper trades and positions from Supabase after restart."""
        if self._repository is None:
            logger.info("paper_restore_skipped", reason="repository_not_configured")
            return

        open_trade_rows = await self._repository.select_where(
            "trades",
            filters={
                "status": TradeStatus.OPEN.value,
                "execution_mode": self._settings.execution_mode,
            },
            limit=1000,
            order_column="timestamp",
            desc=False,
        )
        latest_snapshot_rows = await self._repository.select_where(
            "equity_snapshots",
            filters={"execution_mode": self._settings.execution_mode},
            limit=1,
            order_column="timestamp",
            desc=True,
        )

        restored_count = 0
        restored_entry_fees = 0.0
        for trade_row in open_trade_rows:
            trade = self._trade_from_row(trade_row)
            position_rows = await self._repository.select_where(
                "positions",
                filters={
                    "id": str(trade.id),
                    "execution_mode": self._settings.execution_mode,
                },
                limit=1,
                order_column="timestamp",
                desc=True,
            )
            if not position_rows:
                logger.error(
                    "paper_restore_position_missing",
                    trade_id=str(trade.id),
                    symbol=trade.symbol,
                )
                continue

            paper_position = self._paper_position_from_rows(trade, position_rows[0])
            if paper_position.symbol in self.open_positions:
                logger.error(
                    "paper_restore_duplicate_symbol_skipped",
                    trade_id=str(trade.id),
                    symbol=paper_position.symbol,
                )
                continue

            self.open_positions[paper_position.symbol] = paper_position
            self._open_trades[str(trade.id)] = trade
            self._trade_ids_by_symbol[paper_position.symbol] = str(trade.id)
            self._last_position_row_update[paper_position.symbol] = datetime.now(tz=UTC)
            restored_entry_fees += paper_position.fees_paid
            restored_count += 1

        if latest_snapshot_rows:
            self.paper_balance_usd = _money(float(latest_snapshot_rows[0]["balance_usd"]))
        else:
            self.paper_balance_usd = max(
                0.0,
                _money(self._settings.starting_balance_usd - restored_entry_fees),
            )

        logger.info(
            "paper_state_restored",
            restored_count=restored_count,
            open_trade_count=len(open_trade_rows),
            paper_balance_usd=self.paper_balance_usd,
            execution_mode=self._settings.execution_mode,
        )

    async def execute_signal(
        self,
        signal: Signal,
        equity: float,
        asset_realized_vol: float,
    ) -> dict[str, Any] | None:
        """Check risk, size the signal, and simulate an order."""
        await self._log_strategy_signal(signal, action_taken="received")
        if signal.reduce_only:
            trade = await self.close_position(signal.symbol, signal.entry_price, signal.reasoning)
            await self._log_strategy_signal(signal, action_taken="paper_exit_filled")
            return {"status": "filled", "trade_id": str(trade.id)} if trade else None

        if not self._circuit_breakers.can_open_new_entries():
            logger.warning(
                "paper_signal_rejected_circuit_breaker",
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
            )
            await self._log_strategy_signal(signal, action_taken="rejected_circuit_breaker")
            return None

        usd_notional = calculate_position_size(
            equity=equity,
            signal_confidence=signal.confidence,
            asset_realized_vol=asset_realized_vol,
            stop_distance=signal.stop_distance_pct,
        )
        requested_notional = signal.size_pct_equity * equity
        usd_notional = min(usd_notional, requested_notional)
        if usd_notional <= 0:
            await self._log_strategy_signal(signal, action_taken="rejected_zero_size")
            return None

        asset_size = round(usd_notional / signal.entry_price, 6)
        order_side = Side.BUY if signal.side == Side.LONG else Side.SELL
        result = await self.place_order(
            symbol=signal.symbol,
            side=order_side,
            size=asset_size,
            price=signal.entry_price,
            order_type=OrderType.LIMIT,
            reduce_only=False,
            signal=signal,
        )
        await self._log_strategy_signal(signal, action_taken=str(result["status"]))
        return result

    async def place_order(
        self,
        symbol: str,
        side: Side,
        size: float,
        price: float,
        order_type: OrderType,
        reduce_only: bool = False,
        signal: Signal | None = None,
    ) -> dict[str, Any]:
        """Simulate a market or limit order."""
        if size <= 0:
            raise ValueError("paper order size must be positive")
        if reduce_only:
            trade = await self.close_position(symbol, price, "paper reduce-only close")
            return {"status": "filled", "trade_id": str(trade.id)} if trade else {"status": "skipped"}

        if len(self.pending_orders) >= self._settings.paper_max_pending_orders:
            logger.warning("paper_pending_order_cap_reached", cap=self._settings.paper_max_pending_orders)
            return {"status": "rejected", "reason": "pending_order_cap"}
        if symbol in self.open_positions:
            logger.warning("paper_order_rejected_existing_position", symbol=symbol)
            return {"status": "rejected", "reason": "existing_position"}

        if order_type == OrderType.MARKET:
            fill_price = self._market_fill_price(symbol, side)
            if fill_price is None:
                return {"status": "rejected", "reason": "invalid_market_state"}
            trade = await self._open_position(
                symbol=symbol,
                side=side,
                size=size,
                fill_price=fill_price,
                fee_rate=TAKER_FEE_RATE,
                signal=signal,
            )
            return {"status": "filled", "trade_id": str(trade.id), "fill_price": fill_price}

        oid = self._next_oid
        self._next_oid += 1
        order = PendingPaperOrder(
            oid=oid,
            symbol=symbol,
            side=side,
            size=size,
            price=price,
            signal=signal,
            created_at=datetime.now(tz=UTC),
        )
        self.pending_orders[oid] = order
        logger.info("paper_limit_order_pending", oid=oid, symbol=symbol, side=side.value, price=price)
        await self._check_pending_order(order, self._latest_market.get(symbol))
        return {"status": "pending", "oid": oid}

    async def update_market_state(self, market_state: MarketState) -> None:
        """Update latest market state and evaluate paper orders/positions."""
        self._latest_market[market_state.symbol] = market_state
        await self._check_pending_orders(market_state.symbol, market_state)
        await self._check_position_exit(
            market_state.symbol,
            market_state.last_trade_price,
            market_state.timestamp,
        )
        self._update_unrealized(market_state.symbol, market_state.last_trade_price)
        await self._maybe_update_position_row(market_state.symbol)

    async def on_fill(self, signal: Signal, fill: dict[str, Any]) -> Trade:
        """Create a paper position from an externally supplied fill-like event."""
        size = float(fill.get("sz") or fill.get("size") or 0)
        fill_price = float(fill.get("px") or fill.get("price") or signal.entry_price)
        if size <= 0:
            raise ValueError("paper fill size must be positive")
        side = Side.BUY if signal.side == Side.LONG else Side.SELL
        return await self._open_position(
            symbol=signal.symbol,
            side=side,
            size=size,
            fill_price=fill_price,
            fee_rate=MAKER_FEE_RATE,
            signal=signal,
        )

    async def get_open_positions(self) -> list[Position]:
        """Return paper positions using the shared Position model."""
        positions: list[Position] = []
        for symbol, paper_position in self.open_positions.items():
            trade_id = self._trade_ids_by_symbol.get(symbol)
            payload: dict[str, Any] = {
                "symbol": symbol,
                "side": paper_position.side,
                "size": paper_position.size,
                "entry_price": paper_position.entry_price,
                "unrealized_pnl": _money(paper_position.current_unrealized_pnl),
                "leverage": paper_position.leverage,
                "strategy_name": paper_position.strategy_name,
                "stop_loss": paper_position.stop_loss,
                "take_profit": paper_position.take_profit,
            }
            if trade_id:
                payload["id"] = UUID(trade_id)
            positions.append(Position(**payload))
        return positions

    async def close_position(
        self,
        symbol: str,
        trigger_price: float,
        reason_exit: str,
    ) -> Trade | None:
        """Close a paper position at an adverse-slippage exit price."""
        position = self.open_positions.get(symbol)
        if position is None:
            logger.info("paper_close_skipped_no_position", symbol=symbol)
            return None

        exit_price = self._exit_fill_price(position, trigger_price)
        trade_id = self._trade_ids_by_symbol.get(symbol)
        if trade_id is None:
            raise RuntimeError(f"paper position has no tracked trade id: {symbol}")
        trade = self._open_trades[trade_id]
        exit_notional = exit_price * position.size
        exit_fee = _money(exit_notional * TAKER_FEE_RATE)
        gross_pnl = _gross_pnl(position.side, position.entry_price, exit_price, position.size)
        total_fees = _money(position.fees_paid + exit_fee)
        pnl_usd = _money(gross_pnl - total_fees)

        self.paper_balance_usd = max(0.0, _money(self.paper_balance_usd + gross_pnl - exit_fee))
        trade.exit_price = _money(exit_price)
        trade.pnl_usd = pnl_usd
        trade.pnl_pct = pnl_usd / (trade.entry_price * trade.size) if trade.entry_price * trade.size else 0
        trade.fees_usd = total_fees
        trade.hold_duration_sec = int((datetime.now(tz=UTC) - trade.timestamp).total_seconds())
        trade.reason_exit = reason_exit
        trade.status = TradeStatus.CLOSED

        payload = self._trade_payload(trade)
        if self._repository is not None:
            await self._repository.update("trades", str(trade.id), payload)
            await self._repository.delete("positions", str(trade.id))

        self.open_positions.pop(symbol, None)
        self._trade_ids_by_symbol.pop(symbol, None)
        self._open_trades.pop(trade_id, None)
        self._last_position_row_update.pop(symbol, None)

        await self._discord.send(
            (
                f"PAPER CLOSE {trade.symbol} {trade.side.value} | PnL: "
                f"${trade.pnl_usd:.2f} ({trade.pnl_pct:.2%})"
            ),
            trade_id=str(trade.id),
        )
        if self._sms is not None:
            self._sms.send_trade_closed(payload)
        logger.info("paper_trade_closed", trade_id=str(trade.id), pnl_usd=trade.pnl_usd)
        return trade

    async def close_trade(
        self,
        trade_id: UUID,
        exit_price: float,
        reason_exit: str,
        fees_usd: float = 0.0,
    ) -> Trade:
        """Close a tracked paper trade by id."""
        symbol = next(
            (
                candidate_symbol
                for candidate_symbol, candidate_trade_id in self._trade_ids_by_symbol.items()
                if candidate_trade_id == str(trade_id)
            ),
            None,
        )
        if symbol is None:
            raise KeyError(f"paper trade is not tracked: {trade_id}")
        trade = await self.close_position(symbol, exit_price, reason_exit)
        if trade is None:
            raise KeyError(f"paper trade is not open: {trade_id}")
        if fees_usd:
            trade.fees_usd = _money((trade.fees_usd or 0) + fees_usd)
        return trade

    async def close_all_positions(self, reason_exit: str) -> list[Trade]:
        """Close every open paper position at the latest available price."""
        closed: list[Trade] = []
        for symbol, position in list(self.open_positions.items()):
            market_state = self._latest_market.get(symbol)
            trigger_price = (
                market_state.last_trade_price if market_state is not None else position.entry_price
            )
            trade = await self.close_position(symbol, trigger_price, reason_exit)
            if trade is not None:
                closed.append(trade)
        logger.warning("paper_close_all_positions_complete", count=len(closed))
        return closed

    async def _open_position(
        self,
        symbol: str,
        side: Side,
        size: float,
        fill_price: float,
        fee_rate: float,
        signal: Signal | None,
    ) -> Trade:
        position_side = Side.LONG if side in {Side.BUY, Side.LONG} else Side.SHORT
        notional = fill_price * size
        fee = _money(notional * fee_rate)
        if self.paper_balance_usd - fee < 0:
            logger.warning("paper_order_rejected_insufficient_fee_balance", symbol=symbol, fee=fee)
            raise ValueError("paper balance cannot cover simulated fees")

        self.paper_balance_usd = _money(self.paper_balance_usd - fee)
        trade = Trade(
            strategy_name=signal.strategy_name if signal else "manual_paper",
            symbol=symbol,
            side=position_side,
            size=size,
            entry_price=_money(fill_price),
            fees_usd=fee,
            reason_entry=signal.reasoning if signal else "manual paper simulated order",
        )
        max_hold_minutes = (
            self._settings.scalp_max_hold_minutes
            if signal is not None and is_scalp_strategy_name(signal.strategy_name)
            else None
        )
        paper_position = PaperPosition(
            symbol=symbol,
            side=position_side,
            size=size,
            entry_price=_money(fill_price),
            entry_time=trade.timestamp,
            leverage=10,
            stop_loss=signal.stop_loss if signal else None,
            take_profit=signal.take_profit if signal else None,
            strategy_name=trade.strategy_name,
            fees_paid=fee,
            current_unrealized_pnl=0.0,
            max_hold_minutes=max_hold_minutes,
        )
        self.open_positions[symbol] = paper_position
        self._trade_ids_by_symbol[symbol] = str(trade.id)
        self._open_trades[str(trade.id)] = trade

        trade_payload = self._trade_payload(trade)
        position_payload = self._position_payload(trade.id, paper_position)
        if self._repository is not None:
            await self._repository.insert("trades", trade_payload)
            await self._repository.insert("positions", position_payload)

        await self._discord.send(
            (
                f"PAPER OPEN {trade.symbol} {trade.side.value} {trade.size:.6f} @ "
                f"{trade.entry_price:.2f} | {trade.strategy_name}"
            ),
            trade_id=str(trade.id),
        )
        if self._sms is not None:
            self._sms.send_trade_opened(
                trade_payload
                | {
                    "stop_loss": paper_position.stop_loss,
                    "take_profit": paper_position.take_profit,
                }
            )
        logger.info("paper_trade_opened", trade_id=str(trade.id), symbol=symbol)
        return trade

    async def _check_pending_orders(self, symbol: str, market_state: MarketState) -> None:
        for order in list(self.pending_orders.values()):
            if order.symbol == symbol:
                await self._check_pending_order(order, market_state)

    async def _check_pending_order(
        self,
        order: PendingPaperOrder,
        market_state: MarketState | None,
    ) -> None:
        if market_state is None:
            return
        trade_price = market_state.last_trade_price
        crossed = (
            trade_price <= order.price
            if order.side in {Side.BUY, Side.LONG}
            else trade_price >= order.price
        )
        if not crossed:
            return
        if self._market_state_fill_rejection_reason(market_state) is not None:
            return
        self.pending_orders.pop(order.oid, None)
        trade = await self._open_position(
            symbol=order.symbol,
            side=order.side,
            size=order.size,
            fill_price=order.price,
            fee_rate=MAKER_FEE_RATE,
            signal=order.signal,
        )
        logger.info("paper_limit_order_filled", oid=order.oid, trade_id=str(trade.id))

    async def _check_position_exit(
        self,
        symbol: str,
        price: float,
        now: datetime | None = None,
    ) -> None:
        position = self.open_positions.get(symbol)
        if position is None:
            return
        if position.side == Side.LONG:
            if position.stop_loss is not None and price <= position.stop_loss:
                await self.close_position(symbol, position.stop_loss, "paper stop loss triggered")
                return
            if position.take_profit is not None and price >= position.take_profit:
                await self.close_position(symbol, position.take_profit, "paper take profit triggered")
                return
        if position.side == Side.SHORT:
            if position.stop_loss is not None and price >= position.stop_loss:
                await self.close_position(symbol, position.stop_loss, "paper stop loss triggered")
                return
            if position.take_profit is not None and price <= position.take_profit:
                await self.close_position(symbol, position.take_profit, "paper take profit triggered")
                return
        if position.max_hold_minutes is None:
            return
        current_time = now or datetime.now(tz=UTC)
        max_hold = timedelta(minutes=position.max_hold_minutes)
        if position.entry_time + max_hold <= current_time:
            await self.close_position(symbol, price, "scalp max hold time reached")

    def _update_unrealized(self, symbol: str, price: float) -> None:
        position = self.open_positions.get(symbol)
        if position is None:
            return
        position.current_unrealized_pnl = _money(
            _gross_pnl(position.side, position.entry_price, price, position.size)
        )

    async def _maybe_update_position_row(self, symbol: str) -> None:
        if self._repository is None:
            return
        position = self.open_positions.get(symbol)
        trade_id = self._trade_ids_by_symbol.get(symbol)
        if position is None or trade_id is None:
            return
        now = datetime.now(tz=UTC)
        last_update = self._last_position_row_update.get(symbol)
        if last_update is not None and (now - last_update).total_seconds() < 5:
            return
        await self._repository.update("positions", trade_id, self._position_payload(UUID(trade_id), position))
        self._last_position_row_update[symbol] = now

    def _market_fill_price(self, symbol: str, side: Side) -> float | None:
        market_state = self._latest_market.get(symbol)
        if market_state is None:
            logger.warning(
                "paper_fill_rejected_invalid_market",
                symbol=symbol,
                reason="missing_market",
            )
            return None
        if self._market_state_fill_rejection_reason(market_state) is not None:
            return None
        slippage = self._settings.paper_slippage_bps / 10_000
        if side in {Side.BUY, Side.LONG}:
            return _money(market_state.ask * (1 + slippage))
        return _money(market_state.bid * (1 - slippage))

    def _market_state_fill_rejection_reason(self, market_state: MarketState) -> str | None:
        now = datetime.now(tz=UTC)
        market_timestamp = (
            market_state.timestamp
            if market_state.timestamp.tzinfo
            else market_state.timestamp.replace(tzinfo=UTC)
        )
        market_age_sec = abs((now - market_timestamp).total_seconds())
        if market_age_sec > MAX_FILL_MARKET_AGE_SECONDS:
            logger.warning(
                "paper_fill_rejected_stale_market",
                symbol=market_state.symbol,
                market_age_sec=round(market_age_sec, 2),
                max_age_sec=MAX_FILL_MARKET_AGE_SECONDS,
            )
            return "stale_market"
        if market_state.bid <= 0 or market_state.ask <= 0:
            logger.warning(
                "paper_fill_rejected_invalid_market",
                symbol=market_state.symbol,
                bid=market_state.bid,
                ask=market_state.ask,
                reason="non_positive_price",
            )
            return "non_positive_price"
        spread_pct = (market_state.ask - market_state.bid) / market_state.bid
        if spread_pct >= MAX_FILL_SPREAD_PCT:
            logger.warning(
                "paper_fill_rejected_invalid_market",
                symbol=market_state.symbol,
                bid=market_state.bid,
                ask=market_state.ask,
                spread_pct=spread_pct,
                reason="spread_too_wide",
            )
            return "spread_too_wide"
        return None

    def _exit_fill_price(self, position: PaperPosition, trigger_price: float) -> float:
        slippage = self._settings.paper_slippage_bps / 10_000
        if position.side == Side.LONG:
            return _money(trigger_price * (1 - slippage))
        return _money(trigger_price * (1 + slippage))

    async def _log_strategy_signal(self, signal: Signal, action_taken: str) -> None:
        payload = {
            "timestamp": signal.created_at.isoformat(),
            "strategy_name": signal.strategy_name,
            "symbol": signal.symbol,
            "signal_type": "exit" if signal.reduce_only else "entry",
            "signal_strength": signal.signal_strength,
            "features": signal.features | {"reasoning": signal.reasoning},
            "action_taken": action_taken,
        }
        if self._repository is not None:
            await self._repository.insert("strategy_signals", payload)

    def _trade_payload(self, trade: Trade) -> dict[str, Any]:
        payload = trade.model_dump(mode="json")
        payload["execution_mode"] = self._settings.execution_mode
        for key in ("entry_price", "exit_price", "pnl_usd", "fees_usd"):
            if payload.get(key) is not None:
                payload[key] = _money(float(payload[key]))
        return payload

    def _position_payload(self, trade_id: UUID, position: PaperPosition) -> dict[str, Any]:
        return {
            "id": str(trade_id),
            "timestamp": position.entry_time.isoformat(),
            "symbol": position.symbol,
            "side": position.side.value,
            "size": position.size,
            "entry_price": _money(position.entry_price),
            "liquidation_price": None,
            "unrealized_pnl": _money(position.current_unrealized_pnl),
            "leverage": position.leverage,
            "strategy_name": position.strategy_name,
            "stop_loss": _money(position.stop_loss) if position.stop_loss is not None else None,
            "take_profit": _money(position.take_profit) if position.take_profit is not None else None,
            "execution_mode": self._settings.execution_mode,
        }

    def _trade_from_row(self, trade_row: dict[str, Any]) -> Trade:
        payload = dict(trade_row)
        payload.pop("created_at", None)
        payload.pop("execution_mode", None)
        payload["reason_entry"] = payload.get("reason_entry") or "restored paper trade"
        payload["fees_usd"] = float(payload.get("fees_usd") or 0)
        return Trade(**payload)

    def _paper_position_from_rows(
        self,
        trade: Trade,
        position_row: dict[str, Any],
    ) -> PaperPosition:
        side = _position_side(position_row.get("side") or trade.side)
        entry_time = _coerce_datetime(position_row.get("timestamp") or trade.timestamp)
        return PaperPosition(
            symbol=str(position_row.get("symbol") or trade.symbol),
            side=side,
            size=float(position_row.get("size") or trade.size),
            entry_price=_money(float(position_row.get("entry_price") or trade.entry_price)),
            entry_time=entry_time,
            leverage=float(position_row.get("leverage") or 1),
            stop_loss=_maybe_float(position_row.get("stop_loss")),
            take_profit=_maybe_float(position_row.get("take_profit")),
            strategy_name=str(position_row.get("strategy_name") or trade.strategy_name),
            fees_paid=_money(float(trade.fees_usd or 0)),
            current_unrealized_pnl=_money(float(position_row.get("unrealized_pnl") or 0)),
            max_hold_minutes=self._restored_max_hold_minutes(position_row, trade),
        )

    def _restored_max_hold_minutes(
        self,
        position_row: dict[str, Any],
        trade: Trade,
    ) -> int | None:
        strategy_name = str(position_row.get("strategy_name") or trade.strategy_name)
        if not is_scalp_strategy_name(strategy_name):
            return None
        return self._settings.scalp_max_hold_minutes


def _money(value: float) -> float:
    return round(float(value), 2)


def _gross_pnl(side: Side, entry_price: float, exit_price: float, size: float) -> float:
    direction = 1 if side == Side.LONG else -1
    return (exit_price - entry_price) * size * direction


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(tz=UTC)


def _maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _position_side(value: Any) -> Side:
    side = value if isinstance(value, Side) else Side(str(value))
    if side == Side.BUY:
        return Side.LONG
    if side == Side.SELL:
        return Side.SHORT
    return side
