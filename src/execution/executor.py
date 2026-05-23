"""Signal execution and trade lifecycle persistence."""

from __future__ import annotations

from datetime import UTC, datetime
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

logger = structlog.get_logger(__name__)


class Executor:
    """Validate, size, submit, and persist strategy trades."""

    def __init__(
        self,
        exchange: HyperliquidClient,
        repository: SupabaseRepository | None,
        circuit_breakers: CircuitBreakerManager,
        discord: DiscordNotifier,
        sms: TwilioNotifier | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._exchange = exchange
        self._repository = repository
        self._circuit_breakers = circuit_breakers
        self._discord = discord
        self._sms = sms
        self._execution_mode = settings.execution_mode if settings else "testnet_real"
        self._open_trades: dict[str, Trade] = {}

    async def execute_signal(
        self,
        signal: Signal,
        equity: float,
        asset_realized_vol: float,
    ) -> dict[str, Any] | None:
        """Check risk, size the order, and submit the signal."""
        await self._log_strategy_signal(signal, action_taken="received")
        if signal.reduce_only:
            response = await self._exchange.close_position(signal.symbol)
            await self._log_strategy_signal(signal, action_taken="exit_order_placed")
            await self._discord.send(
                f"EXIT {signal.symbol} | {signal.strategy_name} | {signal.reasoning}",
                symbol=signal.symbol,
            )
            logger.info(
                "exit_order_submitted",
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
                response=response,
            )
            return response

        if not self._circuit_breakers.can_open_new_entries():
            logger.warning(
                "signal_rejected_circuit_breaker",
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
            logger.warning("signal_rejected_zero_size", symbol=signal.symbol)
            await self._log_strategy_signal(signal, action_taken="rejected_zero_size")
            return None

        asset_size = round(usd_notional / signal.entry_price, 6)
        order_side = Side.BUY if signal.side == Side.LONG else Side.SELL
        response = await self._exchange.place_order(
            symbol=signal.symbol,
            side=order_side,
            size=asset_size,
            price=signal.entry_price,
            order_type=OrderType.LIMIT,
            reduce_only=False,
        )
        await self._log_strategy_signal(signal, action_taken="order_placed")
        logger.info(
            "order_submitted",
            strategy_name=signal.strategy_name,
            symbol=signal.symbol,
            size=asset_size,
            usd_notional=usd_notional,
            response=response,
        )
        return response

    async def update_market_state(self, market_state: MarketState) -> None:
        """No-op hook for interface parity with PaperExecutor."""
        logger.debug("real_executor_market_state_ignored", symbol=market_state.symbol)

    async def get_open_positions(self) -> list[Position]:
        """Return real open positions from Hyperliquid."""
        return await self._exchange.get_open_positions()

    async def close_all_positions(self, reason_exit: str = "risk close") -> list[dict[str, Any]]:
        """Close all real positions through Hyperliquid."""
        logger.warning("real_executor_close_all_positions", reason_exit=reason_exit)
        return await self._exchange.close_all_positions()

    async def on_fill(self, signal: Signal, fill: dict[str, Any]) -> Trade:
        """Persist a newly opened trade and position after an entry fill."""
        size = float(fill.get("sz") or fill.get("size") or 0)
        entry_price = float(fill.get("px") or fill.get("price") or signal.entry_price)
        trade = Trade(
            strategy_name=signal.strategy_name,
            symbol=signal.symbol,
            side=signal.side,
            size=size,
            entry_price=entry_price,
            reason_entry=signal.reasoning,
        )
        position = Position(
            id=trade.id,
            symbol=signal.symbol,
            side=signal.side,
            size=size,
            entry_price=entry_price,
            leverage=10,
            strategy_name=signal.strategy_name,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
        )
        self._open_trades[str(trade.id)] = trade

        trade_payload = _trade_payload(trade)
        trade_payload["execution_mode"] = self._execution_mode
        position_payload = position.model_dump(mode="json")
        position_payload["execution_mode"] = self._execution_mode
        if self._repository is not None:
            await self._repository.insert("trades", trade_payload)
            await self._repository.insert("positions", position_payload)

        await self._discord.send(
            (
                f"OPEN {trade.symbol} {trade.side.value} {trade.size:.6f} @ "
                f"{trade.entry_price:.2f} | {trade.strategy_name}"
            ),
            trade_id=str(trade.id),
        )
        if self._sms is not None:
            sms_payload = trade_payload | {
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
            }
            self._sms.send_trade_opened(sms_payload)
        logger.info("trade_opened", trade_id=str(trade.id), symbol=trade.symbol)
        return trade

    async def close_trade(
        self,
        trade_id: UUID,
        exit_price: float,
        reason_exit: str,
        fees_usd: float = 0.0,
    ) -> Trade:
        """Close a tracked trade and update persistence."""
        trade = self._open_trades.get(str(trade_id))
        if trade is None:
            raise KeyError(f"trade is not tracked: {trade_id}")

        direction = 1 if trade.side == Side.LONG else -1
        pnl_usd = (exit_price - trade.entry_price) * trade.size * direction - fees_usd
        notional = trade.entry_price * trade.size
        pnl_pct = pnl_usd / notional if notional else 0.0
        trade.exit_price = exit_price
        trade.pnl_usd = round(pnl_usd, 2)
        trade.pnl_pct = pnl_pct
        trade.fees_usd = round(fees_usd, 2)
        trade.hold_duration_sec = int(
            (datetime.now(tz=UTC) - trade.timestamp).total_seconds()
        )
        trade.reason_exit = reason_exit
        trade.status = TradeStatus.CLOSED

        payload = _trade_payload(trade)
        payload["execution_mode"] = self._execution_mode
        if self._repository is not None:
            await self._repository.update("trades", str(trade.id), payload)
            await self._repository.delete("positions", str(trade.id))
        self._open_trades.pop(str(trade_id), None)

        await self._discord.send(
            (
                f"CLOSE {trade.symbol} {trade.side.value} | PnL: "
                f"${trade.pnl_usd:.2f} ({trade.pnl_pct:.2%})"
            ),
            trade_id=str(trade.id),
        )
        if self._sms is not None:
            self._sms.send_trade_closed(payload)
        logger.info("trade_closed", trade_id=str(trade.id), pnl_usd=trade.pnl_usd)
        return trade

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


def _trade_payload(trade: Trade) -> dict[str, Any]:
    payload = trade.model_dump(mode="json")
    for key in ("entry_price", "exit_price", "pnl_usd", "fees_usd"):
        if payload.get(key) is not None:
            payload[key] = round(float(payload[key]), 2)
    return payload
