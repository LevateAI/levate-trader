"""Rate-limited Twilio SMS alerts."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import structlog
from twilio.rest import Client

from src.config import Settings

logger = structlog.get_logger(__name__)


class TwilioNotifier:
    """Async queue wrapper for Twilio's synchronous REST client."""

    def __init__(self, settings: Settings) -> None:
        self._enabled = settings.sms_alerts_enabled
        self._from_number = settings.twilio_from_number
        self._to_number = settings.twilio_to_number
        self._client: Client | None = None
        if settings.twilio_account_sid and settings.twilio_auth_token:
            self._client = Client(settings.twilio_account_sid, settings.twilio_auth_token)
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the rate-limit worker."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="twilio-sms-worker")

    async def stop(self) -> None:
        """Stop the rate-limit worker."""
        if self._worker_task is None:
            return
        self._worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._worker_task
        self._worker_task = None

    def send_trade_opened(self, trade: dict[str, Any]) -> None:
        """Queue an SMS for a newly opened trade."""
        message = (
            f"🟢 OPEN {trade['symbol']} {trade['side']} {trade['size']} @ "
            f"{trade['entry_price']} | {trade['strategy_name']} | "
            f"stop: {trade.get('stop_loss')} target: {trade.get('take_profit')}"
        )
        self._enqueue(message)

    def send_trade_closed(self, trade: dict[str, Any]) -> None:
        """Queue an SMS for a closed trade."""
        pnl = float(trade.get("pnl_usd") or 0)
        emoji = "🟢" if pnl >= 0 else "🔴"
        message = (
            f"{emoji} CLOSE {trade['symbol']} {trade['side']} | "
            f"PnL: ${pnl:.2f} ({float(trade.get('pnl_pct') or 0):.2%}) | "
            f"held: {trade.get('hold_duration_sec')}s"
        )
        self._enqueue(message)

    def send_circuit_breaker(self, event: dict[str, Any]) -> None:
        """Queue an SMS for a circuit breaker event."""
        message = (
            f"⚠️ BREAKER {event['breaker_type']} TRIPPED | threshold "
            f"{event['threshold_value']} hit at {event['observed_value']} | "
            f"action: {event['action']}"
        )
        self._enqueue(message)

    def send_error(self, err: Exception) -> None:
        """Queue an SMS for an error."""
        message = f"🚨 ERROR {type(err).__name__}: {str(err)[:120]}"
        self._enqueue(message)

    def _enqueue(self, message: str) -> None:
        if not self._enabled:
            logger.info("sms_skipped", reason="disabled")
            return
        if self._client is None or not self._from_number or not self._to_number:
            logger.info("sms_skipped", reason="twilio_not_configured")
            return
        self._queue.put_nowait(message)

    async def _worker(self) -> None:
        while True:
            message = await self._queue.get()
            try:
                await asyncio.to_thread(self._send_sync, message)
                logger.info("sms_sent")
            except (OSError, RuntimeError, ValueError) as exc:
                logger.error("sms_send_failed", error=str(exc))
            finally:
                self._queue.task_done()
                await asyncio.sleep(3)

    def _send_sync(self, message: str) -> None:
        if self._client is None:
            raise RuntimeError("Twilio client is not configured")
        self._client.messages.create(
            body=message,
            from_=self._from_number,
            to=self._to_number,
        )
