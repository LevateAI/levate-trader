"""Rate-limited Twilio SMS alerts."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from twilio.rest import Client

from src.config import Settings

logger = structlog.get_logger(__name__)

SMS_FAILURE_DISABLE_THRESHOLD = 5
FIRST_FAILURE_DISABLE_DURATION = timedelta(minutes=5)
REPEATED_FAILURE_DISABLE_DURATION = timedelta(minutes=30)


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
        self._consecutive_failures = 0
        self._disable_count = 0
        self._disabled_until: datetime | None = None
        self._send_interval_sec = 3.0
        self._worker_error_sleep_sec = 5.0

    async def start(self) -> None:
        """Start the rate-limit worker."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="twilio-sms-worker")

    async def stop(self) -> None:
        """Stop the rate-limit worker."""
        if self._worker_task is None:
            return
        self._worker_task.cancel()
        try:
            await self._worker_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(
                "sms_worker_stop_error",
                error_type=type(exc).__name__,
                error_message=_safe_exception_message(exc),
            )
        self._worker_task = None

    def send_trade_opened(self, trade: dict[str, Any]) -> None:
        """Queue an SMS for a newly opened trade."""
        try:
            message = (
                f"🟢 OPEN {trade['symbol']} {trade['side']} {trade['size']} @ "
                f"{trade['entry_price']} | {trade['strategy_name']} | "
                f"stop: {trade.get('stop_loss')} target: {trade.get('take_profit')}"
            )
            self._enqueue(message)
        except Exception as exc:
            self._log_sync_send_failure("send_trade_opened", exc)

    def send_trade_closed(self, trade: dict[str, Any]) -> None:
        """Queue an SMS for a closed trade."""
        try:
            pnl = float(trade.get("pnl_usd") or 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            message = (
                f"{emoji} CLOSE {trade['symbol']} {trade['side']} | "
                f"PnL: ${pnl:.2f} ({float(trade.get('pnl_pct') or 0):.2%}) | "
                f"held: {trade.get('hold_duration_sec')}s"
            )
            self._enqueue(message)
        except Exception as exc:
            self._log_sync_send_failure("send_trade_closed", exc)

    def send_circuit_breaker(self, event: dict[str, Any]) -> None:
        """Queue an SMS for a circuit breaker event."""
        try:
            message = (
                f"⚠️ BREAKER {event['breaker_type']} TRIPPED | threshold "
                f"{event['threshold_value']} hit at {event['observed_value']} | "
                f"action: {event['action']}"
            )
            self._enqueue(message)
        except Exception as exc:
            self._log_sync_send_failure("send_circuit_breaker", exc)

    def send_error(self, err: Exception) -> None:
        """Queue an SMS for an error."""
        try:
            message = f"🚨 ERROR {type(err).__name__}: {_safe_exception_message(err)[:120]}"
            self._enqueue(message)
        except Exception as exc:
            self._log_sync_send_failure("send_error", exc)

    def _enqueue(self, message: str) -> None:
        try:
            if not self._enabled:
                logger.info("sms_skipped", reason="disabled")
                return
            if self._is_temporarily_disabled():
                logger.info("sms_disabled_temporarily", disabled_until=self._disabled_until_iso())
                return
            if self._client is None or not self._from_number or not self._to_number:
                logger.info("sms_skipped", reason="twilio_not_configured")
                return
            self._queue.put_nowait(message)
        except Exception as exc:
            self._log_sync_send_failure("_enqueue", exc)

    async def _worker(self) -> None:
        while True:
            try:
                await self._worker_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "sms_worker_error",
                    error_type=type(exc).__name__,
                    error_message=_safe_exception_message(exc),
                )
                await asyncio.sleep(self._worker_error_sleep_sec)

    async def _worker_once(self) -> None:
        message = await self._queue.get()
        try:
            if self._is_temporarily_disabled():
                logger.info("sms_disabled_temporarily", disabled_until=self._disabled_until_iso())
                return
            try:
                result = await asyncio.to_thread(self._send_sync, message)
                if result is True or result is None:
                    self._consecutive_failures = 0
                    self._disable_count = 0
                    self._disabled_until = None
                    logger.info("sms_sent")
                else:
                    error_type, error_message = result
                    self._record_send_failure(error_type, error_message)
            except Exception as exc:
                self._handle_send_failure(exc)
        finally:
            self._queue.task_done()
            await asyncio.sleep(self._send_interval_sec)

    def _handle_send_failure(self, exc: Exception) -> None:
        self._record_send_failure(type(exc).__name__, _safe_exception_message(exc))

    def _record_send_failure(self, error_type: str, error_message: str) -> None:
        self._consecutive_failures += 1
        logger.warning(
            "sms_send_failed",
            error_type=error_type,
            error_message=error_message,
            consecutive_failures=self._consecutive_failures,
        )
        if self._consecutive_failures < SMS_FAILURE_DISABLE_THRESHOLD:
            return
        duration = (
            FIRST_FAILURE_DISABLE_DURATION
            if self._disable_count == 0
            else REPEATED_FAILURE_DISABLE_DURATION
        )
        self._disabled_until = datetime.now(tz=UTC) + duration
        self._disable_count += 1
        self._consecutive_failures = 0
        logger.warning(
            "sms_disabled_after_failures",
            disabled_until=self._disabled_until_iso(),
            disabled_for_sec=duration.total_seconds(),
        )

    def _is_temporarily_disabled(self) -> bool:
        if self._disabled_until is None:
            return False
        if datetime.now(tz=UTC) < self._disabled_until:
            return True
        self._disabled_until = None
        return False

    def _disabled_until_iso(self) -> str | None:
        return self._disabled_until.isoformat() if self._disabled_until is not None else None

    def _log_sync_send_failure(self, call_path: str, exc: Exception) -> None:
        logger.warning(
            "sms_sync_call_failed",
            call_path=call_path,
            error_type=type(exc).__name__,
            error_message=_safe_exception_message(exc),
        )

    def _send_sync(self, message: str) -> bool | tuple[str, str]:
        try:
            if self._client is None:
                raise RuntimeError("Twilio client is not configured")
            self._client.messages.create(
                body=message,
                from_=self._from_number,
                to=self._to_number,
            )
            return True
        except Exception as exc:
            error_type = type(exc).__name__
            error_message = _safe_exception_message(exc)
            logger.warning(
                "sms_send_sync_failed",
                error_type=error_type,
                error_message=error_message,
            )
            return error_type, error_message


def _safe_exception_message(exc: Exception) -> str:
    try:
        return str(exc)
    except Exception as stringify_exc:
        return f"<unprintable {type(exc).__name__}: {type(stringify_exc).__name__}>"
