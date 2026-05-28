from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from twilio.base.exceptions import TwilioRestException

from src.alerts.twilio_notifier import TwilioNotifier
from src.config import Settings


def _settings() -> Settings:
    return Settings(
        execution_mode="paper_sim",
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
        sms_alerts_enabled=True,
        twilio_from_number="+15555555555",
        twilio_to_number="+15555555556",
    )


@pytest.mark.asyncio
async def test_twilio_failures_disable_temporarily_and_worker_keeps_running() -> None:
    notifier = TwilioNotifier(_settings())
    notifier._client = object()  # type: ignore[assignment]
    notifier._send_interval_sec = 0.01
    attempts = 0

    def failing_send(_: str) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("twilio down")

    notifier._send_sync = failing_send  # type: ignore[method-assign]

    await notifier.start()
    try:
        for _ in range(10):
            notifier.send_error(RuntimeError("simulated"))

        for _ in range(100):
            if notifier._disabled_until is not None:
                break
            await asyncio.sleep(0.01)

        assert attempts == 5
        assert notifier._disabled_until is not None
        assert notifier._disabled_until > datetime.now(tz=UTC)
        assert notifier._worker_task is not None
        assert not notifier._worker_task.done()
    finally:
        await notifier.stop()


@pytest.mark.asyncio
async def test_twilio_rest_exception_cannot_escape_worker() -> None:
    notifier = TwilioNotifier(_settings())
    notifier._client = object()  # type: ignore[assignment]
    notifier._send_interval_sec = 0.01
    attempts = 0

    def failing_send(_: str) -> None:
        nonlocal attempts
        attempts += 1
        raise TwilioRestException(401, "/Messages.json", msg="Authenticate")

    notifier._send_sync = failing_send  # type: ignore[method-assign]

    await notifier.start()
    try:
        for _ in range(10):
            notifier.send_error(RuntimeError("simulated"))

        for _ in range(100):
            if notifier._disabled_until is not None:
                break
            await asyncio.sleep(0.01)

        assert attempts == 5
        assert notifier._disabled_until is not None
        assert notifier._worker_task is not None
        assert not notifier._worker_task.done()
    finally:
        await notifier.stop()


def test_send_sync_catches_twilio_rest_exception() -> None:
    class FailingMessages:
        def create(self, **_: object) -> None:
            raise TwilioRestException(401, "/Messages.json", msg="Authenticate")

    class FailingClient:
        messages = FailingMessages()

    notifier = TwilioNotifier(_settings())
    notifier._client = FailingClient()  # type: ignore[assignment]

    result = notifier._send_sync("hello")

    assert isinstance(result, tuple)
    assert result[0] == "TwilioRestException"
