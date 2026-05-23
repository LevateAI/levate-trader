"""Discord webhook alerts."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from discord_webhook import DiscordWebhook

logger = structlog.get_logger(__name__)


class DiscordNotifier:
    """Fire-and-forget Discord notifier."""

    def __init__(self, webhook_url: str | None) -> None:
        self._webhook_url = webhook_url

    async def send(self, content: str, **metadata: Any) -> None:
        """Send a Discord message when a webhook URL is configured."""
        if not self._webhook_url:
            logger.info("discord_skipped", reason="webhook_not_configured", **metadata)
            return
        try:
            await asyncio.to_thread(self._send_sync, content)
            logger.info("discord_sent", **metadata)
        except (OSError, RuntimeError, ValueError) as exc:
            logger.error("discord_send_failed", error=str(exc), **metadata)

    def _send_sync(self, content: str) -> None:
        webhook = DiscordWebhook(url=self._webhook_url, content=content)
        response = webhook.execute()
        status_code = getattr(response, "status_code", 204)
        if status_code >= 400:
            raise RuntimeError(f"discord webhook failed with status {status_code}")
