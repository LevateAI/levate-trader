import asyncio

import structlog

from src.alerts.twilio_notifier import TwilioNotifier
from src.config import get_settings
from src.logging import configure_logging

logger = structlog.get_logger(__name__)


async def main():
    settings = get_settings()
    configure_logging(settings.log_level)
    notifier = TwilioNotifier(settings)
    await notifier.start()
    notifier.send_error(Exception("test sms from levate-trader - if you got this, twilio works"))
    await asyncio.sleep(4)
    await notifier.stop()
    logger.info("sms_test_sent")


if __name__ == "__main__":
    asyncio.run(main())
