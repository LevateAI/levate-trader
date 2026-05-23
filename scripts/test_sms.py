import asyncio
from src.config import settings
from src.alerts.twilio_notifier import TwilioNotifier

async def main():
    notifier = TwilioNotifier(settings)
    await notifier.send_error(Exception("test sms from levate-trader — if you got this, twilio works"))
    print("sms sent, check your phone")

if __name__ == "__main__":
    asyncio.run(main())
