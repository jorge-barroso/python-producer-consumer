import asyncio
import logging
import signal
from contextlib import suppress

from consumer.src.app.price_change_consumer_app import PriceChangeConsumerApp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    force=True,
)

def get_stop_event_listener(events: list) -> asyncio.Event:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for event in events:
        loop.add_signal_handler(event, stop_event.set)

    return stop_event


async def main() -> None:
    app = PriceChangeConsumerApp()
    stop_event = get_stop_event_listener([signal.SIGINT, signal.SIGTERM])

    app_task = asyncio.create_task(app.start(), name="price-change-consumer-app")
    await stop_event.wait()
    logging.info(f"Shutdown signal received, stopping consumer {app_task}")

    await app.stop_polling()

    app_task.cancel()
    with suppress(asyncio.CancelledError):
        await app_task

    logging.info("App shutdown completed")


if __name__ == '__main__':
    asyncio.run(main())
