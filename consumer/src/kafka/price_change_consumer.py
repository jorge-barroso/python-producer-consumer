import asyncio
import logging
from dataclasses import dataclass, field
from typing import Coroutine

from confluent_kafka import Message
from confluent_kafka.aio import AIOConsumer

from consumer.src.price_change.message_processor import PriceChangeMessageProcessor


@dataclass
class PriceChangeConsumer:
    __COMMIT_INTERVAL = 100
    __QUEUE_SIZE = 1000
    __stopping: bool
    __partition: int
    __topic: str
    __messages_since_last_commit: int
    __consumer: AIOConsumer
    __last_successful_message: Message | None
    __last_commit_attempt: Coroutine | None
    __task: asyncio.Task | None
    __queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)

    def __init__(self, partition: int, topic: str, consumer: AIOConsumer, processor: PriceChangeMessageProcessor) -> None:
        logging.info(f"Initializing consumer for partition {partition}")
        self.__stopping = False
        self.__partition = partition
        self.__topic = topic
        self.__messages_since_last_commit = 0
        self.__consumer = consumer
        self.__last_successful_message = None
        self.__last_commit_attempt = None
        self.__task = None
        self.__price_change_message_processor = processor
        self.__queue = asyncio.Queue(maxsize=self.__QUEUE_SIZE)

    async def enqueue(self, message: Message) -> None:
        logging.info(f"Enqueuing message #{message.offset()}")
        await self.__queue.put(message)

    def start(self):
        logging.info(f"Starting consumer for partition {self.__partition}")
        self.__task = asyncio.create_task(
            self.run(),
            name=f"price-change-partition-{self.__partition}",
        )
        logging.info(f"Consumer for partition {self.__partition} successfully started")


    async def run(self):
        while not self.__stopping:
            logging.info("Waiting for message...")
            message = await self.__queue.get()
            try:
                logging.info(f"Received message in partition consumer {self.__partition}")
                self.__price_change_message_processor.process_message(message)
                await self.try_commit(message)
            except Exception as e:
                logging.exception("Error getting next message from the queue", e)
            finally:
                self.__queue.task_done()

    async def try_commit(self, message: Message):
        self.__last_successful_message = message
        self.__messages_since_last_commit += 1
        if self.__messages_since_last_commit < self.__COMMIT_INTERVAL:
            return

        await self.await_pending_commit()

        self.__last_commit_attempt = self.__consumer.commit(message=message, asynchronous=True)
        self.__messages_since_last_commit = 0

    async def await_pending_commit(self):
        if self.__last_commit_attempt:
            await self.__last_commit_attempt

    async def stop(self):
        logging.info(f"Stopping consumer {self.__partition}")
        self.__stopping = True

        await self.__queue.join()

        if self.__last_successful_message:
            await self.await_pending_commit()
            await self.__consumer.commit(message=self.__last_successful_message, asynchronous=False)

        if self.__task:
            self.__task.cancel()

            try:
                await self.__task
            except asyncio.CancelledError:
                pass
