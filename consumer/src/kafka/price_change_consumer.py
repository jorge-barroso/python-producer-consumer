import asyncio
import csv
import logging
from asyncio import QueueFull
from dataclasses import dataclass, field

from confluent_kafka import Message, TopicPartition
from confluent_kafka.aio import AIOConsumer

from consumer.src.message_processors.price_change_message_processor import PriceChangeMessageProcessor


@dataclass
class PriceChangeConsumer:
    __COMMIT_OFFSET = 100
    __QUEUE_SIZE = 1000
    __paused: bool
    __stopping: bool
    __partition: int
    __topic: str
    __consumer: AIOConsumer
    __queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    __task: asyncio.Task | None = None

    def __init__(self, partition: int, topic: str, consumer: AIOConsumer, processor: PriceChangeMessageProcessor) -> None:
        logging.info(f"Initializing consumer for partition {partition}")
        self.__paused = False
        self.__stopping = False
        self.__partition = partition
        self.__topic = topic
        self.__consumer = consumer
        self.__price_change_message_processor = processor
        self.__queue = asyncio.Queue(maxsize=self.__QUEUE_SIZE)

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
                if message.offset() % self.__COMMIT_OFFSET == 0:
                    await self.__consumer.commit(message=message, asynchronous=False)
            except Exception as e:
                logging.error(f"Error getting next message from the queue: {e}")
            finally:
                self.__queue.task_done()
                if self.__paused and self.__queue.qsize() <= self.__COMMIT_OFFSET / 2:
                    await self.__consumer.resume([TopicPartition[message.topic(), message.partition()]])

    def enqueue(self, message: Message) -> None:
        logging.info(f"Enqueuing message #{message.offset()}")
        try:
            self.__queue.put_nowait(message)
        except QueueFull as q:
            self.__paused = True
            raise q

    async def stop(self):
        self.__stopping = True

        await self.__queue.join()

        if self.__task:
            self.__task.cancel()

            try:
                await self.__task
            except asyncio.CancelledError:
                pass
