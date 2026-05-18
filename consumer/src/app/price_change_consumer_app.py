import logging
from asyncio import QueueFull

from confluent_kafka import TopicPartition
from confluent_kafka.aio import AIOConsumer

from consumer.src.core.settings import settings
from consumer.src.kafka.price_change_consumer import PriceChangeConsumer
from consumer.src.message_processors.price_change_message_processor import PriceChangeMessageProcessor


class PriceChangeConsumerApp:
    __kafka_consumer: AIOConsumer
    __partition_processors: dict[int, PriceChangeConsumer]
    __price_change_message_processor: PriceChangeMessageProcessor

    def __init__(self):
        consumer_options = {'bootstrap.servers': settings.bootstrap_servers, 'group.id': settings.consumer_group,
                            'auto.offset.reset': 'earliest', 'enable.auto.commit': False}
        logging.info(f"""Initializing Kafka Consumer with settings:\n{consumer_options}""")
        self.__kafka_consumer = AIOConsumer(consumer_options)
        self.__partition_processors = {}
        self.__price_change_message_processor = PriceChangeMessageProcessor()

    async def start(self):
        await self.__kafka_consumer.subscribe([settings.topic])
        try:
            await self.run()
        finally:
            await self.shutdown()

    async def run(self) -> None:
        logging.info("Initiated Polling")
        try:
            while True:
                await self.get_message()
        finally:
            await self.__kafka_consumer.close()

    async def get_message(self) -> None:
        message = await self.__kafka_consumer.poll(1.0)
        if not message:
            return
        if message.error():
            logging.error(f"Error consuming message: {message.error()}")
            return

        logging.info(f"Received message: #{message.offset()}")
        partition = message.partition()
        processor = self.__partition_processors.get(partition)
        if not processor:
            logging.info("Building new processor")
            processor = PriceChangeConsumer(partition, message.topic(), self.__kafka_consumer, self.__price_change_message_processor)
            self.__partition_processors[partition] = processor
            processor.start()
        else:
            logging.info("Processor already existed")

        try:
            processor.enqueue(message)
        except QueueFull:
            await self.__kafka_consumer.pause([TopicPartition(settings.topic(), partition)])

    async def shutdown(self) -> None:
        for processor in self.__partition_processors.values():
            await processor.stop()

        await self.__kafka_consumer.close()
