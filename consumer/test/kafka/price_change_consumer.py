from unittest.mock import AsyncMock, create_autospec

from confluent_kafka import Message
from confluent_kafka.aio import AIOConsumer

from consumer.src.kafka.price_change_consumer import PriceChangeConsumer
from consumer.src.message_processors.price_change_message_processor import PriceChangeMessageProcessor


def test_full_queue_raises_and_pauses():
    kafka_consumer: AsyncMock = create_autospec(AIOConsumer, instance=True)
    partition_consumer = PriceChangeConsumer(0, "sample", kafka_consumer, PriceChangeMessageProcessor())

    # We should allow 1000 messages to be enqueued
    for i in range(1000):
        partition_consumer.enqueue(create_autospec(Message, instance=True))

