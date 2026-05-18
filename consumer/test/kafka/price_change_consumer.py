import uuid
from asyncio import QueueFull
from decimal import Decimal
from unittest.mock import AsyncMock, create_autospec

import pytest
from confluent_kafka import Message
from confluent_kafka.aio import AIOConsumer

from consumer.src.kafka.price_change_consumer import PriceChangeConsumer
from messages.price.price_change import PriceChangeEvent


def test_full_queue_raises_and_pauses():
    kafka_consumer: AsyncMock = create_autospec(AIOConsumer, instance=True)
    partition_consumer = PriceChangeConsumer(0, "sample", kafka_consumer)

    # We should allow 1000 messages to be enqueued
    for i in range(1000):
        partition_consumer.enqueue(create_autospec(Message, instance=True))

    # 1001 should raise an exception
    with pytest.raises(QueueFull):
        partition_consumer.enqueue(create_autospec(Message, instance=True))

