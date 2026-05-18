import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

from confluent_kafka import Message

from consumer.src.price_change.message_processor import PriceChangeMessageProcessor
from messages.price.price_change import PriceChangeEvent


def test_message_processing():
    message = AsyncMock(spec=Message)

    message.topic.return_value = "price-changes"
    message.partition.return_value = 0
    message.offset.return_value = 123
    message.value.return_value = (PriceChangeEvent(
        event_uuid=uuid.uuid4(),
        asset_id=uuid.uuid4(),
        amount=Decimal(123.99),
        currency="USD", source_system="legacy")
                                  .model_dump_json())

    processor = PriceChangeMessageProcessor()
    processor.process_message(message)