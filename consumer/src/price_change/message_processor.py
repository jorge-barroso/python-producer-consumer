from confluent_kafka import Message

from consumer.src.price_change.model import PriceChange
from consumer.src.price_change.repository import PriceChangeRepository
from messages.price.price_change import PriceChangeEvent


class PriceChangeMessageProcessor:
    def __init__(self):
        self.repo = PriceChangeRepository()

    def process_message(self, message: Message) -> None:
        """
        :param message: The kafka message to be processed.
        :return: None
        :raises ValidationError: If `message` is not a JSON string or the object could not be validated.
        """
        price_change_event = PriceChangeEvent.model_validate_json(message.value())
        price_change = PriceChange(
            price_change_event.event_uuid,
            price_change_event.asset_id,
            price_change_event.amount,
            price_change_event.currency,
            price_change_event.source_system,
            message.topic(),
            message.partition(),
            message.offset(),
        )
        self.repo.save(price_change)