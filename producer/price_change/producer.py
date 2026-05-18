from confluent_kafka.aio import AIOProducer

from messages.price.price_change import PriceChangeEvent
from producer.price_change.request_model import PriceChangeRequest


class PriceChangeProducer:
    _topic: str = "price-change-v1"
    _producer: AIOProducer

    def __init__(self, producer: AIOProducer):
        self._producer = producer

    async def produce(self, price: PriceChangeRequest):
        event = PriceChangeEvent.model_validate(price.model_dump())
        await self._producer.produce(topic=self._topic, value=event.model_dump_json(), key=event.currency)
        return event
