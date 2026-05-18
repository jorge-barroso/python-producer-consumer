from typing import Annotated

from fastapi import Depends

from producer.kafka.dependencies import KafkaProducerDep
from producer.price_change.producer import PriceChangeProducer


def get_price_change_producer(kafka_producer: KafkaProducerDep) -> PriceChangeProducer:
    return PriceChangeProducer(kafka_producer)

PriceChangeProducerDep: type[PriceChangeProducer] = Annotated[PriceChangeProducer, Depends(get_price_change_producer)]
