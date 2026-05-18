from typing import Annotated

from confluent_kafka.aio import AIOProducer
from fastapi import Request, Depends


def get_kafka_producer(request: Request) -> AIOProducer:
    return request.app.state.kafka_producer

KafkaProducerDep: type[AIOProducer] = Annotated[AIOProducer, Depends(get_kafka_producer)]