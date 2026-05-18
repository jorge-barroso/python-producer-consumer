from contextlib import asynccontextmanager

import uvicorn
from confluent_kafka.aio import AIOProducer
from fastapi import FastAPI

from producer.price_change.routes import price_change_router
from producer.core.settings import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    producer: AIOProducer = AIOProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
            "acks": "all",
            "enable.idempotence": settings.kafka_idempotence,
        }
    )
    app.state.kafka_producer = producer

    try:
        yield
    finally:
        await producer.close() # flushes before shutting down



app = FastAPI(lifespan=lifespan)
app.include_router(router=price_change_router)

if __name__ == "__main__":
    uvicorn.run(
        "producer.main:app",
        host="127.0.0.1",
        port=settings.port,
        reload=settings.reload_app,
    )