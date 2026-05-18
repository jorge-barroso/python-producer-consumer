import logging

from fastapi import APIRouter
from starlette.status import HTTP_202_ACCEPTED

from .dependencies import PriceChangeProducerDep
from .request_model import PriceChangeRequest
from .response_model import PriceResponseModel

price_change_router = APIRouter(prefix="/price_change", tags=["price_change"])


@price_change_router.post("", status_code=HTTP_202_ACCEPTED, response_model=PriceResponseModel)
async def price_change(price_change_request: PriceChangeRequest,
                       producer: PriceChangeProducerDep) -> PriceResponseModel:
    logging.info("Received price change request")
    event = await producer.produce(price_change_request)

    return PriceResponseModel(status="accepted", event_uuid=event.event_uuid)
