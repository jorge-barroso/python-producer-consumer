from decimal import Decimal
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class PriceResponseModel(BaseModel):
    status: str
    event_uuid: UUID
