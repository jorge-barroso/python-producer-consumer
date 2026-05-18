from uuid import UUID, uuid4
from decimal import Decimal

from pydantic import BaseModel, Field


class PriceChangeEvent(BaseModel):
    event_uuid: UUID = Field(default_factory=uuid4)

    asset_id: UUID
    amount: Decimal
    currency: str
    source_system: str