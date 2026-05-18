import uuid
import datetime
from decimal import Decimal

from sqlalchemy import Numeric, String, CHAR, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from consumer.src.db.base import Base


class PriceChange(Base):
    __tablename__ = "price_change"

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)

    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    source_system: Mapped[str] = mapped_column(String(20), nullable=False)

    kafka_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(nullable=False)
    kafka_offset: Mapped[int] = mapped_column(nullable=False)
    time_added: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())