import logging

from sqlalchemy.exc import IntegrityError

from consumer.db.session import SessionLocal
from consumer.src.price_change.model import PriceChange


class PriceChangeRepository:
    @classmethod
    def save(cls, price_change: PriceChange) -> None:
        with SessionLocal() as session:
            session.add(price_change)
            try:
                session.commit()
            except IntegrityError:
                logging.exception("Error saving price change, event may have already been processed")
                session.rollback()