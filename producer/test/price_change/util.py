from uuid import UUID
from decimal import Decimal

from producer.price_change.request_model import PriceChangeRequest


def get_correct_price_request() -> PriceChangeRequest:
    return PriceChangeRequest(
        currency="usd", amount=Decimal(125.99),
        asset_id=UUID("1c8db081-f11e-43c3-9a33-0f62c013c5e9"),
        source_system="legacy system"
    )

def get_price_with_bad_currency() -> PriceChangeRequest:
    price = PriceChangeRequest(currency="usf", amount=Decimal(125.99),
                               asset_id=UUID("1c8db081-f11e-43c3-9a33-0f62c013c5e9"),
                               source_system="legacy system")
    return price

def get_price_with_missing_source() -> PriceChangeRequest:
    price = PriceChangeRequest(currency="usd", amount=Decimal(125.99),
                               asset_id=UUID("1c8db081-f11e-43c3-9a33-0f62c013c5e9"),
                               source_system="")
    return price