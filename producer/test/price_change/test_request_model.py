import uuid
from decimal import Decimal

from pydantic import ValidationError
from pytest import raises

from producer.price_change.request_model import PriceChangeRequest
from . import util


def test_initialization():
    price = PriceChangeRequest(currency="usd", amount=Decimal(125.99),
                               asset_id=uuid.UUID("1c8db081-f11e-43c3-9a33-0f62c013c5e9"),
                               source_system="legacy system")
    assert price.currency == "USD"
    assert price.currency_symbol == "$"
    assert price.amount == 125.99
    assert str(price) == "$125.99"


def test_bad_currency_fails_validation():
    with raises(ValidationError):
        util.get_price_with_bad_currency()


def test_missing_source_fails_validation():
    with raises(ValidationError):
        util.get_price_with_missing_source()
