from decimal import Decimal
from typing import Any
from uuid import UUID

from babel.numbers import format_currency
from currency_symbols import CurrencySymbols
from pydantic import BaseModel, Field, field_validator, computed_field


class PriceChangeRequest(BaseModel):
    asset_id: UUID

    amount: Decimal = Field(..., gt=0, description="The value of the currency cannot be 0")
    currency: str = Field(..., min_length=3, max_length=3, description="The currency code of the asset (e.g. USD)")
    source_system: str = Field(..., min_length=1)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        symbol = CurrencySymbols.get_symbol(value)
        if not symbol:
            raise ValueError(f"Currency symbol is unknown or invalid for currency {value}")
        return value.upper()

    @computed_field
    @property
    def currency_symbol(self) -> str:
        return CurrencySymbols.get_symbol(self.currency)

    @computed_field
    @property
    def formatted_amount(self) -> str:
        return format_currency(self.amount, self.currency_symbol)

    def __str__(self) -> str:
        return self.formatted_amount
