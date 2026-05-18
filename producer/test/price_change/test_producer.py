import json
from decimal import Decimal
from unittest.mock import create_autospec, AsyncMock
from uuid import UUID

import pytest

from . import util
from ...price_change.producer import PriceChangeProducer


@pytest.mark.asyncio
async def test_produce_sends_right_message():
    mocked_producer: AsyncMock = create_autospec(PriceChangeProducer, instance=True)
    mocked_producer.produce = AsyncMock()

    producer = PriceChangeProducer(mocked_producer)

    request = util.get_correct_price_request()

    await producer.produce(request)
    call_kwargs = mocked_producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == "price-change-v1"
    assert call_kwargs["key"] == request.currency

    payload = json.loads(call_kwargs["value"])
    assert UUID(payload['event_uuid']).version == 4
    assert UUID(payload["asset_id"]) == request.asset_id
    assert payload["currency"] == request.currency
    assert pytest.approx(Decimal(payload["amount"])) == pytest.approx(Decimal(request.amount))
    assert payload["source_system"] == request.source_system
