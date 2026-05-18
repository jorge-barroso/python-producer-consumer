import asyncio
import random
import uuid
from decimal import Decimal

import httpx

URL = "http://127.0.0.1:12021/price_change"

CURRENCIES = ["usd", "eur", "gbp", "jpy", "cad", "aud", "chf", "cny", "hkd", "sgd", "nzd", "mxn", "brl", "ars", "clp",
              "cop", "pen", "zar", "sek", "nok", "dkk", "pln", "czk", "huf", "ron", "try", "ils", "aed", "sar", "inr",
              "krw", "thb", "myr", "idr", "php", "vnd", "twd"]
SOURCE_SYSTEMS = ["legacy system", "market feed", "pricing engine", "backoffice", "risk system", "manual override", ]


def random_amount() -> float:
    amount = Decimal(random.randrange(100, 500_000)) / Decimal("100")
    return float(amount)


def build_payload() -> dict:
    return {"currency": random.choice(CURRENCIES), "amount": random_amount(), "asset_id": str(uuid.uuid4()),
        "source_system": random.choice(SOURCE_SYSTEMS), }


async def send_one(client: httpx.AsyncClient, index: int) -> tuple[int, int | None, str | None]:
    payload = build_payload()

    try:
        response = await client.post(URL, json=payload)
        return index, response.status_code, None
    except Exception as exc:
        return index, None, str(exc)


async def main() -> None:
    total_requests = random.randint(6_000, 12_000)
    concurrency = 100

    print(f"Sending {total_requests} requests with concurrency={concurrency}")

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency, )

    timeout = httpx.Timeout(10.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded_send(index: int):
            async with semaphore:
                return await send_one(client, index)

        tasks = [bounded_send(i) for i in range(total_requests)]

        success = 0
        failures = 0
        status_counts: dict[int, int] = {}

        for completed in asyncio.as_completed(tasks):
            index, status_code, error = await completed

            if status_code is not None and 200 <= status_code < 300:
                success += 1
            else:
                failures += 1

            if status_code is not None:
                status_counts[status_code] = status_counts.get(status_code, 0) + 1

            if error:
                print(f"[{index}] ERROR: {error}")

        print()
        print("Done")
        print(f"Success: {success}")
        print(f"Failures: {failures}")
        print(f"Status counts: {status_counts}")


if __name__ == "__main__":
    asyncio.run(main())
