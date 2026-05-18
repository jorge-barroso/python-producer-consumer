FROM python:3.14-slim

LABEL authors="jorge"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY producer/pyproject.toml producer/uv.lock ./

RUN uv sync --frozen --no-dev --python /usr/local/bin/python

COPY producer ./producer

EXPOSE 12021

CMD ["sh", "-c", "uv run uvicorn producer.main:app --host 0.0.0.0 --port ${PRODUCER_PORT:-12021}"]