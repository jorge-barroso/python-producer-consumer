FROM python:3.14-slim

LABEL authors="jorge"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY producer/pyproject.toml producer/uv.lock ./producer/
COPY messages/pyproject.toml messages/uv.lock ./messages/

WORKDIR /app/producer

RUN uv sync --frozen --no-dev --python /usr/local/bin/python

COPY producer ./producer
COPY messages ./messages

ENV PYTHONPATH=/app

EXPOSE 12021

CMD ["/app/producer/.venv/bin/python", "-m", "producer.main"]