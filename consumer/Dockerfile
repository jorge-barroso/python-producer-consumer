FROM python:3.14-slim

LABEL authors="jorge"

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY consumer/pyproject.toml consumer/uv.lock ./consumer/
COPY messages/pyproject.toml messages/uv.lock ./messages/
COPY messages ./messages

WORKDIR /app/consumer

RUN uv sync --frozen --no-dev --python /usr/local/bin/python

COPY consumer/src ./consumer/src

ENV PYTHONPATH=/app

CMD ["sh", "-c", "uv run python -m consumer.src.main"]