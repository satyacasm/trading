"""Shared fixtures for the streaming test suite. Redis is real
(docker-compose's `trading_redis`), never mocked -- the same convention
`db_conn` already uses for Postgres."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis

from trading.config import get_settings


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        yield client
    finally:
        client.close()
