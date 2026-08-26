"""Tests for the test/production Redis isolation set up in the root
`tests/conftest.py`.

Two things need proving:
1. The env-var redirect actually works -- `get_settings().redis_url`
   resolves to the test instance (port 6380), not production.
2. The guard actually trips -- given a test URL and production URL that
   resolve to the same host+port, it raises. Tested against the guard's
   comparison function directly rather than trying to corrupt the real
   session fixture (which has already run and passed by the time any test
   body executes).
"""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

from tests.conftest import RedisIsolationError, _assert_redis_isolated
from trading.config import get_settings


def test_get_settings_redis_url_points_at_test_instance() -> None:
    url = get_settings().redis_url
    assert urlsplit(url).port == 6380


def test_guard_trips_when_test_and_prod_share_host_and_port() -> None:
    with pytest.raises(RedisIsolationError):
        _assert_redis_isolated("redis://localhost:6379/0", "redis://localhost:6379/0")


def test_guard_treats_localhost_and_loopback_ip_as_equal() -> None:
    with pytest.raises(RedisIsolationError):
        _assert_redis_isolated("redis://localhost:6379/0", "redis://127.0.0.1:6379/0")

    with pytest.raises(RedisIsolationError):
        _assert_redis_isolated("redis://127.0.0.1:6379/0", "redis://localhost:6379/1")


def test_guard_passes_for_genuinely_distinct_instances() -> None:
    _assert_redis_isolated("redis://localhost:6379/0", "redis://localhost:6380/0")  # no raise
