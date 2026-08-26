"""Shared test fixtures.

`db_conn` hands every test a rolled-back transaction on a **dedicated test
database** (`trading_test`), created and migrated on first use, never on the
warehouse named by `DATABASE_URL`.

That separation is load-bearing rather than tidiness. The reconcile and
pipeline checks query whole tables filtered only by date range, while tests
seed their fixtures inside a rolled-back transaction and then assert on the
global result. Those two facts agree only while `bars_daily` is empty -- the
moment the real backfill lands a single day that overlaps a test's chosen
dates, assertions like `assert count == 50` start seeing production rows
(observed for real: 121,928). Choosing "safe" dates is not a fix, because the
backfill's range is 2016-2026 and will only grow.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import redis

from trading.config import get_settings

TEST_DB_NAME = "trading_test"
REPO_ROOT = Path(__file__).resolve().parent.parent

# Default test Redis: docker-compose's `redis_test` service (container
# `trading_redis_test`, no volume). A distinct *instance* from production
# Redis, not merely a distinct db number -- Redis pub/sub is not scoped by
# db (a subscriber on db1 receives what's published on db7, verified against
# this image), so a fixture message published during a test would otherwise
# land on live ingestors' channels. See docker-compose.yml for the incident
# this caused.
DEFAULT_TEST_REDIS_URL = "redis://localhost:6380/0"


class RedisIsolationError(RuntimeError):
    """Raised when the resolved test Redis and production Redis are the
    same instance. Never caught silently -- this must fail the session."""


def _with_database(url: str, name: str) -> str:
    return urlunsplit(urlsplit(url)._replace(path=f"/{name}"))


def _redis_host_port(url: str) -> tuple[str, int]:
    """Normalize a redis URL to a comparable (host, port) pair.

    `localhost` and `127.0.0.1` name the same loopback interface a
    docker-compose port mapping binds to, so they compare equal.
    """
    parts = urlsplit(url)
    host = parts.hostname or "localhost"
    if host == "127.0.0.1":
        host = "localhost"
    port = parts.port or 6379
    return host, port


def _assert_redis_isolated(prod_url: str, test_url: str) -> None:
    """Guard against test and production Redis resolving to the same
    instance. Compares normalized host+port, not raw URL strings, so
    `redis://localhost:6379/0` and `redis://127.0.0.1:6379/1` are still
    correctly recognized as the same instance.

    This is the check that makes isolation enforced rather than
    conventional -- everything downstream (the env-var redirect, the
    per-test flushdb) assumes this has already passed.
    """
    if _redis_host_port(prod_url) == _redis_host_port(test_url):
        raise RedisIsolationError(
            f"Test Redis ({test_url}) and production Redis ({prod_url}) "
            "resolve to the same host+port. Running the suite would "
            "publish fixture traffic onto live ingestors' channels -- "
            "point TEST_REDIS_URL at a distinct instance (see the "
            "`redis_test` service in docker-compose.yml)."
        )


@pytest.fixture(scope="session", autouse=True)
def _isolate_test_redis() -> Iterator[None]:
    """Redirect every `get_settings().redis_url` call at the dedicated
    `redis_test` instance instead of production Redis, for the whole
    session, before any test runs.

    This redirects at the source (the `REDIS_URL` env var `Settings`
    reads) rather than patching each of the ~20+ call sites, so it covers
    every code path automatically -- including ones that build their own
    client internally, like `streaming/gateway.py`. That's safe here
    because every `get_settings()` call in `src/trading/` happens lazily
    inside a function body; none of them cache a Redis URL at import time,
    so setting the env var before the first test runs is sufficient.

    An explicit env var outranks the `.env` file under pydantic-settings
    (verified empirically), and `get_settings.cache_clear()` before and
    after ensures the override is picked up and nothing leaks past the
    session boundary.
    """
    prod_url = get_settings().redis_url  # captured before any override
    test_url = os.environ.get("TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)

    _assert_redis_isolated(prod_url, test_url)

    previous = os.environ.get("REDIS_URL")
    os.environ["REDIS_URL"] = test_url
    get_settings.cache_clear()

    resolved = get_settings().redis_url
    if _redis_host_port(resolved) != _redis_host_port(test_url):
        raise RedisIsolationError(
            f"After overriding REDIS_URL, get_settings() resolved to "
            f"{resolved!r} instead of the test instance {test_url!r} -- "
            "something outranks the env var override."
        )

    # Fail with the actual remedy rather than letting the first test die on a
    # bare ConnectionError from `_flush_test_redis`. `docker compose up -d`
    # starts this service along with everything else, so this only trips on a
    # selectively-started or stale stack.
    probe = redis.Redis.from_url(test_url)
    try:
        probe.ping()
    except redis.exceptions.ConnectionError as exc:
        raise RedisIsolationError(
            f"Test Redis at {test_url} is not reachable ({exc}). Start it with "
            "`docker compose up -d redis_test`. The suite deliberately will not "
            "fall back to production Redis."
        ) from exc
    finally:
        probe.close()

    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("REDIS_URL", None)
        else:
            os.environ["REDIS_URL"] = previous
        get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _flush_test_redis(_isolate_test_redis: None) -> Iterator[None]:
    """Flush the test Redis between tests so fixture state never leaks
    from one test into the next.

    Depending on `_isolate_test_redis` (session-scoped, so it has already
    run and raised if isolation didn't hold) is what makes an unqualified
    `flushdb()` safe here -- without that guard this would risk flushing
    production.
    """
    client = redis.Redis.from_url(get_settings().redis_url)
    try:
        client.flushdb()
        yield
    finally:
        client.flushdb()
        client.close()


@pytest.fixture(scope="session")
def db_url() -> str:
    base = get_settings().database_url
    test_url = _with_database(base, TEST_DB_NAME)

    admin = psycopg.connect(_with_database(base, "postgres"), autocommit=True)
    try:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB_NAME,)
        ).fetchone()
        if exists is None:
            admin.execute(f'CREATE DATABASE "{TEST_DB_NAME}"')
    finally:
        admin.close()

    # A no-op once the test database is at head, so this costs a round trip
    # per session rather than a rebuild. `migrations/env.py` reads the URL
    # from `get_settings()`, and an explicit env var outranks the .env file.
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=REPO_ROOT,
        env={**os.environ, "DATABASE_URL": test_url},
        check=True,
        capture_output=True,
    )
    return test_url


@pytest.fixture
def db_conn(db_url: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(db_url, autocommit=False)
    try:
        yield conn
    finally:
        conn.rollback()  # nothing a test does is ever persisted
        conn.close()
