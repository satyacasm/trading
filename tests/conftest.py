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

from trading.config import get_settings

TEST_DB_NAME = "trading_test"
REPO_ROOT = Path(__file__).resolve().parent.parent


def _with_database(url: str, name: str) -> str:
    return urlunsplit(urlsplit(url)._replace(path=f"/{name}"))


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
