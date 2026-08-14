from collections.abc import Iterator

import psycopg
import pytest

from trading.config import get_settings


@pytest.fixture(scope="session")
def db_url() -> str:
    return get_settings().database_url


@pytest.fixture
def db_conn(db_url: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(db_url, autocommit=False)
    try:
        yield conn
    finally:
        conn.rollback()  # nothing a test does is ever persisted
        conn.close()
