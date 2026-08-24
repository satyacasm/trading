"""Shared FastAPI dependency: one Postgres connection per request.

Extracted out of `gateway.py` so `market_data_api.py`'s router can depend
on the exact same dependency object `gateway.py` uses -- tests override
this single dependency once (`app.dependency_overrides[get_db_connection]
= ...`) and every router mounted on `gateway.app` sees that override,
regardless of which module defines the route.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
from psycopg import Connection

from trading.config import get_settings


def get_db_connection() -> Iterator[Connection]:
    """A real connection per request. Tests override this dependency with
    their own `db_conn` fixture so a route's reads/writes happen inside the
    same rolled-back test transaction instead of committing a second, real
    connection."""
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
