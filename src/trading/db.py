"""A Postgres connection that repairs itself.

Every long-running process here opened one psycopg connection at start
and kept it forever -- fine until the laptop sleeps through a router
reset and TimescaleDB drops the socket. `ReconnectingConnection` replaces
that "open once" pattern: `.get()` returns a connection that is either
already known-good or has just been reconnected, with the same 1s-30s
backoff `resilient_pubsub` uses for Redis.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import psycopg
import structlog

log = structlog.get_logger(__name__)

__all__ = ["ReconnectingConnection"]

_INITIAL_BACKOFF = 1.0


class ReconnectingConnection:
    def __init__(
        self,
        url: str,
        *,
        autocommit: bool = True,
        sleep: Callable[[float], None] = time.sleep,
        connect: Callable[..., psycopg.Connection] = psycopg.connect,
        max_backoff: float = 30.0,
    ) -> None:
        self._url = url
        self._autocommit = autocommit
        self._sleep = sleep
        self._connect = connect
        self._max_backoff = max_backoff
        self._backoff = _INITIAL_BACKOFF
        self._conn = self._open()
        self._first_get = True

    def _open(self) -> psycopg.Connection:
        return self._connect(self._url, autocommit=self._autocommit)

    def _reconnect(self) -> None:
        log.warning("db.reconnecting", backoff=self._backoff)
        self._sleep(self._backoff)
        self._backoff = min(self._backoff * 2, self._max_backoff)
        self._conn = self._open()

    def get(self) -> psycopg.Connection:
        """A connection known to be alive right now. Closed connections
        are caught for free (`.closed`); a connection that merely looks
        open but whose socket died silently is caught by a `SELECT 1`
        probe, the same check Postgres client libraries use everywhere
        for exactly this failure mode."""
        if self._first_get:
            self._first_get = False
            return self._conn
        if self._conn.closed:
            self._reconnect()
            return self._conn
        try:
            self._conn.execute("SELECT 1")
        except psycopg.OperationalError as exc:
            log.warning("db.probe_failed", reason=str(exc))
            self._reconnect()
            return self._conn
        self._backoff = _INITIAL_BACKOFF
        return self._conn
