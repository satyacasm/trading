"""ReconnectingConnection: a dead Postgres connection is repaired on the
next .get(), with the same 1s-30s backoff resilient_pubsub uses."""

from __future__ import annotations

import psycopg

from trading.db import ReconnectingConnection


class _FakeConn:
    def __init__(self, alive=True, probe_raises=False):
        self.closed = 0 if alive else 1
        self._probe_raises = probe_raises
        self.executed: list[str] = []

    def execute(self, sql):
        self.executed.append(sql)
        if self._probe_raises:
            raise psycopg.OperationalError("server closed the connection unexpectedly")


def test_get_returns_the_same_connection_while_it_is_healthy():
    conns = [_FakeConn()]
    rc = ReconnectingConnection("postgresql://x", connect=lambda *a, **k: conns[0])

    first = rc.get()
    second = rc.get()
    assert first is second is conns[0]


def test_a_closed_connection_is_replaced():
    old = _FakeConn(alive=False)
    new = _FakeConn(alive=True)
    made = [old, new]

    def _connect(*a, **k):
        return made.pop(0)

    rc = ReconnectingConnection("postgresql://x", connect=_connect)
    assert rc.get() is old  # closed=0 wasn't checked until the NEXT get()
    old.closed = 1
    assert rc.get() is new


def test_a_probe_failure_triggers_backoff_then_reconnect():
    calls: list[float] = []
    bad = _FakeConn(alive=True, probe_raises=True)
    good = _FakeConn(alive=True)
    made = [bad, good]

    rc = ReconnectingConnection(
        "postgresql://x", connect=lambda *a, **k: made.pop(0), sleep=calls.append
    )
    first = rc.get()
    assert first is bad
    second = rc.get()  # probes with SELECT 1, which raises on `bad`
    assert second is good
    assert calls == [1.0]


def test_backoff_caps_at_max_backoff_and_keeps_reconnecting():
    calls: list[float] = []
    always_bad = _FakeConn(alive=True, probe_raises=True)

    def _connect(*a, **k):
        return always_bad

    rc = ReconnectingConnection(
        "postgresql://x", connect=_connect, sleep=calls.append, max_backoff=4.0
    )
    rc.get()
    for _ in range(4):
        rc.get()
    assert calls == [1.0, 2.0, 4.0, 4.0]
