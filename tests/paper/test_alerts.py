"""Tests for `src/trading/paper/alerts.py`: the transactional outbox that
sits between the paper-trading engine/breaker and Telegram.

Every test in this file is DB-backed (even `enqueue_alert` alone writes a
row), so the whole module carries `pytest.mark.db`, matching
`tests/paper/test_engine.py`'s module-level marking rather than
`test_breaker.py`'s per-test marking -- this file has no pure functions.

The single most important test here is
`test_telegram_failure_never_affects_the_fill`: it is the whole reason the
outbox pattern exists, not just one case among many. See the module
docstring of `trading.paper.alerts` for the argument.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal

import httpx
import psycopg
import pytest
import structlog

from tests.paper.helpers import decision_at, make_order, make_portfolio, simple_charges
from trading.paper.alerts import build_telegram_sender, enqueue_alert, run_alert_worker
from trading.paper.enums import OrderStatus, Side
from trading.paper.ledger import apply_fill

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _clean_alert_deliveries(db_url: str) -> Iterator[None]:
    """`run_alert_worker` commits (deliberately -- see alerts.py's module
    docstring), which defeats `db_conn`'s usual rollback-at-teardown
    isolation for this one table: a row a test's worker call marks SENT
    or FAILED is genuinely persisted, not undone when `db_conn.rollback()`
    runs at teardown. Wiped before and after every test in this file for
    the same reason `tests/paper/test_engine.py`'s `_cleanup` wipes it --
    without this, one test's committed row leaks into the next test's (or
    another file's) unfiltered `SELECT ... FROM alert_deliveries`.
    """
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("DELETE FROM alert_deliveries")
    yield
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("DELETE FROM alert_deliveries")


def _sync_no_sleep(_seconds: float) -> None:
    """`run_alert_worker` is synchronous (unlike `run_engine`/
    `run_ingestion_loop`) -- the retry/backoff interval between batches is
    just `time.sleep`, so tests inject a no-op instead to avoid actually
    waiting `poll_interval_seconds` between batches."""


def _delivery_row(db_conn) -> tuple[int, str, str, str, int, str | None]:
    row = db_conn.execute(
        "SELECT delivery_id, kind, payload, status, attempts, last_error FROM alert_deliveries"
    ).fetchone()
    assert row is not None
    return row


# --- enqueue_alert: no network, PENDING row --------------------------------


def test_enqueue_alert_writes_a_pending_row_with_no_network_call(db_conn, monkeypatch) -> None:
    """`enqueue_alert` must be pure DB I/O -- proven here by making any
    attempt to construct a real `httpx.Client` blow up, then asserting
    the write still succeeds."""

    def _forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("enqueue_alert must never construct an httpx.Client")

    monkeypatch.setattr(httpx, "Client", _forbidden)

    enqueue_alert(db_conn, "FILL", {"order_id": 42})

    delivery_id, kind, payload, status, attempts, last_error = _delivery_row(db_conn)
    assert kind == "FILL"
    assert status == "PENDING"
    assert attempts == 0
    assert last_error is None
    assert json.loads(payload) == {"order_id": 42}


def test_enqueue_alert_does_not_commit(db_conn) -> None:
    """The caller owns the transaction (matches `apply_fill`/`trip`'s
    convention) -- a rollback on `db_conn` must undo the enqueue too."""
    enqueue_alert(db_conn, "FILL", {"order_id": 1})
    db_conn.rollback()
    row = db_conn.execute("SELECT 1 FROM alert_deliveries").fetchone()
    assert row is None


def test_enqueue_alert_serialises_decimal_payload_values_as_exact_strings(db_conn) -> None:
    """JSON has no Decimal type. Money is serialised as a *string*, not a
    float -- a float would silently round e.g. quantities carrying eight
    fractional digits (positions.quantity is NUMERIC(18,8))."""
    enqueue_alert(
        db_conn,
        "FILL",
        {"quantity": Decimal("0.00000001"), "price": Decimal("79090.0100")},
    )
    _, _, payload, _, _, _ = _delivery_row(db_conn)
    decoded = json.loads(payload)
    assert decoded == {"quantity": "0.00000001", "price": "79090.0100"}


def test_enqueue_alert_rejects_a_non_serialisable_payload_value(db_conn) -> None:
    """No silent fallbacks: an accidentally-included non-JSON-native value
    (e.g. a stray float, or some other object) must raise loudly rather
    than being dropped or coerced."""
    with pytest.raises(TypeError):
        enqueue_alert(db_conn, "FILL", {"bad": object()})


# --- run_alert_worker: success, failure, max attempts -----------------------


def test_run_alert_worker_sends_a_pending_row_and_marks_it_sent(db_conn) -> None:
    enqueue_alert(db_conn, "FILL", {"order_id": 7})
    sent: list[str] = []

    run_alert_worker(lambda: db_conn, sent.append, max_batches=1, sleep=_sync_no_sleep)

    assert len(sent) == 1
    assert "FILL" in sent[0] and "7" in sent[0]
    delivery_id, kind, payload, status, attempts, last_error = _delivery_row(db_conn)
    assert status == "SENT"
    assert attempts == 0
    sent_at = db_conn.execute(
        "SELECT sent_at FROM alert_deliveries WHERE delivery_id=%s", (delivery_id,)
    ).fetchone()
    assert sent_at is not None and sent_at[0] is not None


def test_run_alert_worker_leaves_pending_and_records_the_error_on_failure(db_conn) -> None:
    enqueue_alert(db_conn, "FILL", {"order_id": 7})

    def _always_fails(_text: str) -> None:
        raise RuntimeError("telegram is down")

    run_alert_worker(lambda: db_conn, _always_fails, max_batches=1, sleep=_sync_no_sleep)

    _, _, _, status, attempts, last_error = _delivery_row(db_conn)
    assert status == "PENDING"
    assert attempts == 1
    assert last_error is not None and "telegram is down" in last_error


def test_run_alert_worker_marks_failed_after_max_attempts_and_stops_retrying(db_conn) -> None:
    enqueue_alert(db_conn, "FILL", {"order_id": 7})
    calls = 0

    def _always_fails(_text: str) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("telegram is down")

    # Two batches, max_attempts=2: batch 1 -> attempts=1/PENDING,
    # batch 2 -> attempts=2/FAILED.
    run_alert_worker(
        lambda: db_conn,
        _always_fails,
        max_batches=2,
        max_attempts=2,
        sleep=_sync_no_sleep,
    )
    assert calls == 2
    _, _, _, status, attempts, _ = _delivery_row(db_conn)
    assert status == "FAILED"
    assert attempts == 2

    # A FAILED row must never be picked up again.
    run_alert_worker(lambda: db_conn, _always_fails, max_batches=1, sleep=_sync_no_sleep)
    assert calls == 2


class _DeadConnection:
    """A connection double whose every operation raises -- the exact shape
    of an already-dropped connection. `execute` raises inside
    `_drain_pending` (a transient DB error, e.g. a severed socket), and
    `rollback` raises too when `run_alert_worker`'s outer handler then
    tries to recover (Fix round 1: an already-dead connection cannot be
    rolled back, and the first version of this code let that second
    exception propagate and kill the worker process). A test that only
    makes `execute` raise would pass without that fix -- `rollback` must
    raise too to actually exercise the guard.
    """

    def execute(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("connection already closed")

    def rollback(self) -> None:
        raise RuntimeError("rollback failed: connection already closed")


def test_run_alert_worker_survives_a_batch_where_rollback_itself_raises() -> None:
    """The load-bearing regression test for Fix round 1: a batch failure
    whose own rollback() also raises must be logged and swallowed, not
    propagated -- the worker must complete all max_batches rather than
    crashing the process on what both the original design and the outer
    except intended to be a recoverable failure."""
    conn = _DeadConnection()

    with structlog.testing.capture_logs() as cap:
        run_alert_worker(lambda: conn, lambda _t: None, max_batches=3, sleep=_sync_no_sleep)

    batch_failed = [e for e in cap if e.get("event") == "alerts.batch_failed"]
    rollback_failed = [e for e in cap if e.get("event") == "alerts.rollback_failed"]
    assert len(batch_failed) == 3  # one per batch -- the loop kept going
    assert len(rollback_failed) == 3  # rollback failed every time too, and was swallowed


def test_run_alert_worker_does_not_close_the_connection_it_is_given(db_conn) -> None:
    """`run_alert_worker` treats the connection's lifecycle as the
    caller's -- unlike `trading.paper.engine`'s per-operation fresh
    connections, closed by the engine itself, this worker must leave
    `db_conn` open so the caller can keep using it afterward (this is
    exactly what every other test in this file, and the outbox's own
    failure test, already depends on -- called out explicitly here so a
    future change that starts closing it fails loudly, in one place,
    instead of as a confusing error in a dozen unrelated tests)."""
    enqueue_alert(db_conn, "FILL", {"order_id": 1})
    run_alert_worker(lambda: db_conn, lambda _t: None, max_batches=1, sleep=_sync_no_sleep)
    assert db_conn.execute("SELECT 1").fetchone() == (1,)


# --- The point of the whole pattern -----------------------------------------


def test_telegram_failure_never_affects_the_fill(db_conn) -> None:
    """The outbox exists so a third-party outage cannot reach the fill
    path. The fill is committed; only the notification is late.

    `run_alert_worker` commits for real (see alerts.py's module
    docstring), which means it also commits the fill/portfolio/order this
    test set up earlier on the same `db_conn` -- `db_conn`'s usual
    rollback-at-teardown can no longer undo them, exactly like
    `_clean_alert_deliveries` above but for the other tables `make_
    portfolio`/`apply_fill` touch. Cleaned up explicitly in `finally`,
    with its own commit, mirroring `tests/paper/test_engine.py`'s
    `_cleanup` helper.
    """
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    order = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    decision = decision_at(Decimal("100"), quantity=Decimal("10"))
    apply_fill(db_conn, order, decision, simple_charges())
    enqueue_alert(db_conn, "FILL", {"order_id": order.order_id})

    def always_fails(_payload: str) -> None:
        raise RuntimeError("telegram is down")

    try:
        run_alert_worker(lambda: db_conn, always_fails, max_batches=1, sleep=_sync_no_sleep)

        status = db_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()[0]
        assert status == OrderStatus.FILLED, "the fill must survive a failed alert"
        delivery = db_conn.execute("SELECT status, attempts FROM alert_deliveries").fetchone()
        assert delivery[0] == "PENDING"
        assert delivery[1] == 1
    finally:
        db_conn.execute("DELETE FROM ledger_entries WHERE portfolio_id=%s", (pid,))
        db_conn.execute("DELETE FROM fills WHERE order_id=%s", (order.order_id,))
        db_conn.execute("DELETE FROM positions WHERE portfolio_id=%s", (pid,))
        db_conn.execute("DELETE FROM orders WHERE order_id=%s", (order.order_id,))
        db_conn.execute("DELETE FROM portfolios WHERE portfolio_id=%s", (pid,))
        db_conn.commit()


# --- run_alert_worker: unconfigured bot -------------------------------------


def test_run_alert_worker_idles_without_crashing_when_sender_is_none(db_conn) -> None:
    """An unset `telegram_bot_token` is a configuration state, not an
    error (this is a personal single-user system). The worker must log
    once and idle -- never touch `alert_deliveries`, never raise."""
    enqueue_alert(db_conn, "FILL", {"order_id": 1})

    with structlog.testing.capture_logs() as cap:
        run_alert_worker(lambda: db_conn, None, max_batches=3, sleep=_sync_no_sleep)

    idle_logs = [e for e in cap if e.get("event") == "alerts.worker_idle_no_bot_token"]
    assert len(idle_logs) == 1  # logged once, not once per idle batch

    _, _, _, status, attempts, _ = _delivery_row(db_conn)
    assert status == "PENDING"
    assert attempts == 0


# --- build_telegram_sender ---------------------------------------------------


class _StubSettings:
    def __init__(self, token: str | None, chat_id: str | None) -> None:
        self.telegram_bot_token = token
        self.telegram_chat_id = chat_id


def test_build_telegram_sender_returns_none_when_bot_token_is_unset() -> None:
    assert build_telegram_sender(_StubSettings(None, "123")) is None  # type: ignore[arg-type]


def test_build_telegram_sender_returns_none_when_chat_id_is_unset() -> None:
    assert build_telegram_sender(_StubSettings("tok", None)) is None  # type: ignore[arg-type]


def test_build_telegram_sender_posts_to_the_bot_api_with_no_real_network() -> None:
    """No test may make a real network call: `httpx.MockTransport` proves
    the request shape (URL, chat_id, text) without a socket ever opening,
    matching `tests/streaming/test_upstox_intraday_backfill.py`'s pattern."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    sender = build_telegram_sender(
        _StubSettings("test-token", "555"),  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    assert sender is not None
    sender("hello from a test")

    assert captured["url"] == "https://api.telegram.org/bottest-token/sendMessage"
    assert captured["body"] == {"chat_id": "555", "text": "hello from a test"}


def test_build_telegram_sender_raises_on_a_non_2xx_response() -> None:
    """A non-2xx response must raise, not be swallowed -- this is what
    feeds `run_alert_worker`'s retry/backoff its failure signal."""
    sender = build_telegram_sender(
        _StubSettings("test-token", "555"),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda r: httpx.Response(401, json={"ok": False})),
    )
    assert sender is not None
    with pytest.raises(httpx.HTTPStatusError):
        sender("hello")
