"""Supervisor behaviour that a container test would be too slow to cover."""

from __future__ import annotations

import io
import json
import os
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg
import structlog.testing

from trading.live.protocol import FRAME_ERROR, FRAME_ORDERS, encode_frame
from trading.live.supervisor import MAX_ORDERS_PER_MINUTE, LiveRun, handle_bar


def _pipe_stdout(lines: list[str]):  # noqa: ANN201
    """A real OS pipe standing in for process.stdout. All lines are
    written and the write end is closed immediately -- reading sees
    everything at once (tests need no real wait) and then EOF, the
    same end-of-output signal the old MagicMock's trailing b"" gave."""
    read_fd, write_fd = os.pipe()
    with os.fdopen(write_fd, "wb") as w:
        for line in lines:
            w.write(line.encode())
    return os.fdopen(read_fd, "rb", buffering=0)


def _run(stdout_lines: list[str], *, live_run_id: int = 1) -> LiveRun:
    process = MagicMock(spec=subprocess.Popen)
    process.poll.return_value = None
    process.stdin = MagicMock()
    process.stdout = _pipe_stdout(stdout_lines)
    return LiveRun(
        live_run_id=live_run_id,
        strategy_id=1,
        portfolio_id=1,
        process=process,
        instrument_ids={1},
        runtime="runsc",
        kernel_isolated=True,
        started_at=datetime(2026, 9, 4, tzinfo=UTC),
    )


def _intent() -> dict[str, object]:
    return {
        "instrument_id": 1,
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": "1",
        "limit_price": None,
        "product": "DELIVERY",
        "rationale": "test",
    }


def _bar() -> dict[str, object]:
    return {
        "instrument_id": 1,
        "ts": "2026-09-04T10:00:00+00:00",
        "interval_sec": 60,
        "open": "100",
        "high": "100",
        "low": "100",
        "close": "100",
        "volume": None,
    }


def test_read_frames_returns_at_the_deadline_when_nothing_is_written() -> None:
    """The actual production bug (design §6): a blocking readline()'s
    30s bound was only checked BETWEEN reads, so a hung container froze
    every run indefinitely. select() enforces the deadline directly."""
    from trading.live import supervisor

    read_fd, write_fd = os.pipe()  # write_fd deliberately never written or closed
    process = MagicMock(spec=subprocess.Popen)
    process.poll.return_value = None
    process.stdout = os.fdopen(read_fd, "rb", buffering=0)
    run = LiveRun(
        live_run_id=1,
        strategy_id=1,
        portfolio_id=1,
        process=process,
        instrument_ids={1},
        runtime="runsc",
        kernel_isolated=True,
        started_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    start = time.monotonic()
    frames = supervisor.read_frames(run, timeout=0.2)
    elapsed = time.monotonic() - start
    os.close(write_fd)

    assert frames == []
    assert elapsed < 0.7  # timeout (0.2s) + 0.5s slack


def test_the_rate_limit_stops_a_runaway_and_says_so(monkeypatch) -> None:  # noqa: ANN001
    """§166 puts order-rate limiting in the supervisor, and a strategy
    emitting an order per bar across a wide universe is a plausible bug --
    the engine would faithfully fill every one of them."""
    from trading.live import supervisor

    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )

    orders = [
        {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": "1",
            "limit_price": None,
            "product": "DELIVERY",
            "rationale": "runaway",
        }
    ] * (MAX_ORDERS_PER_MINUTE + 5)
    run = _run([encode_frame(FRAME_ORDERS, ts="t", orders=orders, alive=True)])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "STOPPED"
    assert "order-rate limit" in (stopped[0][1] or "")


def test_a_crashed_strategy_stops_the_run_rather_than_restarting_it(monkeypatch) -> None:  # noqa: ANN001
    """A strategy whose in-memory state vanished mid-session is not the same
    strategy, and its next orders would not follow from what it saw."""
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run([encode_frame(FRAME_ERROR, error="ZeroDivisionError: division by zero")])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "CRASHED"
    assert "ZeroDivisionError" in (stopped[0][1] or "")


def test_a_silent_container_is_treated_as_crashed(monkeypatch) -> None:  # noqa: ANN001
    """No frames at all means the process is gone or wedged. Continuing to
    feed it bars would leave a run that looks alive and trades nothing."""
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run([])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "CRASHED"


def test_a_latched_breaker_stops_the_run_with_its_reason(monkeypatch) -> None:  # noqa: ANN001
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run(
        [
            encode_frame(
                FRAME_ORDERS, ts="t", orders=[], alive=False, breaker_reason="max_daily_loss: ..."
            )
        ]
    )

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "STOPPED"
    assert "max_daily_loss" in (stopped[0][1] or "")


def test_an_ordinary_bar_keeps_the_run_alive(monkeypatch) -> None:  # noqa: ANN001
    from trading.live import supervisor

    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    run = _run([encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True)])
    conn = MagicMock()

    assert handle_bar(conn, "http://x", run, _bar()) is True
    assert run.bars_seen == 1


def test_a_refused_order_is_recorded_on_the_run(monkeypatch) -> None:  # noqa: ANN001
    """A run whose every order is refused looks, from `orders_placed`
    alone, exactly like a run that decided to sit still. The currency
    gate's own sentence is the answer to "why is nothing happening", so
    it is kept on the row rather than only in the supervisor's log."""
    import urllib.error
    import urllib.request

    from trading.live import supervisor

    detail = (
        b'{"detail":"portfolio 10 has base_currency=\'INR\'; instrument_id=642283'
        b" is denominated in 'USDT'\"}"
    )
    # `place_order` imports urllib inside the function, so the patch has to
    # land on the module itself rather than on a supervisor attribute.
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        MagicMock(
            side_effect=urllib.error.HTTPError(
                "http://x/orders", 400, "Bad Request", {}, io.BytesIO(detail)
            )
        ),
    )
    run = _run([])

    assert supervisor.place_order("http://x", run, _intent(), 0) is False
    assert run.orders_refused == 1
    assert "base_currency" in (run.last_refusal or "")


def test_a_refused_order_reaches_the_row(monkeypatch) -> None:  # noqa: ANN001
    """The counter is only useful if the bar loop writes it down."""
    from trading.live import supervisor

    def refuse(api_url: str, run: LiveRun, intent: dict[str, object], seq: int) -> bool:
        run.orders_refused += 1
        run.last_refusal = "the currency gate said no"
        return False

    monkeypatch.setattr(supervisor, "place_order", refuse)
    run = _run([encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)])
    conn = MagicMock()

    assert handle_bar(conn, "http://x", run, _bar()) is True
    assert run.orders_placed == 0
    written = conn.execute.call_args[0][1]
    assert 1 in written and "the currency gate said no" in written


def test_a_perpetual_order_carries_its_leverage_to_the_gateway(monkeypatch) -> None:  # noqa: ANN001
    """A short reaches the gateway only if it says what leverage it is at.
    Without it the order is refused -- correctly, since leverage decides
    the margin locked up -- and the strategy sits at zero fills with the
    reason buried in a refusal count."""
    import urllib.error
    import urllib.request

    from trading.live import supervisor

    captured: dict[str, object] = {}

    class _Response:
        status = 201

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    def _urlopen(request, timeout=None):  # noqa: ANN001, ANN202, ARG001
        captured.update(json.loads(request.data))
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", _urlopen)
    run = _run([])
    run.leverage = Decimal("20")

    assert supervisor.place_order("http://x", run, _intent(), 0) is True
    assert captured["leverage"] == "20"


def test_an_unlevered_run_sends_no_leverage_at_all(monkeypatch) -> None:  # noqa: ANN001
    """None, not 1. An equity strategy has no leverage concept, and
    sending 1 would put a number that reads as a fact on every order this
    platform has ever placed."""
    import urllib.error
    import urllib.request

    from trading.live import supervisor

    captured: dict[str, object] = {}

    class _Response:
        status = 201

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout=None: (captured.update(json.loads(request.data)), _Response())[1],  # noqa: ARG005
    )
    assert supervisor.place_order("http://x", _run([]), _intent(), 0) is True
    assert captured["leverage"] is None


def _seed_live_run(db_conn, *, started_at) -> int:
    """A minimal strategies/portfolios/live_runs row -- same shape as
    tests/live/test_cursors.py's _live_run helper, duplicated here
    because this file has no shared conftest fixture for it yet."""
    user_id = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    portfolio_id = db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance) "
        "VALUES (%s, 'supervisor-test', 'USDT', 1000, 1000) RETURNING portfolio_id",
        (user_id,),
    ).fetchone()[0]
    strategy_id = db_conn.execute(
        "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
        "status, contract_version) VALUES (%s,'t','1.0.0','x','y','REGISTERED','0.1') "
        "RETURNING strategy_id",
        (user_id,),
    ).fetchone()[0]
    row = db_conn.execute(
        "INSERT INTO live_runs (strategy_id, portfolio_id, status, started_at) "
        "VALUES (%s, %s, 'RUNNING', %s) RETURNING live_run_id",
        (strategy_id, portfolio_id, started_at),
    ).fetchone()
    return row[0]


def test_deliver_pending_feeds_every_bar_since_the_cursor_in_order(db_conn, monkeypatch) -> None:
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    for minute, close in ((1, "100"), (2, "101")):
        db_conn.execute(
            "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
            "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
            (iid, datetime(2026, 9, 25, 10, minute, tzinfo=UTC), close, close, close, close),
        )
    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    run = _run(
        [
            encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True),
            encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True),
        ],
        live_run_id=live_run_id,
    )
    run.instrument_ids = {iid}
    run.started_at = started_at

    ok = supervisor.deliver_pending(
        db_conn, "http://x", run, datetime(2026, 9, 25, 10, 3, tzinfo=UTC)
    )

    assert ok is True
    assert run.bars_seen == 2
    cursor = db_conn.execute(
        "SELECT last_ts FROM live_run_cursors WHERE live_run_id=%s AND instrument_id=%s",
        (live_run_id, iid),
    ).fetchone()
    assert cursor[0] == datetime(2026, 9, 25, 10, 2, tzinfo=UTC)


def test_a_catchup_bars_orders_are_refused_not_placed(db_conn, monkeypatch) -> None:
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 10, 1, tzinfo=UTC)),
    )
    placed: list[int] = []
    monkeypatch.setattr(
        supervisor, "place_order", lambda *a, **k: (placed.append(1), True)[1]
    )
    run = _run(
        [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)],
        live_run_id=live_run_id,
    )
    run.instrument_ids = {iid}
    run.started_at = started_at

    # 10:10 is well past the bar's 10:02 close + the 2-minute catchup
    # threshold -- this bar is delivered with catchup=True.
    ok = supervisor.deliver_pending(
        db_conn, "http://x", run, datetime(2026, 9, 25, 10, 10, tzinfo=UTC)
    )

    assert ok is True
    assert placed == []
    assert run.orders_refused == 1
    assert run.last_refusal == "catch-up bar: price no longer tradeable"


def test_a_catchup_refusal_is_logged(db_conn, monkeypatch) -> None:
    """I6/M2: the docs point an operator at this log (plus
    live_runs.orders_refused/last_refusal) to check that catch-up refusal
    is actually happening, instead of a claim about the paper API that
    isn't true (it only checks the latest bar)."""
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 10, 1, tzinfo=UTC)),
    )
    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    run = _run(
        [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)],
        live_run_id=live_run_id,
    )
    run.instrument_ids = {iid}
    run.started_at = started_at

    with structlog.testing.capture_logs() as cap:
        ok = supervisor.deliver_pending(
            db_conn, "http://x", run, datetime(2026, 9, 25, 10, 10, tzinfo=UTC)
        )

    assert ok is True
    refusal_logs = [e for e in cap if e.get("event") == "live.catchup_refused"]
    assert len(refusal_logs) == 1
    assert refusal_logs[0]["live_run_id"] == live_run_id
    assert refusal_logs[0]["instrument_id"] == iid
    assert refusal_logs[0]["bar_ts"] == datetime(2026, 9, 25, 10, 1, tzinfo=UTC).isoformat()


def test_catchup_is_recomputed_per_bar_immediately_before_it_is_sent(db_conn, monkeypatch) -> None:
    """I3: `pending_bars` fixes `catchup` once, at query time, for the
    whole pass. A long replay across several bars takes real wall time to
    dispatch (each one a subprocess round trip) -- a bar that was fresh
    when queried can go stale by the time its own turn to be sent comes
    up, and the paper API's guard only checks the latest bar, not this
    one. `deliver_pending` must recompute `catchup` immediately before
    each `handle_bar`, using a real send-time clock, and only ever
    escalate it (never un-flag a bar the query already marked catch-up)."""
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    for minute in (1, 2):
        db_conn.execute(
            "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
            "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
            (iid, datetime(2026, 9, 25, 10, minute, tzinfo=UTC)),
        )
    placed: list[int] = []
    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: (placed.append(1), True)[1])
    run = _run(
        [
            encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True),
            encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True),
        ],
        live_run_id=live_run_id,
    )
    run.instrument_ids = {iid}
    run.started_at = started_at

    # At query time ("now"=10:03), both bars are fresh: bar1 closes at
    # 10:02 (60s before now), bar2 closes at 10:03 (0s before now) --
    # neither exceeds the 120s catchup_after threshold.
    query_now = datetime(2026, 9, 25, 10, 3, tzinfo=UTC)
    # The send-time clock: fresh for bar1's send, then jumps far past the
    # threshold before bar2's send -- as if dispatching bar1 took a long
    # time (a slow container round trip).
    clock_values = iter(
        [
            datetime(2026, 9, 25, 10, 3, tzinfo=UTC),
            datetime(2026, 9, 25, 10, 20, tzinfo=UTC),
        ]
    )

    ok = supervisor.deliver_pending(
        db_conn, "http://x", run, query_now, clock=lambda: next(clock_values)
    )

    assert ok is True
    assert placed == [1]  # only bar1's order was placed
    assert run.orders_placed == 1
    assert run.orders_refused == 1  # bar2, stale by send time, was refused
    assert run.last_refusal == "catch-up bar: price no longer tradeable"


def test_catchup_orders_do_not_count_toward_the_rate_limit(db_conn, monkeypatch) -> None:
    """A long replay dispatches many bars in seconds. Orders refused as
    catch-up are never placed, so counting them would trip the
    60-a-minute limit and stop the very run being recovered."""
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    bar_count = supervisor.MAX_ORDERS_PER_MINUTE + 10
    for minute in range(1, bar_count + 1):
        db_conn.execute(
            "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
            "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
            (iid, started_at + timedelta(minutes=minute)),
        )
    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    stopped: list[str] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append(status)
    )
    run = _run(
        [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)] * bar_count,
        live_run_id=live_run_id,
    )
    run.instrument_ids = {iid}
    run.started_at = started_at

    # Every bar is hours old at "now": all catch-up.
    ok = supervisor.deliver_pending(
        db_conn, "http://x", run, started_at + timedelta(hours=6)
    )

    assert ok is True
    assert stopped == []
    assert run.orders_refused == bar_count


def test_a_replay_cap_gap_is_recorded_on_the_run_even_with_nothing_to_deliver(
    db_conn,
) -> None:
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    started_at = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    live_run_id = _seed_live_run(db_conn, started_at=started_at)
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 8, 1, tzinfo=UTC)),
    )
    run = _run([], live_run_id=live_run_id)
    run.instrument_ids = {iid}
    run.started_at = started_at

    # now is 2 days later with a 1-hour replay cap -- the single stored
    # bar is entirely outside the window, so nothing is delivered.
    with structlog.testing.capture_logs() as cap:
        ok = supervisor.deliver_pending(
            db_conn, "http://x", run, datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
        )

    assert ok is True
    row = db_conn.execute(
        "SELECT last_gap_note FROM live_runs WHERE live_run_id=%s", (live_run_id,)
    ).fetchone()
    assert row[0] is not None and "replay cap" in row[0]

    # Spec §4: a replay-cap skip is "logged and recorded" -- the row
    # write above is the "recorded" half, this is the "logged" half.
    gap_logs = [e for e in cap if e.get("event") == "live.replay_gap"]
    assert len(gap_logs) == 1
    assert gap_logs[0]["live_run_id"] == live_run_id
    assert "replay cap" in gap_logs[0]["note"]


def test_a_db_outage_does_not_crash_the_supervisor_loop(monkeypatch) -> None:  # noqa: ANN001
    """ReconnectingConnection.get() can raise psycopg.OperationalError
    while the database is still down, after sleeping its own backoff
    (Task 3). run_supervisor's loop must survive that: log it, skip this
    pass, and try again next iteration -- not let the whole process die
    on an outage that recovers on its own."""
    from trading.live import supervisor

    calls = {"n": 0}

    class _FakeDB:
        def get(self):  # noqa: ANN202
            calls["n"] += 1
            if calls["n"] == 1:
                raise psycopg.OperationalError("still down")
            stop.set()
            return MagicMock()

    class _FakePubSub:
        def get_message(self, timeout):  # noqa: ANN001, ANN202, ARG002
            return None

    monkeypatch.setattr(supervisor, "ReconnectingConnection", lambda *a, **k: _FakeDB())  # noqa: ARG005
    monkeypatch.setattr(supervisor, "SyncResilientPubSub", lambda *a, **k: _FakePubSub())  # noqa: ARG005
    monkeypatch.setattr(supervisor, "reconcile", lambda conn, runs: None)  # noqa: ARG005

    stop = threading.Event()

    supervisor.run_supervisor(stop)

    assert calls["n"] == 2


def test_a_delivery_failure_for_one_run_does_not_stop_the_others(monkeypatch) -> None:  # noqa: ANN001
    """A query failure mid-pass (e.g. the DB drops between pending_bars
    and the cursor UPDATE) must not propagate out of run_supervisor and
    kill delivery for every other run in the same pass. The failing
    run's cursor was never advanced, so its bar is simply retried next
    pass."""
    from trading.live import supervisor

    stop = threading.Event()

    class _FakeDB:
        def get(self):  # noqa: ANN202
            return MagicMock()

    class _FakePubSub:
        def get_message(self, timeout):  # noqa: ANN001, ANN202, ARG002
            return None

    monkeypatch.setattr(supervisor, "ReconnectingConnection", lambda *a, **k: _FakeDB())  # noqa: ARG005
    monkeypatch.setattr(supervisor, "SyncResilientPubSub", lambda *a, **k: _FakePubSub())  # noqa: ARG005

    run1 = _run([], live_run_id=1)
    run2 = _run([], live_run_id=2)
    populated = {"done": False}

    def _fake_reconcile(conn, runs):  # noqa: ANN001, ANN202, ARG001
        if not populated["done"]:
            runs[1] = run1
            runs[2] = run2
            populated["done"] = True

    monkeypatch.setattr(supervisor, "reconcile", _fake_reconcile)

    delivered: list[int] = []

    def _fake_deliver(conn, api_url, run, now):  # noqa: ANN001, ANN202, ARG001
        if run.live_run_id == 1:
            raise psycopg.OperationalError("boom mid-pass")
        delivered.append(run.live_run_id)
        stop.set()
        return True

    monkeypatch.setattr(supervisor, "deliver_pending", _fake_deliver)

    supervisor.run_supervisor(stop)

    assert delivered == [2]


def test_launch_passes_leverage_to_the_container(monkeypatch) -> None:
    """The actual bug: _launch built SmokePayload WITHOUT leverage=leverage
    even though start_run passes it -- a relaunched perpetual strategy
    silently traded unlevered."""
    import subprocess
    from unittest.mock import MagicMock

    from trading.live import supervisor

    captured: dict[str, object] = {}

    def _fake_encode_payload(payload):  # noqa: ANN001
        captured["leverage"] = payload.leverage
        return b"x"

    monkeypatch.setattr(supervisor, "encode_payload", _fake_encode_payload)
    fake_process = MagicMock(spec=subprocess.Popen)
    fake_process.stdin = MagicMock()
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fake_process)

    supervisor._launch(
        MagicMock(),
        live_run_id=1,
        strategy_id=1,
        portfolio_id=1,
        source="x",
        instrument_ids=[1],
        schedules=(),
        starting_cash=Decimal("1000"),
        started_at=datetime(2026, 9, 4, tzinfo=UTC),
        leverage=Decimal("20"),
    )
    assert captured["leverage"] == Decimal("20")


def test_launch_restores_counters_so_idempotency_keys_never_repeat(monkeypatch) -> None:
    """The actual bug: a relaunched LiveRun starts bars_seen/orders_placed
    at 0, so place_order's idempotency key `live-{id}-{seq}` repeats a
    key already used before the restart -- the gateway then returns the
    OLD order instead of placing a new one."""
    import subprocess
    from unittest.mock import MagicMock

    from trading.live import supervisor

    monkeypatch.setattr(supervisor, "encode_payload", lambda payload: b"x")
    fake_process = MagicMock(spec=subprocess.Popen)
    fake_process.stdin = MagicMock()
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fake_process)

    run = supervisor._launch(
        MagicMock(),
        live_run_id=1,
        strategy_id=1,
        portfolio_id=1,
        source="x",
        instrument_ids=[1],
        schedules=(),
        starting_cash=Decimal("1000"),
        started_at=datetime(2026, 9, 4, tzinfo=UTC),
        bars_seen=42,
        orders_placed=7,
        orders_refused=2,
        last_refusal="stale",
        strategy_state={"seen": 5},
    )
    assert run.bars_seen == 42
    assert run.orders_placed == 7
    assert run.orders_refused == 2
    assert run.last_refusal == "stale"


def test_handle_bar_persists_the_orders_frames_state_column(db_conn, monkeypatch) -> None:
    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _seed_live_run(db_conn, started_at=datetime(2026, 9, 25, tzinfo=UTC))
    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    run = _run(
        [
            encode_frame(
                FRAME_ORDERS, ts="t", orders=[], alive=True, state={"seen": 1}
            )
        ],
        live_run_id=live_run_id,
    )

    # _bar()'s hardcoded instrument_id=1 does not exist in this test's
    # instruments table -- advance_cursor's FK requires the seeded iid,
    # matching every other db_conn test in this file.
    bar = {**_bar(), "instrument_id": iid}
    assert supervisor.handle_bar(db_conn, "http://x", run, bar) is True

    row = db_conn.execute(
        "SELECT strategy_state FROM live_runs WHERE live_run_id=%s", (live_run_id,)
    ).fetchone()
    assert row[0] == {"seen": 1}


def test_start_run_carries_state_max_bytes_from_settings(monkeypatch) -> None:
    """The actual bug: start_run (the fresh-launch path) built SmokePayload
    without state_max_bytes=get_settings().live_state_max_bytes, unlike
    _launch -- so tuning Settings.live_state_max_bytes only ever affected a
    relaunch, never a strategy's first run."""
    import subprocess
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from trading.live import supervisor

    captured: dict[str, object] = {}

    def _fake_encode_payload(payload):  # noqa: ANN001
        captured["state_max_bytes"] = payload.state_max_bytes
        return b"x"

    monkeypatch.setattr(supervisor, "encode_payload", _fake_encode_payload)
    monkeypatch.setattr(
        supervisor, "get_settings", lambda: SimpleNamespace(live_state_max_bytes=131072)
    )
    fake_process = MagicMock(spec=subprocess.Popen)
    fake_process.stdin = MagicMock()
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fake_process)

    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (1, datetime(2026, 9, 4, tzinfo=UTC))

    supervisor.start_run(
        conn,
        strategy_id=1,
        portfolio_id=1,
        source="x",
        instrument_ids=[1],
        schedules=(),
        starting_cash=Decimal("1000"),
        slippage_bps=Decimal("5"),
    )

    assert captured["state_max_bytes"] == 131072


def test_a_relaunched_runs_next_order_never_reuses_an_idempotency_key(
    db_conn, monkeypatch
) -> None:
    """End-to-end characterization: a run relaunched with restored counters
    places its next order under an idempotency key that was never used
    before the restart. place_order builds the key as
    f"live-{run.live_run_id}-{seq}" where handle_bar passes
    run.orders_placed (the count of orders already placed) as seq -- so a
    run restored with orders_placed=42 must send seq=42, never repeating
    any of the 42 keys (0..41) already used before the crash."""
    import urllib.request

    from trading.live import supervisor
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _seed_live_run(db_conn, started_at=datetime(2026, 9, 25, tzinfo=UTC))
    db_conn.execute(
        "UPDATE live_runs SET bars_seen=%s, orders_placed=%s WHERE live_run_id=%s",
        (100, 42, live_run_id),
    )

    captured: dict[str, object] = {}

    class _Response:
        status = 201

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    def _urlopen(request, timeout=None):  # noqa: ANN001, ANN202, ARG001
        captured.update(json.loads(request.data))
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", _urlopen)

    run = _run(
        [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)],
        live_run_id=live_run_id,
    )
    run.orders_placed = 42
    run.instrument_ids = {iid}

    bar = {**_bar(), "instrument_id": iid}
    assert supervisor.handle_bar(db_conn, "http://x", run, bar) is True

    assert captured["idempotency_key"] == f"live-{live_run_id}-42"
    assert run.orders_placed == 43


def test_run_supervisor_places_orders_through_the_configured_gateway_url(monkeypatch) -> None:  # noqa: ANN001
    """The supervisor used to hardcode http://localhost:8000."""
    from trading.config import get_settings
    from trading.live import supervisor

    stop = threading.Event()

    class _FakeDB:
        def get(self):  # noqa: ANN202
            return MagicMock()

    class _FakePubSub:
        def get_message(self, timeout):  # noqa: ANN001, ANN202, ARG002
            return None

    monkeypatch.setattr(supervisor, "ReconnectingConnection", lambda *a, **k: _FakeDB())  # noqa: ARG005
    monkeypatch.setattr(supervisor, "SyncResilientPubSub", lambda *a, **k: _FakePubSub())  # noqa: ARG005
    run = _run([], live_run_id=1)
    monkeypatch.setattr(supervisor, "reconcile", lambda conn, runs: runs.setdefault(1, run))  # noqa: ARG005

    seen: list[str] = []

    def _fake_deliver(conn, api_url, run, now):  # noqa: ANN001, ANN202, ARG001
        seen.append(api_url)
        stop.set()
        return True

    monkeypatch.setattr(supervisor, "deliver_pending", _fake_deliver)

    supervisor.run_supervisor(stop)

    assert seen == [get_settings().gateway_url]
