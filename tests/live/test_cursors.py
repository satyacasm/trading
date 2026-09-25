"""pending_bars/advance_cursor: the supervisor's "deliver everything
after my cursor" mechanism (design §4) -- covers a missed pub/sub
message, an aggregator restart, and a supervisor restart with one
mechanism, because Postgres (not Redis) is the record."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trading.live.cursors import advance_cursor, pending_bars
from trading.streaming.seed_instruments import seed_crypto_instruments


def _live_run(db_conn) -> int:
    """A minimal strategies/portfolios/live_runs row, matching the real
    NOT NULL columns in migrations/versions/0010_strategies.py and
    0007_paper_trading_core.py -- mirrors
    tests/agent_contract/test_backtest_persistence.py's own `_strategy`
    helper rather than inventing a second shape."""
    user_id = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    portfolio_id = db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance) "
        "VALUES (%s, 'cursors-test', 'USDT', 1000, 1000) RETURNING portfolio_id",
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
        (strategy_id, portfolio_id, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
    ).fetchone()
    return row[0]


def _bar(db_conn, iid: int, minute: int, close: str = "100") -> None:
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 10, minute, tzinfo=UTC), close, close, close, close),
    )


def test_a_late_bar_for_one_instrument_is_still_delivered(db_conn) -> None:
    ids = seed_crypto_instruments(db_conn, pairs=["BTC-USDT", "ETH-USDT"])
    btc, eth = ids["BTC-USDT"], ids["ETH-USDT"]
    live_run_id = _live_run(db_conn)
    _bar(db_conn, btc, 5)
    _bar(db_conn, eth, 5)
    # BTC's cursor already moved to 10:05; ETH's has not -- a run-wide
    # cursor would have skipped ETH's bar entirely.
    advance_cursor(db_conn, live_run_id, btc, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))

    pending, gap_note = pending_bars(
        db_conn,
        live_run_id,
        [btc, eth],
        started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
        catchup_after=timedelta(minutes=2),
        replay_cap=timedelta(hours=24),
    )
    assert gap_note is None
    assert [p.frame["instrument_id"] for p in pending] == [eth]
    assert pending[0].frame["ts"] == "2026-09-25T10:05:00+00:00"
    assert pending[0].catchup is False


def test_repeated_call_after_advance_returns_nothing(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _live_run(db_conn)
    _bar(db_conn, iid, 5)

    pending, _ = pending_bars(
        db_conn, live_run_id, [iid],
        started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
        catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
    )
    advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))

    pending_again, gap_note = pending_bars(
        db_conn, live_run_id, [iid],
        started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
        catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
    )
    assert pending and pending_again == []
    assert gap_note is None


def test_a_bar_older_than_the_replay_cap_is_skipped_and_noted(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _live_run(db_conn)
    _bar(db_conn, iid, 0)  # 10:00 -- older than a 1-hour cap from 12:00
    _bar(db_conn, iid, 5)

    pending, gap_note = pending_bars(
        db_conn, live_run_id, [iid],
        started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        now=datetime(2026, 9, 25, 12, 0, tzinfo=UTC),
        catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=1),
    )
    assert [p.frame["ts"] for p in pending] == []  # both bars predate the 11:00 floor
    assert gap_note is not None and "replay cap" in gap_note


def test_catchup_boundary_at_exactly_two_minutes(db_conn) -> None:
    """A bar's close (ts + 60s) more than 2 minutes before delivery is
    catchup; at or under 2 minutes it is not (design §4)."""
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _live_run(db_conn)
    _bar(db_conn, iid, 0)  # close at 10:01:00

    # now = 10:03:00 -> now - close = 120s = exactly catchup_after: NOT catchup
    pending, _ = pending_bars(
        db_conn, live_run_id, [iid],
        started_at=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
        now=datetime(2026, 9, 25, 10, 3, 0, tzinfo=UTC),
        catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
    )
    assert pending[0].catchup is False

    advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 9, 0, tzinfo=UTC))
    # now = 10:03:01 -> 121s: catchup
    pending2, _ = pending_bars(
        db_conn, live_run_id, [iid],
        started_at=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
        now=datetime(2026, 9, 25, 10, 3, 1, tzinfo=UTC),
        catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
    )
    assert pending2[0].catchup is True


def test_advance_cursor_never_moves_backwards(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    live_run_id = _live_run(db_conn)
    advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))
    advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 2, tzinfo=UTC))
    row = db_conn.execute(
        "SELECT last_ts FROM live_run_cursors WHERE live_run_id=%s AND instrument_id=%s",
        (live_run_id, iid),
    ).fetchone()
    assert row[0] == datetime(2026, 9, 25, 10, 5, tzinfo=UTC)
