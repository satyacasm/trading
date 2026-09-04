"""The 3c store: what a backtest produced, kept.

`db_conn` is the rolled-back transaction fixture, which is also what makes
the atomicity property honest here -- nothing in this module commits,
exactly as `record_backtest_run` does not.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.db


def _verdict(points, *, passed=True, error=None):  # noqa: ANN001, ANN201
    from trading.agent_contract.smoke import BacktestPlan, BacktestVerdict
    from trading.agent_contract.validation import ValidationReport

    plan = BacktestPlan(
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 31, tzinfo=UTC),
        dispatch_from=datetime(2024, 1, 8, tzinfo=UTC),
        history_bars_requested=5,
        history_bars_available=5,
        instruments=1,
        sessions=20,
    )
    return BacktestVerdict(
        passed=passed,
        report=ValidationReport(findings=()),
        plan=plan,
        bars="1d",
        outcome={
            "ok": passed,
            "bar_calls": len(points),
            "orders": [],
            "fills": 1,
            "final_cash": "924286.24",
            "final_equity": "1055886.2400",
            "breaker_reason": None,
            "error": error,
            "equity_curve": points,
        },
        runtime="runsc",
        kernel_isolated=True,
    )


def _strategy(db_conn, name="persist-fixture") -> int:  # noqa: ANN001
    user = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    return db_conn.execute(
        "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
        "status, contract_version) VALUES (%s,%s,'1.0.0','x','y','REGISTERED','0.1') "
        "RETURNING strategy_id",
        (user, name),
    ).fetchone()[0]


_POINTS = [
    {"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"},
    {"ts": "2024-01-09T10:00:00+00:00", "equity": "1000123.4567", "cash": "924286.2400"},
    {"ts": "2024-01-10T10:00:00+00:00", "equity": "999876.5433", "cash": "924286.2400"},
]


def test_the_curve_round_trips_exactly_in_value_and_order(db_conn) -> None:  # noqa: ANN001
    """Money is stored as numeric(18,4) and must come back as the same
    Decimal, not a float that compares approximately.

    This is the money path, and this codebase's experience is that a wrong
    number survives a green suite comfortably -- six quantization
    asymmetries were found by review, not by tests. Asserting exact
    Decimals and the type is what makes routing a value through float()
    redden this test.
    """
    from trading.agent_contract.persistence import record_backtest_run

    strategy_id = _strategy(db_conn)
    run_id = record_backtest_run(
        db_conn,
        strategy_id,
        _verdict(_POINTS),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )

    rows = db_conn.execute(
        "SELECT ts, equity, cash FROM backtest_equity_points WHERE backtest_run_id=%s ORDER BY ts",
        (run_id,),
    ).fetchall()

    assert [r[0].isoformat() for r in rows] == [p["ts"] for p in _POINTS]
    assert [r[1] for r in rows] == [
        Decimal("1000000.0000"),
        Decimal("1000123.4567"),
        Decimal("999876.5433"),
    ]
    assert all(isinstance(r[1], Decimal) for r in rows)
    assert all(isinstance(r[2], Decimal) for r in rows)


def test_the_run_row_records_the_request_and_the_resolved_window(db_conn) -> None:  # noqa: ANN001
    """They differ by warm-up, and the difference is deliberate. Storing only
    one of them leaves a run's bar_calls unexplainable a week later."""
    from trading.agent_contract.persistence import record_backtest_run

    run_id = record_backtest_run(
        db_conn,
        _strategy(db_conn),
        _verdict(_POINTS),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607, 101],
    )
    row = db_conn.execute(
        "SELECT status, requested_start, requested_end, fetch_start, dispatch_from, "
        "sessions, instruments, history_bars_requested, history_bars_available, bars, "
        "bar_calls, fills, final_cash, final_equity, runtime, kernel_isolated "
        "FROM backtest_runs WHERE backtest_run_id=%s",
        (run_id,),
    ).fetchone()

    assert row[0] == "PASSED"
    assert (row[1], row[2]) == (date(2024, 1, 8), date(2024, 1, 10))
    # The fetch reaches earlier than dispatch: that IS the warm-up.
    assert row[3] < row[4]
    assert row[5] == 20
    # The resolved universe, sorted -- not a count.
    assert row[6] == [101, 58607]
    assert (row[7], row[8]) == (5, 5)
    assert row[9] == "1d"
    assert row[10] == 3
    assert row[12] == Decimal("924286.2400")
    assert row[13] == Decimal("1055886.2400")
    # A stored run must never read as better isolated than it was.
    assert (row[14], row[15]) == ("runsc", True)


def test_a_crashed_run_is_stored_with_its_partial_curve(db_conn) -> None:  # noqa: ANN001
    """The most useful artifact in this table, and the one an "only store
    successes" implementation quietly drops.

    `run_loop` returns the curve on its crash path too, and a partial curve
    says WHERE a run died -- which is what diagnoses the platform rather
    than the strategy. 3b's 64 KiB gVisor truncation was found from exactly
    this kind of evidence.
    """
    from trading.agent_contract.persistence import record_backtest_run

    run_id = record_backtest_run(
        db_conn,
        _strategy(db_conn),
        _verdict(_POINTS[:1], passed=False, error="[SMOKE_OOM] container was OOM-killed"),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )

    status, error = db_conn.execute(
        "SELECT status, error FROM backtest_runs WHERE backtest_run_id=%s", (run_id,)
    ).fetchone()
    assert status == "FAILED"
    assert "SMOKE_OOM" in error
    assert (
        db_conn.execute(
            "SELECT count(*) FROM backtest_equity_points WHERE backtest_run_id=%s", (run_id,)
        ).fetchone()[0]
        == 1
    )


def test_the_write_does_not_commit(db_conn, db_url) -> None:  # noqa: ANN001
    """The caller owns the transaction boundary, so a run and its points land
    as one unit or not at all -- a half-written curve is not a state a
    metrics layer should have to defend against.

    Asserted by rolling back and looking from a connection that could only
    see committed data.
    """
    import psycopg

    from trading.agent_contract.persistence import record_backtest_run

    run_id = record_backtest_run(
        db_conn,
        _strategy(db_conn),
        _verdict(_POINTS),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )
    db_conn.rollback()

    with psycopg.connect(db_url, autocommit=True) as other:
        assert (
            other.execute(
                "SELECT count(*) FROM backtest_runs WHERE backtest_run_id=%s", (run_id,)
            ).fetchone()[0]
            == 0
        )
        assert (
            other.execute(
                "SELECT count(*) FROM backtest_equity_points WHERE backtest_run_id=%s", (run_id,)
            ).fetchone()[0]
            == 0
        )


def test_money_is_never_routed_through_a_float(db_conn) -> None:  # noqa: ANN001
    """The guard the other tests do NOT provide, discovered by mutation.

    Replacing `Decimal(str(raw))` with `float(raw)` left every other test in
    this module green: psycopg binds the float, Postgres casts it to
    numeric(18,4), and around 10^6 with four decimals -- roughly eleven
    significant digits -- a float64 is exact enough that the round trip is
    indistinguishable. Those tests were checking the column's behaviour, not
    the code's.

    `numeric(18,4)` promises eighteen significant digits and a float64
    carries fifteen to seventeen, so a value at the column's limit is where
    the two part company:

        Decimal("12345678901234.5678")  -> 12345678901234.5678
        float  ("12345678901234.5678")  -> 12345678901234.568

    Unrealistic as an equity, entirely realistic as a promise the column
    makes. A money path that cannot keep what its column stores is broken
    whether or not today's magnitudes expose it.
    """
    from trading.agent_contract.persistence import record_backtest_run

    exact = "12345678901234.5678"
    run_id = record_backtest_run(
        db_conn,
        _strategy(db_conn),
        _verdict([{"ts": "2024-01-08T10:00:00+00:00", "equity": exact, "cash": exact}]),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 8),
        instrument_ids=[58607],
    )

    equity, cash = db_conn.execute(
        "SELECT equity, cash FROM backtest_equity_points WHERE backtest_run_id=%s",
        (run_id,),
    ).fetchone()
    assert equity == Decimal(exact)
    assert cash == Decimal(exact)
