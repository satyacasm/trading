"""Storing what a backtest produced, and reading it back.

Separate from `smoke.py` deliberately. That module orchestrates -- it
resolves manifests, resolves universes, fetches bars and drives containers
-- and it is already large. Storage is a different responsibility with a
different transaction rule, and it is what 3d, 3e and 3f import: reading a
stored curve should not require importing the backtester that produced it.

**Nothing here commits.** The caller owns the transaction boundary, so a
run row and its equity points land as one unit or not at all -- a
half-written curve is not a state a metrics layer should have to defend
against. This is the discipline `record_smoke_run` follows, for the same
reason.

Only runs that reached the container are stored, `PASSED` or `FAILED`.
A crash is stored *with its partial curve*, because that curve says where
the run died and is what diagnoses the platform rather than the strategy.
Pre-flight refusals are returned to their caller and never stored: each is
a deterministic function of the request and the data available, so
re-deriving one costs a COUNT, and a stored row would imply to a later
reader that a run happened.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from psycopg import Connection

from trading.agent_contract.registry import CONTRACT_VERSION

__all__ = [
    "get_backtest_run",
    "identical_run_count",
    "list_backtest_runs",
    "record_backtest_run",
]

_INSERT_RUN = """
    INSERT INTO backtest_runs (
        strategy_id, status, requested_start, requested_end, fetch_start,
        dispatch_from, sessions, instruments, history_bars_requested,
        history_bars_available, bars, bar_calls, orders_placed, fills,
        final_cash, final_equity, breaker_reason, error, findings,
        runtime, kernel_isolated, contract_version, stress
    ) VALUES (
        %(strategy_id)s, %(status)s, %(requested_start)s, %(requested_end)s,
        %(fetch_start)s, %(dispatch_from)s, %(sessions)s, %(instruments)s,
        %(history_bars_requested)s, %(history_bars_available)s, %(bars)s,
        %(bar_calls)s, %(orders_placed)s, %(fills)s, %(final_cash)s,
        %(final_equity)s, %(breaker_reason)s, %(error)s, %(findings)s,
        %(runtime)s, %(kernel_isolated)s, %(contract_version)s, %(stress)s
    ) RETURNING backtest_run_id
"""

_FILL_COLUMNS = (
    "ts",
    "instrument_id",
    "side",
    "product",
    "quantity",
    "price",
    "brokerage",
    "stt",
    "exchange_txn",
    "sebi_fee",
    "stamp_duty",
    "ipft",
    "gst",
    "dp_charges",
    "tds",
    "total_charges",
)

_INSERT_FILL = f"""
    INSERT INTO backtest_fills (
        backtest_run_id, ordinal, {", ".join(_FILL_COLUMNS)}
    ) VALUES ({", ".join(["%s"] * (len(_FILL_COLUMNS) + 2))})
"""

_SELECT_FILLS = f"""
    SELECT ordinal, {", ".join(_FILL_COLUMNS)} FROM backtest_fills
    WHERE backtest_run_id = %s ORDER BY ordinal
"""

_INSERT_POINT = """
    INSERT INTO backtest_equity_points (backtest_run_id, ts, equity, cash)
    VALUES (%s, %s, %s, %s)
"""


def _money(raw: Any) -> Decimal | None:
    """Money arrives from the container as a string, by design (JSON numbers
    are IEEE 754 doubles). Parsed to `Decimal` here so the driver binds a
    numeric and no float ever touches the value."""
    return None if raw is None else Decimal(str(raw))


def record_backtest_run(
    conn: Connection,
    strategy_id: int,
    verdict: Any,
    *,
    requested_start: date,
    requested_end: date,
    instrument_ids: Sequence[int],
) -> int:
    """Store one executed backtest and its curve. Does not commit.

    Call this only for a run that reached the container. `verdict.plan` and
    `verdict.outcome` are both non-None in that case and both None for a
    pre-flight refusal, which is how the caller tells them apart.
    """
    plan = verdict.plan
    outcome = verdict.outcome or {}
    curve = outcome.get("equity_curve") or []

    row = conn.execute(
        _INSERT_RUN,
        {
            "strategy_id": strategy_id,
            "status": "PASSED" if verdict.passed else "FAILED",
            "requested_start": requested_start,
            "requested_end": requested_end,
            "fetch_start": plan.start,
            "dispatch_from": plan.dispatch_from,
            "sessions": plan.sessions,
            # The resolved universe, sorted, not a count: the same manifest
            # can resolve differently as listings change, and a count would
            # record that something was traded without recording what.
            "instruments": json.dumps(sorted(int(i) for i in instrument_ids)),
            "history_bars_requested": plan.history_bars_requested,
            "history_bars_available": plan.history_bars_available,
            "bars": verdict.bars,
            "bar_calls": outcome.get("bar_calls", 0),
            "orders_placed": len(outcome.get("orders") or []),
            "fills": outcome.get("fills", 0),
            "final_cash": _money(outcome.get("final_cash")),
            "final_equity": _money(outcome.get("final_equity")),
            "breaker_reason": outcome.get("breaker_reason"),
            "error": outcome.get("error"),
            "findings": json.dumps(
                [
                    {
                        "code": f.code,
                        "message": f.message,
                        "line": f.line,
                        "contract_section": f.contract_section,
                    }
                    for f in verdict.report.findings
                ]
            ),
            "runtime": verdict.runtime or "",
            "kernel_isolated": bool(verdict.kernel_isolated),
            "contract_version": CONTRACT_VERSION,
            "stress": None
            if getattr(verdict, "stress", None) is None
            else json.dumps(verdict.stress),
        },
    ).fetchone()
    assert row is not None  # noqa: S101 - RETURNING always yields a row
    run_id = int(row[0])

    ledger = outcome.get("fill_ledger") or []
    if ledger:
        # `ordinal` is the run's own sequence, which FIFO round-trip
        # matching depends on and a timestamp cannot supply: two fills can
        # legitimately share one bar.
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_FILL,
                [
                    (run_id, index, *(fill[column] for column in _FILL_COLUMNS))
                    for index, fill in enumerate(ledger)
                ],
            )

    if curve:
        # `executemany`, not COPY: this codebase reserves COPY for the bulk
        # bar loader at row counts three orders of magnitude larger, and a
        # daily decade is ~2,600 points. The table shape (0012) is what
        # leaves COPY available the day intraday curves arrive.
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_POINT,
                [(run_id, p["ts"], _money(p["equity"]), _money(p["cash"])) for p in curve],
            )
    return run_id


_RUN_COLUMNS = """
    backtest_run_id, strategy_id, status, requested_start, requested_end,
    fetch_start, dispatch_from, sessions, instruments, history_bars_requested,
    history_bars_available, bars, bar_calls, orders_placed, fills,
    final_cash, final_equity, breaker_reason, error, findings,
    runtime, kernel_isolated, contract_version, ran_at, stress
"""

_SELECT_RUNS = f"""
    SELECT {_RUN_COLUMNS} FROM backtest_runs
    WHERE strategy_id = %s
    ORDER BY ran_at DESC, backtest_run_id DESC
    LIMIT %s
"""

_SELECT_RUN = f"SELECT {_RUN_COLUMNS} FROM backtest_runs WHERE backtest_run_id = %s"

_SELECT_POINTS = """
    SELECT ts, equity, cash FROM backtest_equity_points
    WHERE backtest_run_id = %s ORDER BY ts
"""


def _run_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    """One run row, money rendered as strings.

    `str(Decimal)` rather than `float()`: JSON has no decimal type, so a
    number here would reach every client as a double -- the one
    representation this codebase refuses to let money take.
    """
    return {
        "backtest_run_id": row[0],
        "strategy_id": row[1],
        "status": row[2],
        "requested_start": row[3].isoformat(),
        "requested_end": row[4].isoformat(),
        "fetch_start": row[5].isoformat(),
        "dispatch_from": row[6].isoformat(),
        "sessions": row[7],
        "instruments": row[8],
        "history_bars_requested": row[9],
        "history_bars_available": row[10],
        "bars": row[11],
        "bar_calls": row[12],
        "orders_placed": row[13],
        "fills": row[14],
        "final_cash": None if row[15] is None else str(row[15]),
        "final_equity": None if row[16] is None else str(row[16]),
        "breaker_reason": row[17],
        "error": row[18],
        "findings": row[19],
        "runtime": row[20],
        "kernel_isolated": row[21],
        "contract_version": row[22],
        "ran_at": row[23].isoformat(),
        "stress": row[24],
    }


def list_backtest_runs(
    conn: Connection, strategy_id: int, *, limit: int = 50
) -> list[dict[str, Any]]:
    """One strategy's runs, newest first, WITHOUT their curves.

    The omission is the point: a history view would otherwise transfer every
    point of every run to render a table of dates and final equities.
    """
    rows = conn.execute(_SELECT_RUNS, (strategy_id, limit)).fetchall()
    return [_run_to_dict(row) for row in rows]


def get_backtest_run(conn: Connection, backtest_run_id: int) -> dict[str, Any]:
    """One run with its equity curve, in `ts` order.

    Raises `KeyError` if it does not exist -- a missing run is a caller bug,
    not an empty result, matching `registry.get_strategy`.
    """
    row = conn.execute(_SELECT_RUN, (backtest_run_id,)).fetchone()
    if row is None:
        raise KeyError(f"no backtest run with backtest_run_id={backtest_run_id}")
    run = _run_to_dict(row)
    run["fills_ledger"] = [
        dict(
            zip(
                ("ordinal", *_FILL_COLUMNS),
                (row[0], row[1].isoformat(), *map(str, row[2:])),
                strict=True,
            )
        )
        for row in conn.execute(_SELECT_FILLS, (backtest_run_id,)).fetchall()
    ]
    run["equity_curve"] = [
        {"ts": ts.isoformat(), "equity": str(equity), "cash": str(cash)}
        for ts, equity, cash in conn.execute(_SELECT_POINTS, (backtest_run_id,)).fetchall()
    ]
    return run


_COUNT_IDENTICAL_RUNS = """
    SELECT count(*) FROM backtest_runs
    WHERE strategy_id = %s AND requested_start = %s AND requested_end = %s
"""


def identical_run_count(conn: Connection, strategy_id: int, start: date, end: date) -> int:
    """How many times this exact window has been run for this strategy.

    §6 asks for "a gentle warning when a user re-runs the same strategy many
    times on identical data". Now that runs are stored, the overfitting
    guardrail is a COUNT rather than a feature.
    """
    row = conn.execute(_COUNT_IDENTICAL_RUNS, (strategy_id, start, end)).fetchone()
    return 0 if row is None else int(row[0])
