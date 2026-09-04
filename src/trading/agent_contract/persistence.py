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

__all__ = ["record_backtest_run"]

_INSERT_RUN = """
    INSERT INTO backtest_runs (
        strategy_id, status, requested_start, requested_end, fetch_start,
        dispatch_from, sessions, instruments, history_bars_requested,
        history_bars_available, bars, bar_calls, orders_placed, fills,
        final_cash, final_equity, breaker_reason, error, findings,
        runtime, kernel_isolated, contract_version
    ) VALUES (
        %(strategy_id)s, %(status)s, %(requested_start)s, %(requested_end)s,
        %(fetch_start)s, %(dispatch_from)s, %(sessions)s, %(instruments)s,
        %(history_bars_requested)s, %(history_bars_available)s, %(bars)s,
        %(bar_calls)s, %(orders_placed)s, %(fills)s, %(final_cash)s,
        %(final_equity)s, %(breaker_reason)s, %(error)s, %(findings)s,
        %(runtime)s, %(kernel_isolated)s, %(contract_version)s
    ) RETURNING backtest_run_id
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
        },
    ).fetchone()
    assert row is not None  # noqa: S101 - RETURNING always yields a row
    run_id = int(row[0])

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
