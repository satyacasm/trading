"""Storing a smoke run (contract §9 stage 2).

The load-bearing property here is not "can we insert a row" but that a
stored run stays *interpretable*: which window it saw, how well isolated
it was, and why its orders bounced. A pass read six months from now has
nothing but this row to go on.
"""

from __future__ import annotations

import pytest

from trading.agent_contract.smoke import SmokeVerdict, record_smoke_run
from trading.agent_contract.validation import Finding, ValidationReport


def _verdict(**overrides) -> SmokeVerdict:  # noqa: ANN003
    base = dict(
        passed=True,
        warnings_only=True,
        report=ValidationReport(findings=(Finding(code="NO_ORDERS", message="0 orders"),)),
        window={
            "start": "2026-08-27T00:00:00+00:00",
            "end": "2026-09-02T00:00:00+00:00",
            "sessions": 5,
            "instruments": {"1401": {"bars": 1875}},
        },
        outcome={
            "ok": True,
            "bar_calls": 1875,
            "orders": [],
            "fills": 0,
            "rejections": [],
            "final_cash": "100000",
            "final_equity": "100000",
            "breaker_reason": None,
            "logs": [],
            "error": None,
            "crashed_at": None,
        },
        runtime="runc",
        kernel_isolated=False,
    )
    base.update(overrides)
    return SmokeVerdict(**base)  # type: ignore[arg-type]


@pytest.mark.db
def test_a_warned_pass_is_stored_with_its_counts(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, _verdict())
    row = db_conn.execute(
        "SELECT verdict, sessions, bar_calls, orders_placed, fills, runtime, kernel_isolated "
        "FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row == ("PASSED_WITH_WARNINGS", 5, 1875, 0, 0, "runc", False)


@pytest.mark.db
def test_a_clean_pass_is_labelled_differently_from_a_warned_one(
    db_conn,
    registered_strategy_id,  # noqa: ANN001
) -> None:
    # The vacuity guard on the label: a `verdict` column that always said
    # PASSED_WITH_WARNINGS would satisfy the test above.
    clean = _verdict(warnings_only=False, report=ValidationReport(findings=()))
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, clean)
    row = db_conn.execute(
        "SELECT verdict FROM strategy_smoke_runs WHERE smoke_run_id = %s", (smoke_run_id,)
    ).fetchone()
    assert row[0] == "PASSED"


@pytest.mark.db
def test_a_rejection_is_stored_with_its_findings(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    verdict = _verdict(
        passed=False,
        warnings_only=False,
        report=ValidationReport(findings=(Finding(code="SMOKE_CRASH", message="boom"),)),
        outcome=None,
    )
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, verdict)
    row = db_conn.execute(
        "SELECT verdict, findings FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row[0] == "REJECTED"
    assert row[1][0]["code"] == "SMOKE_CRASH"


@pytest.mark.db
def test_why_orders_bounced_survives_the_write(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    """A count records that three orders bounced; only the reasons say
    what to change. `findings` carries a reason for ALL_ORDERS_REJECTED
    runs and only the first one, so a run where *some* orders bounced
    would otherwise store none at all."""
    reasons = ["insufficient funds: need 12,400 INR, have 900", "quantity below lot size"]
    verdict = _verdict(
        outcome={
            **(_verdict().outcome or {}),
            "orders": [{"order_id": 1}, {"order_id": 2}, {"order_id": 3}],
            "rejections": reasons,
        }
    )
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, verdict)
    row = db_conn.execute(
        "SELECT rejections, rejection_reasons FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row[0] == 2
    assert row[1] == reasons


@pytest.mark.db
def test_a_version_can_be_smoked_more_than_once(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    # D-S4: the window moves, so a second run against the same immutable
    # version is a new fact, not an overwrite of the old one.
    record_smoke_run(db_conn, registered_strategy_id, _verdict())
    record_smoke_run(db_conn, registered_strategy_id, _verdict())
    count = db_conn.execute(
        "SELECT COUNT(*) FROM strategy_smoke_runs WHERE strategy_id = %s",
        (registered_strategy_id,),
    ).fetchone()[0]
    assert count == 2


@pytest.mark.db
def test_the_window_a_run_saw_is_stored_not_just_that_it_passed(
    db_conn,
    registered_strategy_id,  # noqa: ANN001
) -> None:
    """The reason this is a table and not a `smoked: true` flag: the same
    immutable version re-smoked next week meets different bars, so a pass
    is only interpretable alongside the window that produced it."""
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, _verdict())
    row = db_conn.execute(
        "SELECT window_start, window_end, instruments "
        "FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row[0].isoformat() == "2026-08-27T00:00:00+00:00"
    assert row[1].isoformat() == "2026-09-02T00:00:00+00:00"
    assert row[2] == {"1401": {"bars": 1875}}
