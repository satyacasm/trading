# Persisting backtest runs and their equity curves — Implementation Plan (Phase 3, 3c)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store what a backtest ran and what it produced, and let it be read back.

**Architecture:** Two tables (migration `0012`), one write function following
`record_smoke_run`'s transaction discipline exactly, and two read routes.
No analysis: 3d computes metrics from what this stores.

**Tech Stack:** Python 3.12, psycopg 3 (sync), alembic, FastAPI, pytest,
TimescaleDB.

**Spec:** `docs/superpowers/specs/2026-09-04-backtest-persistence-design.md`

## Global Constraints

- **C1. FastAPI routes are `def`, never `async def`.** psycopg is
  synchronous; an async route running a blocking DB call on the event loop
  deadlocked this gateway permanently once already.
  `test_no_route_is_a_coroutine_function` guards it. **GETs must never write.**
- **C2. Money is `numeric(18,4)` in storage and `str` on the wire.** JSON
  numbers are IEEE 754 doubles. Never let a `float` touch a money value in
  either direction.
- **C3. The write path does not commit.** `record_backtest_run` writes and
  returns; the caller owns the transaction boundary, so a run and its
  points land as one unit. Same rule `record_smoke_run` follows.
- **C4. Only executed runs are stored** — `PASSED` or `FAILED`, crashes
  included with their partial curve. Pre-flight refusals are returned and
  never stored.
- **C5. Verify by mutation, not by a green suite.** For each property:
  break it deliberately and confirm the test reddens.
- **C6. Migration numbering:** latest is `0011`; this is `0012`.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `migrations/versions/0012_backtest_runs.py` | `backtest_runs` + `backtest_equity_points` | 1 |
| `src/trading/agent_contract/persistence.py` | `record_backtest_run`, `get_backtest_run`, `list_backtest_runs` | 2, 4 |
| `src/trading/agent_contract/smoke.py` | nothing — `backtest()` stays pure; the route persists | — |
| `src/trading/agent_contract/api.py` | persist on the POST; two GET routes | 3, 4 |
| `tests/test_migrations.py` | the tables and the composite key exist | 1 |
| `tests/agent_contract/test_backtest_persistence.py` | write path | 2, 3 |
| `tests/agent_contract/test_api.py` | read routes | 4 |

A new module rather than more of `smoke.py`: that file is already ~1,300
lines and owns orchestration. Storage is a different responsibility with a
different transaction rule, and it is what 3d/3e/3f import — they should
not have to import the backtester to read a curve.

---

### Task 1: Migration `0012` — the two tables

**Files:**
- Create: `migrations/versions/0012_backtest_runs.py`
- Test: `tests/test_migrations.py`

**Interfaces:**
- Produces: tables `backtest_runs`, `backtest_equity_points` exactly as in
  the spec's D3c-0.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_migrations.py`:

```python
def test_backtest_tables_exist(db_conn):
    rows = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    assert {"backtest_runs", "backtest_equity_points"} <= {r[0] for r in rows}


def test_an_equity_point_cannot_repeat_a_timestamp_within_a_run(db_conn):
    """`run_loop` emits one point per dispatched bar and bars are grouped by
    close_ts, so timestamps within a run are unique by construction. The
    composite primary key makes the database refuse to let that break: a
    doubled point would reach 3d as a wrong Sharpe and 3e as a real feature
    of the equity path, with nothing anywhere raising an error.
    """
    from datetime import UTC, datetime
    from decimal import Decimal

    import psycopg

    user = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    strategy_id = db_conn.execute(
        "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
        "status, contract_version) VALUES (%s,'dupe-pk','1.0.0','x','y','ACTIVE','0.1') "
        "RETURNING strategy_id",
        (user,),
    ).fetchone()[0]
    run_id = db_conn.execute(
        "INSERT INTO backtest_runs (strategy_id, status, requested_start, requested_end, "
        "fetch_start, dispatch_from, runtime, kernel_isolated, contract_version) "
        "VALUES (%s,'PASSED','2024-01-01','2024-01-02',now(),now(),'runc',false,'0.1') "
        "RETURNING backtest_run_id",
        (strategy_id,),
    ).fetchone()[0]
    ts = datetime(2024, 1, 2, 10, 0, tzinfo=UTC)
    db_conn.execute(
        "INSERT INTO backtest_equity_points (backtest_run_id, ts, equity, cash) "
        "VALUES (%s,%s,%s,%s)",
        (run_id, ts, Decimal("1"), Decimal("1")),
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        db_conn.execute(
            "INSERT INTO backtest_equity_points (backtest_run_id, ts, equity, cash) "
            "VALUES (%s,%s,%s,%s)",
            (run_id, ts, Decimal("2"), Decimal("2")),
        )
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_migrations.py -v -k backtest`
Expected: FAIL — `assert {'backtest_runs', 'backtest_equity_points'} <= {...}` is false; the tables do not exist.

- [ ] **Step 3: Write the migration**

Create `migrations/versions/0012_backtest_runs.py`, following `0011`'s
style (module docstring explaining the load-bearing choices, then
`op.create_table`):

```python
"""Add `backtest_runs` and `backtest_equity_points`: what a backtest produced.

The curve is a table rather than a jsonb column on the run, against the
closer precedent of `strategy_smoke_runs`. Money belongs in numeric(18,4)
like every other money column here; a curve of JSON strings would let a
wire-format constraint (JSON numbers are IEEE 754 doubles) reach into
storage, and would leave min()/max() for drawdown unavailable to SQL.

`(backtest_run_id, ts)` is the primary key, not a surrogate id. `run_loop`
emits one point per dispatched bar and bars are grouped by close_ts, so
timestamps within a run are unique by construction -- making that the key
turns the property into one the database refuses to let break. A doubled
point would otherwise reach 3d as a wrong Sharpe and 3e as a real feature
of the equity path. It also serves `WHERE run_id = ? ORDER BY ts` as a
straight index scan, so no second index is needed.

`status` carries only PASSED and FAILED: refusals are returned to the
caller and never stored, so a REFUSED value would be unreachable, and an
unreachable enum member invites someone to make it reachable.

There is no `updated_at`. A run is an immutable record of an event.

Revision ID: 0012
Revises: 0011
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("backtest_run_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "strategy_id",
            sa.BigInteger,
            sa.ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("requested_start", sa.Date, nullable=False),
        sa.Column("requested_end", sa.Date, nullable=False),
        sa.Column("fetch_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("dispatch_from", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("sessions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("instruments", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("history_bars_requested", sa.Integer, nullable=False, server_default="0"),
        sa.Column("history_bars_available", sa.Integer, nullable=False, server_default="0"),
        sa.Column("bars", sa.Text, nullable=True),
        sa.Column("bar_calls", sa.Integer, nullable=False, server_default="0"),
        sa.Column("orders_placed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("fills", sa.Integer, nullable=False, server_default="0"),
        sa.Column("final_cash", sa.Numeric(18, 4), nullable=True),
        sa.Column("final_equity", sa.Numeric(18, 4), nullable=True),
        sa.Column("breaker_reason", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("findings", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("runtime", sa.Text, nullable=False),
        sa.Column("kernel_isolated", sa.Boolean, nullable=False),
        sa.Column("contract_version", sa.Text, nullable=False),
        sa.Column(
            "ran_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint("status IN ('PASSED','FAILED')", name="ck_backtest_run_status"),
    )
    op.create_index(
        "ix_backtest_runs_strategy", "backtest_runs", ["strategy_id", "ran_at"]
    )
    op.create_table(
        "backtest_equity_points",
        sa.Column(
            "backtest_run_id",
            sa.BigInteger,
            sa.ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash", sa.Numeric(18, 4), nullable=False),
        sa.PrimaryKeyConstraint("backtest_run_id", "ts", name="pk_backtest_equity_points"),
    )


def downgrade() -> None:
    op.drop_table("backtest_equity_points")
    op.drop_index("ix_backtest_runs_strategy", table_name="backtest_runs")
    op.drop_table("backtest_runs")
```

- [ ] **Step 4: Apply it to both databases and run the test**

```bash
uv run alembic upgrade head
TEST_DATABASE_URL_APPLIED=1 uv run pytest tests/test_migrations.py -v -k backtest
```
Expected: PASS. If `trading_test` is migrated separately in this repo,
apply there too — both databases must be at `0012`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(db): backtest_runs and backtest_equity_points (0012)"
```

---

### Task 2: The write path

**Files:**
- Create: `src/trading/agent_contract/persistence.py`
- Test: `tests/agent_contract/test_backtest_persistence.py`

**Interfaces:**
- Consumes: `BacktestVerdict` from `trading.agent_contract.smoke` (fields
  `passed`, `report`, `plan`, `bars`, `outcome`, `runtime`,
  `kernel_isolated`); `BacktestPlan` (`start`, `end`, `dispatch_from`,
  `history_bars_requested`, `history_bars_available`, `instruments`,
  `sessions`).
- Produces: `record_backtest_run(conn, strategy_id, verdict, *, requested_start: date, requested_end: date, instrument_ids: Sequence[int]) -> int`
  returning `backtest_run_id`. **Does not commit.**

- [ ] **Step 1: Write the failing test — the curve round-trips exactly**

Create `tests/agent_contract/test_backtest_persistence.py`:

```python
"""The 3c store: what a backtest produced, kept.

`db_conn` is the rolled-back transaction fixture, which is also what makes
the atomicity test below honest -- nothing here commits, exactly as
`record_backtest_run` does not.
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


def _strategy(db_conn) -> int:  # noqa: ANN001
    user = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    return db_conn.execute(
        "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
        "status, contract_version) VALUES (%s,'persist-fixture','1.0.0','x','y',"
        "'ACTIVE','0.1') RETURNING strategy_id",
        (user,),
    ).fetchone()[0]


def test_the_curve_round_trips_exactly_in_value_and_order(db_conn) -> None:  # noqa: ANN001
    """Money is stored as numeric(18,4) and must come back as the same
    Decimal, not a float that compares approximately. This is the money
    path, and this codebase's experience is that a wrong number survives a
    green suite comfortably.
    """
    from trading.agent_contract.persistence import record_backtest_run

    points = [
        {"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"},
        {"ts": "2024-01-09T10:00:00+00:00", "equity": "1000123.4567", "cash": "924286.2400"},
        {"ts": "2024-01-10T10:00:00+00:00", "equity": "999876.5433", "cash": "924286.2400"},
    ]
    strategy_id = _strategy(db_conn)
    run_id = record_backtest_run(
        db_conn,
        strategy_id,
        _verdict(points),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )

    rows = db_conn.execute(
        "SELECT ts, equity, cash FROM backtest_equity_points "
        "WHERE backtest_run_id=%s ORDER BY ts",
        (run_id,),
    ).fetchall()

    assert [r[0].isoformat() for r in rows] == [p["ts"] for p in points]
    # Exact Decimals. numeric(18,4) rounds the half-cent away; assert what
    # the column actually promises rather than what the string said.
    assert [r[1] for r in rows] == [Decimal("1000000.0000"), Decimal("1000123.4567"), Decimal("999876.5433")]
    assert all(isinstance(r[1], Decimal) for r in rows)
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/agent_contract/test_backtest_persistence.py -v -k round_trips`
Expected: FAIL — `ModuleNotFoundError: trading.agent_contract.persistence`.

- [ ] **Step 3: Write the module**

Create `src/trading/agent_contract/persistence.py`:

```python
"""Storing what a backtest produced, and reading it back.

Separate from `smoke.py` deliberately. That module orchestrates -- it
resolves manifests, fetches bars, and drives containers -- and it is
already large. Storage is a different responsibility with a different
transaction rule, and it is what 3d, 3e and 3f import: reading a stored
curve should not require importing the backtester that produced it.

Nothing here commits. The caller owns the transaction boundary, so a run
row and its equity points land as one unit or not at all -- a half-written
curve is not a state a metrics layer should have to defend against. This
is the same discipline `record_smoke_run` follows, and for the same
reason.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from psycopg import Connection

from trading.agent_contract.registry import CONTRACT_VERSION

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
    """Money arrives from the container as a string, by design. Parsed to
    Decimal here so the driver binds a numeric, never a float."""
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

    Only call this for a run that reached the container. A pre-flight
    refusal is returned to its caller and never stored: it is a
    deterministic function of the request and the data available, so it
    costs a COUNT to re-derive, and a stored row would imply to a later
    reader that a run happened.
    """
    plan = verdict.plan
    outcome = verdict.outcome or {}
    curve = outcome.get("equity_curve") or []

    run_id: int = conn.execute(
        _INSERT_RUN,
        {
            "strategy_id": strategy_id,
            "status": "PASSED" if verdict.passed else "FAILED",
            "requested_start": requested_start,
            "requested_end": requested_end,
            "fetch_start": plan.start,
            "dispatch_from": plan.dispatch_from,
            "sessions": plan.sessions,
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
    ).fetchone()[0]

    if curve:
        # executemany, not COPY: this codebase reserves COPY for the bulk
        # bar loader at row counts three orders of magnitude larger, and
        # a daily decade is ~2,600 points.
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_POINT,
                [
                    (run_id, p["ts"], _money(p["equity"]), _money(p["cash"]))
                    for p in curve
                ],
            )
    return run_id
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/agent_contract/test_backtest_persistence.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing test — a crashed run keeps its partial curve**

```python
def test_a_crashed_run_is_stored_with_its_partial_curve(db_conn) -> None:  # noqa: ANN001
    """The most useful artifact in this table, and the one an "only store
    successes" implementation quietly drops. `run_loop` returns the curve on
    the crash path too, and a partial curve says WHERE a run died -- which
    is what diagnoses the platform rather than the strategy.
    """
    from trading.agent_contract.persistence import record_backtest_run

    points = [{"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"}]
    run_id = record_backtest_run(
        db_conn,
        _strategy(db_conn),
        _verdict(points, passed=False, error="[SMOKE_OOM] container was OOM-killed"),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )

    status, error = db_conn.execute(
        "SELECT status, error FROM backtest_runs WHERE backtest_run_id=%s", (run_id,)
    ).fetchone()
    assert status == "FAILED"
    assert "SMOKE_OOM" in error
    count = db_conn.execute(
        "SELECT count(*) FROM backtest_equity_points WHERE backtest_run_id=%s", (run_id,)
    ).fetchone()[0]
    assert count == 1
```

- [ ] **Step 6: Run it — it should pass without new code**

Run: `uv run pytest tests/agent_contract/test_backtest_persistence.py -v -k crashed`
Expected: PASS. If it fails, the write path is special-casing failure
somewhere it should not.

- [ ] **Step 7: Mutation check**

Change `_money` to `float(raw)` and confirm
`test_the_curve_round_trips_exactly_in_value_and_order` reddens (the
Decimal comparison and the `isinstance` assertion both break). Restore.

- [ ] **Step 8: Commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
git add -A
git commit -m "feat(agent-contract): store a backtest run and its equity curve"
```

---

### Task 3: Persist on the POST, and only for runs that executed

**Files:**
- Modify: `src/trading/agent_contract/api.py` (`run_backtest`)
- Test: `tests/agent_contract/test_api.py`

**Interfaces:**
- Consumes: `record_backtest_run` from Task 2.
- Produces: `BacktestResponse` gains `backtest_run_id: int | None`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_refused_backtest_stores_no_run(client, db_conn, local_user_id) -> None:  # noqa: ANN001
    """Assert zero rows, not a status field. A gate that refuses and writes
    anyway passes an assertion on the response alone."""
    # Registered WITH a manifest, and with daily bars only to 2024-01-04 --
    # the same fixture shape as
    # test_a_backtest_refuses_an_uncovered_window_before_running_anything.
    # Registered without a manifest, `backtest` refuses on
    # MANIFEST_UNRESOLVABLE before the gate runs, and this test would pass
    # without the coverage path ever executing.
    strategy_id = _registered_with_daily_bars(db_conn, local_user_id, last_day=4)
    before = db_conn.execute("SELECT count(*) FROM backtest_runs").fetchone()[0]
    response = client.post(
        f"/strategies/{strategy_id}/backtests",
        json={"start": "2024-01-02", "end": "2026-12-31"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "REFUSED"
    assert response.json()["backtest_run_id"] is None
    after = db_conn.execute("SELECT count(*) FROM backtest_runs").fetchone()[0]
    assert after == before
```

- [ ] **Step 2: Run it and watch it fail**

Expected: FAIL — `KeyError: 'backtest_run_id'`, the response has no such field.

- [ ] **Step 3: Persist executed runs in the route**

In `run_backtest`, after `verdict = backtest(...)`:

```python
    run_id: int | None = None
    if verdict.plan is not None and verdict.outcome is not None:
        # Reached the container: PASSED or FAILED, both stored. A pre-flight
        # refusal has an outcome of None and is deliberately not recorded.
        run_id = record_backtest_run(
            conn,
            strategy_id,
            verdict,
            requested_start=request.start,
            requested_end=request.end,
            instrument_ids=verdict.instrument_ids,
        )
```

`backtest()` must therefore carry the resolved instrument ids on the
verdict. Add `instrument_ids: tuple[int, ...] = ()` to `BacktestVerdict`
and populate it on the executed path in `smoke.backtest`.

Add `backtest_run_id: int | None = None` to `BacktestResponse` and set it
in `_backtest_response`.

- [ ] **Step 4: Run it and watch it pass**

- [ ] **Step 5: Write and pass the positive case**

A run that executes stores exactly one row whose `bar_calls` matches the
response, and `backtest_run_id` is returned. Mark it `sandbox` — it spawns
containers.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(agent-contract): a backtest that ran is recorded; a refusal is not"
```

---

### Task 4: The read routes

**Files:**
- Modify: `src/trading/agent_contract/persistence.py` (readers)
- Modify: `src/trading/agent_contract/api.py` (two GET routes)
- Test: `tests/agent_contract/test_api.py`

**Interfaces:**
- Produces: `list_backtest_runs(conn, strategy_id, *, limit=50) -> list[dict]`
  (no curve) and `get_backtest_run(conn, backtest_run_id) -> dict` (with
  `equity_curve`, money as strings). `get_backtest_run` raises `KeyError`
  when absent; the route turns that into 404.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_backtest_list_route_omits_curves_and_the_detail_route_includes_them(
    client, db_conn, local_user_id  # noqa: ANN001
) -> None:
    """The cheap list is the entire reason there are two routes, and nothing
    else enforces it: a list that embedded curves would transfer every point
    of every run to render a table of dates and final equities."""
    run_id = _seed_stored_run(db_conn, local_user_id, points=3)  # helper below

    listed = client.get(f"/strategies/{_strategy_of(db_conn, run_id)}/backtests")
    assert listed.status_code == 200
    row = listed.json()[0]
    assert row["backtest_run_id"] == run_id
    assert "equity_curve" not in row

    detail = client.get(f"/backtests/{run_id}")
    assert detail.status_code == 200
    assert len(detail.json()["equity_curve"]) == 3
    # Money as strings on the wire, for the reason RunSummary gives.
    assert all(isinstance(p["equity"], str) for p in detail.json()["equity_curve"])


def test_an_unknown_backtest_run_is_404(client) -> None:  # noqa: ANN001
    response = client.get("/backtests/99999999")
    assert response.status_code == 404
    # Not the vacuous 404 an unrouted path returns.
    assert "99999999" in response.json()["detail"]


def test_the_backtest_read_routes_are_not_coroutines() -> None:
    """C1. Also: both are GETs and neither writes."""
    import inspect

    from trading.agent_contract.api import get_backtest, list_backtests

    assert not inspect.iscoroutinefunction(get_backtest)
    assert not inspect.iscoroutinefunction(list_backtests)
```

Write `_seed_stored_run` beside the tests using `record_backtest_run` and
the `_verdict` helper from Task 2, imported rather than duplicated.

- [ ] **Step 2: Run them and watch them fail**

Expected: FAIL — 404 from unrouted paths, and `ImportError` for the route
functions.

- [ ] **Step 3: Implement the readers**

```python
_SELECT_RUNS = """
    SELECT backtest_run_id, strategy_id, status, requested_start, requested_end,
           fetch_start, dispatch_from, sessions, history_bars_requested,
           history_bars_available, bars, bar_calls, orders_placed, fills,
           final_cash, final_equity, breaker_reason, error, runtime,
           kernel_isolated, contract_version, ran_at
    FROM backtest_runs WHERE strategy_id = %s ORDER BY ran_at DESC, backtest_run_id DESC
    LIMIT %s
"""
```

`get_backtest_run` runs the same projection for one id plus

```python
_SELECT_POINTS = """
    SELECT ts, equity, cash FROM backtest_equity_points
    WHERE backtest_run_id = %s ORDER BY ts
"""
```

and renders money with `str(...)` so no float reaches JSON.

- [ ] **Step 4: Implement the routes**

```python
@router.get("/strategies/{strategy_id}/backtests", response_model=list[BacktestSummary])
def list_backtests(
    strategy_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> list[BacktestSummary]:
    """A strategy's run history, newest first, WITHOUT curves.

    Plain `def`, and a GET that never writes.
    """


@router.get("/backtests/{backtest_run_id}", response_model=BacktestDetail)
def get_backtest(
    backtest_run_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> BacktestDetail:
    """One stored run, with its equity curve in `ts` order."""
```

Neither is scoped by `user_id`, matching `GET /strategies` — the same line
to change when auth lands. Say so in a comment.

- [ ] **Step 5: Run them and watch them pass**

- [ ] **Step 6: Full verification and commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run pytest tests/ -q -m sandbox
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
git add -A
git commit -m "feat(agent-contract): read stored backtests -- list without curves, detail with"
```

---

## Verification before calling 3c done

- [ ] Fast suite green; sandbox suite green; ruff, format, mypy clean
- [ ] Every mutation check in C5 performed and restored
- [ ] Both `trading` and `trading_test` at migration `0012`
- [ ] **A real run stored end to end**, not only tests: re-run the 3b
      verification backtest (`strategy_id=17`, 2020-01-01 → 2026-08-21),
      then `GET /backtests/{id}` and confirm 1,647 points come back in
      order with the first at the session close, and that the stored
      `final_equity` equals what the POST returned
- [ ] `docs/STATUS.md` updated: 3c shipped, 3d next
