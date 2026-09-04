"""Add `backtest_runs` and `backtest_equity_points`: what a backtest produced.

The curve is a table rather than a jsonb column on the run row, against
the closer precedent of `strategy_smoke_runs` (which stores `instruments`,
`findings` and `rejection_reasons` that way). Three reasons, in order of
weight:

Money belongs in numeric(18,4) like every other money column this platform
owns -- `fills`, `portfolios`, `positions`. A curve in jsonb is a curve of
strings, because JSON numbers are IEEE 754 doubles and `outcome.py`
already refuses to let money take that representation. Storing money as
text in a database that has a decimal type, purely because the wire format
needs strings, lets a transport constraint reach into storage; stored as
numeric, min()/max() for drawdown stay available to SQL.

It is also the format 3f could not migrate away from cheaply: an intraday
curve is 100k+ points where a daily one is ~2,600, and a table makes
downsampling a stride and paging a LIMIT.

Volume is a non-argument. A thousand runs at 1,650 points is 1.65M rows,
against a database already holding 51,081,227 daily bars.

`(backtest_run_id, ts)` is the primary key rather than a surrogate id.
`run_loop` emits one point per dispatched bar and bars are grouped by
close_ts (`InMemoryBars.indexed_groups`), so timestamps within a run are
unique by construction -- making that the key turns the property into one
the database refuses to let break. A doubled point would otherwise reach
3d as a wrong Sharpe and 3e as a real feature of the equity path, with
nothing raising anywhere. It also serves `WHERE backtest_run_id = ? ORDER
BY ts` as a straight index scan, so the read path needs no second index.

`status` carries only PASSED and FAILED. Pre-flight refusals are returned
to the caller and never stored -- they are deterministic functions of the
request and cost a COUNT to re-derive -- so a REFUSED value would be
unreachable, and an unreachable enum member invites someone to make it
reachable.

There is no `updated_at`. A run is an immutable record of an event, and a
column implying otherwise would be a lie the schema tells.

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
        # What the caller asked for...
        sa.Column("requested_start", sa.Date, nullable=False),
        sa.Column("requested_end", sa.Date, nullable=False),
        # ...and what the plan resolved. These differ by warm-up, and the
        # difference is deliberate (D3b-3): storing only one of them makes a
        # run's bar_calls unexplainable a week later.
        sa.Column("fetch_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("dispatch_from", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("sessions", sa.Integer, nullable=False, server_default="0"),
        # The resolved universe, not a count: `resolve_universe` is
        # point-in-time at the window's end, so the same manifest can resolve
        # differently as listings change. A count would record that something
        # was traded without recording what.
        sa.Column("instruments", postgresql.JSONB, nullable=False, server_default="[]"),
        # A strategy warmed on 40 of the 200 bars it asked for is a different
        # experiment from the one requested.
        sa.Column("history_bars_requested", sa.Integer, nullable=False, server_default="0"),
        sa.Column("history_bars_available", sa.Integer, nullable=False, server_default="0"),
        # The interval actually served, not the one declared.
        sa.Column("bars", sa.Text, nullable=True),
        sa.Column("bar_calls", sa.Integer, nullable=False, server_default="0"),
        sa.Column("orders_placed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("fills", sa.Integer, nullable=False, server_default="0"),
        sa.Column("final_cash", sa.Numeric(18, 4), nullable=True),
        sa.Column("final_equity", sa.Numeric(18, 4), nullable=True),
        sa.Column("breaker_reason", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("findings", postgresql.JSONB, nullable=False, server_default="[]"),
        # Carried forward for the reason SandboxResult and
        # strategy_smoke_runs both carry them: a stored run must never be
        # readable as better isolated than it was.
        sa.Column("runtime", sa.Text, nullable=False),
        sa.Column("kernel_isolated", sa.Boolean, nullable=False),
        sa.Column("contract_version", sa.Text, nullable=False),
        sa.Column(
            "ran_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint("status IN ('PASSED','FAILED')", name="ck_backtest_run_status"),
    )
    op.create_index("ix_backtest_runs_strategy", "backtest_runs", ["strategy_id", "ran_at"])
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
