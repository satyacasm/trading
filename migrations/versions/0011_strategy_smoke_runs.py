"""Add `strategy_smoke_runs`: the record of §9 stage 2.

One row per smoke run, many rows per strategy version -- deliberately not
one column set on `strategies`. A version is immutable, but the window it
was smoked against is not: D-S4 selects the most recent sessions every
instrument shares, so re-running the same version next week meets
different bars. Collapsing that to a single "smoked: true" flag would
lose the only thing that makes an old pass interpretable.

`runtime` and `kernel_isolated` are carried forward from `SandboxResult`
for the same reason they exist there: a stored pass must never be
readable as better isolated than it was. Dropping them here would
reintroduce exactly the ambiguity the sandbox refuses to leave open.

`rejections` is a count and `rejection_reasons` the strings behind it.
The count alone would keep the numeric columns uniform and throw away the
actionable half: "3 orders bounced" is not something an agent can fix,
"insufficient funds: need 12,400 INR, have 900" is. Only the
ALL_ORDERS_REJECTED finding carries a reason into `findings`, and only
the first one, so a run where *some* orders bounced would otherwise
store no reason at all.

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_smoke_runs",
        sa.Column("smoke_run_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "strategy_id",
            sa.BigInteger,
            sa.ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("verdict", sa.Text, nullable=False),
        sa.Column("window_start", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("window_end", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("sessions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("instruments", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("bar_calls", sa.Integer, nullable=False, server_default="0"),
        sa.Column("orders_placed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("fills", sa.Integer, nullable=False, server_default="0"),
        sa.Column("rejections", sa.Integer, nullable=False, server_default="0"),
        sa.Column("rejection_reasons", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("final_cash", sa.Numeric(18, 4), nullable=True),
        sa.Column("final_equity", sa.Numeric(18, 4), nullable=True),
        sa.Column("breaker_reason", sa.Text, nullable=True),
        sa.Column("findings", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("runtime", sa.Text, nullable=False),
        sa.Column("kernel_isolated", sa.Boolean, nullable=False),
        sa.Column("contract_version", sa.Text, nullable=False),
        sa.Column(
            "ran_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint(
            "verdict IN ('PASSED','PASSED_WITH_WARNINGS','REJECTED')",
            name="ck_smoke_run_verdict",
        ),
    )
    op.create_index(
        "ix_strategy_smoke_runs_strategy",
        "strategy_smoke_runs",
        ["strategy_id", "ran_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_strategy_smoke_runs_strategy", table_name="strategy_smoke_runs")
    op.drop_table("strategy_smoke_runs")
