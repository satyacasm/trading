"""Add `live_runs`, and tag the orders a live strategy places.

A forward run is an event with a lifetime, unlike a backtest which is an
event with a result. It has a state (`RUNNING`, `STOPPED`, `CRASHED`), a
portfolio it trades, and a reason it ended -- and the reason matters most:
a run that stopped because its breaker latched, because the supervisor's
order-rate limit fired, or because the container died are three different
things and a single "stopped" would lose the distinction.

`orders.live_run_id` is nullable and null means a human placed it. A live
strategy's orders are deliberately ordinary orders -- same table, same
validation, same fills, same cost model -- so the blotter shows them beside
manual trades, and this column is the only thing that tells them apart.

Revision ID: 0017
Revises: 0016
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "live_runs",
        sa.Column("live_run_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "strategy_id",
            sa.BigInteger,
            sa.ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            nullable=False,
        ),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("stopped_reason", sa.Text, nullable=True),
        # Carried forward for the reason SandboxResult and every stored run
        # carry them: a live run must never read as better isolated than it
        # was.
        sa.Column("runtime", sa.Text, nullable=True),
        sa.Column("kernel_isolated", sa.Boolean, nullable=True),
        sa.Column("bars_seen", sa.Integer, nullable=False, server_default="0"),
        sa.Column("orders_placed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("stopped_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('RUNNING','STOPPED','CRASHED')", name="ck_live_run_status"
        ),
    )
    op.create_index("ix_live_runs_strategy", "live_runs", ["strategy_id", "started_at"])
    # Partial unique index: one live run per portfolio at a time. D6's
    # one-strategy-one-portfolio rule would otherwise be enforced only by
    # convention, and two strategies trading one portfolio would each see
    # the other's fills as inexplicable cash movements.
    op.execute(
        "CREATE UNIQUE INDEX uq_live_run_active_portfolio ON live_runs (portfolio_id) "
        "WHERE status = 'RUNNING'"
    )
    op.add_column(
        "orders",
        sa.Column(
            "live_run_id",
            sa.BigInteger,
            sa.ForeignKey("live_runs.live_run_id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("orders", "live_run_id")
    op.execute("DROP INDEX IF EXISTS uq_live_run_active_portfolio")
    op.drop_index("ix_live_runs_strategy", table_name="live_runs")
    op.drop_table("live_runs")
