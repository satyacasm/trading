"""What a perpetual run paid in carry, and what the exchange closed.

Two facts a perpetual backtest produces that a spot one cannot, and that
the cost-drag report has no place for.

Funding is not a charge -- it is a signed transfer, and it is often the
whole return of a carry strategy -- so it gets its own row rather than
being folded into `total_charges`, where it would read as a cost and
could only ever be negative. Stored per instrument because a run holding
two contracts can be paid by one and pay the other, and a single total
hides that.

A liquidation stores the two numbers that decided it: the mark, and the
maintenance requirement the position's equity fell below. A position that
simply stops appearing in a report is unexplainable months later, and
"why did this strategy stop losing money in January" is exactly the
question somebody will ask.

Revision ID: 0024
Revises: 0023
Create Date: 2026-09-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backtest_funding",
        sa.Column("backtest_run_id", sa.BigInteger(), nullable=False),
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        # Positive means the run paid; negative means it was paid. The same
        # sign convention as `funding_payment`, so a reader who learns it
        # once knows it everywhere.
        sa.Column("amount", sa.Numeric(28, 8), nullable=False),
        sa.ForeignKeyConstraint(
            ["backtest_run_id"], ["backtest_runs.backtest_run_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        sa.PrimaryKeyConstraint("backtest_run_id", "instrument_id"),
    )
    op.create_table(
        "backtest_liquidations",
        sa.Column("backtest_liquidation_id", sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id", sa.BigInteger(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        sa.Column("quantity", sa.Numeric(28, 8), nullable=False),
        sa.Column("mark", sa.Numeric(28, 8), nullable=False),
        sa.Column("fill_price", sa.Numeric(28, 8), nullable=False),
        sa.Column("equity", sa.Numeric(28, 8), nullable=False),
        sa.Column("maintenance", sa.Numeric(28, 8), nullable=False),
        sa.Column("fee", sa.Numeric(28, 8), nullable=False),
        sa.ForeignKeyConstraint(
            ["backtest_run_id"], ["backtest_runs.backtest_run_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        sa.UniqueConstraint(
            "backtest_run_id", "ordinal", name="uq_backtest_liquidation_ordinal"
        ),
    )


def downgrade() -> None:
    op.drop_table("backtest_liquidations")
    op.drop_table("backtest_funding")
