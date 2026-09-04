"""Add `backtest_fills`: what each fill cost, itemised.

`ChargeBreakdown` states the reason this table stores components rather
than a total: "§8's cost-drag report needs the breakdown and it cannot be
reconstructed from a lump sum afterwards." A gross-versus-net report over
a strategy that traded daily for a decade is the number this platform
exists to show honestly, and it is unavailable from a total.

Shaped like `backtest_equity_points` (0012): a child of `backtest_runs`
with cascade, money in numeric(18,4), written in the same transaction as
the run so a half-recorded run is not a state a metrics layer must defend
against.

Unlike the curve, the key here is a surrogate `backtest_fill_id` rather
than `(run_id, ts)`. Two fills genuinely can share a timestamp -- one bar
can fill orders on several instruments, and a single bar's price events
can fill more than one order on the same one -- so a composite key would
reject correct data. `ordinal` preserves the sequence the run produced,
which is what FIFO round-trip matching depends on and what a timestamp
alone cannot give when two fills share one.

Revision ID: 0013
Revises: 0012
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backtest_fills",
        sa.Column("backtest_fill_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "backtest_run_id",
            sa.BigInteger,
            sa.ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        # The order the run produced them in. FIFO round-trip matching needs
        # a total order, and a timestamp does not provide one when two fills
        # share a bar.
        sa.Column("ordinal", sa.Integer, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("instrument_id", sa.BigInteger, nullable=False),
        sa.Column("side", sa.Text, nullable=False),
        sa.Column("product", sa.Text, nullable=False),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("price", sa.Numeric(18, 4), nullable=False),
        # Every component, beside the total, so the two can be checked
        # against each other rather than one trusted.
        sa.Column("brokerage", sa.Numeric(18, 4), nullable=False),
        sa.Column("stt", sa.Numeric(18, 4), nullable=False),
        sa.Column("exchange_txn", sa.Numeric(18, 4), nullable=False),
        sa.Column("sebi_fee", sa.Numeric(18, 4), nullable=False),
        sa.Column("stamp_duty", sa.Numeric(18, 4), nullable=False),
        sa.Column("ipft", sa.Numeric(18, 4), nullable=False),
        sa.Column("gst", sa.Numeric(18, 4), nullable=False),
        sa.Column("dp_charges", sa.Numeric(18, 4), nullable=False),
        sa.Column("tds", sa.Numeric(18, 4), nullable=False),
        sa.Column("total_charges", sa.Numeric(18, 4), nullable=False),
        sa.UniqueConstraint("backtest_run_id", "ordinal", name="uq_backtest_fill_ordinal"),
    )
    op.create_index(
        "ix_backtest_fills_run", "backtest_fills", ["backtest_run_id", "ordinal"]
    )


def downgrade() -> None:
    op.drop_index("ix_backtest_fills_run", table_name="backtest_fills")
    op.drop_table("backtest_fills")
