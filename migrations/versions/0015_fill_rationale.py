"""Add `backtest_fills.rationale`: the strategy's own words for each trade.

The contract already requires a non-empty rationale on every order (§9,
enforced by `ck_rationale_present` on `orders`), and `run_loop` had it in
hand at fill time and dropped it. Carrying it through is what lets a chart
marker say *why* a trade happened -- the only part of a trade a price chart
cannot infer from the price.

Nullable: fills recorded before this column existed have no rationale, and
an empty string would read as "the strategy gave none", which is a
different and impossible claim.

Revision ID: 0015
Revises: 0014
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("backtest_fills", sa.Column("rationale", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("backtest_fills", "rationale")
