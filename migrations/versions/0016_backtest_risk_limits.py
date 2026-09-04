"""Add the risk limits a backtest actually ran under.

A run can now override the strategy's declared `max_daily_loss` and
`max_drawdown_pct`, which means the limits that constrained a stored result
are no longer readable from the strategy row -- two runs of the same version
can have been stopped by different rules. Recording them here is what keeps
a result interpretable: a run that halted early is only explicable next to
the limit it hit.

Nullable, and null means "the strategy's own": storing the resolved value
would erase the distinction between a caller who chose 20,000 and a caller
who chose nothing and got 20,000 from the manifest. The first is a decision
about this run; the second is not.

Revision ID: 0016
Revises: 0015
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("backtest_runs", sa.Column("max_daily_loss", sa.Numeric(18, 4), nullable=True))
    op.add_column("backtest_runs", sa.Column("max_drawdown_pct", sa.Numeric(9, 4), nullable=True))


def downgrade() -> None:
    op.drop_column("backtest_runs", "max_drawdown_pct")
    op.drop_column("backtest_runs", "max_daily_loss")
