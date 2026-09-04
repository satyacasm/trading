"""Add `backtest_runs.stress`: the 2x cost-and-slippage rerun's result.

Stored, unlike the Monte Carlo reshuffle, because of the line 3c and 3d
already drew and this migration makes concrete: **anything that required
running the world is stored; anything that is arithmetic over what was
stored is computed.**

Doubling slippage changes which fills happen -- an order that filled at the
real price may not fill at all -- so the stress result cannot be re-derived
from the base run's output. It is an observation. The reshuffle, by
contrast, is a pure function of `backtest_fills` and is computed on read,
so improving it applies retroactively to every run ever recorded.

`jsonb` rather than columns, deliberately, and for the opposite reason to
0012's equity curve. That is a long, homogeneous series of money that SQL
should be able to aggregate; this is a short, heterogeneous summary of one
extra run whose shape will grow as the robustness suite does (walk-forward
folds are the next addition). A column set would need a migration per
addition; a document does not, and nothing aggregates across it.

Revision ID: 0014
Revises: 0013
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # Nullable, not defaulted to '{}': a run recorded before this column
    # existed, or one that crashed before the stress pass could run, has no
    # stress result -- and an empty object would read as "it ran and found
    # nothing", which is a different claim.
    op.add_column("backtest_runs", sa.Column("stress", postgresql.JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("backtest_runs", "stress")
