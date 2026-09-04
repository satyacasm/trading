"""Count the orders a live run was refused, and keep the last reason.

A refused order is not a failure -- the currency gate, market hours and
the position checks doing their job is exactly what a live strategy
should experience. But a run whose every order is refused looks, from
`orders_placed` alone, identical to a run that decided to sit still, and
the two call for opposite responses from whoever is watching.

The refusal's own sentence is kept rather than a code: the gateway
already writes the good one ("portfolio 10 has base_currency='INR';
instrument_id=642283 is denominated in 'USDT'"), and a code would have
to be translated back into that sentence somewhere else.

Only the last one is kept. A run refused a thousand times was refused for
the same reason a thousand times; the count carries the scale and the
supervisor's log carries the history.

Revision ID: 0018
Revises: 0017
Create Date: 2026-09-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "live_runs",
        sa.Column("orders_refused", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("live_runs", sa.Column("last_refusal", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("live_runs", "last_refusal")
    op.drop_column("live_runs", "orders_refused")
