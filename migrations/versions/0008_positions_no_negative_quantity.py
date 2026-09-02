"""Add a CHECK constraint forbidding a negative `positions.quantity`.

Part of the paper-trading sub-project (.superpowers/sdd/2026-08-31-
paper-trading-core/). Migration 0007 protected `portfolios.cash_balance`
with `ck_no_negative_cash`, but left `positions.quantity` unguarded: a
sell exceeding the held quantity would silently drive a position negative
at the database instead of raising. Task 7's API validates sufficient
position before submit, but that check is exactly the thing a DB
constraint exists to distrust -- both layers, not one.

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_no_negative_position",
        "positions",
        "quantity >= 0",
    )


def downgrade() -> None:
    op.drop_constraint("ck_no_negative_position", "positions", type_="check")
