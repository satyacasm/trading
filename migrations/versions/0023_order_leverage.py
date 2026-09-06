"""What leverage an order was placed at.

Only meaningful for a perpetual, so nullable rather than defaulted: a
leverage on an equity order would be a number that reads as a fact and is
not one. NULL means "this instrument has no leverage concept", which is
true of everything this platform traded before perpetuals existed.

It lives on the order rather than the portfolio because it is a property
of the decision, not of the account: the same portfolio can hold a 2x
position it intends to sit on and a 20x one it intends to scalp, and a
single account-level setting would make the second silently rewrite the
risk of the first.

Revision ID: 0023
Revises: 0022
Create Date: 2026-09-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023"
down_revision: str | None = "0022"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("leverage", sa.Numeric(6, 2), nullable=True))
    op.create_check_constraint(
        "ck_order_leverage_positive", "orders", "leverage IS NULL OR leverage > 0"
    )


def downgrade() -> None:
    op.drop_constraint("ck_order_leverage_positive", "orders", type_="check")
    op.drop_column("orders", "leverage")
