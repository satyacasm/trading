"""Isolated or cross margin, per portfolio.

Two answers to one question: what backs a losing position.

**Isolated** backs it with the margin posted for it and nothing else. A
position can be liquidated while the rest of the account is untouched,
and the most that can be lost on it is what was put behind it. That is
the safer default and the one to learn on, which is why it stays the
default here.

**Cross** backs every position with the whole free balance. Positions
survive far deeper drawdowns, because the account's other equity is
available to them -- and when it does liquidate, it liquidates against
everything, so one bad position can take the account. The trade is
strictly more room in exchange for strictly worse tail risk, and a
platform whose purpose is to show people what leverage does should let
them see both.

A column on the portfolio rather than the position: it is a property of
how the account is margined, and per-position modes would make the
account-level maintenance sum in cross mode mean nothing.

Revision ID: 0025
Revises: 0024
Create Date: 2026-09-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025"
down_revision: str | None = "0024"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "portfolios",
        sa.Column("margin_mode", sa.Text(), nullable=False, server_default="ISOLATED"),
    )
    op.create_check_constraint(
        "ck_portfolio_margin_mode",
        "portfolios",
        "margin_mode IN ('ISOLATED', 'CROSS')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_portfolio_margin_mode", "portfolios", type_="check")
    op.drop_column("portfolios", "margin_mode")
