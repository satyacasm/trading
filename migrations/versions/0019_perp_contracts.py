"""Perpetual contract specifications and maintenance-margin tiers.

Two tables of reference data a perpetual cannot be traded without, kept out
of `instruments` for the reason `instrument_lot_history` is: they are
asset-class specific and they change over time. Binance revises tick sizes,
minimum notionals and margin tiers, and an order accepted in March must be
reconstructible against March's filters rather than today's.

`perp_contract_specs` holds every filter including `tick_size`, even though
`instruments.tick_size` exists. One home for the truth: a value in both
places is a value that will disagree with itself.

`perp_margin_tiers` is what liquidation reads. It is deliberately seeded
empty -- Binance's `leverageBracket` endpoint is signed, and inventing
maintenance rates from memory is the exact failure `MissingChargeSchedule`
exists to prevent. A perpetual with no tier is a perpetual that cannot be
ordered, which is the honest state until a key exists.

Revision ID: 0019
Revises: 0018
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "perp_contract_specs",
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("tick_size", sa.Numeric(28, 12), nullable=False),
        sa.Column("step_size", sa.Numeric(28, 12), nullable=False),
        sa.Column("min_qty", sa.Numeric(28, 12), nullable=False),
        sa.Column("min_notional", sa.Numeric(18, 4), nullable=False),
        sa.Column("liquidation_fee", sa.Numeric(10, 6), nullable=False),
        sa.Column("source_note", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        sa.PrimaryKeyConstraint("instrument_id", "effective_from"),
        # A step or notional floor of zero would accept any quantity and any
        # dust respectively -- the defaults the parser refuses to invent
        # must not be reachable through the table either.
        sa.CheckConstraint("tick_size > 0", name="ck_perp_tick_positive"),
        sa.CheckConstraint("step_size > 0", name="ck_perp_step_positive"),
        sa.CheckConstraint("min_qty > 0", name="ck_perp_min_qty_positive"),
        sa.CheckConstraint("min_notional > 0", name="ck_perp_min_notional_positive"),
    )

    op.create_table(
        "perp_margin_tiers",
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        sa.Column("notional_floor", sa.Numeric(18, 2), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("notional_cap", sa.Numeric(18, 2), nullable=False),
        sa.Column("max_leverage", sa.Numeric(6, 2), nullable=False),
        sa.Column("maintenance_rate", sa.Numeric(10, 6), nullable=False),
        sa.Column("maintenance_amount", sa.Numeric(18, 8), nullable=False),
        sa.Column("source_note", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        sa.PrimaryKeyConstraint("instrument_id", "notional_floor", "effective_from"),
        sa.CheckConstraint("notional_cap > notional_floor", name="ck_perp_tier_ordered"),
        sa.CheckConstraint("max_leverage > 0", name="ck_perp_tier_leverage_positive"),
        # A maintenance rate of zero means a position can never be
        # liquidated, which reads as "safe" and is the most dangerous
        # possible default.
        sa.CheckConstraint("maintenance_rate > 0", name="ck_perp_tier_rate_positive"),
    )


def downgrade() -> None:
    op.drop_table("perp_margin_tiers")
    op.drop_table("perp_contract_specs")
