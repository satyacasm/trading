"""Signed perpetual positions, and the cost of trading them.

Deliberately a second table rather than a signed `positions`. Spot keeps
`ck_no_negative_position`, its notional cash model and its charge model
untouched: a perpetual and a spot holding of the same pair are different
exposures with different money mechanics, and the constraint that makes a
spot short unrepresentable is load-bearing in the fill path.

The mechanics differ where it matters most. Buying spot moves cash by
notional; opening a perpetual moves no cash at all -- margin is
*reserved*, and cash changes only on realised P&L, fees and funding. So
`reserved_margin` is a column here rather than a subtraction from
`portfolios.cash_balance`, which would make an open position look like
spent money.

The charge row is Binance USDⓈ-M's published taker fee, 0.05% of turnover
on both sides. Two things deliberately absent:

Funding is not a charge. It is a signed transfer between position holders,
so modelling it as a fee would make it always cost the holder, which is
false roughly half the time. It gets its own ledger entry type.

The 1.25% liquidation fee is not here either. It is charged by an event
this platform cannot yet produce -- liquidation is task 5 -- and a rate
sitting in the table before anything can levy it reads as implemented.
`perp_contract_specs.liquidation_fee` already carries the number, fetched
per contract, which is where task 5 will read it from.

Revision ID: 0022
Revises: 0021
Create Date: 2026-09-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0022"
down_revision: str | None = "0021"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "perp_positions",
        sa.Column("portfolio_id", sa.BigInteger(), nullable=False),
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        # SIGNED, and deliberately without the CHECK its spot sibling
        # carries: the sign is the direction.
        sa.Column("quantity", sa.Numeric(28, 8), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 8), nullable=False),
        sa.Column("leverage", sa.Numeric(6, 2), nullable=False),
        sa.Column("reserved_margin", sa.Numeric(28, 8), nullable=False, server_default="0"),
        sa.Column("realised_pnl", sa.Numeric(28, 8), nullable=False, server_default="0"),
        sa.Column("funding_paid", sa.Numeric(28, 8), nullable=False, server_default="0"),
        sa.Column("opened_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.portfolio_id"]),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        # One signed position per contract: one-way mode. Hedge mode, where
        # a long and a short are held at once, would need a side in the key.
        sa.PrimaryKeyConstraint("portfolio_id", "instrument_id"),
        sa.CheckConstraint("reserved_margin >= 0", name="ck_perp_margin_not_negative"),
        sa.CheckConstraint("leverage > 0", name="ck_perp_leverage_positive"),
        # A flat position holds nothing and must reserve nothing, or margin
        # leaks a little on every round trip until the portfolio cannot
        # open anything and no position explains why.
        sa.CheckConstraint(
            "quantity <> 0 OR reserved_margin = 0", name="ck_perp_flat_reserves_nothing"
        ),
    )

    op.execute(
        """
        INSERT INTO charge_schedules
            (broker, exchange, asset_class, product, charge_type, basis,
             applies_to_side, rate, rounding, effective_from, source_note)
        VALUES
            ('BINANCE', 'BINANCE_FUTURES', 'PERP', 'INTRADAY', 'BROKERAGE',
             'PERCENT_OF_TURNOVER', 'BOTH', 0.0005, 'TWO_DECIMALS', '2019-09-08',
             'Binance USDS-M taker fee, 0.05%')
        """
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM charge_schedules WHERE asset_class = 'PERP' AND exchange = 'BINANCE_FUTURES'"
    )
    op.drop_table("perp_positions")
