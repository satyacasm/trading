"""Funding settlements, and provenance codes for Binance's futures venue.

A perpetual has no expiry, so nothing forces its price back to spot at a
date. Funding is the mechanism that does it continuously: every eight
hours longs pay shorts, or the reverse, in proportion to the position's
notional. It is the whole carry of the instrument, and a backtest that
omits it shows every carry strategy earning free money.

Stored as its own series rather than as a bar column because it settles
three times a day on fixed UTC boundaries, not once per bar, and because
it must be queryable for the whole history independently of whichever bar
interval a strategy happens to use.

`mark_price` is nullable: Binance's earliest 2019 settlements carry an
empty one. Those settlements happened and a position held then really paid
them, so the row is worth keeping without a mark.

Revision ID: 0020
Revises: 0019
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0020"
down_revision: str | None = "0019"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "perp_funding",
        sa.Column("instrument_id", sa.BigInteger(), nullable=False),
        sa.Column("funding_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rate", sa.Numeric(16, 12), nullable=False),
        sa.Column("mark_price", sa.Numeric(28, 12), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.instrument_id"]),
        sa.PrimaryKeyConstraint("instrument_id", "funding_time"),
    )
    # Every read is "the settlements for this contract over this window",
    # which the primary key already serves, and "everything due at this
    # boundary across contracts", which it does not.
    op.create_index("ix_perp_funding_time", "perp_funding", ["funding_time"])

    # Append-only, per DataSource's own instruction never to renumber.
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES"
        " (9, 'BINANCE_FUTURES_KLINE'), (10, 'BINANCE_FUTURES_WS')"
        " ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id IN (9, 10)")
    op.drop_index("ix_perp_funding_time", table_name="perp_funding")
    op.drop_table("perp_funding")
