"""Instrument series identity and bars_daily.underlying_price.

Task 18 (task-18-brief.md), rulings S1 and S2, both surfaced by Task 17's
tooling run against a real dataset -- see task-17-report.md findings F3
and F4.

Ruling S1: NSE CM bhavcopy files carry a `SERIES`/`SctySrs` column, and the
same `SYMBOL`/`TckrSymb` can legitimately appear more than once a day under
different series -- an equity and one or more unrelated debt securities
(NCDs), each with its own ISIN and price, not duplicates of one another
(the DHFL case: 15 rows, one EQ equity plus 14 distinct NCD series, all
under the symbol `DHFL`). Without `series` in the natural key, every row
after the first collapsed onto the first and was quarantined as a false
`duplicate_key`, measured at ~4% of one legacy day (400x the 0.01%
threshold) -- silently discarding every listed NCD in the dataset. This
migration adds `series TEXT` to `instruments` and includes it in
`uq_instrument_natural`.

Ruling S2: `UdiffNormalizer` already computes `underlying_price` into every
canonical F&O row (`UndrlygPric`), but `bars_daily` had no such column, so
the value was silently dropped between normalize and load on every F&O row
ever loaded. This migration adds `underlying_price NUMERIC(18,4)` to
`bars_daily`; the loader and reconciliation check are updated separately
(src/trading/loaders/bars.py, src/trading/reconcile.py).

`bars_daily` and `instruments` are both empty at the time this migration is
written (Task 17 proved every row it wrote during tool verification and
deleted it again -- see task-17-report.md), so no data migration is needed
for either change.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-23
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # --- Ruling S1: instrument series joins the natural key ---
    op.execute("ALTER TABLE instruments ADD COLUMN series TEXT")
    op.execute("ALTER TABLE instruments DROP CONSTRAINT uq_instrument_natural")
    op.execute(
        """
        ALTER TABLE instruments ADD CONSTRAINT uq_instrument_natural
            UNIQUE NULLS NOT DISTINCT (exchange, segment, symbol, series,
                                        expiry, strike, option_type)
        """
    )

    # --- Ruling S2: bars_daily gains underlying_price ---
    op.execute("ALTER TABLE bars_daily ADD COLUMN underlying_price NUMERIC(18,4)")


def downgrade() -> None:
    op.execute("ALTER TABLE bars_daily DROP COLUMN underlying_price")

    op.execute("ALTER TABLE instruments DROP CONSTRAINT uq_instrument_natural")
    op.execute(
        """
        ALTER TABLE instruments ADD CONSTRAINT uq_instrument_natural
            UNIQUE NULLS NOT DISTINCT (exchange, segment, symbol, expiry, strike, option_type)
        """
    )
    op.execute("ALTER TABLE instruments DROP COLUMN series")
