"""bars_intraday.volume widens to NUMERIC, DataSource.BINANCE_WS seeded.

Part of the bar-aggregator sub-project (docs/superpowers/specs/
2026-08-24-bar-aggregator-design.md). Crypto trade quantities are
fractional Decimals (e.g. 0.01000000 BTC) and cannot be represented in a
BIGINT column. Widens only `bars_intraday.volume` -- `bars_daily.volume`
is deliberately left untouched: this migration's caller never writes to
`bars_daily`, real NSE/BSE equity and F&O volumes are always whole-unit
integers, and that table is live and already covered by Phase 0's
closed-out reconcile/validation checks.

`bars_intraday` is empty at the time this migration is written (Task 6 of
the crypto-streaming plan never wrote to it -- that plan explicitly
deferred persistence), so this is a pure schema change, no data migration.
The downgrade path assumes the same: reverting after real fractional
volumes have been written would truncate them, but the table is empty as
of every migration currently in this repo's history.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE bars_intraday ALTER COLUMN volume TYPE NUMERIC(28,8)")
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES (6, 'BINANCE_WS') "
        "ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 6")
    op.execute("ALTER TABLE bars_intraday ALTER COLUMN volume TYPE BIGINT")
