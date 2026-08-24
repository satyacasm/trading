"""Seed data_sources with UPSTOX_HISTORICAL_CANDLE.

Part of the Upstox intraday backfill sub-project (docs/superpowers/specs/
2026-08-24-upstox-intraday-backfill-design.md). Mirrors migration 0003's
seeding of (6, 'BINANCE_WS') -- pure metadata insert, no table changes.

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES (7, 'UPSTOX_HISTORICAL_CANDLE') "
        "ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 7")
