"""Seed data_sources with UPSTOX_WS.

Part of the Upstox I1-bar sub-project: the live V3 feed's authoritative
per-minute `marketOHLC` bars are published under this provenance code,
distinct from `UPSTOX_HISTORICAL_CANDLE` (backfilled REST candles). Mirrors
migration 0004's shape exactly -- pure metadata insert, no table changes.

Numbered 0006, not 0005 (the value the sub-project's design doc named),
because `0005_watchlist.py` landed first and already claimed that slot --
revision ids must be unique, so this chains after it instead.

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-26
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES (8, 'UPSTOX_WS') "
        "ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 8")
