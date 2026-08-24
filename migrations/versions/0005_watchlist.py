"""Add watchlists table for the charts + watchlist web UI.

Part of the charts+watchlist sub-project (docs/superpowers/specs/
2026-08-24-charts-watchlist-ui-design.md). No user_id column: V1 has
exactly one implicit user and no auth (implementation-plan.md Sec 12 Q1),
so a single global list is the honest shape here, not a user_id column
carrying a fake sentinel value for the only row that will ever exist.

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE watchlists (
            instrument_id BIGINT PRIMARY KEY
                REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            added_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS watchlists")
