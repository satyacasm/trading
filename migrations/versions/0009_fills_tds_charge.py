"""Add `fills.tds`: the TDS charge component was computed by
`compute_charges` and dropped on the floor -- `ChargeBreakdown` had no
field for it (IMP-2, final-review fix wave). `fills` stores every other
charge component individually (`brokerage`, `stt`, ..., `dp_charges`), so
TDS gets the same treatment rather than being folded silently into
`total_charges` alone.

Not seeded: no `charge_schedules` row produces a non-zero TDS amount yet
(switching on crypto TDS is a product decision, not this fix's to make).
This migration only wires the column so a future TDS row is never
silently dropped.

Part of the paper-trading sub-project (.superpowers/sdd/2026-08-31-
paper-trading-core/final-review-fix-brief.md, IMP-2). Numbered 0009, not
0008 (as the brief's prose suggested) -- 0008 was already taken by the
`ck_no_negative_position` constraint by the time this fix wave started.

Revision ID: 0009
Revises: 0008
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "fills",
        sa.Column("tds", sa.Numeric(18, 4), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("fills", "tds")
