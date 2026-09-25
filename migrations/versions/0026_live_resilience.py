"""Delivery cursors, persisted strategy state, and the spot-kline source.

Part of docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md.
Three additions that ship together because every later task in that
plan depends on all three existing:

- `live_run_cursors`: per (live_run, instrument) delivery position (§4).
  Per instrument, not per run -- a single run-wide position would skip
  ETH's 10:05 bar if BTC's 10:05 bar had already moved it forward.
- `live_runs.strategy_state`: `ctx.state`, persisted every bar so a
  relaunched run resumes instead of starting cold (§5.2).
- `live_runs.last_gap_note`: the replay cap's skip, recorded on the run
  so its own history shows the discontinuity rather than reading as
  continuous (§4).
- `DataSource.BINANCE_SPOT_KLINE` (11): provenance for backfilled spot
  minutes, distinct from BINANCE_WS (6) ticks and BINANCE_FUTURES_KLINE
  (9) perpetual bars -- the same pair's spot and perpetual prices are
  different series and must never share a code.

Revision ID: 0026
Revises: 0025
Create Date: 2026-09-25
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0026"
down_revision: str | None = "0025"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "live_run_cursors",
        sa.Column(
            "live_run_id",
            sa.BigInteger,
            sa.ForeignKey("live_runs.live_run_id"),
            nullable=False,
        ),
        sa.Column(
            "instrument_id",
            sa.BigInteger,
            sa.ForeignKey("instruments.instrument_id"),
            nullable=False,
        ),
        sa.Column("last_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("live_run_id", "instrument_id"),
    )
    op.add_column("live_runs", sa.Column("strategy_state", postgresql.JSONB(), nullable=True))
    op.add_column("live_runs", sa.Column("last_gap_note", sa.Text(), nullable=True))
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES (11, 'BINANCE_SPOT_KLINE') "
        "ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 11")
    op.drop_column("live_runs", "last_gap_note")
    op.drop_column("live_runs", "strategy_state")
    op.drop_table("live_run_cursors")
