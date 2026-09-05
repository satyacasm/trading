"""Rename provenance 10 to what it will actually record.

0020 added `BINANCE_FUTURES_WS` on the assumption the live perpetual feed
would stream, like every other feed here. It cannot: Binance's futures
socket connects from this jurisdiction, acknowledges a SUBSCRIBE, and then
sends no market data, while its REST endpoints answer normally. The live
path polls instead, and its bars come from the same `klines` endpoint the
backfill uses -- so they carry provenance 9, and 10 was left describing a
transport that is not used.

Renaming rather than deleting: `DataSource` is append-only because the
integer is persisted on every bar, and 10 keeps its number. No row
references it yet, so only the lookup row's text changes. It is reserved
for the persisted mark series liquidation will need, which is a different
series from the traded price and needs its own provenance.

Revision ID: 0021
Revises: 0020
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0021"
down_revision: str | None = "0020"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("UPDATE data_sources SET source_key='BINANCE_FUTURES_MARK' WHERE source_id=10")


def downgrade() -> None:
    op.execute("UPDATE data_sources SET source_key='BINANCE_FUTURES_WS' WHERE source_id=10")
