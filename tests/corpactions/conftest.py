"""Ruling A3 (task-16 addendum): `seeded_instrument` is referenced by every
test in the plan's brief but defined nowhere there. It builds an instrument
and its bars through the real write path -- `DbInstrumentResolver` +
`BarLoader` -- rather than hand-rolled SQL, so these tests exercise the
same path production ingestion uses. It never commits: `tests/conftest.py`'s
`db_conn` isolates by rolling back at teardown, and the whole suite depends
on that.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import count

import polars as pl
import pytest
from psycopg import Connection

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, ValidationOutcome
from trading.loaders.bars import BarLoader
from trading.resolver.instruments import DbInstrumentResolver

SeededInstrument = Callable[..., int]


@pytest.fixture
def seeded_instrument(db_conn: Connection) -> Iterator[SeededInstrument]:
    symbol_seq = count(1)

    def _make(
        closes: dict[date, int | str | Decimal],
        *,
        volume: int = 1000,
        symbol: str | None = None,
    ) -> int:
        sym = symbol or f"CORPTEST{next(symbol_seq)}"
        rows: list[dict[str, object]] = []
        for day, close in closes.items():
            close_dec = Decimal(str(close))
            row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
            row.update(
                exchange="NSE",
                segment="CM",
                symbol=sym,
                asset_class="EQUITY",
                ts=datetime(day.year, day.month, day.day, 10, 0, tzinfo=UTC),
                open=close_dec,
                high=close_dec,
                low=close_dec,
                close=close_dec,
                prev_close=close_dec,
                volume=volume,
                lot_size=1,
            )
            rows.append(row)

        frame = pl.DataFrame(rows, schema=CANONICAL_BAR_SCHEMA)
        loader = BarLoader(DbInstrumentResolver(), DataSource.NSE_CM_UDIFF)
        loader.load(ValidationOutcome(valid=frame), db_conn)

        found = db_conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol=%s AND exchange='NSE'"
            " AND segment='CM'",
            (sym,),
        ).fetchone()
        assert found is not None, f"seeded_instrument: {sym} was not created"
        return int(found[0])

    yield _make
