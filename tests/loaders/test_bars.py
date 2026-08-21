from datetime import UTC, datetime
from decimal import Decimal

import polars as pl
import pytest

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, ValidationAbort, ValidationOutcome
from trading.loaders.bars import BarLoader
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db
TS = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


def _outcome(
    close: str = "105", symbol: str = "LOADTEST", open_: str | None = "100"
) -> ValidationOutcome:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    row.update(
        exchange="NSE",
        segment="CM",
        symbol=symbol,
        asset_class="EQUITY",
        ts=TS,
        open=Decimal(open_) if open_ is not None else None,
        high=Decimal("110"),
        low=Decimal("95"),
        close=Decimal(close),
        volume=1000,
        lot_size=1,
    )
    return ValidationOutcome(valid=pl.DataFrame([row], schema=CANONICAL_BAR_SCHEMA))


def _loader() -> BarLoader:
    # Ruling L3: `source` is a required constructor argument, no default.
    return BarLoader(DbInstrumentResolver(), DataSource.NSE_CM_UDIFF)


def test_load_writes_a_row(db_conn):
    result = _loader().load(_outcome(), db_conn)
    assert result.rows_written == 1
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 1


def test_loading_the_same_batch_twice_is_a_no_op(db_conn):
    loader = _loader()
    loader.load(_outcome(), db_conn)
    loader.load(_outcome(), db_conn)
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 1


def test_reloading_with_a_corrected_price_overwrites(db_conn):
    """NSE restates files; the newer value must win, not duplicate."""
    loader = _loader()
    loader.load(_outcome(close="105"), db_conn)
    loader.load(_outcome(close="107"), db_conn)
    rows = db_conn.execute("SELECT close FROM bars_daily").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == Decimal("107.0000")


def test_lot_size_is_recorded_in_history(db_conn):
    _loader().load(_outcome(), db_conn)
    count = db_conn.execute("SELECT count(*) FROM instrument_lot_history").fetchone()[0]
    assert count == 1


def test_empty_outcome_writes_nothing(db_conn):
    empty = ValidationOutcome(valid=pl.DataFrame(schema=CANONICAL_BAR_SCHEMA))
    assert _loader().load(empty, db_conn).rows_written == 0


def test_null_open_raises_a_named_error(db_conn):
    """Ruling L5: a null in a NOT NULL column must fail loudly and by name.

    The validator is expected to catch this upstream (ohlc_missing), but the
    loader is defence in depth: it must never let a null reach COPY and blow
    up the whole statement with an opaque NotNullViolation.
    """
    with pytest.raises(ValidationAbort, match="open"):
        _loader().load(_outcome(open_=None), db_conn)
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 0


def test_nullable_columns_survive_copy_as_sql_null(db_conn):
    """Ruling L6: verify NULL handling through COPY ... WITH CSV, not assume it.

    csv.writer renders None as an unquoted empty field, which Postgres's CSV
    COPY treats as NULL. A quoted "" would instead be an empty string and
    fail on a numeric column, so this pins the assumption the whole loader
    rests on.
    """
    _loader().load(_outcome(), db_conn)
    row = db_conn.execute(
        "SELECT prev_close, settle_price, open_interest, delivery_qty FROM bars_daily"
    ).fetchone()
    assert row == (None, None, None, None)
