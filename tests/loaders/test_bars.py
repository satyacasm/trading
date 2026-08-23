from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl
import pytest

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, ValidationAbort, ValidationOutcome
from trading.loaders.bars import BarLoader
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db
TS = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


def _outcome(
    close: str = "105",
    symbol: str = "LOADTEST",
    open_: str | None = "100",
    series: str | None = None,
    underlying_price: str | None = None,
    segment: str = "CM",
    asset_class: str = "EQUITY",
    expiry: date | None = None,
) -> ValidationOutcome:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    row.update(
        exchange="NSE",
        segment=segment,
        symbol=symbol,
        series=series,
        asset_class=asset_class,
        expiry=expiry,
        ts=TS,
        open=Decimal(open_) if open_ is not None else None,
        high=Decimal("110"),
        low=Decimal("95"),
        close=Decimal(close),
        underlying_price=Decimal(underlying_price) if underlying_price is not None else None,
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


# --- Ruling S2 (task-18-brief.md): underlying_price must round-trip ---


def test_underlying_price_round_trips_through_the_loader(db_conn):
    """Finding F3 (task-17-report.md): UdiffNormalizer computes
    underlying_price into every F&O canonical row, but it used to be
    silently dropped between normalize and load. Prove the full loader path
    -- not a raw SQL UPDATE -- actually persists it."""
    _loader().load(
        _outcome(
            symbol="UNDRLTEST",
            segment="FO",
            asset_class="FUTURE",
            expiry=date(2026, 8, 27),
            underlying_price="1234.5600",
        ),
        db_conn,
    )
    row = db_conn.execute("SELECT underlying_price FROM bars_daily").fetchone()
    assert row == (Decimal("1234.5600"),)


def test_underlying_price_null_survives_copy_as_sql_null(db_conn):
    _loader().load(_outcome(), db_conn)
    row = db_conn.execute("SELECT underlying_price FROM bars_daily").fetchone()
    assert row == (None,)


# --- Ruling S1 (task-18-brief.md): series joins the instrument identity ---


def test_same_symbol_different_series_resolve_to_distinct_instruments(db_conn):
    """The DHFL case (task-18-brief.md): the same SYMBOL under different
    SERIES values are different securities -- an equity plus its unrelated
    NCDs -- and must resolve to distinct instrument ids, load as distinct
    rows, and quarantine nothing."""
    rows: list[dict[str, object]] = []
    for series, close in (("EQ", "131.90"), ("N2", "960.00"), ("N4", "805.51")):
        row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
        row.update(
            exchange="NSE",
            segment="CM",
            symbol="DHFL",
            series=series,
            asset_class="EQUITY",
            ts=TS,
            open=Decimal(close),
            high=Decimal(close),
            low=Decimal(close),
            close=Decimal(close),
            volume=1000,
            lot_size=1,
        )
        rows.append(row)
    outcome = ValidationOutcome(valid=pl.DataFrame(rows, schema=CANONICAL_BAR_SCHEMA))

    result = _loader().load(outcome, db_conn)

    assert result.rows_written == 3
    assert result.instruments_created == 3
    db_rows = db_conn.execute(
        "SELECT series, instrument_id FROM instruments WHERE symbol='DHFL' ORDER BY series"
    ).fetchall()
    assert [r[0] for r in db_rows] == ["EQ", "N2", "N4"]
    assert len({r[1] for r in db_rows}) == 3
    bar_count = db_conn.execute(
        "SELECT count(*) FROM bars_daily b JOIN instruments i ON i.instrument_id = b.instrument_id"
        " WHERE i.symbol = 'DHFL'"
    ).fetchone()[0]
    assert bar_count == 3


def test_series_is_persisted_on_the_instrument_row(db_conn):
    _loader().load(_outcome(symbol="SERIESTEST", series="BE"), db_conn)
    row = db_conn.execute("SELECT series FROM instruments WHERE symbol='SERIESTEST'").fetchone()
    assert row == ("BE",)


def test_series_absent_leaves_the_column_null(db_conn):
    _loader().load(_outcome(symbol="NOSERIESTEST"), db_conn)
    row = db_conn.execute("SELECT series FROM instruments WHERE symbol='NOSERIESTEST'").fetchone()
    assert row == (None,)
