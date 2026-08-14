import pytest

pytestmark = pytest.mark.db


def test_core_tables_exist(db_conn):
    rows = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {
        "instruments",
        "instrument_lot_history",
        "corporate_actions",
        "trading_calendar",
        "bars_daily",
        "bars_intraday",
        "ingest_jobs",
        "quarantine",
        "users",
        "data_sources",
    } <= names


def test_bars_daily_is_a_hypertable(db_conn):
    row = db_conn.execute(
        "SELECT hypertable_name FROM timescaledb_information.hypertables "
        "WHERE hypertable_name = 'bars_daily'"
    ).fetchone()
    assert row is not None


def test_untraded_option_row_is_accepted(db_conn):
    """Finding F2: OHLC=0 with a real close and zero volume is legitimate."""
    # NOTE: expiry/strike/option_type are supplied (matching the canonical_key)
    # because spec §4.1's ck_option_fields / ck_derivative_expiry CHECK
    # constraints require them for asset_class='OPTION'. The brief's literal
    # INSERT omitted them; that is a gap in the illustrative INSERT, not a
    # reason to weaken the transcribed DDL (see task-4-report.md).
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key, expiry, strike, option_type) VALUES "
        "('OPTION','NSE','FO','TESTOPT','INR','ACTIVE',"
        "'NSE:FO:TESTOPT:2026-10-27:430:CE','2026-10-27',430,'CE') RETURNING instrument_id"
    )
    iid = db_conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol='TESTOPT'"
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)"
        " VALUES (%s, '2026-08-13T10:00:00Z', 0, 0, 0, 19.45, 0, 2)",
        (iid,),
    )  # must not raise


def test_traded_row_with_impossible_ohlc_is_rejected(db_conn):
    from psycopg.errors import CheckViolation

    iid = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES ('EQUITY','NSE','CM','TESTEQ','INR','ACTIVE','NSE:CM:TESTEQ')"
        " RETURNING instrument_id"
    ).fetchone()[0]
    with pytest.raises(CheckViolation):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)"
            " VALUES (%s, '2026-08-13T10:00:00Z', 100, 90, 95, 99, 5000, 1)",
            (iid,),
        )  # high < low with real volume


def test_equity_natural_key_is_actually_unique(db_conn):
    """UNIQUE NULLS NOT DISTINCT: two RELIANCE rows must collide despite NULLs."""
    from psycopg.errors import UniqueViolation

    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES ('EQUITY','NSE','CM','DUPTEST','INR','ACTIVE','NSE:CM:DUPTEST')"
    )
    with pytest.raises(UniqueViolation):
        db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
            " canonical_key) VALUES ('EQUITY','NSE','CM','DUPTEST','INR','ACTIVE','other-key')"
        )


def test_data_sources_match_the_python_enum(db_conn):
    from trading.contracts import DataSource

    rows = db_conn.execute("SELECT source_id, source_key FROM data_sources").fetchall()
    assert {(r[0], r[1]) for r in rows} == {(s.value, s.name) for s in DataSource}


def test_data_source_values_are_pinned():
    """These integers are persisted on every bar row (~250M at full backfill).

    Renumbering them would silently reattribute the provenance of all existing
    data with no error anywhere. This test is the guard rail: adding a member is
    fine, changing an existing member's value must break the build.
    """
    from trading.contracts import DataSource

    assert {s.name: s.value for s in DataSource} == {
        "NSE_CM_UDIFF": 1,
        "NSE_FO_UDIFF": 2,
        "BSE_CM_UDIFF": 3,
        "NSE_CM_LEGACY": 4,
        "AMFI_NAV": 5,
    }
