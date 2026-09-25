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


# --- Task 18, Ruling S1: series joins uq_instrument_natural ---


def test_instruments_has_a_series_column(db_conn):
    row = db_conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='instruments' AND column_name='series'"
    ).fetchone()
    assert row is not None


def test_same_symbol_different_series_does_not_collide(db_conn):
    """The DHFL case at the constraint level: EQ and N2 under the same
    symbol must be allowed to coexist -- proves adding `series` to
    uq_instrument_natural actually loosens the constraint as intended,
    not just that InstrumentRef/the resolver happen to agree not to try."""
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, series, currency,"
        " status, canonical_key) VALUES "
        "('EQUITY','NSE','CM','DHFL','EQ','INR','ACTIVE','NSE:CM:DHFL:EQ')"
    )
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, series, currency,"
        " status, canonical_key) VALUES "
        "('EQUITY','NSE','CM','DHFL','N2','INR','ACTIVE','NSE:CM:DHFL:N2')"
    )  # must not raise
    count = db_conn.execute("SELECT count(*) FROM instruments WHERE symbol='DHFL'").fetchone()[0]
    assert count == 2


def test_same_symbol_same_series_still_collides(db_conn):
    """The constraint must still catch a genuine duplicate once series is
    part of it -- adding a column must not accidentally widen every group
    to be distinct."""
    from psycopg.errors import UniqueViolation

    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, series, currency,"
        " status, canonical_key) VALUES "
        "('EQUITY','NSE','CM','SERIESDUP','EQ','INR','ACTIVE','NSE:CM:SERIESDUP:EQ')"
    )
    with pytest.raises(UniqueViolation):
        db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, series, currency,"
            " status, canonical_key) VALUES "
            "('EQUITY','NSE','CM','SERIESDUP','EQ','INR','ACTIVE','other-key')"
        )


def test_same_symbol_null_series_still_collides(db_conn):
    """NULLS NOT DISTINCT must still apply to the new column: two rows with
    no series at all (F&O/AMFI-shaped) must collide exactly like before
    series existed."""
    from psycopg.errors import UniqueViolation

    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES "
        "('EQUITY','NSE','FO','NULLSERIESDUP','INR','ACTIVE','NSE:FO:NULLSERIESDUP')"
    )
    with pytest.raises(UniqueViolation):
        db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
            " canonical_key) VALUES "
            "('EQUITY','NSE','FO','NULLSERIESDUP','INR','ACTIVE','other-key')"
        )


def test_null_series_and_a_real_series_do_not_collide(db_conn):
    """A row with no series recorded and a row explicitly series='EQ' for
    the same symbol are different natural identities -- NULLS NOT DISTINCT
    only equates NULL with NULL, never with a real value."""
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES "
        "('EQUITY','NSE','CM','MIXEDSERIES','INR','ACTIVE','NSE:CM:MIXEDSERIES')"
    )
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, series, currency,"
        " status, canonical_key) VALUES "
        "('EQUITY','NSE','CM','MIXEDSERIES','EQ','INR','ACTIVE','NSE:CM:MIXEDSERIES:EQ')"
    )  # must not raise
    count = db_conn.execute(
        "SELECT count(*) FROM instruments WHERE symbol='MIXEDSERIES'"
    ).fetchone()[0]
    assert count == 2


# --- Task 18, Ruling S2: bars_daily gains underlying_price ---


def test_bars_daily_has_an_underlying_price_column(db_conn):
    row = db_conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='bars_daily' AND column_name='underlying_price'"
    ).fetchone()
    assert row is not None


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
        "BINANCE_WS": 6,
        "UPSTOX_HISTORICAL_CANDLE": 7,
        "UPSTOX_WS": 8,
        "BINANCE_FUTURES_KLINE": 9,
        "BINANCE_FUTURES_MARK": 10,
        "BINANCE_SPOT_KLINE": 11,
    }


def test_bars_intraday_accepts_a_fractional_volume(db_conn):
    """Crypto trade quantities are fractional Decimals (e.g. 0.01000000 BTC)
    -- volume must not be a BIGINT. Migration 0003 widens it to NUMERIC."""
    from decimal import Decimal

    iid = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency,"
        " status, canonical_key) VALUES ('CRYPTO','BINANCE','SPOT','TESTUSDT','USDT',"
        "'ACTIVE','BINANCE:SPOT:TESTUSDT') RETURNING instrument_id"
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low,"
        " close, volume, trades, source) VALUES (%s, '2026-08-24T12:00:00Z', 60,"
        " 100, 105, 98, 102, %s, 4, 6)",
        (iid, Decimal("0.01000000")),
    )  # must not raise
    row = db_conn.execute(
        "SELECT volume FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row[0] == Decimal("0.01000000")


def test_backtest_tables_exist(db_conn):
    rows = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    assert {"backtest_runs", "backtest_equity_points"} <= {r[0] for r in rows}


def test_an_equity_point_cannot_repeat_a_timestamp_within_a_run(db_conn):
    """`run_loop` emits one point per dispatched bar and bars are grouped by
    close_ts, so timestamps within a run are unique by construction. The
    composite primary key makes the database refuse to let that break: a
    doubled point would otherwise reach 3d as a wrong Sharpe and 3e as a
    real feature of the equity path, with nothing anywhere raising.
    """
    from datetime import UTC, datetime
    from decimal import Decimal

    import psycopg

    user = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    strategy_id = db_conn.execute(
        "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
        "status, contract_version) VALUES (%s,'dupe-pk','1.0.0','x','y','REGISTERED','0.1') "
        "RETURNING strategy_id",
        (user,),
    ).fetchone()[0]
    run_id = db_conn.execute(
        "INSERT INTO backtest_runs (strategy_id, status, requested_start, requested_end, "
        "fetch_start, dispatch_from, runtime, kernel_isolated, contract_version) "
        "VALUES (%s,'PASSED','2024-01-01','2024-01-02',now(),now(),'runc',false,'0.1') "
        "RETURNING backtest_run_id",
        (strategy_id,),
    ).fetchone()[0]
    ts = datetime(2024, 1, 2, 10, 0, tzinfo=UTC)
    db_conn.execute(
        "INSERT INTO backtest_equity_points (backtest_run_id, ts, equity, cash) "
        "VALUES (%s,%s,%s,%s)",
        (run_id, ts, Decimal("1"), Decimal("1")),
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        db_conn.execute(
            "INSERT INTO backtest_equity_points (backtest_run_id, ts, equity, cash) "
            "VALUES (%s,%s,%s,%s)",
            (run_id, ts, Decimal("2"), Decimal("2")),
        )
