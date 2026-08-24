from __future__ import annotations

import pytest

from trading.streaming.seed_upstox_instruments import (
    UPSTOX_WATCHLIST,
    seed_upstox_instrument_keys,
)

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _seed_watchlist_instruments(db_conn):
    """Seed the NSE watchlist instruments required for the tests."""
    # Data: (symbol, isin, series)
    data = [
        ("RELIANCE", "INE002A01018", "EQ"),
        ("RELIANCE", "INE002A01018", "BL"),
        ("TCS", "INE467B01029", "EQ"),
        ("TCS", "INE467B01029", "BL"),
        ("INFY", "INE009A01021", "EQ"),
        ("INFY", "INE009A01021", "BL"),
        ("HDFCBANK", "INE040A01034", "EQ"),
        ("HDFCBANK", "INE040A01034", "BL"),
        ("ICICIBANK", "INE090A01021", "EQ"),
        ("ICICIBANK", "INE090A01021", "BL"),
    ]

    for symbol, isin, series in data:
        db_conn.execute(
            """
            INSERT INTO instruments
            (asset_class, exchange, segment, symbol, series, currency, isin, status, canonical_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (canonical_key) DO NOTHING
            """,
            (
                "EQUITY",
                "NSE",
                "CM",
                symbol,
                series,
                "INR",
                isin,
                "ACTIVE",
                f"NSE:CM:{symbol}:{series}",
            ),
        )


def test_seed_maps_every_watchlist_symbol_to_its_eq_series_instrument(db_conn):
    result = seed_upstox_instrument_keys(db_conn)

    assert len(result) == len(UPSTOX_WATCHLIST)
    for upstox_key, instrument_id in result.items():
        assert upstox_key.startswith("NSE_EQ|")
        row = db_conn.execute(
            "SELECT series, source_bindings ->> 'upstox_instrument_key' "
            "FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        ).fetchone()
        assert row is not None
        series, stored_key = row
        assert series == "EQ"
        assert stored_key == upstox_key


def test_seed_is_idempotent(db_conn):
    first = seed_upstox_instrument_keys(db_conn)
    second = seed_upstox_instrument_keys(db_conn)
    assert first == second


def test_seed_accepts_a_custom_symbol_list(db_conn):
    result = seed_upstox_instrument_keys(db_conn, symbols=["RELIANCE"])
    assert set(result.values()) == {
        db_conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol = 'RELIANCE' "
            "AND exchange = 'NSE' AND segment = 'CM' AND series = 'EQ'"
        ).fetchone()[0]
    }


def test_seed_raises_a_clear_error_for_an_unknown_symbol(db_conn):
    with pytest.raises(ValueError, match="NOTASYMBOL"):
        seed_upstox_instrument_keys(db_conn, symbols=["NOTASYMBOL"])
