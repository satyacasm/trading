from __future__ import annotations

import pytest

from trading.streaming.seed_instruments import CRYPTO_PAIRS, seed_crypto_instruments

pytestmark = pytest.mark.db


def test_seed_creates_one_instrument_per_pair(db_conn):
    result = seed_crypto_instruments(db_conn)

    assert set(result) == set(CRYPTO_PAIRS)
    for symbol, instrument_id in result.items():
        row = db_conn.execute(
            "SELECT asset_class, exchange, segment, symbol, currency, status "
            "FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        ).fetchone()
        assert row == ("CRYPTO", "BINANCE", "SPOT", symbol, "USDT", "ACTIVE")


def test_seed_is_idempotent(db_conn):
    first = seed_crypto_instruments(db_conn)
    second = seed_crypto_instruments(db_conn)

    assert first == second  # same instrument_ids, no duplicate rows

    count = db_conn.execute(
        "SELECT count(*) FROM instruments WHERE exchange = 'BINANCE'"
    ).fetchone()[0]
    assert count == len(CRYPTO_PAIRS)


def test_seed_accepts_a_custom_pair_list(db_conn):
    result = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])
    assert set(result) == {"BTC-USDT"}
