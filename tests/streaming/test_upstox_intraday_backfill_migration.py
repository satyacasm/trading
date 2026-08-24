from __future__ import annotations

import pytest

pytestmark = pytest.mark.db


def test_upstox_historical_candle_source_row_exists(db_conn):
    row = db_conn.execute("SELECT source_key FROM data_sources WHERE source_id = 7").fetchone()
    assert row is not None
    assert row[0] == "UPSTOX_HISTORICAL_CANDLE"
