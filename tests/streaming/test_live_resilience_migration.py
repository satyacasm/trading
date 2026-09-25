"""migrations/versions/0026_live_resilience.py: the delivery-cursor table,
persisted ctx.state, the gap note, and the spot-kline provenance code."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.db


def test_live_run_cursors_table_exists_with_the_right_shape(db_conn) -> None:
    rows = db_conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'live_run_cursors'"
    ).fetchall()
    by_name = {r[0]: r[1] for r in rows}
    assert by_name == {
        "live_run_id": "bigint",
        "instrument_id": "bigint",
        "last_ts": "timestamp with time zone",
    }
    pk = db_conn.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = 'live_run_cursors'::regclass AND i.indisprimary"
    ).fetchall()
    assert {r[0] for r in pk} == {"live_run_id", "instrument_id"}


def test_live_runs_gained_strategy_state_and_last_gap_note(db_conn) -> None:
    rows = db_conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'live_runs'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {"strategy_state", "last_gap_note"} <= names


def test_binance_spot_kline_source_is_seeded(db_conn) -> None:
    row = db_conn.execute(
        "SELECT source_key FROM data_sources WHERE source_id = 11"
    ).fetchone()
    assert row == ("BINANCE_SPOT_KLINE",)
