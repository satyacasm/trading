from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.db


def test_watchlists_table_has_expected_columns(db_conn):
    rows = db_conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'watchlists'"
    ).fetchall()
    assert {row[0] for row in rows} == {"instrument_id", "added_at"}


def test_watchlists_instrument_id_references_instruments(db_conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db_conn.execute("INSERT INTO watchlists (instrument_id) VALUES (999999999)")
