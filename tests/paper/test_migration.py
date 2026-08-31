"""Migration 0007 creates the paper-trading schema and seeds charge rates."""

from __future__ import annotations

from decimal import Decimal

import pytest
from psycopg import Connection

pytestmark = pytest.mark.db

EXPECTED_TABLES = [
    "portfolios",
    "orders",
    "fills",
    "ledger_entries",
    "positions",
    "portfolio_equity_snapshots",
    "circuit_breaker_events",
    "alert_deliveries",
    "charge_schedules",
]


@pytest.mark.parametrize("table", EXPECTED_TABLES)
def test_migration_creates_table(db_conn: Connection, table: str) -> None:
    row = db_conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=%s",
        (table,),
    ).fetchone()
    assert row is not None, f"{table} was not created"


def test_seeds_a_single_local_user(db_conn: Connection) -> None:
    row = db_conn.execute("SELECT count(*) FROM users").fetchone()
    assert row is not None
    assert row[0] >= 1


def test_nse_transaction_charge_has_two_dated_regimes(db_conn: Connection) -> None:
    """The 2026-03-01 revision must be seeded as two rows, not one.

    NSE cash transaction charges moved 0.00297% -> 0.00307% effective
    2026-03-01. The backfill spans 2022-2026 and crosses that boundary,
    so a single row would misprice most of the historical period.
    """
    # Filter by product: both DELIVERY and INTRADAY carry both date
    # regimes, so an unfiltered query returns four rows, not two.
    rows = db_conn.execute(
        "SELECT rate, effective_from, effective_to FROM charge_schedules "
        "WHERE exchange='NSE' AND charge_type='EXCHANGE_TXN' "
        "AND product='DELIVERY' "
        "ORDER BY effective_from"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == Decimal("0.0000297")
    assert rows[0][2] is not None, "the older regime must be closed off"
    assert rows[1][0] == Decimal("0.0000307")
    assert rows[1][2] is None, "the current regime must be open-ended"


def test_money_columns_are_numeric_not_float(db_conn: Connection) -> None:
    rows = db_conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='fills' AND column_name IN "
        "('price','brokerage','stt','exchange_txn','sebi_fee',"
        "'stamp_duty','ipft','gst','dp_charges')"
    ).fetchall()
    assert len(rows) == 9
    for name, dtype in rows:
        assert dtype == "numeric", f"{name} is {dtype}, must be numeric"
