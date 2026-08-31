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


CURRENT_REGIME_RATES = [
    # (product, charge_type, applies_to_side, rate, cap)
    ("DELIVERY", "BROKERAGE", "BOTH", Decimal("20"), None),
    ("INTRADAY", "BROKERAGE", "BOTH", Decimal("0.001"), Decimal("20")),
    ("DELIVERY", "STT", "BOTH", Decimal("0.001"), None),
    ("INTRADAY", "STT", "SELL", Decimal("0.00025"), None),
    ("DELIVERY", "SEBI_FEE", "BOTH", Decimal("0.000001"), None),
    ("DELIVERY", "STAMP_DUTY", "BUY", Decimal("0.00015"), None),
    ("INTRADAY", "STAMP_DUTY", "BUY", Decimal("0.00003"), None),
    ("DELIVERY", "IPFT", "BOTH", Decimal("0.000000001"), None),
    ("DELIVERY", "DP_CHARGES", "SELL", Decimal("20"), None),
    ("DELIVERY", "GST", "BOTH", Decimal("0.18"), None),
]


@pytest.mark.parametrize(
    "product,charge_type,side,expected_rate,expected_cap", CURRENT_REGIME_RATES
)
def test_equity_charge_rate_seeded_correctly(
    db_conn: Connection,
    product: str,
    charge_type: str,
    side: str,
    expected_rate: Decimal,
    expected_cap: Decimal | None,
) -> None:
    """Every seeded equity charge rate must match the verified source value.

    Catches transcription errors like the IPFT rate being off by 100x
    (0.0000001 seeded instead of the correct 0.000000001) that the
    two-regime EXCHANGE_TXN test alone cannot catch.
    """
    rows = db_conn.execute(
        "SELECT rate, cap FROM charge_schedules "
        "WHERE broker='UPSTOX' AND exchange='NSE' AND asset_class='EQUITY' "
        "AND product=%s AND charge_type=%s AND applies_to_side=%s "
        "AND effective_to IS NULL",
        (product, charge_type, side),
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one current-regime row for "
        f"{product}/{charge_type}/{side}, got {len(rows)}"
    )
    rate, cap = rows[0]
    assert rate == expected_rate
    assert cap == expected_cap


def test_gst_base_types_for_delivery_and_intraday(db_conn: Connection) -> None:
    rows = db_conn.execute(
        "SELECT product, gst_base_types FROM charge_schedules "
        "WHERE broker='UPSTOX' AND exchange='NSE' AND charge_type='GST' "
        "ORDER BY product"
    ).fetchall()
    assert dict(rows) == {
        "DELIVERY": "BROKERAGE,EXCHANGE_TXN,DP_CHARGES,IPFT",
        "INTRADAY": "BROKERAGE,EXCHANGE_TXN,IPFT",
    }


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
