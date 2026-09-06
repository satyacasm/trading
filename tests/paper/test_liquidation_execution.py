"""Closing a position the exchange would have closed."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from trading.paper.liquidation import liquidate_open_positions

D = Decimal
pytestmark = pytest.mark.db


def _setup(db_conn, quantity: str, *, cash: str = "10000", margin: str = "8000"):
    from datetime import date

    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    instrument_id = seed_perp_instruments(
        db_conn,
        [
            PerpContractSpec(
                "BTCUSDT", "BTC", "USDT", D("0.10"), D("0.001"), D("0.001"), D("50"), D("0.0125")
            )
        ],
        on=date(2026, 9, 6),
    )["BTC-USDT"]
    db_conn.execute(
        "INSERT INTO perp_margin_tiers (instrument_id, notional_floor, effective_from,"
        " notional_cap, max_leverage, maintenance_rate, maintenance_amount)"
        " VALUES (%s, 0, '2019-09-08', 300000, 125, 0.004, 0) ON CONFLICT DO NOTHING",
        (instrument_id,),
    )
    db_conn.execute(
        "INSERT INTO users (user_id, email) VALUES (903, 'liq@test') ON CONFLICT DO NOTHING"
    )
    portfolio_id = db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance,"
        " status) VALUES (903, %s, 'USDT', %s, %s, 'ACTIVE') RETURNING portfolio_id",
        (f"liq-{uuid4()}", cash, cash),
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO perp_positions (portfolio_id, instrument_id, quantity, entry_price,"
        " leverage, reserved_margin) VALUES (%s,%s,%s,80000,10,%s)",
        (portfolio_id, instrument_id, quantity, margin),
    )
    return instrument_id, portfolio_id


def test_a_healthy_position_is_left_alone(db_conn) -> None:
    instrument_id, portfolio_id = _setup(db_conn, "1")
    closed = liquidate_open_positions(db_conn, {instrument_id: D("79000")}, now=datetime.now(UTC))
    assert closed == []
    quantity = db_conn.execute(
        "SELECT quantity FROM perp_positions WHERE portfolio_id=%s", (portfolio_id,)
    ).fetchone()[0]
    assert quantity == D("1.00000000")


def test_a_long_past_its_line_is_closed_and_lands_in_the_blotter(db_conn) -> None:
    """Task 5's demo. The forced close goes through the ordinary order and
    fill path, so it appears beside every other trade rather than as a
    position that silently vanished."""
    instrument_id, portfolio_id = _setup(db_conn, "1")

    closed = liquidate_open_positions(db_conn, {instrument_id: D("71500")}, now=datetime.now(UTC))
    assert len(closed) == 1

    quantity, reserved = db_conn.execute(
        "SELECT quantity, reserved_margin FROM perp_positions WHERE portfolio_id=%s",
        (portfolio_id,),
    ).fetchone()
    assert quantity == D("0E-8")
    assert reserved == D("0E-8")

    side, status, rationale = db_conn.execute(
        "SELECT side, status, rationale FROM orders WHERE portfolio_id=%s"
        " ORDER BY order_id DESC LIMIT 1",
        (portfolio_id,),
    ).fetchone()
    # Opposite side, because closing a long is a sell.
    assert side == "SELL"
    assert status == "FILLED"
    assert "liquidat" in rationale.lower()


def test_a_short_is_liquidated_by_a_rally(db_conn) -> None:
    instrument_id, portfolio_id = _setup(db_conn, "-1")
    closed = liquidate_open_positions(db_conn, {instrument_id: D("88000")}, now=datetime.now(UTC))
    assert len(closed) == 1
    side = db_conn.execute(
        "SELECT side FROM orders WHERE portfolio_id=%s ORDER BY order_id DESC LIMIT 1",
        (portfolio_id,),
    ).fetchone()[0]
    assert side == "BUY"


def test_the_liquidation_fee_is_charged_on_the_closing_notional(db_conn) -> None:
    """1.25% of what was closed, from `perp_contract_specs` -- the rate the
    exchange publishes per contract, not a constant.

    The mark is inside the maintenance buffer on purpose. For this
    position liquidation is ~72,289 and bankruptcy is 72,000, so anything
    below 72,000 is capped at the bankruptcy price and would charge the
    fee on that instead -- which is how this test was first written wrong.
    """
    instrument_id, portfolio_id = _setup(db_conn, "1")
    liquidate_open_positions(db_conn, {instrument_id: D("72100")}, now=datetime.now(UTC))

    charges = db_conn.execute(
        "SELECT total_charges FROM fills f JOIN orders o USING (order_id)"
        " WHERE o.portfolio_id = %s ORDER BY f.fill_id DESC LIMIT 1",
        (portfolio_id,),
    ).fetchone()[0]
    assert charges == D("901.2500")  # 72,100 x 0.0125


def test_a_gap_beyond_bankruptcy_caps_the_loss_and_records_the_shortfall(db_conn) -> None:
    """The market can move faster than a liquidation can execute. A real
    venue's insurance fund absorbs the difference; this platform has none,
    so it caps the fill at the bankruptcy price and says what the gap was
    rather than driving cash negative or pretending the loss stopped."""
    instrument_id, portfolio_id = _setup(db_conn, "1")

    # Bankruptcy for this position is 72,000. A print at 60,000 is a gap.
    closed = liquidate_open_positions(db_conn, {instrument_id: D("60000")}, now=datetime.now(UTC))
    assert len(closed) == 1
    assert closed[0].shortfall > D("0")

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (portfolio_id,)
    ).fetchone()[0]
    assert cash >= D("0")

    entry_type = db_conn.execute(
        "SELECT entry_type FROM ledger_entries WHERE portfolio_id=%s"
        " AND entry_type='LIQUIDATION' LIMIT 1",
        (portfolio_id,),
    ).fetchone()
    assert entry_type is not None, "a shortfall must leave a row saying so"
