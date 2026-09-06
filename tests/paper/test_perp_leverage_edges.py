"""Leverage when orders disagree about it.

A position has one leverage, because one number decides the margin behind
it. What happens when a second order names a different one is not obvious,
and getting it wrong is silent: the position keeps trading and its
liquidation price is quietly not the one either order implied.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.ledger import ConflictingLeverage, apply_fill
from trading.paper.models import ChargeBreakdown, FillDecision, Order

D = Decimal
_NOW = datetime(2026, 9, 6, tzinfo=UTC)
_ZERO = dict.fromkeys(
    (
        "brokerage",
        "stt",
        "exchange_txn",
        "sebi_fee",
        "stamp_duty",
        "ipft",
        "gst",
        "dp_charges",
        "tds",
    ),
    D("0"),
)


def _perp(db_conn) -> int:
    from datetime import date

    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    return seed_perp_instruments(
        db_conn,
        [
            PerpContractSpec(
                "BTCUSDT", "BTC", "USDT", D("0.10"), D("0.001"), D("0.001"), D("50"), D("0.0125")
            )
        ],
        on=date(2026, 9, 6),
    )["BTC-USDT"]


def _portfolio(db_conn) -> int:
    db_conn.execute(
        "INSERT INTO users (user_id, email) VALUES (905, 'lev@test') ON CONFLICT DO NOTHING"
    )
    return db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance,"
        " status) VALUES (905, %s, 'USDT', 100000, 100000, 'ACTIVE') RETURNING portfolio_id",
        (f"lev-{uuid4()}",),
    ).fetchone()[0]


def _fill(db_conn, portfolio_id, instrument_id, side: Side, qty: str, price: str, leverage: str):
    row = db_conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
        " product, time_in_force, status, rationale, leverage, idempotency_key)"
        " VALUES (%s,%s,%s,'MARKET',%s,'INTRADAY','GTC','OPEN','t',%s,%s)"
        " RETURNING order_id, submitted_at",
        (portfolio_id, instrument_id, side.value, qty, leverage, f"lev-{uuid4()}"),
    ).fetchone()
    order = Order(
        order_id=int(row[0]),
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=D(qty),
        filled_quantity=D("0"),
        limit_price=None,
        product=Product.INTRADAY,
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.OPEN,
        rationale="t",
        submitted_at=row[1],
        leverage=D(leverage),
    )
    return apply_fill(
        db_conn,
        order,
        FillDecision(quantity=D(qty), price=D(price), tick_ts=_NOW),
        ChargeBreakdown(**_ZERO),
    )


def _position(db_conn, portfolio_id, instrument_id):
    return db_conn.execute(
        "SELECT quantity, entry_price, leverage, reserved_margin FROM perp_positions"
        " WHERE portfolio_id=%s AND instrument_id=%s",
        (portfolio_id, instrument_id),
    ).fetchone()


def test_adding_at_a_different_leverage_is_refused(db_conn) -> None:
    """Silently keeping the open position's leverage would mean the second
    order reserved an amount its own leverage never implied, and the
    trader's liquidation price moved for a reason nothing on screen
    explains. Refusing says so while it can still be acted on."""
    instrument_id, portfolio_id = _perp(db_conn), _portfolio(db_conn)
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "80000", "10")

    with pytest.raises(ConflictingLeverage, match="10"):
        _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "80000", "20")


def test_adding_at_the_same_leverage_is_fine(db_conn) -> None:
    instrument_id, portfolio_id = _perp(db_conn), _portfolio(db_conn)
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "80000", "10")
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "90000", "10")

    quantity, entry, leverage, margin = _position(db_conn, portfolio_id, instrument_id)
    assert quantity == D("2.00000000")
    assert entry == D("85000.00000000")
    assert leverage == D("10.00")
    # Recomputed from the whole position at its averaged entry.
    assert margin == D("17000.00000000")


def test_reducing_at_a_different_leverage_is_allowed(db_conn) -> None:
    """Closing does not need to agree about leverage: it is releasing
    margin, not posting it. Refusing here would leave a trader unable to
    exit a position because they typed the wrong number in a box that no
    longer matters."""
    instrument_id, portfolio_id = _perp(db_conn), _portfolio(db_conn)
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "2", "80000", "10")
    _fill(db_conn, portfolio_id, instrument_id, Side.SELL, "1", "81000", "3")

    quantity, _entry, leverage, _margin = _position(db_conn, portfolio_id, instrument_id)
    assert quantity == D("1.00000000")
    # The surviving half keeps the leverage it was opened at.
    assert leverage == D("10.00")


def test_crossing_through_flat_adopts_the_new_orders_leverage(db_conn) -> None:
    """The bug this file was written for. Selling 3 against a long of 1
    closes the long and opens a short of 2 -- a *new* position, at this
    order's price and therefore at this order's leverage. Carrying the old
    one forward would margin the new short by a number the trader chose
    for a position that no longer exists.
    """
    instrument_id, portfolio_id = _perp(db_conn), _portfolio(db_conn)
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "80000", "10")
    _fill(db_conn, portfolio_id, instrument_id, Side.SELL, "3", "82000", "4")

    quantity, entry, leverage, margin = _position(db_conn, portfolio_id, instrument_id)
    assert quantity == D("-2.00000000")
    assert entry == D("82000.00000000")
    assert leverage == D("4.00")
    # 2 x 82,000 / 4.
    assert margin == D("41000.00000000")


def test_reopening_after_a_full_close_takes_the_new_leverage(db_conn) -> None:
    """A flat row is not a position. The next order opens a new one and
    brings its own leverage."""
    instrument_id, portfolio_id = _perp(db_conn), _portfolio(db_conn)
    _fill(db_conn, portfolio_id, instrument_id, Side.BUY, "1", "80000", "10")
    _fill(db_conn, portfolio_id, instrument_id, Side.SELL, "1", "80000", "10")
    _fill(db_conn, portfolio_id, instrument_id, Side.SELL, "1", "80000", "25")

    quantity, _entry, leverage, _margin = _position(db_conn, portfolio_id, instrument_id)
    assert quantity == D("-1.00000000")
    assert leverage == D("25.00")
