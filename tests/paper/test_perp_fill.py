"""Applying a perpetual fill: cash, margin, and a signed position."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.ledger import apply_fill
from trading.paper.models import ChargeBreakdown, FillDecision, Order

D = Decimal
_NOW = datetime(2026, 9, 6, 7, 30, tzinfo=UTC)


def _perp_instrument(db_conn) -> int:
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


def _portfolio(db_conn, cash: str = "100000") -> int:
    db_conn.execute(
        "INSERT INTO users (user_id, email) VALUES (900, 'perp@test') ON CONFLICT DO NOTHING"
    )
    row = db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance,"
        " status) VALUES (900, %s, 'USDT', %s, %s, 'ACTIVE') RETURNING portfolio_id",
        (f"perp-{cash}", cash, cash),
    ).fetchone()
    return int(row[0])


def _order(db_conn, portfolio_id: int, instrument_id: int, side: Side, qty: str) -> Order:
    row = db_conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
        " product, time_in_force, status, rationale, leverage, idempotency_key)"
        " VALUES (%s,%s,%s,'MARKET',%s,'INTRADAY','GTC','OPEN','test',10,%s)"
        " RETURNING order_id, submitted_at",
        (portfolio_id, instrument_id, side.value, qty, f"perp-test-{uuid4()}"),
    ).fetchone()
    return Order(
        order_id=int(row[0]),
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=D(qty),
        limit_price=None,
        product=Product.INTRADAY,
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.OPEN,
        filled_quantity=D("0"),
        submitted_at=row[1],
        updated_at=row[1],
        rationale="test",
    )


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
    Decimal("0"),
)


def _no_charges() -> ChargeBreakdown:
    return ChargeBreakdown(**_ZERO)


def test_opening_a_short_does_not_credit_the_notional(db_conn) -> None:
    """The difference that defines a perpetual. Selling 1 BTC of spot at
    80,000 credits 80,000 of cash. Selling 1 BTC of perpetual credits
    nothing -- it locks up margin and cash only moves on realised P&L,
    fees and funding. Crediting the notional would hand the portfolio
    80,000 it does not have.
    """
    instrument_id = _perp_instrument(db_conn)
    portfolio_id = _portfolio(db_conn)
    order = _order(db_conn, portfolio_id, instrument_id, Side.SELL, "1")

    apply_fill(
        db_conn, order, FillDecision(quantity=D("1"), price=D("80000"), tick_ts=_NOW), _no_charges()
    )

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert cash == D("100000.0000")

    quantity, entry, reserved = db_conn.execute(
        "SELECT quantity, entry_price, reserved_margin FROM perp_positions"
        " WHERE portfolio_id=%s AND instrument_id=%s",
        (portfolio_id, instrument_id),
    ).fetchone()
    assert quantity == D("-1.00000000")
    assert entry == D("80000.00000000")
    # 1 x 80,000 at 10x.
    assert reserved == D("8000.00000000")


def test_closing_a_short_moves_cash_by_the_profit_only(db_conn) -> None:
    instrument_id = _perp_instrument(db_conn)
    portfolio_id = _portfolio(db_conn)

    opening = _order(db_conn, portfolio_id, instrument_id, Side.SELL, "1")
    apply_fill(
        db_conn,
        opening,
        FillDecision(quantity=D("1"), price=D("80000"), tick_ts=_NOW),
        _no_charges(),
    )

    closing = _order(db_conn, portfolio_id, instrument_id, Side.BUY, "1")
    apply_fill(
        db_conn,
        closing,
        FillDecision(quantity=D("1"), price=D("79000"), tick_ts=_NOW),
        _no_charges(),
    )

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    # Sold at 80,000, bought back at 79,000: 1,000 made, and nothing else.
    assert cash == D("101000.0000")

    quantity, reserved = db_conn.execute(
        "SELECT quantity, reserved_margin FROM perp_positions"
        " WHERE portfolio_id=%s AND instrument_id=%s",
        (portfolio_id, instrument_id),
    ).fetchone()
    assert quantity == D("0E-8")
    # Flat releases everything. Margin that leaks on a round trip
    # eventually stops the portfolio opening anything, with no position
    # left to explain why.
    assert reserved == D("0E-8")


def test_charges_leave_the_account_on_an_opening_fill(db_conn) -> None:
    """Cash does not move by notional, but it does move by fees -- a
    perpetual that cost nothing to open would make every strategy look
    better than it is."""
    instrument_id = _perp_instrument(db_conn)
    portfolio_id = _portfolio(db_conn)
    order = _order(db_conn, portfolio_id, instrument_id, Side.SELL, "1")

    apply_fill(
        db_conn,
        order,
        FillDecision(quantity=D("1"), price=D("80000"), tick_ts=_NOW),
        ChargeBreakdown(**{**_ZERO, "brokerage": D("40")}),
    )

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert cash == D("99960.0000")


def test_a_spot_fill_is_untouched_by_any_of_this(db_conn) -> None:
    """The regression that matters: spot still moves cash by notional."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    instrument_id = seed_crypto_instruments(db_conn, pairs=["ETH-USDT"])["ETH-USDT"]
    portfolio_id = _portfolio(db_conn)
    order = _order(db_conn, portfolio_id, instrument_id, Side.BUY, "2")

    apply_fill(
        db_conn, order, FillDecision(quantity=D("2"), price=D("100"), tick_ts=_NOW), _no_charges()
    )

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert cash == D("99800.0000")
    held = db_conn.execute(
        "SELECT quantity FROM positions WHERE portfolio_id=%s AND instrument_id=%s",
        (portfolio_id, instrument_id),
    ).fetchone()[0]
    assert held == D("2.00000000")
    assert (
        db_conn.execute(
            "SELECT count(*) FROM perp_positions WHERE portfolio_id=%s", (portfolio_id,)
        ).fetchone()[0]
        == 0
    )
