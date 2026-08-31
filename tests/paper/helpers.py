"""Test fixtures for `tests/paper/test_ledger.py` (and any other paper
test that needs a portfolio, an order, or a fill decision).

Each helper inserts the minimal row a real caller would need and returns
the id or model, mirroring the pattern `tests/corpactions/conftest.py`
uses for `seeded_instrument`. Nothing here commits -- `tests/conftest.py`'s
`db_conn` fixture rolls back at teardown, and every helper in this module
relies on that for isolation between tests.

`decision_at` defaults its quantity to the most recently created order's
remaining quantity (tracked in `_last_order`, set by `make_order`). This
mirrors real usage -- `decide_fill` always returns a decision sized to
`order.remaining` -- and it is what lets the brief's test calls read as
plain `decision_at(price)` while still filling a 4-quantity sell order for
exactly 4 and a 10-quantity buy order for exactly 10, without every call
site threading a redundant quantity through by hand. Pass `quantity=`
explicitly to override it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from itertools import count

from psycopg import Connection

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import ChargeBreakdown, FillDecision, Order, Position

_ZERO = Decimal("0")

_CHARGE_FIELDS = (
    "brokerage",
    "stt",
    "exchange_txn",
    "sebi_fee",
    "stamp_duty",
    "ipft",
    "gst",
    "dp_charges",
)

_seq = count(1)

# Set by `make_order`, read by `decision_at`'s default. See module docstring.
_last_order: Order | None = None

_TEST_INSTRUMENT_KEY = "TEST/LEDGER/INSTRUMENT"


def _default_instrument(conn: Connection) -> int:
    """Get-or-create the one instrument every helper trades by default.

    Every `make_order` call in a test that doesn't pass `instrument_id`
    resolves to this same row, so two orders for the same portfolio land
    on the same `positions` row -- required by
    `test_position_average_cost_after_two_buys` and every other test that
    fills more than one order per portfolio.
    """
    row = conn.execute(
        "SELECT instrument_id FROM instruments WHERE canonical_key=%s",
        (_TEST_INSTRUMENT_KEY,),
    ).fetchone()
    if row is not None:
        return int(row[0])
    row = conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol,"
        " status, canonical_key) VALUES ('EQUITY', 'NSE', 'CM', 'LEDGERTEST',"
        " 'ACTIVE', %s) RETURNING instrument_id",
        (_TEST_INSTRUMENT_KEY,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def make_portfolio(conn: Connection, *, cash: Decimal) -> int:
    """Insert a portfolio owned by migration 0007's seeded local user,
    with `cash` as both `initial_capital` and `cash_balance` -- the state
    a freshly funded, untraded portfolio is in."""
    user_row = conn.execute(
        "SELECT user_id FROM users WHERE email='local@paper.trading'"
    ).fetchone()
    assert user_row is not None, "migration 0007 must seed the local user"
    row = conn.execute(
        "INSERT INTO portfolios (user_id, name, initial_capital, cash_balance, status)"
        " VALUES (%s, %s, %s, %s, 'ACTIVE') RETURNING portfolio_id",
        (user_row[0], f"test-portfolio-{next(_seq)}", cash, cash),
    ).fetchone()
    assert row is not None
    return int(row[0])


def make_order(
    conn: Connection,
    portfolio_id: int,
    *,
    side: Side,
    quantity: Decimal,
    instrument_id: int | None = None,
    order_type: OrderType = OrderType.MARKET,
    limit_price: Decimal | None = None,
    product: Product = Product.DELIVERY,
    time_in_force: TimeInForce = TimeInForce.DAY,
    status: OrderStatus = OrderStatus.OPEN,
    filled_quantity: Decimal = _ZERO,
    submitted_at: datetime | None = None,
) -> Order:
    """Insert an order and return it as the `Order` model `apply_fill`
    consumes."""
    global _last_order
    iid = instrument_id if instrument_id is not None else _default_instrument(conn)
    ts = submitted_at or datetime.now(UTC)
    row = conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type,"
        " quantity, filled_quantity, limit_price, product, time_in_force,"
        " status, rationale, idempotency_key, submitted_at)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        " RETURNING order_id, submitted_at",
        (
            portfolio_id,
            iid,
            side.value,
            order_type.value,
            quantity,
            filled_quantity,
            limit_price,
            product.value,
            time_in_force.value,
            status.value,
            "test order",
            f"test-order-{next(_seq)}",
            ts,
        ),
    ).fetchone()
    assert row is not None
    order_id, submitted_at_db = row
    order = Order(
        order_id=int(order_id),
        portfolio_id=portfolio_id,
        instrument_id=iid,
        side=side,
        order_type=order_type,
        quantity=quantity,
        filled_quantity=filled_quantity,
        limit_price=limit_price,
        product=product,
        time_in_force=time_in_force,
        status=status,
        rationale="test order",
        submitted_at=submitted_at_db,
    )
    _last_order = order
    return order


def simple_charges(**overrides: Decimal) -> ChargeBreakdown:
    """A `ChargeBreakdown` with every component zero except those passed."""
    fields = dict.fromkeys(_CHARGE_FIELDS, _ZERO)
    fields.update(overrides)
    return ChargeBreakdown(**fields)  # type: ignore[arg-type]


def decision_at(
    price: Decimal,
    *,
    quantity: Decimal | None = None,
    tick_ts: datetime | None = None,
) -> FillDecision:
    """A `FillDecision` at `price`. `quantity` defaults to the remaining
    quantity of the most recently created order (see module docstring)."""
    if quantity is None:
        assert _last_order is not None, "decision_at needs a prior make_order call"
        quantity = _last_order.remaining
    return FillDecision(quantity=quantity, price=price, tick_ts=tick_ts or datetime.now(UTC))


def _positions(conn: Connection, portfolio_id: int) -> dict[int, Position]:
    """Every `positions` row for `portfolio_id`, keyed by `instrument_id` --
    directly comparable to `replay_portfolio`'s second return value."""
    rows = conn.execute(
        "SELECT instrument_id, quantity, avg_cost, realised_pnl FROM positions"
        " WHERE portfolio_id=%s",
        (portfolio_id,),
    ).fetchall()
    return {
        int(instrument_id): Position(
            portfolio_id=portfolio_id,
            instrument_id=int(instrument_id),
            quantity=quantity,
            avg_cost=avg_cost,
            realised_pnl=realised_pnl,
        )
        for instrument_id, quantity, avg_cost, realised_pnl in rows
    }
