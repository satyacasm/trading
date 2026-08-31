"""Pure fill-rule tests: no I/O, no clock, no DB. `decide_fill` takes an
order and one price event and returns a `FillDecision | None` -- these
tests pin the four rules that carry real design weight (limit-at-limit,
adverse-only slippage, no slippage on limits, anti-lookahead) plus the
terminal-status and partial-quantity guards.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.fills import decide_fill
from trading.paper.models import Order

T0 = datetime(2026, 8, 31, 6, 0, 0, tzinfo=UTC)
BPS = Decimal("10")  # 10 bps


def _order(**kw) -> Order:
    base = dict(
        order_id=1,
        portfolio_id=1,
        instrument_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        filled_quantity=Decimal("0"),
        limit_price=None,
        product=Product.DELIVERY,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.OPEN,
        rationale="test",
        submitted_at=T0,
    )
    base.update(kw)
    return Order(**base)


def test_market_buy_fills_with_adverse_slippage() -> None:
    d = decide_fill(_order(), Decimal("100"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("100.10")  # +10bps, against the buyer
    assert d.quantity == Decimal("10")


def test_market_sell_slippage_is_also_adverse() -> None:
    d = decide_fill(_order(side=Side.SELL), Decimal("100"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99.90")


def test_limit_buy_does_not_fill_above_the_limit() -> None:
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None


def test_limit_buy_fills_at_the_limit_not_the_better_tick_price() -> None:
    """Deliberate conservatism: assuming price improvement manufactures
    free money on every limit order, and Phase 3 would inherit it."""
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    d = decide_fill(o, Decimal("97"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99")


def test_limit_sell_fills_at_the_limit_when_crossed() -> None:
    o = _order(side=Side.SELL, order_type=OrderType.LIMIT, limit_price=Decimal("101"))
    d = decide_fill(o, Decimal("105"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("101")


def test_limit_orders_take_no_slippage() -> None:
    """Slippage models uncertainty about the traded price. A limit fill's
    price is known by construction."""
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    d = decide_fill(o, Decimal("98"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99")


def test_never_fills_on_a_tick_older_than_the_order() -> None:
    """The anti-lookahead invariant, asserted at the source."""
    assert decide_fill(_order(), Decimal("100"), T0 - timedelta(seconds=1), BPS) is None


def test_fills_only_the_remaining_quantity() -> None:
    o = _order(quantity=Decimal("10"), filled_quantity=Decimal("4"))
    d = decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.quantity == Decimal("6")


def test_terminal_orders_never_fill() -> None:
    for status in (
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    ):
        o = _order(status=status)
        assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None


def test_tick_at_exactly_submitted_at_can_fill() -> None:
    """Boundary case the brief's suite doesn't hit: `tick_ts ==
    order.submitted_at` is not 'earlier than', so the anti-lookahead guard
    (`tick_ts < order.submitted_at`) must not reject it. Getting this
    boundary's direction wrong either lets in one extra bar of lookahead
    or drops the very first tick every order is eligible for."""
    d = decide_fill(_order(), Decimal("100"), T0, BPS)
    assert d is not None
    assert d.price == Decimal("100.10")


def test_remaining_zero_never_fills() -> None:
    """An order fully filled but not yet transitioned to FILLED status
    (quantity == filled_quantity) must not fill again -- `remaining <= 0`
    is a guard independent of `status`."""
    o = _order(quantity=Decimal("10"), filled_quantity=Decimal("10"))
    assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None


def test_limit_order_with_no_limit_price_never_fills() -> None:
    """A LIMIT order with `limit_price=None` is malformed data, not a
    market order in disguise -- it must not silently fall through to a
    market fill or raise; it must simply never fill."""
    o = _order(order_type=OrderType.LIMIT, limit_price=None)
    assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None


def test_limit_buy_fills_exactly_at_the_limit_price() -> None:
    """Boundary: a tick printing exactly at the limit crosses (`<=` for a
    buy), it does not need to trade through it."""
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    d = decide_fill(o, Decimal("99"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99")
