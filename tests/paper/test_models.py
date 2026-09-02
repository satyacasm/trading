import json
from datetime import datetime
from decimal import Decimal

from trading.paper.enums import (
    ChargeType,
    OrderStatus,
    OrderType,
    Product,
    Side,
    TimeInForce,
)
from trading.paper.models import ChargeBreakdown, Order, Portfolio, Position


def test_charge_breakdown_totals_its_components() -> None:
    b = ChargeBreakdown(
        brokerage=Decimal("20.00"),
        stt=Decimal("131.00"),
        exchange_txn=Decimal("4.02"),
        sebi_fee=Decimal("0.13"),
        stamp_duty=Decimal("19.65"),
        ipft=Decimal("0.01"),
        gst=Decimal("4.35"),
        dp_charges=Decimal("0.00"),
        tds=Decimal("0.00"),
    )
    assert b.total == Decimal("179.16")


def test_charge_breakdown_includes_tds_in_total() -> None:
    """IMP-2: TDS (crypto 1% TDS, plan Sec4.3) is a real ChargeType but had
    no field on ChargeBreakdown at all -- compute_charges accumulated a TDS
    row into its internal `amounts` dict and then dropped it on the floor
    when building the return value. Seeding a TDS charge-schedule row would
    have zeroed it silently, forever."""
    b = ChargeBreakdown(
        brokerage=Decimal("0.00"),
        stt=Decimal("0.00"),
        exchange_txn=Decimal("0.00"),
        sebi_fee=Decimal("0.00"),
        stamp_duty=Decimal("0.00"),
        ipft=Decimal("0.00"),
        gst=Decimal("0.00"),
        dp_charges=Decimal("0.00"),
        tds=Decimal("50.00"),
    )
    assert b.tds == Decimal("50.00")
    assert b.total == Decimal("50.00")


def test_money_serialises_as_json_number_not_string() -> None:
    """Task 7b lesson: pydantic v2 renders Decimal as a string by default,
    which broke the frontend's arithmetic. Money must be a JSON number."""
    b = ChargeBreakdown(
        brokerage=Decimal("20.00"),
        stt=Decimal("0"),
        exchange_txn=Decimal("0"),
        sebi_fee=Decimal("0"),
        stamp_duty=Decimal("0"),
        ipft=Decimal("0"),
        gst=Decimal("0"),
        dp_charges=Decimal("0"),
        tds=Decimal("0"),
    )
    payload = json.loads(b.model_dump_json())
    assert isinstance(payload["brokerage"], (int, float))
    assert payload["brokerage"] == 20.0


def test_enums_are_string_valued() -> None:
    assert Side.BUY == "BUY"
    assert Product.DELIVERY == "DELIVERY"
    assert OrderStatus.PENDING == "PENDING"
    assert ChargeType.STT == "STT"


def _make_order(*, limit_price: Decimal | None) -> Order:
    return Order(
        order_id=1,
        portfolio_id=1,
        instrument_id=1,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("10"),
        filled_quantity=Decimal("4"),
        limit_price=limit_price,
        product=Product.DELIVERY,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.PARTIALLY_FILLED,
        rationale="test",
        submitted_at=datetime(2026, 8, 31, 9, 15),
    )


def test_order_money_and_quantity_serialise_as_json_numbers() -> None:
    order = _make_order(limit_price=Decimal("101.50"))
    payload = json.loads(order.model_dump_json())
    assert isinstance(payload["quantity"], (int, float))
    assert isinstance(payload["filled_quantity"], (int, float))
    assert isinstance(payload["limit_price"], (int, float))
    assert payload["quantity"] == 10.0
    assert payload["filled_quantity"] == 4.0
    assert payload["limit_price"] == 101.50


def test_order_limit_price_none_passes_through_as_null() -> None:
    order = _make_order(limit_price=None)
    payload = json.loads(order.model_dump_json())
    assert payload["limit_price"] is None


def test_position_money_serialises_as_json_numbers() -> None:
    position = Position(
        portfolio_id=1,
        instrument_id=1,
        quantity=Decimal("25"),
        avg_cost=Decimal("101.50"),
        realised_pnl=Decimal("-12.34"),
    )
    payload = json.loads(position.model_dump_json())
    assert isinstance(payload["quantity"], (int, float))
    assert isinstance(payload["avg_cost"], (int, float))
    assert isinstance(payload["realised_pnl"], (int, float))
    assert payload["quantity"] == 25.0
    assert payload["avg_cost"] == 101.50
    assert payload["realised_pnl"] == -12.34


def _make_portfolio(
    *, max_daily_loss: Decimal | None, max_drawdown_pct: Decimal | None
) -> Portfolio:
    return Portfolio(
        portfolio_id=1,
        user_id=1,
        name="test",
        base_currency="INR",
        initial_capital=Decimal("100000.00"),
        cash_balance=Decimal("87654.32"),
        status="ACTIVE",
        max_daily_loss=max_daily_loss,
        max_drawdown_pct=max_drawdown_pct,
    )


def test_portfolio_money_serialises_as_json_numbers() -> None:
    portfolio = _make_portfolio(
        max_daily_loss=Decimal("5000.00"), max_drawdown_pct=Decimal("10.00")
    )
    payload = json.loads(portfolio.model_dump_json())
    assert isinstance(payload["initial_capital"], (int, float))
    assert isinstance(payload["cash_balance"], (int, float))
    assert isinstance(payload["max_daily_loss"], (int, float))
    assert isinstance(payload["max_drawdown_pct"], (int, float))
    assert payload["initial_capital"] == 100000.0
    assert payload["cash_balance"] == 87654.32
    assert payload["max_daily_loss"] == 5000.0
    assert payload["max_drawdown_pct"] == 10.0


def test_portfolio_optional_limits_none_passes_through_as_null_not_zero() -> None:
    """A null limit means 'no circuit breaker configured'. Coercing it to
    0.0 would trip the breaker instantly on the first evaluation."""
    portfolio = _make_portfolio(max_daily_loss=None, max_drawdown_pct=None)
    payload = json.loads(portfolio.model_dump_json())
    assert payload["max_daily_loss"] is None
    assert payload["max_drawdown_pct"] is None
