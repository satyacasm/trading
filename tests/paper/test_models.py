import json
from decimal import Decimal

from trading.paper.enums import ChargeType, OrderStatus, Product, Side
from trading.paper.models import ChargeBreakdown


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
    )
    assert b.total == Decimal("179.16")


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
    )
    payload = json.loads(b.model_dump_json())
    assert isinstance(payload["brokerage"], (int, float))
    assert payload["brokerage"] == 20.0


def test_enums_are_string_valued() -> None:
    assert Side.BUY == "BUY"
    assert Product.DELIVERY == "DELIVERY"
    assert OrderStatus.PENDING == "PENDING"
    assert ChargeType.STT == "STT"
