"""What a perpetual needs from the backtest path that spot does not."""

from __future__ import annotations

from decimal import Decimal

from trading.paper.enums import Product

D = Decimal


def test_a_perpetual_prices_at_intraday_not_delivery() -> None:
    """You never take delivery of a perpetual -- it has no expiry to
    deliver at. Its charge row is seeded under INTRADAY, so a backtest
    asking for DELIVERY finds nothing and refuses a run it could have
    priced perfectly well."""
    from trading.agent_contract.smoke import product_for_asset_class

    assert product_for_asset_class("PERP") is Product.INTRADAY
    assert product_for_asset_class("EQUITY") is Product.DELIVERY
    assert product_for_asset_class("CRYPTO") is Product.DELIVERY


def test_a_perpetual_has_a_broker() -> None:
    from trading.agent_contract.smoke import _BROKER_FOR_ASSET_CLASS

    assert _BROKER_FOR_ASSET_CLASS["PERP"] == "BINANCE"


def test_the_payload_tells_the_runtime_which_instruments_are_perpetual(db_conn) -> None:
    """The container has no database. Whether an instrument settles as a
    derivative decides how its fills move cash, so the answer has to
    travel with the payload -- a runtime that guessed would apply spot's
    notional to a perpetual and credit a short with money it never got."""
    from trading.runtime.payload import SmokePayload, decode_payload, encode_payload

    payload = SmokePayload(
        mode="smoke",
        source="x = 1\n",
        starting_cash=D("1000"),
        slippage_bps=D("0"),
        perp_instruments=(7, 9),
        leverage=D("5"),
    )
    decoded = decode_payload(encode_payload(payload))
    assert decoded.perp_instruments == (7, 9)
    assert decoded.leverage == D("5")


def test_an_ordinary_payload_names_no_perpetuals() -> None:
    from trading.runtime.payload import SmokePayload, decode_payload, encode_payload

    payload = SmokePayload(
        mode="smoke", source="x = 1\n", starting_cash=D("1"), slippage_bps=D("0")
    )
    assert decode_payload(encode_payload(payload)).perp_instruments == ()
