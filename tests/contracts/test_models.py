from datetime import date
from decimal import Decimal

import pytest

from trading.contracts import InstrumentRef, OptionType


def test_canonical_key_for_equity_omits_derivative_parts():
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")
    assert ref.canonical_key == "NSE:CM:RELIANCE"


def test_canonical_key_for_option_includes_all_parts():
    ref = InstrumentRef(
        exchange="NSE",
        segment="FO",
        symbol="NIFTY",
        expiry=date(2026, 8, 27),
        strike=Decimal("24500.00"),
        option_type=OptionType.CE,
    )
    assert ref.canonical_key == "NSE:FO:NIFTY:2026-08-27:24500:CE"


def test_strike_scale_does_not_change_the_key():
    """24500, 24500.0 and 24500.0000 are the same contract; one key."""
    keys = {
        InstrumentRef(
            exchange="NSE",
            segment="FO",
            symbol="NIFTY",
            expiry=date(2026, 8, 27),
            strike=Decimal(s),
            option_type=OptionType.CE,
        ).canonical_key
        for s in ("24500", "24500.0", "24500.0000")
    }
    assert keys == {"NSE:FO:NIFTY:2026-08-27:24500:CE"}


def test_fractional_strike_is_preserved():
    ref = InstrumentRef(
        exchange="NSE",
        segment="FO",
        symbol="BANKNIFTY",
        expiry=date(2026, 8, 27),
        strike=Decimal("52350.50"),
        option_type=OptionType.PE,
    )
    assert ref.canonical_key == "NSE:FO:BANKNIFTY:2026-08-27:52350.5:PE"


def test_ref_is_hashable_so_it_can_be_deduplicated():
    a = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    b = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    assert len({a, b}) == 1


def test_ref_is_immutable():
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    with pytest.raises(Exception):  # noqa: B017 -- frozen-model error type is pydantic's to choose
        ref.symbol = "INFY"  # type: ignore[misc]


def test_empty_canonical_frame_satisfies_its_own_contract():
    from trading.contracts import assert_canonical, empty_canonical_frame

    assert_canonical(empty_canonical_frame())  # must not raise


def test_assert_canonical_rejects_a_missing_column():
    from trading.contracts import assert_canonical, empty_canonical_frame

    frame = empty_canonical_frame().drop("close")
    with pytest.raises(ValueError, match="missing canonical columns"):
        assert_canonical(frame)


# --- Ruling S1 (task-18-brief.md): series joins the natural key ---


def test_canonical_key_without_series_is_unchanged():
    """Existing keys for instruments WITHOUT a series must not churn."""
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE", series=None)
    assert ref.canonical_key == "NSE:CM:RELIANCE"


def test_canonical_key_with_series_includes_it_after_symbol():
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="DHFL", series="N2")
    assert ref.canonical_key == "NSE:CM:DHFL:N2"


def test_canonical_key_distinguishes_series_for_the_same_symbol():
    """The DHFL case: same symbol, different series, must be different keys."""
    keys = {
        InstrumentRef(exchange="NSE", segment="CM", symbol="DHFL", series=s).canonical_key
        for s in ("EQ", "N2", "N4")
    }
    assert keys == {"NSE:CM:DHFL:EQ", "NSE:CM:DHFL:N2", "NSE:CM:DHFL:N4"}


def test_canonical_key_for_option_with_series_orders_series_before_expiry():
    ref = InstrumentRef(
        exchange="NSE",
        segment="FO",
        symbol="NIFTY",
        series="OP",
        expiry=date(2026, 8, 27),
        strike=Decimal("24500"),
        option_type=OptionType.CE,
    )
    assert ref.canonical_key == "NSE:FO:NIFTY:OP:2026-08-27:24500:CE"
