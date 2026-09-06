"""Leverage as a manifest declaration."""

from __future__ import annotations

from decimal import Decimal

import pytest

D = Decimal


def _manifest(**over: object):
    from trading.agent_contract.platform_sdk import (
        DataRequest,
        InstrumentRef,
        StrategyManifest,
    )

    kwargs: dict[str, object] = {
        "name": "shorty",
        "version": "1.0.0",
        "universe": [InstrumentRef(exchange="BINANCE_FUTURES", segment="PERP", symbol="BTC-USDT")],
        "data": DataRequest(bars="1m", history_bars=20),
        "capital": D("100000"),
        "base_currency": "USDT",
    }
    kwargs.update(over)
    return StrategyManifest(**kwargs)  # type: ignore[arg-type]


def test_leverage_is_declared_once_for_the_strategy() -> None:
    """One strategy, one portfolio, one currency, one asset class (D6) --
    so one leverage. A per-instrument mapping would be more precise about
    a thing this platform will not let a strategy do anyway."""
    assert _manifest(leverage=D("10")).leverage == D("10")


def test_leverage_is_absent_by_default() -> None:
    """None means "this strategy does not trade anything levered", which
    is true of every strategy written before perpetuals existed. Defaulting
    it to 1 would silently make every one of them a perpetual trader."""
    assert _manifest().leverage is None


def test_leverage_must_be_a_decimal_like_every_other_number_here() -> None:
    with pytest.raises(TypeError):
        _manifest(leverage=10.0)


def test_leverage_must_be_positive() -> None:
    with pytest.raises(ValueError, match="leverage"):
        _manifest(leverage=D("0"))
    with pytest.raises(ValueError, match="leverage"):
        _manifest(leverage=D("-2"))


def test_it_survives_the_payload_round_trip() -> None:
    """The manifest crosses a process boundary into the sandbox. A field
    the codec drops is a field the strategy declared and the platform
    never saw -- which is how `knowable_at` was lost once already."""
    from trading.runtime.payload import SmokePayload, decode_payload, encode_payload

    payload = SmokePayload(
        mode="smoke",
        source="x = 1\n",
        starting_cash=D("100000"),
        slippage_bps=D("0"),
        leverage=D("20"),
    )
    assert decode_payload(encode_payload(payload)).leverage == D("20")


def test_an_absent_leverage_survives_the_round_trip_as_none() -> None:
    from trading.runtime.payload import SmokePayload, decode_payload, encode_payload

    payload = SmokePayload(
        mode="smoke", source="x = 1\n", starting_cash=D("1"), slippage_bps=D("0")
    )
    assert decode_payload(encode_payload(payload)).leverage is None
