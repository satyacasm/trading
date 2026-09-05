"""Parsing Binance's maintenance-margin brackets."""

from __future__ import annotations

import json
from decimal import Decimal

from trading.sources.binance_margin_tiers import parse_margin_tiers, signed_query

_PAYLOAD = [
    {
        "symbol": "BTCUSDT",
        "brackets": [
            {
                "bracket": 1,
                "initialLeverage": 125,
                "notionalCap": 50000,
                "notionalFloor": 0,
                "maintMarginRatio": 0.004,
                "cum": 0.0,
            },
            {
                "bracket": 2,
                "initialLeverage": 100,
                "notionalCap": 600000,
                "notionalFloor": 50000,
                "maintMarginRatio": 0.005,
                "cum": 50.0,
            },
        ],
    }
]


def test_brackets_become_tiers_with_every_field_liquidation_needs() -> None:
    tiers = parse_margin_tiers(json.dumps(_PAYLOAD).encode())
    assert [t.symbol for t in tiers] == ["BTCUSDT", "BTCUSDT"]
    first, second = tiers
    assert first.notional_floor == Decimal("0")
    assert first.notional_cap == Decimal("50000")
    assert first.max_leverage == Decimal("125")
    # Decimal, not float: this multiplies a notional to decide whether a
    # position is liquidated, and 0.004 is not representable in binary.
    assert first.maintenance_rate == Decimal("0.004")
    assert isinstance(first.maintenance_rate, Decimal)
    # `cum` is the maintenance amount deducted at this tier -- without it
    # the tiered formula overstates the requirement at every tier above the
    # first, liquidating positions that were never near the line.
    assert second.maintenance_amount == Decimal("50")


def test_the_signature_covers_the_whole_query_string() -> None:
    """A signature over anything less than the full query lets a
    man-in-the-middle alter the unsigned part. Binance rejects it anyway;
    getting it wrong locally just produces an opaque -1022."""
    query = signed_query({"timestamp": 1788585993001}, secret="NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP")
    assert query.startswith("timestamp=1788585993001&signature=")
    # HMAC-SHA256 is 64 hex characters.
    assert len(query.split("signature=")[1]) == 64


def test_the_signature_is_stable_for_the_same_input() -> None:
    args = ({"timestamp": 1, "symbol": "BTCUSDT"}, "secret")
    assert signed_query(args[0], secret=args[1]) == signed_query(args[0], secret=args[1])
