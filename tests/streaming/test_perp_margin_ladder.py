"""Binance's real maintenance-margin ladder, checked against itself.

Run against a captured live payload rather than a hand-written fixture:
the property worth testing is that what Binance actually sends survives
our parse, and a fixture invented to match the parser would prove nothing.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from trading.sources.binance_margin_tiers import parse_margin_tiers

_PAYLOAD = Path(__file__).parent.parent / "fixtures" / "binance" / "leverage_bracket_btcusdt.json"


def _ladder():
    return sorted(parse_margin_tiers(_PAYLOAD.read_bytes()), key=lambda t: t.notional_floor)


def test_the_maintenance_ladder_is_continuous_at_every_boundary() -> None:
    """`maintenance_amount` (Binance's `cum`) exists to make the tiered
    formula continuous:

        maintenance = notional x rate - amount

    At each boundary the tier below and the tier above must agree, or a
    position sitting exactly on the line has two different maintenance
    requirements depending on which row you read.

    `cum` is the easy field to overlook when parsing this endpoint, and
    dropping it breaks continuity at every boundary above the first *and*
    overstates the requirement everywhere above it -- liquidating positions
    that were never near the line. This test fails if it is ever dropped.
    """
    ladder = _ladder()
    assert len(ladder) >= 6

    for below, above in zip(ladder, ladder[1:], strict=False):
        assert below.notional_cap == above.notional_floor, f"gap at {below.notional_cap}"
        at_boundary = below.notional_cap
        assert (
            at_boundary * below.maintenance_rate - below.maintenance_amount
            == at_boundary * above.maintenance_rate - above.maintenance_amount
        ), f"discontinuous at {at_boundary}"


def test_the_first_tier_starts_at_zero_with_no_deduction() -> None:
    """Nothing is deducted in the first tier, so a small position's
    requirement is simply notional x rate."""
    first = _ladder()[0]
    assert first.notional_floor == Decimal("0")
    assert first.maintenance_amount == Decimal("0")


def test_size_costs_leverage() -> None:
    """The whole point of tiers. A ladder that did not tighten would let a
    position of any size run at maximum leverage."""
    ladder = _ladder()
    assert [t.maintenance_rate for t in ladder] == sorted(t.maintenance_rate for t in ladder)
    assert [t.max_leverage for t in ladder] == sorted(
        (t.max_leverage for t in ladder), reverse=True
    )
