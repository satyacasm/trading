"""When the exchange closes a position, and at what price.

Every number here is checked against the definition rather than against a
remembered constant: at the liquidation price, the position's remaining
equity must equal its maintenance requirement exactly. A formula that
merely looks right fails that.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.paper.liquidation import (
    bankruptcy_price,
    liquidation_price,
    position_equity,
    should_liquidate,
)
from trading.paper.perp import PerpPosition, maintenance_margin, unrealised_pnl

D = Decimal

# Binance's real first BTC tier: 0.4% below 300,000 notional, nothing deducted.
TIERS = [
    (D("0"), D("300000"), D("0.004"), D("0")),
    (D("300000"), D("800000"), D("0.005"), D("300")),
]


def _long(qty: str = "1", entry: str = "80000", leverage: str = "10") -> PerpPosition:
    return PerpPosition(D(qty), D(entry), D(leverage))


def _short(qty: str = "-1", entry: str = "80000", leverage: str = "10") -> PerpPosition:
    return PerpPosition(D(qty), D(entry), D(leverage))


def test_a_long_liquidates_below_its_entry() -> None:
    """10x on a long: roughly a 10% adverse move exhausts the margin, less
    the maintenance buffer the exchange keeps back."""
    price = liquidation_price(_long(), margin=D("8000"), tiers=TIERS)
    assert D("72000") < price < D("72500")


def test_at_the_liquidation_price_equity_equals_maintenance_exactly() -> None:
    """The definition, restated as a test. This is what makes the formula
    checkable without trusting a remembered number: liquidation is the
    price at which what is left of the margin is precisely the requirement,
    so the two sides must meet, not merely come close."""
    for position, margin in ((_long(), D("8000")), (_short(), D("8000"))):
        price = liquidation_price(position, margin=margin, tiers=TIERS)
        left = margin + unrealised_pnl(position, mark=price)
        required = maintenance_margin(position.quantity, mark=price, tiers=TIERS)
        assert abs(left - required) < D("0.01"), (position.quantity, left, required)


def test_a_short_liquidates_above_its_entry() -> None:
    """The direction spot cannot express. A short is closed by a rally."""
    price = liquidation_price(_short(), margin=D("8000"), tiers=TIERS)
    assert price > D("80000")
    assert D("87500") < price < D("87800")


def test_more_leverage_liquidates_sooner() -> None:
    """The point of the whole mechanism, and the thing a trader most needs
    to see before choosing a number."""
    at_2x = liquidation_price(_long(leverage="2"), margin=D("40000"), tiers=TIERS)
    at_20x = liquidation_price(_long(leverage="20"), margin=D("4000"), tiers=TIERS)
    assert at_2x < at_20x
    assert at_20x > D("76000")


def test_a_flat_position_has_no_liquidation_price() -> None:
    flat = PerpPosition(D("0"), D("0"), D("10"))
    assert liquidation_price(flat, margin=D("0"), tiers=TIERS) is None


def test_position_equity_is_the_margin_plus_what_the_move_did() -> None:
    long_position = _long()
    assert position_equity(long_position, margin=D("8000"), mark=D("80000")) == D("8000")
    assert position_equity(long_position, margin=D("8000"), mark=D("79000")) == D("7000")
    # A short gains on the way down.
    assert position_equity(_short(), margin=D("8000"), mark=D("79000")) == D("9000")


def test_it_liquidates_once_equity_falls_below_maintenance() -> None:
    position = _long()
    # Comfortable.
    assert should_liquidate(position, margin=D("8000"), mark=D("79000"), tiers=TIERS) is False
    # Past the line.
    assert should_liquidate(position, margin=D("8000"), mark=D("71000"), tiers=TIERS) is True


def test_the_boundary_itself_does_not_liquidate() -> None:
    """Strictly below, matching the circuit breaker's own convention: a
    position landing exactly on its requirement is still adequately
    margined."""
    position = _long()
    price = liquidation_price(position, margin=D("8000"), tiers=TIERS)
    assert should_liquidate(position, margin=D("8000"), mark=price, tiers=TIERS) is False


def test_a_move_in_your_favour_never_liquidates() -> None:
    assert should_liquidate(_long(), margin=D("8000"), mark=D("200000"), tiers=TIERS) is False
    assert should_liquidate(_short(), margin=D("8000"), mark=D("1"), tiers=TIERS) is False


def test_a_notional_beyond_every_tier_refuses_rather_than_guessing() -> None:
    with pytest.raises(LookupError):
        should_liquidate(_long(qty="1000"), margin=D("8000"), mark=D("80000"), tiers=TIERS)


def test_bankruptcy_is_where_the_collateral_is_exactly_gone() -> None:
    """Past the liquidation price sits the bankruptcy price, where the
    position has consumed all of its margin and none of the maintenance
    buffer is left. A real exchange closes between the two and its
    insurance fund covers any gap; this platform has no fund, so it has to
    say what the gap was rather than absorb it silently."""
    # Long 1 at 80,000 with 8,000 posted: gone at 72,000.
    assert bankruptcy_price(_long(), margin=D("8000")) == D("72000")
    # Short 1 at 80,000 with 8,000 posted: gone at 88,000.
    assert bankruptcy_price(_short(), margin=D("8000")) == D("88000")


def test_bankruptcy_sits_beyond_liquidation_on_both_sides() -> None:
    """The buffer between them is what the maintenance requirement is for.
    If liquidation ever fell outside it, the exchange would be closing
    positions after they were already unrecoverable."""
    for position in (_long(), _short()):
        liq = liquidation_price(position, margin=D("8000"), tiers=TIERS)
        bankrupt = bankruptcy_price(position, margin=D("8000"))
        if position.quantity > 0:
            assert bankrupt < liq
        else:
            assert bankrupt > liq
