"""Signed position keeping for perpetuals.

Spot's `_apply_position` is long-only by construction: a sell can only
reduce, and `ck_no_negative_position` makes a short unrepresentable. A
perpetual is the opposite -- the sign carries the direction, and every
operation has to work in both.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.paper.perp import PerpPosition, apply_perp_fill, maintenance_margin, unrealised_pnl

D = Decimal


def _flat() -> PerpPosition:
    return PerpPosition(quantity=D("0"), entry_price=D("0"), leverage=D("10"))


def test_opening_a_long_sets_the_entry() -> None:
    after, realised = apply_perp_fill(_flat(), side="BUY", quantity=D("2"), price=D("100"))
    assert after.quantity == D("2")
    assert after.entry_price == D("100")
    assert realised == D("0")


def test_opening_a_short_gives_a_negative_quantity() -> None:
    """The sign is the direction. There is no separate 'side' column,
    because a position that is long and short at once is not a state this
    platform can be in (one-way mode)."""
    after, realised = apply_perp_fill(_flat(), side="SELL", quantity=D("3"), price=D("100"))
    assert after.quantity == D("-3")
    assert after.entry_price == D("100")
    assert realised == D("0")


def test_adding_to_a_position_averages_the_entry() -> None:
    first, _ = apply_perp_fill(_flat(), side="BUY", quantity=D("1"), price=D("100"))
    second, realised = apply_perp_fill(first, side="BUY", quantity=D("1"), price=D("120"))
    assert second.quantity == D("2")
    assert second.entry_price == D("110")
    # Adding realises nothing: no exposure was closed.
    assert realised == D("0")


def test_adding_to_a_short_averages_the_entry_too() -> None:
    first, _ = apply_perp_fill(_flat(), side="SELL", quantity=D("1"), price=D("100"))
    second, _ = apply_perp_fill(first, side="SELL", quantity=D("1"), price=D("80"))
    assert second.quantity == D("-2")
    assert second.entry_price == D("90")


def test_closing_a_long_realises_the_gain_and_leaves_the_entry_alone() -> None:
    opened, _ = apply_perp_fill(_flat(), side="BUY", quantity=D("2"), price=D("100"))
    after, realised = apply_perp_fill(opened, side="SELL", quantity=D("1"), price=D("130"))
    assert after.quantity == D("1")
    # The remaining unit is still held at its original cost. Re-averaging on
    # a partial close would silently rewrite the basis of what is still open.
    assert after.entry_price == D("100")
    assert realised == D("30")


def test_closing_a_short_realises_the_gain_when_price_fell() -> None:
    """The direction that spot cannot express at all: sold at 100, bought
    back at 70, made 30."""
    opened, _ = apply_perp_fill(_flat(), side="SELL", quantity=D("1"), price=D("100"))
    after, realised = apply_perp_fill(opened, side="BUY", quantity=D("1"), price=D("70"))
    assert after.quantity == D("0")
    assert realised == D("30")


def test_a_short_closed_higher_realises_a_loss() -> None:
    opened, _ = apply_perp_fill(_flat(), side="SELL", quantity=D("1"), price=D("100"))
    _after, realised = apply_perp_fill(opened, side="BUY", quantity=D("1"), price=D("140"))
    assert realised == D("-40")


def test_a_fill_that_crosses_through_flat_reverses_the_position() -> None:
    """Selling 3 against a long of 1 closes the long and opens a short of
    2. The realised part must cover only the 1 that was actually closed,
    and the new short's entry is the fill price -- not a blend of a long's
    basis with a short's."""
    opened, _ = apply_perp_fill(_flat(), side="BUY", quantity=D("1"), price=D("100"))
    after, realised = apply_perp_fill(opened, side="SELL", quantity=D("3"), price=D("120"))
    assert after.quantity == D("-2")
    assert after.entry_price == D("120")
    assert realised == D("20")


def test_unrealised_is_signed_by_direction() -> None:
    long_position = PerpPosition(quantity=D("2"), entry_price=D("100"), leverage=D("10"))
    short_position = PerpPosition(quantity=D("-2"), entry_price=D("100"), leverage=D("10"))
    assert unrealised_pnl(long_position, mark=D("110")) == D("20")
    assert unrealised_pnl(short_position, mark=D("110")) == D("-20")
    assert unrealised_pnl(short_position, mark=D("90")) == D("20")


def test_a_flat_position_has_no_unrealised_pnl() -> None:
    assert unrealised_pnl(_flat(), mark=D("999")) == D("0")


def test_maintenance_uses_the_tier_the_notional_falls_in() -> None:
    """Binance's real BTC ladder: 0.4% below 300k, 0.5% above with 300
    deducted. The deduction is what makes the two agree at the boundary."""
    tiers = [
        (D("0"), D("300000"), D("0.004"), D("0")),
        (D("300000"), D("800000"), D("0.005"), D("300")),
    ]
    # 1 BTC at 100,000 -- first tier.
    assert maintenance_margin(D("1"), mark=D("100000"), tiers=tiers) == D("400")
    # 5 BTC at 100,000 = 500,000 -- second tier, and the deduction applies.
    assert maintenance_margin(D("5"), mark=D("100000"), tiers=tiers) == D("2200")
    # A short of the same size requires the same margin: risk is symmetric.
    assert maintenance_margin(D("-5"), mark=D("100000"), tiers=tiers) == D("2200")


def test_a_notional_above_every_tier_is_refused() -> None:
    """Better to refuse than to silently apply the top tier's rate to a
    position the exchange would not have allowed at all."""
    tiers = [(D("0"), D("300000"), D("0.004"), D("0"))]
    with pytest.raises(LookupError, match="no maintenance tier"):
        maintenance_margin(D("100"), mark=D("100000"), tiers=tiers)


def test_equity_counts_a_perp_by_its_profit_not_its_notional() -> None:
    """The whole reason `compute_equity` needed a second term.

    Spot's `cash + quantity x mark` works because buying spot already moved
    cash by the full notional. Opening a perpetual moves no cash, so adding
    `quantity x mark` would credit the portfolio with the entire position
    value out of nowhere -- and for a short, would subtract it.
    """
    from trading.paper.breaker import compute_equity

    cash = D("100000")
    # Short 2 at 100, mark now 90: made 20.
    short = PerpPosition(quantity=D("-2"), entry_price=D("100"), leverage=D("10"))
    equity = compute_equity(cash, [], {}, perp_positions=[(1, short)], perp_marks={1: D("90")})
    assert equity == D("100020")

    # Same short, mark now 130: lost 60.
    equity = compute_equity(cash, [], {}, perp_positions=[(1, short)], perp_marks={1: D("130")})
    assert equity == D("99940")


def test_a_perp_without_a_mark_fails_loudly_like_a_spot_position() -> None:
    """Same reasoning as `MissingMark` for spot: valuing at zero understates
    equity and spuriously trips the breaker, valuing at entry hides a real
    loss. A perpetual makes this worse -- it is the position that can lose
    more than it cost."""
    from trading.paper.breaker import MissingMark, compute_equity

    short = PerpPosition(quantity=D("-2"), entry_price=D("100"), leverage=D("10"))
    with pytest.raises(MissingMark):
        compute_equity(D("1"), [], {}, perp_positions=[(7, short)], perp_marks={})


def test_a_flat_perp_needs_no_mark() -> None:
    from trading.paper.breaker import compute_equity

    flat = PerpPosition(quantity=D("0"), entry_price=D("0"), leverage=D("10"))
    assert compute_equity(D("500"), [], {}, perp_positions=[(1, flat)], perp_marks={}) == D("500")
