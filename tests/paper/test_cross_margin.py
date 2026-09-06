"""What backs a losing position: its own margin, or the whole account."""

from __future__ import annotations

from decimal import Decimal

from trading.paper.liquidation import cross_margin_breach, position_equity, should_liquidate
from trading.paper.perp import PerpPosition

D = Decimal
TIERS = ((D("0"), D("300000"), D("0.004"), D("0")),)


def _long(qty: str = "1", entry: str = "80000") -> PerpPosition:
    return PerpPosition(D(qty), D(entry), D("10"))


def test_isolated_liquidates_on_the_positions_own_margin() -> None:
    """The account's other money is not available to it. 8,000 posted
    against a 1 BTC long is all that stands behind it."""
    assert should_liquidate(_long(), margin=D("8000"), mark=D("71000"), tiers=TIERS) is True


def test_cross_survives_where_isolated_would_not() -> None:
    """The whole point of cross: the same position, the same price, and it
    lives because the account's free equity is standing behind it."""
    breached = cross_margin_breach(
        account_equity=D("50000"),
        positions=[(_long(), D("71000"), TIERS)],
    )
    assert breached is False
    # Isolated, at the same mark, is already gone.
    assert should_liquidate(_long(), margin=D("8000"), mark=D("71000"), tiers=TIERS) is True


def test_cross_liquidates_once_the_account_cannot_cover_maintenance() -> None:
    """And when it does go, it goes against everything -- which is the
    price of the extra room."""
    assert (
        cross_margin_breach(
            account_equity=D("100"),
            positions=[(_long(), D("71000"), TIERS)],
        )
        is True
    )


def test_cross_sums_maintenance_across_every_position() -> None:
    """One position's requirement is not the test. An account can meet each
    position's maintenance separately and still be unable to meet them
    together, which is exactly the case a per-position check misses."""
    two = [
        (_long(qty="1"), D("80000"), TIERS),
        (_long(qty="2"), D("80000"), TIERS),
    ]
    # 3 BTC at 80,000 x 0.4% = 960 required in total.
    assert cross_margin_breach(account_equity=D("1000"), positions=two) is False
    assert cross_margin_breach(account_equity=D("900"), positions=two) is True


def test_a_flat_position_requires_nothing_in_cross_either() -> None:
    flat = PerpPosition(D("0"), D("0"), D("10"))
    assert cross_margin_breach(account_equity=D("0"), positions=[(flat, D("1"), TIERS)]) is False


def test_position_equity_is_unchanged_by_any_of_this() -> None:
    assert position_equity(_long(), margin=D("8000"), mark=D("79000")) == D("7000")
