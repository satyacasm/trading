"""Funding applied inside a run, at each boundary's own rate."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trading.paper.models import Position
from trading.runtime.state import RunState

D = Decimal


def _state() -> RunState:
    state = RunState(
        now=datetime(2026, 9, 6, tzinfo=UTC),
        cash=D("100000"),
        starting_cash=D("100000"),
        perp_instruments={7},
    )
    state.positions[7] = Position(
        portfolio_id=1,
        instrument_id=7,
        quantity=D("-1"),
        avg_cost=D("80000"),
        realised_pnl=D("0"),
    )
    state.marks[7] = D("80000")
    return state


def test_each_boundary_settles_at_its_own_rate() -> None:
    """Rates move -- BTC's went negative at 23% of settlements over the
    last year. Applying one average rate to a step that crossed three
    boundaries would smooth away exactly the variation a carry strategy
    trades."""
    from trading.runtime.loop import settle_funding_for_step

    state = _state()
    rates = {
        (7, datetime(2026, 9, 5, 8, tzinfo=UTC)): D("0.0001"),
        (7, datetime(2026, 9, 5, 16, tzinfo=UTC)): D("0.0002"),
        (7, datetime(2026, 9, 6, 0, tzinfo=UTC)): D("-0.00005"),
    }
    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, tzinfo=UTC),
        now=datetime(2026, 9, 6, tzinfo=UTC),
        rates=rates,
    )
    # Short: receives 8 and 16 at the positive rates, pays 4 at the
    # negative one. Net +20.
    assert state.cash == D("100020")
    assert state.funding_paid[7] == D("-20")


def test_a_boundary_with_no_published_rate_is_skipped() -> None:
    """The series has gaps -- Binance's earliest rows carry no mark, and a
    contract listed mid-history has no settlements before it existed.
    Settling those at zero would be indistinguishable from a genuine
    zero-rate settlement."""
    from trading.runtime.loop import settle_funding_for_step

    state = _state()
    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, tzinfo=UTC),
        now=datetime(2026, 9, 6, tzinfo=UTC),
        rates={(7, datetime(2026, 9, 5, 8, tzinfo=UTC)): D("0.0001")},
    )
    assert state.cash == D("100008")
