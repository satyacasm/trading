"""A perpetual inside the backtest runtime.

The runtime already keeps signed positions and handles a reversal through
zero, so the position arithmetic was never the gap. What was missing is
the money: a perpetual moves cash on realised P&L rather than notional,
accrues funding three times a day, and can be closed by the exchange.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trading.runtime.state import RunState

D = Decimal


def _flat_rates(instrument_id: int, rate: Decimal) -> dict[tuple[int, datetime], Decimal]:
    """The same rate at every boundary in the tested window. Real rates
    move -- `test_funding_in_loop` covers that -- but a test about *how
    many* boundaries a step crosses is clearer when the rate does not."""
    return {(instrument_id, datetime(2026, 9, 5, hour, tzinfo=UTC)): rate for hour in (8, 16)} | {
        (instrument_id, datetime(2026, 9, 6, 0, tzinfo=UTC)): rate
    }


def _position(instrument_id: int, quantity: str, avg_cost: str):
    from trading.paper.models import Position

    return Position(
        portfolio_id=1,
        instrument_id=instrument_id,
        quantity=D(quantity),
        avg_cost=D(avg_cost),
        realised_pnl=D("0"),
    )


def _state(cash: str = "100000", perps: set[int] | None = None) -> RunState:
    return RunState(
        now=datetime(2026, 9, 6, tzinfo=UTC),
        cash=D(cash),
        starting_cash=D(cash),
        perp_instruments=perps or set(),
    )


def test_a_spot_fill_still_moves_cash_by_notional() -> None:
    """The regression that matters most: nothing about spot changes."""
    from trading.runtime.loop import apply_fill_cash

    state = _state()
    apply_fill_cash(
        state,
        instrument_id=1,
        side="BUY",
        quantity=D("2"),
        price=D("100"),
        charges=D("1"),
        realised=D("0"),
    )
    assert state.cash == D("99799")


def test_opening_a_perpetual_moves_cash_only_by_charges() -> None:
    """Margin is reserved, not spent. Crediting a short with its notional
    would hand the run money it never received."""
    from trading.runtime.loop import apply_fill_cash

    state = _state(perps={7})
    apply_fill_cash(
        state,
        instrument_id=7,
        side="SELL",
        quantity=D("1"),
        price=D("80000"),
        charges=D("40"),
        realised=D("0"),
    )
    assert state.cash == D("99960")


def test_closing_a_perpetual_moves_cash_by_what_was_realised() -> None:
    from trading.runtime.loop import apply_fill_cash

    state = _state(perps={7})
    apply_fill_cash(
        state,
        instrument_id=7,
        side="BUY",
        quantity=D("1"),
        price=D("79000"),
        charges=D("40"),
        realised=D("1000"),
    )
    assert state.cash == D("100960")


def test_equity_values_a_perpetual_by_its_profit_not_its_notional() -> None:
    """Spot's `quantity x mark` assumes cash already moved by the notional.
    A perpetual's did not, so only the change since entry belongs in
    equity -- and for a short, the spot formula would subtract the whole
    position value from a portfolio that is up."""
    from trading.paper.models import Position
    from trading.runtime.context import LivePortfolioView

    state = _state(perps={7})
    state.positions[7] = Position(
        portfolio_id=1,
        instrument_id=7,
        quantity=D("-1"),
        avg_cost=D("80000"),
        realised_pnl=D("0"),
    )
    state.marks[7] = D("79000")

    # Short one at 80,000, mark 79,000: up 1,000 on a 100,000 book.
    assert LivePortfolioView(state).equity == D("101000")


def test_a_short_that_moved_against_the_run_shows_a_loss() -> None:
    from trading.paper.models import Position
    from trading.runtime.context import LivePortfolioView

    state = _state(perps={7})
    state.positions[7] = Position(
        portfolio_id=1,
        instrument_id=7,
        quantity=D("-1"),
        avg_cost=D("80000"),
        realised_pnl=D("0"),
    )
    state.marks[7] = D("84000")
    assert LivePortfolioView(state).equity == D("96000")


def test_spot_equity_is_unchanged_by_any_of_this() -> None:
    from trading.paper.models import Position
    from trading.runtime.context import LivePortfolioView

    state = _state()
    state.positions[1] = Position(
        portfolio_id=1,
        instrument_id=1,
        quantity=D("2"),
        avg_cost=D("50"),
        realised_pnl=D("0"),
    )
    state.marks[1] = D("60")
    assert LivePortfolioView(state).equity == D("100120")


def test_funding_settles_at_the_boundaries_a_bar_crossed() -> None:
    """A backtest steps bar to bar, so it must settle every boundary the
    step passed over -- a daily bar crosses three. Settling once per bar
    would under-charge carry by two thirds on daily data and never charge
    it at all on a bar shorter than eight hours."""
    from trading.runtime.loop import settle_funding_for_step

    state = _state(perps={7})
    state.positions[7] = _position(7, "-1", "80000")
    state.marks[7] = D("80000")

    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, tzinfo=UTC),
        now=datetime(2026, 9, 6, tzinfo=UTC),
        rates=_flat_rates(7, D("0.0001")),
    )
    # Three boundaries crossed (08:00, 16:00, 00:00), 8 received each time.
    assert state.cash == D("100024")
    assert state.funding_paid[7] == D("-24")


def test_a_long_pays_across_the_same_step() -> None:
    from trading.runtime.loop import settle_funding_for_step

    state = _state(perps={7})
    state.positions[7] = _position(7, "1", "80000")
    state.marks[7] = D("80000")
    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, tzinfo=UTC),
        now=datetime(2026, 9, 6, tzinfo=UTC),
        rates=_flat_rates(7, D("0.0001")),
    )
    assert state.cash == D("99976")


def test_a_step_inside_one_interval_settles_nothing() -> None:
    from trading.runtime.loop import settle_funding_for_step

    state = _state(perps={7})
    state.positions[7] = _position(7, "1", "80000")
    state.marks[7] = D("80000")
    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, 9, tzinfo=UTC),
        now=datetime(2026, 9, 5, 15, tzinfo=UTC),
        rates=_flat_rates(7, D("0.0001")),
    )
    assert state.cash == D("100000")


def test_a_spot_position_never_accrues_funding() -> None:
    from trading.runtime.loop import settle_funding_for_step

    state = _state()
    state.positions[1] = _position(1, "10", "100")
    state.marks[1] = D("100")
    settle_funding_for_step(
        state,
        previous=datetime(2026, 9, 5, tzinfo=UTC),
        now=datetime(2026, 9, 6, tzinfo=UTC),
        rates=_flat_rates(1, D("0.0001")),
    )
    assert state.cash == D("100000")
