"""Liquidation inside a backtest.

The arithmetic is `paper.liquidation`'s and already tested. What is tested
here is that a run actually applies it: a backtest where an over-levered
position survives a move that would have ended it is the most flattering
possible lie about leverage.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trading.paper.models import Position
from trading.runtime.state import RunState

D = Decimal

# Binance's real first BTC tier.
TIERS = {7: ((D("0"), D("300000"), D("0.004"), D("0")),)}


def _state(quantity: str = "1", leverage: str = "10") -> RunState:
    state = RunState(
        now=datetime(2026, 9, 6, tzinfo=UTC),
        cash=D("100000"),
        starting_cash=D("100000"),
        perp_instruments={7},
    )
    state.positions[7] = Position(
        portfolio_id=1,
        instrument_id=7,
        quantity=D(quantity),
        avg_cost=D("80000"),
        realised_pnl=D("0"),
    )
    state.reserved_margin[7] = abs(D(quantity)) * D("80000") / D(leverage)
    return state


def test_a_healthy_position_is_left_alone() -> None:
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    assert liquidate_for_step(state, mark_by_instrument={7: D("79000")}, tiers=TIERS) == []
    assert state.positions[7].quantity == D("1")


def test_an_over_levered_long_is_closed_at_the_mark() -> None:
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    closed = liquidate_for_step(state, mark_by_instrument={7: D("72100")}, tiers=TIERS)

    assert len(closed) == 1
    assert state.positions[7].quantity == D("0")
    # Margin released with the position. Margin that survives a close is
    # margin no position explains, and it accumulates until the run cannot
    # open anything.
    assert state.reserved_margin.get(7, D("0")) == D("0")


def test_the_loss_reaches_cash() -> None:
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    liquidate_for_step(state, mark_by_instrument={7: D("72100")}, tiers=TIERS)
    # Bought at 80,000, closed at 72,100: 7,900 gone, plus the fee.
    assert state.cash < D("92200")
    assert state.cash > D("91000")


def test_a_short_is_liquidated_by_a_rally() -> None:
    from trading.runtime.loop import liquidate_for_step

    state = _state(quantity="-1")
    closed = liquidate_for_step(state, mark_by_instrument={7: D("87800")}, tiers=TIERS)
    assert len(closed) == 1
    assert state.positions[7].quantity == D("0")


def test_the_run_records_what_happened_and_why() -> None:
    """A position that simply vanished from the report is unexplainable
    months later. The record names the mark and the requirement it fell
    below -- the two numbers that decided it."""
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    liquidate_for_step(state, mark_by_instrument={7: D("72100")}, tiers=TIERS)

    assert len(state.liquidations) == 1
    event = state.liquidations[0]
    assert event["instrument_id"] == "7"
    assert event["mark"] == "72100"
    assert "maintenance" in event
    assert Decimal(event["equity"]) < Decimal(event["maintenance"])


def test_a_spot_position_is_never_liquidated() -> None:
    """Spot is fully paid for. There is no margin to run out of."""
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    state.perp_instruments = set()
    assert liquidate_for_step(state, mark_by_instrument={7: D("1")}, tiers=TIERS) == []


def test_an_instrument_without_tiers_is_never_liquidated() -> None:
    """No tiers means no maintenance requirement to breach. Guessing one
    would close a position on a number nobody published."""
    from trading.runtime.loop import liquidate_for_step

    state = _state()
    assert liquidate_for_step(state, mark_by_instrument={7: D("1")}, tiers={}) == []
