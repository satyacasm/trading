"""Tests for `src/trading/paper/breaker.py`: the portfolio circuit breaker.

Pure-function tests (`compute_equity`, `evaluate_breach`) need no database
and carry no marker. Everything that touches `portfolio_equity_snapshots`,
`portfolios`, `orders`, or `circuit_breaker_events` is marked
`@pytest.mark.db` individually, matching `tests/paper/test_charges.py`'s
per-test marking rather than a whole-module marker, since this file mixes
both kinds of test.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.paper.helpers import make_order, make_portfolio
from trading.paper.breaker import (
    REASON_MAX_DAILY_LOSS,
    REASON_MAX_DRAWDOWN,
    MissingMark,
    compute_equity,
    evaluate_breach,
    load_day_open_equity,
    load_peak_equity,
    record_snapshot,
    trip,
)
from trading.paper.enums import OrderStatus, Side
from trading.paper.models import Position

_T0 = datetime(2026, 8, 31, 6, 0, 0, tzinfo=UTC)


def _position(*, instrument_id: int = 1, quantity: str, avg_cost: str = "100") -> Position:
    return Position(
        portfolio_id=1,
        instrument_id=instrument_id,
        quantity=Decimal(quantity),
        avg_cost=Decimal(avg_cost),
        realised_pnl=Decimal("0"),
    )


# --- compute_equity -----------------------------------------------------------


def test_compute_equity_is_cash_plus_marked_positions() -> None:
    positions = [
        _position(instrument_id=1, quantity="10"),
        _position(instrument_id=2, quantity="5"),
    ]
    marks = {1: Decimal("100"), 2: Decimal("50")}
    equity = compute_equity(Decimal("1000"), positions, marks)
    assert equity == Decimal("1000") + Decimal("1000") + Decimal("250")


def test_compute_equity_with_no_positions_is_just_cash() -> None:
    assert compute_equity(Decimal("5000"), [], {}) == Decimal("5000")


def test_compute_equity_ignores_a_flat_position_without_needing_a_mark() -> None:
    """A position quantized down to zero (fully closed but still a row in
    `positions`) contributes nothing and must not require a mark -- only a
    held position can misprice equity."""
    positions = [_position(instrument_id=1, quantity="0")]
    assert compute_equity(Decimal("1000"), positions, {}) == Decimal("1000")


def test_compute_equity_raises_on_missing_mark_for_a_held_position() -> None:
    """The load-bearing case: a nonzero position with no mark must raise,
    never silently be valued at zero (understates equity) or at cost
    (hides a real loss)."""
    positions = [_position(instrument_id=1, quantity="10")]
    with pytest.raises(MissingMark, match="instrument_id=1"):
        compute_equity(Decimal("1000"), positions, {})


# --- evaluate_breach ------------------------------------------------------------


def test_evaluate_breach_none_when_loss_is_within_max_daily_loss() -> None:
    reason = evaluate_breach(
        equity=Decimal("9500"),
        day_open_equity=Decimal("10000"),
        peak_equity=Decimal("10000"),
        max_daily_loss=Decimal("1000"),
        max_drawdown_pct=None,
    )
    assert reason is None


def test_evaluate_breach_reports_max_daily_loss_when_loss_exceeds_the_limit() -> None:
    reason = evaluate_breach(
        equity=Decimal("8500"),
        day_open_equity=Decimal("10000"),
        peak_equity=Decimal("10000"),
        max_daily_loss=Decimal("1000"),
        max_drawdown_pct=None,
    )
    assert reason is not None
    assert reason.startswith(REASON_MAX_DAILY_LOSS)


def test_evaluate_breach_reports_max_drawdown_when_drawdown_exceeds_the_limit() -> None:
    # Peak was 10000, equity fell to 9000 -- a 10% drawdown from the peak,
    # independent of day_open_equity (set here so daily loss alone would
    # not have breached, isolating the drawdown check).
    reason = evaluate_breach(
        equity=Decimal("9000"),
        day_open_equity=Decimal("9500"),
        peak_equity=Decimal("10000"),
        max_daily_loss=None,
        max_drawdown_pct=Decimal("5"),
    )
    assert reason is not None
    assert reason.startswith(REASON_MAX_DRAWDOWN)


def test_evaluate_breach_none_when_drawdown_is_within_the_limit() -> None:
    reason = evaluate_breach(
        equity=Decimal("9700"),
        day_open_equity=Decimal("9700"),
        peak_equity=Decimal("10000"),
        max_daily_loss=None,
        max_drawdown_pct=Decimal("5"),
    )
    assert reason is None


def test_evaluate_breach_none_when_both_limits_are_none() -> None:
    """A portfolio with no declared limits can never breach, however far
    equity has fallen -- the absence of a limit is not the same as an
    implicit limit of zero."""
    reason = evaluate_breach(
        equity=Decimal("1"),
        day_open_equity=Decimal("1000000"),
        peak_equity=Decimal("1000000"),
        max_daily_loss=None,
        max_drawdown_pct=None,
    )
    assert reason is None


def test_evaluate_breach_ignores_a_daily_loss_that_would_breach_when_the_limit_is_none() -> None:
    """`max_daily_loss=None` must not fall back to some default limit --
    a huge loss must still not breach when the portfolio declared no
    daily-loss limit at all, even while a drawdown limit is set and
    passes."""
    reason = evaluate_breach(
        equity=Decimal("1"),
        day_open_equity=Decimal("1000000"),
        peak_equity=Decimal("1"),  # equity == peak -> 0% drawdown
        max_daily_loss=None,
        max_drawdown_pct=Decimal("50"),
    )
    assert reason is None


def test_evaluate_breach_checks_daily_loss_before_drawdown() -> None:
    """When both would independently breach, the daily-loss reason wins --
    documented behaviour, not incidental: `trading.paper.engine` picks the
    threshold to hand `trip` from the reason's prefix, so the ordering
    must be deterministic."""
    reason = evaluate_breach(
        equity=Decimal("5000"),
        day_open_equity=Decimal("10000"),  # 5000 loss, breaches a 1000 limit
        peak_equity=Decimal("10000"),  # also a 50% drawdown, breaches a 5% limit
        max_daily_loss=Decimal("1000"),
        max_drawdown_pct=Decimal("5"),
    )
    assert reason is not None
    assert reason.startswith(REASON_MAX_DAILY_LOSS)


# --- evaluate_breach: the exact threshold boundary ------------------------------
#
# `evaluate_breach` compares with strict `>`, so a loss or drawdown landing
# *exactly* on its declared limit does not breach; it takes one more paisa (or
# one more basis point) to trip. These four tests lock that in from both sides.
# They are characterization tests -- they record the behaviour the code has, not
# a policy decision made here -- and each "exactly at" case is paired with a
# "one increment past" case so the pair cannot be satisfied by a breaker that
# simply never trips. Verified non-vacuous by flipping `>` to `>=` in
# `evaluate_breach` and confirming both "does_not_trip" tests fail.


def test_loss_exactly_at_max_daily_loss_does_not_trip() -> None:
    """A loss landing precisely on the limit is within it, not past it."""
    reason = evaluate_breach(
        equity=Decimal("9000"),
        day_open_equity=Decimal("10000"),  # loss of exactly 1000
        peak_equity=Decimal("10000"),
        max_daily_loss=Decimal("1000"),
        max_drawdown_pct=None,
    )
    assert reason is None


def test_loss_one_paisa_past_max_daily_loss_does_trip() -> None:
    """The companion to the boundary test above: one paisa further and it
    breaches, which is what proves the boundary is where it is rather than
    the breaker being inert."""
    reason = evaluate_breach(
        equity=Decimal("8999.99"),
        day_open_equity=Decimal("10000"),  # loss of 1000.01
        peak_equity=Decimal("10000"),
        max_daily_loss=Decimal("1000"),
        max_drawdown_pct=None,
    )
    assert reason is not None
    assert reason.startswith(REASON_MAX_DAILY_LOSS)


def test_drawdown_exactly_at_max_drawdown_pct_does_not_trip() -> None:
    """Same boundary, the other limit: a drawdown of exactly 5.0000% against
    a 5% limit is within it. Chosen so the division is exact in `Decimal`
    ((10000 - 9500) / 10000 * 100 == 5), leaving nothing for a rounding
    artefact to hide behind."""
    reason = evaluate_breach(
        equity=Decimal("9500"),
        day_open_equity=Decimal("9500"),
        peak_equity=Decimal("10000"),
        max_daily_loss=None,
        max_drawdown_pct=Decimal("5"),
    )
    assert reason is None


def test_drawdown_one_basis_point_past_max_drawdown_pct_does_trip() -> None:
    reason = evaluate_breach(
        equity=Decimal("9499"),
        day_open_equity=Decimal("9499"),
        peak_equity=Decimal("10000"),  # 5.01% drawdown
        max_daily_loss=None,
        max_drawdown_pct=Decimal("5"),
    )
    assert reason is not None
    assert reason.startswith(REASON_MAX_DRAWDOWN)


# --- record_snapshot ------------------------------------------------------------


@pytest.mark.db
def test_record_snapshot_first_snapshot_sets_peak_to_equity_and_zero_drawdown(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    peak = record_snapshot(db_conn, pid, _T0, Decimal("100000"))
    assert peak == Decimal("100000.0000")

    row = db_conn.execute(
        "SELECT equity, peak_equity, drawdown_pct FROM portfolio_equity_snapshots"
        " WHERE portfolio_id=%s AND ts=%s",
        (pid, _T0),
    ).fetchone()
    assert row == (Decimal("100000.0000"), Decimal("100000.0000"), Decimal("0.0000"))


@pytest.mark.db
def test_record_snapshot_higher_equity_raises_the_peak(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    record_snapshot(db_conn, pid, _T0, Decimal("100000"))
    peak = record_snapshot(db_conn, pid, _T0 + timedelta(seconds=5), Decimal("110000"))
    assert peak == Decimal("110000.0000")

    row = db_conn.execute(
        "SELECT peak_equity, drawdown_pct FROM portfolio_equity_snapshots"
        " WHERE portfolio_id=%s AND ts=%s",
        (pid, _T0 + timedelta(seconds=5)),
    ).fetchone()
    assert row == (Decimal("110000.0000"), Decimal("0.0000"))


@pytest.mark.db
def test_record_snapshot_lower_equity_leaves_the_peak_and_reports_positive_drawdown(
    db_conn,
) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    record_snapshot(db_conn, pid, _T0, Decimal("100000"))
    peak = record_snapshot(db_conn, pid, _T0 + timedelta(seconds=5), Decimal("90000"))
    assert peak == Decimal("100000.0000")  # unchanged -- equity fell below it

    row = db_conn.execute(
        "SELECT peak_equity, drawdown_pct FROM portfolio_equity_snapshots"
        " WHERE portfolio_id=%s AND ts=%s",
        (pid, _T0 + timedelta(seconds=5)),
    ).fetchone()
    assert row is not None
    peak_equity, drawdown_pct = row
    assert peak_equity == Decimal("100000.0000")
    assert drawdown_pct == Decimal("10.0000")  # (100000 - 90000) / 100000 * 100


@pytest.mark.db
def test_record_snapshot_peak_survives_a_simulated_restart(db_conn) -> None:
    """The load-bearing restart case: `record_snapshot` never trusts an
    in-memory peak, only what `load_peak_equity` reads back from the
    database -- so a process that "restarts" between two evaluations
    (modelled here as simply calling `load_peak_equity` again, since
    nothing about `record_snapshot` depends on any in-process state) sees
    the same peak a long-running process would have, not a peak reset to
    the latest equity."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    record_snapshot(db_conn, pid, _T0, Decimal("100000"))
    record_snapshot(db_conn, pid, _T0 + timedelta(seconds=5), Decimal("120000"))

    # "Restart": a fresh read of persisted state, no reference to any
    # Python object the two record_snapshot calls above created.
    restarted_peak = load_peak_equity(db_conn, pid)
    assert restarted_peak == Decimal("120000.0000")

    # A drawdown from the *true* peak (120000), not from a reset peak that
    # would have silently re-armed at the post-restart equity.
    peak = record_snapshot(db_conn, pid, _T0 + timedelta(seconds=10), Decimal("100000"))
    assert peak == Decimal("120000.0000")
    drawdown_pct = db_conn.execute(
        "SELECT drawdown_pct FROM portfolio_equity_snapshots WHERE portfolio_id=%s AND ts=%s",
        (pid, _T0 + timedelta(seconds=10)),
    ).fetchone()[0]
    # (120000 - 100000) / 120000 * 100, quantized to 4dp.
    assert drawdown_pct == Decimal("16.6667")


# --- load_day_open_equity --------------------------------------------------------


@pytest.mark.db
def test_load_day_open_equity_falls_back_to_initial_capital_with_no_history(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("75000"))
    assert load_day_open_equity(db_conn, pid, _T0) == Decimal("75000")


@pytest.mark.db
def test_load_day_open_equity_uses_the_last_snapshot_before_todays_start(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    yesterday_close = _T0 - timedelta(days=1)
    record_snapshot(db_conn, pid, yesterday_close, Decimal("98000"))
    # A snapshot from earlier *today* must not be picked up as day-open --
    # only history strictly before today's start counts.
    record_snapshot(db_conn, pid, _T0 - timedelta(hours=1), Decimal("99000"))

    day_open = load_day_open_equity(db_conn, pid, _T0)
    assert day_open == Decimal("98000.0000")


# --- trip -------------------------------------------------------------------------


@pytest.mark.db
def test_trip_pauses_the_portfolio_cancels_resting_orders_and_writes_an_event(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    open_order = make_order(
        db_conn, pid, side=Side.BUY, quantity=Decimal("1"), status=OrderStatus.OPEN
    )
    pending_order = make_order(
        db_conn, pid, side=Side.BUY, quantity=Decimal("1"), status=OrderStatus.PENDING
    )
    partial_order = make_order(
        db_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("2"),
        status=OrderStatus.PARTIALLY_FILLED,
        filled_quantity=Decimal("1"),
    )
    filled_order = make_order(
        db_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        status=OrderStatus.FILLED,
        filled_quantity=Decimal("1"),
    )

    trip(db_conn, pid, "max_daily_loss: exceeded", Decimal("9000"), Decimal("1000"))

    status = db_conn.execute(
        "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()
    assert status == ("PAUSED",)

    statuses = dict(
        db_conn.execute(
            "SELECT order_id, status FROM orders WHERE order_id = ANY(%s)",
            (
                [
                    open_order.order_id,
                    pending_order.order_id,
                    partial_order.order_id,
                    filled_order.order_id,
                ],
            ),
        ).fetchall()
    )
    assert statuses[open_order.order_id] == "CANCELLED"
    assert statuses[pending_order.order_id] == "CANCELLED"
    assert statuses[partial_order.order_id] == "CANCELLED"
    assert statuses[filled_order.order_id] == "FILLED"  # terminal, untouched

    event = db_conn.execute(
        "SELECT portfolio_id, reason, equity, threshold FROM circuit_breaker_events"
        " WHERE portfolio_id=%s",
        (pid,),
    ).fetchone()
    assert event == (pid, "max_daily_loss: exceeded", Decimal("9000.0000"), Decimal("1000.0000"))


@pytest.mark.db
def test_trip_enqueues_a_breach_alert_in_the_same_transaction(db_conn) -> None:
    """Task 10 deliberately left `trip` without alerting -- Task 11 owns
    wiring it in, and this is the assertion that proves it: a trip must
    enqueue a `BREACH` alert_deliveries row, written through the same
    connection/transaction as the pause/cancel/event writes above (`trip`
    itself never commits, matching `record_snapshot`'s convention)."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))

    trip(db_conn, pid, "max_daily_loss: exceeded", Decimal("9000"), Decimal("1000"))

    delivery = db_conn.execute("SELECT kind, status, payload FROM alert_deliveries").fetchone()
    assert delivery is not None
    kind, status, payload = delivery
    assert kind == "BREACH"
    assert status == "PENDING"
    decoded = json.loads(payload)
    assert decoded["portfolio_id"] == pid
    assert decoded["reason"] == "max_daily_loss: exceeded"
    assert decoded["equity"] == "9000"
    assert decoded["threshold"] == "1000"
