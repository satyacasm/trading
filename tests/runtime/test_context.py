from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.paper.enums import OrderStatus, Side
from trading.runtime.context import LiveContext
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState


def _bar(instrument_id: int, minute: int, close: str) -> BarRecord:
    return BarRecord(
        instrument_id=instrument_id,
        ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
        interval_sec=60,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("100"),
    )


def _ctx(cursor: dict[int, int] | None = None) -> LiveContext:
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11"), _bar(1, 2, "12")]})
    state = RunState(
        now=datetime(2026, 9, 1, 9, 3, tzinfo=UTC),
        cash=Decimal("100000"),
        starting_cash=Decimal("100000"),
    )
    state.cursor = cursor if cursor is not None else {1: 2}
    return LiveContext(state=state, bars=bars)


def test_it_is_a_platform_sdk_context() -> None:
    # D-S2: the offline stub and the live runtime cannot drift on shape,
    # because the live one IS the stub, subclassed.
    from trading.agent_contract import platform_sdk

    assert isinstance(_ctx(), platform_sdk.Context)


def test_now_is_the_simulation_clock() -> None:
    assert _ctx().now == datetime(2026, 9, 1, 9, 3, tzinfo=UTC)


def test_bars_cannot_reach_past_the_cursor() -> None:
    ctx = _ctx(cursor={1: 2})
    assert [b.close for b in ctx.data.bars(1, count=10)] == [Decimal("10"), Decimal("11")]


def test_bars_returns_fewer_than_count_without_complaint() -> None:
    ctx = _ctx(cursor={1: 1})
    assert len(ctx.data.bars(1, count=50)) == 1


def test_last_is_the_most_recent_closed_bar() -> None:
    last = _ctx(cursor={1: 2}).data.last(1)
    assert last is not None
    assert last.close == Decimal("11")


def test_last_is_none_before_any_bar_has_closed() -> None:
    assert _ctx(cursor={1: 0}).data.last(1) is None


def test_order_returns_an_id_and_rests_the_order() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="fast crossed slow")
    order = ctx._state.orders[order_id]
    assert order.status is OrderStatus.OPEN
    assert order.side is Side.BUY
    assert order.quantity == Decimal("10")
    assert order.submitted_at == ctx.now


def test_order_ids_are_sequential_so_two_runs_can_be_compared() -> None:
    ctx = _ctx()
    first = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="a")
    second = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="b")
    assert (first, second) == (1, 2)


def test_a_float_quantity_is_refused() -> None:
    ctx = _ctx()
    with pytest.raises(TypeError, match="Decimal"):
        ctx.order(1, side="BUY", quantity=10.0, rationale="oops")  # type: ignore[arg-type]


def test_an_empty_rationale_is_refused() -> None:
    ctx = _ctx()
    with pytest.raises(ValueError, match="rationale"):
        ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="   ")


def test_an_order_for_an_instrument_outside_the_universe_is_rejected_not_raised() -> None:
    # Contract §6: a rejection is a normal outcome the strategy learns
    # about through on_order_update, never an exception.
    ctx = _ctx()
    order_id = ctx.order(9999, side="BUY", quantity=Decimal("1"), rationale="not mine")
    order = ctx._state.orders[order_id]
    assert order.status is OrderStatus.REJECTED
    assert order.rejection_reason is not None
    assert "universe" in order.rejection_reason


def test_a_non_positive_quantity_is_rejected_not_raised() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("0"), rationale="zero")
    assert ctx._state.orders[order_id].status is OrderStatus.REJECTED


def test_cancel_marks_an_open_order_cancelled() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="a")
    ctx.cancel(order_id)
    assert ctx._state.orders[order_id].status is OrderStatus.CANCELLED


def test_cancelling_an_unknown_order_is_silent() -> None:
    # A strategy cancelling an order that already filled is ordinary, not
    # a crash -- and a crash here would fail an otherwise sound strategy.
    _ctx().cancel(4242)


def test_log_records_structured_events() -> None:
    ctx = _ctx()
    ctx.log("crossover", fast="10.5", slow="10.1")
    assert ctx._state.logs == [
        {"ts": ctx.now.isoformat(), "event": "crossover", "fast": "10.5", "slow": "10.1"}
    ]


def test_portfolio_cash_and_equity_are_visible() -> None:
    ctx = _ctx()
    assert ctx.portfolio.cash == Decimal("100000")
    assert ctx.portfolio.equity == Decimal("100000")
    assert ctx.portfolio.positions == {}


def test_is_catchup_defaults_false() -> None:
    assert _ctx().is_catchup is False
