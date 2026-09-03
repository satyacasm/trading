from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.paper.breaker import REASON_MAX_DAILY_LOSS
from trading.paper.enums import (
    ChargeBasis,
    ChargeType,
    OrderStatus,
    OrderType,
    Product,
    Rounding,
    Side,
    TimeInForce,
)
from trading.paper.models import ChargeSchedule, Order
from trading.runtime.loop import _apply_position, run_loop
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState


def _schedules() -> tuple[ChargeSchedule, ...]:
    return (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("20.00"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )


def _series(instrument_id: int, closes: list[str]) -> list[BarRecord]:
    return [
        BarRecord(
            instrument_id=instrument_id,
            ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal(close),
            high=Decimal(close),
            low=Decimal(close),
            close=Decimal(close),
            volume=Decimal("100"),
        )
        for minute, close in enumerate(closes)
    ]


class _Recorder:
    """A strategy that records what it was handed."""

    def __init__(self) -> None:
        self.bar_batches: list[list[int]] = []
        self.updates: list[tuple[int, str, str]] = []
        self.initialized = False

    def initialize(self, ctx) -> None:  # noqa: ANN001
        self.initialized = True

    def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
        self.bar_batches.append(sorted(bars))

    def on_order_update(self, ctx, update) -> None:  # noqa: ANN001
        self.updates.append(
            (update.order.order_id, str(update.previous_status), str(update.order.status))
        )


def _run(strategy, bars, cash="100000") -> object:  # noqa: ANN001
    return run_loop(
        strategy=strategy,
        bars=bars,
        schedules=_schedules(),
        starting_cash=Decimal(cash),
        slippage_bps=Decimal("0"),
    )


def test_initialize_runs_once_before_any_bar() -> None:
    recorder = _Recorder()
    _run(recorder, InMemoryBars({1: _series(1, ["10", "11"])}))
    assert recorder.initialized is True
    assert len(recorder.bar_batches) == 2


def test_an_instrument_that_did_not_print_is_absent_not_carried_forward() -> None:
    # Contract §4. The platform will not invent a trade that did not happen.
    recorder = _Recorder()
    bars = InMemoryBars({1: _series(1, ["10", "11"]), 2: _series(2, ["20"])})
    _run(recorder, bars)
    assert recorder.bar_batches == [[1, 2], [1]]


def test_a_market_order_fills_on_the_next_bar_never_the_current_one() -> None:
    # The lookahead that would matter most: a strategy that saw this
    # bar's close must not trade at this bar's prices.
    class BuyOnce:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="entry")

    outcome = _run(BuyOnce(), InMemoryBars({1: _series(1, ["10", "20", "30"])}))
    assert outcome.fills == 1
    # Submitted while bar 0 (close 10) was dispatched; filled against bar
    # 1's open of 20, not bar 0's 10.
    assert outcome.orders[0].status == str(OrderStatus.FILLED)
    assert Decimal(outcome.final_cash) == Decimal("100000") - Decimal("200") - Decimal("20")


def test_on_order_update_fires_with_the_previous_status() -> None:
    class BuyOnce:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="entry")

    recorder = _Recorder()
    strategy = BuyOnce()
    strategy.on_order_update = recorder.on_order_update  # type: ignore[attr-defined]
    _run(strategy, InMemoryBars({1: _series(1, ["10", "20"])}))
    assert recorder.updates == [(1, "OPEN", "FILLED")]


def test_a_rejection_is_delivered_through_on_order_update_not_raised() -> None:
    class BadOrder:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(9999, side="BUY", quantity=Decimal("1"), rationale="not mine")

    recorder = _Recorder()
    strategy = BadOrder()
    strategy.on_order_update = recorder.on_order_update  # type: ignore[attr-defined]
    outcome = _run(strategy, InMemoryBars({1: _series(1, ["10", "20"])}))
    assert outcome.ok is True
    assert len(outcome.rejections) == 1
    assert recorder.updates == [(1, "OPEN", "REJECTED")]


def test_a_limit_order_fills_at_the_limit_not_at_the_better_price() -> None:
    class LimitBuy:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(
                    1,
                    side="BUY",
                    quantity=Decimal("10"),
                    order_type="LIMIT",
                    limit_price=Decimal("15"),
                    rationale="limit entry",
                )

    outcome = _run(LimitBuy(), InMemoryBars({1: _series(1, ["20", "10"])}))
    # Bar 1 trades at 10, well below the 15 limit. The fill is at 15.
    assert Decimal(outcome.final_cash) == Decimal("100000") - Decimal("150") - Decimal("20")


def test_a_crash_in_on_bar_is_captured_with_where_it_happened() -> None:
    class Exploding:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            raise ValueError("boom")

    outcome = _run(Exploding(), InMemoryBars({1: _series(1, ["10", "11"])}))
    assert outcome.ok is False
    assert "boom" in (outcome.error or "")
    assert outcome.crashed_at is not None
    assert outcome.crashed_at["handler"] == "on_bar"
    assert outcome.crashed_at["ts"] == "2026-09-01T09:01:00+00:00"


def test_a_strategy_that_never_orders_completes_cleanly() -> None:
    outcome = _run(_Recorder(), InMemoryBars({1: _series(1, ["10", "11"])}))
    assert outcome.ok is True
    assert outcome.orders == ()
    assert outcome.bar_calls == 2


def test_the_breaker_trips_on_a_declared_daily_loss() -> None:
    class BuyAndHold:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("100"), rationale="entry")

    bars = InMemoryBars({1: _series(1, ["100", "100", "10"])})
    outcome = run_loop(
        strategy=BuyAndHold(),
        bars=bars,
        schedules=_schedules(),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
        max_daily_loss=Decimal("1000"),
    )
    assert outcome.breaker_reason is not None
    # Against the constant, not a literal: a hardcoded string would keep
    # passing-or-failing on its own terms if the reason were ever renamed.
    assert outcome.breaker_reason.startswith(REASON_MAX_DAILY_LOSS)


def test_two_identical_runs_produce_identical_order_snapshots() -> None:
    # The property D-S6's double-run check relies on.
    class BuyEveryBar:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="always")

    bars = InMemoryBars({1: _series(1, ["10", "11", "12"])})
    first = _run(BuyEveryBar(), bars)
    second = _run(BuyEveryBar(), bars)
    assert first.orders == second.orders


class _SmaCrossover:
    """Buy when the 2-bar mean crosses above the 4-bar mean, sell when back below."""

    def __init__(self) -> None:
        self.held = False

    def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
        history = ctx.data.bars(1, count=4)
        if len(history) < 4:
            return
        fast = sum(b.close for b in history[-2:]) / Decimal("2")
        slow = sum(b.close for b in history) / Decimal("4")
        if fast > slow and not self.held:
            self.held = True
            ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="fast crossed above slow")
        elif fast < slow and self.held:
            self.held = False
            ctx.order(1, side="SELL", quantity=Decimal("10"), rationale="fast crossed below slow")


def test_golden_sma_crossover_trades_exactly_where_expected() -> None:
    # Closes (0-indexed): 0:11 1:12 2:13 3:14 4:15 5:12 6:10 7:8 8:8
    #
    # ctx.data.bars(instrument, count=4) is strictly-before the bar
    # currently being dispatched (the anti-lookahead cursor), so the
    # window used inside on_bar for bar i is closes[i-4:i] -- bar i's own
    # close never enters its own crossover decision. Hand-computed from
    # that window, not from the original (wrong) comment which folded
    # each bar's own close into its own average:
    #   during bar 4's on_bar, window = closes[0:4] = [11,12,13,14]
    #     fast=(13+14)/2=13.5  slow=(11+12+13+14)/4=12.5  -> cross up, BUY
    #   during bar 7's on_bar, window = closes[3:7] = [14,15,12,10]
    #     fast=(12+10)/2=11.0  slow=(14+15+12+10)/4=12.75 -> cross down, SELL
    # Both orders are MARKET and fill on the following bar's open: the BUY
    # (submitted during bar 4) at bar 5's open of 12, the SELL (submitted
    # during bar 7) at bar 8's open of 8. Money, hand-computed from the
    # _schedules() fixture's flat Rs 20/order brokerage (no other charge
    # type is configured):
    #   BUY  10 @ 12: cash -= 10*12 + 20 = -140
    #   SELL 10 @ 8:  cash += 10*8  - 20 = +60
    #   final_cash = 100000 - 140 + 60 = 99920.00
    # The position is flat after the SELL, so equity == cash: 99920.00.
    # This is the one test whose job is to pin bar selection *and* money
    # together -- side/quantity/status alone would still pass if slippage
    # flipped sign, charges landed on the wrong side, or a market order
    # filled at the bar's own close instead of the next bar's open.
    bars = InMemoryBars({1: _series(1, ["11", "12", "13", "14", "15", "12", "10", "8", "8"])})
    outcome = _run(_SmaCrossover(), bars)

    assert outcome.ok is True
    assert [(o.side, o.quantity, o.status) for o in outcome.orders] == [
        ("BUY", "10", "FILLED"),
        ("SELL", "10", "FILLED"),
    ]
    assert outcome.fills == 2
    assert outcome.final_cash == "99920.00"
    assert outcome.final_equity == "99920.00"


def _order(order_id: int, side: str, quantity: str) -> Order:
    return Order(
        order_id=order_id,
        portfolio_id=1,
        instrument_id=1,
        side=Side(side),
        order_type=OrderType.MARKET,
        quantity=Decimal(quantity),
        filled_quantity=Decimal("0"),
        limit_price=None,
        product=Product.DELIVERY,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.OPEN,
        rationale="test",
        submitted_at=datetime(2026, 9, 1, tzinfo=UTC),
    )


def test_a_position_reversal_reprices_avg_cost_at_the_new_fill_not_the_old_one() -> None:
    # C1: crossing a position through zero opens a brand-new position at
    # the reversing fill's price, not a continuation of the side that
    # just closed. Long 10 @ 100, then SELL 30 @ 120 (closes the 10 long
    # for +200, and opens a fresh 20 short at 120), then BUY 20 @ 120
    # (closes that short flat, for 0 realised since it closes at its own
    # avg_cost). Total realised_pnl must be +200, not the -200 a stale
    # avg_cost of 100 on the short leg would produce.
    state = RunState(
        now=datetime(2026, 9, 1, tzinfo=UTC), cash=Decimal("0"), starting_cash=Decimal("0")
    )
    _apply_position(state, _order(1, "BUY", "10"), Decimal("10"), Decimal("100"))
    _apply_position(state, _order(2, "SELL", "30"), Decimal("30"), Decimal("120"))
    _apply_position(state, _order(3, "BUY", "20"), Decimal("20"), Decimal("120"))

    position = state.positions[1]
    assert position.quantity == Decimal("0")
    assert position.avg_cost == Decimal("0")
    assert position.realised_pnl == Decimal("200")


def _bar(instrument_id: int, ts: datetime, price: str) -> BarRecord:
    return BarRecord(
        instrument_id=instrument_id,
        ts=ts,
        interval_sec=60,
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        volume=Decimal("100"),
    )


def test_day_open_equity_rolls_over_at_the_ist_calendar_boundary() -> None:
    # A run-scoped day_open_equity -- set once, at starting_cash, and
    # never rolled -- would compare day 2's ending equity against day 1's
    # *starting* cash, letting a genuine day-2 loss hide inside a run
    # that is still up overall. Two IST calendar days, one BUY:
    #   Day 1 (2026-09-01 IST): BUY 100 @ 100 (bar 1's open, brokerage
    #     Rs 20). cash = 100000 - 10000 - 20 = 89980. Mark drifts to
    #     120.20 by day 1's last bar:
    #     equity = 89980 + 100*120.20 = 89980 + 12020 = 102000.00
    #   Day 2 (2026-09-02 IST): first bar rolls day_open_equity to that
    #     102000.00 (the equity as of day 1's close, read before this
    #     bar's own mark is applied), then marks drop to 105.20:
    #     equity = 89980 + 100*105.20 = 89980 + 10520 = 100500.00
    #     loss = 102000.00 - 100500.00 = 1500.00 > max_daily_loss(1000)
    #     -> BREACH.
    #   A run-scoped reading (day_open_equity stuck at starting_cash
    #   100000) would compute loss = 100000 - 100500.00 = -500 (a gain)
    #   and never trip -- exactly the miss this rollover exists to close.
    class BuyOnce:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("100"), rationale="entry")

    day1 = datetime(2026, 9, 1, 4, tzinfo=UTC)  # 09:30 IST
    day2 = datetime(2026, 9, 2, 4, tzinfo=UTC)  # 09:30 IST, next calendar day
    series = [
        _bar(1, day1, "100"),
        _bar(1, day1 + timedelta(minutes=1), "100"),
        _bar(1, day1 + timedelta(minutes=2), "120.20"),
        _bar(1, day2, "105.20"),
    ]
    outcome = run_loop(
        strategy=BuyOnce(),
        bars=InMemoryBars({1: series}),
        schedules=_schedules(),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
        max_daily_loss=Decimal("1000"),
    )
    assert outcome.breaker_reason is not None
    assert outcome.breaker_reason.startswith(REASON_MAX_DAILY_LOSS)
    assert outcome.final_equity == "100500.00"
    # The run as a whole is still up on starting cash -- a run-scoped
    # (never-rolled) day_open_equity would have seen a gain, not a loss,
    # against day 2's ending equity and would never have breached.
    assert Decimal(outcome.final_equity) > Decimal("100000") - Decimal("1000")


def test_a_breach_cancels_resting_orders_and_stops_the_loop() -> None:
    # Mirrors trading.paper.breaker.trip: a breach pauses the run, so no
    # fill and no further bar dispatch may occur afterward, and whatever
    # order is left resting at the moment of breach must be cancelled --
    # not left OPEN as if the run had simply kept going.
    class BuyEveryBar:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="always")

    base = datetime(2026, 9, 1, 9, tzinfo=UTC)
    series = [
        _bar(1, base, "100"),
        # bar 1: order from bar 0 fills at this bar's open (100); the
        # mark then crashes to 1 on this same bar's close, which is what
        # trips the breaker in step 6 -- right after bar 1's on_bar has
        # already submitted a second, still-resting order.
        BarRecord(
            instrument_id=1,
            ts=base + timedelta(minutes=1),
            interval_sec=60,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("100"),
        ),
        # bar 2 would fill the resting order from bar 1 at 100 (a
        # sizeable gain) if it were ever dispatched -- it must not be.
        _bar(1, base + timedelta(minutes=2), "100"),
    ]
    outcome = run_loop(
        strategy=BuyEveryBar(),
        bars=InMemoryBars({1: series}),
        schedules=_schedules(),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
        max_daily_loss=Decimal("1000"),
    )
    assert outcome.breaker_reason is not None
    assert outcome.breaker_reason.startswith(REASON_MAX_DAILY_LOSS)
    # Only bars 0 and 1 were ever dispatched.
    assert outcome.bar_calls == 2
    # Only the order from bar 0 filled; the order submitted during bar 1
    # never got a chance to see bar 2's price events.
    assert outcome.fills == 1
    assert [o.status for o in outcome.orders] == [
        str(OrderStatus.FILLED),
        str(OrderStatus.CANCELLED),
    ]
