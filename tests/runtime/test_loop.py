from datetime import UTC, datetime
from decimal import Decimal

from trading.paper.enums import ChargeBasis, ChargeType, OrderStatus, Product, Rounding
from trading.paper.models import ChargeSchedule
from trading.runtime.loop import run_loop
from trading.runtime.provider import BarRecord, InMemoryBars


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
    assert "MAX_DAILY_LOSS" in outcome.breaker_reason


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
    # Both orders are MARKET and fill on the following bar's open (12 and
    # 8 respectively), which is why the assertion below only checks side/
    # quantity/status, not price.
    bars = InMemoryBars({1: _series(1, ["11", "12", "13", "14", "15", "12", "10", "8", "8"])})
    outcome = _run(_SmaCrossover(), bars)

    assert outcome.ok is True
    assert [(o.side, o.quantity, o.status) for o in outcome.orders] == [
        ("BUY", "10", "FILLED"),
        ("SELL", "10", "FILLED"),
    ]
    assert outcome.fills == 2
