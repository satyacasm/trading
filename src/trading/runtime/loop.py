"""The event loop -- the runtime the whole Agent Contract rests on.

Pure: bars in, orders and fills out. No database, no Docker, no clock.
That is not tidiness. This module is what Phase 3's backtester will reuse
unchanged, fed from Timescale instead of from a payload, and it is what
runs inside the sandbox where neither a database nor a network exists.

**Step order is load-bearing, and it is: clock, fills, updates, on_bar.**

`decide_fill`'s anti-lookahead guard is `tick_ts < order.submitted_at`.
An order submitted during bar N's `on_bar` carries
`submitted_at == N.close_ts`, which is not *less than* bar N's own tick
timestamps -- so if `on_bar` ran before fills, an order could fill against
the very bar whose close the strategy had just read. Filling first means
such an order simply does not exist yet when bar N is priced, and the
guard is never asked a question it would answer wrongly.

**Bars are expanded into four price events, open then high then low then
close.** `decide_fill` prices one event at a time by design (it is shared
with the live tick engine), so a bar has to become ticks. Open-high-low-
close is the conventional backtest approximation and it is an
approximation: it assumes a limit order resting inside the bar's range
was reachable, which flatters limit fills, and it cannot know the true
intra-bar path. Stated here rather than discovered later.

**`decide_fill` always fills `order.remaining` in full**, so
`PARTIALLY_FILLED` never occurs in a smoke run. That is a real gap in
what stage 2 exercises, and `trading.agent_contract.smoke` reports it
rather than letting anyone infer coverage that does not exist.

**`day_open_equity` rolls over at the IST calendar boundary, not once at
the start of the run.** A run can span several sessions, and
`max_daily_loss` is a *daily* limit -- comparing every bar's equity
against the run's starting cash would let a gain on day 1 mask an
arbitrarily large loss on day 2. Each bar's `close_ts` is converted to
its `Asia/Kolkata` calendar date (mirroring
`trading.paper.breaker.load_day_open_equity`'s own convention); the
first bar seen on a new IST date records the equity as of the previous
bar -- before that bar's own marks are applied -- as the new day's
opening equity, exactly mirroring `load_day_open_equity`'s use of the
last snapshot strictly before the day started.

**A breach stops the run.** `trading.paper.breaker.trip` pauses the
portfolio and cancels every non-terminal order the moment a limit is
breached; a smoke run that kept dispatching bars afterward would report
cash and equity for a sequence of fills that could never have happened
against a paused portfolio. On breach this loop calls
`_cancel_resting_orders` (mirroring `trip`'s cancellation) and then
stops -- no further bar is dispatched, no further fill occurs.
"""

from __future__ import annotations

import traceback
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from trading.paper.breaker import evaluate_breach
from trading.paper.charges import compute_charges
from trading.paper.enums import OrderStatus, Product, Side
from trading.paper.fills import decide_fill
from trading.paper.models import ChargeSchedule, Order, Position
from trading.runtime.context import SMOKE_PORTFOLIO_ID, LiveContext
from trading.runtime.outcome import OrderSnapshot, RunOutcome
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState

__all__ = ["run_loop"]

_TERMINAL = (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED)

# DP charges (FLAT_PER_SCRIP_PER_DAY) are an Indian broker-day
# convention, matching trading.paper.breaker's _IST / trading.paper.
# engine's _ist_day_bounds_utc -- the scrip-day key below uses this,
# not the UTC date, so a fill near midnight IST is not misclassified.
_IST = ZoneInfo("Asia/Kolkata")


class _StrategyLike(Protocol):
    def initialize(self, ctx: Any) -> None: ...
    def on_bar(self, ctx: Any, bars: dict[int, Any]) -> None: ...


class _Update:
    """The `OrderUpdate` shape the contract promises `on_order_update`."""

    def __init__(self, order: Order, previous_status: OrderStatus) -> None:
        self.order = order
        self.previous_status = previous_status


class _Crash(Exception):
    """A strategy handler raised. Carries where, so the report can say."""

    def __init__(self, handler: str, ts: str, detail: str) -> None:
        super().__init__(detail)
        self.handler = handler
        self.ts = ts
        self.detail = detail


def _call(strategy: object, handler: str, ts: str, *args: Any) -> None:
    method = getattr(strategy, handler, None)
    if method is None:
        return
    try:
        method(*args)
    except Exception as exc:  # noqa: BLE001 - every strategy failure is an outcome
        raise _Crash(handler, ts, traceback.format_exc(limit=20)) from exc


def _snapshot(order: Order) -> OrderSnapshot:
    return OrderSnapshot(
        order_id=order.order_id,
        instrument_id=order.instrument_id,
        side=str(order.side),
        order_type=str(order.order_type),
        quantity=str(order.quantity),
        limit_price=None if order.limit_price is None else str(order.limit_price),
        status=str(order.status),
        submitted_at=order.submitted_at.isoformat(),
    )


def _apply_position(state: RunState, order: Order, quantity: Decimal, price: Decimal) -> None:
    existing = state.positions.get(order.instrument_id)
    signed = quantity if order.side is Side.BUY else -quantity
    if existing is None:
        state.positions[order.instrument_id] = Position(
            portfolio_id=SMOKE_PORTFOLIO_ID,
            instrument_id=order.instrument_id,
            quantity=signed,
            avg_cost=price,
            realised_pnl=Decimal("0"),
        )
        return
    new_quantity = existing.quantity + signed
    if existing.quantity != 0 and (existing.quantity > 0) != (signed > 0):
        # Reducing or reversing: realise against the average cost.
        closed = min(abs(signed), abs(existing.quantity))
        direction = Decimal("1") if existing.quantity > 0 else Decimal("-1")
        realised = (price - existing.avg_cost) * closed * direction
        if new_quantity == 0:
            avg_cost = Decimal("0")
        elif (existing.quantity > 0) != (new_quantity > 0):
            # Reversed through zero (C1): the surviving position is a
            # brand-new one opened at this fill's price, not a
            # continuation of the side that just closed -- carrying the
            # old avg_cost forward here would misprice every subsequent
            # close of the new side (and, since realised_pnl is derived
            # from avg_cost, silently flip its sign).
            avg_cost = price
        else:
            avg_cost = existing.avg_cost
        state.positions[order.instrument_id] = existing.model_copy(
            update={
                "quantity": new_quantity,
                "avg_cost": avg_cost,
                "realised_pnl": existing.realised_pnl + realised,
            }
        )
        return
    total_cost = existing.avg_cost * abs(existing.quantity) + price * quantity
    avg_cost = total_cost / abs(new_quantity) if new_quantity != 0 else Decimal("0")
    state.positions[order.instrument_id] = existing.model_copy(
        update={"quantity": new_quantity, "avg_cost": avg_cost}
    )


def _price_events(bar: BarRecord) -> tuple[Decimal, ...]:
    return (bar.open, bar.high, bar.low, bar.close)


def _snapshots(state: RunState) -> tuple[OrderSnapshot, ...]:
    return tuple(_snapshot(state.orders[i]) for i in state.submissions if i in state.orders)


def _cancel_resting_orders(state: RunState) -> None:
    """I6: mirrors `trading.paper.breaker.trip`, which cancels every
    non-terminal order on a breach rather than leaving OPEN/PENDING/
    PARTIALLY_FILLED orders alive on a portfolio that is supposed to have
    stopped.
    """
    for order_id, order in list(state.orders.items()):
        if order.status not in _TERMINAL:
            state.orders[order_id] = order.model_copy(update={"status": OrderStatus.CANCELLED})


def run_loop(
    strategy: _StrategyLike,
    bars: InMemoryBars,
    schedules: Sequence[ChargeSchedule],
    starting_cash: Decimal,
    slippage_bps: Decimal,
    max_daily_loss: Decimal | None = None,
    max_drawdown_pct: Decimal | None = None,
) -> RunOutcome:
    state = RunState(
        now=None,  # type: ignore[arg-type]  # set before any handler runs
        cash=starting_cash,
        starting_cash=starting_cash,
    )
    state.cursor = dict.fromkeys(bars.instruments(), 0)
    state.day_open_equity = starting_cash
    state.peak_equity = starting_cash
    ctx = LiveContext(state=state, bars=bars)

    fills = 0
    rejections: list[str] = []
    reported: dict[int, OrderStatus] = {}
    # A FLAT_PER_SCRIP_PER_DAY charge (DP) is once per scrip per day, not
    # per fill. `compute_charges` stays pure and is told, not asked.
    scrip_days: set[tuple[int, date]] = set()

    def _deliver_updates(ts_iso: str) -> None:
        nonlocal rejections
        for order_id in list(state.submissions):
            order = state.orders.get(order_id)
            if order is None:
                continue
            previous = reported.get(order_id)
            if previous == order.status:
                continue
            if previous is not None or order.status is not OrderStatus.OPEN:
                update = _Update(order, previous if previous is not None else OrderStatus.OPEN)
                _call(strategy, "on_order_update", ts_iso, ctx, update)
                if order.status is OrderStatus.REJECTED and order.rejection_reason:
                    rejections.append(order.rejection_reason)
            reported[order_id] = order.status

    try:
        first_ts = next(iter(bars.groups()), None)
        if first_ts is None:
            raise _Crash("initialize", "", "no bars were provided to the run")
        state.now = first_ts[0]
        _call(strategy, "initialize", state.now.isoformat(), ctx)

        current_ist_day: date | None = None
        for close_ts, indexed in bars.indexed_groups():
            state.now = close_ts
            ts_iso = close_ts.isoformat()

            # Roll day_open_equity at the IST calendar boundary -- see
            # the module docstring. Read equity *before* this bar's own
            # marks are applied, so the new day's opening reading is the
            # portfolio as it stood at the previous bar's close, not
            # already moved by today's first print.
            ist_day = close_ts.astimezone(_IST).date()
            if current_ist_day is None:
                current_ist_day = ist_day
            elif ist_day != current_ist_day:
                state.day_open_equity = ctx.portfolio.equity
                current_ist_day = ist_day

            printed = {bar.instrument_id: bar for bar, _ in indexed}
            for instrument_id, bar in printed.items():
                state.marks[instrument_id] = bar.close

            # 1. Price resting orders against this bar, before the
            #    strategy has seen it. See the module docstring.
            for order_id in list(state.submissions):
                order = state.orders.get(order_id)
                if order is None or order.status in _TERMINAL:
                    continue
                resting_bar = printed.get(order.instrument_id)
                if resting_bar is None:
                    continue
                for price in _price_events(resting_bar):
                    order = state.orders[order_id]
                    if order.status in _TERMINAL:
                        break
                    decision = decide_fill(order, price, close_ts, slippage_bps)
                    if decision is None:
                        continue
                    # I3: the flag is considered only for SELL,
                    # mirroring trading.paper.engine's
                    # `order.side is Side.SELL and _dp_already_applied_today(...)`
                    # -- a BUY neither reads nor writes it, so a
                    # same-day BUY-then-SELL still pays DP exactly
                    # once, on the SELL. The key's date is IST, not
                    # UTC: DP is an Indian broker-day convention
                    # (breaker.py's _IST / engine.py's
                    # _ist_day_bounds_utc), and a UTC date would
                    # split an IST trading day in two.
                    key = (order.instrument_id, close_ts.astimezone(_IST).date())
                    already = order.side is Side.SELL and key in scrip_days
                    breakdown = compute_charges(
                        schedules,
                        order.side,
                        decision.quantity,
                        decision.price,
                        scrip_day_charge_already_applied=already,
                    )
                    if order.product is Product.DELIVERY and order.side is Side.SELL:
                        scrip_days.add(key)
                    notional = decision.quantity * decision.price
                    if order.side is Side.BUY:
                        state.cash -= notional + breakdown.total
                    else:
                        state.cash += notional - breakdown.total
                    _apply_position(state, order, decision.quantity, decision.price)
                    filled = order.filled_quantity + decision.quantity
                    state.orders[order_id] = order.model_copy(
                        update={
                            "filled_quantity": filled,
                            "status": (
                                OrderStatus.FILLED
                                if filled >= order.quantity
                                else OrderStatus.PARTIALLY_FILLED
                            ),
                        }
                    )
                    fills += 1

            # 2. Tell the strategy what changed.
            _deliver_updates(ts_iso)

            # 3. Dispatch the bar. Only instruments that actually printed.
            state.bar_calls += 1
            _call(strategy, "on_bar", ts_iso, ctx, dict(printed))

            # 4. Any order submitted in on_bar is OPEN and unreported;
            #    a rejection must reach the strategy in the same session.
            _deliver_updates(ts_iso)

            # 5. Advance the cursor. Only now has this bar "closed" for
            #    ctx.data -- during on_bar it was the present, not history.
            for bar, index in indexed:
                state.cursor[bar.instrument_id] = index + 1

            # 6. The breaker. Explicit `is None` checks, not `or` --
            # `state.peak_equity`/`state.day_open_equity` are seeded to
            # starting_cash before the loop and never left `None` here,
            # but a legitimate equity of exactly `0` must not be treated
            # as unset by a truthy-style fallback (see the module's
            # calling brief; both are always Decimal by this point).
            equity = ctx.portfolio.equity
            peak_equity = equity if state.peak_equity is None else max(state.peak_equity, equity)
            state.peak_equity = peak_equity
            if state.day_open_equity is None:
                day_open_equity = starting_cash
            else:
                day_open_equity = state.day_open_equity
            state.breaker_reason = evaluate_breach(
                equity,
                day_open_equity,
                peak_equity,
                max_daily_loss,
                max_drawdown_pct,
            )
            if state.breaker_reason is not None:
                # Mirror trading.paper.breaker.trip: stop trading the
                # instant a declared limit is breached. Continuing would
                # report fills for a run that could never have happened
                # against a portfolio that trip() would have paused.
                _cancel_resting_orders(state)
                break
    except _Crash as crash:
        return RunOutcome(
            ok=False,
            bar_calls=state.bar_calls,
            orders=_snapshots(state),
            fills=fills,
            rejections=tuple(rejections),
            final_cash=str(state.cash),
            final_equity=str(ctx.portfolio.equity),
            breaker_reason=state.breaker_reason,
            logs=tuple(state.logs),
            error=crash.detail,
            crashed_at={"handler": crash.handler, "ts": crash.ts, "bar_calls": state.bar_calls},
        )

    return RunOutcome(
        ok=True,
        bar_calls=state.bar_calls,
        orders=_snapshots(state),
        fills=fills,
        rejections=tuple(rejections),
        final_cash=str(state.cash),
        final_equity=str(ctx.portfolio.equity),
        breaker_reason=state.breaker_reason,
        logs=tuple(state.logs),
    )
