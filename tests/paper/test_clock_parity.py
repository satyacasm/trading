"""Clock-parity proof: "one engine, two clock speeds" (design doc §6).

Phase 3's backtest engine will reuse `decide_fill` unchanged, feeding it
bars instead of ticks. That only works if a bar can stand in for the ticks
it aggregates without changing what fills or at what price. This module
builds one synthetic tick path per side, aggregates it into 1-minute bars
the same way a real bar builder would (high/low bracket every constituent
tick), and feeds one resting limit order through `decide_fill` twice --
tick-by-tick and bar-by-bar (bar's low for a buy trigger, high for a sell
trigger, per the brief) -- to check two properties:

1. If the tick path fills, the bar path fills too (a bar's high/low
   brackets every tick inside it, so a crossing visible in ticks is always
   visible in the bar).
2. The bar path's fill price is never *better* than the tick path's
   (lower for a buy, higher for a sell). This is the one that catches
   lookahead: `decide_fill` fills a crossed limit order at the limit
   price, never at the tick/bar price that crossed it, so routing the
   bar's extreme through `decide_fill` must land on the same price the
   tick path landed on. A bar engine that instead fills at its own
   extreme would produce a *better* price than was knowable at the moment
   of the crossing -- exactly the leak this test exists to catch. Each
   synthetic path below runs a tick well past the limit within the
   crossing bar (bar's own extreme != the crossing tick's price) so a
   naive "fill at the bar extreme" implementation would in fact produce a
   better price here; see task-9-report.md for the RED/GREEN proof this
   is not vacuous.

No I/O, no clock, no DB -- pure functions of in-memory data only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.fills import decide_fill
from trading.paper.models import FillDecision, Order

T0 = datetime(2026, 8, 31, 3, 45, 0, tzinfo=UTC)  # 09:15 IST, NSE open
BAR_SECONDS = 60
BPS = Decimal("10")


def _order(**kw) -> Order:
    base = dict(
        order_id=1,
        portfolio_id=1,
        instrument_id=1,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("10"),
        filled_quantity=Decimal("0"),
        limit_price=Decimal("100.00"),
        product=Product.DELIVERY,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.OPEN,
        rationale="test",
        submitted_at=T0,
    )
    base.update(kw)
    return Order(**base)


@dataclass(frozen=True)
class Bar:
    start: datetime
    end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


def _bars_from_ticks(
    ticks: list[tuple[datetime, Decimal]], bar_seconds: int = BAR_SECONDS
) -> list[Bar]:
    """Aggregate a tick path into fixed-width bars: each bar's high/low is
    the max/min of every tick that fell inside it, and the bar's `end` is
    when that range becomes knowable (the bar closes)."""
    if not ticks:
        return []
    origin = ticks[0][0]
    buckets: dict[int, list[Decimal]] = {}
    for ts, price in ticks:
        idx = int((ts - origin).total_seconds()) // bar_seconds
        buckets.setdefault(idx, []).append(price)
    bars = []
    for idx in sorted(buckets):
        prices = buckets[idx]
        start = origin + timedelta(seconds=idx * bar_seconds)
        bars.append(
            Bar(
                start=start,
                end=start + timedelta(seconds=bar_seconds),
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
            )
        )
    return bars


def _first_tick_fill(
    order: Order, ticks: list[tuple[datetime, Decimal]], slippage_bps: Decimal = BPS
) -> FillDecision | None:
    for ts, price in ticks:
        d = decide_fill(order, price, ts, slippage_bps)
        if d is not None:
            return d
    return None


def _first_bar_fill(
    order: Order, bars: list[Bar], slippage_bps: Decimal = BPS
) -> FillDecision | None:
    for bar in bars:
        bar_price = bar.low if order.side is Side.BUY else bar.high
        d = decide_fill(order, bar_price, bar.end, slippage_bps)
        if d is not None:
            return d
    return None


# Ticks drift down through the limit (100.00), then well past it (99.50)
# within the *same* bar -- so the crossing bar's own low (99.50) is a
# strictly better buy price than the tick that actually crossed (100.00).
# A naive "fill at the bar extreme" bar engine would exploit that; routing
# through `decide_fill` must not.
_BUY_CROSSING_TICKS = [
    (T0 + timedelta(seconds=0), Decimal("105.00")),
    (T0 + timedelta(seconds=15), Decimal("104.00")),
    (T0 + timedelta(seconds=30), Decimal("103.50")),
    (T0 + timedelta(seconds=45), Decimal("103.00")),
    (T0 + timedelta(seconds=60), Decimal("102.00")),
    (T0 + timedelta(seconds=75), Decimal("100.50")),
    (T0 + timedelta(seconds=90), Decimal("100.00")),  # first tick to cross the limit
    (T0 + timedelta(seconds=105), Decimal("99.50")),  # bar's low: past the limit
    (T0 + timedelta(seconds=120), Decimal("101.00")),
    (T0 + timedelta(seconds=135), Decimal("102.00")),
]

# Mirror image for a sell: ticks rally through the limit (110.00) and on
# past it (110.50) within the crossing bar.
_SELL_CROSSING_TICKS = [
    (T0 + timedelta(seconds=0), Decimal("105.00")),
    (T0 + timedelta(seconds=15), Decimal("106.00")),
    (T0 + timedelta(seconds=30), Decimal("106.50")),
    (T0 + timedelta(seconds=45), Decimal("107.00")),
    (T0 + timedelta(seconds=60), Decimal("108.00")),
    (T0 + timedelta(seconds=75), Decimal("109.50")),
    (T0 + timedelta(seconds=90), Decimal("110.00")),  # first tick to cross the limit
    (T0 + timedelta(seconds=105), Decimal("110.50")),  # bar's high: past the limit
    (T0 + timedelta(seconds=120), Decimal("109.00")),
    (T0 + timedelta(seconds=135), Decimal("108.00")),
]


def test_buy_limit_clock_parity() -> None:
    order = _order(side=Side.BUY, limit_price=Decimal("100.00"))
    bars = _bars_from_ticks(_BUY_CROSSING_TICKS)

    tick_fill = _first_tick_fill(order, _BUY_CROSSING_TICKS)
    bar_fill = _first_bar_fill(order, bars)

    assert tick_fill is not None  # sanity: the path is constructed to cross
    assert bar_fill is not None  # property 1: coarser data doesn't miss it
    # property 2: "better" for a buy means lower -- the bar must not
    # undercut the tick-driven price.
    assert bar_fill.price >= tick_fill.price
    # Both actually land on the limit: decide_fill's conservatism means
    # neither path can do better than 100.00, so they agree exactly.
    assert tick_fill.price == Decimal("100.00")
    assert bar_fill.price == Decimal("100.00")


def test_sell_limit_clock_parity() -> None:
    order = _order(side=Side.SELL, limit_price=Decimal("110.00"))
    bars = _bars_from_ticks(_SELL_CROSSING_TICKS)

    tick_fill = _first_tick_fill(order, _SELL_CROSSING_TICKS)
    bar_fill = _first_bar_fill(order, bars)

    assert tick_fill is not None
    assert bar_fill is not None
    # property 2: "better" for a sell means higher.
    assert bar_fill.price <= tick_fill.price
    assert tick_fill.price == Decimal("110.00")
    assert bar_fill.price == Decimal("110.00")


def test_bar_path_never_fills_when_tick_path_does_not() -> None:
    """Property 1 holds vacuously when neither path ever crosses -- pin it
    explicitly so a bug that makes the bar path fill on nothing (e.g.
    ignoring the crossing check) can't hide behind the crossing tests."""
    order = _order(side=Side.BUY, limit_price=Decimal("10.00"))
    bars = _bars_from_ticks(_BUY_CROSSING_TICKS)

    assert _first_tick_fill(order, _BUY_CROSSING_TICKS) is None
    assert _first_bar_fill(order, bars) is None
