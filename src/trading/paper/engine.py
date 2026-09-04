"""Entry point: `python -m trading.paper.engine`.

The long-running process that turns resting orders into fills. Consumes
the existing `ticks:*` Redis fan-out (crypto_ingestor today; any future
publisher tomorrow), holds open orders in memory keyed by
`instrument_id`, and commits each fill -- fill row, ledger entry,
position, cash, order status -- atomically via `trading.paper.ledger.
apply_fill`.

Follows `trading.streaming.crypto_ingestor` and `trading.streaming.
bar_aggregator`'s established shape: reconnect/backoff is inherited for
free from Redis's own pubsub (no outer socket to reconnect, unlike a raw
websocket feed), and per-message exception containment is the same
discipline commit `5373207` established for the bar aggregator -- a
single malformed tick, a single bad control message, must never kill the
loop.

**The transaction boundary is this module's, not `apply_fill`'s.**
`apply_fill` deliberately never commits (see its own docstring). Every
fill here opens a *fresh* connection via `conn_factory` (never the
connection `run_engine` used to load orders at startup, and never shared
across fills), calls `apply_fill`, and commits that one connection before
doing anything else -- including the `fills:{portfolio_id}` publish,
which happens strictly after commit. Dying before the commit means no
fill happened, which is correct; dying after means the UI merely misses
a live event a refresh recovers. A double fill is impossible because a
fully-filled order is removed from the in-memory book the moment its own
commit succeeds, before the next tick can ever see it again.

**A permanently unfillable order is rejected, never retried.** Two
failures inside `_process_fill` are treated as permanent, not transient,
and both get the same response: `apply_fill` can raise
`psycopg.errors.CheckViolation` (`ck_no_negative_cash` or
`ck_no_negative_position`) because the API's submit-time cash check is
necessarily an estimate -- a market order's real fill price, and every
order's charges, are unknowable until the tick that fills it; and
`load_schedules`/`compute_charges` can raise `MissingChargeSchedule` if a
long-resting `GTC` order outlives its `charge_schedules` row's
`effective_to`. Either way, the failed transaction is rolled back, the
order is marked `REJECTED` with a `rejection_reason` in its own fresh
commit, and it is dropped from the in-memory book. Leaving it `OPEN`
would retry -- and fail, and log -- on every subsequent tick, live-locking
the engine against an order that can never fill.

**Per-order isolation on any other failure.** `_handle_tick_message`
wraps each order's `_process_fill` call individually, not the whole tick.
An exception that isn't one of the two permanent cases above (a transient
DB error, say) is logged and the loop moves on to the next order on the
same instrument -- the failing order is left exactly as it was (`OPEN`,
still in the book, retried on the next tick), and, critically, a healthy
neighbour resting on the same instrument still fills on the *same* tick
instead of being starved by the first order's trouble.

**The circuit breaker (Task 10) is evaluated on a 5-second timer and
immediately after every fill**, never per-tick -- a threshold that moves
in minutes doesn't need ~107 evaluations a second. `evaluate_breaker_for_
portfolio` is the one function both triggers call; on a breach it calls
`trading.paper.breaker.trip` (database-only: pauses the portfolio,
cancels its resting orders, writes a `circuit_breaker_events` row) and
then, critically, drops every order belonging to that `portfolio_id` from
this process's in-memory `book` directly -- the same class of bug this
module has already been bitten by twice: a DB-only cancel the in-memory
book never hears about still fills on the next tick, doing the opposite
of what was intended. `apply_fill`'s `OrderNoLongerFillable` guard would
catch a stale fill at the database layer as a backstop, but the book is
never allowed to rely on it here.

**A fill can lose a race against a concurrent cancel -- closed in two
layers.** `_handle_tick_message` snapshots the resting orders for an
instrument at tick start, then `await`s each fill in turn; the
`orders:control` consumer is a *separate* task, so control can yield
between orders and a `cancel` can remove one mid-snapshot. Layer one is a
cheap, best-effort recheck against `book.order_index` immediately before
each `_process_fill` call -- it catches the case where the cancel has
already reached this process's in-memory book. Layer two, the one that
actually closes the race, lives in `apply_fill` itself
(`trading.paper.ledger.OrderNoLongerFillable`): its final order-status
UPDATE is guarded by `WHERE status IN (...)`, so a cancel that committed
on another connection -- even one this process hasn't heard about yet via
`orders:control`, since the API publishes that message *before* its own
commit (`trading.streaming.db.get_db_connection` commits after the route
body returns) -- still wins. When that guard trips, the whole transaction
rolls back and `_process_fill` treats it as success, not error: the order
is dropped from the book and nothing is written, because the order's real
status already reflects the correct outcome.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import structlog
from psycopg import Connection
from psycopg.errors import CheckViolation
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
from trading.paper.alerts import enqueue_alert
from trading.paper.breaker import (
    REASON_MAX_DAILY_LOSS,
    MissingMark,
    compute_equity,
    evaluate_breach,
    load_day_open_equity,
    quantize_money,
    quantize_pct,
    record_snapshot,
    trip,
)
from trading.paper.charges import (
    AmbiguousChargeSchedule,
    InvalidChargeSchedule,
    MissingChargeSchedule,
    compute_charges,
    load_schedules,
)
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.fills import decide_fill
from trading.paper.ledger import OrderNoLongerFillable, apply_fill
from trading.paper.ledger import quantize_money as quantize_fill_price
from trading.paper.models import FillDecision, Order, Position
from trading.streaming.models import Tick

log = structlog.get_logger(__name__)

ConnFactory = Callable[[], Connection]
Sleeper = Callable[[float], Awaitable[None]]

_TICK_PATTERN = "ticks:*"
_CONTROL_CHANNEL = "orders:control"

_IST = ZoneInfo("Asia/Kolkata")

# Charge schedules are seeded per broker (migration 0007): UPSTOX/NSE for
# equity, BINANCE/BINANCE for crypto. Mirrors `trading.paper.api`'s
# identical mapping -- duplicated rather than imported, since api.py's copy
# is a private, route-local constant and this module has no other reason
# to depend on the HTTP layer.
_BROKER_BY_ASSET_CLASS: dict[str, str] = {
    "EQUITY": "UPSTOX",
    "CRYPTO": "BINANCE",
}

_TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)

_ORDER_COLUMNS = (
    "order_id, portfolio_id, instrument_id, side, order_type, quantity,"
    " filled_quantity, limit_price, product, time_in_force, status,"
    " rationale, submitted_at"
)

# Matches trading.paper.api's own literal-string status check -- no
# PortfolioStatus enum exists in this codebase (trading.paper.enums has
# none, and portfolios.status has no CHECK constraint), so this mirrors
# the established convention rather than introducing a new one here.
_PORTFOLIO_STATUS_ACTIVE = "ACTIVE"


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def validate_slippage_bps(slippage_bps: Decimal) -> None:
    """Fail loudly at startup on a negative `slippage_bps`.

    `decide_fill` is deliberately pure and total -- it trusts whatever
    `slippage_bps` it's handed and always moves the fill price *against*
    the order (worse for a BUY, worse for a SELL). A negative value would
    silently flip that into the order's favour, so the validation belongs
    here, at the boundary where the value enters the process, not inside
    `decide_fill` itself.
    """
    if slippage_bps < 0:
        raise ValueError(
            "slippage_bps must be >= 0 -- a negative value would flip slippage into the "
            f"order's favour, violating decide_fill's always-against-the-order rule; got "
            f"{slippage_bps}"
        )


def _order_from_row(row: Sequence[Any]) -> Order:
    (
        order_id,
        portfolio_id,
        instrument_id,
        side,
        order_type,
        quantity,
        filled_quantity,
        limit_price,
        product,
        time_in_force,
        order_status,
        rationale,
        submitted_at,
    ) = row
    return Order(
        order_id=order_id,
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        side=Side(side),
        order_type=OrderType(order_type),
        quantity=quantity,
        filled_quantity=filled_quantity,
        limit_price=limit_price,
        product=Product(product),
        time_in_force=TimeInForce(time_in_force),
        status=OrderStatus(order_status),
        rationale=rationale,
        submitted_at=submitted_at,
    )


def _load_instrument_meta(conn: Connection, instrument_id: int) -> tuple[str, str, str]:
    row = conn.execute(
        "SELECT asset_class, exchange, segment FROM instruments WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone()
    assert row is not None, f"order references instrument_id={instrument_id}, which doesn't exist"
    return (row[0], row[1], row[2])


def _load_positions(conn: Connection, portfolio_id: int) -> list[Position]:
    rows = conn.execute(
        "SELECT instrument_id, quantity, avg_cost, realised_pnl"
        " FROM positions WHERE portfolio_id = %s",
        (portfolio_id,),
    ).fetchall()
    return [
        Position(
            portfolio_id=portfolio_id,
            instrument_id=instrument_id,
            quantity=quantity,
            avg_cost=avg_cost,
            realised_pnl=realised_pnl,
        )
        for instrument_id, quantity, avg_cost, realised_pnl in rows
    ]


def _load_marks(conn: Connection, positions: Sequence[Position]) -> dict[int, Decimal]:
    """Latest `bars_intraday` close per held instrument -- the same
    "reference price" source `trading.paper.api._require_sufficient_cash`
    already uses for a MARKET order's submit-time cash estimate. A
    position with `quantity == 0` needs no mark (see `compute_equity`),
    so it's skipped here too rather than spending a query on it. A
    position with no `bars_intraday` row at all is simply left out of the
    returned mapping -- `compute_equity` is what turns that into a loud
    `MissingMark`, not this loader.
    """
    marks: dict[int, Decimal] = {}
    for position in positions:
        if position.quantity == 0:
            continue
        row = conn.execute(
            "SELECT close FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
            (position.instrument_id,),
        ).fetchone()
        if row is not None:
            marks[position.instrument_id] = row[0]
    return marks


def _drop_portfolio_orders_from_book(book: OpenOrderBook, portfolio_id: int) -> list[int]:
    """Every order belonging to `portfolio_id`, across every instrument,
    removed from `book` directly. This is the engine-side half of a trip:
    `trading.paper.breaker.trip` only ever touches the database, so
    nothing removes a just-cancelled order from this process's in-memory
    book unless this function (or its caller) does -- see the module
    docstring's circuit-breaker paragraph for why that gap is dangerous.
    """
    order_ids = [
        order.order_id
        for orders in book.open_orders.values()
        for order in orders
        if order.portfolio_id == portfolio_id
    ]
    for order_id in order_ids:
        book.remove(order_id)
    return order_ids


def evaluate_breaker_for_portfolio(
    conn_factory: ConnFactory, book: OpenOrderBook, portfolio_id: int, now: datetime
) -> None:
    """Snapshot `portfolio_id`'s equity, then pause it if that snapshot
    breaches its declared limits -- the one function both the 5-second
    timer and the post-fill trigger call.

    Already-`PAUSED`, `LIQUIDATED`, or otherwise non-`ACTIVE` portfolios
    are skipped entirely: re-evaluating a paused portfolio would either
    no-op harmlessly against `trip`'s own idempotent UPDATEs, or -- worse,
    if a status ever gained meaning beyond "paused" -- silently write a
    second `circuit_breaker_events` row for a portfolio that already
    stopped trading. `record_snapshot` still ought to run for every
    *active* portfolio's equity curve (Phase 3 needs the history), but a
    paused portfolio's curve is frozen by construction: nothing can
    change its cash or positions once every resting order is cancelled.

    A `MissingMark` (a held position with no available price) is caught
    here, logged, and swallowed -- not re-raised -- exactly like every
    other per-item failure in this engine (a malformed tick, a rejected
    fill): one portfolio's pricing gap must never abort the sweep for its
    neighbours, and must certainly never crash the loop.
    """
    conn = conn_factory()
    reason: str | None = None
    try:
        row = conn.execute(
            "SELECT cash_balance, status, max_daily_loss, max_drawdown_pct"
            " FROM portfolios WHERE portfolio_id = %s",
            (portfolio_id,),
        ).fetchone()
        if row is None:
            return
        cash, portfolio_status, max_daily_loss, max_drawdown_pct = row
        if portfolio_status != _PORTFOLIO_STATUS_ACTIVE:
            return

        positions = _load_positions(conn, portfolio_id)
        marks = _load_marks(conn, positions)
        # Quantized once, here, at the source -- compute_equity's raw
        # output sums quantity (NUMERIC(18,8)) * mark (NUMERIC(18,4)) and
        # can carry up to twelve fractional digits. Quantizing before this
        # value is threaded through record_snapshot, evaluate_breach, and
        # trip is what guarantees every one of them sees the identical
        # number, rather than each -- or, before this fix, only
        # record_snapshot -- rounding it independently. See breaker.py's
        # module docstring and quantize_money's own docstring.
        equity = quantize_money(compute_equity(cash, positions, marks))

        day_open_equity = load_day_open_equity(conn, portfolio_id, now)
        peak_equity = record_snapshot(conn, portfolio_id, now, equity)
        reason = evaluate_breach(
            equity, day_open_equity, peak_equity, max_daily_loss, max_drawdown_pct
        )
        if reason is not None:
            # evaluate_breach prefixes its reason with whichever limit
            # breached, precisely so this decision doesn't have to
            # re-derive which one it was. The two limits live on
            # different columns/scales (max_daily_loss: NUMERIC(18,4)
            # money; max_drawdown_pct: NUMERIC(9,4) percentage), so the
            # quantizer must match which one actually breached.
            if reason.startswith(REASON_MAX_DAILY_LOSS):
                assert max_daily_loss is not None, (
                    f"breach reason {reason!r} named a limit that is None"
                )
                threshold = quantize_money(max_daily_loss)
            else:
                assert max_drawdown_pct is not None, (
                    f"breach reason {reason!r} named a limit that is None"
                )
                threshold = quantize_pct(max_drawdown_pct)
            trip(conn, portfolio_id, reason, equity, threshold)
        conn.commit()
    except MissingMark as exc:
        conn.rollback()
        log.warning("paper_engine.breaker_missing_mark", portfolio_id=portfolio_id, reason=str(exc))
        return
    finally:
        conn.close()

    if reason is not None:
        dropped = _drop_portfolio_orders_from_book(book, portfolio_id)
        log.warning(
            "paper_engine.breaker_tripped",
            portfolio_id=portfolio_id,
            reason=reason,
            dropped_order_ids=dropped,
        )


def evaluate_breaker_for_all_active_portfolios(
    conn_factory: ConnFactory, book: OpenOrderBook, now: datetime
) -> None:
    """The 5-second timer's entry point: every `ACTIVE` portfolio, each
    evaluated in its own transaction so one portfolio's failure -- a
    missing mark, a transient DB error -- can never block its
    neighbours, mirroring `_handle_tick_message`'s per-order isolation.
    """
    conn = conn_factory()
    try:
        portfolio_ids = [
            row[0]
            for row in conn.execute(
                "SELECT portfolio_id FROM portfolios WHERE status = %s",
                (_PORTFOLIO_STATUS_ACTIVE,),
            ).fetchall()
        ]
    finally:
        conn.close()

    for portfolio_id in portfolio_ids:
        try:
            evaluate_breaker_for_portfolio(conn_factory, book, portfolio_id, now)
        except Exception as exc:  # noqa: BLE001 - one portfolio's failure must never block its
            # neighbours, and must never kill the periodic check itself.
            log.warning(
                "paper_engine.breaker_check_failed", portfolio_id=portfolio_id, reason=str(exc)
            )


@dataclass
class OpenOrderBook:
    """Every resting order the engine is watching, in memory, keyed by
    `instrument_id` -- the shape `decide_fill` needs on each tick.

    `instrument_meta` caches `(asset_class, exchange, segment)` per
    instrument so a fill's `load_schedules` call and the session sweep
    never need an extra round trip on the hot path. `order_index` is the
    reverse map (`order_id -> instrument_id`) `orders:control`'s
    `{"action": "cancel", ...}` messages need for an O(1) removal instead
    of scanning every instrument's list.
    """

    open_orders: dict[int, list[Order]] = field(default_factory=dict)
    instrument_meta: dict[int, tuple[str, str, str]] = field(default_factory=dict)
    order_index: dict[int, int] = field(default_factory=dict)

    def add(self, order: Order, meta: tuple[str, str, str] | None = None) -> None:
        # Idempotent on `order_id`: no code path today adds the same order
        # twice (the control channel's "new" only ever names an order
        # `load_open_orders` hasn't already loaded), but guarding it here
        # is free and turns any future violation of that invariant into a
        # silent no-op instead of a double-processed order sitting twice
        # in the same instrument's list.
        if order.order_id in self.order_index:
            return
        self.open_orders.setdefault(order.instrument_id, []).append(order)
        self.order_index[order.order_id] = order.instrument_id
        if meta is not None:
            self.instrument_meta.setdefault(order.instrument_id, meta)

    def remove(self, order_id: int) -> None:
        instrument_id = self.order_index.pop(order_id, None)
        if instrument_id is None:
            return
        remaining = [o for o in self.open_orders.get(instrument_id, []) if o.order_id != order_id]
        if remaining:
            self.open_orders[instrument_id] = remaining
        else:
            self.open_orders.pop(instrument_id, None)

    def replace(self, order: Order) -> None:
        """Swap in an updated snapshot of an order already in the book
        (e.g. after a partial fill changes `filled_quantity`)."""
        orders = self.open_orders.get(order.instrument_id, [])
        self.open_orders[order.instrument_id] = [
            order if o.order_id == order.order_id else o for o in orders
        ]
        self.order_index[order.order_id] = order.instrument_id


def _promote_pending_and_load_active_orders(conn: Connection) -> list[Order]:
    """Promote every still-`PENDING` order to `OPEN` (a `PENDING` order is
    one the API accepted but no process has picked up yet -- whether
    because the engine crashed, restarted, or is simply slower to start
    than the API, or, for `reconcile_missing_orders`, because the
    `orders:control` `new` message telling this process about it never
    arrived), then return every `OPEN`/`PARTIALLY_FILLED` order as of
    right now.

    Shared by `load_open_orders` (startup: every one of these goes into a
    fresh, empty book) and `reconcile_missing_orders` (steady state: only
    the ones missing from an already-populated book get adopted -- see its
    own docstring). Commits its own writes: this is one-shot housekeeping,
    not a step inside a fill's transaction boundary.
    """
    conn.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE status = %s",
        (OrderStatus.OPEN.value, OrderStatus.PENDING.value),
    )
    rows = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE status IN (%s, %s)",
        (OrderStatus.OPEN.value, OrderStatus.PARTIALLY_FILLED.value),
    ).fetchall()
    return [_order_from_row(row) for row in rows]


def load_open_orders(conn: Connection) -> OpenOrderBook:
    """Startup load: every `PENDING`/`OPEN`/`PARTIALLY_FILLED` order (see
    `_promote_pending_and_load_active_orders`) into a fresh `OpenOrderBook`,
    keyed by `instrument_id`.
    """
    book = OpenOrderBook()
    for order in _promote_pending_and_load_active_orders(conn):
        if order.instrument_id not in book.instrument_meta:
            book.instrument_meta[order.instrument_id] = _load_instrument_meta(
                conn, order.instrument_id
            )
        book.add(order)
    conn.commit()
    return book


def reconcile_missing_orders(conn: Connection, book: OpenOrderBook) -> list[int]:
    """IMP-1's backstop. `create_order` publishes `orders:control`'s `new`
    message *before* its own transaction commits (`trading.streaming.db.
    get_db_connection` commits at dependency teardown, which -- measured
    against the installed FastAPI version, see `test_background_task_runs_
    before_yield_dependency_teardown` in `tests/paper/test_api.py` -- runs
    *after* a `BackgroundTasks` task, not before it, so moving the publish
    there would not fix this). Redis can therefore deliver `new` before
    Postgres shows the order to a fresh connection, in which case
    `_fetch_and_promote_order` finds nothing and the order is dropped
    forever, silently. A dropped pub/sub message has the identical
    symptom. Either way, no source-level fix closes this gap by itself.

    This periodic sweep is the backstop that does: it re-derives the same
    `PENDING`/`OPEN`/`PARTIALLY_FILLED` set `load_open_orders` computes at
    startup (promoting `PENDING` -> `OPEN` the same way) and adopts into
    `book` whatever isn't already tracked there. Every adoption is logged
    at `info` by the caller -- an order arriving via this path means a
    message was lost, and that must be visible, not silent.
    """
    adopted: list[int] = []
    for order in _promote_pending_and_load_active_orders(conn):
        if order.order_id in book.order_index:
            continue
        meta = book.instrument_meta.get(order.instrument_id)
        if meta is None:
            meta = _load_instrument_meta(conn, order.instrument_id)
            book.instrument_meta[order.instrument_id] = meta
        book.add(order, meta)
        adopted.append(order.order_id)
    conn.commit()
    return adopted


def _session_close_utc(session_date: date, session_close: time) -> datetime:
    """`session_close` is a naive wall-clock time in Asia/Kolkata (the
    `trading_calendar` convention -- see `trading.calendar.trading_days`
    and `trading.recorder.__main__._session_close`), so it has to be
    localized before it's comparable to a tz-aware `now`."""
    return datetime.combine(session_date, session_close, tzinfo=_IST).astimezone(UTC)


def _is_session_closed(
    conn: Connection,
    exchange: str,
    segment: str,
    now: datetime,
    session_date: date,
) -> bool:
    """Whether `exchange`/`segment`'s `session_date` session is over as of `now`.

    `session_date` is the session the *order* belongs to, not the one
    `now` falls in. Deriving it from `now` instead answers "is today's
    session closed?", which is a different question and the wrong one:
    a DAY order that outlived its own session close -- because nothing
    swept it at the time, the engine having been down -- would then
    survive every sweep until the *current* session closes, silently
    behaving as GTC and staying fillable at a later session's prices.

    No `trading_calendar` row at all is treated as "not closed" -- an
    unknown calendar state must never be silently assumed to justify
    expiring a live order, mirroring `api.py`'s `_require_market_open`
    treating a missing row as closed-for-submission (the conservative
    direction differs because the actions differ: refusing a *new* order
    is safe to over-trigger, expiring an *existing* one is not)."""
    row = conn.execute(
        "SELECT is_trading_day, session_close FROM trading_calendar"
        " WHERE exchange = %s AND segment = %s AND session_date = %s",
        (exchange, segment, session_date),
    ).fetchone()
    if row is None:
        return False
    is_trading_day, session_close = row
    if not is_trading_day:
        return True
    if session_close is None:
        return False
    return now >= _session_close_utc(session_date, session_close)


def sweep_expired_day_orders(conn: Connection, book: OpenOrderBook, now: datetime) -> list[int]:
    """Expire every resting `DAY` order whose session has closed as of
    `now`. `GTC` orders are never touched, and `CRYPTO` instruments are
    never touched -- crypto trades 24/7, so it has no session to close.

    Commits its own writes (one-shot housekeeping, not part of a fill's
    transaction boundary) and mutates `book` in place, removing every
    order it expires. Returns the swept `order_id`s.
    """
    swept: list[int] = []
    for instrument_id in list(book.open_orders):
        asset_class, exchange, segment = book.instrument_meta[instrument_id]
        if asset_class == "CRYPTO":
            continue
        for order in list(book.open_orders.get(instrument_id, [])):
            if order.time_in_force is not TimeInForce.DAY:
                continue
            # Per order, not per instrument: two resting orders on the same
            # instrument can belong to different sessions.
            if not _is_session_closed(
                conn, exchange, segment, now, order.submitted_at.astimezone(_IST).date()
            ):
                continue
            conn.execute(
                "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
                (OrderStatus.EXPIRED.value, order.order_id),
            )
            book.remove(order.order_id)
            swept.append(order.order_id)
    if swept:
        conn.commit()
    return swept


def _ist_day_bounds_utc(ts: datetime) -> tuple[datetime, datetime]:
    """The `[start, end)` UTC bounds of `ts`'s Asia/Kolkata calendar day --
    DP charges are an Indian broker convention (Rs 20/scrip/day), so "day"
    means the IST day, matching `_is_session_closed`'s identical
    convention for session boundaries, not a UTC midnight-to-midnight
    window that would split an IST trading day in two."""
    ist_date = ts.astimezone(_IST).date()
    start = datetime.combine(ist_date, time.min, tzinfo=_IST).astimezone(UTC)
    end = datetime.combine(ist_date + timedelta(days=1), time.min, tzinfo=_IST).astimezone(UTC)
    return start, end


def _dp_already_applied_today(
    conn: Connection, portfolio_id: int, instrument_id: int, product: Product, tick_ts: datetime
) -> bool:
    """Whether a DELIVERY sell fill for this portfolio+instrument already
    incurred a non-zero DP charge earlier today (IST) -- see
    compute_charges's `scrip_day_charge_already_applied` kwarg. Queried by
    the caller inside the same transaction `apply_fill` is about to run
    in, so two same-tick fills for the same scrip can never both see "not
    yet charged today"."""
    day_start, day_end = _ist_day_bounds_utc(tick_ts)
    row = conn.execute(
        "SELECT 1 FROM fills f JOIN orders o ON o.order_id = f.order_id"
        " WHERE o.portfolio_id = %s AND o.instrument_id = %s AND o.product = %s"
        "   AND o.side = %s AND f.dp_charges > 0"
        "   AND f.filled_at >= %s AND f.filled_at < %s"
        " LIMIT 1",
        (portfolio_id, instrument_id, product.value, Side.SELL.value, day_start, day_end),
    ).fetchone()
    return row is not None


def _parse_tick(raw: str) -> Tick | None:
    try:
        return Tick.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("paper_engine.malformed_tick", reason=str(exc), raw=raw[:200])
        return None


def _rejection_reason(
    exc: CheckViolation | MissingChargeSchedule | AmbiguousChargeSchedule | InvalidChargeSchedule,
) -> str:
    if isinstance(exc, CheckViolation):
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        if constraint:
            return f"fill rejected: constraint {constraint} violated at fill time"
        return f"fill rejected: {exc}"
    if isinstance(exc, AmbiguousChargeSchedule):
        return f"fill rejected: ambiguous charge schedule at fill time: {exc}"
    if isinstance(exc, InvalidChargeSchedule):
        return f"fill rejected: invalid charge schedule at fill time: {exc}"
    return f"fill rejected: no charge schedule available at fill time: {exc}"


async def _process_fill(
    conn_factory: ConnFactory,
    redis: Redis,
    book: OpenOrderBook,
    order: Order,
    decision: FillDecision,
) -> None:
    # Quantize once, here, at the source -- IMP-5. apply_fill's own
    # quantize only rebinds its local name (FillDecision is frozen), so it
    # cannot reach back into this caller's object. Every downstream
    # consumer of decision.price below (compute_charges, the FILL alert
    # payload, apply_fill itself, and the fills:{portfolio_id} publish
    # after it) must share one value, not four independently-precise ones.
    # See ledger.py's module docstring and quantize_money's own docstring.
    decision = decision.model_copy(update={"price": quantize_fill_price(decision.price)})

    asset_class, exchange, segment = book.instrument_meta[order.instrument_id]
    broker = _BROKER_BY_ASSET_CLASS.get(asset_class)

    conn = conn_factory()
    try:
        try:
            schedules = (
                load_schedules(
                    conn, broker, exchange, asset_class, order.product, decision.tick_ts.date()
                )
                if broker is not None
                else []
            )
            # IMP-4: a FLAT_PER_SCRIP_PER_DAY charge (DP charges) is a
            # once-per-scrip-per-day cost, not a per-fill one. Queried
            # inside this same transaction (the one apply_fill is about to
            # run in), so two same-tick fills for the same scrip can't both
            # see "not yet charged today" -- see _dp_already_applied_today.
            already_applied = order.side is Side.SELL and _dp_already_applied_today(
                conn, order.portfolio_id, order.instrument_id, order.product, decision.tick_ts
            )
            charges = compute_charges(
                schedules,
                order.side,
                decision.quantity,
                decision.price,
                scrip_day_charge_already_applied=already_applied,
            )
            fill_id = apply_fill(conn, order, decision, charges)
            # Task 11's wiring: enqueued inside the same transaction
            # apply_fill just wrote to, so the fill and the alert that
            # reports it commit -- or roll back -- as one unit.
            enqueue_alert(
                conn,
                "FILL",
                {
                    "fill_id": fill_id,
                    "order_id": order.order_id,
                    "portfolio_id": order.portfolio_id,
                    "instrument_id": order.instrument_id,
                    "side": order.side.value,
                    "quantity": decision.quantity,
                    "price": decision.price,
                },
            )
            conn.commit()
        except OrderNoLongerFillable as exc:
            # Not an error, and not one of the permanent-rejection cases
            # below: the order's status changed underneath us -- most
            # concretely, a `cancel` published on orders:control that
            # committed on another connection after this fill was decided
            # but before apply_fill's final, guarded UPDATE ran. Rolling
            # back undoes everything apply_fill already wrote (fill row,
            # cash, ledger entry, position) for this no-longer-fillable
            # order; the order's real current status (e.g. CANCELLED)
            # already reflects the correct outcome, so there is nothing
            # further to write here -- just stop tracking it.
            conn.rollback()
            book.remove(order.order_id)
            log.info("paper_engine.fill_lost_race", order_id=order.order_id, reason=str(exc))
            return
        except (
            CheckViolation,
            MissingChargeSchedule,
            AmbiguousChargeSchedule,
            InvalidChargeSchedule,
        ) as exc:
            # All three are permanent, not transient: an unaffordable fill
            # will still be unaffordable on retry (barring a cash deposit
            # this engine has no way to observe); a missing charge schedule
            # can only be fixed by changing data this process doesn't
            # control -- a long-resting GTC order can genuinely outlive its
            # charge_schedules row's effective_to; and an ambiguous schedule
            # (two in-force rows for one charge type) needs a human to fix
            # the data, not a retry, as does an invalid one (a basis that
            # cannot apply to its charge type). Either way, leaving the order OPEN
            # would retry -- and fail, and log -- on every subsequent tick
            # forever. Reject it instead, exactly like the CheckViolation
            # path already did before MissingChargeSchedule joined it here.
            conn.rollback()
            reason = _rejection_reason(exc)
            conn.execute(
                "UPDATE orders SET status = %s, rejection_reason = %s, updated_at = now()"
                " WHERE order_id = %s",
                (OrderStatus.REJECTED.value, reason, order.order_id),
            )
            # Same wiring as the FILL alert above, in this rejection's own
            # fresh (post-rollback) transaction.
            enqueue_alert(
                conn,
                "REJECTED",
                {
                    "order_id": order.order_id,
                    "portfolio_id": order.portfolio_id,
                    "reason": reason,
                },
            )
            conn.commit()
            book.remove(order.order_id)
            log.warning("paper_engine.fill_rejected", order_id=order.order_id, reason=reason)
            return
    finally:
        conn.close()

    filled = order.filled_quantity + decision.quantity
    status = OrderStatus.FILLED if filled >= order.quantity else OrderStatus.PARTIALLY_FILLED
    if status is OrderStatus.FILLED:
        book.remove(order.order_id)
    else:
        book.replace(order.model_copy(update={"filled_quantity": filled, "status": status}))

    # Circuit breaker, trigger 2 of 2 -- "immediately after every fill" (see
    # the module docstring). A failure here must never suppress a fill that
    # already committed; the fill is real regardless of whether the
    # portfolio can be evaluated for a breach right now.
    try:
        evaluate_breaker_for_portfolio(conn_factory, book, order.portfolio_id, datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001 - see comment above
        log.warning(
            "paper_engine.post_fill_breaker_check_failed",
            order_id=order.order_id,
            portfolio_id=order.portfolio_id,
            reason=str(exc),
        )

    payload = {
        "fill_id": fill_id,
        "order_id": order.order_id,
        "portfolio_id": order.portfolio_id,
        "instrument_id": order.instrument_id,
        "side": order.side.value,
        "quantity": str(decision.quantity),
        "price": str(decision.price),
        "tick_ts": decision.tick_ts.isoformat(),
        "status": status.value,
    }
    await redis.publish(f"fills:{order.portfolio_id}", json.dumps(payload))


async def _handle_tick_message(
    conn_factory: ConnFactory,
    redis: Redis,
    book: OpenOrderBook,
    raw: str,
    slippage_bps: Decimal,
) -> None:
    tick = _parse_tick(raw)
    if tick is None:
        return
    orders = book.open_orders.get(tick.instrument_id)
    if not orders:
        return
    for order in list(orders):
        if order.order_id not in book.order_index:
            # Removed from the book since this tick's snapshot was taken
            # -- almost always a `cancel` the control-channel consumer
            # (a separate task; control can yield between orders in this
            # loop) processed in between. Cheap, best-effort skip: this
            # can only see a cancel this process already knows about.
            # apply_fill's OrderNoLongerFillable guard is what closes the
            # race for a cancel that committed elsewhere but hasn't
            # reached this process yet.
            continue
        decision = decide_fill(order, tick.price, tick.ts, slippage_bps)
        if decision is None:
            continue
        try:
            await _process_fill(conn_factory, redis, book, order, decision)
        except Exception as exc:  # noqa: BLE001 - one order's failure must never block its
            # neighbours resting on the same instrument for the same tick.
            # CheckViolation and MissingChargeSchedule are already handled,
            # and terminally, inside _process_fill (the order is rejected
            # and removed from the book there) -- anything that reaches
            # here is unexpected and presumed transient, so the order is
            # left exactly as it was: still OPEN, still in the book, free
            # to retry on the next tick.
            log.warning(
                "paper_engine.fill_processing_failed", order_id=order.order_id, reason=str(exc)
            )


def _fetch_and_promote_order(conn: Connection, order_id: int) -> Order | None:
    """Fetch an order the control channel just told us about, promoting
    it out of `PENDING` (the API's initial status for every submitted
    order) the same way `load_open_orders` does at startup. Returns None
    if the order doesn't exist, or is already in a terminal state (e.g. a
    `cancel` raced ahead of this `new` in some already-broken publish
    order -- nothing to add to the book either way)."""
    row = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE order_id = %s", (order_id,)
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    order = _order_from_row(row)
    if order.status is OrderStatus.PENDING:
        updated = conn.execute(
            f"UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s"
            f" RETURNING {_ORDER_COLUMNS}",
            (OrderStatus.OPEN.value, order_id),
        ).fetchone()
        conn.commit()
        assert updated is not None
        return _order_from_row(updated)
    conn.commit()
    if order.status in _TERMINAL_ORDER_STATUSES:
        return None
    return order


def _handle_control_message(conn_factory: ConnFactory, book: OpenOrderBook, raw: str) -> None:
    try:
        message = json.loads(raw)
        action = str(message["action"])
        order_id = int(message["order_id"])
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("paper_engine.malformed_control_message", reason=str(exc), raw=raw[:200])
        return

    if action == "cancel":
        book.remove(order_id)
        return
    if action != "new":
        log.warning("paper_engine.unknown_control_action", action=action)
        return

    conn = conn_factory()
    try:
        order = _fetch_and_promote_order(conn, order_id)
        if order is None:
            return
        meta = book.instrument_meta.get(order.instrument_id)
        if meta is None:
            meta = _load_instrument_meta(conn, order.instrument_id)
    finally:
        conn.close()
    book.add(order, meta)


async def run_engine(
    redis: Redis,
    conn_factory: ConnFactory,
    *,
    slippage_bps: Decimal = Decimal("5"),
    sweep_check_seconds: float = 30.0,
    breaker_check_seconds: float = 5.0,
    reconcile_check_seconds: float = 5.0,
    sleep: Sleeper = _default_sleep,
    max_ticks: int | None = None,
    pattern: str = _TICK_PATTERN,
    control_channel: str = _CONTROL_CHANNEL,
) -> None:
    """Load resting orders, then react to `pattern` (`ticks:*` by default)
    and `control_channel` (`orders:control` by default) forever.

    Runs forever when `max_ticks` is None (production). Stops once
    `max_ticks` tick messages have been consumed (fill or no fill) when
    it's an int -- a test seam, the same shape as `crypto_ingestor.
    run_ingestion_loop`'s `max_ticks`.

    `reconcile_check_seconds` (default 5.0, matching `breaker_check_
    seconds`'s cadence -- IMP-1) governs `reconcile_missing_orders`'s
    periodic sweep, the backstop for a lost or too-early `orders:control`
    `new` message. See that function's docstring for why no source-level
    fix closes this gap alone.
    """
    validate_slippage_bps(slippage_bps)

    conn = conn_factory()
    try:
        book = load_open_orders(conn)
        startup_swept = sweep_expired_day_orders(conn, book, datetime.now(UTC))
    finally:
        conn.close()
    if startup_swept:
        log.info("paper_engine.startup_sweep", order_ids=startup_swept)
    log.info("paper_engine.starting", instrument_count=len(book.open_orders))

    processed = 0
    done = asyncio.Event()

    async def _consume_ticks(pubsub: PubSub) -> None:
        nonlocal processed
        async for message in pubsub.listen():
            if message["type"] != "pmessage":
                continue
            try:
                await _handle_tick_message(conn_factory, redis, book, message["data"], slippage_bps)
            except Exception as exc:  # noqa: BLE001 - one bad tick must never kill the loop
                log.warning("paper_engine.tick_handling_failed", reason=str(exc))
            processed += 1
            if max_ticks is not None and processed >= max_ticks:
                done.set()
                return

    async def _consume_control(pubsub: PubSub) -> None:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                _handle_control_message(conn_factory, book, message["data"])
            except Exception as exc:  # noqa: BLE001 - one bad control message must never kill the loop
                log.warning("paper_engine.control_handling_failed", reason=str(exc))
            if done.is_set():
                return

    async def _periodic_sweep() -> None:
        while not done.is_set():
            await sleep(sweep_check_seconds)
            sweep_conn = conn_factory()
            try:
                swept = sweep_expired_day_orders(sweep_conn, book, datetime.now(UTC))
                if swept:
                    log.info("paper_engine.session_sweep", order_ids=swept)
            except Exception as exc:  # noqa: BLE001 - a sweep failure must never kill the loop
                log.warning("paper_engine.sweep_failed", reason=str(exc))
            finally:
                sweep_conn.close()

    async def _periodic_breaker_check() -> None:
        while not done.is_set():
            await sleep(breaker_check_seconds)
            try:
                evaluate_breaker_for_all_active_portfolios(conn_factory, book, datetime.now(UTC))
            except Exception as exc:  # noqa: BLE001 - a breaker-check failure must never kill the loop
                log.warning("paper_engine.breaker_sweep_failed", reason=str(exc))

    async def _periodic_reconcile() -> None:
        while not done.is_set():
            await sleep(reconcile_check_seconds)
            reconcile_conn = conn_factory()
            try:
                adopted = reconcile_missing_orders(reconcile_conn, book)
                if adopted:
                    log.info("paper_engine.reconcile_adopted", order_ids=adopted)
            except Exception as exc:  # noqa: BLE001 - a reconcile failure must never kill the loop
                log.warning("paper_engine.reconcile_failed", reason=str(exc))
            finally:
                reconcile_conn.close()

    tick_pubsub = redis.pubsub()
    await tick_pubsub.psubscribe(pattern)
    control_pubsub = redis.pubsub()
    await control_pubsub.subscribe(control_channel)

    tick_task = asyncio.create_task(_consume_ticks(tick_pubsub))
    control_task = asyncio.create_task(_consume_control(control_pubsub))
    sweep_task = asyncio.create_task(_periodic_sweep())
    breaker_task = asyncio.create_task(_periodic_breaker_check())
    reconcile_task = asyncio.create_task(_periodic_reconcile())
    try:
        if max_ticks is None:
            await asyncio.gather(tick_task, control_task, sweep_task, breaker_task, reconcile_task)
        else:
            await done.wait()
    finally:
        tick_task.cancel()
        control_task.cancel()
        sweep_task.cancel()
        breaker_task.cancel()
        reconcile_task.cancel()
        try:
            await tick_pubsub.punsubscribe()
            await tick_pubsub.aclose()  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
            log.debug("paper_engine.tick_pubsub_cleanup_failed", exc_info=True)
        try:
            await control_pubsub.unsubscribe()
            await control_pubsub.aclose()  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
            log.debug("paper_engine.control_pubsub_cleanup_failed", exc_info=True)
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    log.info("paper_engine.starting_process")
    try:
        asyncio.run(
            run_engine(
                redis,
                lambda: psycopg.connect(settings.database_url, autocommit=False),
                slippage_bps=settings.paper_slippage_bps,
            )
        )
    except KeyboardInterrupt:
        log.info("paper_engine.interrupted")


if __name__ == "__main__":
    main()
