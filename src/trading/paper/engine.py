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

**An unaffordable fill is rejected, never retried.** `apply_fill` can
raise `psycopg.errors.CheckViolation` (`ck_no_negative_cash` or
`ck_no_negative_position`) because the API's submit-time cash check is
necessarily an estimate -- a market order's real fill price, and every
order's charges, are unknowable until the tick that fills it. When that
happens here, the failed transaction is rolled back, the order is marked
`REJECTED` with a `rejection_reason` in its own fresh commit, and it is
dropped from the in-memory book. Leaving it `OPEN` would retry on every
subsequent tick and live-lock the engine against a permanently
unaffordable order.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
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
from trading.paper.charges import compute_charges, load_schedules
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.fills import decide_fill
from trading.paper.ledger import apply_fill
from trading.paper.models import FillDecision, Order
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


def load_open_orders(conn: Connection) -> OpenOrderBook:
    """Startup load: promote every `PENDING` order to `OPEN` (a `PENDING`
    order is one the API accepted but this process hadn't picked up yet,
    whether because it crashed, restarted, or was simply slower to start
    than the API), then load every `OPEN`/`PARTIALLY_FILLED` order into a
    fresh `OpenOrderBook`, keyed by `instrument_id`.

    Commits its own writes: this is a one-shot startup read/write, not a
    step inside a fill's transaction boundary.
    """
    conn.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE status = %s",
        (OrderStatus.OPEN.value, OrderStatus.PENDING.value),
    )
    rows = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE status IN (%s, %s)",
        (OrderStatus.OPEN.value, OrderStatus.PARTIALLY_FILLED.value),
    ).fetchall()

    book = OpenOrderBook()
    for row in rows:
        order = _order_from_row(row)
        if order.instrument_id not in book.instrument_meta:
            book.instrument_meta[order.instrument_id] = _load_instrument_meta(
                conn, order.instrument_id
            )
        book.add(order)
    conn.commit()
    return book


def _session_close_utc(session_date: date, session_close: time) -> datetime:
    """`session_close` is a naive wall-clock time in Asia/Kolkata (the
    `trading_calendar` convention -- see `trading.calendar.trading_days`
    and `trading.recorder.__main__._session_close`), so it has to be
    localized before it's comparable to a tz-aware `now`."""
    return datetime.combine(session_date, session_close, tzinfo=_IST).astimezone(UTC)


def _is_session_closed(conn: Connection, exchange: str, segment: str, now: datetime) -> bool:
    """Whether `exchange`/`segment`'s session, as of `now`, is over.

    No `trading_calendar` row at all is treated as "not closed" -- an
    unknown calendar state must never be silently assumed to justify
    expiring a live order, mirroring `api.py`'s `_require_market_open`
    treating a missing row as closed-for-submission (the conservative
    direction differs because the actions differ: refusing a *new* order
    is safe to over-trigger, expiring an *existing* one is not)."""
    session_date = now.astimezone(_IST).date()
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
        if not _is_session_closed(conn, exchange, segment, now):
            continue
        for order in list(book.open_orders.get(instrument_id, [])):
            if order.time_in_force is not TimeInForce.DAY:
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


def _parse_tick(raw: str) -> Tick | None:
    try:
        return Tick.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("paper_engine.malformed_tick", reason=str(exc), raw=raw[:200])
        return None


def _rejection_reason(exc: CheckViolation) -> str:
    constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
    if constraint:
        return f"fill rejected: constraint {constraint} violated at fill time"
    return f"fill rejected: {exc}"


async def _process_fill(
    conn_factory: ConnFactory,
    redis: Redis,
    book: OpenOrderBook,
    order: Order,
    decision: FillDecision,
) -> None:
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
            charges = compute_charges(schedules, order.side, decision.quantity, decision.price)
            fill_id = apply_fill(conn, order, decision, charges)
            conn.commit()
        except CheckViolation as exc:
            conn.rollback()
            reason = _rejection_reason(exc)
            conn.execute(
                "UPDATE orders SET status = %s, rejection_reason = %s, updated_at = now()"
                " WHERE order_id = %s",
                (OrderStatus.REJECTED.value, reason, order.order_id),
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
        decision = decide_fill(order, tick.price, tick.ts, slippage_bps)
        if decision is None:
            continue
        await _process_fill(conn_factory, redis, book, order, decision)


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

    tick_pubsub = redis.pubsub()
    await tick_pubsub.psubscribe(pattern)
    control_pubsub = redis.pubsub()
    await control_pubsub.subscribe(control_channel)

    tick_task = asyncio.create_task(_consume_ticks(tick_pubsub))
    control_task = asyncio.create_task(_consume_control(control_pubsub))
    sweep_task = asyncio.create_task(_periodic_sweep())
    try:
        if max_ticks is None:
            await asyncio.gather(tick_task, control_task, sweep_task)
        else:
            await done.wait()
    finally:
        tick_task.cancel()
        control_task.cancel()
        sweep_task.cancel()
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
            )
        )
    except KeyboardInterrupt:
        log.info("paper_engine.interrupted")


if __name__ == "__main__":
    main()
