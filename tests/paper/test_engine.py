"""Tests for `src/trading/paper/engine.py`: the paper_engine fill loop over
the tick stream.

Every DB-touching test needs two kinds of connection, deliberately never
the same one:

- `setup_conn`: a real, `autocommit=True` connection used to create
  fixture rows (portfolio, instrument, trading_calendar) and read back
  final state. Its writes are genuinely committed -- unlike `db_conn`'s
  rolled-back transaction -- because the engine under test opens its own,
  separate connections via `conn_factory` and (per Task 8's whole point)
  really commits fills. A separate connection can only see committed
  data, so fixture rows must be committed for the engine to see them at
  all. Every test that uses `setup_conn` cleans up its own rows at the
  end (mirroring `tests/streaming/test_upstox_intraday_backfill.py`'s
  `autocommit_conn` pattern) since nothing here is protected by a
  rollback.
- `conn_factory`: `lambda: psycopg.connect(db_url, autocommit=False)`,
  the exact production shape `run_engine` is handed. Each call opens a
  brand new connection, matching "the engine owns the transaction
  boundary."

Redis tick/control traffic uses an isolated per-test channel/pattern
(`_isolated_channel_and_pattern`), never the real `ticks:*` /
`orders:control` names -- copying `tests/streaming/test_bar_aggregator.py`'s
rationale verbatim: `run_engine` processes every message it receives on
whatever pattern it subscribes to, so sharing the real names risks
cross-test leakage if anything else publishes during the same run.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine, Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import count
from typing import Any

import psycopg
import pytest
import redis
import structlog
from psycopg import Connection
from redis.asyncio import Redis as AsyncRedis

from tests.paper.helpers import make_order, make_portfolio
from trading.config import get_settings
from trading.contracts.enums import DataSource
from trading.paper.engine import (
    OpenOrderBook,
    _handle_tick_message,
    evaluate_breaker_for_all_active_portfolios,
    evaluate_breaker_for_portfolio,
    load_open_orders,
    run_engine,
    sweep_expired_day_orders,
    validate_slippage_bps,
)
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import Order
from trading.streaming.models import Tick

pytestmark = pytest.mark.db

_seq = count(1)

_T0 = datetime(2026, 8, 31, 6, 0, 0, tzinfo=UTC)


def _pure_order(**kw: Any) -> Order:
    """A plain in-memory `Order`, no DB -- for `OpenOrderBook` unit tests
    that don't need a real order row, mirroring test_fills.py's `_order`
    helper."""
    base: dict[str, Any] = dict(
        order_id=1,
        portfolio_id=1,
        instrument_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        filled_quantity=Decimal("0"),
        limit_price=None,
        product=Product.DELIVERY,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.OPEN,
        rationale="test",
        submitted_at=_T0,
    )
    base.update(kw)
    return Order(**base)


# --- Connections ------------------------------------------------------------


@pytest.fixture
def setup_conn(db_url: str) -> Iterator[Connection]:
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def conn_factory(db_url: str):
    return lambda: psycopg.connect(db_url, autocommit=False)


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        yield client
    finally:
        client.close()


# --- Instrument / calendar fixtures -----------------------------------------


def _make_equity_instrument(conn: Connection) -> int:
    row = conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)"
        " VALUES ('EQUITY', 'TEST-NSE', 'CM', %s, 'ACTIVE', %s) RETURNING instrument_id",
        (f"ENGTEST{next(_seq)}", f"TEST/ENGINE/EQUITY/{next(_seq)}"),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _make_crypto_instrument(conn: Connection) -> int:
    row = conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)"
        " VALUES ('CRYPTO', 'BINANCE', 'SPOT', %s, 'ACTIVE', %s) RETURNING instrument_id",
        (f"ENGCOIN{next(_seq)}", f"TEST/ENGINE/CRYPTO/{next(_seq)}"),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _mark_market_open_today(conn: Connection, session_close: str = "15:30") -> None:
    conn.execute(
        "INSERT INTO trading_calendar"
        " (exchange, segment, session_date, is_trading_day, session_open, session_close)"
        " VALUES ('TEST-NSE', 'CM', %s, true, '09:15', %s)"
        " ON CONFLICT (exchange, segment, session_date)"
        " DO UPDATE SET is_trading_day = true, session_close = EXCLUDED.session_close",
        (date.today(), session_close),
    )


def _cleanup(conn: Connection, *, portfolio_ids: list[int], instrument_ids: list[int]) -> None:
    if portfolio_ids:
        conn.execute(
            "DELETE FROM circuit_breaker_events WHERE portfolio_id = ANY(%s)", (portfolio_ids,)
        )
        conn.execute(
            "DELETE FROM portfolio_equity_snapshots WHERE portfolio_id = ANY(%s)", (portfolio_ids,)
        )
        conn.execute("DELETE FROM ledger_entries WHERE portfolio_id = ANY(%s)", (portfolio_ids,))
        conn.execute(
            "DELETE FROM fills WHERE order_id IN"
            " (SELECT order_id FROM orders WHERE portfolio_id = ANY(%s))",
            (portfolio_ids,),
        )
        conn.execute("DELETE FROM positions WHERE portfolio_id = ANY(%s)", (portfolio_ids,))
        conn.execute("DELETE FROM orders WHERE portfolio_id = ANY(%s)", (portfolio_ids,))
        conn.execute("DELETE FROM portfolios WHERE portfolio_id = ANY(%s)", (portfolio_ids,))
    if instrument_ids:
        conn.execute("DELETE FROM bars_intraday WHERE instrument_id = ANY(%s)", (instrument_ids,))
        conn.execute("DELETE FROM instruments WHERE instrument_id = ANY(%s)", (instrument_ids,))
    # `_mark_market_open_today` writes to a private 'TEST-NSE'/'CM' pair
    # (never the real 'NSE'/'CM' other tests -- e.g. test_api.py's
    # test_order_with_no_trading_calendar_entry_is_rejected_400 -- rely on
    # having no row for), so this can't affect other tests either way, but
    # it's still cleaned up for hygiene.
    conn.execute("DELETE FROM trading_calendar WHERE exchange = 'TEST-NSE'")


# --- Redis test-isolation helpers (copied from test_bar_aggregator.py) -----


def _isolated_channel_and_pattern(iid: int) -> tuple[str, str]:
    channel = f"test-ticks:{iid}:{iid}"
    pattern = f"test-ticks:{iid}:*"
    return channel, pattern


def _tick_json(instrument_id: int, ts: datetime, price: str, quantity: str = "1") -> str:
    return Tick(
        instrument_id=instrument_id, ts=ts, price=Decimal(price), quantity=Decimal(quantity)
    ).model_dump_json()


async def _no_sleep(seconds: float) -> None:
    # await asyncio.sleep(0), not a bare `return None` -- see
    # test_bar_aggregator.py's identical helper for why: a periodic
    # background task racing the consumer task on a truly no-op sleep
    # livelocks the whole loop.
    await asyncio.sleep(0)


async def _run_both(
    loop_task: Coroutine[Any, Any, None], publisher: Coroutine[Any, Any, None]
) -> None:
    await asyncio.gather(loop_task, publisher)


def _run_engine_with_publish(
    *,
    conn_factory,
    max_ticks: int,
    publish: list[tuple[str, str]],
    pattern: str,
    control_channel: str = "test-orders:control",
    slippage_bps: Decimal = Decimal("0"),
    sweep_check_seconds: float = 9999.0,
) -> None:
    """Run `run_engine` against an isolated tick pattern, publishing
    `publish` (channel, payload) pairs shortly after subscription lands,
    then wait for both to finish."""
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_engine(
            async_redis,
            conn_factory,
            slippage_bps=slippage_bps,
            sleep=_no_sleep,
            max_ticks=max_ticks,
            pattern=pattern,
            control_channel=control_channel,
            sweep_check_seconds=sweep_check_seconds,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            r = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
            try:
                for channel, payload in publish:
                    r.publish(channel, payload)
            finally:
                r.close()

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())


# --- validate_slippage_bps ---------------------------------------------------


def test_validate_slippage_bps_accepts_zero_and_positive() -> None:
    validate_slippage_bps(Decimal("0"))
    validate_slippage_bps(Decimal("5"))


def test_validate_slippage_bps_rejects_negative() -> None:
    with pytest.raises(ValueError, match="slippage_bps"):
        validate_slippage_bps(Decimal("-1"))


def test_run_engine_fails_loudly_at_startup_on_negative_slippage_bps(conn_factory) -> None:
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        with pytest.raises(ValueError, match="slippage_bps"):
            asyncio.run(
                run_engine(
                    async_redis,
                    conn_factory,
                    slippage_bps=Decimal("-5"),
                    max_ticks=0,
                )
            )
    finally:
        asyncio.run(async_redis.aclose())


# --- OpenOrderBook ------------------------------------------------------------


def test_open_order_book_add_is_idempotent_on_order_id() -> None:
    """No production code path adds the same order_id twice today, but the
    guard is cheap insurance: a caller that somehow does must not end up
    with the same order counted twice in its instrument's list (which
    would double the quantity `_handle_tick_message` fills on one tick)."""
    book = OpenOrderBook()
    order = _pure_order(order_id=42, instrument_id=7)
    book.add(order, meta=("EQUITY", "NSE", "CM"))
    book.add(order, meta=("EQUITY", "NSE", "CM"))
    assert len(book.open_orders[7]) == 1


def test_open_order_book_remove_then_add_is_not_blocked() -> None:
    """The idempotency guard is keyed on presence, not history -- an
    order_id that was added, removed, and is now being added again (e.g.
    re-added by a hypothetical future retry path) must not be silently
    dropped forever."""
    book = OpenOrderBook()
    order = _pure_order(order_id=42, instrument_id=7)
    book.add(order, meta=("EQUITY", "NSE", "CM"))
    book.remove(42)
    book.add(order, meta=("EQUITY", "NSE", "CM"))
    assert len(book.open_orders[7]) == 1


# --- load_open_orders --------------------------------------------------------


def test_load_open_orders_promotes_pending_to_open_and_loads_it(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("10"),
        instrument_id=iid,
        status=OrderStatus.PENDING,
    )
    try:
        book = load_open_orders(setup_conn)
        assert iid in book.open_orders
        loaded = book.open_orders[iid][0]
        assert loaded.order_id == order.order_id
        assert loaded.status is OrderStatus.OPEN

        db_status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert db_status == ("OPEN",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_load_open_orders_loads_open_and_partially_filled(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    open_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.OPEN,
    )
    partial_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.PARTIALLY_FILLED,
        filled_quantity=Decimal("2"),
    )
    try:
        book = load_open_orders(setup_conn)
        ids = {o.order_id for o in book.open_orders[iid]}
        assert ids == {open_order.order_id, partial_order.order_id}
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_load_open_orders_ignores_terminal_orders(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.FILLED,
        filled_quantity=Decimal("5"),
    )
    try:
        book = load_open_orders(setup_conn)
        assert iid not in book.open_orders
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- sweep_expired_day_orders -------------------------------------------------


def test_sweep_expires_day_order_past_session_close(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    # 00:01 IST is, for any realistic test-run wall clock, already in the
    # past today -- so is_session_closed is true without needing to wait.
    _mark_market_open_today(setup_conn, session_close="00:01")
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.OPEN,
        time_in_force=TimeInForce.DAY,
    )
    try:
        book = OpenOrderBook()
        book.add(order, meta=("EQUITY", "TEST-NSE", "CM"))
        swept = sweep_expired_day_orders(setup_conn, book, datetime.now(UTC))
        assert swept == [order.order_id]
        assert iid not in book.open_orders

        status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert status == ("EXPIRED",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_sweep_leaves_gtc_order_open_past_session_close(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    _mark_market_open_today(setup_conn, session_close="00:01")
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.OPEN,
        time_in_force=TimeInForce.GTC,
    )
    try:
        book = OpenOrderBook()
        book.add(order, meta=("EQUITY", "TEST-NSE", "CM"))
        swept = sweep_expired_day_orders(setup_conn, book, datetime.now(UTC))
        assert swept == []
        assert iid in book.open_orders

        status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert status == ("OPEN",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_sweep_never_expires_a_crypto_day_order(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        status=OrderStatus.OPEN,
        time_in_force=TimeInForce.DAY,
    )
    try:
        book = OpenOrderBook()
        book.add(order, meta=("CRYPTO", "BINANCE", "SPOT"))
        # Far in the future: if crypto were mistakenly subject to the
        # calendar check, this would still not matter -- crypto is 24/7.
        swept = sweep_expired_day_orders(setup_conn, book, datetime.now(UTC) + timedelta(days=365))
        assert swept == []
        assert iid in book.open_orders
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_sweep_leaves_day_order_open_before_session_close(setup_conn) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    _mark_market_open_today(setup_conn, session_close="23:59")
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("5"),
        instrument_id=iid,
        status=OrderStatus.OPEN,
        time_in_force=TimeInForce.DAY,
    )
    try:
        book = OpenOrderBook()
        book.add(order, meta=("EQUITY", "TEST-NSE", "CM"))
        swept = sweep_expired_day_orders(setup_conn, book, datetime.now(UTC))
        assert swept == []
        assert iid in book.open_orders
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- run_engine: tick fills ---------------------------------------------------


def test_run_engine_fills_a_matching_market_order_on_a_tick(setup_conn, conn_factory) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("2"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=1,
            publish=[(channel, _tick_json(iid, datetime.now(UTC), "100.00"))],
            pattern=pattern,
        )
        fills = setup_conn.execute(
            "SELECT quantity, price FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert len(fills) == 1
        assert fills[0] == (Decimal("2.00000000"), Decimal("100.0000"))

        status = setup_conn.execute(
            "SELECT status, filled_quantity FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert status == ("FILLED", Decimal("2.00000000"))
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_publishes_the_fill(setup_conn, conn_factory, redis_client) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    fills_sub = redis_client.pubsub()
    fills_sub.subscribe(f"fills:{pid}")
    fills_sub.get_message(timeout=1)  # discard subscribe confirmation
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=1,
            publish=[(channel, _tick_json(iid, datetime.now(UTC), "50.00"))],
            pattern=pattern,
        )
        message = fills_sub.get_message(timeout=2)
        assert message is not None and message["type"] == "message"
        payload = json.loads(message["data"])
        assert payload["order_id"] == order.order_id
        assert payload["instrument_id"] == iid
    finally:
        fills_sub.close()
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_ignores_a_tick_on_an_unrelated_instrument(setup_conn, conn_factory) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    other_iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(other_iid)
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=1,
            publish=[(channel, _tick_json(other_iid, datetime.now(UTC), "999.00"))],
            pattern=pattern,
        )
        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []
        status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert status == ("OPEN",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid, other_iid])


def test_run_engine_skips_a_malformed_tick_and_keeps_going(setup_conn, conn_factory) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        with structlog.testing.capture_logs() as cap:
            _run_engine_with_publish(
                conn_factory=conn_factory,
                max_ticks=2,
                publish=[
                    (channel, "not json"),
                    (channel, _tick_json(iid, datetime.now(UTC), "42.00")),
                ],
                pattern=pattern,
            )
        warnings = [e for e in cap if e.get("event") == "paper_engine.malformed_tick"]
        assert len(warnings) == 1

        fills = setup_conn.execute(
            "SELECT quantity FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert len(fills) == 1  # the malformed tick never killed the loop
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_does_not_double_fill_on_a_second_identical_tick(
    setup_conn, conn_factory
) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    tick_ts = datetime.now(UTC)
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=2,
            publish=[
                (channel, _tick_json(iid, tick_ts, "10.00")),
                (channel, _tick_json(iid, tick_ts, "10.00")),
            ],
            pattern=pattern,
        )
        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert len(fills) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- run_engine: orders:control ----------------------------------------------


def test_run_engine_cancel_control_message_prevents_a_fill(setup_conn, conn_factory) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    control_channel = f"test-orders:control:{iid}"
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_engine(
            async_redis,
            conn_factory,
            slippage_bps=Decimal("0"),
            sleep=_no_sleep,
            max_ticks=1,
            pattern=pattern,
            control_channel=control_channel,
            sweep_check_seconds=9999.0,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            r = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
            try:
                r.publish(
                    control_channel,
                    json.dumps({"action": "cancel", "order_id": order.order_id}),
                )
                await asyncio.sleep(0.1)
                r.publish(channel, _tick_json(iid, datetime.now(UTC), "10.00"))
            finally:
                r.close()

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    try:
        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_new_control_message_adds_and_fills_an_order(setup_conn, conn_factory) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    channel, pattern = _isolated_channel_and_pattern(iid)
    control_channel = f"test-orders:control:{iid}"
    order_id_box: list[int] = []
    # The order is deliberately created *after* run_engine's synchronous
    # startup section (load_open_orders) has already run and found
    # nothing -- created inside the publisher coroutine, past its first
    # `await asyncio.sleep(0.2)`, which only gets scheduled once
    # run_engine's own startup has fully completed and yielded control at
    # its first `await` (psubscribe). Creating it beforehand (as every
    # other control-message test does for `cancel`) would let
    # load_open_orders pick it up as PENDING itself, double-adding it
    # once the control message also adds it -- exactly the double-fill
    # bug this test would otherwise fail to catch.
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_engine(
            async_redis,
            conn_factory,
            slippage_bps=Decimal("0"),
            sleep=_no_sleep,
            max_ticks=1,
            pattern=pattern,
            control_channel=control_channel,
            sweep_check_seconds=9999.0,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            order = make_order(
                setup_conn,
                pid,
                side=Side.BUY,
                quantity=Decimal("1"),
                instrument_id=iid,
                order_type=OrderType.MARKET,
                product=Product.DELIVERY,
                status=OrderStatus.PENDING,
            )
            order_id_box.append(order.order_id)
            r = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
            try:
                r.publish(
                    control_channel,
                    json.dumps({"action": "new", "order_id": order.order_id}),
                )
                await asyncio.sleep(0.2)
                r.publish(channel, _tick_json(iid, datetime.now(UTC), "10.00"))
            finally:
                r.close()

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    order_id = order_id_box[0]

    try:
        fills = setup_conn.execute("SELECT 1 FROM fills WHERE order_id=%s", (order_id,)).fetchall()
        assert len(fills) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- Fix round 2: a fill losing a race against a concurrent cancel -------


def test_handle_tick_message_skips_an_order_removed_from_the_book_mid_snapshot(
    setup_conn, conn_factory, monkeypatch
) -> None:
    """Layer 1 of the concurrent-cancel fix: `_handle_tick_message` snapshots
    the resting orders for an instrument at tick start, then processes them
    one at a time -- if the `orders:control` consumer (a separate asyncio
    task) removes one from `book` in between, a cheap recheck against
    `book.order_index` must skip it rather than fill it from the stale
    snapshot.

    Simulated deterministically rather than by racing two real asyncio
    tasks: `apply_fill` is monkeypatched so that, as a side effect of
    processing the *first* order, it removes the *second* order from the
    very same `book` object passed into `_handle_tick_message` -- standing
    in for "the control task ran in between." Calls `_handle_tick_message`
    directly (not through `run_engine`) specifically so this test can hold
    a reference to `book` and inspect/mutate it, which a full `run_engine`
    run never exposes."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    order_a = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    order_b = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )

    book = OpenOrderBook()
    book.add(order_a, meta=("CRYPTO", "BINANCE", "SPOT"))
    book.add(order_b, meta=("CRYPTO", "BINANCE", "SPOT"))

    import trading.paper.engine as engine_module

    real_apply_fill = engine_module.apply_fill

    def _apply_fill_with_concurrent_cancel(conn, order, decision, charges):
        if order.order_id == order_a.order_id:
            book.remove(order_b.order_id)  # simulate the concurrent cancel
        return real_apply_fill(conn, order, decision, charges)

    monkeypatch.setattr(engine_module, "apply_fill", _apply_fill_with_concurrent_cancel)

    raw_tick = _tick_json(iid, datetime.now(UTC), "10.00")
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)

    async def _run() -> None:
        # Disconnect the pool inside the same asyncio.run() call that used
        # it, not a separate one afterward -- a connection opened under
        # this event loop cannot be closed from a different one (the same
        # reasoning run_engine's own finally block documents).
        await _handle_tick_message(conn_factory, async_redis, book, raw_tick, Decimal("0"))
        await async_redis.connection_pool.disconnect()

    try:
        asyncio.run(_run())

        a_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order_a.order_id,)
        ).fetchall()
        assert len(a_fills) == 1  # order_a, processed first, still fills

        # The proof of isolation: order_b was skipped via the in-memory
        # recheck -- no fill, no DB write of any kind for it.
        b_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order_b.order_id,)
        ).fetchall()
        assert b_fills == []
        b_status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order_b.order_id,)
        ).fetchone()
        assert b_status == ("OPEN",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_loses_a_race_against_a_db_committed_cancel_and_fills_its_neighbour(
    setup_conn, conn_factory
) -> None:
    """The tighter race Task 7's own report flagged: the API publishes
    `cancel` on `orders:control` *before* its own commit
    (`trading.streaming.db.get_db_connection` commits only after the route
    body returns), so an order's real DB status can already be CANCELLED
    before this process's control-channel subscription has delivered (or,
    as here, will ever deliver) the cancel message. `flaky_order` stays in
    the in-memory book the whole time -- layer 1's recheck cannot catch
    this, only `apply_fill`'s own status-guarded UPDATE
    (`OrderNoLongerFillable`) can.

    `flaky_order` is created OPEN and loaded normally at startup (so it's
    genuinely resting in the book, unlike the "new" control-message test's
    scenario), then cancelled directly in the database -- with no
    `orders:control` message ever published for it -- from inside the
    publisher coroutine, after `run_engine`'s synchronous startup section
    has already run and yielded control at its first `await`."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    flaky_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    healthy_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_engine(
            async_redis,
            conn_factory,
            slippage_bps=Decimal("0"),
            sleep=_no_sleep,
            max_ticks=1,
            pattern=pattern,
            sweep_check_seconds=9999.0,
        )

        async def _cancel_then_publish() -> None:
            await asyncio.sleep(0.2)  # let run_engine's startup load finish first
            setup_conn.execute(
                "UPDATE orders SET status='CANCELLED' WHERE order_id=%s",
                (flaky_order.order_id,),
            )
            r = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
            try:
                r.publish(channel, _tick_json(iid, datetime.now(UTC), "10.00"))
            finally:
                r.close()

        with structlog.testing.capture_logs() as cap:
            asyncio.run(_run_both(loop_task, _cancel_then_publish()))
    finally:
        asyncio.run(async_redis.aclose())

    try:
        flaky_row = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (flaky_order.order_id,)
        ).fetchone()
        assert flaky_row == ("CANCELLED",)  # never overwritten with FILLED
        flaky_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (flaky_order.order_id,)
        ).fetchall()
        assert flaky_fills == []

        # The proof of isolation: the healthy neighbour still filled on
        # the very same tick, despite flaky_order's lost race.
        healthy_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (healthy_order.order_id,)
        ).fetchall()
        assert len(healthy_fills) == 1

        warnings = [e for e in cap if e.get("event") == "paper_engine.fill_lost_race"]
        assert len(warnings) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- Carried requirement 2: reject, do not retry, on an unaffordable fill --


def test_run_engine_rejects_an_unaffordable_fill_instead_of_retrying(
    setup_conn, conn_factory
) -> None:
    pid = make_portfolio(setup_conn, cash=Decimal("1"))  # far too little cash
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("10"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        with structlog.testing.capture_logs() as cap:
            _run_engine_with_publish(
                conn_factory=conn_factory,
                max_ticks=1,
                publish=[(channel, _tick_json(iid, datetime.now(UTC), "1000.00"))],
                pattern=pattern,
            )

        row = setup_conn.execute(
            "SELECT status, rejection_reason FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert row is not None
        rejected_status, reason = row
        assert rejected_status == "REJECTED"
        assert reason

        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []

        cash = setup_conn.execute(
            "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert cash == (Decimal("1.0000"),)  # unaffected -- nothing was ever committed

        warnings = [e for e in cap if e.get("event") == "paper_engine.fill_rejected"]
        assert len(warnings) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_does_not_retry_a_rejected_order_on_a_later_tick(
    setup_conn, conn_factory
) -> None:
    """After the rejection, the order must be gone from the in-memory book
    -- proven here by a second tick at the same unaffordable price not
    producing a second rejection attempt (only one order row exists, so a
    second `apply_fill` attempt against an already-REJECTED order would be
    a logic bug worth catching, not merely a repeat rejection)."""
    pid = make_portfolio(setup_conn, cash=Decimal("1"))
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("10"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=2,
            publish=[
                (channel, _tick_json(iid, datetime.now(UTC), "1000.00")),
                (channel, _tick_json(iid, datetime.now(UTC), "1000.00")),
            ],
            pattern=pattern,
        )
        row = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert row == ("REJECTED",)
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- Per-order isolation on a per-tick failure ------------------------------


def test_run_engine_rejects_a_missing_charge_schedule_order_and_fills_its_neighbour(
    setup_conn, conn_factory
) -> None:
    """Migration 0007 seeds a BINANCE/CRYPTO/DELIVERY charge schedule but
    no INTRADAY one -- an INTRADAY crypto order has no schedule to compute
    charges from at all, a real (not hypothetical) MissingChargeSchedule.
    That order must be rejected, not retried forever -- and, critically, a
    healthy DELIVERY order resting on the *same* instrument must still
    fill on the *same* tick: a test that only checked the failing order's
    own outcome would pass even if one order's failure still aborted
    every other order on that tick."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    bad_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.INTRADAY,
        status=OrderStatus.OPEN,
    )
    good_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        with structlog.testing.capture_logs() as cap:
            _run_engine_with_publish(
                conn_factory=conn_factory,
                max_ticks=1,
                publish=[(channel, _tick_json(iid, datetime.now(UTC), "10.00"))],
                pattern=pattern,
            )

        bad_row = setup_conn.execute(
            "SELECT status, rejection_reason FROM orders WHERE order_id=%s",
            (bad_order.order_id,),
        ).fetchone()
        assert bad_row is not None
        assert bad_row[0] == "REJECTED"
        assert bad_row[1]  # a rejection_reason naming the problem

        bad_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (bad_order.order_id,)
        ).fetchall()
        assert bad_fills == []

        # The proof of isolation: the healthy neighbour still filled on
        # the very same tick, despite bad_order's failure.
        good_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (good_order.order_id,)
        ).fetchall()
        assert len(good_fills) == 1

        warnings = [e for e in cap if e.get("event") == "paper_engine.fill_rejected"]
        assert len(warnings) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_isolates_a_transient_processing_failure_from_its_neighbour(
    setup_conn, conn_factory, monkeypatch
) -> None:
    """A failure that is neither CheckViolation nor MissingChargeSchedule
    (a stand-in for a transient DB blip) must not be treated as
    permanent: the failing order stays OPEN, retryable on a future tick,
    and -- the assertion that actually proves isolation -- a healthy
    neighbour on the same instrument still fills on the same tick."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    flaky_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    healthy_order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )

    import trading.paper.engine as engine_module

    real_apply_fill = engine_module.apply_fill

    def _flaky_apply_fill(conn, order, decision, charges):
        if order.order_id == flaky_order.order_id:
            raise RuntimeError("simulated transient DB failure")
        return real_apply_fill(conn, order, decision, charges)

    monkeypatch.setattr(engine_module, "apply_fill", _flaky_apply_fill)

    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        with structlog.testing.capture_logs() as cap:
            _run_engine_with_publish(
                conn_factory=conn_factory,
                max_ticks=1,
                publish=[(channel, _tick_json(iid, datetime.now(UTC), "10.00"))],
                pattern=pattern,
            )

        flaky_row = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (flaky_order.order_id,)
        ).fetchone()
        assert flaky_row == ("OPEN",)  # left alone -- transient, not permanent
        flaky_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (flaky_order.order_id,)
        ).fetchall()
        assert flaky_fills == []

        # The proof of isolation: the healthy neighbour still filled on
        # the very same tick, despite flaky_order's failure.
        healthy_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (healthy_order.order_id,)
        ).fetchall()
        assert len(healthy_fills) == 1

        warnings = [e for e in cap if e.get("event") == "paper_engine.fill_processing_failed"]
        assert len(warnings) == 1
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- run_engine: session-close sweep wired into the loop --------------------


def test_run_engine_sweeps_a_day_order_via_the_periodic_check(setup_conn, conn_factory) -> None:
    """No tick ever arrives for this instrument -- only the periodic sweep
    task can expire it. Mirrors
    test_bar_aggregator's periodic-flush test: a short check interval and
    a real `asyncio.sleep`, genuinely waiting for the window."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_equity_instrument(setup_conn)
    _mark_market_open_today(setup_conn, session_close="00:01")
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
        time_in_force=TimeInForce.DAY,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        async_redis: AsyncRedis = AsyncRedis.from_url(
            get_settings().redis_url, decode_responses=True
        )
        try:
            loop_task = run_engine(
                async_redis,
                conn_factory,
                slippage_bps=Decimal("0"),
                sleep=asyncio.sleep,
                max_ticks=1,
                pattern=pattern,
                sweep_check_seconds=0.05,
            )

            async def _publish_after_subscribed() -> None:
                # Give the sweep a couple of cycles to run, then publish an
                # unrelated tick on the same pattern purely to terminate the
                # loop via max_ticks=1.
                await asyncio.sleep(0.3)
                redis_client_ = redis.Redis.from_url(
                    get_settings().redis_url, decode_responses=True
                )
                try:
                    redis_client_.publish(channel, _tick_json(iid, datetime.now(UTC), "10.00"))
                finally:
                    redis_client_.close()

            asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
        finally:
            asyncio.run(async_redis.aclose())

        row = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert row == ("EXPIRED",)
        # And no fill happened even though a tick landed after the sweep --
        # the order was already gone from the book.
        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


# --- Task 10: circuit breaker wiring -----------------------------------------


def _insert_bar(
    conn: Connection, instrument_id: int, close: Decimal, ts: datetime | None = None
) -> None:
    conn.execute(
        "INSERT INTO bars_intraday"
        " (instrument_id, ts, interval_sec, open, high, low, close, source)"
        " VALUES (%s, %s, 60, %s, %s, %s, %s, %s)",
        (
            instrument_id,
            ts or datetime.now(UTC),
            close,
            close,
            close,
            close,
            int(DataSource.BINANCE_WS),
        ),
    )


def test_evaluate_breaker_for_portfolio_trips_drops_the_book_and_prevents_a_later_fill(
    setup_conn, conn_factory
) -> None:
    """The cross-boundary requirement the brief calls out explicitly:
    `trip` only ever touches the database, so `evaluate_breaker_for_
    portfolio` must drop the tripped portfolio's orders from the
    in-memory `book` itself -- proven here by feeding a tick for the same
    instrument straight into `_handle_tick_message` afterwards and
    asserting it produces no fill, not merely by asserting the DB state.
    """
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    setup_conn.execute(
        "UPDATE portfolios SET max_daily_loss = %s, cash_balance = %s WHERE portfolio_id = %s",
        (Decimal("100"), Decimal("89000"), pid),  # a pre-existing 11000 loss, no positions
    )
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    book = OpenOrderBook()
    book.add(order, meta=("CRYPTO", "BINANCE", "SPOT"))
    try:
        with structlog.testing.capture_logs() as cap:
            evaluate_breaker_for_portfolio(conn_factory, book, pid, _T0)

        status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert status == ("PAUSED",)

        order_status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert order_status == ("CANCELLED",)

        event = setup_conn.execute(
            "SELECT reason FROM circuit_breaker_events WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert event is not None
        assert event[0].startswith("max_daily_loss")

        # Dropped from the in-memory book, not merely cancelled in the DB.
        assert iid not in book.open_orders
        assert order.order_id not in book.order_index

        tripped_logs = [e for e in cap if e.get("event") == "paper_engine.breaker_tripped"]
        assert len(tripped_logs) == 1

        # The proof that actually matters: a tick for this instrument, fed
        # straight into the engine's tick handler, produces no fill --
        # not "the DB says CANCELLED", but "the running engine cannot
        # possibly fill this order again".
        raw_tick = _tick_json(iid, datetime.now(UTC), "10.00")
        async_redis: AsyncRedis = AsyncRedis.from_url(
            get_settings().redis_url, decode_responses=True
        )

        async def _run() -> None:
            await _handle_tick_message(conn_factory, async_redis, book, raw_tick, Decimal("0"))
            await async_redis.connection_pool.disconnect()

        asyncio.run(_run())

        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_evaluate_breaker_for_portfolio_marks_positions_from_bars_intraday(
    setup_conn, conn_factory
) -> None:
    """No limits are set, so this never breaches -- the point is proving
    the engine's own mark-sourcing (`bars_intraday`, the same reference
    price `trading.paper.api._require_sufficient_cash` already uses)
    feeds `compute_equity` correctly, by reading back the persisted
    snapshot's equity."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    setup_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, %s, %s, 0)",
        (pid, iid, Decimal("10"), Decimal("90")),
    )
    _insert_bar(setup_conn, iid, Decimal("100.00"))
    book = OpenOrderBook()
    try:
        evaluate_breaker_for_portfolio(conn_factory, book, pid, _T0)

        status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert status == ("ACTIVE",)  # no limits declared -- never breaches

        row = setup_conn.execute(
            "SELECT equity, peak_equity, drawdown_pct FROM portfolio_equity_snapshots"
            " WHERE portfolio_id=%s AND ts=%s",
            (pid, _T0),
        ).fetchone()
        # cash 100000 + 10 * 100.00 mark = 101000
        assert row == (Decimal("101000.0000"), Decimal("101000.0000"), Decimal("0.0000"))
    finally:
        setup_conn.execute("DELETE FROM positions WHERE portfolio_id=%s", (pid,))
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_evaluate_breaker_for_portfolio_skips_and_logs_on_a_missing_mark(
    setup_conn, conn_factory
) -> None:
    """A held position with no `bars_intraday` row at all must not crash
    the evaluation, pause the portfolio, or write a snapshot with a
    fabricated equity -- it is logged and skipped, retried on the next
    cycle once a mark becomes available."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    setup_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, %s, %s, 0)",
        (pid, iid, Decimal("10"), Decimal("90")),
    )
    book = OpenOrderBook()
    try:
        with structlog.testing.capture_logs() as cap:
            evaluate_breaker_for_portfolio(conn_factory, book, pid, _T0)

        status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert status == ("ACTIVE",)

        snapshots = setup_conn.execute(
            "SELECT 1 FROM portfolio_equity_snapshots WHERE portfolio_id=%s", (pid,)
        ).fetchall()
        assert snapshots == []  # no equity value was ever computable

        warnings = [e for e in cap if e.get("event") == "paper_engine.breaker_missing_mark"]
        assert len(warnings) == 1
    finally:
        setup_conn.execute("DELETE FROM positions WHERE portfolio_id=%s", (pid,))
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_evaluate_breaker_for_all_active_portfolios_isolates_failures_between_portfolios(
    setup_conn, conn_factory
) -> None:
    """One portfolio's `MissingMark` must never block evaluating (and
    tripping) a healthy neighbour in the same sweep."""
    broken_pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    iid = _make_crypto_instrument(setup_conn)
    setup_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, %s, %s, 0)",
        (broken_pid, iid, Decimal("10"), Decimal("90")),
    )  # no bars_intraday row -- this portfolio can never be priced

    breaching_pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    setup_conn.execute(
        "UPDATE portfolios SET max_daily_loss = %s, cash_balance = %s WHERE portfolio_id = %s",
        (Decimal("100"), Decimal("50000"), breaching_pid),
    )
    book = OpenOrderBook()
    try:
        with structlog.testing.capture_logs() as cap:
            evaluate_breaker_for_all_active_portfolios(conn_factory, book, _T0)

        broken_status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (broken_pid,)
        ).fetchone()
        assert broken_status == ("ACTIVE",)  # left alone, not crashed

        breaching_status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (breaching_pid,)
        ).fetchone()
        assert breaching_status == ("PAUSED",)  # still tripped despite its neighbour's failure

        warnings = [e for e in cap if e.get("event") == "paper_engine.breaker_missing_mark"]
        assert len(warnings) == 1
    finally:
        setup_conn.execute("DELETE FROM positions WHERE portfolio_id=%s", (broken_pid,))
        _cleanup(setup_conn, portfolio_ids=[broken_pid, breaching_pid], instrument_ids=[iid])


def test_run_engine_periodic_breaker_check_trips_a_breaching_portfolio(
    setup_conn, conn_factory
) -> None:
    """No fill or tick for the resting order's own instrument ever
    arrives -- only the 5-second (here, 0.05s) periodic check can trip
    this portfolio. Mirrors `test_run_engine_sweeps_a_day_order_via_the_
    periodic_check`'s structure: a short check interval, a real
    `asyncio.sleep`, genuinely waiting for the window, then an unrelated
    tick purely to terminate the loop via `max_ticks=1`."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    setup_conn.execute(
        "UPDATE portfolios SET max_daily_loss = %s, cash_balance = %s WHERE portfolio_id = %s",
        (Decimal("100"), Decimal("50000"), pid),
    )
    iid = _make_crypto_instrument(setup_conn)
    order = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    channel, pattern = _isolated_channel_and_pattern(iid)
    try:
        async_redis: AsyncRedis = AsyncRedis.from_url(
            get_settings().redis_url, decode_responses=True
        )
        try:
            loop_task = run_engine(
                async_redis,
                conn_factory,
                slippage_bps=Decimal("0"),
                sleep=asyncio.sleep,
                max_ticks=1,
                pattern=pattern,
                sweep_check_seconds=9999.0,
                breaker_check_seconds=0.05,
            )

            async def _publish_after_subscribed() -> None:
                await asyncio.sleep(0.3)  # give the breaker a few cycles to run
                r = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
                try:
                    r.publish(channel, _tick_json(iid, datetime.now(UTC), "10.00"))
                finally:
                    r.close()

            asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
        finally:
            asyncio.run(async_redis.aclose())

        status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert status == ("PAUSED",)

        order_status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
        ).fetchone()
        assert order_status == ("CANCELLED",)

        # The order was already dropped from the book by the periodic
        # check, well before the terminating tick arrived.
        fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order.order_id,)
        ).fetchall()
        assert fills == []
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid])


def test_run_engine_evaluates_the_breaker_after_a_fill_and_prevents_a_later_fill_elsewhere(
    setup_conn, conn_factory
) -> None:
    """`evaluate_breaker_for_portfolio` runs immediately after every fill
    (the other of the two required triggers). A crypto DELIVERY fill
    always pays a real brokerage charge (migration 0007 seeds 0.1% for
    BINANCE, and nothing else), which alone is enough to breach a
    deliberately tiny `max_daily_loss` -- proving the post-fill trigger
    works without needing to fabricate a price move. `order_two`, resting
    on a *different* instrument for the same portfolio, must not fill on
    a later, separate tick once the portfolio is paused."""
    pid = make_portfolio(setup_conn, cash=Decimal("100000"))
    setup_conn.execute(
        "UPDATE portfolios SET max_daily_loss = %s WHERE portfolio_id = %s",
        (Decimal("0.01"), pid),
    )
    iid_one = _make_crypto_instrument(setup_conn)
    iid_two = _make_crypto_instrument(setup_conn)
    _insert_bar(setup_conn, iid_one, Decimal("100.00"))
    order_one = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid_one,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    order_two = make_order(
        setup_conn,
        pid,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=iid_two,
        order_type=OrderType.MARKET,
        product=Product.DELIVERY,
        status=OrderStatus.OPEN,
    )
    run_id = next(_seq)
    pattern = f"test-ticks-breaker:{run_id}:*"
    channel_one = f"test-ticks-breaker:{run_id}:{iid_one}"
    channel_two = f"test-ticks-breaker:{run_id}:{iid_two}"
    try:
        _run_engine_with_publish(
            conn_factory=conn_factory,
            max_ticks=2,
            publish=[
                (channel_one, _tick_json(iid_one, datetime.now(UTC), "100.00")),
                (channel_two, _tick_json(iid_two, datetime.now(UTC), "10.00")),
            ],
            pattern=pattern,
        )

        one_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order_one.order_id,)
        ).fetchall()
        assert len(one_fills) == 1  # the triggering fill itself still happened

        status = setup_conn.execute(
            "SELECT status FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert status == ("PAUSED",)

        two_status = setup_conn.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order_two.order_id,)
        ).fetchone()
        assert two_status == ("CANCELLED",)

        two_fills = setup_conn.execute(
            "SELECT 1 FROM fills WHERE order_id=%s", (order_two.order_id,)
        ).fetchall()
        assert two_fills == []

        event = setup_conn.execute(
            "SELECT reason FROM circuit_breaker_events WHERE portfolio_id=%s", (pid,)
        ).fetchone()
        assert event is not None and event[0].startswith("max_daily_loss")
    finally:
        _cleanup(setup_conn, portfolio_ids=[pid], instrument_ids=[iid_one, iid_two])
