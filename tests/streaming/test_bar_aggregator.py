from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.bar_aggregator import (
    BarAggregator,
    ClosedBar,
    OpenBar,
    bucket_start,
    run_aggregation_loop,
    write_closed_bar,
)
from trading.streaming.models import Tick


def _tick(
    ts: datetime, price: str = "100.00", quantity: str = "1.00", instrument_id: int = 501
) -> Tick:
    return Tick(
        instrument_id=instrument_id, ts=ts, price=Decimal(price), quantity=Decimal(quantity)
    )


def test_bucket_start_floors_to_the_minute_in_utc() -> None:
    ts = datetime(2026, 8, 24, 12, 0, 45, tzinfo=UTC)
    assert bucket_start(ts) == datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)


def test_bucket_start_is_idempotent_on_an_already_floored_timestamp() -> None:
    ts = datetime(2026, 8, 24, 12, 1, 0, tzinfo=UTC)
    assert bucket_start(ts) == ts


def test_ingest_opens_a_new_bucket_and_returns_nothing_closed() -> None:
    aggregator = BarAggregator()
    closed = aggregator.ingest(_tick(datetime(2026, 8, 24, 12, 0, 10, tzinfo=UTC)))
    assert closed == []


def test_ingest_updates_high_low_close_within_the_same_bucket() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=1), price="100"))
    aggregator.ingest(_tick(base + timedelta(seconds=2), price="105"))
    aggregator.ingest(_tick(base + timedelta(seconds=3), price="98"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=4), price="102"))
    assert closed == []
    # Force the bucket closed by crossing into the next minute.
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert len(closed) == 1
    bar = closed[0].bar
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("105")
    assert bar.low == Decimal("98")
    assert bar.close == Decimal("102")
    assert bar.volume == Decimal("4.00")  # four 1.00-quantity ticks in the first bucket
    assert bar.trades == 4


def test_ingest_closes_the_previous_bucket_with_the_correct_bucket_start() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 5, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30)))
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1, seconds=1)))
    assert len(closed) == 1
    assert closed[0].bucket == base
    assert closed[0].instrument_id == 501


def test_ingest_treats_an_out_of_order_tick_as_an_update_to_the_current_bucket() -> None:
    """Ticks are assumed non-decreasing per instrument (one ordered WS
    connection -> one ordered Redis subscription). An out-of-order tick
    updates the currently-open bucket rather than reopening a closed one --
    an acknowledged simplification, not a crash or a silent data loss."""
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30), price="100"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=10), price="999"))  # earlier ts
    assert closed == []  # no bucket was closed -- just folded into the open one
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert closed[0].bar.close == Decimal("999")  # last-ingested tick, not last-in-time


def test_flush_stale_closes_a_bucket_whose_window_has_fully_elapsed() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert len(closed) == 1
    assert closed[0].bucket == base
    # Flushed buckets are removed -- a second flush at the same `now` finds nothing.
    assert aggregator.flush_stale(now=base + timedelta(seconds=60)) == []


def test_flush_stale_leaves_a_bucket_open_if_its_window_has_not_elapsed_yet() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    assert aggregator.flush_stale(now=base + timedelta(seconds=59)) == []


def test_flush_stale_tracks_multiple_instruments_independently() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10), instrument_id=501))
    aggregator.ingest(_tick(base + timedelta(seconds=20), instrument_id=502))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert {c.instrument_id for c in closed} == {501, 502}


def test_write_closed_bar_upserts_into_bars_intraday(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    closed = ClosedBar(
        instrument_id=iid,
        bucket=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        bar=OpenBar(
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("102"),
            volume=Decimal("0.01000000"),
            trades=4,
        ),
    )
    write_closed_bar(db_conn, closed)
    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, interval_sec, source"
        " FROM bars_intraday WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (
        Decimal("100.0000"),
        Decimal("105.0000"),
        Decimal("98.0000"),
        Decimal("102.0000"),
        Decimal("0.01000000"),
        4,
        60,
        6,
    )


def test_write_closed_bar_is_idempotent_on_conflict(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    bucket = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    first = ClosedBar(
        instrument_id=iid,
        bucket=bucket,
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
            trades=1,
        ),
    )
    second = ClosedBar(
        instrument_id=iid,
        bucket=bucket,
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("9"),
            low=Decimal("1"),
            close=Decimal("5"),
            volume=Decimal("3"),
            trades=3,
        ),
    )
    write_closed_bar(db_conn, first)
    write_closed_bar(db_conn, second)
    rows = db_conn.execute(
        "SELECT close, trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchall()
    assert len(rows) == 1  # upserted, not duplicated
    assert rows[0] == (Decimal("5.0000"), 3)  # second write's values won


async def _no_sleep(seconds: float) -> None:
    # await asyncio.sleep(0), not a bare `return None`: a plain async
    # function that never awaits anything never actually yields control
    # back to the event loop when awaited (only a real suspension point
    # does). run_aggregation_loop's `_periodic_flush` background task loops
    # `while not done.is_set(): await sleep(...)` concurrently with the
    # tick-consuming task -- pairing it with a truly no-op `sleep` here
    # livelocks the whole loop (confirmed by an isolated repro): the flush
    # task spins forever without ever handing control to the consumer task
    # that's the only thing that can set `done`. `asyncio.sleep(0)` keeps
    # "no real delay" while still cooperating with the scheduler.
    await asyncio.sleep(0)


def _tick_json(instrument_id: int, ts: str, price: str, quantity: str = "0.01000000") -> str:
    return Tick(
        instrument_id=instrument_id,
        ts=datetime.fromisoformat(ts),
        price=Decimal(price),
        quantity=Decimal(quantity),
    ).model_dump_json()


def test_run_aggregation_loop_writes_a_closed_bar_once_its_window_elapses(
    db_conn, redis_client: redis.Redis
) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(async_redis, db_conn, sleep=_no_sleep, max_bars_written=1)

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)  # give psubscribe time to land before we publish
            redis_client.publish(channel, _tick_json(iid, "2026-08-24T12:00:10+00:00", "65000.00"))
            redis_client.publish(channel, _tick_json(iid, "2026-08-24T12:01:05+00:00", "65010.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, source"
        " FROM bars_intraday WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (
        Decimal("65000.0000"),
        Decimal("65000.0000"),
        Decimal("65000.0000"),
        Decimal("65000.0000"),
        Decimal("0.01000000"),
        1,
        6,
    )


async def _run_both(
    loop_task: Coroutine[Any, Any, None], publisher: Coroutine[Any, Any, None]
) -> None:
    # Both args are passed as bare coroutines rather than pre-scheduled via
    # asyncio.ensure_future()/create_task(): scheduling them here, inside
    # the coroutine asyncio.run() actually executes, binds each resulting
    # Task to the one event loop that's running. Scheduling loop_task
    # eagerly at the call site (before any loop is running) binds it to a
    # different, implicitly-created loop instead, and asyncio.gather()
    # below then raises "got Future attached to a different loop".
    await asyncio.gather(loop_task, publisher)


def test_run_aggregation_loop_skips_a_malformed_message_and_keeps_going(
    db_conn, redis_client: redis.Redis
) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(async_redis, db_conn, sleep=_no_sleep, max_bars_written=1)

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, "not json")
            redis_client.publish(channel, _tick_json(iid, "2026-08-24T12:00:10+00:00", "65000.00"))
            redis_client.publish(channel, _tick_json(iid, "2026-08-24T12:01:05+00:00", "65010.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)  # only the one valid tick before the bucket closed


def test_run_aggregation_loop_flushes_a_stale_bucket_via_the_periodic_safety_net(
    db_conn, redis_client: redis.Redis
) -> None:
    """No second tick ever arrives to trigger ingest()'s rollover-detection
    -- only the periodic flush can close this bucket. Uses a tick timestamped
    far in the past (not a real multi-minute wall-clock wait): the very
    first periodic check already finds the bucket's window elapsed."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"
    stale_ts = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis,
            db_conn,
            flush_check_seconds=0.05,
            sleep=asyncio.sleep,
            max_bars_written=1,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _tick_json(iid, stale_ts, "65000.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)
