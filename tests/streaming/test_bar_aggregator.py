from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.contracts import DataSource
from trading.streaming.bar_aggregator import (
    BarAggregator,
    ClosedBar,
    OpenBar,
    bucket_start,
    run_aggregation_loop,
    write_closed_bar,
)
from trading.streaming.models import Bar, Tick


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


def test_write_closed_bar_uses_the_default_binance_source(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    closed = ClosedBar(
        instrument_id=iid,
        bucket=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
            trades=1,
        ),
    )
    write_closed_bar(db_conn, closed)
    row = db_conn.execute(
        "SELECT source FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (DataSource.BINANCE_WS.value,)


def test_write_closed_bar_persists_an_explicit_non_default_source(db_conn) -> None:
    """The bucketing logic is publisher-agnostic (it only reads
    `Tick.instrument_id`), so a future non-Binance ingestor publishing to
    the same `ticks:*` convention must be able to record its own
    provenance rather than being silently mis-attributed as Binance's."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    closed = ClosedBar(
        instrument_id=iid,
        bucket=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
            trades=1,
        ),
    )
    write_closed_bar(db_conn, closed, source=DataSource.NSE_CM_UDIFF)
    row = db_conn.execute(
        "SELECT source FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (DataSource.NSE_CM_UDIFF.value,)


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


def _isolated_channel_and_pattern(iid: int) -> tuple[str, str]:
    """A test-unique pub/sub channel and matching psubscribe pattern, never
    the real `ticks:{id}` / `ticks:*` convention -- `run_aggregation_loop`
    processes every message it receives on whatever pattern it subscribes
    to (unlike `stream_gateway`, which filters per-connection), so sharing
    the real `ticks:*` pattern with a live `crypto_ingestor` process on the
    same dev Redis instance risks a stray production tick landing mid-test:
    an FK violation against `trading_test`, or stealing the slot
    `max_bars_written` was waiting on for this test's own scripted tick.
    `iid` is a real auto-incrementing instrument id from `db_conn`'s own
    transaction, so it's already unique per test run."""
    channel = f"test-ticks:{iid}:{iid}"
    pattern = f"test-ticks:{iid}:*"
    return channel, pattern


def test_run_aggregation_loop_writes_a_closed_bar_once_its_window_elapses(
    db_conn, redis_client: redis.Redis
) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    # Comfortably ahead of "now" -- run_aggregation_loop discards any closed
    # bar for a bucket it started before (finding 1's fix), so both ticks'
    # buckets must fall at or after the loop's first_complete_bucket (at
    # most interval_seconds after loop start, which is only ~0.2s before
    # this first tick is published).
    base = datetime.now(UTC) + timedelta(minutes=3)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)  # give psubscribe time to land before we publish
            redis_client.publish(channel, _tick_json(iid, base.isoformat(), "65000.00"))
            redis_client.publish(
                channel,
                _tick_json(iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "65010.00"),
            )

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
    channel, pattern = _isolated_channel_and_pattern(iid)
    base = datetime.now(UTC) + timedelta(minutes=3)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, "not json")
            redis_client.publish(channel, _tick_json(iid, base.isoformat(), "65000.00"))
            redis_client.publish(
                channel,
                _tick_json(iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "65010.00"),
            )

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)  # only the one valid tick before the bucket closed


def test_run_aggregation_loop_skips_a_closed_bar_whose_write_fails_and_keeps_going(
    db_conn, redis_client: redis.Redis, monkeypatch
) -> None:
    """Same discipline as the `bars:*` path's identical test: a tick-
    aggregated closed bar whose `write_closed_bar` call raises (e.g. the
    real production crash: a ForeignKeyViolation for an instrument_id
    absent from `instruments`) must be logged and skipped, not propagated
    through asyncio.gather -- a single unwritable bar must never kill the
    whole consumer. The failure is injected via a flaky `write_closed_bar`
    rather than a real FK violation against `db_conn`: `db_conn` is a
    non-autocommit test transaction, and a genuine DB error would poison it
    for every statement after (unlike production's `autocommit=True`
    connection, per this fix's design), which would make the very
    "subsequent write still succeeds" assertion this test exists to make
    impossible to observe. A subsequent, valid tick-derived bar must still
    get written afterwards -- that's the point of this test, not just "no
    raise"."""
    import psycopg

    from trading.streaming import bar_aggregator as bar_aggregator_module
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    # Comfortably ahead of "now" -- same reasoning as the sibling tick-path
    # tests above: both closed buckets must fall at or after the loop's
    # first_complete_bucket, or _write_all discards them before ever
    # calling write_closed_bar.
    base = datetime.now(UTC) + timedelta(minutes=3)
    tick1_ts = base
    tick2_ts = base + timedelta(minutes=1, seconds=5)  # closes tick1's bucket (the bad one)
    tick3_ts = base + timedelta(minutes=2, seconds=10)  # closes tick2's bucket (the good one)

    real_write_closed_bar = bar_aggregator_module.write_closed_bar
    call_count = {"n": 0}

    def _flaky_write_closed_bar(conn: Any, closed: ClosedBar, **kwargs: Any) -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise psycopg.errors.ForeignKeyViolation(
                "simulated: instrument_id not present in instruments"
            )
        real_write_closed_bar(conn, closed, **kwargs)

    monkeypatch.setattr(bar_aggregator_module, "write_closed_bar", _flaky_write_closed_bar)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)  # give psubscribe time to land before we publish
            redis_client.publish(channel, _tick_json(iid, tick1_ts.isoformat(), "999.00"))
            redis_client.publish(channel, _tick_json(iid, tick2_ts.isoformat(), "500.00"))
            redis_client.publish(channel, _tick_json(iid, tick3_ts.isoformat(), "65000.00"))

        # Wrapped in a timeout: without the fix, the bad write's exception
        # kills consumer silently (an unretrieved task exception, never
        # raised into this test), so `done` never gets set and `done.wait()`
        # would otherwise hang forever instead of failing loudly.
        asyncio.run(asyncio.wait_for(_run_both(loop_task, _publish_after_subscribed()), timeout=10))
    finally:
        asyncio.run(async_redis.aclose())

    assert call_count["n"] == 2  # the failing write was attempted, then the next one too
    rows = db_conn.execute(
        "SELECT ts, close FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchall()
    # Only the second, valid bucket was persisted -- the first write's
    # exception never reached the database at all.
    assert rows == [(bucket_start(tick2_ts), Decimal("500.0000"))]


def test_run_aggregation_loop_flushes_a_stale_bucket_via_the_periodic_safety_net(
    db_conn, redis_client: redis.Redis
) -> None:
    """No second tick ever arrives to trigger ingest()'s rollover-detection
    -- only the periodic flush can close this bucket. Finding 1's startup
    cutoff rules out the old "tick timestamped far in the past" trick (that
    bucket would fall before `first_complete_bucket` and get discarded as a
    partial bar) -- so this test uses a short `interval_seconds` and a tick
    timestamped a couple of seconds into the future, then genuinely waits
    in real time (via the periodic flush, checking every 50ms) for that
    short window to elapse."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    future_ts = (datetime.now(UTC) + timedelta(seconds=2)).isoformat()

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis,
            db_conn,
            interval_seconds=1,
            flush_check_seconds=0.05,
            sleep=asyncio.sleep,
            max_bars_written=1,
            pattern=pattern,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _tick_json(iid, future_ts, "65000.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)


def test_run_aggregation_loop_discards_a_partial_bar_left_over_from_before_startup(
    db_conn, redis_client: redis.Redis
) -> None:
    """The bucket already in progress when this process starts has only
    been partially observed by this process -- it must never be written as
    if it were complete (this is exactly what happened for real in the live
    verification data: a bar with an anomalously low trade count and an
    `open` that didn't match the prior bar's `close`, both the signature of
    a truncated first bar after a process restart). A later tick landing in
    a fully post-startup minute must still be written normally."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    now = datetime.now(UTC)
    # The bucket already running when the loop starts -- must be discarded,
    # not written, once it closes.
    partial_tick_ts = now
    # Comfortably past the loop's first_complete_bucket cutoff (at most
    # interval_seconds=60s after loop start): forces the partial bucket
    # above to close via ingest's rollover detection and opens a new,
    # fully post-startup bucket.
    later_tick_ts = now + timedelta(minutes=2)
    # Forces that later bucket closed in turn, so it actually gets written.
    even_later_tick_ts = now + timedelta(minutes=3)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _tick_json(iid, partial_tick_ts.isoformat(), "100.00"))
            redis_client.publish(channel, _tick_json(iid, later_tick_ts.isoformat(), "200.00"))
            redis_client.publish(channel, _tick_json(iid, even_later_tick_ts.isoformat(), "300.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    rows = db_conn.execute(
        "SELECT ts, open, close FROM bars_intraday WHERE instrument_id = %s ORDER BY ts",
        (iid,),
    ).fetchall()
    # Only the later, fully-post-startup bucket was written -- the partial
    # bucket that predates this process's startup never appears at all.
    assert len(rows) == 1
    assert rows[0] == (bucket_start(later_tick_ts), Decimal("200.0000"), Decimal("200.0000"))


def _bar_json(instrument_id: int, ts: datetime, close: str = "100.00", volume: str = "1000") -> str:
    return Bar(
        instrument_id=instrument_id,
        ts=ts,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal(volume),
    ).model_dump_json()


def _isolated_bars_channel_and_pattern(iid: int) -> tuple[str, str]:
    """Same isolation reasoning as `_isolated_channel_and_pattern`, for the
    `bars:*`-shaped consumer instead of `ticks:*`."""
    channel = f"test-bars:{iid}:{iid}"
    pattern = f"test-bars:{iid}:*"
    return channel, pattern


def _make_upstox_bound_instrument(db_conn) -> int:
    """A minimal EQUITY instrument row carrying an `upstox_instrument_key`
    binding -- enough to exercise the tick-aggregation exclusion query
    without depending on `seed_upstox_instrument_keys`'s fixed watchlist
    (which requires pre-seeded RELIANCE/TCS/... rows this test doesn't
    otherwise need)."""
    row = db_conn.execute(
        """
        INSERT INTO instruments
            (asset_class, exchange, segment, symbol, series, currency, status,
             canonical_key, source_bindings)
        VALUES ('EQUITY', 'NSE', 'CM', 'BARAGGTEST', 'EQ', 'INR', 'ACTIVE',
                'NSE:CM:BARAGGTEST:EQ', '{"upstox_instrument_key": "NSE_EQ|TESTISIN"}'::jsonb)
        RETURNING instrument_id
        """
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_run_aggregation_loop_upserts_a_bars_message_with_upstox_ws_source(
    db_conn, redis_client: redis.Redis
) -> None:
    """A `bars:*` message (an already-complete I1 bar) is upserted
    directly via `_UPSERT_BAR` -- it never touches `BarAggregator`/
    `OpenBar`, and is never subject to the tick path's startup-discard
    rule (it's complete by construction, not partially observed)."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_bars_channel_and_pattern(iid)
    # Deliberately in the past (long before "now") -- unlike the tick path,
    # a bars:* message must never be discarded by the startup cutoff.
    bar_ts = datetime(2020, 1, 1, 9, 16, 0, tzinfo=UTC)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, bars_pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _bar_json(iid, bar_ts, close="65000.00", volume="123"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT ts, open, close, volume, trades, source"
        " FROM bars_intraday WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (
        bar_ts,
        Decimal("65000.0000"),
        Decimal("65000.0000"),
        Decimal("123.00000000"),
        None,
        DataSource.UPSTOX_WS.value,
    )


def test_run_aggregation_loop_skips_a_bar_whose_write_fails_and_keeps_going(
    db_conn, redis_client: redis.Redis, monkeypatch
) -> None:
    """A `bars:*` write that raises (e.g. the real production crash: a
    ForeignKeyViolation for an instrument_id absent from `instruments`) must
    be logged and skipped, not propagated through asyncio.gather -- a single
    unwritable bar must never kill the whole consumer. The failure is
    injected via a flaky `write_upstox_bar` rather than a real FK violation
    against `db_conn`: `db_conn` is a non-autocommit test transaction, and a
    genuine DB error would poison it for every statement after (unlike
    production's `autocommit=True` connection, per this fix's design), which
    would make the very "subsequent write still succeeds" assertion this
    test exists to make impossible to observe. A subsequent, valid bar on
    the same channel must still get written afterwards -- that's the point
    of this test, not just "no raise"."""
    import psycopg

    from trading.streaming import bar_aggregator as bar_aggregator_module
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_bars_channel_and_pattern(iid)
    bad_bar_ts = datetime(2020, 1, 1, 9, 16, 0, tzinfo=UTC)
    good_bar_ts = datetime(2020, 1, 1, 9, 17, 0, tzinfo=UTC)

    real_write_upstox_bar = bar_aggregator_module.write_upstox_bar
    call_count = {"n": 0}

    def _flaky_write_upstox_bar(conn: Any, bar: Bar, **kwargs: Any) -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise psycopg.errors.ForeignKeyViolation(
                "simulated: instrument_id not present in instruments"
            )
        real_write_upstox_bar(conn, bar, **kwargs)

    monkeypatch.setattr(bar_aggregator_module, "write_upstox_bar", _flaky_write_upstox_bar)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, bars_pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _bar_json(iid, bad_bar_ts, close="999.00"))
            redis_client.publish(channel, _bar_json(iid, good_bar_ts, close="65000.00"))

        # Wrapped in a timeout: without the fix, the bad write's exception
        # kills bars_consumer silently (an unretrieved task exception, never
        # raised into this test), so `done` never gets set and `done.wait()`
        # would otherwise hang forever instead of failing loudly.
        asyncio.run(asyncio.wait_for(_run_both(loop_task, _publish_after_subscribed()), timeout=10))
    finally:
        asyncio.run(async_redis.aclose())

    assert call_count["n"] == 2  # the failing write was attempted, then the next one too
    rows = db_conn.execute(
        "SELECT ts, close FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchall()
    # Only the second, valid bar was persisted -- the first write's exception
    # never reached the database at all.
    assert rows == [(good_bar_ts, Decimal("65000.0000"))]


def test_run_aggregation_loop_skips_a_malformed_bar_message_and_keeps_going(
    db_conn, redis_client: redis.Redis
) -> None:
    """Same discipline as the tick path's identical test: a `bars:*` message
    that fails `Bar` validation is logged and skipped by `_parse_bar`, never
    killing the consumer. A subsequent valid bar on the same channel still
    gets written."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_bars_channel_and_pattern(iid)
    bar_ts = datetime(2020, 1, 1, 9, 16, 0, tzinfo=UTC)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, bars_pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, "not json")
            redis_client.publish(channel, _bar_json(iid, bar_ts, close="65000.00", volume="123"))

        asyncio.run(asyncio.wait_for(_run_both(loop_task, _publish_after_subscribed()), timeout=10))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT close FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (Decimal("65000.0000"),)


def test_run_aggregation_loop_excludes_upstox_bound_instruments_from_tick_aggregation(
    db_conn, redis_client: redis.Redis
) -> None:
    """An Upstox-bound instrument's ticks must produce no tick-aggregated
    bar at all (it gets its bars from `bars:*` instead, and a tick-
    aggregated one would be volume-wrong and race the good one for the
    same primary key). A crypto instrument (no Upstox binding) on the same
    pattern must still aggregate normally with source=BINANCE_WS."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    excluded_iid = _make_upstox_bound_instrument(db_conn)
    crypto_iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    tag = f"{excluded_iid}-{crypto_iid}"
    pattern = f"test-ticks:{tag}:*"
    excluded_channel = f"test-ticks:{tag}:{excluded_iid}"
    crypto_channel = f"test-ticks:{tag}:{crypto_iid}"
    base = datetime.now(UTC) + timedelta(minutes=3)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            # Two ticks -> a rollover -> would close a bar if aggregated.
            redis_client.publish(
                excluded_channel, _tick_json(excluded_iid, base.isoformat(), "999.00")
            )
            redis_client.publish(
                excluded_channel,
                _tick_json(
                    excluded_iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "998.00"
                ),
            )
            # Crypto tick rollover -- this is the one that actually closes
            # a bar and stops the loop via max_bars_written=1.
            redis_client.publish(
                crypto_channel, _tick_json(crypto_iid, base.isoformat(), "65000.00")
            )
            redis_client.publish(
                crypto_channel,
                _tick_json(
                    crypto_iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "65010.00"
                ),
            )

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    excluded_rows = db_conn.execute(
        "SELECT 1 FROM bars_intraday WHERE instrument_id = %s", (excluded_iid,)
    ).fetchall()
    assert excluded_rows == []  # never aggregated from ticks

    crypto_row = db_conn.execute(
        "SELECT source FROM bars_intraday WHERE instrument_id = %s", (crypto_iid,)
    ).fetchone()
    assert crypto_row == (DataSource.BINANCE_WS.value,)


def test_run_aggregation_loop_logs_the_excluded_instrument_set_once_at_startup(
    db_conn, redis_client: redis.Redis
) -> None:
    import structlog

    from trading.streaming.seed_instruments import seed_crypto_instruments

    excluded_iid = _make_upstox_bound_instrument(db_conn)
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    base = datetime.now(UTC) + timedelta(minutes=3)

    with structlog.testing.capture_logs() as cap:
        async_redis: AsyncRedis = AsyncRedis.from_url(
            get_settings().redis_url, decode_responses=True
        )
        try:
            loop_task = run_aggregation_loop(
                async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
            )

            async def _publish_after_subscribed() -> None:
                await asyncio.sleep(0.2)
                redis_client.publish(channel, _tick_json(iid, base.isoformat(), "65000.00"))
                redis_client.publish(
                    channel,
                    _tick_json(
                        iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "65010.00"
                    ),
                )

            asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
        finally:
            asyncio.run(async_redis.aclose())

    events = [
        entry for entry in cap if entry.get("event") == "bar_aggregator.excluding_tick_aggregation"
    ]
    assert len(events) == 1
    assert excluded_iid in events[0]["instrument_ids"]
    assert events[0]["count"] == len(events[0]["instrument_ids"])


def _flat_bar_json(instrument_id: int, ts: str, close: str) -> str:
    import json

    return json.dumps(
        {
            "instrument_id": instrument_id,
            "ts": ts,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": "10",
        }
    )


def test_an_upstox_bar_is_announced_on_the_closed_bar_channel(
    db_conn, redis_client: redis.Redis
) -> None:
    """A live run subscribes to `closed_bars:*` and nothing else. Upstox's
    already-complete I1 bars went to the database and were announced to
    nobody, so an NSE strategy could be RUNNING for a whole session and
    never receive a bar -- the aggregator's own tick-built bars were the
    only ones that reached the supervisor."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    bars_channel = f"test-bars:{iid}:{iid}"
    bars_pattern = f"test-bars:{iid}:*"
    ts = (datetime.now(UTC) + timedelta(minutes=3)).replace(second=0, microsecond=0)

    listener = redis_client.pubsub()
    listener.subscribe(f"closed_bars:{iid}")

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis,
            db_conn,
            sleep=_no_sleep,
            max_bars_written=1,
            pattern=f"test-ticks:{iid}:*",
            bars_pattern=bars_pattern,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(bars_channel, _flat_bar_json(iid, ts.isoformat(), "65000.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    announced = _drain_for_closed_bar(listener)
    listener.close()
    assert announced is not None, "the Upstox bar reached the database but was never announced"
    assert announced["instrument_id"] == iid
    assert announced["ts"] == ts.isoformat()
    # The feed's own precision, not the database's numeric(18,4): a bar is
    # announced with the value this process holds, exactly as a tick-built
    # bar is. The two compare equal as Decimals either way.
    assert announced["close"] == "65000.00"
    # The channel now carries both kinds of bar. `source` is what keeps a
    # subscriber able to tell a bar that was received from one this process
    # computed -- the distinction the separate channel used to carry.
    assert announced["source"] == DataSource.UPSTOX_WS.value


def _drain_for_closed_bar(listener: redis.client.PubSub) -> dict[str, Any] | None:
    import json

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        message = listener.get_message(timeout=1.0)
        if not message or message["type"] != "message":
            continue
        return dict(json.loads(message["data"]))
    return None


def test_a_tick_for_an_already_flushed_bucket_does_not_reopen_it() -> None:
    """`flush_stale` deletes the bucket it closed, so a tick arriving for
    that same minute afterwards used to find no open bar, start a fresh one
    for a minute that had already been published, and have it closed and
    published a second time.

    That is not a theoretical race. The host slept, woke with a backlog of
    buffered ticks, and the duplicate reached a live strategy whose runtime
    refused a bar it had already seen -- killing a run that had been
    trading for hours.
    """
    aggregator = BarAggregator()
    base = datetime(2026, 9, 4, 20, 2, tzinfo=UTC)

    aggregator.ingest(_tick(base, price="65000.00"))
    closed = aggregator.flush_stale(base + timedelta(seconds=61))
    assert [c.bucket for c in closed] == [base]

    # The late tick belongs to a minute that has already been announced.
    aggregator.ingest(_tick(base + timedelta(seconds=30), price="66000.00"))

    assert aggregator.flush_stale(base + timedelta(minutes=5)) == [], (
        "the already-closed 20:02 bucket was reopened and closed a second time"
    )
    assert aggregator.late_ticks_dropped == 1


def test_a_tick_for_a_new_bucket_still_opens_one_after_a_flush() -> None:
    """The guard must not wedge the instrument: the very next minute is
    still ordinary business."""
    aggregator = BarAggregator()
    base = datetime(2026, 9, 4, 20, 2, tzinfo=UTC)

    aggregator.ingest(_tick(base))
    aggregator.flush_stale(base + timedelta(seconds=61))
    aggregator.ingest(_tick(base + timedelta(minutes=1)))

    closed = aggregator.flush_stale(base + timedelta(minutes=5))
    assert [c.bucket for c in closed] == [base + timedelta(minutes=1)]
    assert aggregator.late_ticks_dropped == 0


def test_run_aggregation_loop_still_processes_ticks_when_startup_backfill_fails(
    db_conn, redis_client: redis.Redis
) -> None:
    """The DB (or Binance) can be unreachable when this process starts.
    A startup backfill failure must be logged and swallowed -- never
    propagate out of run_aggregation_loop and stop live aggregation
    from starting at all. The spec's silence trigger and sweep (Task 6)
    repair the gap later."""
    import psycopg

    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel, pattern = _isolated_channel_and_pattern(iid)
    base = datetime.now(UTC) + timedelta(minutes=3)

    def _raising_conn_factory():
        raise psycopg.OperationalError("could not connect to server")

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = run_aggregation_loop(
            async_redis,
            db_conn,
            sleep=_no_sleep,
            max_bars_written=1,
            pattern=pattern,
            backfill_conn_factory=_raising_conn_factory,
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)  # give psubscribe time to land before we publish
            redis_client.publish(channel, _tick_json(iid, base.isoformat(), "65000.00"))
            redis_client.publish(
                channel,
                _tick_json(iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "65010.00"),
            )

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT open, trades FROM bars_intraday WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (Decimal("65000.0000"), 1), (
        "a startup backfill failure must not stop the loop from subscribing and "
        "processing live ticks into bars"
    )


def test_seed_closed_through_drops_a_late_tick_for_an_already_written_minute():
    """The actual production bug (design §1 row 4): _closed_through is
    in-memory only, so a late tick after a restart reopened an
    already-announced minute and republished it with different values,
    crashing a live strategy on 'arrived out of order'."""
    from trading.streaming.bar_aggregator import BarAggregator
    from trading.streaming.models import Tick

    aggregator = BarAggregator()
    aggregator.seed_closed_through({1: datetime(2026, 9, 25, 10, 5, tzinfo=UTC)})

    closed = aggregator.ingest(
        Tick(
            instrument_id=1,
            ts=datetime(2026, 9, 25, 10, 3, 30, tzinfo=UTC),  # a minute already seeded
            price=Decimal("100"),
            quantity=Decimal("1"),
        )
    )
    assert closed == []
    assert aggregator.late_ticks_dropped == 1


def test_startup_backfill_writes_and_announces_with_a_fake_fetch(db_conn, redis_client) -> None:
    """Exercises the extracted helper directly rather than the whole
    run_aggregation_loop -- that loop only terminates on a tick/bar
    count, and no ticks are published in this test."""
    import asyncio
    import json

    from redis.asyncio import Redis as AsyncRedis

    from trading.config import get_settings
    from trading.sources.binance_spot import SpotKline
    from trading.streaming.bar_aggregator import BarAggregator, _startup_spot_backfill
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 9, 58, tzinfo=UTC)),
    )

    def _fake_fetch(symbol, *, start_ms, end_ms, client=None):
        return [
            SpotKline(
                ts=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("100"),
                low=Decimal("100"),
                close=Decimal("100"),
                volume=Decimal("0"),
                trades=0,
            )
        ]

    async def _inline_to_thread(fn, /, *args, **kwargs):
        return fn(*args, **kwargs)

    async def _next_pmessage(pubsub):
        async for msg in pubsub.listen():
            if msg["type"] == "pmessage":
                return msg

    async def _run():
        async_redis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
        pubsub = async_redis.pubsub()
        await pubsub.psubscribe("closed_bars:*")
        aggregator = BarAggregator()
        try:
            await _startup_spot_backfill(
                lambda: db_conn,
                aggregator,
                async_redis,
                to_thread=_inline_to_thread,
                fetch=_fake_fetch,
            )
            message = await asyncio.wait_for(_next_pmessage(pubsub), timeout=5)
            return aggregator, message
        finally:
            await pubsub.aclose()
            await async_redis.connection_pool.disconnect()

    aggregator, message = asyncio.run(_run())

    assert aggregator._closed_through[iid] == datetime(2026, 9, 25, 9, 59, tzinfo=UTC)
    row = db_conn.execute(
        "SELECT open, source FROM bars_intraday WHERE instrument_id=%s AND ts=%s",
        (iid, datetime(2026, 9, 25, 9, 59, tzinfo=UTC)),
    ).fetchone()
    assert row == (Decimal("100.0000"), 11)  # 11 = BINANCE_SPOT_KLINE
    body = json.loads(message["data"])
    assert body["instrument_id"] == iid
