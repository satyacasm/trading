"""Bucketing logic and Postgres write path for the crypto bar aggregator.

Entry point (added in a later step of this plan, Task 3):
`python -m trading.streaming.bar_aggregator`. See the design doc
(docs/superpowers/specs/2026-08-24-bar-aggregator-design.md) for why this
exists: turning crypto_ingestor's live ticks into real 1-minute bars in
`bars_intraday` -- data the replay service needs next.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import structlog
from psycopg import Connection
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
from trading.contracts import DataSource
from trading.streaming.models import Bar, Tick

INTERVAL_SECONDS = 60


def bucket_start(ts: datetime, interval_seconds: int = INTERVAL_SECONDS) -> datetime:
    """Floor `ts` to the start of its interval bucket, anchored to UTC
    regardless of `ts`'s own tzinfo (Tick.ts is always tz-aware, but this
    function doesn't assume which zone)."""
    epoch_seconds = int(ts.timestamp())
    floored = epoch_seconds - (epoch_seconds % interval_seconds)
    return datetime.fromtimestamp(floored, tz=UTC)


@dataclass
class OpenBar:
    """A bar still accumulating ticks. Never written to Postgres directly --
    only via a `ClosedBar` once its window has fully elapsed."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int

    @classmethod
    def start(cls, tick: Tick) -> OpenBar:
        return cls(
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.quantity,
            trades=1,
        )

    def update(self, tick: Tick) -> None:
        self.high = max(self.high, tick.price)
        self.low = min(self.low, tick.price)
        self.close = tick.price
        self.volume += tick.quantity
        self.trades += 1


@dataclass
class ClosedBar:
    instrument_id: int
    bucket: datetime
    bar: OpenBar


class BarAggregator:
    """Pure in-memory minute-bucketing -- no I/O. `ingest()`/`flush_stale()`
    are synchronous and return any bars that just closed as a result.

    Ticks are assumed to arrive in non-decreasing timestamp order per
    instrument (true for one ordered Binance WS connection feeding one
    ordered Redis subscription), so an out-of-order tick updates the
    currently-open bucket rather than reopening an already-closed one.

    That held only while the bucket was still open. `flush_stale` deletes
    the bucket it closes, so a late tick for that minute found nothing open,
    started a fresh bar for a minute already written and announced, and had
    it closed and published a second time. A host that sleeps and wakes with
    a backlog of buffered ticks does this routinely, and the duplicate
    killed a live strategy that refused a bar it had already seen.
    `_closed_through` is the memory that makes the assumption true: a bucket
    is closed once, and a tick for it afterwards is dropped and counted.
    """

    def __init__(self, interval_seconds: int = INTERVAL_SECONDS) -> None:
        self._interval_seconds = interval_seconds
        self._open: dict[int, tuple[datetime, OpenBar]] = {}
        # The last bucket closed per instrument. One datetime each, so this
        # is bounded by the instrument count, not by uptime.
        self._closed_through: dict[int, datetime] = {}
        # Late ticks are rare and their rate is the interesting part -- a
        # steady trickle is a feed that reorders, a burst is a host that
        # slept. The loop logs this; the class stays free of I/O.
        self.late_ticks_dropped = 0

    def ingest(self, tick: Tick) -> list[ClosedBar]:
        bucket = bucket_start(tick.ts, self._interval_seconds)
        closed_through = self._closed_through.get(tick.instrument_id)
        if closed_through is not None and bucket <= closed_through:
            self.late_ticks_dropped += 1
            return []
        current = self._open.get(tick.instrument_id)
        if current is None:
            self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
            return []
        current_bucket, bar = current
        if bucket <= current_bucket:
            bar.update(tick)
            return []
        self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
        self._closed_through[tick.instrument_id] = current_bucket
        return [ClosedBar(instrument_id=tick.instrument_id, bucket=current_bucket, bar=bar)]

    def flush_stale(self, now: datetime) -> list[ClosedBar]:
        """Close any bucket whose window has fully elapsed as of `now`, even
        with no new tick to trigger it via `ingest()`. Removes flushed
        buckets from internal state -- calling this twice at the same `now`
        returns the second time's results as empty."""
        closed: list[ClosedBar] = []
        for instrument_id, (bucket, bar) in list(self._open.items()):
            if now >= bucket + timedelta(seconds=self._interval_seconds):
                closed.append(ClosedBar(instrument_id=instrument_id, bucket=bucket, bar=bar))
                del self._open[instrument_id]
                self._closed_through[instrument_id] = bucket
        return closed


_UPSERT_BAR = """
    INSERT INTO bars_intraday
        (instrument_id, ts, interval_sec, open, high, low, close, volume, trades, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume, trades = EXCLUDED.trades
"""


def write_closed_bar(
    conn: Connection,
    closed: ClosedBar,
    *,
    interval_seconds: int = INTERVAL_SECONDS,
    source: DataSource = DataSource.BINANCE_WS,
) -> None:
    """Upsert one closed bar. Never calls `conn.commit()` -- see this plan's
    Global Constraints for why (keeps this function test-safe against
    `db_conn`'s rollback-at-teardown; production commits via an
    `autocommit=True` connection instead).

    `source` defaults to `DataSource.BINANCE_WS` -- today's only publisher
    on the `ticks:*` convention -- but the bucketing logic upstream is
    publisher-agnostic (it only reads `Tick.instrument_id`), so a future
    non-Binance ingestor publishing the same `Tick`-shaped JSON can pass its
    own `DataSource` here instead of silently mis-attributing its bars as
    Binance's.
    """
    conn.execute(
        _UPSERT_BAR,
        (
            closed.instrument_id,
            closed.bucket,
            interval_seconds,
            closed.bar.open,
            closed.bar.high,
            closed.bar.low,
            closed.bar.close,
            closed.bar.volume,
            closed.bar.trades,
            source.value,
        ),
    )


log = structlog.get_logger(__name__)

_TICK_PATTERN = "ticks:*"
_BAR_PATTERN = "bars:*"

_SELECT_UPSTOX_BOUND_INSTRUMENT_IDS = """
    SELECT instrument_id FROM instruments WHERE source_bindings ? 'upstox_instrument_key'
"""

Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _parse_tick(raw: str) -> Tick | None:
    try:
        return Tick.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("bar_aggregator.malformed_message", reason=str(exc), raw=raw[:200])
        return None


def _parse_bar(raw: str) -> Bar | None:
    try:
        return Bar.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("bar_aggregator.malformed_bar_message", reason=str(exc), raw=raw[:200])
        return None


def _query_upstox_bound_instrument_ids(conn: Connection) -> set[int]:
    """Instruments with an Upstox binding get their bars authoritatively
    from `bars:*` (published from `marketOHLC`'s I1 entries) -- letting
    the tick path also aggregate bars for them would both under-count
    volume (mode "full" ticks are LTP snapshots, not a trade stream) and
    race the good bars for the same `(instrument_id, ts, interval_sec)`
    primary key, with the winner decided by arrival order."""
    rows = conn.execute(_SELECT_UPSTOX_BOUND_INSTRUMENT_IDS).fetchall()
    return {int(row[0]) for row in rows}


def write_upstox_bar(
    conn: Connection,
    bar: Bar,
    *,
    interval_seconds: int = INTERVAL_SECONDS,
    source: DataSource = DataSource.UPSTOX_WS,
) -> None:
    """Upsert one already-complete I1 bar straight into `bars_intraday`,
    bypassing `BarAggregator`/`OpenBar` entirely -- unlike a tick-
    aggregated `ClosedBar`, this bar was never partially observed, so
    there is no bucketing state to build up first. `trades` is always
    NULL: the feed gives no trade count, and inventing one would be
    dishonest."""
    conn.execute(
        _UPSERT_BAR,
        (
            bar.instrument_id,
            bar.ts,
            interval_seconds,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            None,
            source.value,
        ),
    )


_CLOSED_BAR_CHANNEL = "closed_bars"


async def _announce_bar(
    redis: Redis,
    *,
    instrument_id: int,
    ts: datetime,
    open_: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal | None,
    interval_seconds: int,
    source: DataSource,
) -> None:
    """Announce a bar that is final, whoever produced it.

    Both kinds go here: the ones this process bucketed out of ticks, and
    Upstox's already-complete I1 bars, which arrive whole on `bars:*`. This
    channel used to carry only the first, on the reasoning that conflating
    them would leave a subscriber unable to tell a bar that was received
    from one that was computed. That distinction is real, but `source`
    already carries it -- and paying for it with a channel meant a live
    strategy on an NSE instrument could be RUNNING all session and never be
    handed a bar, because its bars were written to the database and
    announced to nobody.

    What a subscriber actually asks this channel is "has a minute closed for
    this instrument", and the answer is the same either way.

    Money as strings, like everywhere it crosses a process boundary. A
    publish failure is logged and swallowed: a subscriber being absent or
    Redis being briefly unavailable must not cost the bar its place in the
    database, which is the durable record.
    """
    import json

    try:
        await redis.publish(
            f"{_CLOSED_BAR_CHANNEL}:{instrument_id}",
            json.dumps(
                {
                    "instrument_id": instrument_id,
                    "ts": ts.isoformat(),
                    "interval_sec": interval_seconds,
                    "open": str(open_),
                    "high": str(high),
                    "low": str(low),
                    "close": str(close),
                    "volume": None if volume is None else str(volume),
                    "source": source.value,
                }
            ),
        )
    except Exception as exc:  # noqa: BLE001 - the database write is the durable record
        log.warning(
            "bar_aggregator.publish_failed",
            instrument_id=instrument_id,
            reason=str(exc),
        )


async def _publish_closed_bar(
    redis: Redis, closed: ClosedBar, interval_seconds: int, source: DataSource
) -> None:
    """Announce a bar this process built out of ticks."""
    await _announce_bar(
        redis,
        instrument_id=closed.instrument_id,
        ts=closed.bucket,
        open_=closed.bar.open,
        high=closed.bar.high,
        low=closed.bar.low,
        close=closed.bar.close,
        volume=closed.bar.volume,
        interval_seconds=interval_seconds,
        source=source,
    )


async def run_aggregation_loop(
    redis: Redis,
    conn: Connection,
    *,
    interval_seconds: int = INTERVAL_SECONDS,
    flush_check_seconds: float = 5.0,
    sleep: Sleeper = _default_sleep,
    max_bars_written: int | None = None,
    source: DataSource = DataSource.BINANCE_WS,
    pattern: str = _TICK_PATTERN,
    bars_pattern: str = _BAR_PATTERN,
) -> None:
    """Subscribe to `pattern` (`ticks:*` by default) and `bars_pattern`
    (`bars:*` by default), writing each resulting bar to `bars_intraday`.

    Two independent sources feed the same table, kept from fighting over
    the same `(instrument_id, ts, interval_sec)` primary key by partition,
    not by arrival order:

    - `pattern`: raw ticks, bucketed in-process by `BarAggregator` into
      `ClosedBar`s once a bucket's window has fully elapsed. Ticks for any
      instrument with an Upstox binding (`source_bindings ?
      'upstox_instrument_key'`, queried once at startup) are skipped here
      -- those instruments get authoritative, already-complete bars via
      `bars_pattern` instead, and a tick-aggregated bar for them would
      both under-count volume (mode "full" ticks are LTP snapshots, not a
      trade stream) and race the good bar for the same primary key, with
      the winner decided by arrival order. Crypto instruments carry no
      Upstox binding, so this exclusion never touches them.
    - `bars_pattern`: already-complete bars (e.g. Upstox's I1 minute
      bars), upserted directly via `write_upstox_bar` -- never routed
      through `BarAggregator`/`OpenBar`, and never subject to the tick
      path's startup-discard rule below (a bar arriving this way was never
      partially observed by this process, unlike a tick-aggregated
      bucket).

    Runs forever when `max_bars_written` is None (production). Stops once
    `max_bars_written` bars have been written (from either source
    combined) when it's an int -- a test seam, the same shape as
    `crypto_ingestor.run_ingestion_loop`'s `max_ticks`.

    Whatever minute is already in progress on the tick path when this
    process starts has only been partially observed -- this process cannot
    honestly claim to have captured the whole window. `first_complete_bucket`
    is the first bucket boundary at or after startup that this process
    *can* claim in full; any tick-aggregated bar that closes for an
    earlier bucket is discarded (logged, not written) rather than
    persisted as if it were complete. This is the startup-side counterpart
    to the shutdown path, which already discards any still-open bucket
    instead of force-flushing it. This rule never applies to the
    `bars_pattern` path -- those bars are complete by construction.
    """
    aggregator = BarAggregator(interval_seconds)
    first_complete_bucket = bucket_start(datetime.now(UTC), interval_seconds) + timedelta(
        seconds=interval_seconds
    )
    excluded_instrument_ids = _query_upstox_bound_instrument_ids(conn)
    log.info(
        "bar_aggregator.excluding_tick_aggregation",
        count=len(excluded_instrument_ids),
        instrument_ids=sorted(excluded_instrument_ids),
    )
    written = 0
    done = asyncio.Event()

    async def _write_all(closed_bars: list[ClosedBar]) -> None:
        nonlocal written
        for closed in closed_bars:
            if closed.bucket < first_complete_bucket:
                log.info(
                    "bar_aggregator.discarding_partial_startup_bar",
                    instrument_id=closed.instrument_id,
                    bucket=closed.bucket.isoformat(),
                )
                continue
            try:
                write_closed_bar(conn, closed, interval_seconds=interval_seconds, source=source)
            except Exception as exc:  # noqa: BLE001 - a single unwritable bar (e.g.
                # a ForeignKeyViolation for an instrument_id absent from
                # `instruments`, the same real production crash
                # `_write_upstox_bar` guards against) must never kill the
                # whole consumer -- log and skip it, the same discipline
                # `_parse_tick` already applies to a malformed message.
                log.warning(
                    "bar_aggregator.closed_bar_write_failed",
                    instrument_id=closed.instrument_id,
                    ts=closed.bucket.isoformat(),
                    reason=str(exc),
                )
                continue
            # A closed bar is an event, and until now only this process knew
            # one had happened -- it went to the database and was announced
            # to nobody. The live supervisor needs to be told, and polling a
            # hypertable for rows that may not exist is the worse half of
            # that choice. Published after the write, so a subscriber that
            # reacts by querying finds the row already there.
            await _publish_closed_bar(redis, closed, interval_seconds, source)
            written += 1
        if max_bars_written is not None and written >= max_bars_written:
            done.set()

    async def _write_upstox_bar(bar: Bar) -> None:
        nonlocal written
        try:
            write_upstox_bar(conn, bar, interval_seconds=interval_seconds)
        except Exception as exc:  # noqa: BLE001 - a single unwritable bar (e.g.
            # a ForeignKeyViolation for an instrument_id absent from
            # `instruments`, the real production crash this guards against)
            # must never kill the whole consumer -- log and skip it, the same
            # discipline _parse_bar already applies to a malformed message.
            log.warning(
                "bar_aggregator.bar_write_failed",
                instrument_id=bar.instrument_id,
                ts=bar.ts.isoformat(),
                reason=str(exc),
            )
            return
        # Announced for the same reason a tick-built bar is: the live
        # supervisor learns that a minute closed from this channel and from
        # nowhere else. Without it an NSE strategy runs a whole session on
        # zero bars while its data sits in `bars_intraday`. After the write,
        # so a subscriber that reacts by querying finds the row already
        # there.
        await _announce_bar(
            redis,
            instrument_id=bar.instrument_id,
            ts=bar.ts,
            open_=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            interval_seconds=interval_seconds,
            source=DataSource.UPSTOX_WS,
        )
        written += 1
        if max_bars_written is not None and written >= max_bars_written:
            done.set()

    async def _consume_ticks(pubsub: PubSub) -> None:
        dropped_before = 0
        async for message in pubsub.listen():
            if message["type"] != "pmessage":
                continue
            tick = _parse_tick(message["data"])
            if tick is None:
                continue
            if tick.instrument_id in excluded_instrument_ids:
                continue
            await _write_all(aggregator.ingest(tick))
            # A tick for a minute already closed and announced. Dropping it
            # is right -- see `BarAggregator` -- but silently dropping data
            # is how a feed problem goes unnoticed for a week.
            if aggregator.late_ticks_dropped != dropped_before:
                log.warning(
                    "bar_aggregator.late_tick_dropped",
                    instrument_id=tick.instrument_id,
                    ts=tick.ts.isoformat(),
                    total=aggregator.late_ticks_dropped,
                )
                dropped_before = aggregator.late_ticks_dropped
            if done.is_set():
                return

    async def _consume_bars(pubsub: PubSub) -> None:
        async for message in pubsub.listen():
            if message["type"] != "pmessage":
                continue
            bar = _parse_bar(message["data"])
            if bar is None:
                continue
            await _write_upstox_bar(bar)
            if done.is_set():
                return

    async def _periodic_flush() -> None:
        while not done.is_set():
            await sleep(flush_check_seconds)
            await _write_all(aggregator.flush_stale(datetime.now(UTC)))

    pubsub = redis.pubsub()
    await pubsub.psubscribe(pattern)
    bars_pubsub = redis.pubsub()
    await bars_pubsub.psubscribe(bars_pattern)
    consumer = asyncio.create_task(_consume_ticks(pubsub))
    bars_consumer = asyncio.create_task(_consume_bars(bars_pubsub))
    flusher = asyncio.create_task(_periodic_flush())
    try:
        if max_bars_written is None:
            await asyncio.gather(consumer, bars_consumer, flusher)
        else:
            await done.wait()
    finally:
        consumer.cancel()
        bars_consumer.cancel()
        flusher.cancel()
        for one_pubsub in (pubsub, bars_pubsub):
            try:
                await one_pubsub.punsubscribe()
                # redis-py's PubSub.aclose (unlike Redis.aclose) ships with no
                # type annotations at all -- a real upstream stub gap, matching
                # the same suppression stream_gateway already carries.
                await one_pubsub.aclose()  # type: ignore[no-untyped-call]
            except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
                log.debug("bar_aggregator.pubsub_cleanup_failed", exc_info=True)
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop, so a
        # caller closing `redis` from a *different* asyncio.run() call later
        # (as short-lived test runs do) never hits a stale, cross-loop
        # connection.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    # autocommit=True: each closed bar is its own independent unit of work
    # over a long-running connection -- unlike seed_instruments.py's
    # one-shot atomic batch, there is no reason one bar's write should roll
    # back because a later bar's write fails. This is also what keeps
    # write_closed_bar() safe to call against `db_conn` in tests without any
    # special-casing: it never commits itself either way.
    conn = psycopg.connect(settings.database_url, autocommit=True)
    log.info("bar_aggregator.starting", interval_seconds=INTERVAL_SECONDS)
    try:
        asyncio.run(run_aggregation_loop(redis, conn))
    except KeyboardInterrupt:
        log.info("bar_aggregator.interrupted")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
