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
from trading.streaming.models import Tick

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

    Assumes ticks arrive in non-decreasing timestamp order per instrument
    (true for one ordered Binance WS connection feeding one ordered Redis
    subscription): an out-of-order tick updates the currently-open bucket
    rather than reopening an already-closed one. An acknowledged
    simplification for this proof-of-shape tier, not a silent bug.
    """

    def __init__(self, interval_seconds: int = INTERVAL_SECONDS) -> None:
        self._interval_seconds = interval_seconds
        self._open: dict[int, tuple[datetime, OpenBar]] = {}

    def ingest(self, tick: Tick) -> list[ClosedBar]:
        bucket = bucket_start(tick.ts, self._interval_seconds)
        current = self._open.get(tick.instrument_id)
        if current is None:
            self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
            return []
        current_bucket, bar = current
        if bucket <= current_bucket:
            bar.update(tick)
            return []
        self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
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

Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _parse_tick(raw: str) -> Tick | None:
    try:
        return Tick.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("bar_aggregator.malformed_message", reason=str(exc), raw=raw[:200])
        return None


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
) -> None:
    """Subscribe to `pattern` (`ticks:*` by default), aggregate into bars,
    write each closed bar.

    Runs forever when `max_bars_written` is None (production). Stops once
    `max_bars_written` bars have been written when it's an int -- a test
    seam, the same shape as `crypto_ingestor.run_ingestion_loop`'s
    `max_ticks`.

    Whatever minute is already in progress when this process starts has
    only been partially observed -- this process cannot honestly claim to
    have captured the whole window. `first_complete_bucket` is the first
    bucket boundary at or after startup that this process *can* claim in
    full; any bar that closes for an earlier bucket is discarded (logged,
    not written) rather than persisted as if it were complete. This is the
    startup-side counterpart to the shutdown path, which already discards
    any still-open bucket instead of force-flushing it.
    """
    aggregator = BarAggregator(interval_seconds)
    first_complete_bucket = bucket_start(datetime.now(UTC), interval_seconds) + timedelta(
        seconds=interval_seconds
    )
    written = 0
    done = asyncio.Event()

    def _write_all(closed_bars: list[ClosedBar]) -> None:
        nonlocal written
        for closed in closed_bars:
            if closed.bucket < first_complete_bucket:
                log.info(
                    "bar_aggregator.discarding_partial_startup_bar",
                    instrument_id=closed.instrument_id,
                    bucket=closed.bucket.isoformat(),
                )
                continue
            write_closed_bar(conn, closed, interval_seconds=interval_seconds, source=source)
            written += 1
        if max_bars_written is not None and written >= max_bars_written:
            done.set()

    async def _consume_ticks(pubsub: PubSub) -> None:
        async for message in pubsub.listen():
            if message["type"] != "pmessage":
                continue
            tick = _parse_tick(message["data"])
            if tick is None:
                continue
            _write_all(aggregator.ingest(tick))
            if done.is_set():
                return

    async def _periodic_flush() -> None:
        while not done.is_set():
            await sleep(flush_check_seconds)
            _write_all(aggregator.flush_stale(datetime.now(UTC)))

    pubsub = redis.pubsub()
    await pubsub.psubscribe(pattern)
    consumer = asyncio.create_task(_consume_ticks(pubsub))
    flusher = asyncio.create_task(_periodic_flush())
    try:
        if max_bars_written is None:
            await asyncio.gather(consumer, flusher)
        else:
            await done.wait()
    finally:
        consumer.cancel()
        flusher.cancel()
        try:
            await pubsub.punsubscribe()
            # redis-py's PubSub.aclose (unlike Redis.aclose) ships with no
            # type annotations at all -- a real upstream stub gap, matching
            # the same suppression stream_gateway already carries.
            await pubsub.aclose()  # type: ignore[no-untyped-call]
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
