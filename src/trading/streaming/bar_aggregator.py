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
from typing import Any

import redis as _sync_redis
import structlog
from psycopg import Connection
from redis.asyncio import Redis

from trading.config import get_settings
from trading.contracts import DataSource
from trading.db import ReconnectingConnection
from trading.sources.binance_spot import fetch_spot_klines, spot_symbol
from trading.streaming.heartbeat import start_heartbeat_thread
from trading.streaming.models import Bar, Tick
from trading.streaming.resilient_pubsub import resilient_messages

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

    def seed_closed_through(self, mapping: dict[int, datetime]) -> None:
        """Prime `_closed_through` from the database at startup, so a
        late tick for a minute this PROCESS never bucketed -- because a
        previous process instance already closed and announced it -- is
        dropped rather than reopening and republishing an already-final
        bar (design §1 row 4; `_closed_through` was in-memory only, and
        a restart forgot it).

        `_closed_through` is a never-reopen watermark and only ever moves
        forward, so this is a monotonic max-merge, never a blind
        `dict.update()`. The silence and sweep backfills (Task 6) call
        this concurrently with live tick ingestion: if live ticks have
        already closed a newer bucket by the time a slower backfill's
        seed lands, a blind overwrite would move the watermark backward
        and let the next late tick for the gap in between reopen an
        already-settled bucket and overwrite it via `ON CONFLICT DO
        UPDATE`."""
        for instrument_id, ts in mapping.items():
            self._closed_through[instrument_id] = max(
                self._closed_through.get(instrument_id, ts), ts
            )

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
    SELECT instrument_id FROM instruments
    WHERE source_bindings ? 'upstox_instrument_key' OR asset_class = 'PERP'
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
    """Instruments whose bars come from somewhere better than our own
    bucketing, and must not be built twice.

    Upstox-bound instruments get theirs authoritatively from `bars:*`
    (published from `marketOHLC`'s I1 entries). Perpetuals get theirs from
    Binance's klines endpoint, polled by `perp_ingestor`. In both cases
    letting the tick path also aggregate would under-count volume -- these
    ticks are price snapshots, not a trade stream -- and race the good bars
    for the same `(instrument_id, ts, interval_sec)` primary key, with the
    winner decided by arrival order."""
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


_SELECT_CRYPTO_SPOT_INSTRUMENTS = """
    SELECT instrument_id, symbol FROM instruments
    WHERE asset_class = 'CRYPTO' AND segment = 'SPOT'
"""


def _query_crypto_spot_instruments(conn: Connection) -> dict[int, str]:
    rows = conn.execute(_SELECT_CRYPTO_SPOT_INSTRUMENTS).fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


async def _startup_spot_backfill(
    conn_factory: Callable[[], Connection],
    aggregator: BarAggregator,
    redis: Redis,
    *,
    to_thread: Callable[..., Any],
    fetch: Any = None,
) -> None:
    """Seed `_closed_through` from the database and fill whatever
    elapsed while this process was down, from each crypto spot
    instrument's last stored bar to the first bucket this process can
    honestly claim in full. Runs once, awaited, before ticks are
    consumed, off the event loop thread so a slow Binance response
    never delays the first tick subscription. A REST failure for one
    instrument is logged and skipped -- it never blocks the others or
    startup itself.
    """
    # Imported here, not at module level: trading.streaming.spot_backfill
    # imports bucket_start from this module, so a top-level import here
    # would be circular (whichever of the two modules loads first would
    # find the other only partially initialized).
    from trading.streaming.spot_backfill import backfill_window, last_bar_ts

    fetch = fetch or fetch_spot_klines

    def _do() -> tuple[dict[int, datetime], list[tuple[int, Any]]]:
        conn = conn_factory()
        instruments = _query_crypto_spot_instruments(conn)
        seeded: dict[int, datetime] = {}
        announced: list[tuple[int, Any]] = []
        cutoff = bucket_start(datetime.now(UTC), INTERVAL_SECONDS)
        for instrument_id, symbol in instruments.items():
            latest = last_bar_ts(conn, instrument_id)
            if latest is None:
                continue
            seeded[instrument_id] = latest
            try:
                inserted = backfill_window(
                    conn,
                    instrument_id,
                    spot_symbol(symbol),
                    since=latest + timedelta(seconds=INTERVAL_SECONDS),
                    until=cutoff,
                    fetch=fetch,
                )
            except Exception as exc:  # noqa: BLE001 - a REST failure must never block startup
                log.warning(
                    "bar_aggregator.startup_backfill_failed",
                    instrument_id=instrument_id,
                    reason=str(exc),
                )
                continue
            _log_late_fill(instrument_id, latest, inserted)
            for kline in inserted:
                seeded[instrument_id] = max(seeded[instrument_id], kline.ts)
                announced.append((instrument_id, kline))
        return seeded, announced

    seeded, announced = await to_thread(_do)
    aggregator.seed_closed_through(seeded)
    for instrument_id, kline in announced:
        await _announce_bar(
            redis,
            instrument_id=instrument_id,
            ts=kline.ts,
            open_=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            interval_seconds=INTERVAL_SECONDS,
            source=DataSource.BINANCE_SPOT_KLINE,
        )


async def _run_tracked_backfill(
    in_flight: dict[int, asyncio.Task[None]], instrument_id: int, coro: Any
) -> None:
    """Run a backfill coroutine as a Task registered in `in_flight` for
    the duration of the call (I1a). `_write_all` checks this dict before
    writing a newer tick-aggregated bar for the same instrument, so a
    slow backfill (silence trigger or sweep) can't land behind a tick bar
    that raced ahead of it in Postgres. Removes its own entry when done,
    whether it succeeds or raises (backfill helpers already swallow their
    own exceptions, but this must never leave a stale entry behind on the
    off chance one doesn't)."""
    task = asyncio.ensure_future(coro)
    in_flight[instrument_id] = task
    try:
        await task
    finally:
        if in_flight.get(instrument_id) is task:
            del in_flight[instrument_id]


def _log_late_fill(instrument_id: int, before: datetime | None, inserted: list[Any]) -> None:
    """I1(b): if a backfill just inserted bars older than the newest bar
    already in `bars_intraday` for this instrument (captured in `before`
    right before the backfill ran), a live run's cursor (`ts > last_ts`)
    has already advanced past them and will never receive them. Nothing
    at this layer can fix that -- it can only be made visible."""
    if before is None or not inserted:
        return
    late = [k for k in inserted if k.ts < before]
    if late:
        log.warning(
            "bar_aggregator.late_fill",
            instrument_id=instrument_id,
            count=len(late),
            min_ts=min(k.ts for k in late).isoformat(),
            max_ts=max(k.ts for k in late).isoformat(),
        )


async def _silence_backfill(
    conn_factory: Callable[[], Connection],
    aggregator: BarAggregator,
    redis: Redis,
    *,
    instrument_id: int,
    symbol: str,
    since: datetime,
    until: datetime,
    to_thread: Callable[..., Any],
    fetch: Any = None,
) -> None:
    """A tick resumed after a long silence for `instrument_id` -- fetch
    and announce whatever closed minutes fell in the gap. Symmetric
    with `_startup_spot_backfill` but scoped to one instrument and one
    window, so a silence on BTC never touches ETH's cursor.

    Fired via `asyncio.create_task` from `_consume_ticks` and never
    awaited by its caller, and also called per-instrument from
    `_sweep_backfill` -- either way, nothing downstream may see this
    raise. `conn_factory()` itself can raise `psycopg.OperationalError`
    (Task 3's `ReconnectingConnection.get()` while Postgres is still
    down), so it's inside the same try as the REST call, not before it.
    """
    # Imported here, not at module level -- same reasoning as
    # _startup_spot_backfill's identical import (avoids a circular import
    # with spot_backfill, which imports bucket_start from this module).
    from trading.streaming.spot_backfill import backfill_window, last_bar_ts

    fetch = fetch or fetch_spot_klines

    def _do() -> list[Any]:
        try:
            conn = conn_factory()
            before = last_bar_ts(conn, instrument_id)
            inserted = backfill_window(
                conn, instrument_id, spot_symbol(symbol), since=since, until=until, fetch=fetch
            )
            _log_late_fill(instrument_id, before, inserted)
            return inserted
        except Exception as exc:  # noqa: BLE001 - a REST failure, or
            # backfill_conn_factory() itself raising while Postgres is
            # still down, must never kill tick consumption or the sweep
            # task that also calls this helper.
            log.warning(
                "bar_aggregator.silence_backfill_failed",
                instrument_id=instrument_id,
                reason=str(exc),
            )
            return []

    inserted = await to_thread(_do)
    if inserted:
        aggregator.seed_closed_through({instrument_id: max(k.ts for k in inserted)})
    for kline in inserted:
        await _announce_bar(
            redis,
            instrument_id=instrument_id,
            ts=kline.ts,
            open_=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            interval_seconds=INTERVAL_SECONDS,
            source=DataSource.BINANCE_SPOT_KLINE,
        )


async def _sweep_backfill(
    conn_factory: Callable[[], Connection],
    aggregator: BarAggregator,
    redis: Redis,
    *,
    window_minutes: int,
    to_thread: Callable[..., Any],
    fetch: Any = None,
    in_flight: dict[int, asyncio.Task[None]] | None = None,
) -> None:
    """Safety net: re-check the last `window_minutes` for every crypto
    spot instrument, whether or not a silence was ever detected for it.
    Catches a gap the silence trigger missed -- e.g. a tick stream that
    never fully stopped but dropped individual minutes.

    `_periodic_sweep_backfill` awaits this with no guard of its own, so
    `conn_factory()` raising here (Postgres still down) must be caught
    inside this function -- otherwise it would kill the sweep task and,
    in production, the whole aggregation loop's `asyncio.gather`.

    `until` stops one full bucket short of `now` (M1): the tick path may
    still be flushing the just-closed minute (via `_periodic_flush` or an
    `ingest()` rollover) when the sweep runs, and racing it for the same
    row is pointless -- that minute is the tick path's job, not the
    sweep's. `in_flight`, when given, registers each per-instrument call
    the same way `_consume_ticks`'s silence trigger does (I1a), so a
    concurrent tick-bar write for the same instrument waits for it."""
    fetch = fetch or fetch_spot_klines
    now = datetime.now(UTC)
    since = now - timedelta(minutes=window_minutes)
    until = bucket_start(now, INTERVAL_SECONDS) - timedelta(seconds=INTERVAL_SECONDS)

    def _instruments() -> dict[int, str]:
        try:
            return _query_crypto_spot_instruments(conn_factory())
        except Exception as exc:  # noqa: BLE001 - backfill_conn_factory()
            # raising must never kill the sweep task -- it retries on the
            # next interval instead.
            log.warning("bar_aggregator.sweep_backfill_failed", reason=str(exc))
            return {}

    instruments = await to_thread(_instruments)
    for instrument_id, symbol in instruments.items():
        coro = _silence_backfill(
            conn_factory,
            aggregator,
            redis,
            instrument_id=instrument_id,
            symbol=symbol,
            since=since,
            until=until,
            to_thread=to_thread,
            fetch=fetch,
        )
        if in_flight is None:
            await coro
        else:
            await _run_tracked_backfill(in_flight, instrument_id, coro)


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
    backfill_conn_factory: Callable[[], Connection] | None = None,
    to_thread: Callable[..., Any] = asyncio.to_thread,
    spot_fetch: Any = None,
    backfill_silence_seconds: float = 90.0,
    backfill_sweep_seconds: float = 300.0,
    backfill_sweep_window_minutes: int = 30,
    backfill_wait_timeout_seconds: float = 30.0,
    write_conn_factory: Callable[[], Connection] | None = None,
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
    if backfill_conn_factory is not None:
        # Best-effort: the DB or Binance can be unreachable when this
        # process starts. A startup backfill failure is logged and
        # swallowed here -- it must never stop live aggregation from
        # starting (per-instrument REST/backfill failures are already
        # caught inside _startup_spot_backfill; this catches the wider
        # failure of the connection/instrument-query step itself, e.g. a
        # ReconnectingConnection.get() that can't reconnect at all). The
        # spec's silence trigger and sweep (Task 6) repair the gap later.
        try:
            await _startup_spot_backfill(
                backfill_conn_factory, aggregator, redis, to_thread=to_thread, fetch=spot_fetch
            )
        except Exception as exc:  # noqa: BLE001 - a startup backfill failure must
            # never block live aggregation from starting
            log.warning("bar_aggregator.startup_backfill_failed", reason=str(exc))
    first_complete_bucket = bucket_start(datetime.now(UTC), interval_seconds) + timedelta(
        seconds=interval_seconds
    )
    # I5: main() passes a ReconnectingConnection.get here so a Postgres
    # restart doesn't wedge every tick-path write for the rest of this
    # process's life -- fetched fresh per write batch (never held), same
    # discipline backfill_conn_factory already uses. Tests that pass a
    # plain `conn` and no factory get the exact old behaviour: the same
    # static connection every time.
    get_write_conn: Callable[[], Connection] = write_conn_factory or (lambda: conn)
    excluded_instrument_ids = _query_upstox_bound_instrument_ids(get_write_conn())
    log.info(
        "bar_aggregator.excluding_tick_aggregation",
        count=len(excluded_instrument_ids),
        instrument_ids=sorted(excluded_instrument_ids),
    )
    written = 0
    done = asyncio.Event()
    # Per-instrument in-flight backfill tasks (silence trigger, sweep, and
    # the targeted startup-gap backfill below) -- I1a: a tick bar write
    # for an instrument with a backfill still running waits for it first,
    # so Postgres keeps receiving ts in order for that instrument instead
    # of a slow backfill landing behind a tick bar that raced ahead of it.
    in_flight_backfills: dict[int, asyncio.Task[None]] = {}

    async def _backfill_discarded_startup_bucket(instrument_id: int, bucket: datetime) -> None:
        """I2: the bucket already running at startup is discarded above
        as not honestly complete -- but by the time it closes, Binance's
        kline for that exact minute has too. A targeted backfill for
        `[bucket, bucket + interval_seconds)`, registered in
        `in_flight_backfills` like any other backfill, fills it in before
        the next tick bar for this instrument is written."""
        if backfill_conn_factory is None:
            return
        try:
            symbol = (
                await to_thread(lambda: _query_crypto_spot_instruments(backfill_conn_factory()))
            ).get(instrument_id)
        except Exception as exc:  # noqa: BLE001 - backfill_conn_factory() raising
            # (Postgres still down) must never block tick consumption --
            # the sweep repairs the gap later.
            log.warning(
                "bar_aggregator.startup_gap_backfill_failed",
                instrument_id=instrument_id,
                reason=str(exc),
            )
            return
        if symbol is None:
            return
        await _run_tracked_backfill(
            in_flight_backfills,
            instrument_id,
            _silence_backfill(
                backfill_conn_factory,
                aggregator,
                redis,
                instrument_id=instrument_id,
                symbol=symbol,
                since=bucket,
                until=bucket + timedelta(seconds=interval_seconds),
                to_thread=to_thread,
                fetch=spot_fetch,
            ),
        )

    async def _write_all(closed_bars: list[ClosedBar]) -> None:
        nonlocal written
        # Fetched once per batch (I5), not held for the process's life --
        # a ReconnectingConnection.get() here repairs a dead connection
        # (e.g. a Postgres restart) before the next batch's writes.
        write_conn = get_write_conn()
        for closed in closed_bars:
            backfill_task = in_flight_backfills.get(closed.instrument_id)
            if backfill_task is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(backfill_task), timeout=backfill_wait_timeout_seconds
                    )
                except Exception as exc:  # noqa: BLE001 - a timeout (the common case)
                    # or any other failure waiting on the backfill must never
                    # block writing the newer tick bar -- log and proceed.
                    log.warning(
                        "bar_aggregator.backfill_wait_failed",
                        instrument_id=closed.instrument_id,
                        reason=str(exc),
                    )
            if closed.bucket < first_complete_bucket:
                log.info(
                    "bar_aggregator.discarding_partial_startup_bar",
                    instrument_id=closed.instrument_id,
                    bucket=closed.bucket.isoformat(),
                )
                asyncio.create_task(
                    _backfill_discarded_startup_bucket(closed.instrument_id, closed.bucket)
                )
                continue
            try:
                write_closed_bar(
                    write_conn, closed, interval_seconds=interval_seconds, source=source
                )
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
        write_conn = get_write_conn()  # fetched per batch (I5), same as _write_all
        try:
            write_upstox_bar(write_conn, bar, interval_seconds=interval_seconds)
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

    last_tick_at: dict[int, datetime] = {}

    async def _consume_ticks() -> None:
        dropped_before = 0
        async for message in resilient_messages(redis, patterns=[pattern]):
            if message["type"] != "pmessage":
                continue
            tick = _parse_tick(message["data"])
            if tick is None:
                continue
            if tick.instrument_id in excluded_instrument_ids:
                continue
            now = datetime.now(UTC)
            previous = last_tick_at.get(tick.instrument_id)
            last_tick_at[tick.instrument_id] = now
            if (
                backfill_conn_factory is not None
                and previous is not None
                and (now - previous).total_seconds() > backfill_silence_seconds
            ):
                # Off the event loop, like the startup and sweep paths:
                # a DB round-trip here would stall every instrument's ticks.
                try:
                    symbol = (
                        await to_thread(
                            lambda: _query_crypto_spot_instruments(backfill_conn_factory())
                        )
                    ).get(tick.instrument_id)
                except Exception as exc:  # noqa: BLE001 - backfill_conn_factory()
                    # raising (Postgres still down) must never end tick
                    # consumption -- the sweep repairs the gap later.
                    log.warning(
                        "bar_aggregator.silence_backfill_failed",
                        instrument_id=tick.instrument_id,
                        reason=str(exc),
                    )
                    symbol = None
                if symbol is not None:
                    asyncio.create_task(
                        _run_tracked_backfill(
                            in_flight_backfills,
                            tick.instrument_id,
                            _silence_backfill(
                                backfill_conn_factory,
                                aggregator,
                                redis,
                                instrument_id=tick.instrument_id,
                                symbol=symbol,
                                since=previous,
                                until=now,
                                to_thread=to_thread,
                                fetch=spot_fetch,
                            ),
                        )
                    )
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

    async def _consume_bars() -> None:
        async for message in resilient_messages(redis, patterns=[bars_pattern]):
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

    async def _periodic_sweep_backfill() -> None:
        while not done.is_set():
            await sleep(backfill_sweep_seconds)
            if backfill_conn_factory is None:
                continue
            await _sweep_backfill(
                backfill_conn_factory,
                aggregator,
                redis,
                window_minutes=backfill_sweep_window_minutes,
                to_thread=to_thread,
                fetch=spot_fetch,
                in_flight=in_flight_backfills,
            )

    consumer = asyncio.create_task(_consume_ticks())
    bars_consumer = asyncio.create_task(_consume_bars())
    flusher = asyncio.create_task(_periodic_flush())
    sweep_task = asyncio.create_task(_periodic_sweep_backfill())
    try:
        if max_bars_written is None:
            await asyncio.gather(consumer, bars_consumer, flusher, sweep_task)
        else:
            await done.wait()
    finally:
        consumer.cancel()
        bars_consumer.cancel()
        flusher.cancel()
        sweep_task.cancel()
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop, so a
        # caller closing `redis` from a *different* asyncio.run() call later
        # (as short-lived test runs do) never hits a stale, cross-loop
        # connection.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()

    start_heartbeat_thread(
        lambda: _sync_redis.Redis.from_url(settings.redis_url, decode_responses=True),
        "bar_aggregator",
        ttl=settings.heartbeat_ttl_seconds,
        every=settings.heartbeat_refresh_seconds,
    )

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    # I5: a ReconnectingConnection, not a plain psycopg.connect -- the
    # tick path used to hold one connection for the process's whole life,
    # so a Postgres/colima restart broke every write until the process
    # itself restarted. autocommit=True (ReconnectingConnection's
    # default): each closed bar is its own independent unit of work over
    # a long-running connection -- unlike seed_instruments.py's one-shot
    # atomic batch, there is no reason one bar's write should roll back
    # because a later bar's write fails. This is also what keeps
    # write_closed_bar() safe to call against `db_conn` in tests without
    # any special-casing: it never commits itself either way.
    write_conn = ReconnectingConnection(settings.database_url)
    log.info("bar_aggregator.starting", interval_seconds=INTERVAL_SECONDS)
    try:
        # One long-lived connection for every backfill thread, reconnecting
        # if Postgres restarts. The backfill helpers never close what the
        # factory returns, which is only correct because it is shared.
        backfill_conn = ReconnectingConnection(settings.database_url)
        asyncio.run(
            run_aggregation_loop(
                redis,
                write_conn.get(),
                write_conn_factory=write_conn.get,
                backfill_conn_factory=backfill_conn.get,
                backfill_silence_seconds=settings.backfill_silence_seconds,
                backfill_sweep_seconds=settings.backfill_sweep_seconds,
                backfill_sweep_window_minutes=settings.backfill_sweep_window_minutes,
            )
        )
    except KeyboardInterrupt:
        log.info("bar_aggregator.interrupted")


if __name__ == "__main__":
    main()
