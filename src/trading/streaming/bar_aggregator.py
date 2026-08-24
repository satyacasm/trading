"""Bucketing logic and Postgres write path for the crypto bar aggregator.

Entry point (added in a later step of this plan, Task 3):
`python -m trading.streaming.bar_aggregator`. See the design doc
(docs/superpowers/specs/2026-08-24-bar-aggregator-design.md) for why this
exists: turning crypto_ingestor's live ticks into real 1-minute bars in
`bars_intraday` -- data the replay service needs next.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from psycopg import Connection

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
    conn: Connection, closed: ClosedBar, *, interval_seconds: int = INTERVAL_SECONDS
) -> None:
    """Upsert one closed bar. Never calls `conn.commit()` -- see this plan's
    Global Constraints for why (keeps this function test-safe against
    `db_conn`'s rollback-at-teardown; production commits via an
    `autocommit=True` connection instead)."""
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
            DataSource.BINANCE_WS.value,
        ),
    )
