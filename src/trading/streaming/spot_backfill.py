"""Fill the gap a Wi-Fi outage leaves in crypto spot bars.

Modelled on `trading.streaming.perp_backfill`, but bounded rather than
walk-to-now: a live outage window is always `[since, until)` (design
§3), never "everything since the beginning". Pure Postgres here --
publishing the `closed_bars:*` notification for whatever this inserts
is `bar_aggregator`'s job (it already owns that channel and the
`_announce_bar` helper), not this module's.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from psycopg import Connection

from trading.contracts import DataSource
from trading.sources.binance_spot import SpotKline, fetch_spot_klines
from trading.streaming.bar_aggregator import bucket_start

__all__ = ["backfill_window", "last_bar_ts"]

_UPSERT = """
    INSERT INTO bars_intraday
        (instrument_id, ts, interval_sec, open, high, low, close, volume, trades, source)
    VALUES (%s, %s, 60, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO NOTHING
    RETURNING ts
"""


def last_bar_ts(conn: Connection, instrument_id: int) -> datetime | None:
    row = conn.execute(
        "SELECT max(ts) FROM bars_intraday WHERE instrument_id = %s AND interval_sec = 60",
        (instrument_id,),
    ).fetchone()
    return None if row is None else row[0]


def backfill_window(
    conn: Connection,
    instrument_id: int,
    symbol: str,
    since: datetime,
    until: datetime,
    *,
    fetch: Callable[..., list[SpotKline]] = fetch_spot_klines,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[SpotKline]:
    """Fetch and write every closed minute in `[since, until)`, clamped
    so the forming minute is never dispatched. Returns only the klines
    actually inserted -- a tick-built row for the same minute is left
    alone (`ON CONFLICT DO NOTHING`), because a strategy may already
    have been sent it."""
    clamped_until = min(until, bucket_start(now(), 60))
    if clamped_until <= since:
        return []
    klines = fetch(
        symbol,
        start_ms=int(since.timestamp() * 1000),
        end_ms=int(clamped_until.timestamp() * 1000),
    )
    inserted_ts: set[datetime] = set()
    for kline in klines:
        row = conn.execute(
            _UPSERT,
            (
                instrument_id,
                kline.ts,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
                kline.trades,
                DataSource.BINANCE_SPOT_KLINE.value,
            ),
        ).fetchone()
        if row is not None:
            inserted_ts.add(row[0])
    return [k for k in klines if k.ts in inserted_ts]
