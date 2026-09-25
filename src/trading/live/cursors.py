"""Per-run, per-instrument delivery position (design §4).

Redis is a notification, Postgres is the record: a `closed_bars:*`
message only means "new bars may exist". Each live run keeps, per
instrument, the timestamp of the last bar it was sent, and every
notification (or a timer, if none arrives) the supervisor asks this
module for everything after that timestamp, oldest first. One
mechanism covers a missed message, an aggregator restart, and a
supervisor restart -- duplicates cannot occur because the cursor only
moves forward.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg import Connection

__all__ = ["PendingBar", "advance_cursor", "pending_bars"]


@dataclass(frozen=True)
class PendingBar:
    frame: dict[str, Any]
    catchup: bool


_SELECT_PENDING = """
    SELECT b.instrument_id, b.ts, b.open, b.high, b.low, b.close, b.volume
    FROM bars_intraday b
    JOIN unnest(%(instrument_ids)s::bigint[]) AS u(instrument_id)
        ON u.instrument_id = b.instrument_id
    LEFT JOIN live_run_cursors c
        ON c.live_run_id = %(live_run_id)s AND c.instrument_id = b.instrument_id
    WHERE b.interval_sec = 60
      AND b.ts > COALESCE(c.last_ts, date_trunc('minute', %(started_at)s))
      AND b.ts >= %(floor)s
    ORDER BY b.ts, b.instrument_id
"""

_COUNT_SKIPPED = """
    SELECT count(*)
    FROM bars_intraday b
    JOIN unnest(%(instrument_ids)s::bigint[]) AS u(instrument_id)
        ON u.instrument_id = b.instrument_id
    LEFT JOIN live_run_cursors c
        ON c.live_run_id = %(live_run_id)s AND c.instrument_id = b.instrument_id
    WHERE b.interval_sec = 60
      AND b.ts > COALESCE(c.last_ts, date_trunc('minute', %(started_at)s))
      AND b.ts < %(floor)s
"""

_UPSERT_CURSOR = """
    INSERT INTO live_run_cursors (live_run_id, instrument_id, last_ts)
    VALUES (%s, %s, %s)
    ON CONFLICT (live_run_id, instrument_id) DO UPDATE
    SET last_ts = GREATEST(live_run_cursors.last_ts, EXCLUDED.last_ts)
"""


def _frame(
    instrument_id: int, ts: datetime, open_, high, low, close, volume, catchup: bool
) -> dict[str, Any]:
    """Identical shape to `live.supervisor._bar_frame`, plus `catchup`."""
    return {
        "instrument_id": instrument_id,
        "ts": ts.isoformat(),
        "interval_sec": 60,
        "open": str(open_),
        "high": str(high),
        "low": str(low),
        "close": str(close),
        "volume": None if volume is None else str(volume),
        "catchup": catchup,
    }


def pending_bars(
    conn: Connection,
    live_run_id: int,
    instrument_ids: Iterable[int],
    started_at: datetime,
    *,
    now: datetime,
    catchup_after: timedelta,
    replay_cap: timedelta,
) -> tuple[list[PendingBar], str | None]:
    """Every bar this run has not yet been sent, oldest first, ordered
    `(ts, instrument_id)` so a tie between instruments is deterministic.
    A bar older than `now - replay_cap` is skipped entirely (never
    delivered, never counted as catchup) and folded into the returned
    gap note; a bar whose close (`ts + 60s`) is more than `catchup_after`
    before `now` is delivered with `catchup=True`.
    """
    ids = list(instrument_ids)
    if not ids:
        return [], None
    floor = now - replay_cap
    params = {
        "instrument_ids": ids,
        "live_run_id": live_run_id,
        "started_at": started_at,
        "floor": floor,
    }
    rows = conn.execute(_SELECT_PENDING, params).fetchall()
    skipped = conn.execute(_COUNT_SKIPPED, params).fetchone()[0]

    pending: list[PendingBar] = []
    for instrument_id, ts, open_, high, low, close, volume in rows:
        catchup = (now - (ts + timedelta(seconds=60))) > catchup_after
        pending.append(
            PendingBar(
                frame=_frame(int(instrument_id), ts, open_, high, low, close, volume, catchup),
                catchup=catchup,
            )
        )
    gap_note = (
        None
        if not skipped
        else f"replay cap: skipped {skipped} bar(s) older than {floor.isoformat()}"
    )
    return pending, gap_note


def advance_cursor(conn: Connection, live_run_id: int, instrument_id: int, ts: datetime) -> None:
    """Upsert the cursor, never moving it backwards -- a crash between a
    strategy's reply and this write replays at most one bar, identical
    in value, which the runtime already treats as a harmless
    redelivery."""
    conn.execute(_UPSERT_CURSOR, (live_run_id, instrument_id, ts))
