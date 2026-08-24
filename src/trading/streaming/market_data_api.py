"""REST endpoints for the charts + watchlist web UI: historical candles and
the (single, global -- no auth, no user_id, see this plan's Global
Constraints) watchlist. Mounted onto `gateway.py`'s FastAPI app rather than
defined there directly, keeping that file from accumulating responsibilities
unrelated to WebSocket tick fan-out.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import Connection
from pydantic import BaseModel

from trading.streaming.db import get_db_connection

router = APIRouter()


class WatchlistItem(BaseModel):
    instrument_id: int
    symbol: str
    asset_class: str
    exchange: str
    added_at: datetime
    last_price: float | None
    last_ts: datetime | None


class AddWatchlistRequest(BaseModel):
    instrument_id: int


_GET_WATCHLIST_SQL = """
    SELECT
        w.instrument_id, i.symbol, i.asset_class, i.exchange, w.added_at,
        latest.close AS last_price, latest.ts AS last_ts
    FROM watchlists w
    JOIN instruments i ON i.instrument_id = w.instrument_id
    LEFT JOIN LATERAL (
        SELECT close, ts FROM bars_intraday b
        WHERE b.instrument_id = w.instrument_id
        ORDER BY b.ts DESC
        LIMIT 1
    ) latest ON true
    ORDER BY w.added_at
"""


@router.get("/watchlist", response_model=list[WatchlistItem])
def get_watchlist(conn: Connection = Depends(get_db_connection)) -> list[WatchlistItem]:  # noqa: B008
    rows = conn.execute(_GET_WATCHLIST_SQL).fetchall()
    return [
        WatchlistItem(
            instrument_id=row[0],
            symbol=row[1],
            asset_class=row[2],
            exchange=row[3],
            added_at=row[4],
            last_price=row[5],
            last_ts=row[6],
        )
        for row in rows
    ]


@router.post("/watchlist")
def add_to_watchlist(
    body: AddWatchlistRequest,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> dict[str, bool]:
    exists = conn.execute(
        "SELECT 1 FROM instruments WHERE instrument_id = %s", (body.instrument_id,)
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={body.instrument_id}"
        )
    conn.execute(
        "INSERT INTO watchlists (instrument_id) VALUES (%s) ON CONFLICT (instrument_id) DO NOTHING",
        (body.instrument_id,),
    )
    return {"ok": True}


@router.delete("/watchlist/{instrument_id}")
def remove_from_watchlist(
    instrument_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> dict[str, bool]:
    conn.execute("DELETE FROM watchlists WHERE instrument_id = %s", (instrument_id,))
    return {"ok": True}


class Candle(BaseModel):
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class CandlesResponse(BaseModel):
    instrument_id: int
    interval: str
    candles: list[Candle]


_INTERVAL_BUCKETS: dict[str, str] = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
}
_VALID_INTERVALS = frozenset({*_INTERVAL_BUCKETS, "1d"})
_DEFAULT_LIMIT = 300

_BUCKETED_CANDLES_SQL = """
    SELECT
        time_bucket(%s::interval, ts) AS bucket_ts,
        first(open, ts) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, ts) AS close,
        sum(volume) AS volume
    FROM bars_intraday
    WHERE instrument_id = %s AND interval_sec = 60
    GROUP BY bucket_ts
    ORDER BY bucket_ts DESC
    LIMIT %s
"""


def _fetch_bucketed_candles(
    conn: Connection, instrument_id: int, bucket: str, limit: int
) -> list[Candle]:
    rows = conn.execute(_BUCKETED_CANDLES_SQL, (bucket, instrument_id, limit)).fetchall()
    candles = [
        Candle(
            ts=ts,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=float(volume or Decimal(0)),
        )
        for ts, open_, high, low, close, volume in rows
    ]
    return list(reversed(candles))


_DAILY_CANDLES_SQL = """
    SELECT ts, open, high, low, close, volume
    FROM bars_daily
    WHERE instrument_id = %s
    ORDER BY ts DESC
    LIMIT %s
"""


def _fetch_daily_candles(conn: Connection, instrument_id: int, limit: int) -> list[Candle]:
    rows = conn.execute(_DAILY_CANDLES_SQL, (instrument_id, limit)).fetchall()
    candles = [
        Candle(
            ts=ts,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=float(Decimal(volume) if volume is not None else Decimal(0)),
        )
        for ts, open_, high, low, close, volume in rows
    ]
    return list(reversed(candles))


@router.get("/candles/{instrument_id}", response_model=CandlesResponse)
def get_candles(
    instrument_id: int,
    interval: str = Query(...),
    limit: int = _DEFAULT_LIMIT,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> CandlesResponse:
    if interval not in _VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid interval {interval!r}; expected one of {sorted(_VALID_INTERVALS)}",
        )
    row = conn.execute(
        "SELECT asset_class FROM instruments WHERE instrument_id = %s", (instrument_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )
    asset_class = row[0]

    if interval == "1d" and asset_class != "CRYPTO":
        candles = _fetch_daily_candles(conn, instrument_id, limit)
    else:
        bucket = _INTERVAL_BUCKETS.get(interval, "1 day")
        candles = _fetch_bucketed_candles(conn, instrument_id, bucket, limit)

    return CandlesResponse(instrument_id=instrument_id, interval=interval, candles=candles)
