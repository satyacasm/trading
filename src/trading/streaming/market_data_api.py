"""REST endpoints for the charts + watchlist web UI: historical candles and
the (single, global -- no auth, no user_id, see this plan's Global
Constraints) watchlist. Mounted onto `gateway.py`'s FastAPI app rather than
defined there directly, keeping that file from accumulating responsibilities
unrelated to WebSocket tick fan-out.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

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


class StringCandle(BaseModel):
    """OHLCV as decimal text.

    A price is money, and JSON has no decimal type. A client that must not
    round -- anything sizing a position, and every indicator computed from
    these bars -- asks for this shape instead.
    """

    ts: datetime
    open: str
    high: str
    low: str
    close: str
    volume: str


class StringCandlesResponse(BaseModel):
    instrument_id: int
    interval: str
    candles: list[StringCandle]


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


_DAILY_CANDLES_SQL = """
    SELECT ts, open, high, low, close, volume
    FROM bars_daily
    WHERE instrument_id = %s
    ORDER BY ts DESC
    LIMIT %s
"""

_Row = tuple[datetime, Decimal, Decimal, Decimal, Decimal, Decimal]


def _decimal(value: object) -> Decimal:
    """Whatever the driver returned, as a Decimal. `None` volume is zero.

    `str()` first for a float: `Decimal(0.1)` is 0.1000000000000000055…,
    while `Decimal(str(0.1))` is the 0.1 the database meant.
    """
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fetch_bucketed_rows(
    conn: Connection, instrument_id: int, bucket: str, limit: int
) -> list[_Row]:
    rows = conn.execute(_BUCKETED_CANDLES_SQL, (bucket, instrument_id, limit)).fetchall()
    built = [
        (ts, _decimal(o), _decimal(h), _decimal(low), _decimal(c), _decimal(v))
        for ts, o, h, low, c, v in rows
    ]
    return list(reversed(built))


def _fetch_daily_rows(conn: Connection, instrument_id: int, limit: int) -> list[_Row]:
    rows = conn.execute(_DAILY_CANDLES_SQL, (instrument_id, limit)).fetchall()
    built = [
        (ts, _decimal(o), _decimal(h), _decimal(low), _decimal(c), _decimal(v))
        for ts, o, h, low, c, v in rows
    ]
    return list(reversed(built))


def _as_float_candles(rows: list[_Row]) -> list[Candle]:
    return [
        Candle(
            ts=ts,
            open=float(o),
            high=float(h),
            low=float(low),
            close=float(c),
            volume=float(v),
        )
        for ts, o, h, low, c, v in rows
    ]


def _as_string_candles(rows: list[_Row]) -> list[StringCandle]:
    return [
        StringCandle(ts=ts, open=str(o), high=str(h), low=str(low), close=str(c), volume=str(v))
        for ts, o, h, low, c, v in rows
    ]


@router.get("/candles/{instrument_id}", response_model=None)
def get_candles(
    instrument_id: int,
    interval: str = Query(...),
    limit: int = _DEFAULT_LIMIT,
    precision: Literal["float", "string"] = "float",
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> CandlesResponse | StringCandlesResponse:
    """`precision` defaults to `float`, which is what the web charts read.

    `string` is for clients that must not round: money has no float
    representation, and an indicator or a position size computed from a
    rounded close is wrong in a way nothing downstream can detect.
    """
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
        rows = _fetch_daily_rows(conn, instrument_id, limit)
    else:
        bucket = _INTERVAL_BUCKETS.get(interval, "1 day")
        rows = _fetch_bucketed_rows(conn, instrument_id, bucket, limit)

    if precision == "string":
        return StringCandlesResponse(
            instrument_id=instrument_id, interval=interval, candles=_as_string_candles(rows)
        )
    return CandlesResponse(
        instrument_id=instrument_id, interval=interval, candles=_as_float_candles(rows)
    )
