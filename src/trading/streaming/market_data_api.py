"""REST endpoints for the charts + watchlist web UI: historical candles and
the (single, global -- no auth, no user_id, see this plan's Global
Constraints) watchlist. Mounted onto `gateway.py`'s FastAPI app rather than
defined there directly, keeping that file from accumulating responsibilities
unrelated to WebSocket tick fan-out.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
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
    last_price: Decimal | None
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
