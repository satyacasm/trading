"""Entry point: `uvicorn trading.streaming.gateway:app`.

Holds browser WebSocket connections and fans out ticks published by
crypto_ingestor to whichever instruments each connection has asked for.
Each connection owns its own Redis subscription lifetime -- see the plan's
Task 5 for why that's the right scope, not a shared connection manager.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
import structlog
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from psycopg import Connection
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
from trading.streaming import market_data_api
from trading.streaming.db import get_db_connection
from trading.streaming.seed_instruments import crypto_canonical_keys, seed_crypto_instruments
from trading.streaming.seed_upstox_instruments import (
    seed_upstox_instrument_keys,
    upstox_canonical_keys,
)

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Seed the fixed crypto and Upstox-equity watchlists exactly once per
    process, at startup -- never per request.

    `GET /instruments` used to call these seed functions' upserts on every
    request, taking a row lock each time. Two concurrent requests could then
    deadlock the single blocking psycopg connection running on the event
    loop thread: one held an uncommitted upsert while the other blocked on
    its row lock, also on the event loop, so the first could never reach
    `conn.commit()` (observed live, unrecoverable). Moving the writes here
    and turning the route below into a plain `def` removes both halves of
    that defect.

    Tests override `get_db_connection` with their own rolled-back
    transaction (`app.dependency_overrides`); honoring that override here,
    instead of unconditionally opening a fresh connection to
    `get_settings().database_url`, keeps startup seeding inside that same
    test transaction rather than writing to a real database.
    """
    override = app.dependency_overrides.get(get_db_connection)
    if override is not None:
        conn = override()
        seed_crypto_instruments(conn)
        seed_upstox_instrument_keys(conn)
    else:
        conn = psycopg.connect(get_settings().database_url, autocommit=False)
        try:
            seed_crypto_instruments(conn)
            seed_upstox_instrument_keys(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(market_data_api.router)

_STATIC_ROOT = Path(__file__).parent / "static"


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_ROOT / "proof.html")


class InstrumentSummary(BaseModel):
    instrument_id: int
    symbol: str
    asset_class: str
    exchange: str


@app.get("/instruments", response_model=list[InstrumentSummary])
def instruments(conn: Connection = Depends(get_db_connection)) -> list[InstrumentSummary]:  # noqa: B008
    # Plain `def`, not `async def`: psycopg here is a blocking call, and
    # FastAPI dispatches plain `def` routes to a threadpool instead of
    # running them on the event loop -- the same pattern every route in
    # `market_data_api.py` already uses. Seeding happens once at process
    # startup (see `lifespan` above), so this route only ever reads: it
    # resolves the watchlists' canonical keys in-process (no DB round trip,
    # no write) and looks up the matching rows by `canonical_key`, rather
    # than the whole `instruments` table.
    canonical_keys = crypto_canonical_keys() + upstox_canonical_keys()
    if not canonical_keys:
        return []
    rows = conn.execute(
        "SELECT instrument_id, symbol, asset_class, exchange FROM instruments "
        "WHERE canonical_key = ANY(%s)",
        (canonical_keys,),
    ).fetchall()
    return [
        InstrumentSummary(instrument_id=row[0], symbol=row[1], asset_class=row[2], exchange=row[3])
        for row in rows
    ]


# One connection-lifetime pattern subscription instead of per-instrument
# subscribe()/unsubscribe(). redis-py's PubSub.subscribe()/unsubscribe()
# only *write* the SUBSCRIBE/UNSUBSCRIBE command and return -- they never
# wait for Redis's confirmation (see PubSub.execute_command). Combined with
# TestClient's WebSocket harness, whose ws.send_json() returns as soon as
# the message is queued for the ASGI app rather than once the app has
# processed it, that made a per-instrument subscribe/unsubscribe design
# race against a real, separately-connected publisher: the publish could
# reach Redis before the matching SUBSCRIBE/UNSUBSCRIBE had. Subscribing
# once to the whole `ticks:*` pattern and filtering per instrument_id
# in-process removes that race entirely -- after the single upfront
# psubscribe, "should this connection forward this tick" is decided
# synchronously against `subscribed`, with no further Redis round trip in
# the loop, so there's nothing left to race the test's publish() against.
# It also makes `PubSub.listen()`'s `while self.subscribed:` gate a
# non-issue: one live pattern subscription is never empty.
_TICK_PATTERN = "ticks:*"


async def _forward_loop(pubsub: PubSub, websocket: WebSocket, subscribed: set[int]) -> None:
    async for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue
        try:
            instrument_id = int(message["channel"].split(":", 1)[1])
        except (IndexError, ValueError):
            continue
        if instrument_id not in subscribed:
            continue
        await websocket.send_text(message["data"])


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    redis: Redis = Redis.from_url(get_settings().redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.psubscribe(_TICK_PATTERN)
    subscribed: set[int] = set()
    listener = asyncio.create_task(_forward_loop(pubsub, websocket, subscribed))

    try:
        while True:
            try:
                message = await websocket.receive_json()
            except WebSocketDisconnect:
                raise
            except Exception:  # noqa: BLE001 - one bad client frame is skipped, never fatal
                log.warning("gateway.malformed_client_message", exc_info=True)
                continue
            if not isinstance(message, dict):
                log.warning("gateway.malformed_client_message", reason="not a JSON object")
                continue
            action = message.get("action")
            instrument_id = message.get("instrument_id")
            # bool is a subclass of int in Python, so exclude it explicitly --
            # otherwise `{"instrument_id": true}` would silently pass as 1.
            if not isinstance(instrument_id, int) or isinstance(instrument_id, bool):
                log.warning("gateway.bad_instrument_id", value=repr(instrument_id))
                continue
            if action == "subscribe":
                subscribed.add(instrument_id)
            elif action == "unsubscribe":
                subscribed.discard(instrument_id)
    except WebSocketDisconnect:
        pass
    finally:
        listener.cancel()
        try:
            await pubsub.punsubscribe()
            # redis-py's PubSub.aclose (unlike Redis.aclose) ships with no
            # type annotations at all -- a real upstream stub gap, not a
            # mistake here.
            await pubsub.aclose()  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the handler
            log.debug("gateway.pubsub_cleanup_failed", exc_info=True)
        await redis.aclose()
