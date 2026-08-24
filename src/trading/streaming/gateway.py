"""Entry point: `uvicorn trading.streaming.gateway:app`.

Holds browser WebSocket connections and fans out ticks published by
crypto_ingestor to whichever instruments each connection has asked for.
Each connection owns its own Redis subscription lifetime -- see the plan's
Task 5 for why that's the right scope, not a shared connection manager.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import psycopg
import structlog
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from psycopg import Connection
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
from trading.streaming.seed_instruments import seed_crypto_instruments

log = structlog.get_logger(__name__)

app = FastAPI()

_STATIC_ROOT = Path(__file__).parent / "static"


def get_db_connection() -> Iterator[Connection]:
    """A real connection per request. Tests override this dependency with
    their own `db_conn` fixture (`app.dependency_overrides[get_db_connection]
    = lambda: db_conn`) so `/instruments` reads inside the same rolled-back
    test transaction instead of committing a second, real connection."""
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_ROOT / "proof.html")


@app.get("/instruments")
async def instruments(conn: Connection = Depends(get_db_connection)) -> dict[str, int]:  # noqa: B008
    # psycopg here is a synchronous, blocking call inside an async route --
    # an accepted simplification for this endpoint (called once per page
    # load, not a hot path); see the design doc's scope notes.
    return seed_crypto_instruments(conn)


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
            message = await websocket.receive_json()
            action = message.get("action")
            instrument_id = message.get("instrument_id")
            if instrument_id is None:
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
