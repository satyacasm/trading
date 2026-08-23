"""Upstox market-data WebSocket capture loop.

Governing principle (spec 5.6): record raw, parse later. This module never
interprets a frame's contents — it authorises, subscribes, and then hands
every frame it receives to `RecordingSession.write_frame` unchanged.

Fault handling:
  * Connection-level failure (auth, subscribe, or the socket dying mid-
    stream) is NOT fatal: it is recorded as a gap (`note_disconnect`), then
    the loop backs off exponentially (capped at `max_backoff_seconds`) and
    reconnects (`note_connect`).
  * A single frame the transport hands us in an unexpected shape (ruling
    R2x: "malformed") is NOT fatal either, and is NOT dropped: its raw bytes
    are still written to the frame file, and the anomaly is logged with a
    reason so a future, smarter parser (and a human) can find it.

Nothing here does real network I/O directly against a hardcoded client —
the connection is supplied by a `feed_factory` callable so the loop can be
driven by a fake in tests without touching the network.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx
import structlog
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

from trading.recorder.session import RecordingSession

log = structlog.get_logger(__name__)

Frame = bytes | str | object
"""What one iteration of an `UpstoxFeed` may yield.

Real Upstox frames are binary (`bytes`); `str` is accepted because some
WebSocket clients decode text frames automatically. Anything else is treated
as a malformed frame per ruling R2x: its `repr()` is captured as the raw
payload and an anomaly is logged, but the loop never crashes over it.
"""


class UpstoxFeed(Protocol):
    """One live connection: authorise, subscribe, then stream frames.

    A real implementation wraps a `websockets` connection to the Upstox feed
    endpoint. Tests supply a fake that yields a scripted sequence of frames
    and, optionally, raises to simulate a dropped connection.
    """

    async def authorize(self) -> None:
        """Perform the auth handshake. Raise on failure."""

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        """Request `instrument_keys`; return the keys the broker acknowledged."""

    def __aiter__(self) -> AsyncIterator[Frame]:
        """Yield raw frames as they arrive. May raise to signal disconnect."""

    async def aclose(self) -> None:
        """Best-effort close; errors here must never propagate."""


FeedFactory = Callable[[], UpstoxFeed]
Clock = Callable[[], datetime]
Sleeper = Callable[[float], Awaitable[None]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _coerce_frame(message: Frame) -> tuple[bytes, str | None]:
    """Return `(raw_bytes_to_persist, anomaly_reason_or_None)`.

    Never raises: even a frame of a shape we don't recognise still gets a
    best-effort raw representation captured, because record raw, parse
    later means the frame we couldn't interpret is the one most worth
    keeping.
    """
    if isinstance(message, bytes | bytearray):
        return bytes(message), None
    if isinstance(message, str):
        return message.encode("utf-8"), None
    return repr(message).encode("utf-8"), f"unexpected frame type: {type(message).__name__}"


async def run_recording_loop(
    session: RecordingSession,
    feed_factory: FeedFactory,
    *,
    universe: list[str],
    until: datetime,
    heartbeat_interval: timedelta = timedelta(minutes=1),
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
    clock: Clock = _utcnow,
    sleep: Sleeper = _default_sleep,
) -> None:
    """Capture frames from `feed_factory()` into `session` until `until`.

    Reconnects with exponential backoff (capped at `max_backoff_seconds`) on
    any failure — auth, subscribe, or a broken stream — and records each
    such failure as a gap in the session manifest. Exits cleanly, calling
    `session.close()`, once `clock()` reaches `until`.
    """
    backoff = initial_backoff_seconds
    last_heartbeat = clock()

    while clock() < until:
        feed = feed_factory()
        try:
            await feed.authorize()
            session.note_connect()
            backoff = initial_backoff_seconds

            acknowledged = await feed.subscribe(universe)
            session.record_subscriptions(requested=universe, acknowledged=acknowledged)

            async for message in feed:
                if clock() >= until:
                    break

                raw, anomaly_reason = _coerce_frame(message)
                session.write_frame(raw)
                if anomaly_reason is not None:
                    log.warning("recorder.malformed_frame", reason=anomaly_reason)
                    session.note_anomaly(anomaly_reason)

                now = clock()
                if now - last_heartbeat >= heartbeat_interval:
                    session.heartbeat()
                    last_heartbeat = now
        except Exception as exc:  # noqa: BLE001 - any failure here is a gap, not a crash
            log.warning("recorder.disconnected", reason=str(exc))
            session.note_disconnect(str(exc))
            wait = min(backoff, max_backoff_seconds)
            await sleep(wait)
            backoff = min(backoff * 2, max_backoff_seconds)
        finally:
            try:
                await feed.aclose()
            except Exception:  # noqa: BLE001 - closing must never itself crash the loop
                log.debug("recorder.close_failed", exc_info=True)

    session.close()


AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"


class LiveUpstoxFeed:
    """Real `UpstoxFeed`: talks to the actual Upstox V3 market-data feed.

    Authorises over HTTPS to obtain a one-time redirect URI, then opens a
    WebSocket to that URI and streams whatever bytes arrive completely
    unparsed — including Upstox's Protobuf-encoded response frames. Decoding
    them is explicitly out of scope: record raw, parse later.

    This class does real network I/O and is therefore never exercised by
    the test suite (no network in tests, per the addendum). Its behaviour
    against the live feed is unverified until an operator runs it with real
    credentials — see the operator section of the task report.
    """

    def __init__(self, access_token: str, *, http_timeout_seconds: float = 10.0) -> None:
        self._access_token = access_token
        self._http_timeout_seconds = http_timeout_seconds
        self._connection: ClientConnection | None = None

    async def authorize(self) -> None:
        async with httpx.AsyncClient(timeout=self._http_timeout_seconds) as client:
            response = await client.get(
                AUTHORIZE_URL,
                headers={
                    "Authorization": f"Bearer {self._access_token}",
                    "Accept": "application/json",
                },
            )
            response.raise_for_status()
            redirect_uri = response.json()["data"]["authorized_redirect_uri"]
        self._connection = await ws_connect(redirect_uri, max_size=None)

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        if self._connection is None:
            raise RuntimeError("subscribe() called before a successful authorize()")
        request = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {"mode": "full", "instrumentKeys": instrument_keys},
        }
        # `websockets` picks the WebSocket frame type from the Python type:
        # `str` -> Text frame, `bytes` -> Binary frame. Upstox expects the
        # subscribe control message as Text; pre-encoding to bytes would
        # send it as Binary and get it silently ignored.
        await self._connection.send(json.dumps(request))
        # Upstox's feed does not return a synchronous, distinguishable
        # subscription ack -- confirmation lives in the (unparsed) frame
        # stream itself. We record what we asked for as "acknowledged";
        # a future parser can reconcile actual coverage against the raw
        # frames if that ever matters.
        return instrument_keys

    def __aiter__(self) -> AsyncIterator[Frame]:
        if self._connection is None:
            raise RuntimeError("iteration started before a successful authorize()")
        return self._connection.__aiter__()

    async def aclose(self) -> None:
        if self._connection is not None:
            await self._connection.close()
