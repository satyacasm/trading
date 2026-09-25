"""A pubsub subscription that survives a Redis disconnect.

Two real bugs this fixes, both found live: `bar_aggregator.py:484-518`'s
`pubsub.listen()` returns normally on a connection loss instead of
raising, so the consuming `async for` loop ends silently and bar writing
stops with no error; `live/supervisor.py:512-525`'s `get_message()`
raises `ConnectionError` and kills the whole process.

Lost messages are harmless here (design §2): Postgres is the record and
a `closed_bars:*` message is only a wake-up, so a message dropped during
a reconnect is recovered by whichever poller notices next (bar_aggregator's
sweep, the supervisor's delivery timer). Reconnecting and resubscribing
is therefore the whole fix -- there is nothing to replay at this layer.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import redis
import redis.asyncio as aioredis
import structlog

log = structlog.get_logger(__name__)

__all__ = ["SyncResilientPubSub", "resilient_messages"]

Sleeper = Callable[[float], Awaitable[None]]

_INITIAL_BACKOFF = 1.0


async def _subscribe(
    redis_client: aioredis.Redis, patterns: Sequence[str], channels: Sequence[str]
) -> Any:
    pubsub = redis_client.pubsub()
    if patterns:
        await pubsub.psubscribe(*patterns)
    if channels:
        await pubsub.subscribe(*channels)
    return pubsub


async def resilient_messages(
    redis_client: aioredis.Redis,
    *,
    patterns: Sequence[str] = (),
    channels: Sequence[str] = (),
    sleep: Sleeper = asyncio.sleep,
    max_backoff: float = 30.0,
    pubsub: Any | None = None,
) -> AsyncIterator[dict]:
    """Yield every actual pub/sub message forever (subscribe/psubscribe
    confirmation events from `listen()` are consumed but not yielded, and
    do not themselves count as delivery). A raised exception from
    `listen()` and a `listen()` that returns having delivered no real
    message are both treated as a disconnect worth backing off for: log
    it, back off (1s, 2s, 4s, ... capped at `max_backoff`, reset to 1s the
    moment a real message is delivered), open a fresh pubsub, and
    resubscribe to the same patterns/channels. A `listen()` that returns
    after delivering at least one real message just needs a fresh
    generator (the per-connection iterator ended) -- that is not itself
    evidence of a problem, so it does not incur another backoff sleep. A
    session that only ever produces subscribe confirmations before dying
    (a flaky connection that never gets past resubscribing) must still be
    scored as a disconnect, not as delivery, or the backoff never engages.

    `pubsub`, when given, is used as the first connection verbatim --
    already subscribed by the caller, no extra `redis_client.pubsub()`/
    subscribe call is made before the first `listen()`. This lets a
    caller subscribe synchronously before doing anything else (before
    scheduling any concurrent task, in particular) and only then hand off
    to this generator for ongoing reconnect-on-failure. Without this, the
    subscribe that used to be the first thing `run_engine` awaited moves
    inside a task that has to wait its turn on the event loop next to
    everything else the caller schedules, which can delay it long enough
    for a message published right after startup to be lost -- paper/
    engine.py's task-8 regression, reproduced by publishing on a real
    Redis right after `run_engine` starts while its other periodic tasks
    are busy. Every reconnect after this first one still opens and
    subscribes a brand new pubsub via `_subscribe`, same as always."""
    backoff = _INITIAL_BACKOFF
    pubsub = pubsub if pubsub is not None else await _subscribe(redis_client, patterns, channels)
    while True:
        delivered = False
        raised = False
        try:
            async for message in pubsub.listen():
                if message.get("type") not in ("message", "pmessage"):
                    continue  # subscribe/psubscribe confirmation, not a real message
                backoff = _INITIAL_BACKOFF
                delivered = True
                yield message
        except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect
            raised = True
            log.warning("resilient_pubsub.disconnected", reason=str(exc))
        if not raised and not delivered:
            log.warning("resilient_pubsub.disconnected", reason="listen() returned")
        try:
            await pubsub.aclose()  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
            log.debug("resilient_pubsub.cleanup_failed", exc_info=True)
        if raised or not delivered:
            log.warning("resilient_pubsub.reconnecting", backoff=backoff)
            await sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        pubsub = await _subscribe(redis_client, patterns, channels)


class SyncResilientPubSub:
    """The sync twin, for the live supervisor's `get_message()`-driven
    loop. Never raises on a connection error: `get_message` returns
    `None` for that call and reconnects (with the same backoff) before
    the next one, so a caller already treating `None` as "nothing right
    now" needs no new branch. A subscribe/psubscribe confirmation also
    returns `None` -- it is not a real message -- but unlike a genuine
    delivered message it does not reset the backoff, so a connection that
    keeps resubscribing and dying without ever delivering a real message
    still escalates instead of retrying at the initial step forever."""

    def __init__(
        self,
        client_factory: Callable[[], redis.Redis],
        patterns: Sequence[str] = (),
        channels: Sequence[str] = (),
        sleep: Callable[[float], None] = time.sleep,
        max_backoff: float = 30.0,
    ) -> None:
        self._client_factory = client_factory
        self._patterns = list(patterns)
        self._channels = list(channels)
        self._sleep = sleep
        self._max_backoff = max_backoff
        self._backoff = _INITIAL_BACKOFF
        self._pubsub = self._connect()

    def _connect(self) -> Any:
        client = self._client_factory()
        pubsub = client.pubsub()
        if self._patterns:
            pubsub.psubscribe(*self._patterns)
        if self._channels:
            pubsub.subscribe(*self._channels)
        return pubsub

    def get_message(self, timeout: float) -> dict | None:
        try:
            message = self._pubsub.get_message(timeout=timeout)
        except redis.exceptions.RedisError as exc:
            log.warning("resilient_pubsub.sync_disconnected", reason=str(exc))
            self._sleep(self._backoff)
            self._backoff = min(self._backoff * 2, self._max_backoff)
            self._pubsub = self._connect()
            return None
        if message is None:
            return None
        if message.get("type") not in ("message", "pmessage"):
            return None  # subscribe/psubscribe confirmation -- not a real message, no backoff reset
        self._backoff = _INITIAL_BACKOFF
        return message
