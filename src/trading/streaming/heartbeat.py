"""Each long-running process here sets `health:<name>` in Redis with a
TTL, refreshed well before it expires. `GET /health` (gateway.py) reads
these keys rather than polling every process directly -- a stale key
means the process is gone or wedged, exactly the two failure modes
this whole plan is about recovering from (design §6).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable

import redis
import structlog
from redis.asyncio import Redis

log = structlog.get_logger(__name__)

__all__ = ["run_heartbeat", "start_heartbeat_thread"]

Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def run_heartbeat(
    redis_client: Redis,
    name: str,
    *,
    ttl: int,
    every: float,
    sleep: Sleeper = _default_sleep,
    stop: asyncio.Event | None = None,
) -> None:
    """Set `health:<name>` forever, refreshed every `every` seconds, each
    write carrying a fresh `ttl`-second expiry. A failed write is logged
    and never fatal -- a missed beat should read as "unhealthy a little
    early", not crash the very process the beat exists to watch."""
    while stop is None or not stop.is_set():
        try:
            await redis_client.set(f"health:{name}", "1", ex=ttl)
        except Exception as exc:  # noqa: BLE001 - a heartbeat failure must never kill the process
            log.warning("heartbeat.write_failed", name=name, reason=str(exc))
        await sleep(every)


def start_heartbeat_thread(
    client_factory: Callable[[], redis.Redis], name: str, *, ttl: int, every: float
) -> threading.Event:
    """The sync twin, for a process whose main loop is not asyncio.
    Runs in a daemon thread with its own sync Redis client, so it needs
    no cooperation from whatever loop the caller's real work runs on.
    Returns the `Event` that stops it."""
    stop = threading.Event()

    def _loop() -> None:
        client = client_factory()
        while not stop.is_set():
            try:
                client.set(f"health:{name}", "1", ex=ttl)
            except Exception as exc:  # noqa: BLE001 - never kill the caller's process
                log.warning("heartbeat.write_failed", name=name, reason=str(exc))
            stop.wait(every)

    threading.Thread(target=_loop, name=f"heartbeat-{name}", daemon=True).start()
    return stop
