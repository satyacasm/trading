"""run_heartbeat/start_heartbeat_thread: each long-running process sets
health:<name> with a TTL, refreshed well before it expires, so GET
/health can tell a live process from a dead one without polling it
directly (design §6)."""

from __future__ import annotations

import asyncio
import time

from trading.streaming.heartbeat import run_heartbeat, start_heartbeat_thread


def test_run_heartbeat_sets_the_key_with_a_ttl(redis_client) -> None:
    import redis.asyncio as aioredis

    from trading.config import get_settings

    async def _run() -> None:
        client = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        stop = asyncio.Event()

        async def _sleep(seconds: float) -> None:
            stop.set()  # stop after exactly one beat

        try:
            await run_heartbeat(client, "bar_aggregator", ttl=30, every=10, sleep=_sleep, stop=stop)
        finally:
            await client.aclose()

    asyncio.run(_run())
    assert redis_client.get("health:bar_aggregator") == "1"
    ttl = redis_client.ttl("health:bar_aggregator")
    assert 0 < ttl <= 30


def test_start_heartbeat_thread_refreshes_until_stopped(redis_client) -> None:
    import redis as sync_redis

    from trading.config import get_settings

    stop = start_heartbeat_thread(
        lambda: sync_redis.Redis.from_url(get_settings().redis_url, decode_responses=True),
        "live_supervisor",
        ttl=30,
        every=0.05,
    )
    time.sleep(0.3)
    assert redis_client.get("health:live_supervisor") == "1"
    stop.set()
    redis_client.delete("health:live_supervisor")
    time.sleep(0.3)
    # Stopped -- nothing refreshes it back in.
    assert redis_client.get("health:live_supervisor") is None
