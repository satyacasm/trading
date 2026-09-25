"""run_heartbeat/start_heartbeat_thread: each long-running process sets
health:<name> with a TTL, refreshed well before it expires, so GET
/health can tell a live process from a dead one without polling it
directly (design §6)."""

from __future__ import annotations

import asyncio
import time

import redis
import structlog.testing

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


def test_run_heartbeat_survives_a_write_failure_and_keeps_beating(redis_client) -> None:
    """A single failed write (e.g. Redis briefly unreachable) must be
    logged, not fatal -- the loop tries again next beat and a later write
    still lands."""
    import redis.asyncio as aioredis

    from trading.config import get_settings

    async def _run() -> tuple[list[dict], int]:
        real_client = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        calls = {"n": 0}

        class _FlakyClient:
            async def set(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
                calls["n"] += 1
                if calls["n"] == 1:
                    raise redis.ConnectionError("boom")
                return await real_client.set(*args, **kwargs)

        stop = asyncio.Event()
        beats = {"n": 0}

        async def _sleep(seconds: float) -> None:
            beats["n"] += 1
            if beats["n"] >= 2:  # stop after the failed beat and the recovered one
                stop.set()

        try:
            with structlog.testing.capture_logs() as cap:
                await run_heartbeat(
                    _FlakyClient(), "bar_aggregator", ttl=30, every=10, sleep=_sleep, stop=stop
                )
        finally:
            await real_client.aclose()
        return cap, calls["n"]

    cap, call_count = asyncio.run(_run())
    assert call_count == 2  # the first write was attempted, then a second after it failed
    failures = [e for e in cap if e.get("event") == "heartbeat.write_failed"]
    assert len(failures) == 1
    assert failures[0]["name"] == "bar_aggregator"
    # The loop kept going: the second (real) write landed.
    assert redis_client.get("health:bar_aggregator") == "1"


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
