"""Restart the real trading_redis_test container mid-stream and confirm
resilient_messages resumes delivering -- the actual infrastructure
event every unit test with a fake disconnect is standing in for.
Marked like this repo's other Docker-dependent tests (see the `db` and
`sandbox` markers in pyproject.toml): it needs a running Docker daemon
and the compose stack, which the default dev setup already provides."""

from __future__ import annotations

import asyncio
import subprocess

import pytest
import redis.asyncio as aioredis

from trading.config import get_settings
from trading.streaming.resilient_pubsub import resilient_messages

pytestmark = pytest.mark.db


def _docker_available() -> bool:
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


async def _wait_until_ready(client: aioredis.Redis, *, timeout: float = 30.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            await client.ping()
            return
        except Exception:  # noqa: BLE001 - still coming back up
            await asyncio.sleep(0.5)
    raise TimeoutError("trading_redis_test did not become ready again")


async def _wait_until_resubscribed(client: aioredis.Redis, *, timeout: float = 30.0) -> None:
    """The server answering PING again does not mean resilient_messages'
    reconnect-and-resubscribe (behind its own backoff sleep) has finished
    yet -- publishing "after" the moment PING succeeds is a race that
    drops the message and hangs the test on its `wait_for`. `PUBSUB
    NUMPAT` reports the count directly, so poll it instead of guessing a
    sleep long enough to outlast the backoff."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            if await client.pubsub_numpat() >= 1:
                return
        except Exception:  # noqa: BLE001 - still coming back up
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError("resilient_messages did not resubscribe in time")


@pytest.mark.skipif(not _docker_available(), reason="docker is not available")
def test_resilient_messages_resumes_after_a_real_redis_container_restart() -> None:
    async def _scenario() -> list[str]:
        subscriber = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        publisher = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        received: list[str] = []

        async def _consume() -> None:
            async for message in resilient_messages(subscriber, patterns=["restart-drill:*"]):
                if message["type"] != "pmessage":
                    continue
                received.append(message["data"])
                if len(received) >= 2:
                    return

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.5)  # let the initial psubscribe land

        await publisher.publish("restart-drill:1", "before")
        await asyncio.sleep(0.5)

        subprocess.run(
            ["docker", "restart", "trading_redis_test"], check=True, capture_output=True
        )
        await _wait_until_ready(publisher)
        await _wait_until_resubscribed(publisher)
        await publisher.publish("restart-drill:1", "after")

        try:
            await asyncio.wait_for(task, timeout=30)
        finally:
            await publisher.aclose()
            await subscriber.connection_pool.disconnect()
        return received

    received = asyncio.run(_scenario())
    assert received == ["before", "after"]
