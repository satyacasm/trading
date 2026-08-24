from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.crypto_ingestor import run_ingestion_loop


class ScriptedFeed:
    """A fake `BinanceFeed`: yields scripted raw messages, then optionally fails."""

    def __init__(self, messages: Sequence[str], *, fail_after: BaseException | None = None) -> None:
        self.messages = list(messages)
        self.fail_after = fail_after
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def __aiter__(self) -> AsyncIterator[str]:
        for message in self.messages:
            yield message
        if self.fail_after is not None:
            raise self.fail_after

    async def aclose(self) -> None:
        self.closed = True


async def _no_sleep(seconds: float) -> None:
    return None


def _trade(symbol: str = "BTCUSDT", price: str = "65000.50") -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@trade",
            "data": {
                "e": "trade",
                "s": symbol,
                "p": price,
                "q": "0.01000000",
                "T": 1724500000000,
                "m": False,
            },
        }
    )


def test_run_ingestion_loop_publishes_parsed_ticks_to_redis(redis_client: redis.Redis) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)  # discard the subscribe confirmation

    feed = ScriptedFeed([_trade()])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    message = pubsub.get_message(timeout=2)
    assert message is not None and message["type"] == "message"
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501
    assert Decimal(str(payload["price"])) == Decimal("65000.50")


def test_run_ingestion_loop_skips_a_malformed_message_and_keeps_going(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedFeed(["not json", _trade()])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    # Only the second (valid) message ever reached Redis.
    message = pubsub.get_message(timeout=2)
    assert message is not None
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501


def test_run_ingestion_loop_reconnects_after_a_dropped_connection(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    failing_feed = ScriptedFeed([], fail_after=ConnectionError("dropped"))
    working_feed = ScriptedFeed([_trade()])
    feeds = iter([failing_feed, working_feed])

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: next(feeds),
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert failing_feed.closed
    message = pubsub.get_message(timeout=2)
    assert message is not None
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501
