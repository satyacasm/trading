from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.upstox_ingestor import run_ingestion_loop
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


class ScriptedUpstoxFeed:
    """A fake `UpstoxFeed`: records authorize()/subscribe() calls, yields
    scripted raw frames (bytes, str, or object), then optionally fails."""

    def __init__(
        self,
        frames: Sequence[bytes | str | object],
        *,
        fail_after: BaseException | None = None,
    ) -> None:
        self.frames = list(frames)
        self.fail_after = fail_after
        self.authorized = False
        self.subscribed_keys: list[str] | None = None
        self.closed = False

    async def authorize(self) -> None:
        self.authorized = True

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        self.subscribed_keys = instrument_keys
        return instrument_keys

    async def __aiter__(self) -> AsyncIterator[bytes | str | object]:
        for frame in self.frames:
            yield frame  # type: ignore[misc]
        if self.fail_after is not None:
            raise self.fail_after

    async def aclose(self) -> None:
        self.closed = True


async def _no_sleep(seconds: float) -> None:
    return None


def _ltpc_frame(instrument_key: str, ltp: float, ltq: int = 10, ltt: int = 1724500000123) -> bytes:
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ltpc.ltp = ltp
    feed.ltpc.ltq = ltq
    feed.ltpc.ltt = ltt
    response.feeds[instrument_key].CopyFrom(feed)
    return response.SerializeToString()


def _full_frame_with_i1(
    instrument_key: str,
    ltp: float,
    *,
    ltq: int = 10,
    ltt: int = 1724500000123,
    i1_close: float = 2500.5,
    i1_volume: int = 11496,
    i1_ts: int = 1724499960000,
) -> bytes:
    """A mode "full" `marketFF` frame carrying both an `ltpc` (so the tick
    path still fires) and an `I1` `marketOHLC` entry (so the bar path
    fires)."""
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ff.marketFF.ltpc.ltp = ltp
    feed.ff.marketFF.ltpc.ltq = ltq
    feed.ff.marketFF.ltpc.ltt = ltt
    ohlc = feed.ff.marketFF.marketOHLC.ohlc.add()
    ohlc.interval = "I1"
    ohlc.open = i1_close
    ohlc.high = i1_close
    ohlc.low = i1_close
    ohlc.close = i1_close
    ohlc.volume = i1_volume
    ohlc.ts = i1_ts
    response.feeds[instrument_key].CopyFrom(feed)
    return response.SerializeToString()


def test_run_ingestion_loop_authorizes_subscribes_and_publishes_parsed_ticks(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedUpstoxFeed([_ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert feed.authorized
    assert feed.subscribed_keys == ["NSE_EQ|INE002A01018"]
    message = pubsub.get_message(timeout=2)
    assert message is not None and message["type"] == "message"
    payload = message["data"]
    import json

    parsed = json.loads(payload)
    assert parsed["instrument_id"] == 501
    assert Decimal(str(parsed["price"])) == Decimal("2500.5")


def test_run_ingestion_loop_skips_a_malformed_frame_and_keeps_going(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedUpstoxFeed([b"not a valid frame", _ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    message = pubsub.get_message(timeout=2)
    assert message is not None


def test_run_ingestion_loop_grows_backoff_on_repeated_connect_then_drop(
    redis_client: redis.Redis,
) -> None:
    """Same lesson already learned on crypto_ingestor: a connection that
    authorizes/subscribes successfully but yields zero frames before
    dropping must not reset backoff to the initial value -- only a
    connection that actually yields a message proves itself."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    failing_1 = ScriptedUpstoxFeed([], fail_after=ConnectionError("dropped"))
    failing_2 = ScriptedUpstoxFeed([], fail_after=ConnectionError("dropped"))
    working = ScriptedUpstoxFeed([_ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    feeds = iter([failing_1, failing_2, working])

    sleeps: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: next(feeds),
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_record_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert sleeps == [1.0, 2.0]
    assert working.authorized and working.subscribed_keys == ["NSE_EQ|INE002A01018"]


def test_run_ingestion_loop_skips_non_bytes_frame_and_continues(
    redis_client: redis.Redis,
) -> None:
    """A non-bytes frame (str or object) is skipped without crashing the
    loop or attempting to parse it. Subsequent valid bytes frames are
    processed normally."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    # Mix of non-bytes frames (str and object) followed by a valid bytes frame
    feed = ScriptedUpstoxFeed(
        [
            "unexpected_str_frame",  # str frame (should be skipped)
            object(),  # object frame (should be skipped)
            _ltpc_frame("NSE_EQ|INE002A01018", 2500.50),  # valid bytes frame
        ]
    )
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    # Verify that the feed was authorized and subscribed
    assert feed.authorized
    assert feed.subscribed_keys == ["NSE_EQ|INE002A01018"]

    # Verify that a tick was published for the valid bytes frame only
    message = pubsub.get_message(timeout=2)
    assert message is not None and message["type"] == "message"
    payload = message["data"]
    import json

    parsed = json.loads(payload)
    assert parsed["instrument_id"] == 501
    assert Decimal(str(parsed["price"])) == Decimal("2500.5")


def test_run_ingestion_loop_publishes_a_bar_only_once_for_a_repeated_i1_signature(
    redis_client: redis.Redis,
) -> None:
    """The same I1 bar is re-sent on every ~3s push while the minute is
    live -- the same frame processed twice must publish to `bars:{id}`
    only once."""
    import json

    pubsub = redis_client.pubsub()
    pubsub.subscribe("bars:501")
    pubsub.get_message(timeout=1)

    frame = _full_frame_with_i1("NSE_EQ|INE002A01018", 2500.50)
    feed = ScriptedUpstoxFeed([frame, frame])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=2,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    first = pubsub.get_message(timeout=2)
    assert first is not None and first["type"] == "message"
    parsed = json.loads(first["data"])
    assert parsed["instrument_id"] == 501
    assert Decimal(str(parsed["volume"])) == Decimal("11496")

    second = pubsub.get_message(timeout=0.5)
    assert second is None  # the repeated, unchanged signature was not republished


def test_run_ingestion_loop_publishes_again_when_the_bar_signature_changes(
    redis_client: redis.Redis,
) -> None:
    """A genuinely new/changed I1 bar (e.g. the minute rolled over, or the
    still-forming minute's volume grew) must publish again."""
    import json

    pubsub = redis_client.pubsub()
    pubsub.subscribe("bars:501")
    pubsub.get_message(timeout=1)

    first_frame = _full_frame_with_i1("NSE_EQ|INE002A01018", 2500.50, i1_volume=11496)
    second_frame = _full_frame_with_i1("NSE_EQ|INE002A01018", 2500.50, i1_volume=20000)
    feed = ScriptedUpstoxFeed([first_frame, second_frame])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=2,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    first = pubsub.get_message(timeout=2)
    assert first is not None and first["type"] == "message"
    assert Decimal(str(json.loads(first["data"])["volume"])) == Decimal("11496")

    second = pubsub.get_message(timeout=2)
    assert second is not None and second["type"] == "message"
    assert Decimal(str(json.loads(second["data"])["volume"])) == Decimal("20000")
