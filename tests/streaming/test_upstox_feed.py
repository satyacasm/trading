from __future__ import annotations

from decimal import Decimal

import pytest

from trading.streaming.upstox_feed import parse_upstox_frame
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


def _frame(entries: dict[str, tuple[float, int, int]], feed_type: int = pb.live_feed) -> bytes:
    """`entries` maps instrument_key -> (ltp, ltq, ltt)."""
    response = pb.FeedResponse()
    response.type = feed_type
    response.currentTs = 1724500000000
    for key, (ltp, ltq, ltt) in entries.items():
        feed = pb.Feed()
        feed.ltpc.ltp = ltp
        feed.ltpc.ltq = ltq
        feed.ltpc.ltt = ltt
        response.feeds[key].CopyFrom(feed)
    return response.SerializeToString()


def test_parse_upstox_frame_builds_a_tick_for_a_tracked_instrument() -> None:
    raw = _frame({"NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123)})

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.instrument_id == 501
    assert tick.price == Decimal("2500.5")
    assert tick.quantity == Decimal("10")
    assert tick.ts.year == 2024  # 1724500000123 ms


def test_parse_upstox_frame_builds_multiple_ticks_from_one_frame() -> None:
    raw = _frame(
        {
            "NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123),
            "NSE_EQ|INE467B01029": (3800.00, 5, 1724500000456),
        }
    )

    ticks = parse_upstox_frame(
        raw, instrument_ids={"NSE_EQ|INE002A01018": 501, "NSE_EQ|INE467B01029": 502}
    )

    assert {t.instrument_id for t in ticks} == {501, 502}


def test_parse_upstox_frame_ignores_an_untracked_instrument() -> None:
    raw = _frame({"NSE_EQ|UNTRACKED": (100.0, 1, 1724500000123)})

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert ticks == []


def test_parse_upstox_frame_includes_initial_feed_ticks_too() -> None:
    raw = _frame({"NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123)}, feed_type=pb.initial_feed)

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert len(ticks) == 1


def test_parse_upstox_frame_ignores_a_tracked_instrument_with_no_ltpc_payload() -> None:
    response = pb.FeedResponse()
    response.feeds["NSE_EQ|INE002A01018"].CopyFrom(pb.Feed())  # oneof unset
    raw = response.SerializeToString()

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert ticks == []


def test_parse_upstox_frame_returns_empty_list_for_malformed_bytes() -> None:
    assert parse_upstox_frame(b"not a valid protobuf frame", instrument_ids={}) == []


@pytest.mark.live
def test_live_upstox_frame_matches_the_documented_ltpc_shape() -> None:
    """One real frame from Upstox's feed, shape-checked against what
    `parse_upstox_frame` assumes. Excluded from the default run. Only
    produces real data during NSE market hours (9:15-15:30 IST) -- outside
    that window this test will time out with no data, which is expected,
    not a failure to chase; run it during a trading session instead."""
    import asyncio

    from trading.config import get_settings
    from trading.recorder.upstox_ws import LiveUpstoxFeed

    token = get_settings().upstox_analytics_token
    if not token:
        pytest.skip("UPSTOX_ANALYTICS_TOKEN not set")

    async def _probe() -> bytes:
        feed = LiveUpstoxFeed(token)
        try:
            await feed.authorize()
            await feed.subscribe(["NSE_EQ|INE002A01018"])  # RELIANCE
            async for raw in feed:
                return raw
        finally:
            await feed.aclose()
        raise RuntimeError("feed closed with no frame received")

    raw = asyncio.run(asyncio.wait_for(_probe(), timeout=20))

    response = pb.FeedResponse()
    response.ParseFromString(raw)  # must not raise -- the real shape check
    assert response.type in (pb.initial_feed, pb.live_feed)
