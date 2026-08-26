from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from trading.streaming.upstox_feed import parse_upstox_frame
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb

FIXTURES = Path(__file__).parent / "fixtures"


def _read_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


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


def test_parse_upstox_frame_handles_a_real_live_ff_marketff_frame() -> None:
    """Real captured frame: mode "full" delivers every equity tick as
    `feed.ff.marketFF.ltpc`, never bare `feed.ltpc`. This is the regression
    guard for BUG 2 -- the old `WhichOneof(...) != "ltpc"` check discarded
    every one of these."""
    raw = _read_fixture("upstox_live_feed.bin")

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE090A01021": 505})

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.instrument_id == 505
    assert tick.price == Decimal("1445.2")
    assert tick.price > 0
    assert tick.quantity == Decimal("1")
    assert tick.ts == datetime.fromtimestamp(1787716849071 / 1000, tz=UTC)
    assert tick.ts.tzinfo is UTC


def test_parse_upstox_frame_handles_a_real_initial_ff_frame_for_all_tracked_instruments() -> None:
    raw = _read_fixture("upstox_initial_feed.bin")
    instrument_ids = {
        "NSE_EQ|INE002A01018": 501,  # RELIANCE
        "NSE_EQ|INE467B01029": 502,  # TCS
        "NSE_EQ|INE009A01021": 503,  # INFY
        "NSE_EQ|INE040A01034": 504,  # HDFCBANK
        "NSE_EQ|INE090A01021": 505,  # ICICIBANK
    }

    ticks = parse_upstox_frame(raw, instrument_ids=instrument_ids)

    assert {t.instrument_id for t in ticks} == set(instrument_ids.values())
    expected_prices = {
        501: Decimal("1311.5"),
        502: Decimal("2293.7"),
        503: Decimal("1131.7"),
        504: Decimal("727.0"),
        505: Decimal("1445.2"),
    }
    assert {t.instrument_id: t.price for t in ticks} == expected_prices
    assert all(t.price > 0 for t in ticks)
    assert all(t.ts.tzinfo is UTC for t in ticks)


def test_parse_upstox_frame_returns_empty_list_for_a_market_info_frame_with_no_feeds() -> None:
    raw = _read_fixture("upstox_market_info.bin")

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert ticks == []


def test_parse_upstox_frame_handles_an_index_full_feed() -> None:
    """No real index fixture was captured; synthesised to cover the
    `ff.indexFF` arm of the FullFeedUnion (indices, e.g. NIFTY, arrive this
    way rather than as marketFF)."""
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ff.indexFF.ltpc.ltp = 24500.75
    feed.ff.indexFF.ltpc.ltq = 0
    feed.ff.indexFF.ltpc.ltt = 1724500000123
    response.feeds["NSE_INDEX|Nifty 50"].CopyFrom(feed)
    raw = response.SerializeToString()

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_INDEX|Nifty 50": 601})

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.instrument_id == 601
    assert tick.price == Decimal("24500.75")
    assert tick.price > 0
    assert tick.ts.year == 2024


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
