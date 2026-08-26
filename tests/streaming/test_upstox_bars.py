from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from trading.streaming.upstox_feed import parse_upstox_bars
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb

FIXTURES = Path(__file__).parent / "fixtures"


def _read_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_parse_upstox_bars_extracts_the_i1_bar_from_the_real_initial_feed_fixture() -> None:
    """Real captured frame (same fixture `test_upstox_feed.py` uses for
    ticks). Values transcribed directly from decoding the fixture, not
    hardcoded from the design doc's illustrative RELIANCE example."""
    raw = _read_fixture("upstox_initial_feed.bin")
    instrument_ids = {
        "NSE_EQ|INE002A01018": 501,  # RELIANCE
        "NSE_EQ|INE467B01029": 502,  # TCS
        "NSE_EQ|INE009A01021": 503,  # INFY
        "NSE_EQ|INE040A01034": 504,  # HDFCBANK
        "NSE_EQ|INE090A01021": 505,  # ICICIBANK
    }

    bars = parse_upstox_bars(raw, instrument_ids)

    assert {b.instrument_id for b in bars} == set(instrument_ids.values())
    by_id = {b.instrument_id: b for b in bars}

    expected_ts = datetime.fromtimestamp(1787716740000 / 1000, tz=UTC)

    reliance = by_id[501]
    assert reliance.ts == expected_ts
    assert reliance.open == Decimal("1311.1")
    assert reliance.high == Decimal("1311.3")
    assert reliance.low == Decimal("1310.6")
    assert reliance.close == Decimal("1311.1")
    assert reliance.volume == Decimal("11496")

    tcs = by_id[502]
    assert tcs.open == Decimal("2297.0")
    assert tcs.high == Decimal("2297.3")
    assert tcs.low == Decimal("2295.6")
    assert tcs.close == Decimal("2297.2")
    assert tcs.volume == Decimal("7316")

    infy = by_id[503]
    assert infy.open == Decimal("1133.0")
    assert infy.high == Decimal("1133.0")
    assert infy.low == Decimal("1132.0")
    assert infy.close == Decimal("1132.1")
    assert infy.volume == Decimal("11701")

    hdfcbank = by_id[504]
    assert hdfcbank.open == Decimal("726.15")
    assert hdfcbank.high == Decimal("726.5")
    assert hdfcbank.low == Decimal("725.95")
    assert hdfcbank.close == Decimal("726.45")
    assert hdfcbank.volume == Decimal("91594")

    icicibank = by_id[505]
    assert icicibank.open == Decimal("1444.8")
    assert icicibank.high == Decimal("1445.6")
    assert icicibank.low == Decimal("1443.9")
    assert icicibank.close == Decimal("1445.5")
    assert icicibank.volume == Decimal("28417")

    assert all(b.ts.tzinfo is UTC for b in bars)


def test_parse_upstox_bars_extracts_the_i1_bar_from_the_real_live_feed_fixture() -> None:
    raw = _read_fixture("upstox_live_feed.bin")

    bars = parse_upstox_bars(raw, instrument_ids={"NSE_EQ|INE090A01021": 505})

    assert len(bars) == 1
    bar = bars[0]
    assert bar.instrument_id == 505
    assert bar.ts == datetime.fromtimestamp(1787716740000 / 1000, tz=UTC)
    assert bar.open == Decimal("1444.8")
    assert bar.high == Decimal("1445.6")
    assert bar.low == Decimal("1443.9")
    assert bar.close == Decimal("1445.5")
    assert bar.volume == Decimal("28417")


def test_parse_upstox_bars_returns_empty_list_for_a_market_info_frame_with_no_feeds() -> None:
    raw = _read_fixture("upstox_market_info.bin")

    bars = parse_upstox_bars(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert bars == []


def test_parse_upstox_bars_returns_empty_list_for_malformed_bytes() -> None:
    assert parse_upstox_bars(b"not a valid protobuf frame", instrument_ids={}) == []


def test_parse_upstox_bars_ignores_an_untracked_instrument() -> None:
    raw = _read_fixture("upstox_initial_feed.bin")

    bars = parse_upstox_bars(raw, instrument_ids={"NSE_EQ|UNTRACKED": 999})

    assert bars == []


def test_parse_upstox_bars_handles_an_index_full_feed() -> None:
    """No real index fixture was captured (same gap as `test_upstox_feed.py`'s
    equivalent tick test); synthesised to cover the `ff.indexFF` arm."""
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ff.indexFF.ltpc.ltp = 24500.75
    feed.ff.indexFF.ltpc.ltt = 1724500000123
    ohlc = feed.ff.indexFF.marketOHLC.ohlc.add()
    ohlc.interval = "I1"
    ohlc.open = 24480.0
    ohlc.high = 24510.0
    ohlc.low = 24470.0
    ohlc.close = 24500.75
    ohlc.volume = 12345
    ohlc.ts = 1724499900000
    response.feeds["NSE_INDEX|Nifty 50"].CopyFrom(feed)
    raw = response.SerializeToString()

    bars = parse_upstox_bars(raw, instrument_ids={"NSE_INDEX|Nifty 50": 601})

    assert len(bars) == 1
    bar = bars[0]
    assert bar.instrument_id == 601
    assert bar.open == Decimal("24480.0")
    assert bar.high == Decimal("24510.0")
    assert bar.low == Decimal("24470.0")
    assert bar.close == Decimal("24500.75")
    assert bar.volume == Decimal("12345")
    assert bar.ts == datetime.fromtimestamp(1724499900000 / 1000, tz=UTC)


def test_parse_upstox_bars_skips_a_frame_with_no_i1_interval_present() -> None:
    """Only a '1d' OHLC entry present, no 'I1' -- must be skipped silently,
    not raise or fall back to some other interval."""
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ff.marketFF.ltpc.ltp = 1311.1
    feed.ff.marketFF.ltpc.ltt = 1724500000123
    ohlc = feed.ff.marketFF.marketOHLC.ohlc.add()
    ohlc.interval = "1d"
    ohlc.open = 1310.0
    ohlc.high = 1314.8
    ohlc.low = 1308.0
    ohlc.close = 1311.5
    ohlc.volume = 565764
    ohlc.ts = 1787682600000
    response.feeds["NSE_EQ|INE002A01018"].CopyFrom(feed)
    raw = response.SerializeToString()

    bars = parse_upstox_bars(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert bars == []
