from __future__ import annotations

from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


def test_feed_response_round_trips_an_ltpc_message() -> None:
    response = pb.FeedResponse()
    response.type = pb.live_feed
    response.currentTs = 1724500000000
    feed = pb.Feed()
    feed.ltpc.ltp = 65000.50
    feed.ltpc.ltt = 1724500000123
    feed.ltpc.ltq = 10
    response.feeds["NSE_EQ|INE002A01018"].CopyFrom(feed)

    raw = response.SerializeToString()
    restored = pb.FeedResponse()
    restored.ParseFromString(raw)

    assert restored.type == pb.live_feed
    assert restored.currentTs == 1724500000000
    entry = restored.feeds["NSE_EQ|INE002A01018"]
    assert entry.WhichOneof("FeedUnion") == "ltpc"
    assert entry.ltpc.ltp == 65000.50
    assert entry.ltpc.ltt == 1724500000123
    assert entry.ltpc.ltq == 10


def test_feed_response_reports_no_feed_type_for_an_empty_entry() -> None:
    response = pb.FeedResponse()
    response.feeds["NSE_EQ|UNSET"].CopyFrom(pb.Feed())

    assert response.feeds["NSE_EQ|UNSET"].WhichOneof("FeedUnion") is None
