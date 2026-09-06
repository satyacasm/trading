"""Polling Binance for live perpetual marks and closed bars.

The futures WebSocket connects from here, accepts a SUBSCRIBE with a
success ack, and then delivers nothing -- Binance gates streaming
derivatives data by jurisdiction while leaving public REST open. So this
path polls. These tests inject the HTTP layer; none of them touch a
network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from trading.streaming.perp_ingestor import (
    closed_klines,
    marks_for,
    publish_mark,
)


def _premium(symbol: str, mark: str, rate: str = "0.00001036") -> dict[str, object]:
    return {
        "symbol": symbol,
        "markPrice": mark,
        "indexPrice": "79612.41913043",
        "lastFundingRate": rate,
        "nextFundingTime": 1788595200000,
        "time": 1788588331000,
    }


def test_one_call_is_filtered_to_the_universe_we_seeded() -> None:
    """`premiumIndex` returns all 898 contracts Binance lists. Publishing
    marks for 890 instruments this platform has never heard of would be
    891 wasted writes a second."""
    payload = [_premium("BTCUSDT", "79580"), _premium("SHIBUSDT", "0.00001")]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        marks = marks_for(client, {"BTCUSDT": 1})

    assert [m.symbol for m in marks] == ["BTCUSDT"]
    assert marks[0].instrument_id == 1
    assert marks[0].mark_price == Decimal("79580")
    assert marks[0].funding_rate == Decimal("0.00001036")
    # Funding settles on the 00/08/16 UTC boundaries.
    assert marks[0].next_funding_time == datetime(2026, 9, 5, 8, 0, tzinfo=UTC)


def test_only_a_finished_kline_is_taken() -> None:
    """Binance's last kline is the interval still in progress: its close
    moves every second. Treating it as final would hand a strategy a bar
    that has not happened yet -- lookahead, from the live feed."""
    now = datetime(2026, 9, 5, 6, 6, 30, tzinfo=UTC)
    rows = [
        # closed at 06:04:59.999
        [
            1788588240000,
            "79560.00",
            "79570",
            "79550",
            "79566.20",
            "10",
            1788588299999,
            "800000",
            100,
            "0",
            "0",
            "0",
        ],
        # closed at 06:05:59.999
        [
            1788588300000,
            "79566.20",
            "79590",
            "79560",
            "79580.00",
            "12",
            1788588359999,
            "900000",
            120,
            "0",
            "0",
            "0",
        ],
        # still open: closes at 06:06:59.999, which is after `now`
        [
            1788588360000,
            "79580.00",
            "79585",
            "79575",
            "79581.00",
            "3",
            1788588419999,
            "200000",
            30,
            "0",
            "0",
            "0",
        ],
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=rows)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        bars = closed_klines(client, "BTCUSDT", now=now)

    assert [b.ts for b in bars] == [
        datetime(2026, 9, 5, 6, 4, tzinfo=UTC),
        datetime(2026, 9, 5, 6, 5, tzinfo=UTC),
    ]


def test_a_mark_is_published_and_left_where_the_engine_can_read_it_back() -> None:
    """Two different needs. A liquidation check that wakes up between
    publishes needs the last known mark, and a subscriber that was not
    listening a second ago cannot recover one from a pub/sub channel."""
    from trading.streaming.perp_ingestor import Mark

    published: list[tuple[str, str]] = []
    stored: dict[str, str] = {}

    class FakeRedis:
        def publish(self, channel: str, payload: str) -> None:
            published.append((channel, payload))

        def set(self, key: str, value: str) -> None:
            stored[key] = value

    mark = Mark(
        symbol="BTCUSDT",
        instrument_id=7,
        mark_price=Decimal("79580"),
        index_price=Decimal("79612"),
        funding_rate=Decimal("0.00001036"),
        next_funding_time=datetime(2026, 9, 4, 16, 0, tzinfo=UTC),
        as_of=datetime(2026, 9, 4, 15, 45, tzinfo=UTC),
    )
    publish_mark(FakeRedis(), mark)

    by_channel = dict(published)
    assert json.loads(by_channel["perp_marks:7"])["mark_price"] == "79580"
    assert stored["perp_mark:7"] == by_channel["perp_marks:7"]

    # And a tick, because the paper engine prices resting orders from
    # `ticks:*` and nowhere else. Without it a perpetual order rests
    # forever -- which is what a short placed from the order ticket did.
    tick = json.loads(by_channel["ticks:7"])
    assert tick["price"] == "79580"
    assert tick["ts"] == "2026-09-04T15:45:00+00:00"
    # No size: a mark is not a trade. Inventing a volume would put a
    # number in the tick stream that never happened.
    assert tick["quantity"] == "0"


def test_a_bar_is_published_once_however_often_it_is_polled(db_conn) -> None:
    """Each poll asks for the last three klines, so the same finished bar
    comes back every two seconds. Publishing it again each time feeds the
    live supervisor a bar the strategy has already acted on -- harmless
    since the runtime ignores exact repeats, and still wrong: it inflates
    `bars_seen` and spends a dispatch per poll.

    Comparing against only the newest timestamp is not enough. A page
    holding two finished bars sets `seen` to the second, so the first
    compares unequal on the next poll and is republished forever.
    """
    from datetime import date

    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.perp_ingestor import run_ingestion_loop
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    iid = seed_perp_instruments(
        db_conn,
        [
            PerpContractSpec(
                "BTCUSDT",
                "BTC",
                "USDT",
                Decimal("0.10"),
                Decimal("0.001"),
                Decimal("0.001"),
                Decimal("50"),
                Decimal("0.0125"),
            )
        ],
        on=date(2026, 9, 5),
    )["BTC-USDT"]

    # The loop commits -- correctly, since a live bar must survive a crash
    # -- so the fixture's rollback does not undo it and a previous run of
    # this test leaves rows behind. State the precondition rather than
    # depending on the table being empty.
    db_conn.execute(
        "DELETE FROM bars_intraday WHERE instrument_id = %s AND interval_sec = 60", (iid,)
    )
    db_conn.commit()

    rows = [
        [
            1788588240000,
            "79560.00",
            "79570",
            "79550",
            "79566.20",
            "10",
            1788588299999,
            "800000",
            100,
            "0",
            "0",
            "0",
        ],
        [
            1788588300000,
            "79566.20",
            "79590",
            "79560",
            "79580.00",
            "12",
            1788588359999,
            "900000",
            120,
            "0",
            "0",
            "0",
        ],
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if "klines" in str(request.url):
            return httpx.Response(200, json=rows)
        return httpx.Response(200, json=[])

    published: list[str] = []

    class FakeRedis:
        def publish(self, channel: str, message: str) -> None:
            if channel.startswith("closed_bars"):
                published.append(json.loads(message)["ts"])

        def set(self, name: str, value: str) -> None:
            return None

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        run_ingestion_loop(
            db_conn,
            FakeRedis(),
            client,
            {"BTCUSDT": iid},
            mark_interval_seconds=0,
            bar_interval_seconds=0,
            iterations=4,
            sleep=lambda _s: None,
            clock=lambda: datetime(2026, 9, 5, 6, 10, tzinfo=UTC),
        )

    assert len(published) == 2, published
    assert len(set(published)) == 2


def test_a_restart_does_not_replay_bars_already_stored(db_conn) -> None:
    """The high-water mark lives in memory, so a fresh process would
    otherwise republish whatever the page still carries -- and a restart
    is exactly when a live strategy least wants a bar it already acted on
    arriving again."""
    from datetime import date

    from trading.sources.binance_futures import PerpBar, PerpContractSpec
    from trading.streaming.perp_backfill import write_bars
    from trading.streaming.perp_ingestor import high_water_marks
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    iid = seed_perp_instruments(
        db_conn,
        [
            PerpContractSpec(
                "BTCUSDT",
                "BTC",
                "USDT",
                Decimal("0.10"),
                Decimal("0.001"),
                Decimal("0.001"),
                Decimal("50"),
                Decimal("0.0125"),
            )
        ],
        on=date(2026, 9, 5),
    )["BTC-USDT"]
    stamped = datetime(2026, 9, 5, 6, 5, tzinfo=UTC)
    db_conn.execute(
        "DELETE FROM bars_intraday WHERE instrument_id = %s AND interval_sec = 60", (iid,)
    )
    write_bars(
        db_conn,
        iid,
        [
            PerpBar(
                stamped,
                Decimal("1"),
                Decimal("2"),
                Decimal("1"),
                Decimal("2"),
                Decimal("1"),
                Decimal("2"),
                1,
            )
        ],
        "1m",
    )

    assert high_water_marks(db_conn, {"BTCUSDT": iid}) == {iid: stamped}
