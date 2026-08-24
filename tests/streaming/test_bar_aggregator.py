from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.streaming.bar_aggregator import (
    BarAggregator,
    ClosedBar,
    OpenBar,
    bucket_start,
    write_closed_bar,
)
from trading.streaming.models import Tick


def _tick(
    ts: datetime, price: str = "100.00", quantity: str = "1.00", instrument_id: int = 501
) -> Tick:
    return Tick(
        instrument_id=instrument_id, ts=ts, price=Decimal(price), quantity=Decimal(quantity)
    )


def test_bucket_start_floors_to_the_minute_in_utc() -> None:
    ts = datetime(2026, 8, 24, 12, 0, 45, tzinfo=UTC)
    assert bucket_start(ts) == datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)


def test_bucket_start_is_idempotent_on_an_already_floored_timestamp() -> None:
    ts = datetime(2026, 8, 24, 12, 1, 0, tzinfo=UTC)
    assert bucket_start(ts) == ts


def test_ingest_opens_a_new_bucket_and_returns_nothing_closed() -> None:
    aggregator = BarAggregator()
    closed = aggregator.ingest(_tick(datetime(2026, 8, 24, 12, 0, 10, tzinfo=UTC)))
    assert closed == []


def test_ingest_updates_high_low_close_within_the_same_bucket() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=1), price="100"))
    aggregator.ingest(_tick(base + timedelta(seconds=2), price="105"))
    aggregator.ingest(_tick(base + timedelta(seconds=3), price="98"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=4), price="102"))
    assert closed == []
    # Force the bucket closed by crossing into the next minute.
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert len(closed) == 1
    bar = closed[0].bar
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("105")
    assert bar.low == Decimal("98")
    assert bar.close == Decimal("102")
    assert bar.volume == Decimal("4.00")  # four 1.00-quantity ticks in the first bucket
    assert bar.trades == 4


def test_ingest_closes_the_previous_bucket_with_the_correct_bucket_start() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 5, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30)))
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1, seconds=1)))
    assert len(closed) == 1
    assert closed[0].bucket == base
    assert closed[0].instrument_id == 501


def test_ingest_treats_an_out_of_order_tick_as_an_update_to_the_current_bucket() -> None:
    """Ticks are assumed non-decreasing per instrument (one ordered WS
    connection -> one ordered Redis subscription). An out-of-order tick
    updates the currently-open bucket rather than reopening a closed one --
    an acknowledged simplification, not a crash or a silent data loss."""
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30), price="100"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=10), price="999"))  # earlier ts
    assert closed == []  # no bucket was closed -- just folded into the open one
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert closed[0].bar.close == Decimal("999")  # last-ingested tick, not last-in-time


def test_flush_stale_closes_a_bucket_whose_window_has_fully_elapsed() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert len(closed) == 1
    assert closed[0].bucket == base
    # Flushed buckets are removed -- a second flush at the same `now` finds nothing.
    assert aggregator.flush_stale(now=base + timedelta(seconds=60)) == []


def test_flush_stale_leaves_a_bucket_open_if_its_window_has_not_elapsed_yet() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    assert aggregator.flush_stale(now=base + timedelta(seconds=59)) == []


def test_flush_stale_tracks_multiple_instruments_independently() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10), instrument_id=501))
    aggregator.ingest(_tick(base + timedelta(seconds=20), instrument_id=502))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert {c.instrument_id for c in closed} == {501, 502}


def test_write_closed_bar_upserts_into_bars_intraday(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    closed = ClosedBar(
        instrument_id=iid,
        bucket=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        bar=OpenBar(
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("102"),
            volume=Decimal("0.01000000"),
            trades=4,
        ),
    )
    write_closed_bar(db_conn, closed)
    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, interval_sec, source"
        " FROM bars_intraday WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (
        Decimal("100.0000"),
        Decimal("105.0000"),
        Decimal("98.0000"),
        Decimal("102.0000"),
        Decimal("0.01000000"),
        4,
        60,
        6,
    )


def test_write_closed_bar_is_idempotent_on_conflict(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    bucket = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    first = ClosedBar(
        instrument_id=iid,
        bucket=bucket,
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
            trades=1,
        ),
    )
    second = ClosedBar(
        instrument_id=iid,
        bucket=bucket,
        bar=OpenBar(
            open=Decimal("1"),
            high=Decimal("9"),
            low=Decimal("1"),
            close=Decimal("5"),
            volume=Decimal("3"),
            trades=3,
        ),
    )
    write_closed_bar(db_conn, first)
    write_closed_bar(db_conn, second)
    rows = db_conn.execute(
        "SELECT close, trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchall()
    assert len(rows) == 1  # upserted, not duplicated
    assert rows[0] == (Decimal("5.0000"), 3)  # second write's values won
