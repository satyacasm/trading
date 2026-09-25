"""backfill_window: fills the outage minutes from Binance klines without
ever touching a tick-built row or dispatching the forming minute."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trading.sources.binance_spot import SpotKline
from trading.streaming.seed_instruments import seed_crypto_instruments
from trading.streaming.spot_backfill import backfill_window, last_bar_ts


def _instrument(db_conn) -> int:
    return seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]


def _kline(minute: int, close: str = "63000") -> SpotKline:
    return SpotKline(
        ts=datetime(2026, 9, 25, 10, minute, tzinfo=UTC),
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("0"),
        trades=0,
    )


def test_until_is_clamped_to_the_current_minutes_start():
    """The forming minute is never dispatched -- clamping `until` rather
    than trusting the caller means a caller that (wrongly) asks for the
    current minute still doesn't get it."""
    captured = {}

    def _fetch(symbol, *, start_ms, end_ms, client=None):
        captured["start_ms"] = start_ms
        captured["end_ms"] = end_ms
        return []

    backfill_window(
        None,
        1,
        "BTCUSDT",
        since=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        until=datetime(2026, 9, 25, 10, 10, 30, tzinfo=UTC),
        fetch=_fetch,
        now=lambda: datetime(2026, 9, 25, 10, 10, 30, tzinfo=UTC),
    )
    # Clamped to 10:10:00, not the caller's 10:10:30.
    assert captured["end_ms"] == int(datetime(2026, 9, 25, 10, 10, tzinfo=UTC).timestamp() * 1000)


def test_a_zero_trade_minute_is_written(db_conn):
    iid = _instrument(db_conn)
    inserted = backfill_window(
        db_conn,
        iid,
        "BTCUSDT",
        since=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
        until=datetime(2026, 9, 25, 10, 2, tzinfo=UTC),
        fetch=lambda *a, **k: [_kline(0), _kline(1)],
        now=lambda: datetime(2026, 9, 25, 10, 5, tzinfo=UTC),
    )
    assert [k.ts.minute for k in inserted] == [0, 1]
    rows = db_conn.execute(
        "SELECT ts, volume, trades, source FROM bars_intraday "
        "WHERE instrument_id=%s ORDER BY ts",
        (iid,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][1] == Decimal("0")
    assert rows[0][3] == 11  # BINANCE_SPOT_KLINE


def test_an_existing_tick_built_row_is_never_overwritten(db_conn):
    """A strategy may already have been sent the tick-built bar --
    ON CONFLICT DO NOTHING is what keeps it from silently changing
    under a run that already saw it."""
    iid = _instrument(db_conn)
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
        (iid, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
    )
    inserted = backfill_window(
        db_conn,
        iid,
        "BTCUSDT",
        since=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
        until=datetime(2026, 9, 25, 10, 1, tzinfo=UTC),
        fetch=lambda *a, **k: [_kline(0, close="99999")],
        now=lambda: datetime(2026, 9, 25, 10, 5, tzinfo=UTC),
    )
    assert inserted == []  # ON CONFLICT DO NOTHING -- nothing was inserted
    row = db_conn.execute(
        "SELECT close FROM bars_intraday WHERE instrument_id=%s AND ts=%s",
        (iid, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
    ).fetchone()
    assert row[0] == Decimal("1.0000")  # untouched


def test_last_bar_ts_is_the_max_ts_for_that_instrument(db_conn):
    iid = _instrument(db_conn)
    assert last_bar_ts(db_conn, iid) is None
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 11)",
        (iid, datetime(2026, 9, 25, 10, 3, tzinfo=UTC)),
    )
    assert last_bar_ts(db_conn, iid) == datetime(2026, 9, 25, 10, 3, tzinfo=UTC)
