"""latest_reference_price: one query, two very different callers.
_require_sufficient_cash refuses a stale MARKET order; _load_marks
logs and carries on -- equity must not vanish because a feed paused."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.paper.reference_price import StalePrice, latest_reference_price
from trading.streaming.seed_instruments import seed_crypto_instruments


def _bar(db_conn, iid: int, ts: datetime, close: str = "100") -> None:
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
        (iid, ts, close, close, close, close),
    )


def test_no_bar_at_all_returns_none(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    assert latest_reference_price(
        db_conn, iid, now=datetime(2026, 9, 25, 10, 5, tzinfo=UTC), max_age=timedelta(minutes=3)
    ) is None


def test_a_bar_179_seconds_old_is_fresh(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))
    result = latest_reference_price(
        db_conn,
        iid,
        now=datetime(2026, 9, 25, 10, 2, 59, tzinfo=UTC),
        max_age=timedelta(seconds=180),
    )
    assert result == (Decimal("100.0000"), datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))


def test_a_bar_181_seconds_old_is_stale(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))
    result = latest_reference_price(
        db_conn,
        iid,
        now=datetime(2026, 9, 25, 10, 3, 1, tzinfo=UTC),
        max_age=timedelta(seconds=180),
    )
    assert isinstance(result, StalePrice)
    assert result.instrument_id == iid
    assert result.age_seconds == 181.0
