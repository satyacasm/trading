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


def test_a_bar_179_seconds_past_close_is_fresh(db_conn) -> None:
    """`bars_intraday.ts` is the bar's OPEN time. A 1m bar opened at
    10:00:00 closes at 10:01:00 -- staleness is measured from there, not
    from the open, or a bar would be reported up to `interval_sec`
    seconds staler than it really is."""
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))  # closes at 10:01:00
    result = latest_reference_price(
        db_conn,
        iid,
        now=datetime(2026, 9, 25, 10, 3, 59, tzinfo=UTC),  # 179s past close
        max_age=timedelta(seconds=180),
    )
    assert result == (Decimal("100.0000"), datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))


def test_a_bar_181_seconds_past_close_is_stale(db_conn) -> None:
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))  # closes at 10:01:00
    result = latest_reference_price(
        db_conn,
        iid,
        now=datetime(2026, 9, 25, 10, 4, 1, tzinfo=UTC),  # 181s past close
        max_age=timedelta(seconds=180),
    )
    assert isinstance(result, StalePrice)
    assert result.instrument_id == iid
    assert result.age_seconds == 181.0


def test_staleness_is_measured_from_close_not_open(db_conn) -> None:
    """The regression this ruling exists for: a bar whose CLOSE is 150s
    ago (fresh, under a 180s ceiling) but whose OPEN is 210s ago (which
    the old open-time computation would have called stale) must count
    as fresh. This fails under `age = now - ts` and passes under
    `age = now - (ts + interval_sec)`."""
    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    now = datetime(2026, 9, 25, 10, 5, 0, tzinfo=UTC)
    opened_at = now - timedelta(seconds=210)  # closes 60s later, i.e. 150s ago
    _bar(db_conn, iid, opened_at)
    result = latest_reference_price(db_conn, iid, now=now, max_age=timedelta(seconds=180))
    assert result == (Decimal("100.0000"), opened_at)
