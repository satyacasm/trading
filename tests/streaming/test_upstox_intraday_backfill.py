from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest

from trading.contracts import DataSource
from trading.streaming.upstox_intraday_backfill import (
    BackfillCandle,
    backfill_symbol,
    fetch_candles,
    month_windows,
    parse_candle_response,
    write_backfill_candle,
)


def test_month_windows_single_full_month():
    windows = month_windows(date(2022, 1, 1), date(2022, 1, 31))
    assert windows == [(date(2022, 1, 1), date(2022, 1, 31))]


def test_month_windows_spans_multiple_months():
    windows = month_windows(date(2022, 1, 1), date(2022, 3, 15))
    assert windows == [
        (date(2022, 1, 1), date(2022, 1, 31)),
        (date(2022, 2, 1), date(2022, 2, 28)),
        (date(2022, 3, 1), date(2022, 3, 15)),
    ]


def test_month_windows_partial_first_month():
    windows = month_windows(date(2022, 1, 20), date(2022, 2, 10))
    assert windows == [
        (date(2022, 1, 20), date(2022, 1, 31)),
        (date(2022, 2, 1), date(2022, 2, 10)),
    ]


def test_month_windows_single_day():
    windows = month_windows(date(2024, 6, 15), date(2024, 6, 15))
    assert windows == [(date(2024, 6, 15), date(2024, 6, 15))]


def test_month_windows_empty_when_start_after_end():
    assert month_windows(date(2024, 1, 1), date(2023, 12, 31)) == []


def test_month_windows_handles_december_to_january_rollover():
    windows = month_windows(date(2022, 12, 15), date(2023, 1, 15))
    assert windows == [
        (date(2022, 12, 15), date(2022, 12, 31)),
        (date(2023, 1, 1), date(2023, 1, 15)),
    ]


def test_parse_candle_response_builds_candles_from_a_valid_payload():
    payload = {
        "status": "success",
        "data": {
            "candles": [
                ["2024-01-02T09:15:00+05:30", 2456.5, 2460.0, 2455.0, 2458.25, 12345, 0],
                ["2024-01-02T09:16:00+05:30", 2458.25, 2459.0, 2457.0, 2457.5, 6789, 0],
            ]
        },
    }

    candles = parse_candle_response(payload, instrument_id=501)

    assert len(candles) == 2
    first = candles[0]
    assert first == BackfillCandle(
        instrument_id=501,
        ts=datetime(2024, 1, 2, 3, 45, tzinfo=UTC),
        open=Decimal("2456.5"),
        high=Decimal("2460.0"),
        low=Decimal("2455.0"),
        close=Decimal("2458.25"),
        volume=Decimal("12345"),
        open_interest=0,
    )


def test_parse_candle_response_converts_ist_offset_to_utc():
    payload = {
        "status": "success",
        "data": {"candles": [["2024-06-15T15:29:00+05:30", 100.0, 101.0, 99.0, 100.5, 1, 0]]},
    }

    candles = parse_candle_response(payload, instrument_id=1)

    assert candles[0].ts == datetime(2024, 6, 15, 9, 59, tzinfo=UTC)


def test_parse_candle_response_returns_empty_list_for_no_candles_in_window():
    payload = {"status": "success", "data": {"candles": []}}

    assert parse_candle_response(payload, instrument_id=1) == []


def test_parse_candle_response_raises_when_data_key_is_missing():
    with pytest.raises(ValueError, match="candles"):
        parse_candle_response({"status": "success"}, instrument_id=1)


def test_parse_candle_response_raises_when_a_candle_row_has_the_wrong_arity():
    payload = {"status": "success", "data": {"candles": [["2024-01-02T09:15:00+05:30", 1.0, 2.0]]}}

    with pytest.raises(ValueError, match="7"):
        parse_candle_response(payload, instrument_id=1)


def test_parse_candle_response_raises_when_candles_is_not_a_list():
    payload = {"status": "success", "data": {"candles": None}}

    with pytest.raises(ValueError, match="candles"):
        parse_candle_response(payload, instrument_id=1)


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_candles_issues_the_documented_request_and_returns_json():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"status": "success", "data": {"candles": []}})

    client = _mock_client(handler)

    result = fetch_candles(
        client,
        instrument_key="NSE_EQ|INE002A01018",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 31),
        token="tok123",
    )

    assert result == {"status": "success", "data": {"candles": []}}
    assert captured["auth"] == "Bearer tok123"
    assert (
        captured["url"]
        == "https://api.upstox.com/v3/historical-candle/NSE_EQ|INE002A01018/minutes/1/2024-01-31/2024-01-01"
    )


def test_fetch_candles_raises_on_non_2xx():
    client = _mock_client(lambda request: httpx.Response(401, json={"error": "invalid token"}))

    with pytest.raises(httpx.HTTPStatusError):
        fetch_candles(
            client,
            instrument_key="NSE_EQ|INE002A01018",
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 31),
            token="badtoken",
        )


@pytest.mark.live
def test_live_fetch_candles_matches_the_documented_response_shape():
    """One real request against Upstox's historical-candle API, confirming
    the response shape this module assumes. Excluded from the default run.
    Requires a real, currently-valid UPSTOX_ACCESS_TOKEN (minted via
    `uv run python -m trading.auth.upstox`) -- skips cleanly if unset."""
    from trading.config import get_settings

    token = get_settings().upstox_access_token
    if not token:
        pytest.skip("UPSTOX_ACCESS_TOKEN not set")

    with httpx.Client(timeout=10.0) as client:
        payload = fetch_candles(
            client,
            instrument_key="NSE_EQ|INE002A01018",  # RELIANCE
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            token=token,
        )

    assert payload["status"] == "success"
    candles = payload["data"]["candles"]
    assert isinstance(candles, list)
    if candles:
        assert len(candles[0]) == 7


@pytest.fixture
def fixture_instrument_id(db_conn) -> int:
    """`db_conn` hands out a rolled-back transaction on a freshly migrated,
    otherwise-empty `trading_test` database (tests/conftest.py) -- there is
    no pre-seeded instrument row to reuse. This inserts one minimal, real
    `instruments` row so `bars_intraday`'s foreign key has something to
    resolve against, and returns its generated `instrument_id`."""
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('EQUITY', 'NSE', 'CM', 'RELIANCE', 'ACTIVE', 'NSE:CM:RELIANCE:EQ')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


@pytest.mark.db
def test_write_backfill_candle_inserts_a_row_with_null_trades_and_correct_source(
    db_conn, fixture_instrument_id
):
    candle = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 45, tzinfo=UTC),
        open=Decimal("100.5"),
        high=Decimal("101.0"),
        low=Decimal("99.5"),
        close=Decimal("100.75"),
        volume=Decimal("12345"),
        open_interest=0,
    )

    write_backfill_candle(db_conn, candle)

    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, source, open_interest "
        "FROM bars_intraday WHERE instrument_id = %s AND ts = %s AND interval_sec = 60",
        (fixture_instrument_id, datetime(2024, 1, 2, 3, 45, tzinfo=UTC)),
    ).fetchone()

    assert row is not None
    assert row[0] == Decimal("100.5")
    assert row[4] == Decimal("12345")
    assert row[5] is None  # trades always NULL for backfilled rows
    assert row[6] == DataSource.UPSTOX_HISTORICAL_CANDLE
    assert row[7] == 0


@pytest.mark.db
def test_write_backfill_candle_upserts_rather_than_duplicates(db_conn, fixture_instrument_id):
    candle = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 46, tzinfo=UTC),
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("1"),
        open_interest=None,
    )
    write_backfill_candle(db_conn, candle)

    updated = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 46, tzinfo=UTC),
        open=Decimal("2"),
        high=Decimal("2"),
        low=Decimal("2"),
        close=Decimal("2"),
        volume=Decimal("2"),
        open_interest=None,
    )
    write_backfill_candle(db_conn, updated)

    rows = db_conn.execute(
        (
            "SELECT close FROM bars_intraday "
            "WHERE instrument_id = %s AND ts = %s AND interval_sec = 60"
        ),
        (fixture_instrument_id, datetime(2024, 1, 2, 3, 46, tzinfo=UTC)),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == Decimal("2")


def test_backfill_symbol_writes_candles_across_windows_and_reports_count(
    db_conn, fixture_instrument_id
):
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {"candles": [["2022-01-15T09:15:00+05:30", 1.0, 1.0, 1.0, 1.0, 1, 0]]},
            },
        )

    client = _mock_client(handler)
    sleeps: list[float] = []

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 1, 31),
        sleep=sleeps.append,
    )

    assert call_count["n"] == 1  # single-month window
    assert report.candles_written == 1
    assert report.skipped_windows == []
    assert report.instrument_key == "NSE_EQ|INE002A01018"


def test_backfill_symbol_retries_once_then_skips_on_persistent_transient_failure(
    db_conn, fixture_instrument_id
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server error")

    client = _mock_client(handler)
    sleeps: list[float] = []

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 1, 31),
        sleep=sleeps.append,
    )

    assert report.candles_written == 0
    assert report.skipped_windows == [(date(2022, 1, 1), date(2022, 1, 31))]
    assert 2.0 in sleeps  # the retry backoff


def test_backfill_symbol_aborts_immediately_on_401(db_conn, fixture_instrument_id):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad token"})

    client = _mock_client(handler)

    with pytest.raises(httpx.HTTPStatusError):
        backfill_symbol(
            db_conn,
            client,
            instrument_key="NSE_EQ|INE002A01018",
            instrument_id=fixture_instrument_id,
            token="badtoken",
            start=date(2022, 1, 1),
            end=date(2022, 3, 31),
            sleep=lambda _: None,
        )


def test_backfill_symbol_continues_past_a_skipped_window_to_the_next(
    db_conn, fixture_instrument_id
):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "2022-01-31" in str(request.url):
            return httpx.Response(500, text="server error")
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {"candles": [["2022-02-01T09:15:00+05:30", 1.0, 1.0, 1.0, 1.0, 1, 0]]},
            },
        )

    client = _mock_client(handler)

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 2, 28),
        sleep=lambda _: None,
    )

    assert report.candles_written == 1
    assert report.skipped_windows == [(date(2022, 1, 1), date(2022, 1, 31))]
