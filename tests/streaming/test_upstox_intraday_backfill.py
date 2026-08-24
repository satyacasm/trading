from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from trading.streaming.upstox_intraday_backfill import (
    BackfillCandle,
    month_windows,
    parse_candle_response,
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
