from __future__ import annotations

from datetime import date

from trading.streaming.upstox_intraday_backfill import month_windows


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
