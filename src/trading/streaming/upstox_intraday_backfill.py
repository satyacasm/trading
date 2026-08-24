"""Upstox V3 historical-candle backfill for the NSE equity watchlist.

One-shot, synchronous CLI: walks each watchlist symbol's history in
month-sized windows (Upstox's per-request cap for 1-minute candles) from
2022-01-01 (the API's own floor for 1-minute data) through yesterday,
upserting into `bars_intraday` under `DataSource.UPSTOX_HISTORICAL_CANDLE`.

Deliberately does not reuse `bar_aggregator`'s `OpenBar`/`ClosedBar`/
`write_closed_bar` -- those model tick accumulation (a running trade count),
which a pre-aggregated REST candle doesn't have. See this module's own
`write_backfill_candle` instead.

Usage: uv run python -m trading.streaming.upstox_intraday_backfill
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month-aligned (from_date, to_date)
    pairs, each spanning at most one calendar month -- Upstox's V3
    historical-candle API caps 1-minute candle requests at ~1 month per
    call. Empty list if start > end."""
    if start > end:
        return []

    windows: list[tuple[date, date]] = []
    window_start = start
    while window_start <= end:
        last_day_of_month = calendar.monthrange(window_start.year, window_start.month)[1]
        month_end = date(window_start.year, window_start.month, last_day_of_month)
        window_end = min(month_end, end)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows
