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
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
from psycopg import Connection

from trading.contracts import DataSource
from trading.streaming.bar_aggregator import INTERVAL_SECONDS


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


@dataclass(frozen=True)
class BackfillCandle:
    instrument_id: int
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_interest: int | None


def parse_candle_response(payload: dict[str, Any], instrument_id: int) -> list[BackfillCandle]:
    """Parse one historical-candle API response into BackfillCandles.

    Raises ValueError for a response shape that doesn't match the
    documented contract -- not a silent skip. A REST response is one
    deliberate, retryable request; a shape mismatch means the API contract
    changed, and every subsequent window in this run would fail identically,
    so it must surface immediately rather than be swallowed row by row.
    """
    data = payload.get("data")
    if not isinstance(data, dict) or "candles" not in data:
        raise ValueError(f"response missing data.candles: {payload!r}")

    raw_candles = data["candles"]
    if not isinstance(raw_candles, list):
        raise ValueError(f"data.candles must be a list, got {type(raw_candles).__name__}")

    candles: list[BackfillCandle] = []
    for row in raw_candles:
        if len(row) != 7:
            raise ValueError(f"expected 7 elements per candle row, got {len(row)}: {row!r}")
        ts_str, open_, high, low, close, volume, open_interest = row
        candles.append(
            BackfillCandle(
                instrument_id=instrument_id,
                ts=datetime.fromisoformat(ts_str).astimezone(UTC),
                open=Decimal(str(open_)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=Decimal(str(volume)),
                open_interest=int(open_interest) if open_interest is not None else None,
            )
        )
    return candles


_BASE_URL = "https://api.upstox.com"


def fetch_candles(
    client: httpx.Client, instrument_key: str, from_date: date, to_date: date, token: str
) -> dict[str, Any]:
    """One GET against Upstox's V3 historical-candle endpoint for 1-minute
    candles. Raises httpx.HTTPStatusError on any non-2xx response --
    callers distinguish 401/403 (abort the whole backfill) from other
    statuses (retry-then-skip this window) via the exception's
    response.status_code."""
    url = (
        f"{_BASE_URL}/v3/historical-candle/{instrument_key}/minutes/1/"
        f"{to_date.isoformat()}/{from_date.isoformat()}"
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = client.get(url, headers=headers)
    response.raise_for_status()
    return response.json()  # type: ignore[no-any-return]


_UPSERT_BACKFILL_CANDLE = """
    INSERT INTO bars_intraday (
        instrument_id, ts, interval_sec, open, high, low, close,
        volume, trades, open_interest, source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        open_interest = EXCLUDED.open_interest, source = EXCLUDED.source
"""


def write_backfill_candle(conn: Connection, candle: BackfillCandle) -> None:
    """Upsert one backfilled candle into bars_intraday. trades is always
    written NULL -- the historical-candle API gives no trade count, and
    writing 0 would falsely claim zero trades occurred. Never commits; the
    caller controls transaction boundaries (see this module's main())."""
    conn.execute(
        _UPSERT_BACKFILL_CANDLE,
        (
            candle.instrument_id,
            candle.ts,
            INTERVAL_SECONDS,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.open_interest,
            DataSource.UPSTOX_HISTORICAL_CANDLE,
        ),
    )
