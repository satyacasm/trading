"""Binance's public spot klines -- the backfill source for crypto minute
bars (docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md
§3), the same endpoint family as `trading.sources.binance_futures` but
unauthenticated and unmargined.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import httpx
import structlog

__all__ = [
    "KLINES_URL",
    "SpotKline",
    "fetch_spot_klines",
    "parse_spot_klines",
    "spot_symbol",
]

log = structlog.get_logger(__name__)

KLINES_URL = "https://api.binance.com/api/v3/klines"
KLINES_PAGE = 1000


def spot_symbol(pair: str) -> str:
    """'BTC-USDT' -> 'BTCUSDT' -- Binance's REST symbol, no separator,
    uppercase (the futures/WS feeds lowercase theirs; klines wants
    upper)."""
    return pair.replace("-", "").upper()


@dataclass(frozen=True)
class SpotKline:
    """One closed 1-minute spot kline. `ts` is the START of the interval,
    matching `PerpBar` and `bars_intraday.ts`."""

    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int


def parse_spot_klines(raw: bytes) -> list[SpotKline]:
    bars: list[SpotKline] = []
    for row in json.loads(raw):
        try:
            bars.append(
                SpotKline(
                    ts=datetime.fromtimestamp(int(row[0]) / 1000, UTC),
                    open=Decimal(str(row[1])),
                    high=Decimal(str(row[2])),
                    low=Decimal(str(row[3])),
                    close=Decimal(str(row[4])),
                    volume=Decimal(str(row[5])),
                    trades=int(row[8]),
                )
            )
        except (IndexError, KeyError, TypeError, ValueError, InvalidOperation):
            log.warning("binance_spot.kline_row_skipped")
    return bars


def fetch_spot_klines(
    symbol: str, *, start_ms: int, end_ms: int, client: httpx.Client | None = None
) -> list[SpotKline]:
    """Every closed 1-minute kline in `[start_ms, end_ms)`, paged.

    Bounded by `end_ms`, unlike `binance_futures.fetch_klines`'s
    walk-to-now: a backfill window is always `[since, until)` (design
    §3), never open-ended.
    """
    owned = client is None
    client = client or httpx.Client(timeout=30.0)
    collected: list[SpotKline] = []
    try:
        cursor = start_ms
        while cursor < end_ms:
            response = client.get(
                KLINES_URL,
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": KLINES_PAGE,
                },
            )
            response.raise_for_status()
            page = parse_spot_klines(response.content)
            if not page:
                return collected
            collected.extend(page)
            if len(page) < KLINES_PAGE:
                return collected
            cursor = int(page[-1].ts.timestamp() * 1000) + 60_000
        return collected
    finally:
        if owned:
            client.close()
