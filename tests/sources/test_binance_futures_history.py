"""Parsing Binance USDⓈ-M funding history and klines."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from trading.sources.binance_futures import parse_funding_history, parse_klines

_FUNDING = [
    {
        "symbol": "BTCUSDT",
        "fundingTime": 1788508800000,
        "fundingRate": "0.00006498",
        "markPrice": "80628.00000000",
    },
    # The earliest settlements Binance served in 2019 carry no mark price.
    {
        "symbol": "BTCUSDT",
        "fundingTime": 1568102400000,
        "fundingRate": "0.00010000",
        "markPrice": "",
    },
]

_KLINE = [
    [
        1567900800000,
        "10000",
        "10412.65",
        "10000",
        "10391.63",
        "3096.291",
        1567987199999,
        "32096280.44997",
        3754,
        "0.039",
        "393.35627",
        "0",
    ]
]


def test_a_settlement_becomes_a_dated_rate() -> None:
    first, _ = parse_funding_history(json.dumps(_FUNDING).encode())
    # Settlements land on the 00/08/16 UTC boundaries, never between.
    assert first.funding_time == datetime(2026, 9, 4, 8, 0, tzinfo=UTC)
    # Decimal: this multiplies a position notional to move real cash.
    assert first.rate == Decimal("0.00006498")
    assert first.mark_price == Decimal("80628.00000000")


def test_a_settlement_without_a_mark_is_kept_with_a_null_mark() -> None:
    """Binance's 2019 rows have an empty markPrice. Dropping them would
    lose real settlements a position held then actually paid; inventing a
    mark would be worse."""
    _, early = parse_funding_history(json.dumps(_FUNDING).encode())
    assert early.rate == Decimal("0.00010000")
    assert early.mark_price is None


def test_a_kline_is_stamped_at_the_start_of_its_interval() -> None:
    """Contract §5: `ts` marks the START of an intraday interval, and a
    crypto day is a true 24-hour interval, so `ts + 86400` is the close.
    That is why a perp daily bar needs no `knowable_at` where an NSE daily
    bar does -- an NSE session is 6h15m inside a 24-hour calendar day, so
    no arithmetic on `ts` reaches its close."""
    (bar,) = parse_klines(json.dumps(_KLINE).encode())
    assert bar.ts == datetime(2019, 9, 8, 0, 0, tzinfo=UTC)
    assert bar.open == Decimal("10000")
    assert bar.high == Decimal("10412.65")
    assert bar.low == Decimal("10000")
    assert bar.close == Decimal("10391.63")
    assert bar.volume == Decimal("3096.291")
    assert bar.trades == 3754


def test_an_unparseable_row_is_skipped_rather_than_killing_the_page() -> None:
    payload = json.dumps([_KLINE[0], ["nonsense"]]).encode()
    assert len(parse_klines(payload)) == 1
