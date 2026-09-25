"""Parsing Binance's spot klines payload into SpotKline rows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from trading.sources.binance_spot import (
    SpotKline,
    fetch_spot_klines,
    parse_spot_klines,
    spot_symbol,
)


def test_spot_symbol_strips_the_dash_and_uppercases():
    assert spot_symbol("BTC-USDT") == "BTCUSDT"


def test_parse_spot_klines_reads_the_row_shape():
    # [openTime, open, high, low, close, volume, closeTime, quoteVolume,
    # trades, takerBuyBase, takerBuyQuote, ignore]
    raw = json.dumps(
        [
            [
                1758700800000,
                "63000.00",
                "63100.50",
                "62950.00",
                "63050.25",
                "12.500000",
                1758700859999,
                "788125.00",
                340,
                "6.0",
                "378000.00",
                "0",
            ]
        ]
    ).encode()

    bars = parse_spot_klines(raw)

    assert bars == [
        SpotKline(
            ts=datetime.fromtimestamp(1758700800, UTC),
            open=Decimal("63000.00"),
            high=Decimal("63100.50"),
            low=Decimal("62950.00"),
            close=Decimal("63050.25"),
            volume=Decimal("12.500000"),
            trades=340,
        )
    ]


def test_zero_trade_minute_parses_with_zero_volume_and_zero_trades():
    """Klines fill every minute, traded or not -- a zero-trade minute
    must parse, not be skipped, or the backfill's whole point (no
    holes) is lost."""
    raw = json.dumps(
        [
            [
                1758700800000,
                "63000",
                "63000",
                "63000",
                "63000",
                "0",
                1758700859999,
                "0",
                0,
                "0",
                "0",
                "0",
            ]
        ]
    ).encode()

    bars = parse_spot_klines(raw)

    assert bars[0].volume == Decimal("0")
    assert bars[0].trades == 0


def test_fetch_spot_klines_paginates_and_stops_after_short_page():
    """Pagination stops after a short page, and the cursor advances by
    60_000 ms (1 minute) from the last bar's open time."""
    request_count = 0
    captured_requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        captured_requests.append(request)

        if request_count == 1:
            # First page: 1000 rows, open times 60_000 ms apart
            # Start from 1758700800000 (2026-09-25 10:00:00 UTC)
            rows = []
            for i in range(1000):
                open_time = 1758700800000 + (i * 60_000)
                rows.append(
                    [
                        open_time,
                        "63000",
                        "63100",
                        "62900",
                        "63050",
                        "10.5",
                        open_time + 59999,
                        "661650",
                        100,
                        "5.0",
                        "315000",
                        "0",
                    ]
                )
            return httpx.Response(200, content=json.dumps(rows).encode())
        elif request_count == 2:
            # Second page: 440 rows (short page, stops pagination)
            rows = []
            for i in range(440):
                open_time = 1758700800000 + (1000 * 60_000) + (i * 60_000)
                rows.append(
                    [
                        open_time,
                        "63000",
                        "63100",
                        "62900",
                        "63050",
                        "10.5",
                        open_time + 59999,
                        "661650",
                        100,
                        "5.0",
                        "315000",
                        "0",
                    ]
                )
            return httpx.Response(200, content=json.dumps(rows).encode())
        else:
            raise AssertionError(f"Unexpected request #{request_count}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    klines = fetch_spot_klines(
        "BTCUSDT",
        start_ms=1758700800000,
        end_ms=1758700800000 + (2000 * 60_000),
        client=client,
    )

    # All 1440 klines returned
    assert len(klines) == 1440
    # In order, no duplicates
    timestamps = [k.ts.timestamp() for k in klines]
    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == 1440

    # Second request's startTime = first page's last open time + 60_000
    assert request_count == 2
    second_request = captured_requests[1]
    first_last_open_time = 1758700800000 + (999 * 60_000)
    expected_second_start = first_last_open_time + 60_000
    assert int(second_request.url.params["startTime"]) == expected_second_start
