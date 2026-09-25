"""Parsing Binance's spot klines payload into SpotKline rows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from trading.sources.binance_spot import SpotKline, parse_spot_klines, spot_symbol


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
        [[1758700800000, "63000", "63000", "63000", "63000", "0", 1758700859999, "0", 0, "0", "0", "0"]]
    ).encode()

    bars = parse_spot_klines(raw)

    assert bars[0].volume == Decimal("0")
    assert bars[0].trades == 0
