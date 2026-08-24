from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from websockets.asyncio.client import connect as ws_connect

from trading.streaming.binance_feed import _stream_url, parse_trade_message


def test_stream_url_joins_multiple_pairs_lowercase_no_dash() -> None:
    url = _stream_url(["BTC-USDT", "ETH-USDT"])
    assert url == ("wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade")


def _envelope(**data_overrides: object) -> str:
    data = {
        "e": "trade",
        "s": "BTCUSDT",
        "p": "65000.50",
        "q": "0.01000000",
        "T": 1724500000000,
        "m": False,
    }
    data.update(data_overrides)
    return json.dumps({"stream": "btcusdt@trade", "data": data})


def test_parse_trade_message_builds_a_tick() -> None:
    tick = parse_trade_message(_envelope(), instrument_ids={"btcusdt": 501})

    assert tick is not None
    assert tick.instrument_id == 501
    assert tick.price == Decimal("65000.50")
    assert tick.quantity == Decimal("0.01000000")
    assert tick.side == "buy"  # m=False: taker bought
    assert tick.ts.year == 2024  # 1724500000000 ms


def test_parse_trade_message_maps_maker_flag_to_sell_side() -> None:
    tick = parse_trade_message(_envelope(m=True), instrument_ids={"btcusdt": 501})
    assert tick is not None
    assert tick.side == "sell"


def test_parse_trade_message_ignores_an_untracked_symbol() -> None:
    tick = parse_trade_message(_envelope(s="ETHUSDT"), instrument_ids={"btcusdt": 501})
    assert tick is None


def test_parse_trade_message_ignores_a_non_trade_event() -> None:
    tick = parse_trade_message(_envelope(e="aggTrade"), instrument_ids={"btcusdt": 501})
    assert tick is None


def test_parse_trade_message_returns_none_for_malformed_json() -> None:
    assert parse_trade_message("not json", instrument_ids={"btcusdt": 501}) is None


def test_parse_trade_message_returns_none_for_a_missing_field() -> None:
    raw = json.dumps({"stream": "btcusdt@trade", "data": {"e": "trade", "s": "BTCUSDT"}})
    assert parse_trade_message(raw, instrument_ids={"btcusdt": 501}) is None


@pytest.mark.live
def test_live_trade_message_matches_the_documented_shape() -> None:
    """One real message from Binance's public feed, shape-checked against
    what parse_trade_message expects. Excluded from the default run."""

    async def _probe() -> str:
        async with ws_connect(_stream_url(["BTC-USDT"])) as connection:
            return await asyncio.wait_for(connection.recv(), timeout=15)

    raw = asyncio.run(_probe())
    envelope = json.loads(raw)
    assert envelope["stream"] == "btcusdt@trade"
    data = envelope["data"]
    assert data["e"] == "trade"
    assert {"s", "p", "q", "T", "m"}.issubset(data)
