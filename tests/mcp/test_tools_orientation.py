from __future__ import annotations

import httpx
import pytest

from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, get_capabilities, get_strategy_contract, list_instruments

# `InstrumentSummary` (gateway.py:100-105) carries exactly these four
# fields -- no segment, no lot size, no tick size. The mock must not
# invent columns the real route does not serve.
_INSTRUMENTS = [
    {"instrument_id": 1, "symbol": "BTCUSDT", "asset_class": "PERP", "exchange": "BINANCE_FUTURES"},
    {"instrument_id": 2, "symbol": "RELIANCE", "asset_class": "EQUITY", "exchange": "NSE"},
    {"instrument_id": 3, "symbol": "NIFTY", "asset_class": "INDEX", "exchange": "NSE"},
]


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


def _routes(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/instruments":
        return httpx.Response(200, json=_INSTRUMENTS)
    if request.url.path == "/strategies/contract":
        return httpx.Response(200, json={"version": "1.0", "schema": {"type": "object"}})
    return httpx.Response(404, json={"detail": "no route"})


@pytest.mark.anyio
async def test_capabilities_name_only_the_asset_classes_that_can_be_traded() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert set(result["tradeable_asset_classes"]) == {"EQUITY", "CRYPTO", "PERP"}


@pytest.mark.anyio
async def test_capabilities_list_every_indicator_with_a_description() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert "rsi" in result["indicators"]
    assert result["indicators"]["rsi"]


@pytest.mark.anyio
async def test_capabilities_carry_the_order_vocabulary() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert set(result["order_types"]) == {"MARKET", "LIMIT"}
    assert set(result["sides"]) == {"BUY", "SELL"}
    assert set(result["products"]) == {"DELIVERY", "INTRADAY"}
    assert set(result["time_in_force"]) == {"DAY", "GTC"}


@pytest.mark.anyio
async def test_list_instruments_returns_every_instrument_by_default() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, None)
    assert len(result["instruments"]) == 3


@pytest.mark.anyio
async def test_list_instruments_filters_by_asset_class() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), "PERP", None)
    assert [i["symbol"] for i in result["instruments"]] == ["BTCUSDT"]


@pytest.mark.anyio
async def test_list_instruments_matches_a_symbol_substring_case_insensitively() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, "reli")
    assert [i["symbol"] for i in result["instruments"]] == ["RELIANCE"]


@pytest.mark.anyio
async def test_list_instruments_marks_which_ones_cannot_be_traded() -> None:
    # An INDEX has no charge schedule, so an order in it would be refused.
    # Saying so here saves the agent a rejection it cannot diagnose.
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, None)
    by_symbol = {i["symbol"]: i for i in result["instruments"]}
    assert by_symbol["NIFTY"]["tradeable"] is False
    assert by_symbol["RELIANCE"]["tradeable"] is True


@pytest.mark.anyio
async def test_get_strategy_contract_passes_the_bundle_through() -> None:
    result = await get_strategy_contract(_deps(httpx.MockTransport(_routes)))
    assert result["version"] == "1.0"
