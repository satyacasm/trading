from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools, "_utcnow", lambda: _NOW)


def _candles(count: int, *, last_ts: datetime, close: str = "100.25") -> dict[str, object]:
    return {
        "instrument_id": 1,
        "interval": "1d",
        "candles": [
            {
                "ts": (last_ts - timedelta(days=count - 1 - offset)).isoformat(),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": "1000",
            }
            for offset in range(count)
        ],
    }


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_get_candles_returns_prices_as_strings() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(200, json=_candles(3, last_ts=_NOW))
    )
    result = await tools.get_candles(_deps(handler), 1, "1d", 3)
    assert result["candles"][0]["close"] == "100.25"
    assert isinstance(result["candles"][0]["close"], str)


@pytest.mark.anyio
async def test_get_candles_asks_the_gateway_for_string_precision() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=_candles(3, last_ts=_NOW))

    await tools.get_candles(_deps(httpx.MockTransport(handler)), 1, "1d", 3)
    assert seen["precision"] == "string"


@pytest.mark.anyio
async def test_get_candles_attaches_freshness() -> None:
    stale_end = _NOW - timedelta(days=17)
    handler = httpx.MockTransport(
        lambda request: httpx.Response(200, json=_candles(3, last_ts=stale_end))
    )
    result = await tools.get_candles(_deps(handler), 1, "1d", 3)
    assert result["freshness"]["stale"] is True


@pytest.mark.anyio
async def test_get_candles_refuses_an_unknown_instrument_with_the_gateway_wording() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(404, json={"detail": "no instrument with instrument_id=99"})
    )
    result = await tools.get_candles(_deps(handler), 99, "1d", 3)
    assert result["status"] == "REFUSED"
    assert "no instrument with instrument_id=99" in result["reason"]


@pytest.mark.anyio
async def test_data_freshness_reports_each_instrument_separately() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        instrument_id = int(request.url.path.rsplit("/", 1)[-1])
        last = _NOW if instrument_id == 1 else _NOW - timedelta(days=17)
        return httpx.Response(200, json=_candles(1, last_ts=last))

    result = await tools.get_data_freshness(_deps(httpx.MockTransport(handler)), [1, 2], "1d")
    by_id = {row["instrument_id"]: row for row in result["instruments"]}
    assert by_id[1]["stale"] is False
    assert by_id[2]["stale"] is True


@pytest.mark.anyio
async def test_data_freshness_flags_an_instrument_with_no_bars_at_all() -> None:
    empty = {"instrument_id": 1, "interval": "1d", "candles": []}
    handler = httpx.MockTransport(lambda request: httpx.Response(200, json=empty))
    result = await tools.get_data_freshness(_deps(handler), [1], "1d")
    assert result["instruments"][0]["stale"] is True
    assert result["any_stale"] is True


@pytest.mark.anyio
async def test_get_perp_context_passes_the_contract_filters_through() -> None:
    context = {
        "instrument_id": 1,
        "symbol": "DOGEUSDT",
        "step_size": "1",
        "min_qty": "1",
        "min_notional": "5",
        "liquidation_fee": "0.015",
        "max_leverage": "75",
        "latest_funding_rate": "0.0001",
        "latest_funding_time": "2026-09-01T08:00:00+00:00",
        "latest_mark_price": "0.21",
        "margin_tiers": [],
    }
    handler = httpx.MockTransport(lambda request: httpx.Response(200, json=context))
    result = await tools.get_perp_context(_deps(handler), 1)
    assert result["step_size"] == "1"
    assert result["min_notional"] == "5"


@pytest.mark.anyio
async def test_get_perp_context_refuses_a_spot_instrument_with_the_reason() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(
            404, json={"detail": "instrument_id=2 is not a perpetual (asset_class=CRYPTO)"}
        )
    )
    result = await tools.get_perp_context(_deps(handler), 2)
    assert result["status"] == "REFUSED"
    assert "not a perpetual" in result["reason"]
