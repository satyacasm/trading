from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

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


def _rising_candles(count: int) -> dict[str, object]:
    return {
        "instrument_id": 1,
        "interval": "1d",
        "candles": [
            {
                "ts": (_NOW - timedelta(days=count - 1 - offset)).isoformat(),
                "open": str(offset + 1),
                "high": str(offset + 2),
                "low": str(offset),
                "close": str(offset + 1),
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


def _serving(count: int) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(200, json=_rising_candles(count)))


@pytest.mark.anyio
async def test_snapshot_returns_the_last_price_as_a_string() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(120)), [1], "1d", [], 5)
    instrument = result["instruments"][0]
    assert instrument["last_price"] == "120"
    assert isinstance(instrument["last_price"], str)


@pytest.mark.anyio
async def test_snapshot_trims_bars_to_the_requested_history() -> None:
    # The warmup is fetched but must not be dumped on the agent.
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    assert len(result["instruments"][0]["bars"]) == 5


@pytest.mark.anyio
async def test_snapshot_requests_history_plus_warmup_from_the_gateway() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=_rising_candles(200))

    # rsi14 needs max(5*14, 50) = 70 warmup bars on top of 5 of history.
    await tools.get_market_snapshot(_deps(httpx.MockTransport(handler)), [1], "1d", ["rsi14"], 5)
    assert int(seen["limit"]) == 75


@pytest.mark.anyio
async def test_snapshot_computes_the_requested_indicators() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    # A series that only rises has no losses, so Wilder RSI pins at 100.
    assert Decimal(result["instruments"][0]["indicators"]["rsi14"]) == Decimal(100)


@pytest.mark.anyio
async def test_snapshot_reports_multi_valued_indicators_as_a_mapping() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["bb20"], 5)
    bands = result["instruments"][0]["indicators"]["bb20"]
    assert set(bands) == {"lower", "mid", "upper"}
    assert isinstance(bands["mid"], str)


@pytest.mark.anyio
async def test_snapshot_says_when_the_warmup_was_not_available() -> None:
    # Only 30 bars exist, but rsi14 asked for 5 + 70. The number must not
    # be presented as though it converged.
    result = await tools.get_market_snapshot(_deps(_serving(30)), [1], "1d", ["rsi14"], 5)
    instrument = result["instruments"][0]
    assert instrument["warmup_sufficient"] is False
    assert instrument["warmup_bars_used"] == 70


@pytest.mark.anyio
async def test_snapshot_marks_warmup_sufficient_when_the_data_is_there() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    assert result["instruments"][0]["warmup_sufficient"] is True


@pytest.mark.anyio
async def test_snapshot_reports_none_for_an_indicator_with_too_little_data() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(10)), [1], "1d", ["rsi14"], 5)
    assert result["instruments"][0]["indicators"]["rsi14"] is None


@pytest.mark.anyio
async def test_snapshot_refuses_an_unknown_indicator_and_names_the_known_ones() -> None:
    # Refused outright rather than skipped: a snapshot missing what the
    # agent asked for, but shaped as though complete, is the worse failure.
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["supertrend"], 5)
    assert result["status"] == "REFUSED"
    assert "supertrend" in result["reason"]


@pytest.mark.anyio
async def test_snapshot_carries_freshness_per_instrument() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(120)), [1], "1d", [], 5)
    assert result["instruments"][0]["freshness"]["stale"] is False


@pytest.mark.anyio
async def test_snapshot_reports_one_instrument_refusal_without_losing_the_others() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/99"):
            return httpx.Response(404, json={"detail": "no instrument with instrument_id=99"})
        return httpx.Response(200, json=_rising_candles(120))

    result = await tools.get_market_snapshot(
        _deps(httpx.MockTransport(handler)), [1, 99], "1d", [], 5
    )
    by_id = {row["instrument_id"]: row for row in result["instruments"]}
    assert by_id[1]["last_price"] == "120"
    assert by_id[99]["status"] == "REFUSED"
