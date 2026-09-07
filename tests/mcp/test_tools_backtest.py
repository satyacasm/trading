from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient, GatewayUnavailable
from trading.mcp.session import SessionRefused, SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, 30, 15, 123456, tzinfo=UTC)

# Shaped exactly like `UploadStrategyResponse` (trading/agent_contract/api.py),
# not the brief's guess -- that route reports `accepted`/`verdict`, never a
# `status` field, and every finding carries `line`/`contract_section` too.
_ACCEPTED = {
    "accepted": True,
    "verdict": "PASSED",
    "strategy_id": 3,
    "feedback": "accepted on the first pass",
    "findings": [],
    "window": {"start": "2024-01-01", "end": "2024-01-08"},
    "runtime": "runc",
    "kernel_isolated": True,
    "summary": {
        "bar_calls": 5,
        "orders": 1,
        "fills": 1,
        "starting_cash": "100000",
        "final_cash": "99000",
        "final_equity": "99500",
        "pnl": "-500",
        "pnl_pct": "-0.5",
        "currency": "INR",
        "rejections": 0,
        "rejection_reasons": [],
        "breaker_reason": None,
    },
}
_REJECTED = {
    "accepted": False,
    "verdict": "REJECTED",
    "strategy_id": None,
    "feedback": "1 finding: E_IMPORT -- import of 'socket' is not permitted",
    "findings": [
        {
            "code": "E_IMPORT",
            "message": "import of 'socket' is not permitted",
            "line": 3,
            "contract_section": "3.2",
        }
    ],
    "window": None,
    "runtime": None,
    "kernel_isolated": None,
    "summary": None,
}

# Shaped like `BacktestResponse` (same module): every money field and the
# curve are already text via `_as_str`/`str(...)`, so nothing here is a
# JSON float the way `Portfolio`/`Position`/`Order` are.
_PASSED = {
    "strategy_id": 3,
    "status": "PASSED",
    "backtest_run_id": 9,
    "bars": "1d",
    "window_start": "2024-01-01",
    "window_end": "2026-09-01",
    "dispatch_from": "2023-12-01",
    "sessions": 660,
    "history_bars_requested": 20,
    "history_bars_available": 20,
    "bar_calls": 660,
    "fills": 12,
    "final_cash": "4200.75",
    "final_equity": "104200.75",
    "breaker_reason": None,
    "equity_curve": [{"ts": "2026-01-01T00:00:00+00:00", "equity": "100000", "cash": "100000"}],
    "fills_ledger": [],
    "findings": [],
    "notes": [],
    "runtime": "runc",
    "kernel_isolated": True,
}

# Shaped like `BacktestDetail`: a superset of `BacktestSummary` plus the
# curve/fills/funding/liquidations/metrics/notes/stress/reshuffle that only
# the single-run read carries.
_DETAIL = {
    "backtest_run_id": 9,
    "strategy_id": 3,
    "status": "PASSED",
    "requested_start": "2024-01-01",
    "requested_end": "2026-09-01",
    "fetch_start": "2023-12-01",
    "dispatch_from": "2023-12-01",
    "sessions": 660,
    "instruments": [7],
    "history_bars_requested": 20,
    "history_bars_available": 20,
    "bars": "1d",
    "bar_calls": 660,
    "orders_placed": 12,
    "fills": 12,
    "final_cash": "4200.75",
    "final_equity": "104200.75",
    "breaker_reason": None,
    "error": None,
    "findings": [],
    "runtime": "runc",
    "kernel_isolated": True,
    "contract_version": "0.1",
    "ran_at": "2026-09-07T12:00:00+00:00",
    "max_daily_loss": None,
    "max_drawdown_pct": None,
    "equity_curve": [{"ts": "2026-01-01T00:00:00+00:00", "equity": "100000", "cash": "100000"}],
    "fills_ledger": [],
    "funding": [],
    "liquidations": [],
    "metrics": {"sharpe": "1.2"},
    "notes": [],
    "stress": None,
    "reshuffle": None,
}


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools, "_utcnow", lambda: _NOW)


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


def _read_json(request: httpx.Request) -> dict[str, object]:
    return dict(json.loads(request.read()))


def _unknown_token_deps() -> ToolDeps:
    """A session that must refuse before any request reaches the wire."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request should have been sent")

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ToolDeps(
        client=GatewayClient("http://gateway", http),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "unknown-token",
    )


# ---------------------------------------------------------------- submit_strategy


@pytest.mark.anyio
async def test_submit_strategy_sends_the_name_version_and_source() -> None:
    # The real route (`UploadStrategyRequest`) takes `name`/`version`/`source`,
    # not `name`/`code` as the brief's sample body guessed.
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        assert request.url.path == "/strategies"
        return httpx.Response(200, json=_ACCEPTED)

    result = await tools.submit_strategy(_deps(httpx.MockTransport(handler)), "ema-cross", "code")
    assert sent["name"] == "ema-cross"
    assert sent["source"] == "code"
    assert "code" not in sent
    assert result["strategy_id"] == 3


@pytest.mark.anyio
async def test_submit_strategy_derives_a_version_from_the_clock() -> None:
    # There is no `version` parameter on this tool -- an agent iterating on
    # one strategy resubmits the same `name` after every fix, and a version
    # already on file with different source is a `VersionConflict` the
    # route does not catch. A fresh version per call keeps every attempt
    # its own row without ever asking the caller to name one.
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        return httpx.Response(200, json=_ACCEPTED)

    await tools.submit_strategy(_deps(httpx.MockTransport(handler)), "ema-cross", "code")
    assert sent["version"] == "20260907T123015123456"


@pytest.mark.anyio
async def test_a_rejected_strategy_returns_its_findings_rather_than_an_error() -> None:
    # The findings are the whole point: they tell the agent what to fix.
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_REJECTED))
    result = await tools.submit_strategy(_deps(handler), "bad", "import socket")
    assert result["verdict"] == "REJECTED"
    assert "socket" in result["findings"][0]["message"]


@pytest.mark.anyio
async def test_submit_strategy_a_request_validation_refusal_is_returned_as_data() -> None:
    # Driven through the real GatewayClient._decode via httpx.MockTransport,
    # not hand-thrown -- this is what a name failing UploadStrategyRequest's
    # own field constraints (FastAPI's 422) actually looks like.
    handler = httpx.MockTransport(
        lambda r: httpx.Response(422, json={"detail": "name: field required"})
    )
    result = await tools.submit_strategy(_deps(handler), "", "code")
    assert result["status"] == "REFUSED"
    assert "field required" in result["reason"]


@pytest.mark.anyio
async def test_submit_strategy_refuses_before_any_request_when_the_token_is_unknown() -> None:
    with pytest.raises(SessionRefused):
        await tools.submit_strategy(_unknown_token_deps(), "ema-cross", "code")


# ------------------------------------------------------------------- run_backtest


@pytest.mark.anyio
async def test_run_backtest_posts_to_the_strategys_backtests_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")
    assert seen == [("POST", "/strategies/3/backtests")]


@pytest.mark.anyio
async def test_run_backtest_passes_the_window_through() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")
    assert sent["start"] == "2024-01-01"
    assert sent["end"] == "2026-09-01"


@pytest.mark.anyio
async def test_run_backtest_omits_overrides_that_were_not_given() -> None:
    # Sending starting_cash=None would override the manifest with nothing.
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")
    assert "starting_cash" not in sent
    assert "max_daily_loss" not in sent
    assert "max_drawdown_pct" not in sent


@pytest.mark.anyio
async def test_run_backtest_forwards_an_explicit_starting_cash() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(
        _deps(httpx.MockTransport(handler)),
        3,
        "2024-01-01",
        "2026-09-01",
        starting_cash="50000",
    )
    assert sent["starting_cash"] == "50000"


@pytest.mark.anyio
async def test_run_backtest_forwards_the_risk_limit_overrides() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(_read_json(request))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(
        _deps(httpx.MockTransport(handler)),
        3,
        "2024-01-01",
        "2026-09-01",
        max_daily_loss="2000",
        max_drawdown_pct="0.1",
    )
    assert sent["max_daily_loss"] == "2000"
    assert sent["max_drawdown_pct"] == "0.1"


@pytest.mark.anyio
async def test_run_backtest_returns_the_curve_and_the_metrics() -> None:
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_PASSED))
    result = await tools.run_backtest(_deps(handler), 3, "2024-01-01", "2026-09-01")
    assert result["final_equity"] == "104200.75"
    assert isinstance(result["final_equity"], str)
    assert result["equity_curve"][0]["equity"] == "100000"


@pytest.mark.anyio
async def test_run_backtest_on_an_unknown_strategy_is_a_refusal() -> None:
    # Driven through the real GatewayClient._decode: run_backtest raises
    # HTTPException(404, ...) on a KeyError from `backtest()`.
    handler = httpx.MockTransport(
        lambda r: httpx.Response(404, json={"detail": "no strategy with strategy_id=99"})
    )
    result = await tools.run_backtest(_deps(handler), 99, "2024-01-01", "2026-09-01")
    assert result["status"] == "REFUSED"
    assert "no strategy" in result["reason"]


@pytest.mark.anyio
async def test_run_backtest_refuses_before_any_request_when_the_token_is_unknown() -> None:
    with pytest.raises(SessionRefused):
        await tools.run_backtest(_unknown_token_deps(), 3, "2024-01-01", "2026-09-01")


@pytest.mark.anyio
async def test_run_backtest_propagates_a_gateway_unavailable() -> None:
    # POST is never retried, and run_backtest adds no catch of its own for a
    # 5xx -- a crash must reach the caller as an exception, not an adapted
    # REFUSED.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    with pytest.raises(GatewayUnavailable):
        await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")


# -------------------------------------------------------------------- get_backtest


@pytest.mark.anyio
async def test_get_backtest_reads_the_specific_run_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(200, json=_DETAIL)

    await tools.get_backtest(_deps(httpx.MockTransport(handler)), 9)
    assert seen == [("GET", "/backtests/9")]


@pytest.mark.anyio
async def test_get_backtest_reads_a_stored_run() -> None:
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_DETAIL))
    result = await tools.get_backtest(_deps(handler), 9)
    assert result["backtest_run_id"] == 9
    assert result["equity_curve"][0]["equity"] == "100000"
    assert result["final_equity"] == "104200.75"


@pytest.mark.anyio
async def test_get_backtest_on_an_unknown_run_is_a_refusal() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(404, json={"detail": "no backtest run with backtest_run_id=404"})
    )
    result = await tools.get_backtest(_deps(handler), 404)
    assert result["status"] == "REFUSED"
    assert "no backtest run" in result["reason"]


@pytest.mark.anyio
async def test_get_backtest_refuses_before_any_request_when_the_token_is_unknown() -> None:
    with pytest.raises(SessionRefused):
        await tools.get_backtest(_unknown_token_deps(), 9)
