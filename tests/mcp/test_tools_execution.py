from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, 30, 15, tzinfo=UTC)
_ORDER = {
    "order_id": 11,
    "portfolio_id": 1,
    "instrument_id": 7,
    "status": "PENDING",
    "side": "BUY",
    "quantity": "10",
    "order_type": "MARKET",
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


async def _place(deps: ToolDeps, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "instrument_id": 7,
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": "10",
        "product": "DELIVERY",
        "rationale": "momentum breakout",
    }
    kwargs.update(overrides)
    return await tools.place_order(deps, **kwargs)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_place_order_injects_the_session_portfolio() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(request.read() and __import__("json").loads(request.read()))
        return httpx.Response(201, json=_ORDER)

    await _place(_deps(httpx.MockTransport(handler)))
    assert sent["portfolio_id"] == 1


@pytest.mark.anyio
async def test_place_order_ignores_a_caller_supplied_portfolio_id() -> None:
    # `portfolio_id` is not a parameter at all, so this must be a
    # TypeError rather than a silently honoured override.
    with pytest.raises(TypeError):
        await _place(
            _deps(httpx.MockTransport(lambda r: httpx.Response(201, json=_ORDER))), portfolio_id=2
        )


@pytest.mark.anyio
async def test_place_order_derives_a_stable_key_within_the_same_minute() -> None:
    first = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    second = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    assert first == second


@pytest.mark.anyio
async def test_a_different_quantity_derives_a_different_key() -> None:
    first = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    second = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "11", None, _NOW)
    assert first != second


@pytest.mark.anyio
async def test_an_explicit_idempotency_key_is_used_unchanged() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(__import__("json").loads(request.read()))
        return httpx.Response(201, json=_ORDER)

    await _place(_deps(httpx.MockTransport(handler)), idempotency_key="mine-1")
    assert sent["idempotency_key"] == "mine-1"


@pytest.mark.anyio
async def test_place_order_sends_the_derived_key_when_none_is_supplied() -> None:
    # The two tests above pin the derivation function's own properties in
    # isolation. This pins that place_order actually wires that function's
    # output into the request body it sends -- the plumbing between them.
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(201, json=_ORDER)

    await _place(_deps(httpx.MockTransport(handler)))
    expected = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    assert sent["idempotency_key"] == expected


@pytest.mark.anyio
async def test_a_refusal_reaches_the_agent_with_the_gateway_wording() -> None:
    detail = "insufficient cash: order needs 500, portfolio has 100"
    handler = httpx.MockTransport(lambda r: httpx.Response(400, json={"detail": detail}))
    result = await _place(_deps(handler))
    assert result["status"] == "REFUSED"
    assert result["reason"] == detail


@pytest.mark.anyio
async def test_a_market_closed_refusal_is_not_an_exception() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(400, json={"detail": "market closed for NSE CM on 2026-09-07"})
    )
    result = await _place(_deps(handler))
    assert result["status"] == "REFUSED"
    assert "market closed" in result["reason"]


@pytest.mark.anyio
async def test_a_timed_out_write_is_retried_once_with_the_same_key() -> None:
    # POST /orders is idempotent on idempotency_key, so retrying with the
    # same key yields exactly one order whether or not the first attempt
    # committed.
    attempts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = __import__("json").loads(request.read())
        attempts.append(str(body["idempotency_key"]))
        if len(attempts) == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(201, json=_ORDER)

    result = await _place(_deps(httpx.MockTransport(handler)))
    assert result["order_id"] == 11
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]


@pytest.mark.anyio
async def test_a_write_that_keeps_timing_out_reports_the_key_to_reconcile_with() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    result = await _place(_deps(httpx.MockTransport(handler)))
    assert result["status"] == "UNKNOWN"
    assert result["idempotency_key"]
    assert "may or may not" in str(result["reason"])


@pytest.mark.anyio
async def test_cancel_order_returns_the_cancelled_order() -> None:
    cancelled = {**_ORDER, "status": "CANCELLED"}
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=cancelled))
    result = await tools.cancel_order(_deps(handler), 11)
    assert result["status"] == "CANCELLED"


@pytest.mark.anyio
async def test_cancel_order_surfaces_a_refusal_verbatim() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(400, json={"detail": "order 11 is already FILLED"})
    )
    result = await tools.cancel_order(_deps(handler), 11)
    assert result["status"] == "REFUSED"
    assert "already FILLED" in result["reason"]


# The two tests below back place_order/cancel_order with
# `trading.paper.models.Order`, which -- like `Portfolio` and `Position`
# (see tests/mcp/test_tools_portfolio.py) -- carries an explicit
# `field_serializer` that renders its Decimal money fields as JSON
# *floats*, not text. `_ORDER` above uses string fields throughout and so
# never exercises that boundary; a real gateway response does.


@pytest.mark.anyio
async def test_place_order_re_renders_float_money_fields_as_exact_text() -> None:
    float_order = {**_ORDER, "quantity": 10.5, "limit_price": 101.25, "filled_quantity": 0.0}
    handler = httpx.MockTransport(lambda r: httpx.Response(201, json=float_order))
    result = await _place(_deps(handler))
    assert result["quantity"] == "10.5"
    assert isinstance(result["quantity"], str)
    assert result["limit_price"] == "101.25"
    assert result["filled_quantity"] == "0.0"


@pytest.mark.anyio
async def test_cancel_order_re_renders_float_money_fields_as_exact_text() -> None:
    float_order = {**_ORDER, "status": "CANCELLED", "quantity": 10.0}
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=float_order))
    result = await tools.cancel_order(_deps(handler), 11)
    assert result["quantity"] == "10.0"
    assert isinstance(result["quantity"], str)
