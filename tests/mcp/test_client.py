from __future__ import annotations

import httpx
import pytest

from trading.mcp.client import GatewayClient, GatewayRefusal, GatewayUnavailable


def _client(handler: httpx.MockTransport) -> GatewayClient:
    return GatewayClient("http://gateway", httpx.AsyncClient(transport=handler))


@pytest.mark.anyio
async def test_get_returns_the_decoded_body() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
    assert await _client(transport).get("/instruments") == {"ok": True}


@pytest.mark.anyio
async def test_a_four_hundred_becomes_a_refusal_carrying_the_detail() -> None:
    detail = "insufficient cash: order needs 500, portfolio has 100"
    transport = httpx.MockTransport(lambda request: httpx.Response(400, json={"detail": detail}))
    with pytest.raises(GatewayRefusal) as excinfo:
        await _client(transport).post("/orders", {"quantity": "1"})
    assert excinfo.value.detail == detail
    assert excinfo.value.status_code == 400


@pytest.mark.anyio
async def test_a_five_hundred_is_unavailable_not_a_refusal() -> None:
    # A refusal is the platform saying no; a 500 is the platform broken.
    # An agent must not adapt its strategy to a crash.
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="boom"))
    with pytest.raises(GatewayUnavailable):
        await _client(transport).get("/instruments")


@pytest.mark.anyio
async def test_a_timeout_is_also_unavailable_not_a_refusal() -> None:
    # httpx.TimeoutException is a distinct exception from ConnectError, but
    # both are transport failures, and both must land as GatewayUnavailable.
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    with pytest.raises(GatewayUnavailable):
        await _client(httpx.MockTransport(handler)).get("/instruments")


@pytest.mark.anyio
async def test_a_read_is_retried_once_on_a_transport_error() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json=[])

    assert await _client(httpx.MockTransport(handler)).get("/instruments") == []
    assert attempts == 2


@pytest.mark.anyio
async def test_a_write_is_never_retried() -> None:
    # A timed-out order may or may not exist. Re-POSTing is how one
    # decision becomes two positions.
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(GatewayUnavailable):
        await _client(httpx.MockTransport(handler)).post("/orders", {})
    assert attempts == 1
