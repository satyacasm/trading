"""`register()`'s job as the last line of defence against a stray
`GatewayRefusal`.

`get_strategy_contract`, `list_instruments` and `get_perp_positions`
(`trading/mcp/tools.py`) call the gateway with no try/except of their
own -- they already rely on `register()`'s wrapper to catch a refusal
they don't handle. These tests drive a real 4xx/5xx through
`httpx.MockTransport` so `GatewayClient._decode` raises the actual
`GatewayRefusal`/`GatewayUnavailable`, never a hand-thrown stand-in, and
run the call through the real installed `MCPServer` (mcp 2.1.1) rather
than a hand-rolled double -- this is the property the whole task exists
to protect, so it gets checked against the real SDK, not a mock of it.
"""

from __future__ import annotations

import httpx
import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import UnexpectedToolError

from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, register


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_a_gatewayrefusal_a_tool_forgot_to_catch_still_reaches_the_agent_as_a_refusal() -> (
    None
):
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no contract configured"})

    server = MCPServer("test")
    register(server, _deps(httpx.MockTransport(respond)))

    result = await server.call_tool("get_strategy_contract", {})

    assert result.is_error is False
    assert result.content is not None
    text = "".join(getattr(block, "text", "") for block in result.content)
    assert '"status": "REFUSED"' in text
    assert "no contract configured" in text


@pytest.mark.anyio
async def test_a_gatewayrefusal_from_a_tool_that_already_catches_its_own_is_unaffected() -> None:
    # place_order already wraps its own POST in try/except GatewayRefusal
    # (tools.py). This proves the wrapper is transparent when a tool
    # handles its own refusal -- it must not double-wrap or change the
    # shape a tool already produces correctly.
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "market is closed"})

    server = MCPServer("test")
    register(server, _deps(httpx.MockTransport(respond)))

    result = await server.call_tool(
        "place_order",
        {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": "1",
            "product": "DELIVERY",
            "rationale": "test",
        },
    )

    assert result.is_error is False
    text = "".join(getattr(block, "text", "") for block in result.content or [])
    assert '"status": "REFUSED"' in text
    assert "market is closed" in text


@pytest.mark.anyio
async def test_a_gatewayunavailable_crash_is_not_adapted_into_a_plausible_answer() -> None:
    # 500, not 4xx: GatewayClient._decode raises GatewayUnavailable for
    # this status, distinct from GatewayRefusal on purpose (client.py).
    # The wrapper must NOT catch it -- an agent adapting a strategy to a
    # 500 is a strategy fitted to a bug, not to the market.
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal error")

    server = MCPServer("test")
    register(server, _deps(httpx.MockTransport(respond)))

    with pytest.raises(UnexpectedToolError) as excinfo:
        await server.call_tool("get_strategy_contract", {})

    # The crash reaches the agent looking like a crash: no gateway detail
    # leaks into the generic message, only the traceback (server-side).
    assert "Error executing tool get_strategy_contract" in str(excinfo.value)
