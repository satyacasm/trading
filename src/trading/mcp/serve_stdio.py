"""stdio entrypoint. Scope comes from configuration.

There is no token over stdio: the subprocess is spawned by the agent's
own harness and is already inside the trust boundary. `SessionStore`
refuses to resolve at all unless `mcp_stdio_portfolio_id` is set, so a
misconfigured server fails on the first tool call rather than guessing a
book.
"""

from __future__ import annotations

from mcp.server import MCPServer

from trading.config import get_settings
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, register


def build() -> MCPServer:
    settings = get_settings()
    deps = ToolDeps(
        client=GatewayClient.open(settings.mcp_gateway_url),
        sessions=SessionStore.from_settings(settings),
        token_provider=lambda: None,
    )
    server = MCPServer("trading")
    register(server, deps)
    return server


def main() -> None:
    # `run()` defaults to transport="stdio" on the installed SDK (mcp
    # 2.1.1) -- there is no constructor-level transport argument to set,
    # unlike the older FastMCP API the plan warned might be stale.
    build().run()


if __name__ == "__main__":
    main()
