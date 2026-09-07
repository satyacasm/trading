"""Streamable HTTP entrypoint. Scope comes from the bearer token.

Hosted as a launchd daemon by the human partner -- this is the delivery
path, not optional polish.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from mcp.server import MCPServer

from trading.config import get_settings
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, register

bearer_token: ContextVar[str | None] = ContextVar("mcp_bearer_token", default=None)

_SCHEME = "bearer "


class BearerTokenMiddleware:
    """Stash the request's bearer token where the tools can find it.

    Our own middleware rather than the SDK's `TokenVerifier`: that
    machinery is built for OAuth resource servers and brings
    protected-resource metadata with it, while the requirement here is a
    static token-to-portfolio map (`SessionStore`, already built by Task
    8). Five lines with nothing to misconfigure beats a framework whose
    defaults would have to be audited.

    The token is set fresh on every request (`ContextVar.set`, not
    mutated in place), so one caller's scope can never be read by the
    next -- a leak there is a request trading another agent's book.
    """

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            header = ""
            for key, value in scope.get("headers", []):
                if key.decode().lower() == "authorization":
                    header = value.decode()
                    break
            token = header[len(_SCHEME) :].strip() if header.lower().startswith(_SCHEME) else None
            bearer_token.set(token or None)
        await self._app(scope, receive, send)


def build_app() -> Any:
    """The ASGI app to serve, with tools registered and auth wrapped."""
    settings = get_settings()
    deps = ToolDeps(
        client=GatewayClient.open(settings.mcp_gateway_url),
        sessions=SessionStore.from_settings(settings),
        token_provider=bearer_token.get,
    )
    server = MCPServer("trading")
    register(server, deps)
    # Stateless so each tool call resolves its own request scope; a
    # long-lived session would outlive the ContextVar the token lives in.
    return BearerTokenMiddleware(server.streamable_http_app(stateless_http=True))


app = build_app()
