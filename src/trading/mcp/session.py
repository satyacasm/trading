"""Which portfolio an agent is allowed to trade.

The scope is never a tool parameter. An agent cannot name a book, so a
confused or misled one cannot trade the wrong book -- the only guardrail
between the agent and the order API, and the reason it has to be
airtight.
"""

from __future__ import annotations

from dataclasses import dataclass

from trading.config import Settings


class SessionRefused(Exception):
    """The caller has no valid scope. Never carries the token."""


@dataclass(frozen=True)
class AgentSession:
    """One authenticated agent, bound to one portfolio.

    `token` is `None` over stdio, where `get_access_token()` returns
    nothing because the subprocess is already inside the trust boundary.
    """

    token: str | None
    portfolio_id: int


class SessionStore:
    """Token to portfolio, with an optional stdio fallback."""

    def __init__(self, tokens: dict[str, int], stdio_portfolio_id: int | None = None) -> None:
        self._tokens = dict(tokens)
        self._stdio_portfolio_id = stdio_portfolio_id

    @classmethod
    def from_pairs(cls, pairs: str, stdio_portfolio_id: int | None) -> SessionStore:
        """Parse `"token:portfolio_id, token:portfolio_id"`.

        Malformed entries raise here rather than at first use: a typo
        would otherwise surface as an authentication failure in the
        middle of a trading session, which is the worst time to debug
        configuration.
        """
        tokens: dict[str, int] = {}
        for entry in (part.strip() for part in pairs.split(",")):
            if not entry:
                continue
            token, separator, portfolio = entry.partition(":")
            if not separator or not token.strip() or not portfolio.strip():
                raise ValueError(
                    "mcp_tokens entries must look like 'token:portfolio_id'; "
                    f"one entry has no ':' separator (position {len(tokens) + 1})"
                )
            try:
                tokens[token.strip()] = int(portfolio.strip())
            except ValueError:
                raise ValueError(
                    f"mcp_tokens portfolio ids must be integers; entry {len(tokens) + 1} is not"
                ) from None
        return cls(tokens, stdio_portfolio_id)

    @classmethod
    def from_settings(cls, settings: Settings) -> SessionStore:
        return cls.from_pairs(settings.mcp_tokens, settings.mcp_stdio_portfolio_id)

    def resolve(self, token: str | None) -> AgentSession:
        """The session for this caller, or `SessionRefused`.

        The refusal never repeats the token back: a rejected credential
        must not reach a log or an agent's transcript.
        """
        if token is None:
            if self._stdio_portfolio_id is None:
                raise SessionRefused("no bearer token, and no mcp_stdio_portfolio_id is configured")
            return AgentSession(token=None, portfolio_id=self._stdio_portfolio_id)
        portfolio_id = self._tokens.get(token)
        if portfolio_id is None:
            raise SessionRefused("the supplied token is not recognised")
        return AgentSession(token=token, portfolio_id=portfolio_id)
