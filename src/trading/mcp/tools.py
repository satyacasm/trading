"""Every MCP tool, defined once and served over both transports.

Each tool is a module-level `async def` taking `ToolDeps` first, so the
tests exercise them without any MCP machinery and `register()` (Task 15)
stays a thin adapter.

Business refusals are returned as data (`formatting.refused`), not
raised: the gateway's refusals already say what would satisfy them, and
an agent can only act on wording it receives.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from trading.indicators import CATALOGUE
from trading.mcp.client import GatewayClient
from trading.mcp.session import AgentSession, SessionStore
from trading.paper.charges import BROKER_BY_ASSET_CLASS
from trading.paper.enums import OrderType, Product, Side, TimeInForce


@dataclass(frozen=True)
class ToolDeps:
    """What every tool needs. `token_provider` is the only thing that
    differs between transports: over HTTP it reads the bearer token, over
    stdio it returns `None` and the session falls back to config."""

    client: GatewayClient
    sessions: SessionStore
    token_provider: Callable[[], str | None]


def _session(deps: ToolDeps) -> AgentSession:
    return deps.sessions.resolve(deps.token_provider())


async def get_capabilities(deps: ToolDeps) -> dict[str, Any]:
    """What this platform can actually do, so an agent need not be told.

    `tradeable_asset_classes` comes from `BROKER_BY_ASSET_CLASS` rather
    than from `AssetClass`: the enum lists eight, but an order in one
    without a charge schedule is refused by `MissingChargeSchedule`.
    Advertising the enum would promise five markets that do not exist.
    """
    return {
        "tradeable_asset_classes": sorted(BROKER_BY_ASSET_CLASS),
        "brokers": dict(BROKER_BY_ASSET_CLASS),
        "sides": [s.value for s in Side],
        "order_types": [o.value for o in OrderType],
        "products": [p.value for p in Product],
        "time_in_force": [t.value for t in TimeInForce],
        # margin_modes stays hardcoded: its source is Literal["ISOLATED","CROSS"]
        # on a FastAPI request model (src/trading/paper/api.py:126). Importing
        # trading.paper.api would pull psycopg in, breaching the hard constraint
        # that no psycopg import exists anywhere under src/trading/mcp/.
        "margin_modes": ["ISOLATED", "CROSS"],
        # bar_intervals stays hardcoded: its source is a Literal type alias in
        # the agent_contract package; deriving it needs typing.get_args across a
        # package boundary and costs more than it buys.
        "bar_intervals": ["1m", "5m", "15m", "1h", "1d"],
        "indicators": dict(CATALOGUE),
        "notes": [
            "leverage is required for a PERP order and meaningless otherwise",
            "rationale is required on every order and is stored with it",
            "an order's portfolio comes from the session, never from a parameter",
        ],
    }


async def list_instruments(
    deps: ToolDeps, asset_class: str | None = None, query: str | None = None
) -> dict[str, Any]:
    """Instruments, optionally filtered, each flagged tradeable or not."""
    rows: list[dict[str, Any]] = await deps.client.get("/instruments")
    if asset_class is not None:
        wanted = asset_class.upper()
        rows = [row for row in rows if row.get("asset_class") == wanted]
    if query is not None:
        needle = query.strip().lower()
        rows = [row for row in rows if needle in str(row.get("symbol", "")).lower()]
    instruments = [
        {**row, "tradeable": row.get("asset_class") in BROKER_BY_ASSET_CLASS} for row in rows
    ]
    return {"count": len(instruments), "instruments": instruments}


async def get_strategy_contract(deps: ToolDeps) -> dict[str, Any]:
    """The contract a strategy script must satisfy, served verbatim.

    Passed through rather than summarised: the validator checks against
    this document, and a paraphrase here would send an agent to write
    against rules that are not the ones enforced.
    """
    bundle: dict[str, Any] = await deps.client.get("/strategies/contract")
    return bundle
