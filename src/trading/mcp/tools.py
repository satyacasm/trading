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
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from trading.indicators import CATALOGUE, IndicatorRequest, compute, parse, warmup_for
from trading.mcp.client import GatewayClient, GatewayRefusal
from trading.mcp.formatting import freshness, money, refused
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


def _utcnow() -> datetime:
    """Indirection so tests can pin the clock without patching `datetime`."""
    return datetime.now(UTC)


def _last_ts(candles: list[dict[str, Any]]) -> datetime | None:
    if not candles:
        return None
    return datetime.fromisoformat(str(candles[-1]["ts"]))


async def _fetch_candles(
    deps: ToolDeps, instrument_id: int, interval: str, limit: int
) -> list[dict[str, Any]]:
    """Bars as decimal text, oldest first. Raises `GatewayRefusal`."""
    body = await deps.client.get(
        f"/candles/{instrument_id}",
        params={"interval": interval, "limit": limit, "precision": "string"},
    )
    return list(body.get("candles", []))


async def get_candles(
    deps: ToolDeps, instrument_id: int, interval: str = "1d", limit: int = 300
) -> dict[str, Any]:
    """Raw OHLCV for an agent's own analysis, with a freshness verdict."""
    try:
        candles = await _fetch_candles(deps, instrument_id, interval, limit)
    except GatewayRefusal as refusal:
        return refused(refusal.detail, instrument_id=instrument_id)
    return {
        "instrument_id": instrument_id,
        "interval": interval,
        "count": len(candles),
        "candles": candles,
        "freshness": freshness(_last_ts(candles), interval, _utcnow()),
    }


async def get_data_freshness(
    deps: ToolDeps, instrument_ids: list[int], interval: str = "1d"
) -> dict[str, Any]:
    """How current each series is.

    Its own tool rather than only a field on a snapshot, because the
    question "is this database current?" is one an agent should be able
    to ask before it reasons, not only after. The bhavcopy feed has gone
    weeks without a write before now, and a backtest run against it looks
    exactly like one run against fresh data.
    """
    rows: list[dict[str, Any]] = []
    for instrument_id in instrument_ids:
        try:
            candles = await _fetch_candles(deps, instrument_id, interval, 1)
        except GatewayRefusal as refusal:
            rows.append({"instrument_id": instrument_id, "stale": True, "warning": refusal.detail})
            continue
        rows.append(
            {
                "instrument_id": instrument_id,
                **freshness(_last_ts(candles), interval, _utcnow()),
            }
        )
    return {
        "interval": interval,
        "any_stale": any(row["stale"] for row in rows),
        "instruments": rows,
    }


async def get_perp_context(deps: ToolDeps, instrument_id: int) -> dict[str, Any]:
    """Contract filters and the latest funding print for one perpetual.

    Sizing a perpetual without these is guesswork: DOGE steps by a whole
    coin, BTC by 0.001, and the order path refuses anything off-step.
    """
    try:
        return dict(await deps.client.get(f"/perp-context/{instrument_id}"))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, instrument_id=instrument_id)


def _column(candles: list[dict[str, Any]], field: str) -> list[Decimal]:
    return [Decimal(str(candle[field])) for candle in candles]


def _rendered(value: Decimal | dict[str, Decimal] | None) -> Any:
    """Indicator output as text, preserving the shape.

    `None` survives as `None` rather than becoming a number: an indicator
    that could not be computed must not be indistinguishable from one
    that computed to zero.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: money(inner) for key, inner in value.items()}
    return money(value)


async def _snapshot_one(
    deps: ToolDeps,
    instrument_id: int,
    interval: str,
    requests: list[IndicatorRequest],
    history: int,
    warmup: int,
) -> dict[str, Any]:
    try:
        candles = await _fetch_candles(deps, instrument_id, interval, history + warmup)
    except GatewayRefusal as refusal:
        # One bad instrument must not lose the others: an agent watching
        # six symbols should still see five when the sixth is unknown.
        return refused(refusal.detail, instrument_id=instrument_id)

    highs = _column(candles, "high")
    lows = _column(candles, "low")
    closes = _column(candles, "close")
    computed = {
        request.token: _rendered(compute(request, highs=highs, lows=lows, closes=closes))
        for request in requests
    }
    return {
        "instrument_id": instrument_id,
        "interval": interval,
        "last_price": money(closes[-1]) if closes else None,
        "bars": candles[-history:] if history else [],
        "indicators": computed,
        "warmup_bars_used": warmup,
        # Whether the database could supply the run-up the indicators
        # needed. An RSI computed from 15 bars is not the RSI computed
        # from 100, and a caller told nothing would never know which it
        # holds.
        "warmup_sufficient": len(candles) >= history + warmup,
        "freshness": freshness(_last_ts(candles), interval, _utcnow()),
    }


async def get_market_snapshot(
    deps: ToolDeps,
    instrument_ids: list[int],
    interval: str = "1d",
    indicators: list[str] | None = None,
    history: int = 50,
) -> dict[str, Any]:
    """Current state of several instruments, with indicators computed here.

    Indicators are computed server-side from decimal bars rather than
    handed over as raw OHLCV for the caller to reduce: a language model
    doing Wilder smoothing over 200 rows in its head produces a number
    that looks right and is not, and nothing downstream would catch it.
    """
    try:
        requests = [parse(token) for token in (indicators or [])]
    except ValueError as error:
        # The whole call is refused, not the one token. A snapshot missing
        # what the agent asked for, but shaped as though complete, is the
        # worse failure.
        return refused(str(error))

    warmup = warmup_for(requests)
    rows = [
        await _snapshot_one(deps, instrument_id, interval, requests, history, warmup)
        for instrument_id in instrument_ids
    ]
    return {
        "interval": interval,
        "requested_indicators": [request.token for request in requests],
        "history": history,
        "instruments": rows,
    }


_OPEN_ORDER_STATUSES = frozenset({"PENDING", "OPEN", "PARTIALLY_FILLED"})

# `Portfolio`, `Position` and `Order` (src/trading/paper/models.py) each
# carry an explicit `field_serializer` that renders their Decimal money
# fields as a JSON *number*, not text -- unlike every other model on this
# platform, and unlike `PerpPositionOut`, which builds every field with
# `str(...)`. By the time httpx has decoded that response the float has
# already lost precision no downstream `Decimal(str(...))` recovers, so
# these rows are re-rendered through `money()` before they cross this
# boundary. A field absent from a row (e.g. a test fixture that only sets
# some keys) is left alone rather than injected.
_PORTFOLIO_MONEY_FIELDS = ("initial_capital", "cash_balance", "max_daily_loss", "max_drawdown_pct")
_POSITION_MONEY_FIELDS = ("quantity", "avg_cost", "realised_pnl")
_ORDER_MONEY_FIELDS = ("quantity", "filled_quantity", "limit_price", "leverage")


def _with_money_fields(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """`row`, with each of `fields` re-rendered as exact text where present."""
    rendered = dict(row)
    for field in fields:
        if field in rendered:
            rendered[field] = money(rendered[field])
    return rendered


async def get_portfolio_state(deps: ToolDeps) -> dict[str, Any]:
    """Cash, positions and working orders for the session's portfolio.

    The portfolio is filtered here from the session, never passed as a
    parameter. `GET /portfolios` returns every book the operator owns,
    and an agent that could name one could name the wrong one.
    """
    session = _session(deps)
    portfolios: list[dict[str, Any]] = await deps.client.get("/portfolios")
    mine = next(
        (row for row in portfolios if row.get("portfolio_id") == session.portfolio_id), None
    )
    if mine is None:
        return refused(
            f"the session's portfolio_id={session.portfolio_id} does not exist; "
            f"check the mcp_tokens configuration"
        )
    positions: list[dict[str, Any]] = await deps.client.get(
        f"/portfolios/{session.portfolio_id}/positions"
    )
    # `portfolio_id` is a REQUIRED query parameter on this route, not an
    # optional filter: omitting it is a 422, and an unknown id is a 404
    # rather than an empty list. The scoping therefore happens server-side;
    # the comprehension below is belt-and-braces against a future change.
    orders: list[dict[str, Any]] = await deps.client.get(
        "/orders", params={"portfolio_id": session.portfolio_id, "limit": 500}
    )
    mine_orders = [row for row in orders if row.get("portfolio_id") == session.portfolio_id]
    return {
        "portfolio": _with_money_fields(mine, _PORTFOLIO_MONEY_FIELDS),
        "positions": [_with_money_fields(row, _POSITION_MONEY_FIELDS) for row in positions],
        "open_orders": [
            _with_money_fields(row, _ORDER_MONEY_FIELDS)
            for row in mine_orders
            if row.get("status") in _OPEN_ORDER_STATUSES
        ],
    }


async def get_perp_positions(deps: ToolDeps) -> dict[str, Any]:
    """Open perpetual positions with their margin and liquidation price.

    Unlike `Portfolio`/`Position`/`Order`, `PerpPositionOut` builds every
    field with `str(...)` rather than a float `field_serializer` -- it is
    already exact text on arrival, so nothing here needs re-rendering.
    """
    session = _session(deps)
    positions = await deps.client.get(f"/portfolios/{session.portfolio_id}/perp-positions")
    return {"portfolio_id": session.portfolio_id, "positions": positions}


async def list_orders(deps: ToolDeps, status: str | None = None, limit: int = 50) -> dict[str, Any]:
    """The session portfolio's order blotter, newest first (`GET /orders`
    orders by `order_id DESC`)."""
    session = _session(deps)
    # `portfolio_id` is required by the route; `limit` is capped at 500,
    # the route's own `le=500` -- sending more would be refused rather
    # than clamped, and the cap belongs here so a caller-supplied limit
    # never turns a read into a refusal.
    orders: list[dict[str, Any]] = await deps.client.get(
        "/orders", params={"portfolio_id": session.portfolio_id, "limit": min(limit, 500)}
    )
    rows = [row for row in orders if row.get("portfolio_id") == session.portfolio_id]
    if status is not None:
        wanted = status.upper()
        rows = [row for row in rows if row.get("status") == wanted]
    rows = rows[:limit]
    return {
        "count": len(rows),
        "orders": [_with_money_fields(row, _ORDER_MONEY_FIELDS) for row in rows],
    }
