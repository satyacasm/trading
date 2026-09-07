"""Every MCP tool, defined once and served over both transports.

Each tool is a module-level `async def` taking `ToolDeps` first, so the
tests exercise them without any MCP machinery and `register()` (Task 15)
stays a thin adapter.

Business refusals are returned as data (`formatting.refused`), not
raised: the gateway's refusals already say what would satisfy them, and
an agent can only act on wording it receives.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, cast

from trading.indicators import CATALOGUE, IndicatorRequest, compute, parse, warmup_for
from trading.mcp.client import GatewayClient, GatewayRefusal, GatewayUnavailable
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
    orders by `order_id DESC`).

    `GET /orders` 404s when `portfolio_id` does not exist (mirroring
    `get_positions`), and that is reachable here: an operator's
    `mcp_tokens` entry can point at a portfolio that has since been
    deleted, or was mistyped at configuration time. `get_portfolio_state`
    treats the same failure mode as a first-class refusal rather than a
    crash, and this tool follows the same convention this module states
    at the top of the file -- a business refusal is data, not a raised
    exception a caller has to catch.
    """
    session = _session(deps)
    # `portfolio_id` is required by the route; `limit` is capped at 500,
    # the route's own `le=500` -- sending more would be refused rather
    # than clamped, and the cap belongs here so a caller-supplied limit
    # never turns a read into a refusal.
    try:
        orders: list[dict[str, Any]] = await deps.client.get(
            "/orders", params={"portfolio_id": session.portfolio_id, "limit": min(limit, 500)}
        )
    except GatewayRefusal as refusal:
        return refused(refusal.detail, portfolio_id=session.portfolio_id)
    rows = [row for row in orders if row.get("portfolio_id") == session.portfolio_id]
    if status is not None:
        wanted = status.upper()
        rows = [row for row in rows if row.get("status") == wanted]
    rows = rows[:limit]
    return {
        "count": len(rows),
        "orders": [_with_money_fields(row, _ORDER_MONEY_FIELDS) for row in rows],
    }


def _derive_idempotency_key(
    portfolio_id: int,
    instrument_id: int,
    side: str,
    order_type: str,
    quantity: str,
    limit_price: str | None,
    now: datetime,
) -> str:
    """A key that is stable across retries of the same decision.

    Bucketed to the minute: an agent that retries a decision seconds
    later means the same order, and two orders would be the wrong answer.
    An agent that genuinely wants to buy twice in one minute passes its
    own key -- which is why the parameter stays.
    """
    material = "|".join(
        [
            str(portfolio_id),
            str(instrument_id),
            side,
            order_type,
            quantity,
            limit_price or "",
            now.strftime("%Y-%m-%dT%H:%M"),
        ]
    )
    return hashlib.sha256(material.encode()).hexdigest()


async def place_order(
    deps: ToolDeps,
    instrument_id: int,
    side: str,
    order_type: str,
    quantity: str,
    product: str,
    rationale: str,
    limit_price: str | None = None,
    time_in_force: str = "DAY",
    leverage: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Place one order in the session's portfolio.

    There is no `portfolio_id` parameter. The book comes from the session,
    so a confused or misled agent cannot trade the wrong one.

    `rationale` is required by the API and stored with the order. It is
    the only record of why an autonomous decision was taken, so it is
    passed through rather than defaulted.

    A timed-out POST is retried exactly once, with the *same*
    idempotency_key -- never re-derived, since a fresh derivation would
    stamp a different minute-bucket onto a retry that crossed a minute
    boundary and defeat the whole point of the key. `POST /orders` holds
    a unique constraint on idempotency_key and `_insert_order`
    (`trading/paper/api.py`) recovers a `UniqueViolation` by returning
    the order that won the race rather than raising, so a same-key retry
    converges on exactly one order whether or not the first attempt
    reached the database. Two failures in a row is reported as `UNKNOWN`
    rather than guessed at either way: claiming FILLED or REFUSED here
    would be inventing an outcome for real money.
    """
    session = _session(deps)
    key = idempotency_key or _derive_idempotency_key(
        session.portfolio_id, instrument_id, side, order_type, quantity, limit_price, _utcnow()
    )
    body: dict[str, Any] = {
        "portfolio_id": session.portfolio_id,
        "instrument_id": instrument_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "product": product,
        "time_in_force": time_in_force,
        "rationale": rationale,
        "idempotency_key": key,
    }
    if limit_price is not None:
        body["limit_price"] = limit_price
    if leverage is not None:
        body["leverage"] = leverage

    for attempt in (1, 2):
        try:
            order: dict[str, Any] = await deps.client.post("/orders", body)
        except GatewayRefusal as refusal:
            # A rejected order (insufficient cash, market closed, a
            # contract-filter violation) is business data, not a crash --
            # the gateway's wording already says what would satisfy it.
            return refused(refusal.detail, idempotency_key=key)
        except GatewayUnavailable as unavailable:
            # Safe to repeat: idempotency_key carries a unique constraint
            # and `_insert_order` returns the winner of a race rather than
            # creating a second order. Exactly one order exists whether or
            # not the first attempt reached the database.
            if attempt == 2:
                return {
                    "status": "UNKNOWN",
                    "reason": (
                        f"the gateway did not answer, so this order may or may not have been "
                        f"placed: {unavailable}. Call list_orders before retrying; re-sending "
                        f"with the same idempotency_key will not create a second order."
                    ),
                    "idempotency_key": key,
                }
            continue
        # `Order` (trading.paper.models) serialises its Decimal money
        # fields as JSON floats, exactly like Portfolio and Position --
        # see the module-level comment above _PORTFOLIO_MONEY_FIELDS.
        return _with_money_fields(order, _ORDER_MONEY_FIELDS)
    raise AssertionError("unreachable")


async def cancel_order(deps: ToolDeps, order_id: int) -> dict[str, Any]:
    """Cancel a working order. Refusals -- already terminal, unknown id --
    carry the gateway's wording verbatim.

    `DELETE /orders/{order_id}` (`trading/paper/api.py`) selects and
    cancels by `order_id` alone -- unlike `GET /orders` just below it in
    that file, it takes no `portfolio_id` and enforces no ownership at
    all. So before issuing the DELETE, this confirms `order_id` is among
    the session's own orders, reusing the same `GET /orders?portfolio_id=`
    call `list_orders` already makes. This is *not* a second enforcement
    path duplicating something the gateway already checks -- the gateway
    checks nothing here, so this is the only check that exists. If the
    route is ever fixed to scope by portfolio itself, this guard becomes
    redundant and should be revisited then, not deleted now as
    duplication before that happens.

    The refusal for an order outside the session's portfolio never says
    whether `order_id` exists under some other book -- and structurally
    cannot: `GET /orders` is itself filtered server-side to the caller's
    own `portfolio_id`, so this tool never learns anything about anyone
    else's orders to leak in the first place.

    Never retried: `DELETE /orders/{id}` carries no idempotency key, so a
    second attempt after a timeout cannot be told apart from a second
    genuine cancel, and `GatewayClient.delete` already does not retry for
    exactly this reason. A `GatewayUnavailable` from either call
    propagates uncaught -- a crash here must not be adapted into a
    plausible-looking REFUSED or CANCELLED.
    """
    session = _session(deps)
    try:
        mine_orders: list[dict[str, Any]] = await deps.client.get(
            "/orders", params={"portfolio_id": session.portfolio_id, "limit": 500}
        )
    except GatewayRefusal as refusal:
        return refused(refusal.detail, order_id=order_id)
    owned = any(
        row.get("order_id") == order_id and row.get("portfolio_id") == session.portfolio_id
        for row in mine_orders
    )
    if not owned:
        return refused(
            f"no order with order_id={order_id} in this session's portfolio", order_id=order_id
        )

    try:
        order: dict[str, Any] = await deps.client.delete(f"/orders/{order_id}")
    except GatewayRefusal as refusal:
        return refused(refusal.detail, order_id=order_id)
    return _with_money_fields(order, _ORDER_MONEY_FIELDS)


def _strategy_version(now: datetime) -> str:
    """A version distinct on every call, to the microsecond.

    `(name, version)` is immutable once registered (`register_strategy`'s
    docstring, `trading/agent_contract/registry.py`): resubmitting the
    same name with edited code under a version already on file raises
    `VersionConflict`, which `POST /strategies` never catches, so it would
    surface here as an unhandled 500 -- `GatewayUnavailable`, not a
    refusal an agent could read and act on. There is no `version`
    parameter on this tool, and an agent iterating on one strategy
    resubmits the same `name` after every fix, so the version is derived
    from the clock rather than asked of the caller: every attempt lands
    as its own row without anyone having to name one.
    """
    return now.strftime("%Y%m%dT%H%M%S%f")


async def submit_strategy(deps: ToolDeps, name: str, python_code: str) -> dict[str, Any]:
    """Register a strategy: validated statically, then smoke-run sandboxed.

    Gated on `_session` even though a strategy is not portfolio-scoped --
    `UploadStrategyRequest` carries no `portfolio_id` at all -- because
    `POST /strategies` (`trading/agent_contract/api.py`) spawns up to
    three Docker containers per call, and only a recognised token should
    be able to trigger that.

    The brief's sample body was `{"name": ..., "code": ...}`; the real
    route is `UploadStrategyRequest{name, version, source, user_id}`, so
    the body sent here is `name`/`version`/`source` instead -- see
    `_strategy_version` for where `version` comes from.

    A rejection comes back as findings rather than an error, because the
    findings are the useful part -- they say what to change. An agent
    iterating on a strategy reads them and resubmits. `UploadStrategyResponse`
    (and the `RunSummary` nested inside it) already render every field as
    text via `str(...)`/`_as_str`, never a float `field_serializer` the
    way `Portfolio`/`Position`/`Order` do, so nothing here needs `money()`.
    """
    _session(deps)
    body: dict[str, Any] = {
        "name": name,
        "version": _strategy_version(_utcnow()),
        "source": python_code,
    }
    try:
        return dict(await deps.client.post("/strategies", body))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, name=name)


async def run_backtest(
    deps: ToolDeps,
    strategy_id: int,
    start: str,
    end: str,
    starting_cash: str | None = None,
    max_daily_loss: str | None = None,
    max_drawdown_pct: str | None = None,
) -> dict[str, Any]:
    """Replay a registered strategy over a window and return its metrics.

    Blocks for the run, matching the route: `POST
    /strategies/{id}/backtests` is a plain `def` (psycopg is synchronous,
    and it shells out to Docker) that runs the replay to completion and
    returns the finished result. Its own module docstring puts the honest
    expectation at seconds for a daily decade and explicitly declines to
    build a job queue for a wait that does not exist, so there is nothing
    here to poll -- this coroutine just waits for the one response the
    route was built to give.

    Overrides that were not given are omitted from the body rather than
    sent as null: the route reads a missing field as "use the strategy's
    own", and an explicit null would override the manifest with nothing.

    `BacktestResponse` renders every money field and the equity curve as
    text (`_as_str`/`str(...)` throughout `trading/agent_contract/api.py`),
    so nothing here needs `money()` either.
    """
    _session(deps)
    body: dict[str, Any] = {"start": start, "end": end}
    if starting_cash is not None:
        body["starting_cash"] = starting_cash
    if max_daily_loss is not None:
        body["max_daily_loss"] = max_daily_loss
    if max_drawdown_pct is not None:
        body["max_drawdown_pct"] = max_drawdown_pct
    try:
        return dict(await deps.client.post(f"/strategies/{strategy_id}/backtests", body))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, strategy_id=strategy_id)


async def get_backtest(deps: ToolDeps, backtest_run_id: int) -> dict[str, Any]:
    """Re-read a stored run: its curve, itemised fills, and computed metrics.

    `GET /backtests/{id}` returns `BacktestDetail`
    (`trading/agent_contract/api.py`) whole, with no pagination of its
    own -- the platform's own answer to an unbounded payload is the
    *other* read route, `GET /strategies/{id}/backtests`
    (`BacktestSummary`, no curve, no fills, not this task's tool), not
    truncation of the detail view. So this tool is a straight pass
    through of the one route that carries the curve, rather than a second,
    divergent limit invented here. Money and the curve are text
    end-to-end on this route too, the same as `run_backtest`.
    """
    _session(deps)
    try:
        return dict(await deps.client.get(f"/backtests/{backtest_run_id}"))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, backtest_run_id=backtest_run_id)


async def _call(call: Awaitable[dict[str, Any]]) -> dict[str, Any]:
    """The last line of defence against a `GatewayRefusal` a tool forgot to catch.

    Every tool above is supposed to catch its own `GatewayRefusal` and
    return `formatting.refused(...)` -- that is the module convention
    stated at the top of this file, and most of the fifteen do. A few
    (`get_strategy_contract`, `list_instruments`, `get_perp_positions`)
    call the gateway with no try/except at all, relying on this wrapper;
    Task 12 shipped one that forgot with nothing to catch it, and only a
    review caught it before it shipped. `register()` (below) routes every
    tool call through here so a sixteenth tool that forgets is caught the
    same way, without every call site having to remember to guard itself.

    `GatewayRefusal` becomes the *same* shape a tool's own catch produces
    -- `refused(refusal.detail)` -- not the SDK's `ToolError`. Keeping the
    shape uniform matters more than which layer caught it: an agent that
    pattern-matches on `status == "REFUSED"` sees identical data whether
    the tool or this wrapper did the catching.

    `GatewayUnavailable` is deliberately left alone. `client.py`'s own
    docstring is the rule this follows: "an agent should adapt to a
    refusal and must not adapt to a crash." Catching it here and handing
    back a dict would do exactly the thing that docstring forbids --
    manufacture a plausible-looking answer for a platform that could not
    be reached. Left uncaught, it propagates out of the tool coroutine and
    the SDK wraps it as `UnexpectedToolError`: `is_error=True`, the
    message withheld to `Error executing tool <name>`, and the traceback
    logged at ERROR. A crash reaches the agent looking like a crash.
    """
    try:
        return await call
    except GatewayRefusal as refusal:
        return refused(refusal.detail)


class _ToolServer(Protocol):
    """The one method `register` needs from an MCP server.

    `register`'s own parameter stays typed `Any` -- that is the plan's
    interface, and a fresh `MCPServer()` per transport is the only thing
    ever passed in practice. The cast to this `Protocol` inside the
    function body exists solely so `@typed.tool(...)` is a *typed*
    decorator under mypy strict's `disallow_untyped_decorators`; decorating
    with a method resolved from `Any` (`@server.tool(...)` directly) makes
    every decorated function's type `Any` too, which would erase every
    tool's `dict[str, Any]` return annotation right where it matters most.
    """

    def tool(self, *, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def register(server: Any, deps: ToolDeps) -> None:
    """Expose every tool on an MCP server.

    A thin adapter on purpose: the tools above are plain functions, so
    both transports register the same objects and the tests never touch
    MCP machinery. The docstrings become the tool descriptions an agent
    reads, which is why they say what a tool refuses and why. Every call
    is routed through `_call` -- see its docstring for what that buys and
    why `GatewayUnavailable` is excluded from it on purpose.

    Names are passed explicitly (`@typed.tool(name=...)`): the installed
    SDK (`mcp` 2.1.1, `MCPServer.tool`) takes one, so the agent sees the
    clean name (`place_order`) rather than the trailing-underscore name
    (`place_order_`) that dodges shadowing the module-level function of
    the same name inside this function's body.
    """
    typed = cast(_ToolServer, server)

    @typed.tool(name="get_capabilities")
    async def get_capabilities_() -> dict[str, Any]:
        """What this platform can trade, and the vocabulary it accepts."""
        return await _call(get_capabilities(deps))

    @typed.tool(name="list_instruments")
    async def list_instruments_(
        asset_class: str | None = None, query: str | None = None
    ) -> dict[str, Any]:
        """Instruments, optionally filtered, each flagged tradeable or not."""
        return await _call(list_instruments(deps, asset_class, query))

    @typed.tool(name="get_strategy_contract")
    async def get_strategy_contract_() -> dict[str, Any]:
        """The contract a strategy script must satisfy to be accepted."""
        return await _call(get_strategy_contract(deps))

    @typed.tool(name="get_market_snapshot")
    async def get_market_snapshot_(
        instrument_ids: list[int],
        interval: str = "1d",
        indicators: list[str] | None = None,
        history: int = 50,
    ) -> dict[str, Any]:
        """Prices, bars and computed indicators, with a freshness verdict."""
        return await _call(get_market_snapshot(deps, instrument_ids, interval, indicators, history))

    @typed.tool(name="get_candles")
    async def get_candles_(
        instrument_id: int, interval: str = "1d", limit: int = 300
    ) -> dict[str, Any]:
        """Raw OHLCV as decimal strings, oldest first."""
        return await _call(get_candles(deps, instrument_id, interval, limit))

    @typed.tool(name="get_data_freshness")
    async def get_data_freshness_(
        instrument_ids: list[int], interval: str = "1d"
    ) -> dict[str, Any]:
        """How current each series is. Ask before trusting a backtest."""
        return await _call(get_data_freshness(deps, instrument_ids, interval))

    @typed.tool(name="get_perp_context")
    async def get_perp_context_(instrument_id: int) -> dict[str, Any]:
        """Step size, minimum notional, leverage tiers and latest funding."""
        return await _call(get_perp_context(deps, instrument_id))

    @typed.tool(name="get_portfolio_state")
    async def get_portfolio_state_() -> dict[str, Any]:
        """Cash, positions and working orders for this session's portfolio."""
        return await _call(get_portfolio_state(deps))

    @typed.tool(name="get_perp_positions")
    async def get_perp_positions_() -> dict[str, Any]:
        """Open perpetual positions with margin and liquidation price."""
        return await _call(get_perp_positions(deps))

    @typed.tool(name="list_orders")
    async def list_orders_(status: str | None = None, limit: int = 50) -> dict[str, Any]:
        """This session portfolio's order blotter."""
        return await _call(list_orders(deps, status, limit))

    @typed.tool(name="place_order")
    async def place_order_(
        instrument_id: int,
        side: str,
        order_type: str,
        quantity: str,
        product: str,
        rationale: str,
        limit_price: str | None = None,
        time_in_force: str = "DAY",
        leverage: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Place an order in this session's portfolio.

        Quantities and prices are decimal STRINGS, never numbers.
        `rationale` is required and stored with the order. `leverage` is
        required for a perpetual and meaningless otherwise. A refusal
        comes back as status REFUSED with the platform's own reason;
        retrying it unchanged will be refused again.
        """
        return await _call(
            place_order(
                deps,
                instrument_id,
                side,
                order_type,
                quantity,
                product,
                rationale,
                limit_price,
                time_in_force,
                leverage,
                idempotency_key,
            )
        )

    @typed.tool(name="cancel_order")
    async def cancel_order_(order_id: int) -> dict[str, Any]:
        """Cancel a working order."""
        return await _call(cancel_order(deps, order_id))

    @typed.tool(name="submit_strategy")
    async def submit_strategy_(name: str, python_code: str) -> dict[str, Any]:
        """Register a strategy: statically validated, then smoke-run."""
        return await _call(submit_strategy(deps, name, python_code))

    @typed.tool(name="run_backtest")
    async def run_backtest_(
        strategy_id: int,
        start: str,
        end: str,
        starting_cash: str | None = None,
        max_daily_loss: str | None = None,
        max_drawdown_pct: str | None = None,
    ) -> dict[str, Any]:
        """Replay a strategy over a window. Dates are YYYY-MM-DD."""
        return await _call(
            run_backtest(
                deps, strategy_id, start, end, starting_cash, max_daily_loss, max_drawdown_pct
            )
        )

    @typed.tool(name="get_backtest")
    async def get_backtest_(backtest_run_id: int) -> dict[str, Any]:
        """Re-read a stored backtest run with its curve and fills."""
        return await _call(get_backtest(deps, backtest_run_id))
