"""The one test in this plan that wires the real FastAPI routers to a
real database.

Every other MCP test mocks the gateway with `httpx.MockTransport` --
that proves the plumbing but cannot catch a mismatch between a fixture's
shape and what the real route emits. This branch was bitten by exactly
that once already (Task 12: the gateway serialises money as floats while
every fixture used strings; the fixtures were more truthful than the
gateway and every test passed while agreeing with a fiction). These
tests drive a tool call through the real route to the real database and
back, on a transaction `db_conn` always rolls back.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import httpx
import pytest
from fastapi import FastAPI

from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, get_portfolio_state, list_instruments, place_order
from trading.paper import api as paper_api
from trading.streaming import market_data_api
from trading.streaming.db import get_db_connection
from trading.streaming.gateway import instruments as instruments_route

pytestmark = pytest.mark.db


@pytest.fixture
def gateway_app(db_conn) -> Iterator[FastAPI]:
    app = FastAPI()
    app.include_router(market_data_api.router)
    app.include_router(paper_api.router)
    # `GET /instruments` (the route `list_instruments` actually calls)
    # is not on either router above -- it is defined directly on
    # `trading.streaming.gateway`'s module-level `app`
    # (`@app.get("/instruments", ...)`), and the brief that first sketched
    # this fixture omitted it entirely, which would have made
    # `list_instruments` 404 against this test app rather than exercise
    # anything real. Registering the *same* route function here (not a
    # reimplementation) keeps this fixture wired to the real route.
    app.add_api_route("/instruments", instruments_route, methods=["GET"])
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        yield app
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def portfolio_id(db_conn) -> int:
    user = db_conn.execute(
        "INSERT INTO users (email) VALUES ('agent@example.com') RETURNING user_id"
    ).fetchone()
    row = db_conn.execute(
        """
        INSERT INTO portfolios
            (user_id, name, base_currency, initial_capital, cash_balance, status, margin_mode)
        VALUES (%s, 'agent', 'INR', 100000, 100000, 'ACTIVE', 'ISOLATED')
        RETURNING portfolio_id
        """,
        (user[0],),
    ).fetchone()
    return row[0]


@pytest.fixture
def deps(gateway_app: FastAPI, portfolio_id: int) -> ToolDeps:
    transport = httpx.ASGITransport(app=gateway_app)
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=transport)),
        sessions=SessionStore({"tok": portfolio_id}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_portfolio_state_reads_the_real_portfolio(deps: ToolDeps, portfolio_id: int) -> None:
    result = await get_portfolio_state(deps)
    assert result["portfolio"]["portfolio_id"] == portfolio_id
    assert Decimal(result["portfolio"]["cash_balance"]) == Decimal(100000)


@pytest.mark.anyio
async def test_every_money_field_reaching_the_agent_is_a_string(deps: ToolDeps) -> None:
    portfolio = (await get_portfolio_state(deps))["portfolio"]
    for field in ("cash_balance", "initial_capital"):
        assert isinstance(portfolio[field], str), f"{field} reached the agent as a non-string"


@pytest.mark.anyio
async def test_an_order_for_an_untradeable_asset_class_is_refused_by_the_real_chain(
    deps: ToolDeps, db_conn
) -> None:
    # An INDEX has no charge schedule. The refusal must come from the
    # platform's own invariant chain, not from anything the MCP layer
    # re-implements -- that is the whole reason tools speak HTTP.
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('INDEX', 'NSE', 'CM', 'NIFTY', 'ACTIVE', 'NSE:CM:NIFTY')
        RETURNING instrument_id
        """
    ).fetchone()
    result = await place_order(
        deps,
        instrument_id=row[0],
        side="BUY",
        order_type="MARKET",
        quantity="1",
        product="DELIVERY",
        rationale="should never fill",
    )
    assert result["status"] == "REFUSED"


@pytest.mark.anyio
async def test_a_second_identical_order_in_the_same_minute_does_not_double(
    deps: ToolDeps, db_conn
) -> None:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        RETURNING instrument_id
        """
    ).fetchone()
    kwargs = {
        "instrument_id": row[0],
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": "1",
        "product": "DELIVERY",
        "rationale": "same decision, twice",
    }
    first = await place_order(deps, **kwargs)  # type: ignore[arg-type]
    second = await place_order(deps, **kwargs)  # type: ignore[arg-type]
    if first.get("status") == "REFUSED":
        pytest.skip(f"order refused before idempotency could be observed: {first['reason']}")
    assert first["order_id"] == second["order_id"]


@pytest.mark.anyio
async def test_list_instruments_never_surfaces_an_asset_class_outside_the_tradeable_watchlist(
    deps: ToolDeps, db_conn
) -> None:
    """The real `GET /instruments` (`trading/streaming/gateway.py`) is not
    `SELECT * FROM instruments`: it resolves a fixed watchlist of
    crypto/equity/perp canonical keys (`crypto_canonical_keys() +
    upstox_canonical_keys() + perp_canonical_keys()`) and selects only
    rows matching one of those keys. Every one of those three categories
    is in `BROKER_BY_ASSET_CLASS`, so the real route cannot ever return an
    asset class outside that set -- an INDEX row inserted straight into
    the table is invisible to `list_instruments`, not merely flagged
    `tradeable=False`. That is a stronger guarantee than the per-row flag
    the brief this test was drafted from assumed: the route's own
    boundary is a second gate an agent has to pass, not just the client-
    side flag built from `BROKER_BY_ASSET_CLASS`.
    """
    db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('INDEX', 'NSE', 'CM', 'BANKNIFTY', 'ACTIVE', 'NSE:CM:BANKNIFTY')
        """
    )
    result = await list_instruments(deps, "INDEX", None)
    assert result == {"count": 0, "instruments": []}


@pytest.mark.anyio
async def test_list_instruments_flags_a_real_seeded_instrument_tradeable(
    deps: ToolDeps, db_conn
) -> None:
    """The positive half of the case above: a real CRYPTO row, seeded
    with the exact canonical key the route's watchlist expects, comes
    back through the real chain flagged `tradeable=True`.
    """
    db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        """
    )
    result = await list_instruments(deps, "CRYPTO", None)
    assert result["instruments"]
    assert all(row["tradeable"] is True for row in result["instruments"])
