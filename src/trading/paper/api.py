"""REST endpoints for paper-trading portfolios and orders: submit,
cancel, list, and read positions.

Mounted onto `gateway.py`'s FastAPI app rather than defined there
directly, matching `market_data_api.py`'s pattern.

**Every route is a plain `def`, never `async def`.** psycopg is
synchronous; FastAPI dispatches plain `def` routes to a threadpool but
runs `async def` routes directly on the event loop. Commit `5d03a2e`
fixed a permanent, unrecoverable gateway deadlock caused by exactly the
opposite mistake elsewhere in this codebase -- see
`test_no_route_is_a_coroutine_function` in `tests/paper/test_api.py`,
which guards against reintroducing it here.

**Validation at submit, all before the row is written.** `POST /orders`
checks, in order: idempotency (a repeated key returns the original order
rather than re-validating or duplicating it), portfolio exists and is
ACTIVE, instrument exists, market open per `trading_calendar` for the
instrument's exchange (skipped entirely for CRYPTO, which is 24/7),
sufficient cash for a buy or sufficient position for a sell, and a
`charge_schedules` row covering this instrument/product/today. Quantity
positive and rationale non-empty are enforced by `CreateOrderRequest`
itself (a plain Pydantic validation failure, 422) rather than as
explicit steps in the route body -- FastAPI validates the request body
before the route function ever runs, so there is no way to check "does
the portfolio exist" before that regardless of prose ordering.

A missing charge schedule is a hard rejection (400), never a silently
zero-charge order -- `MissingChargeSchedule` (Task 3) names exactly what
is missing.

**Cash/position checks are deliberately duplicated at the database.**
`portfolios.cash_balance` carries `ck_no_negative_cash` and
`positions.quantity` carries `ck_no_negative_position` (migration 0008).
The checks here are a best-effort, submission-time estimate -- a BUY's
notional is estimated from `limit_price` (LIMIT orders) or the latest
`bars_intraday` close (MARKET orders, no charges included, since the
actual fill price and charges are unknown until the engine fills it) --
never the sole line of defence.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Annotated, Any

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from psycopg import Connection
from psycopg.errors import UniqueViolation
from pydantic import BaseModel, Field, model_validator

from trading.config import get_settings
from trading.paper.charges import MissingChargeSchedule, load_schedules
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import Order, Portfolio, Position
from trading.streaming.db import get_db_connection

router = APIRouter()

_ORDERS_CONTROL_CHANNEL = "orders:control"

# Charge schedules are seeded per broker (migration 0007): UPSTOX/NSE for
# equity, BINANCE/BINANCE for crypto. An asset_class outside this map has
# no broker profile at all yet, so it falls straight into the "no charge
# schedule" rejection below rather than guessing one.
_BROKER_BY_ASSET_CLASS: dict[str, str] = {
    "EQUITY": "UPSTOX",
    "CRYPTO": "BINANCE",
}

_TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)

_PORTFOLIO_COLUMNS = (
    "portfolio_id, user_id, name, base_currency, initial_capital,"
    " cash_balance, status, max_daily_loss, max_drawdown_pct"
)
_ORDER_COLUMNS = (
    "order_id, portfolio_id, instrument_id, side, order_type, quantity,"
    " filled_quantity, limit_price, product, time_in_force, status,"
    " rationale, submitted_at"
)


class CreatePortfolioRequest(BaseModel):
    user_id: int
    name: str
    initial_capital: Annotated[Decimal, Field(gt=0)]
    base_currency: str = "INR"
    max_daily_loss: Decimal | None = None
    max_drawdown_pct: Decimal | None = None


class CreateOrderRequest(BaseModel):
    portfolio_id: int
    instrument_id: int
    side: Side
    order_type: OrderType
    quantity: Annotated[Decimal, Field(gt=0)]
    limit_price: Decimal | None = None
    product: Product
    time_in_force: TimeInForce = TimeInForce.DAY
    rationale: str
    idempotency_key: str = Field(min_length=1)

    @model_validator(mode="after")
    def _rationale_non_empty(self) -> CreateOrderRequest:
        if not self.rationale.strip():
            raise ValueError("rationale must not be empty")
        return self

    @model_validator(mode="after")
    def _limit_price_matches_order_type(self) -> CreateOrderRequest:
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price is required for a LIMIT order")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("limit_price must not be set for a MARKET order")
        return self


def _portfolio_from_row(row: Sequence[Any]) -> Portfolio:
    (
        portfolio_id,
        user_id,
        name,
        base_currency,
        initial_capital,
        cash_balance,
        portfolio_status,
        max_daily_loss,
        max_drawdown_pct,
    ) = row
    return Portfolio(
        portfolio_id=portfolio_id,
        user_id=user_id,
        name=name,
        base_currency=base_currency,
        initial_capital=initial_capital,
        cash_balance=cash_balance,
        status=portfolio_status,
        max_daily_loss=max_daily_loss,
        max_drawdown_pct=max_drawdown_pct,
    )


def _order_from_row(row: Sequence[Any]) -> Order:
    (
        order_id,
        portfolio_id,
        instrument_id,
        side,
        order_type,
        quantity,
        filled_quantity,
        limit_price,
        product,
        time_in_force,
        order_status,
        rationale,
        submitted_at,
    ) = row
    return Order(
        order_id=order_id,
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        side=Side(side),
        order_type=OrderType(order_type),
        quantity=quantity,
        filled_quantity=filled_quantity,
        limit_price=limit_price,
        product=Product(product),
        time_in_force=TimeInForce(time_in_force),
        status=OrderStatus(order_status),
        rationale=rationale,
        submitted_at=submitted_at,
    )


def _position_from_row(row: Sequence[Any]) -> Position:
    portfolio_id, instrument_id, quantity, avg_cost, realised_pnl = row
    return Position(
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        quantity=quantity,
        avg_cost=avg_cost,
        realised_pnl=realised_pnl,
    )


def _publish_new_order(order_id: int) -> None:
    client = redis.Redis.from_url(get_settings().redis_url)
    try:
        client.publish(_ORDERS_CONTROL_CHANNEL, json.dumps({"action": "new", "order_id": order_id}))
    finally:
        client.close()


def _require_market_open(conn: Connection, exchange: str, segment: str, today: date) -> None:
    """No row at all is treated the same as an explicitly closed session --
    an unknown market state must never be silently assumed open."""
    row = conn.execute(
        "SELECT is_trading_day FROM trading_calendar"
        " WHERE exchange = %s AND segment = %s AND session_date = %s",
        (exchange, segment, today),
    ).fetchone()
    if row is None or not row[0]:
        raise HTTPException(
            status_code=400,
            detail=f"market is closed for {exchange}/{segment} on {today.isoformat()}",
        )


def _require_sufficient_cash(
    conn: Connection, body: CreateOrderRequest, cash_balance: Decimal
) -> None:
    price = body.limit_price
    if price is None:
        row = conn.execute(
            "SELECT close FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
            (body.instrument_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"no reference price available for instrument_id={body.instrument_id}; "
                    "cannot validate this MARKET order's cash requirement"
                ),
            )
        price = row[0]
    needed = body.quantity * price
    if needed > cash_balance:
        raise HTTPException(
            status_code=400,
            detail=f"insufficient cash: order needs {needed}, portfolio has {cash_balance}",
        )


def _require_sufficient_position(conn: Connection, body: CreateOrderRequest) -> None:
    row = conn.execute(
        "SELECT quantity FROM positions WHERE portfolio_id = %s AND instrument_id = %s",
        (body.portfolio_id, body.instrument_id),
    ).fetchone()
    held: Decimal = row[0] if row is not None else Decimal("0")
    if held < body.quantity:
        raise HTTPException(
            status_code=400,
            detail=f"insufficient position: holding {held}, attempting to sell {body.quantity}",
        )


@router.post("/portfolios", response_model=Portfolio, status_code=status.HTTP_201_CREATED)
def create_portfolio(
    body: CreatePortfolioRequest,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> Portfolio:
    user_exists = conn.execute("SELECT 1 FROM users WHERE user_id = %s", (body.user_id,)).fetchone()
    if user_exists is None:
        raise HTTPException(status_code=404, detail=f"no user with user_id={body.user_id}")

    try:
        row = conn.execute(
            "INSERT INTO portfolios"
            " (user_id, name, base_currency, initial_capital, cash_balance,"
            "  max_daily_loss, max_drawdown_pct)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            f" RETURNING {_PORTFOLIO_COLUMNS}",
            (
                body.user_id,
                body.name,
                body.base_currency,
                body.initial_capital,
                body.initial_capital,
                body.max_daily_loss,
                body.max_drawdown_pct,
            ),
        ).fetchone()
    except UniqueViolation as exc:
        # The test-suite's `get_db_connection` override is a bare `lambda:
        # db_conn` with no wrapping try/except, unlike the real dependency
        # below -- so this connection must be put back in a usable state
        # itself rather than relying on that wrapper to do it.
        conn.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"portfolio {body.name!r} already exists for user_id={body.user_id}",
        ) from exc

    assert row is not None
    return _portfolio_from_row(row)


@router.get("/portfolios", response_model=list[Portfolio])
def list_portfolios(
    user_id: Annotated[int | None, Query()] = None,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> list[Portfolio]:
    if user_id is None:
        rows = conn.execute(
            f"SELECT {_PORTFOLIO_COLUMNS} FROM portfolios ORDER BY portfolio_id"
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_PORTFOLIO_COLUMNS} FROM portfolios WHERE user_id = %s ORDER BY portfolio_id",
            (user_id,),
        ).fetchall()
    return [_portfolio_from_row(row) for row in rows]


@router.post("/orders", response_model=Order, status_code=status.HTTP_201_CREATED)
def create_order(
    body: CreateOrderRequest,
    response: Response,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> Order:
    existing = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE idempotency_key = %s",
        (body.idempotency_key,),
    ).fetchone()
    if existing is not None:
        response.status_code = status.HTTP_200_OK
        return _order_from_row(existing)

    portfolio_row = conn.execute(
        "SELECT status, cash_balance FROM portfolios WHERE portfolio_id = %s",
        (body.portfolio_id,),
    ).fetchone()
    if portfolio_row is None:
        raise HTTPException(
            status_code=404, detail=f"no portfolio with portfolio_id={body.portfolio_id}"
        )
    portfolio_status, cash_balance = portfolio_row
    if portfolio_status != "ACTIVE":
        raise HTTPException(
            status_code=400,
            detail=f"portfolio {body.portfolio_id} is {portfolio_status}, not ACTIVE",
        )

    instrument_row = conn.execute(
        "SELECT asset_class, exchange, segment FROM instruments WHERE instrument_id = %s",
        (body.instrument_id,),
    ).fetchone()
    if instrument_row is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={body.instrument_id}"
        )
    asset_class, exchange, segment = instrument_row

    today = date.today()
    if asset_class != "CRYPTO":
        _require_market_open(conn, exchange, segment, today)

    if body.side is Side.BUY:
        _require_sufficient_cash(conn, body, cash_balance)
    else:
        _require_sufficient_position(conn, body)

    broker = _BROKER_BY_ASSET_CLASS.get(asset_class)
    schedules = (
        load_schedules(conn, broker, exchange, asset_class, body.product, today)
        if broker is not None
        else []
    )
    if not schedules:
        exc = MissingChargeSchedule(
            f"no charge schedule for broker={broker!r} exchange={exchange!r} "
            f"asset_class={asset_class!r} product={body.product.value!r} on {today.isoformat()}"
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    row = conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
        " limit_price, product, time_in_force, status, rationale, idempotency_key)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        f" RETURNING {_ORDER_COLUMNS}",
        (
            body.portfolio_id,
            body.instrument_id,
            body.side.value,
            body.order_type.value,
            body.quantity,
            body.limit_price,
            body.product.value,
            body.time_in_force.value,
            OrderStatus.PENDING.value,
            body.rationale,
            body.idempotency_key,
        ),
    ).fetchone()
    assert row is not None
    order = _order_from_row(row)
    _publish_new_order(order.order_id)
    return order


@router.delete("/orders/{order_id}", response_model=Order)
def cancel_order(
    order_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> Order:
    row = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE order_id = %s", (order_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no order with order_id={order_id}")
    order = _order_from_row(row)
    if order.status in _TERMINAL_ORDER_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"order {order_id} is {order.status.value}, cannot be cancelled",
        )

    updated = conn.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s"
        f" RETURNING {_ORDER_COLUMNS}",
        (OrderStatus.CANCELLED.value, order_id),
    ).fetchone()
    assert updated is not None
    return _order_from_row(updated)


@router.get("/portfolios/{portfolio_id}/positions", response_model=list[Position])
def get_positions(
    portfolio_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> list[Position]:
    exists = conn.execute(
        "SELECT 1 FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=404, detail=f"no portfolio with portfolio_id={portfolio_id}"
        )
    rows = conn.execute(
        "SELECT portfolio_id, instrument_id, quantity, avg_cost, realised_pnl"
        " FROM positions WHERE portfolio_id = %s ORDER BY instrument_id",
        (portfolio_id,),
    ).fetchall()
    return [_position_from_row(row) for row in rows]
