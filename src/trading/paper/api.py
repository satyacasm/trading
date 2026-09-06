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

**`DELETE /orders/{order_id}` also publishes to `orders:control`**, with
`{"action": "cancel", "order_id": ...}`. The Task 8 engine holds open
orders in memory and only reloads from the DB at startup -- without this
publish, a cancellation made while the engine is running would be
invisible to it until restart, and the next tick would fill an order the
user already cancelled.

**The idempotency-key check is itself racy against a second, concurrent
identical submit** -- the pre-check `SELECT` finding nothing is not a
guarantee the following `INSERT` won't collide with a request that
committed in between. `_insert_order` wraps that `INSERT` in
`conn.transaction()` (a `SAVEPOINT`, since the route has already run
several `SELECT`s by this point, so the connection is always
mid-transaction) so that a `UniqueViolation` rolls back to the savepoint
instead of aborting the whole transaction, then re-reads the row the
other request inserted and returns *that* -- the same response a
sequential duplicate already gets, rather than a 500.
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
from trading.contracts import AssetClass
from trading.paper.charges import (
    BROKER_BY_ASSET_CLASS,
    AmbiguousChargeSchedule,
    MissingChargeSchedule,
    load_schedules,
)
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import Order, Portfolio, Position
from trading.streaming.db import get_db_connection

router = APIRouter()

_ORDERS_CONTROL_CHANNEL = "orders:control"

# Charge schedules are seeded per broker (migration 0007): UPSTOX/NSE for
# equity, BINANCE/BINANCE for crypto. An asset_class outside this map has
# no broker profile at all yet, so it falls straight into the "no charge
# schedule" rejection below rather than guessing one.
_BROKER_BY_ASSET_CLASS = BROKER_BY_ASSET_CLASS

# Instruments that trade around the clock, so a DAY order on one has no
# session to expire at.
_ALWAYS_OPEN = frozenset({"CRYPTO", "PERP"})

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
    " rationale, submitted_at, rejection_reason, leverage"
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
    # Required for a perpetual, meaningless otherwise. Not defaulted:
    # leverage decides how much margin the position locks up, and assuming
    # one would reserve an amount the trader never chose.
    leverage: Annotated[Decimal, Field(gt=0)] | None = None
    idempotency_key: str = Field(min_length=1)
    # Set by the live supervisor, never by a human. Null means a
    # person placed this order; it is the only thing distinguishing a
    # strategy's orders from manual ones in the same blotter.
    live_run_id: int | None = None

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
        rejection_reason,
        leverage,
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
        rejection_reason=rejection_reason,
        leverage=leverage,
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


def _publish_order_control(action: str, order_id: int) -> None:
    client = redis.Redis.from_url(get_settings().redis_url)
    try:
        client.publish(
            _ORDERS_CONTROL_CHANNEL, json.dumps({"action": action, "order_id": order_id})
        )
    finally:
        client.close()


def _find_existing_order(conn: Connection, idempotency_key: str) -> Order | None:
    row = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE idempotency_key = %s",
        (idempotency_key,),
    ).fetchone()
    return _order_from_row(row) if row is not None else None


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


_CONTRACT_SPEC = """
    SELECT step_size, min_qty, min_notional FROM perp_contract_specs
    WHERE instrument_id = %s AND effective_to IS NULL
"""

_MAX_LEVERAGE = """
    SELECT max_leverage, notional_cap FROM perp_margin_tiers
    WHERE instrument_id = %s ORDER BY notional_floor
"""

_RESERVED_MARGIN = """
    SELECT coalesce(sum(reserved_margin), 0) FROM perp_positions WHERE portfolio_id = %s
"""


def _require_perp_order_is_tradable(
    conn: Connection, body: CreateOrderRequest, cash_balance: Decimal
) -> None:
    """Everything a perpetual order must satisfy that a spot order need not.

    Four checks, and each one refuses a fill the venue itself would never
    have given:

    - **Leverage is declared.** Assuming one would reserve margin the
      trader never chose.
    - **The quantity is on the contract's step and above its floors.**
      Binance rejects 0.0005 BTC outright; filling it here would invent a
      trade that could not have happened.
    - **The leverage is inside the tier's ceiling** for the notional being
      opened. Tiers exist because size costs leverage.
    - **The margin is there.** Leverage is not free money: the exposure is
      still collateralised, out of cash not already reserved against
      another position.
    """
    if body.leverage is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "a perpetual order must declare its leverage; refusing to assume one, "
                "since it decides how much margin the position locks up"
            ),
        )

    spec = conn.execute(_CONTRACT_SPEC, (body.instrument_id,)).fetchone()
    if spec is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"no contract specification for instrument_id={body.instrument_id}; "
                "its tick, step and minimum notional are unknown, so no order can be "
                "checked against them"
            ),
        )
    step_size, min_qty, min_notional = spec
    if body.quantity < min_qty:
        raise HTTPException(
            status_code=400,
            detail=f"quantity {body.quantity} is below the contract minimum of {min_qty}",
        )
    if body.quantity % step_size != 0:
        raise HTTPException(
            status_code=400,
            detail=(
                f"quantity {body.quantity} is not a multiple of the contract step "
                f"{step_size}; the venue would refuse it"
            ),
        )

    price = body.limit_price or _last_price(conn, body.instrument_id)
    if price is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"no price available for instrument_id={body.instrument_id}; the margin "
                "this order needs cannot be computed without one"
            ),
        )
    notional = body.quantity * price
    if notional < min_notional:
        raise HTTPException(
            status_code=400,
            detail=f"notional {notional} is below the contract minimum of {min_notional}",
        )

    tiers = conn.execute(_MAX_LEVERAGE, (body.instrument_id,)).fetchall()
    if not tiers:
        raise HTTPException(
            status_code=400,
            detail=(
                f"no maintenance-margin tiers for instrument_id={body.instrument_id}; "
                "without them a position could never be liquidated, so it must not be "
                "opened"
            ),
        )
    ceiling = next((max_leverage for max_leverage, cap in tiers if notional <= cap), tiers[-1][0])
    if body.leverage > ceiling:
        raise HTTPException(
            status_code=400,
            detail=(
                f"leverage {body.leverage} exceeds the {ceiling}x ceiling for a notional "
                f"of {notional} on this contract"
            ),
        )

    required = notional / body.leverage
    reserved_row = conn.execute(_RESERVED_MARGIN, (body.portfolio_id,)).fetchone()
    reserved = Decimal("0") if reserved_row is None else Decimal(reserved_row[0])
    free = cash_balance - reserved
    if required > free:
        raise HTTPException(
            status_code=400,
            detail=(
                f"insufficient margin: this order needs {required} and only {free} is free "
                f"({cash_balance} cash less {reserved} already reserved)"
            ),
        )


def _last_price(conn: Connection, instrument_id: int) -> Decimal | None:
    row = conn.execute(
        "SELECT close FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
        (instrument_id,),
    ).fetchone()
    if row is not None:
        return Decimal(row[0])
    row = conn.execute(
        "SELECT close FROM bars_daily WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
        (instrument_id,),
    ).fetchone()
    return None if row is None else Decimal(row[0])


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


def _insert_order(conn: Connection, body: CreateOrderRequest) -> tuple[Order, bool]:
    """Insert the order, or -- if a concurrent request committed the same
    idempotency_key between the caller's pre-check and this INSERT --
    return that other request's order instead of letting the unique
    violation surface as a 500. Returns `(order, created)`.

    `conn.transaction()` opens a SAVEPOINT here rather than a fresh
    transaction: the route has already run several SELECTs by this point,
    so the connection is always already mid-transaction. On
    `UniqueViolation` it rolls back to that savepoint automatically
    (and re-raises), leaving the connection usable for the recovery
    SELECT below instead of stuck aborted.
    """
    try:
        with conn.transaction():
            row = conn.execute(
                "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
                " limit_price, product, time_in_force, status, rationale, idempotency_key,"
                " live_run_id, leverage)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
                    body.live_run_id,
                    body.leverage,
                ),
            ).fetchone()
        assert row is not None
        return _order_from_row(row), True
    except UniqueViolation:
        # Deliberately not `_find_existing_order` -- that name is also the
        # route's pre-check, and a test forcing the pre-check to miss (to
        # exercise this exact recovery path) must not also blind this
        # lookup, or recovery could never find the row that just won the
        # race.
        winner_row = conn.execute(
            f"SELECT {_ORDER_COLUMNS} FROM orders WHERE idempotency_key = %s",
            (body.idempotency_key,),
        ).fetchone()
        assert winner_row is not None, (
            "UniqueViolation on idempotency_key but no matching order found"
        )
        return _order_from_row(winner_row), False


@router.post("/orders", response_model=Order, status_code=status.HTTP_201_CREATED)
def create_order(
    body: CreateOrderRequest,
    response: Response,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> Order:
    existing = _find_existing_order(conn, body.idempotency_key)
    if existing is not None:
        response.status_code = status.HTTP_200_OK
        return existing

    portfolio_row = conn.execute(
        "SELECT status, cash_balance, base_currency FROM portfolios WHERE portfolio_id = %s",
        (body.portfolio_id,),
    ).fetchone()
    if portfolio_row is None:
        raise HTTPException(
            status_code=404, detail=f"no portfolio with portfolio_id={body.portfolio_id}"
        )
    portfolio_status, cash_balance, base_currency = portfolio_row
    if portfolio_status != "ACTIVE":
        raise HTTPException(
            status_code=400,
            detail=f"portfolio {body.portfolio_id} is {portfolio_status}, not ACTIVE",
        )

    instrument_row = conn.execute(
        "SELECT asset_class, exchange, segment, currency FROM instruments WHERE instrument_id = %s",
        (body.instrument_id,),
    ).fetchone()
    if instrument_row is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={body.instrument_id}"
        )
    asset_class, exchange, segment, instrument_currency = instrument_row

    today = date.today()
    if asset_class not in _ALWAYS_OPEN:
        _require_market_open(conn, exchange, segment, today)

    if asset_class == AssetClass.PERP.value:
        # A perpetual sell with no position is not an error: it opens a
        # short, and the sell IS the position. What replaces the
        # position check is a margin check -- leverage is not free money,
        # and the exposure still has to be collateralised.
        _require_perp_order_is_tradable(conn, body, cash_balance)
    elif body.side is Side.BUY:
        _require_sufficient_cash(conn, body, cash_balance)
    else:
        _require_sufficient_position(conn, body)

    # CRIT-1 (Ruling 26): a portfolio is single-currency -- no FX
    # conversion anywhere (spec decision #6). Without this gate, an INR
    # portfolio could buy a USDT instrument: the cash check above would
    # compare a USDT notional against an INR balance, and apply_fill would
    # later subtract USDT from INR cash, both silently wrong by ~90x.
    # Checked here, beside the charge-schedule lookup below, which already
    # holds the instrument row -- and only here: the engine never adds a
    # second check, since orders enter only through this route (an
    # engine-side check would be unreachable dead code).
    if instrument_currency != base_currency:
        raise HTTPException(
            status_code=400,
            detail=(
                f"portfolio {body.portfolio_id} has base_currency={base_currency!r}; "
                f"instrument_id={body.instrument_id} is denominated in "
                f"{instrument_currency!r} -- a portfolio is single-currency, no FX "
                "conversion"
            ),
        )

    broker = _BROKER_BY_ASSET_CLASS.get(asset_class)
    try:
        schedules = (
            load_schedules(conn, broker, exchange, asset_class, body.product, today)
            if broker is not None
            else []
        )
    except AmbiguousChargeSchedule as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not schedules:
        missing_exc = MissingChargeSchedule(
            f"no charge schedule for broker={broker!r} exchange={exchange!r} "
            f"asset_class={asset_class!r} product={body.product.value!r} on {today.isoformat()}"
        )
        raise HTTPException(status_code=400, detail=str(missing_exc)) from missing_exc

    order, created = _insert_order(conn, body)
    if created:
        _publish_order_control("new", order.order_id)
    else:
        # Lost the race to a concurrent identical submit -- same response
        # a sequential duplicate already gets.
        response.status_code = status.HTTP_200_OK
    return order


class PerpPositionOut(BaseModel):
    """One open perpetual, with the numbers a spot position has no room for.

    A separate route from `/positions` rather than a widened one: a
    perpetual and a spot holding answer different questions. Spot has an
    average cost and a market value; a perpetual has an entry, a margin
    that is reserved rather than spent, and a price at which the exchange
    closes it. Folding them into one shape would leave half the fields
    meaningless on each.
    """

    instrument_id: int
    symbol: str
    quantity: str
    entry_price: str
    leverage: str
    reserved_margin: str
    realised_pnl: str
    funding_paid: str
    mark: str | None
    unrealised_pnl: str | None
    liquidation_price: str | None


_PERP_POSITIONS_SQL = """
    SELECT p.instrument_id, i.symbol, p.quantity, p.entry_price, p.leverage,
           p.reserved_margin, p.realised_pnl, p.funding_paid
    FROM perp_positions p JOIN instruments i USING (instrument_id)
    WHERE p.portfolio_id = %s AND p.quantity <> 0
    ORDER BY i.symbol
"""

_PERP_TIERS_SQL = """
    SELECT notional_floor, notional_cap, maintenance_rate, maintenance_amount
    FROM perp_margin_tiers WHERE instrument_id = %s ORDER BY notional_floor
"""


@router.get("/portfolios/{portfolio_id}/perp-positions", response_model=list[PerpPositionOut])
def perp_positions(
    portfolio_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> list[PerpPositionOut]:
    """Open perpetuals, marked and with their liquidation price.

    The mark comes from where the ingestor leaves it, not from a bar: a
    mark is not a traded price and has no bar. A position whose mark is
    unavailable reports `None` for everything derived from it rather than
    a number computed from a stale price -- the liquidation price is the
    last figure that should be quietly wrong.

    A GET that never writes.
    """
    import json as _json

    from redis import Redis

    from trading.paper.liquidation import liquidation_price
    from trading.paper.perp import PerpPosition, unrealised_pnl

    redis = Redis.from_url(get_settings().redis_url, decode_responses=True)
    out: list[PerpPositionOut] = []
    for row in conn.execute(_PERP_POSITIONS_SQL, (portfolio_id,)).fetchall():
        instrument_id, symbol, quantity, entry, leverage, margin, realised, funding = row
        mark: Decimal | None = None
        raw = redis.get(f"perp_mark:{instrument_id}")
        if raw is not None:
            try:
                mark = Decimal(str(_json.loads(raw)["mark_price"]))
            except (ValueError, KeyError, TypeError):
                mark = None

        position = PerpPosition(quantity, entry, leverage)
        tiers = [tuple(t) for t in conn.execute(_PERP_TIERS_SQL, (instrument_id,)).fetchall()]
        liquidation = liquidation_price(position, margin=margin, tiers=tiers) if tiers else None

        out.append(
            PerpPositionOut(
                instrument_id=instrument_id,
                symbol=symbol,
                quantity=str(quantity),
                entry_price=str(entry),
                leverage=str(leverage),
                reserved_margin=str(margin),
                realised_pnl=str(realised),
                funding_paid=str(funding),
                mark=None if mark is None else str(mark),
                unrealised_pnl=(
                    None if mark is None else str(unrealised_pnl(position, mark=mark))
                ),
                liquidation_price=None if liquidation is None else str(liquidation),
            )
        )
    return out


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
    cancelled_order = _order_from_row(updated)
    _publish_order_control("cancel", cancelled_order.order_id)
    return cancelled_order


@router.get("/orders", response_model=list[Order])
def list_orders(
    portfolio_id: int,
    limit: int = Query(default=50, ge=1, le=500),
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> list[Order]:
    """This portfolio's orders, newest first -- the blotter's one query.

    A plain `def`, like every route in this module: psycopg is
    synchronous and an `async def` route running a blocking DB call on
    the event loop deadlocked the gateway permanently under concurrency
    (see this module's docstring).

    An unknown `portfolio_id` is a 404 rather than an empty list,
    mirroring `get_positions`: a typo in the id must be visible, not look
    like a portfolio that simply has not traded yet.
    """
    exists = conn.execute(
        "SELECT 1 FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=404, detail=f"no portfolio with portfolio_id={portfolio_id}"
        )
    rows = conn.execute(
        f"SELECT {_ORDER_COLUMNS} FROM orders WHERE portfolio_id = %s"
        " ORDER BY order_id DESC LIMIT %s",
        (portfolio_id, limit),
    ).fetchall()
    return [_order_from_row(row) for row in rows]


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
