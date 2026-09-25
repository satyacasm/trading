"""The order and portfolio HTTP API: `src/trading/paper/api.py`.

Every route is a plain `def` -- psycopg is synchronous, and an `async def`
route running a blocking DB call on the event loop deadlocked the gateway
permanently under concurrency (commit `5d03a2e`). `test_no_route_is_a_
coroutine_function` is the regression guard for that; it must never be
weakened.

`client` mirrors `tests/streaming/test_market_data_api.py`'s fixture:
mount just this router on a bare `FastAPI()` and override
`get_db_connection` with the test's own rolled-back transaction, so
nothing here ever commits to a real database.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal

import pytest
import redis
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from tests.paper.helpers import make_order, make_portfolio
from trading.config import get_settings
from trading.paper import api as paper_api
from trading.paper.api import router
from trading.paper.enums import OrderStatus, Side
from trading.streaming.db import get_db_connection

pytestmark = pytest.mark.db


@pytest.fixture
def client(db_conn) -> Iterator[TestClient]:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(get_settings().redis_url)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def local_user_id(db_conn) -> int:
    row = db_conn.execute("SELECT user_id FROM users WHERE email='local@paper.trading'").fetchone()
    assert row is not None, "migration 0007 must seed the local user"
    return int(row[0])


@pytest.fixture
def portfolio_id(db_conn) -> int:
    return make_portfolio(db_conn, cash=Decimal("100000"))


@pytest.fixture
def usdt_portfolio_id(db_conn) -> int:
    """CRIT-1: a USDT-base-currency portfolio, for tests that must submit
    a crypto order without tripping the currency-mismatch gate -- crypto
    instruments are seeded/resolved with currency='USDT' (see
    trading.streaming.seed_instruments and trading.resolver.instruments),
    never 'INR'."""
    return make_portfolio(db_conn, cash=Decimal("100000"), base_currency="USDT")


@pytest.fixture
def equity_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)"
        " VALUES ('EQUITY', 'NSE', 'CM', 'APITEST', 'ACTIVE', 'TEST/API/EQUITY')"
        " RETURNING instrument_id"
    ).fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture
def open_market(db_conn, equity_instrument_id: int) -> None:
    """A `trading_calendar` row marking NSE/CM open today, so an equity
    order submitted "now" (whenever the suite happens to run) passes the
    market-open check deterministically instead of depending on the wall
    clock."""
    db_conn.execute(
        "INSERT INTO trading_calendar"
        " (exchange, segment, session_date, is_trading_day, session_open, session_close)"
        " VALUES ('NSE', 'CM', %s, true, '09:15', '15:30')"
        " ON CONFLICT (exchange, segment, session_date)"
        " DO UPDATE SET is_trading_day = true",
        (date.today(),),
    )


@pytest.fixture
def crypto_instrument_id(db_conn) -> int:
    """currency='USDT' explicitly -- matching how a real crypto instrument
    is actually seeded/resolved (trading.streaming.seed_instruments,
    trading.resolver.instruments), never the `instruments.currency`
    column's own 'INR' server default, which would silently make this
    fixture pass CRIT-1's currency gate against an INR portfolio for the
    wrong reason."""
    row = db_conn.execute(
        "INSERT INTO instruments"
        " (asset_class, exchange, segment, symbol, currency, status, canonical_key)"
        " VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'APICOIN', 'USDT', 'ACTIVE', 'TEST/API/CRYPTO')"
        " RETURNING instrument_id"
    ).fetchone()
    assert row is not None
    return int(row[0])


def _valid_order_body(
    *,
    portfolio_id: int,
    instrument_id: int,
    side: str = "BUY",
    order_type: str = "LIMIT",
    quantity: str = "10",
    limit_price: str | None = "100.00",
    product: str = "DELIVERY",
    rationale: str = "test rationale",
    idempotency_key: str = "key-1",
) -> dict:
    body = {
        "portfolio_id": portfolio_id,
        "instrument_id": instrument_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "product": product,
        "time_in_force": "DAY",
        "rationale": rationale,
        "idempotency_key": idempotency_key,
    }
    if limit_price is not None:
        body["limit_price"] = limit_price
    return body


# --- The 5d03a2e regression guard -------------------------------------------


def test_no_route_is_a_coroutine_function() -> None:
    """psycopg is synchronous; an `async def` route runs it on the event
    loop and deadlocks the gateway under concurrency (see 5d03a2e)."""
    for route in router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        assert not inspect.iscoroutinefunction(endpoint), f"{endpoint.__name__} must be a plain def"


# --- POST /orders: happy path ------------------------------------------------


def test_valid_order_returns_201_pending(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=equity_instrument_id),
    )
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "PENDING"
    assert body["portfolio_id"] == portfolio_id
    assert body["instrument_id"] == equity_instrument_id
    assert isinstance(body["quantity"], float)
    assert body["quantity"] == 10.0
    assert isinstance(body["limit_price"], float)


def test_valid_order_publishes_to_orders_control(
    client: TestClient,
    redis_client: redis.Redis,
    portfolio_id: int,
    equity_instrument_id: int,
    open_market: None,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("orders:control")
    pubsub.get_message(timeout=1)  # the subscribe confirmation itself

    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=equity_instrument_id),
    )
    order_id = response.json()["order_id"]

    message = pubsub.get_message(timeout=2)
    assert message is not None
    assert message["type"] == "message"
    assert json.loads(message["data"]) == {"action": "new", "order_id": order_id}


# --- Structural validation (422, before the row is written) -----------------


def test_order_with_empty_rationale_is_rejected_422(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id, instrument_id=equity_instrument_id, rationale=""
        ),
    )
    assert response.status_code == 422


def test_order_with_whitespace_only_rationale_is_rejected_422(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    """A brief-adjacent gap: `length(rationale) > 0` at the DB level would
    happily accept "   " as non-empty. Whitespace-only is not a rationale
    either."""
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id, instrument_id=equity_instrument_id, rationale="   "
        ),
    )
    assert response.status_code == 422


def test_order_with_non_positive_quantity_is_rejected_422(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id, instrument_id=equity_instrument_id, quantity="0"
        ),
    )
    assert response.status_code == 422


def test_limit_order_without_limit_price_is_rejected_422(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id, instrument_id=equity_instrument_id, limit_price=None
        ),
    )
    assert response.status_code == 422


def test_market_order_with_limit_price_is_rejected_422(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            order_type="MARKET",
            limit_price="100.00",
        ),
    )
    assert response.status_code == 422


# --- Business validation (400, before the row is written) -------------------


def test_order_for_unknown_portfolio_is_rejected_404(
    client: TestClient, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=999999999, instrument_id=equity_instrument_id),
    )
    assert response.status_code == 404


def test_order_for_a_paused_portfolio_is_rejected_400(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    db_conn.execute(
        "UPDATE portfolios SET status = 'PAUSED' WHERE portfolio_id = %s", (portfolio_id,)
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=equity_instrument_id),
    )
    assert response.status_code == 400
    assert "PAUSED" in response.json()["detail"] or "ACTIVE" in response.json()["detail"]


def test_order_for_unknown_instrument_is_rejected_404(
    client: TestClient, portfolio_id: int
) -> None:
    response = client.post(
        "/orders", json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=999999999)
    )
    assert response.status_code == 404


def test_order_when_market_closed_is_rejected_400(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    db_conn.execute(
        "INSERT INTO trading_calendar"
        " (exchange, segment, session_date, is_trading_day)"
        " VALUES ('NSE', 'CM', %s, false)"
        " ON CONFLICT (exchange, segment, session_date) DO UPDATE SET is_trading_day = false",
        (date.today(),),
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=equity_instrument_id),
    )
    assert response.status_code == 400
    assert "market" in response.json()["detail"].lower()


def test_order_with_no_trading_calendar_entry_is_rejected_400(
    client: TestClient, portfolio_id: int, equity_instrument_id: int
) -> None:
    """No calendar row at all (never seeded that far out) must not be
    silently treated as open -- that would be exactly the silent fallback
    this project designs against."""
    response = client.post(
        "/orders",
        json=_valid_order_body(portfolio_id=portfolio_id, instrument_id=equity_instrument_id),
    )
    assert response.status_code == 400


def test_crypto_order_skips_the_calendar_check(
    client: TestClient, usdt_portfolio_id: int, crypto_instrument_id: int
) -> None:
    """Crypto is 24/7 -- no trading_calendar row exists for it at all, and
    the order must still be accepted (BINANCE/CRYPTO/DELIVERY has a seeded
    charge schedule from migration 0007). Uses a USDT-base-currency
    portfolio (CRIT-1): crypto_instrument_id is USDT-denominated, and an
    INR portfolio buying it would now be rejected by the currency gate --
    a *different* concern from the one this test proves."""
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=usdt_portfolio_id,
            instrument_id=crypto_instrument_id,
            idempotency_key="crypto-key-1",
        ),
    )
    assert response.status_code == 201


def test_order_exceeding_cash_is_rejected_400_with_reason(
    client: TestClient, db_conn, equity_instrument_id: int, open_market: None
) -> None:
    poor_portfolio_id = make_portfolio(db_conn, cash=Decimal("100"))
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=poor_portfolio_id,
            instrument_id=equity_instrument_id,
            quantity="10",
            limit_price="100.00",  # notional 1000 > cash 100
        ),
    )
    assert response.status_code == 400
    assert "cash" in response.json()["detail"].lower()


def test_sell_without_any_position_is_rejected_400(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id, instrument_id=equity_instrument_id, side="SELL"
        ),
    )
    assert response.status_code == 400
    assert "position" in response.json()["detail"].lower()


def test_sell_exceeding_held_position_is_rejected_400(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    db_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, 3, 100, 0)",
        (portfolio_id, equity_instrument_id),
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            side="SELL",
            quantity="10",
        ),
    )
    assert response.status_code == 400


def test_sell_within_held_position_is_accepted(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    db_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, 10, 100, 0)",
        (portfolio_id, equity_instrument_id),
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            side="SELL",
            quantity="5",
        ),
    )
    assert response.status_code == 201


def test_market_buy_with_no_reference_price_is_rejected_400(
    client: TestClient, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    """A MARKET buy has no limit_price to check cash against, so the API
    falls back to the latest bars_intraday close. With no price data at
    all, it must refuse rather than silently assume the order is
    affordable."""
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            order_type="MARKET",
            limit_price=None,
        ),
    )
    assert response.status_code == 400


def test_market_buy_uses_latest_bar_close_for_the_cash_check(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    db_conn.execute(
        "INSERT INTO bars_intraday"
        " (instrument_id, ts, interval_sec, open, high, low, close, volume, source)"
        " VALUES (%s, now(), 60, 50, 51, 49, 50, 10, 6)",
        (equity_instrument_id,),
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            order_type="MARKET",
            limit_price=None,
            quantity="10",  # notional ~500, well within the 100000 cash
        ),
    )
    assert response.status_code == 201


def test_order_for_instrument_with_no_charge_schedule_is_rejected_400_naming_it(
    client: TestClient, usdt_portfolio_id: int, crypto_instrument_id: int
) -> None:
    """Migration 0007 seeds BINANCE/CRYPTO/DELIVERY but not INTRADAY --
    this must be a hard rejection naming the gap, never a silent
    zero-charge order. Uses usdt_portfolio_id (CRIT-1) so this stays a
    pure missing-charge-schedule test, not a currency-mismatch one."""
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=usdt_portfolio_id, instrument_id=crypto_instrument_id, product="INTRADAY"
        ),
    )
    assert response.status_code == 400
    detail = response.json()["detail"].lower()
    assert "charge schedule" in detail
    assert "intraday" in detail


def test_order_with_ambiguous_charge_schedule_is_rejected_400(
    client: TestClient, db_conn, usdt_portfolio_id: int, crypto_instrument_id: int
) -> None:
    """IMP-3 fallout: load_schedules can now raise AmbiguousChargeSchedule
    (a data defect -- two in-force rows for one charge type on the same
    date), not just return an empty list. create_order must turn that into
    a 400 like every other charge-schedule failure, not let it surface as
    an unhandled 500."""
    db_conn.execute(
        "INSERT INTO charge_schedules (broker, exchange, asset_class, product,"
        " charge_type, basis, applies_to_side, rate, cap, rounding,"
        " gst_base_types, effective_from, effective_to, source_note)"
        " VALUES ('BINANCE','BINANCE','CRYPTO','DELIVERY','BROKERAGE',"
        " 'PERCENT_OF_TURNOVER', 'BOTH', 0.002, NULL, 'TWO_DECIMALS', NULL,"
        " '2024-06-01', NULL, 'dup test')"
    )
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=usdt_portfolio_id,
            instrument_id=crypto_instrument_id,
            idempotency_key="ambiguous-schedule-1",
        ),
    )
    assert response.status_code == 400
    assert "brokerage" in response.json()["detail"].lower()

    count = db_conn.execute(
        "SELECT count(*) FROM orders WHERE portfolio_id = %s", (usdt_portfolio_id,)
    ).fetchone()[0]
    assert count == 0


def test_missing_charge_schedule_does_not_write_a_row(
    client: TestClient, db_conn, usdt_portfolio_id: int, crypto_instrument_id: int
) -> None:
    client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=usdt_portfolio_id, instrument_id=crypto_instrument_id, product="INTRADAY"
        ),
    )
    count = db_conn.execute(
        "SELECT count(*) FROM orders WHERE portfolio_id = %s", (usdt_portfolio_id,)
    ).fetchone()[0]
    assert count == 0


# --- CRIT-1: portfolio base_currency must match the instrument's --------


def test_order_with_mismatched_currency_is_rejected_400(
    client: TestClient, portfolio_id: int, crypto_instrument_id: int
) -> None:
    """An INR portfolio buying a USDT instrument must be rejected --
    otherwise the cash check compares a USDT notional against an INR
    balance and apply_fill subtracts USDT from INR cash, both silently
    wrong by ~90x (no FX conversion anywhere; a portfolio is
    single-currency, spec decision #6)."""
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,  # INR
            instrument_id=crypto_instrument_id,  # USDT
            idempotency_key="currency-mismatch-1",
        ),
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "INR" in detail
    assert "USDT" in detail


def test_order_with_mismatched_currency_does_not_write_a_row(
    client: TestClient, db_conn, portfolio_id: int, crypto_instrument_id: int
) -> None:
    client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=crypto_instrument_id,
            idempotency_key="currency-mismatch-2",
        ),
    )
    count = db_conn.execute(
        "SELECT count(*) FROM orders WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert count == 0


def test_order_with_matching_currency_is_accepted(
    client: TestClient, usdt_portfolio_id: int, crypto_instrument_id: int
) -> None:
    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=usdt_portfolio_id,
            instrument_id=crypto_instrument_id,
            idempotency_key="currency-match-1",
        ),
    )
    assert response.status_code == 201


# --- GET /orders (the blotter's data source) --------------------------------


def test_list_orders_returns_this_portfolios_orders_newest_first(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    """The frontend blotter's one query. Newest-first because a blotter is
    read top-down and the order you just placed is the one you are looking
    for."""
    first = make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("10"),
        instrument_id=equity_instrument_id,
    )
    second = make_order(
        db_conn,
        portfolio_id,
        side=Side.SELL,
        quantity=Decimal("5"),
        instrument_id=equity_instrument_id,
    )

    response = client.get(f"/orders?portfolio_id={portfolio_id}")

    assert response.status_code == 200
    body = response.json()
    assert [o["order_id"] for o in body] == [second.order_id, first.order_id]


def test_list_orders_excludes_another_portfolios_orders(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    """Portfolios are independent track records (plan §4.3) -- one
    portfolio's blotter must never show another's orders."""
    mine = make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=equity_instrument_id,
    )
    other_portfolio = make_portfolio(db_conn, cash=Decimal("100000"))
    make_order(
        db_conn,
        other_portfolio,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=equity_instrument_id,
    )

    response = client.get(f"/orders?portfolio_id={portfolio_id}")

    assert response.status_code == 200
    assert [o["order_id"] for o in response.json()] == [mine.order_id]


def test_list_orders_404s_for_an_unknown_portfolio(client: TestClient) -> None:
    """Mirrors get_positions: an unknown portfolio is a 404, not an empty
    list, so a typo in the id is visible instead of looking like a
    portfolio that simply has not traded."""
    response = client.get("/orders?portfolio_id=99999999")
    assert response.status_code == 404
    assert "99999999" in response.json()["detail"]


def test_list_orders_respects_limit(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    for _ in range(3):
        make_order(
            db_conn,
            portfolio_id,
            side=Side.BUY,
            quantity=Decimal("1"),
            instrument_id=equity_instrument_id,
        )

    response = client.get(f"/orders?portfolio_id={portfolio_id}&limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_list_orders_exposes_the_rejection_reason(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    """A blotter that shows *that* an order was rejected without showing
    *why* is the silent-failure shape this project spends its effort
    avoiding: the engine writes a precise reason (a currency mismatch, an
    unaffordable fill, a missing charge schedule) and it must reach the
    screen. `Order` carries the column the table has always had."""
    order = make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=equity_instrument_id,
    )
    db_conn.execute(
        "UPDATE orders SET status=%s, rejection_reason=%s WHERE order_id=%s",
        (
            OrderStatus.REJECTED.value,
            "fill rejected: insufficient cash at fill time",
            order.order_id,
        ),
    )

    response = client.get(f"/orders?portfolio_id={portfolio_id}")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["status"] == "REJECTED"
    assert row["rejection_reason"] == "fill rejected: insufficient cash at fill time"


def test_list_orders_reports_a_null_rejection_reason_for_a_live_order(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    """The field is optional, not an empty string -- a resting order has no
    reason, and the UI distinguishes "no reason" from "reason we failed to
    read"."""
    make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("1"),
        instrument_id=equity_instrument_id,
    )

    response = client.get(f"/orders?portfolio_id={portfolio_id}")

    assert response.json()[0]["rejection_reason"] is None


# --- Idempotency --------------------------------------------------------


def test_repeated_idempotency_key_returns_the_original_order(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int, open_market: None
) -> None:
    body = _valid_order_body(
        portfolio_id=portfolio_id, instrument_id=equity_instrument_id, idempotency_key="dup-key"
    )
    first = client.post("/orders", json=body)
    second = client.post("/orders", json=body)

    assert first.status_code == 201
    # 200, not 201: `create_order` rewrites the status code when it returns
    # an order it did not create. Its sibling race test asserts the same
    # thing; a duplicate that silently reported 201 would tell a client it
    # had just placed a second order.
    assert second.status_code == 200
    assert second.json()["order_id"] == first.json()["order_id"]

    count = db_conn.execute(
        "SELECT count(*) FROM orders WHERE idempotency_key = 'dup-key'"
    ).fetchone()[0]
    assert count == 1


def test_concurrent_identical_submit_returns_the_winners_order_without_duplicating(
    client: TestClient,
    db_conn,
    monkeypatch: pytest.MonkeyPatch,
    portfolio_id: int,
    equity_instrument_id: int,
    open_market: None,
) -> None:
    """The sequential-duplicate test above only proves the pre-check
    SELECT works -- it would pass even if the INSERT had no race handling
    at all, since the pre-check already finds the first order. This test
    forces the exact race a pre-check cannot close: another request's
    INSERT has already committed the same idempotency_key, but *this*
    request's pre-check somehow still finds nothing (the real-world case
    being "in between the two"). That must drive create_order's INSERT
    into a UniqueViolation, which _insert_order must recover from -- by
    returning the row that actually won -- rather than raising a 500 or
    leaving the connection's transaction aborted.
    """
    winner = db_conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
        " limit_price, product, time_in_force, status, rationale, idempotency_key)"
        " VALUES (%s, %s, 'BUY', 'LIMIT', 10, 100.00, 'DELIVERY', 'DAY', 'PENDING',"
        " 'the request that won the race', 'race-key')"
        " RETURNING order_id",
        (portfolio_id, equity_instrument_id),
    ).fetchone()
    assert winner is not None
    winner_order_id = winner[0]

    # Simulate "another request committed in between our pre-check and our
    # INSERT" by making the pre-check itself report nothing found, even
    # though the row above already exists.
    monkeypatch.setattr(paper_api, "_find_existing_order", lambda conn, key: None)

    response = client.post(
        "/orders",
        json=_valid_order_body(
            portfolio_id=portfolio_id,
            instrument_id=equity_instrument_id,
            idempotency_key="race-key",
        ),
    )

    assert response.status_code == 200
    assert response.json()["order_id"] == winner_order_id
    assert response.json()["rationale"] == "the request that won the race"

    count = db_conn.execute(
        "SELECT count(*) FROM orders WHERE idempotency_key = 'race-key'"
    ).fetchone()[0]
    assert count == 1

    # The connection must not be left in an aborted-transaction state by
    # the caught UniqueViolation -- prove it is still usable.
    db_conn.execute("SELECT 1").fetchone()


# --- DELETE /orders/{id} -----------------------------------------------


def test_cancel_open_order_sets_cancelled(client: TestClient, db_conn, portfolio_id: int) -> None:
    order = make_order(
        db_conn, portfolio_id, side=Side.BUY, quantity=Decimal("1"), status=OrderStatus.OPEN
    )

    response = client.delete(f"/orders/{order.order_id}")

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"

    stored = db_conn.execute(
        "SELECT status FROM orders WHERE order_id = %s", (order.order_id,)
    ).fetchone()[0]
    assert stored == "CANCELLED"


def test_cancel_open_order_publishes_cancel_to_orders_control(
    client: TestClient, redis_client: redis.Redis, db_conn, portfolio_id: int
) -> None:
    """The Task 8 engine holds open orders in memory and only reloads from
    the DB at startup -- without this publish, a cancellation made while
    the engine is running would be invisible to it, and the next tick
    would fill an order the user already cancelled."""
    order = make_order(
        db_conn, portfolio_id, side=Side.BUY, quantity=Decimal("1"), status=OrderStatus.OPEN
    )

    pubsub = redis_client.pubsub()
    pubsub.subscribe("orders:control")
    pubsub.get_message(timeout=1)  # the subscribe confirmation itself

    response = client.delete(f"/orders/{order.order_id}")
    assert response.status_code == 200

    message = pubsub.get_message(timeout=2)
    assert message is not None
    assert message["type"] == "message"
    assert json.loads(message["data"]) == {"action": "cancel", "order_id": order.order_id}


def test_cancel_filled_order_returns_409(client: TestClient, db_conn, portfolio_id: int) -> None:
    order = make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        status=OrderStatus.FILLED,
    )

    response = client.delete(f"/orders/{order.order_id}")

    assert response.status_code == 409


def test_cancel_filled_order_does_not_publish(
    client: TestClient, redis_client: redis.Redis, db_conn, portfolio_id: int
) -> None:
    """A rejected cancel (409) must not tell the engine to drop anything --
    there is nothing for it to drop, and the order already finished."""
    order = make_order(
        db_conn,
        portfolio_id,
        side=Side.BUY,
        quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        status=OrderStatus.FILLED,
    )

    pubsub = redis_client.pubsub()
    pubsub.subscribe("orders:control")
    pubsub.get_message(timeout=1)  # the subscribe confirmation itself

    response = client.delete(f"/orders/{order.order_id}")
    assert response.status_code == 409

    assert pubsub.get_message(timeout=0.5) is None


def test_cancel_unknown_order_returns_404(client: TestClient) -> None:
    response = client.delete("/orders/999999999")
    assert response.status_code == 404


def test_cancel_pending_order_sets_cancelled(
    client: TestClient, db_conn, portfolio_id: int
) -> None:
    """PENDING (accepted, not yet acknowledged by the engine) must also be
    cancellable -- a user should be able to pull back an order the engine
    hasn't picked up yet, not just one already resting as OPEN."""
    order = make_order(
        db_conn, portfolio_id, side=Side.BUY, quantity=Decimal("1"), status=OrderStatus.PENDING
    )

    response = client.delete(f"/orders/{order.order_id}")

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"


# --- Portfolios -----------------------------------------------------------


def test_create_portfolio_returns_the_full_row(client: TestClient, local_user_id: int) -> None:
    response = client.post(
        "/portfolios",
        json={"user_id": local_user_id, "name": "growth", "initial_capital": "50000"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["user_id"] == local_user_id
    assert body["name"] == "growth"
    assert body["base_currency"] == "INR"
    assert body["status"] == "ACTIVE"
    assert isinstance(body["initial_capital"], float)
    assert body["initial_capital"] == 50000.0
    assert body["cash_balance"] == 50000.0


def test_create_portfolio_with_duplicate_name_is_rejected_409(
    client: TestClient, local_user_id: int
) -> None:
    body = {"user_id": local_user_id, "name": "dup-portfolio", "initial_capital": "1000"}
    first = client.post("/portfolios", json=body)
    second = client.post("/portfolios", json=body)

    assert first.status_code == 201
    assert second.status_code == 409


def test_get_portfolios_lists_created_portfolios(client: TestClient, local_user_id: int) -> None:
    client.post(
        "/portfolios", json={"user_id": local_user_id, "name": "listed", "initial_capital": "1000"}
    )
    response = client.get("/portfolios")
    assert response.status_code == 200
    names = [p["name"] for p in response.json()]
    assert "listed" in names


def test_get_portfolios_filters_by_user_id(client: TestClient, db_conn, local_user_id: int) -> None:
    other_user = db_conn.execute(
        "INSERT INTO users (email) VALUES ('other@paper.trading') RETURNING user_id"
    ).fetchone()[0]
    client.post(
        "/portfolios", json={"user_id": local_user_id, "name": "mine", "initial_capital": "1000"}
    )
    client.post(
        "/portfolios", json={"user_id": other_user, "name": "theirs", "initial_capital": "1000"}
    )

    response = client.get(f"/portfolios?user_id={local_user_id}")
    names = [p["name"] for p in response.json()]
    assert "mine" in names
    assert "theirs" not in names


# --- GET /portfolios/{id}/positions ----------------------------------------


def test_get_positions_returns_seeded_rows_as_json_numbers(
    client: TestClient, db_conn, portfolio_id: int, equity_instrument_id: int
) -> None:
    db_conn.execute(
        "INSERT INTO positions (portfolio_id, instrument_id, quantity, avg_cost, realised_pnl)"
        " VALUES (%s, %s, 7, 123.45, 10)",
        (portfolio_id, equity_instrument_id),
    )
    response = client.get(f"/portfolios/{portfolio_id}/positions")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["instrument_id"] == equity_instrument_id
    assert isinstance(body[0]["quantity"], float)
    assert body[0]["quantity"] == 7.0
    assert body[0]["avg_cost"] == 123.45


def test_get_positions_for_unknown_portfolio_returns_404(client: TestClient) -> None:
    response = client.get("/portfolios/999999999/positions")
    assert response.status_code == 404


def test_get_positions_is_empty_for_a_portfolio_with_no_trades(
    client: TestClient, portfolio_id: int
) -> None:
    response = client.get(f"/portfolios/{portfolio_id}/positions")
    assert response.status_code == 200
    assert response.json() == []


# --- IMP-1: FastAPI background-task vs. yield-dependency teardown ordering --


def test_background_task_runs_before_yield_dependency_teardown() -> None:
    """IMP-1's empirical unknown, settled by measurement rather than
    assumption. Ruling 27 requires moving create_order's `new`
    orders:control publish into a FastAPI `BackgroundTasks` task -- but
    only if the installed FastAPI version runs background tasks *after* a
    `yield`-dependency's post-yield code (get_db_connection's
    `conn.commit()` happens there). If background tasks instead run
    *before* teardown, moving the publish there changes nothing: it would
    still race ahead of the commit exactly as today, with extra ceremony
    and no fix.

    This reproduces that exact shape -- a yield-dependency plus a
    background task -- independent of any real DB or Redis, so it settles
    the question for whichever FastAPI version is actually installed.

    Result (see final-review-fix-report.md): background tasks run BEFORE
    teardown on the installed FastAPI (0.141.1) -- traced to
    `fastapi.routing.request_response`'s `app()`: `await response(scope,
    receive, send)` (which sends the response and, per Starlette, runs its
    background tasks) happens *inside* the `async with AsyncExitStack() as
    request_stack:` block that yield-dependencies are torn down on exiting.
    So the 'move the publish into BackgroundTasks' source fix from the
    brief would not fix IMP-1 -- this branch ships the reconciliation-sweep
    backstop alone (`trading.paper.engine.reconcile_missing_orders`) and
    leaves the publish exactly where it was. If this test ever starts
    asserting the opposite order, that is the trigger to revisit the
    source fix.
    """
    events: list[str] = []

    def _yield_dependency() -> Iterator[None]:
        yield
        events.append("teardown")

    probe_app = FastAPI()

    @probe_app.get("/probe")
    def probe(
        background_tasks: BackgroundTasks,
        _: None = Depends(_yield_dependency),
    ) -> dict[str, bool]:
        background_tasks.add_task(lambda: events.append("background"))
        return {"ok": True}

    with TestClient(probe_app) as probe_client:
        response = probe_client.get("/probe")
    assert response.status_code == 200

    assert events == ["background", "teardown"], (
        "FastAPI's background-task-vs-teardown ordering changed: "
        f"observed {events!r}. Revisit IMP-1's source fix (ruling 27)."
    )


def test_a_market_order_is_refused_when_the_reference_price_is_stale(db_conn) -> None:
    from datetime import UTC, datetime

    from trading.paper.api import CreateOrderRequest, _require_sufficient_cash
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
        "close, volume, trades, source) VALUES (%s, %s, 60, 100, 100, 100, 100, 1, 1, 6)",
        (iid, datetime(2020, 1, 1, tzinfo=UTC)),  # ancient
    )
    body = CreateOrderRequest(
        portfolio_id=1,
        instrument_id=iid,
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("1"),
        product="DELIVERY",
        rationale="test",
        idempotency_key="test-stale-reference-price",
    )
    with pytest.raises(HTTPException) as exc_info:
        _require_sufficient_cash(db_conn, body, Decimal("1000000"))
    assert exc_info.value.status_code == 400
    assert "reference price stale" in exc_info.value.detail
