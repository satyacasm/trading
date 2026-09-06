"""Accepting a perpetual order: shorts allowed, margin required."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trading.paper.api import router
from trading.streaming.db import get_db_connection

pytestmark = pytest.mark.db

D = Decimal


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
def client_db(db_conn):
    return db_conn


def _seed(client_db, cash: str = "100000") -> tuple[int, int]:
    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    instrument_id = seed_perp_instruments(
        client_db,
        [
            PerpContractSpec(
                "BTCUSDT", "BTC", "USDT", D("0.10"), D("0.001"), D("0.001"), D("50"), D("0.0125")
            )
        ],
        on=date(2026, 9, 6),
    )["BTC-USDT"]
    client_db.execute(
        "INSERT INTO perp_margin_tiers (instrument_id, notional_floor, effective_from,"
        " notional_cap, max_leverage, maintenance_rate, maintenance_amount)"
        " VALUES (%s, 0, '2019-09-08', 300000, 125, 0.004, 0)"
        " ON CONFLICT DO NOTHING",
        (instrument_id,),
    )
    client_db.execute(
        "INSERT INTO users (user_id, email) VALUES (901, 'perporders@test') ON CONFLICT DO NOTHING"
    )
    row = client_db.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance,"
        " status) VALUES (901, %s, 'USDT', %s, %s, 'ACTIVE') RETURNING portfolio_id",
        (f"perp-orders-{uuid4()}", cash, cash),
    ).fetchone()
    # Deliberately no commit: the route runs on this same connection via
    # the dependency override, so it sees uncommitted rows -- and the
    # fixture's rollback then leaves the database as it found it.
    return instrument_id, int(row[0])


def _body(portfolio_id: int, instrument_id: int, **over: object) -> dict[str, object]:
    body: dict[str, object] = {
        "portfolio_id": portfolio_id,
        "instrument_id": instrument_id,
        "side": "SELL",
        "order_type": "MARKET",
        "quantity": "0.1",
        "product": "INTRADAY",
        "time_in_force": "GTC",
        "rationale": "short the top",
        "leverage": "10",
        "idempotency_key": "perp-order-1",
    }
    body.update(over)
    return body


def test_a_short_with_no_position_is_accepted(client, client_db) -> None:
    """The gate that had to move. Every sell on this platform used to
    require a position behind it, because a spot short is unrepresentable.
    For a perpetual the sell IS the position."""
    instrument_id, portfolio_id = _seed(client_db)
    response = client.post("/orders", json=_body(portfolio_id, instrument_id))
    assert response.status_code == 201, response.text
    assert response.json()["side"] == "SELL"


def test_a_spot_short_is_still_refused(client, client_db) -> None:
    """The rule did not go away; it stopped applying to instruments where
    it makes no sense."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    _instrument_id, portfolio_id = _seed(client_db, cash="90000")
    spot = seed_crypto_instruments(client_db, pairs=["SOL-USDT"])["SOL-USDT"]

    response = client.post(
        "/orders", json=_body(portfolio_id, spot, leverage=None, idempotency_key="spot-short")
    )
    assert response.status_code == 400
    assert "insufficient position" in response.text


def test_an_order_needing_more_margin_than_the_portfolio_has_is_refused(client, client_db) -> None:
    """Leverage is not free money. 10 BTC at 80,000 is 800,000 of notional;
    at 10x that needs 80,000 of margin, which a 1,000 portfolio does not
    have."""
    instrument_id, portfolio_id = _seed(client_db, cash="1000")
    response = client.post(
        "/orders", json=_body(portfolio_id, instrument_id, quantity="10", idempotency_key="big")
    )
    assert response.status_code == 400
    assert "margin" in response.text.lower()


def test_leverage_above_the_contract_ceiling_is_refused(client, client_db) -> None:
    instrument_id, portfolio_id = _seed(client_db)
    response = client.post(
        "/orders",
        json=_body(portfolio_id, instrument_id, leverage="500", idempotency_key="overlevered"),
    )
    assert response.status_code == 400
    assert "leverage" in response.text.lower()


def test_a_quantity_off_the_contract_step_is_refused(client, client_db) -> None:
    """Binance would reject 0.0015 BTC outright: the step is 0.001. Filling
    it here would be a fill the venue could not have given.

    Above the 0.001 minimum on purpose -- 0.0005 is both too small and
    off-step, and would prove only that the first check fires."""
    instrument_id, portfolio_id = _seed(client_db)
    response = client.post(
        "/orders",
        json=_body(portfolio_id, instrument_id, quantity="0.0015", idempotency_key="offstep"),
    )
    assert response.status_code == 400
    assert "step" in response.text.lower()


def test_a_perpetual_order_without_leverage_is_refused(client, client_db) -> None:
    """Assuming one would silently reserve margin the trader never chose."""
    instrument_id, portfolio_id = _seed(client_db)
    response = client.post(
        "/orders",
        json=_body(portfolio_id, instrument_id, leverage=None, idempotency_key="nolev"),
    )
    assert response.status_code == 400
    assert "leverage" in response.text.lower()
