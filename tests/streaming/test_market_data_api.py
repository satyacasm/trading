from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trading.streaming.db import get_db_connection
from trading.streaming.market_data_api import router

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
def fixture_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


def test_get_watchlist_is_empty_when_nothing_added(client: TestClient) -> None:
    response = client.get("/watchlist")
    assert response.status_code == 200
    assert response.json() == []


def test_post_then_get_watchlist_returns_the_added_instrument(
    client: TestClient, fixture_instrument_id: int
) -> None:
    post_response = client.post("/watchlist", json={"instrument_id": fixture_instrument_id})
    assert post_response.status_code == 200

    get_response = client.get("/watchlist")
    body = get_response.json()
    assert len(body) == 1
    assert body[0]["instrument_id"] == fixture_instrument_id
    assert body[0]["symbol"] == "BTC-USDT"
    assert body[0]["asset_class"] == "CRYPTO"
    assert body[0]["last_price"] is None
    assert body[0]["last_ts"] is None


def test_get_watchlist_includes_last_price_from_bars_intraday(
    client: TestClient, db_conn, fixture_instrument_id: int
) -> None:
    client.post("/watchlist", json={"instrument_id": fixture_instrument_id})
    db_conn.execute(
        """
        INSERT INTO bars_intraday
            (instrument_id, ts, interval_sec, open, high, low, close, volume, source)
        VALUES (%s, '2026-08-24T09:16:00Z', 60, 100, 101, 99, 100.5, 10, 6)
        """,
        (fixture_instrument_id,),
    )

    response = client.get("/watchlist")
    body = response.json()
    assert Decimal(str(body[0]["last_price"])) == Decimal("100.5")
    assert body[0]["last_ts"] is not None


def test_post_watchlist_is_idempotent_for_a_duplicate_add(
    client: TestClient, fixture_instrument_id: int
) -> None:
    client.post("/watchlist", json={"instrument_id": fixture_instrument_id})
    second = client.post("/watchlist", json={"instrument_id": fixture_instrument_id})
    assert second.status_code == 200

    body = client.get("/watchlist").json()
    assert len(body) == 1


def test_post_watchlist_404s_for_an_unknown_instrument(client: TestClient) -> None:
    response = client.post("/watchlist", json={"instrument_id": 999999999})
    assert response.status_code == 404


def test_delete_watchlist_removes_an_instrument(
    client: TestClient, fixture_instrument_id: int
) -> None:
    client.post("/watchlist", json={"instrument_id": fixture_instrument_id})

    delete_response = client.delete(f"/watchlist/{fixture_instrument_id}")
    assert delete_response.status_code == 200

    body = client.get("/watchlist").json()
    assert body == []


def test_delete_watchlist_is_a_noop_for_an_instrument_not_in_the_list(client: TestClient) -> None:
    response = client.delete("/watchlist/999999999")
    assert response.status_code == 200
