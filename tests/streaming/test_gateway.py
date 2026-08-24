from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
import redis
from fastapi.testclient import TestClient

from trading.streaming.gateway import app, get_db_connection
from trading.streaming.seed_instruments import CRYPTO_PAIRS, seed_crypto_instruments
from trading.streaming.seed_upstox_instruments import UPSTOX_WATCHLIST

pytestmark = pytest.mark.db


@pytest.fixture
def seeded_instrument_id(db_conn) -> int:
    return seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]


@pytest.fixture
def seeded_upstox_equities(db_conn) -> None:
    """`/instruments` calls seed_upstox_instrument_keys(conn) with its
    default 5-symbol UPSTOX_WATCHLIST, which raises ValueError if any
    symbol has no matching series='EQ' NSE row. The real database already
    has these (Phase 0's backfill); this test's dedicated, freshly-migrated
    `trading_test` database does not, so this fixture seeds them."""
    for i, symbol in enumerate(UPSTOX_WATCHLIST):
        db_conn.execute(
            """
            INSERT INTO instruments
                (asset_class, exchange, segment, symbol, series, isin, status, canonical_key)
            VALUES ('EQUITY', 'NSE', 'CM', %s, 'EQ', %s, 'ACTIVE', %s)
            """,
            (symbol, f"INE{i:03d}TEST01", f"NSE:CM:{symbol}:EQ"),
        )


@pytest.fixture
def client(db_conn) -> Iterator[TestClient]:
    # `/instruments` reads through get_db_connection; overriding it with the
    # test's own db_conn means the endpoint sees this test's uncommitted
    # seed row (same transaction, same connection) without ever committing
    # -- db_conn's fixture rolls everything back at teardown either way.
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()


def test_instruments_endpoint_lists_crypto_and_equity_instruments(
    client: TestClient, seeded_instrument_id: int, seeded_upstox_equities: None
) -> None:
    response = client.get("/instruments")
    assert response.status_code == 200
    body = response.json()

    by_symbol = {row["symbol"]: row for row in body}
    assert by_symbol["BTC-USDT"]["instrument_id"] == seeded_instrument_id
    assert by_symbol["BTC-USDT"]["asset_class"] == "CRYPTO"
    assert by_symbol["BTC-USDT"]["exchange"] == "BINANCE"
    assert set(by_symbol) >= set(CRYPTO_PAIRS)

    assert by_symbol["RELIANCE"]["asset_class"] == "EQUITY"
    assert by_symbol["RELIANCE"]["exchange"] == "NSE"


def test_index_serves_the_proof_page(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_cors_allows_the_local_web_dev_origin(
    client: TestClient, seeded_instrument_id: int, seeded_upstox_equities: None
) -> None:
    response = client.get("/instruments", headers={"Origin": "http://localhost:3000"})
    assert response.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_market_data_routes_are_mounted_on_the_gateway_app(client: TestClient) -> None:
    response = client.get("/watchlist")
    assert response.status_code == 200


def test_ws_forwards_a_published_tick_to_a_subscribed_client(
    client: TestClient, seeded_instrument_id: int, redis_client: redis.Redis
) -> None:
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"action": "subscribe", "instrument_id": seeded_instrument_id})

        published = {"instrument_id": seeded_instrument_id, "price": "65000.50"}
        redis_client.publish(f"ticks:{seeded_instrument_id}", json.dumps(published))

        received = json.loads(ws.receive_text())
        assert received == published


def test_ws_does_not_forward_a_tick_for_an_unsubscribed_instrument(
    client: TestClient, seeded_instrument_id: int, redis_client: redis.Redis
) -> None:
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"action": "subscribe", "instrument_id": seeded_instrument_id})
        ws.send_json({"action": "unsubscribe", "instrument_id": seeded_instrument_id})

        redis_client.publish(f"ticks:{seeded_instrument_id}", json.dumps({"x": 1}))

        # A second, still-subscribed instrument proves the socket is alive
        # and simply never received the unsubscribed one.
        other_id = seeded_instrument_id + 1_000_000  # guaranteed distinct channel
        ws.send_json({"action": "subscribe", "instrument_id": other_id})
        redis_client.publish(f"ticks:{other_id}", json.dumps({"instrument_id": other_id}))

        received = json.loads(ws.receive_text())
        assert received == {"instrument_id": other_id}


def test_ws_skips_a_malformed_frame_and_stays_connected(
    client: TestClient, seeded_instrument_id: int, redis_client: redis.Redis
) -> None:
    """A non-JSON frame must be logged and skipped, never crash the
    connection -- `receive_json()` raises on it if unhandled."""
    with client.websocket_connect("/ws") as ws:
        ws.send_text("not json")

        ws.send_json({"action": "subscribe", "instrument_id": seeded_instrument_id})
        published = {"instrument_id": seeded_instrument_id, "price": "1"}
        redis_client.publish(f"ticks:{seeded_instrument_id}", json.dumps(published))

        received = json.loads(ws.receive_text())
        assert received == published


def test_ws_skips_a_non_object_json_frame_and_stays_connected(
    client: TestClient, seeded_instrument_id: int, redis_client: redis.Redis
) -> None:
    """Valid JSON that isn't an object (e.g. a bare array) must not crash
    the connection -- `.get()` on a non-dict raises `AttributeError` if
    unhandled."""
    with client.websocket_connect("/ws") as ws:
        ws.send_json([1, 2, 3])

        ws.send_json({"action": "subscribe", "instrument_id": seeded_instrument_id})
        published = {"instrument_id": seeded_instrument_id, "price": "1"}
        redis_client.publish(f"ticks:{seeded_instrument_id}", json.dumps(published))

        received = json.loads(ws.receive_text())
        assert received == published


def test_ws_rejects_a_non_hashable_instrument_id_and_stays_connected(
    client: TestClient, seeded_instrument_id: int, redis_client: redis.Redis
) -> None:
    """A non-hashable `instrument_id` (e.g. a JSON array) must not crash the
    connection -- `subscribed.add(...)` raises `TypeError` on it if
    unhandled."""
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"action": "subscribe", "instrument_id": [1, 2]})

        ws.send_json({"action": "subscribe", "instrument_id": seeded_instrument_id})
        published = {"instrument_id": seeded_instrument_id, "price": "1"}
        redis_client.publish(f"ticks:{seeded_instrument_id}", json.dumps(published))

        received = json.loads(ws.receive_text())
        assert received == published
