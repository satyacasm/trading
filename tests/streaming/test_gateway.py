from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
import redis
from fastapi.testclient import TestClient

from trading.streaming.gateway import app, get_db_connection
from trading.streaming.seed_instruments import CRYPTO_PAIRS, seed_crypto_instruments

pytestmark = pytest.mark.db


@pytest.fixture
def seeded_instrument_id(db_conn) -> int:
    return seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]


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


def test_instruments_endpoint_lists_the_seeded_pairs(
    client: TestClient, seeded_instrument_id: int
) -> None:
    response = client.get("/instruments")
    assert response.status_code == 200
    body = response.json()
    assert body["BTC-USDT"] == seeded_instrument_id
    assert set(body) >= set(CRYPTO_PAIRS)


def test_index_serves_the_proof_page(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


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
