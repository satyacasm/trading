from __future__ import annotations

import inspect
import json
from collections.abc import Iterator

import pytest
import redis
from fastapi.testclient import TestClient

from trading.streaming.gateway import _HEALTH_COMPONENTS, app, get_db_connection
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
def client(db_conn, seeded_upstox_equities: None) -> Iterator[TestClient]:
    # `/instruments` reads through get_db_connection; overriding it with the
    # test's own db_conn means the endpoint sees this test's uncommitted
    # seed row (same transaction, same connection) without ever committing
    # -- db_conn's fixture rolls everything back at teardown either way.
    #
    # Requesting `seeded_upstox_equities` here (rather than leaving it to
    # individual tests) guarantees it runs before `TestClient(app)` below,
    # which triggers the gateway's startup lifespan. That lifespan seeds the
    # Upstox equity watchlist unconditionally, and `seed_upstox_instrument_keys`
    # raises if the underlying NSE rows don't exist yet -- exactly the rows
    # this fixture creates in the test database.
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

    # Keyed by (symbol, asset_class), because symbol alone stopped being
    # unique when perpetuals arrived: BTC-USDT spot and BTC-USDT perpetual
    # are different instruments with the same name, different prices and
    # different cost models. Anything that identifies an instrument by
    # symbol -- including a picker in the UI -- has to say which.
    by_key = {(row["symbol"], row["asset_class"]): row for row in body}
    assert by_key[("BTC-USDT", "CRYPTO")]["instrument_id"] == seeded_instrument_id
    assert by_key[("BTC-USDT", "CRYPTO")]["exchange"] == "BINANCE"
    assert {symbol for symbol, kind in by_key if kind == "CRYPTO"} >= set(CRYPTO_PAIRS)

    assert by_key[("RELIANCE", "EQUITY")]["exchange"] == "NSE"


def test_instruments_endpoint_performs_no_writes(
    client: TestClient, seeded_instrument_id: int, seeded_upstox_equities: None, db_conn
) -> None:
    """GET must never write. The old implementation called
    `seed_crypto_instruments` (an upsert with `ON CONFLICT DO UPDATE SET
    updated_at = now()`) on every request, taking a row lock each time --
    the root cause of a live deadlock.

    Comparing `updated_at` directly is not a reliable signal here: the test
    and the request share one open transaction (`db_conn`, via the
    dependency override), and Postgres's `now()` is stable for the whole
    transaction, so an in-transaction `UPDATE ... SET updated_at = now()`
    would not actually change the value. `ctid` (the row's physical
    version) does change on every UPDATE, including ones inside the same
    still-open transaction, so it reliably proves whether a write touched
    the row at all.
    """
    before = db_conn.execute(
        "SELECT ctid FROM instruments WHERE instrument_id = %s", (seeded_instrument_id,)
    ).fetchone()

    response = client.get("/instruments")
    assert response.status_code == 200

    after = db_conn.execute(
        "SELECT ctid FROM instruments WHERE instrument_id = %s", (seeded_instrument_id,)
    ).fetchone()

    assert before == after


def test_instruments_route_is_not_a_coroutine_function() -> None:
    """Guards the whole blocking-call-on-event-loop defect class: an
    `async def` route running blocking psycopg calls executes on the event
    loop thread, where two concurrent requests can deadlock each other (as
    happened live). A plain `def` route is dispatched to FastAPI's
    threadpool instead, matching every route in `market_data_api.py`."""
    route = next(r for r in app.routes if getattr(r, "path", None) == "/instruments")
    assert inspect.iscoroutinefunction(route.endpoint) is False


def test_instruments_endpoint_is_scoped_to_the_seeded_watchlists(
    client: TestClient,
    seeded_instrument_id: int,
    seeded_upstox_equities: None,
    db_conn,
) -> None:
    """The response must be the seeded crypto pairs plus the seeded Upstox
    equities -- never the whole `instruments` table, which also holds a
    large backfilled NSE universe unrelated to either watchlist."""
    db_conn.execute(
        """
        INSERT INTO instruments
            (asset_class, exchange, segment, symbol, series, isin, status, canonical_key)
        VALUES ('EQUITY', 'NSE', 'CM', 'WIPRO', 'EQ', 'INE999TEST99', 'ACTIVE', 'NSE:CM:WIPRO:EQ')
        """
    )

    response = client.get("/instruments")
    assert response.status_code == 200
    body = response.json()

    by_symbol = {row["symbol"]: row for row in body}
    assert set(by_symbol) >= set(CRYPTO_PAIRS)
    assert "RELIANCE" in by_symbol
    assert "WIPRO" not in by_symbol


def test_index_serves_the_proof_page(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_cors_allows_the_local_web_dev_origin(
    client: TestClient, seeded_instrument_id: int, seeded_upstox_equities: None
) -> None:
    response = client.get("/instruments", headers={"Origin": "http://localhost:3010"})
    assert response.headers.get("access-control-allow-origin") == "http://localhost:3010"


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


def test_instruments_endpoint_lists_perpetuals_distinguishably(
    client: TestClient, seeded_instrument_id: int
) -> None:
    """A perpetual absent from this list cannot be charted, watched or
    ordered from the UI at all, however completely the backend supports
    it -- which is exactly the state it was in when the order ticket had
    no way to go short.

    It must also be tellable from its spot twin: same symbol, different
    instrument, different price series, different cost model.
    """

    body = client.get("/instruments").json()
    perps = [row for row in body if row["asset_class"] == "PERP"]
    if not perps:  # pragma: no cover - only when the universe is unseeded
        return

    perp = next(row for row in perps if row["symbol"] == "BTC-USDT")
    spot = next(
        row for row in body if row["symbol"] == "BTC-USDT" and row["asset_class"] == "CRYPTO"
    )
    assert perp["instrument_id"] != spot["instrument_id"]
    assert perp["exchange"] == "BINANCE_FUTURES"


def test_health_reports_each_component_present_or_absent(client: TestClient, redis_client) -> None:
    redis_client.set("health:bar_aggregator", "1", ex=30)
    # crypto_ingestor's key is deliberately absent/expired.

    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["components"]["bar_aggregator"] is True
    assert body["components"]["crypto_ingestor"] is False
    assert body["ok"] is False  # not every component is up
    assert set(body["components"]) == set(_HEALTH_COMPONENTS)


def test_health_performs_no_writes(client: TestClient, redis_client) -> None:
    before = redis_client.dbsize()
    client.get("/health")
    assert redis_client.dbsize() == before
