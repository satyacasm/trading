from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trading.streaming.db import get_db_connection
from trading.streaming.perp_reference_api import router

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
def perp_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('PERP', 'BINANCE_FUTURES', 'PERP', 'DOGEUSDT', 'ACTIVE',
                'BINANCE_FUTURES:PERP:DOGEUSDT')
        RETURNING instrument_id
        """
    ).fetchone()
    instrument_id = row[0]
    # perp_contract_specs.tick_size is NOT NULL with ck_perp_tick_positive
    # (migrations/versions/0019_perp_contracts.py) -- the brief's fixture
    # omits it, which would fail the insert outright.
    db_conn.execute(
        """
        INSERT INTO perp_contract_specs
            (instrument_id, tick_size, step_size, min_qty, min_notional, liquidation_fee,
             effective_from, effective_to)
        VALUES (%s, 0.0001, 1, 1, 5, 0.015, '2020-01-01', NULL)
        """,
        (instrument_id,),
    )
    # perp_margin_tiers.effective_from is NOT NULL with no default and is
    # part of the primary key (migrations/versions/0019_perp_contracts.py)
    # -- the brief's fixture omits it too.
    db_conn.execute(
        """
        INSERT INTO perp_margin_tiers
            (instrument_id, notional_floor, effective_from, notional_cap, max_leverage,
             maintenance_rate, maintenance_amount)
        VALUES (%s, 0, '2020-01-01', 5000, 75, 0.0065, 0)
        """,
        (instrument_id,),
    )
    db_conn.execute(
        """
        INSERT INTO perp_funding (instrument_id, funding_time, rate, mark_price)
        VALUES (%s, %s, %s, %s)
        """,
        (instrument_id, datetime(2026, 9, 1, 8, tzinfo=UTC), Decimal("0.0001"), Decimal("0.21")),
    )
    return instrument_id


@pytest.fixture
def other_perp_instrument_id(db_conn) -> int:
    """A second perpetual with different contract filters and no funding.

    Exists to prove the route filters by instrument_id rather than by
    happening to be the only perp row in the table -- a query missing a
    WHERE clause, or one that grabs the first row regardless of id, would
    still pass a single-fixture test.
    """
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('PERP', 'BINANCE_FUTURES', 'PERP', 'BTCUSDT', 'ACTIVE',
                'BINANCE_FUTURES:PERP:BTCUSDT')
        RETURNING instrument_id
        """
    ).fetchone()
    instrument_id = row[0]
    db_conn.execute(
        """
        INSERT INTO perp_contract_specs
            (instrument_id, tick_size, step_size, min_qty, min_notional, liquidation_fee,
             effective_from, effective_to)
        VALUES (%s, 0.1, 0.001, 0.001, 20, 0.0125, '2020-01-01', NULL)
        """,
        (instrument_id,),
    )
    return instrument_id


@pytest.fixture
def spot_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


def test_perp_context_carries_the_contract_filters_as_exact_text(
    client: TestClient, perp_instrument_id: int
) -> None:
    response = client.get(f"/perp-context/{perp_instrument_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["instrument_id"] == perp_instrument_id
    assert body["symbol"] == "DOGEUSDT"
    assert Decimal(body["step_size"]) == Decimal(1)
    assert Decimal(body["min_notional"]) == Decimal(5)
    assert isinstance(body["step_size"], str)


def test_perp_context_returns_this_instruments_own_filters_not_anothers(
    client: TestClient, perp_instrument_id: int, other_perp_instrument_id: int
) -> None:
    # Two perpetuals with different steps and floors coexist. If the route
    # ignored instrument_id -- or joined without filtering -- this would
    # come back with DOGE's numbers, or BTC's, or both.
    doge = client.get(f"/perp-context/{perp_instrument_id}").json()
    btc = client.get(f"/perp-context/{other_perp_instrument_id}").json()
    assert doge["symbol"] == "DOGEUSDT"
    assert Decimal(doge["step_size"]) == Decimal(1)
    assert Decimal(doge["min_notional"]) == Decimal(5)
    assert btc["symbol"] == "BTCUSDT"
    assert Decimal(btc["step_size"]) == Decimal("0.001")
    assert Decimal(btc["min_notional"]) == Decimal(20)
    # BTC has no funding rows and no margin tiers seeded in this fixture.
    assert btc["latest_funding_rate"] is None
    assert btc["margin_tiers"] == []
    assert btc["max_leverage"] is None


def test_perp_context_reports_the_latest_funding_observation(
    client: TestClient, perp_instrument_id: int
) -> None:
    body = client.get(f"/perp-context/{perp_instrument_id}").json()
    assert Decimal(body["latest_funding_rate"]) == Decimal("0.0001")
    assert Decimal(body["latest_mark_price"]) == Decimal("0.21")
    assert body["latest_funding_time"].startswith("2026-09-01T08:00:00")


def test_perp_context_lists_the_margin_tiers_in_notional_order(
    client: TestClient, perp_instrument_id: int
) -> None:
    tiers = client.get(f"/perp-context/{perp_instrument_id}").json()["margin_tiers"]
    assert len(tiers) == 1
    assert Decimal(tiers[0]["max_leverage"]) == Decimal(75)
    assert Decimal(tiers[0]["maintenance_rate"]) == Decimal("0.0065")


def test_perp_context_refuses_an_instrument_that_is_not_a_perpetual(
    client: TestClient, spot_instrument_id: int
) -> None:
    # Answering with empty filters would read as "no constraints", which
    # is the opposite of the truth for a spot instrument.
    response = client.get(f"/perp-context/{spot_instrument_id}")
    assert response.status_code == 404
    assert "not a perpetual" in response.json()["detail"]


def test_perp_context_404s_for_an_unknown_instrument(client: TestClient) -> None:
    assert client.get("/perp-context/999999999").status_code == 404
