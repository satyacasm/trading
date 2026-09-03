"""The strategy upload endpoint: `src/trading/agent_contract/api.py`.

One POST drives the whole of §9 -- validate, smoke, register -- because
the stages are only useful as a unit: an agent wants to know whether its
code is in, and if not, what to fix.

Every route is a plain `def`. psycopg is synchronous, and an `async def`
route running a blocking DB call on the event loop deadlocked this
gateway permanently once already (commit `5d03a2e`); this endpoint also
shells out to Docker for seconds at a time, which would be far worse on
the loop. `test_no_route_is_a_coroutine_function` is the regression
guard.

`client` mirrors `tests/paper/test_api.py`'s fixture: mount just this
router on a bare `FastAPI()` and override `get_db_connection` with the
test's own rolled-back transaction, so nothing here ever commits.
"""

from __future__ import annotations

import inspect
import textwrap
from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trading.agent_contract.api import router
from trading.streaming.db import get_db_connection

pytestmark = pytest.mark.db


@pytest.fixture
def client(db_conn) -> Iterator[TestClient]:  # noqa: ANN001
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()


def _source(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"


REACHES_THE_NETWORK = _source(
    """
    import requests


    class MyStrategy(Strategy):
        def configure(self):
            return None

        def on_bar(self, ctx, bars):
            pass
    """
)


def test_no_route_is_a_coroutine_function() -> None:
    """psycopg is synchronous and this endpoint blocks on Docker for
    seconds; an `async def` route would run both on the event loop."""
    for route in router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        assert not inspect.iscoroutinefunction(endpoint), f"{endpoint.__name__} must be a plain def"


def test_a_statically_invalid_strategy_is_rejected_without_running_it(client) -> None:  # noqa: ANN001
    """Stage 1 is a cheap local filter and must short-circuit stage 2:
    spawning three containers to discover an import that an AST scan
    catches in milliseconds would be pure waste."""
    response = client.post(
        "/strategies",
        json={"name": "reaches-out", "version": "1.0.0", "source": REACHES_THE_NETWORK},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["accepted"] is False
    assert body["verdict"] == "REJECTED"
    assert body["strategy_id"] is None
    assert "IMPORT_NOT_ALLOWED" in [f["code"] for f in body["findings"]]
    assert "requests" in body["feedback"]
    # Stage 2 never ran, so there is no window and no isolation to report.
    assert body["window"] is None


def test_a_rejection_stores_nothing(client, db_conn) -> None:  # noqa: ANN001
    before = db_conn.execute("SELECT COUNT(*) FROM strategies").fetchone()[0]
    client.post(
        "/strategies",
        json={"name": "reaches-out", "version": "1.0.0", "source": REACHES_THE_NETWORK},
    )
    assert db_conn.execute("SELECT COUNT(*) FROM strategies").fetchone()[0] == before


@pytest.mark.sandbox
def test_a_passing_strategy_is_registered_and_its_run_recorded(client, db_conn) -> None:  # noqa: ANN001
    """The three stages are only worth one endpoint if a pass leaves both
    marks: the strategy is referable, and the run that justified it is
    interpretable later. Three containers run inside this request."""
    symbol = "APIE2E"
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s)",
        (symbol, f"NSE:CM:{symbol}"),
    )
    instrument_id = db_conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol = %s", (symbol,)
    ).fetchone()[0]
    from datetime import UTC, datetime

    for day in (25, 26, 27):
        for minute in range(5):
            db_conn.execute(
                "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
                "close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
                (instrument_id, datetime(2026, 8, day, 9, 15 + minute, tzinfo=UTC)),
            )

    source = _source(
        f"""
        from decimal import Decimal
        from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


        class MyStrategy(Strategy):
            def configure(self):
                return StrategyManifest(
                    name="api-e2e",
                    version="1.0.0",
                    universe=[
                        InstrumentRef(exchange="NSE", segment="CM", symbol="{symbol}"),
                    ],
                    data=DataRequest(bars="1m", history_bars=10),
                    capital=Decimal("1000000"),
                    base_currency="INR",
                )

            def initialize(self, ctx):
                self._ordered = False

            def on_bar(self, ctx, bars):
                if not self._ordered:
                    self._ordered = True
                    ctx.order(
                        list(bars)[0],
                        side="BUY",
                        quantity=Decimal("1"),
                        rationale="api end-to-end",
                    )
        """
    )

    response = client.post(
        "/strategies", json={"name": "api-e2e", "version": "1.0.0", "source": source}
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["accepted"] is True, body["feedback"]
    assert body["verdict"] == "PASSED"
    assert body["strategy_id"] is not None
    assert body["window"]["sessions"] == 3
    assert body["kernel_isolated"] in (True, False)

    stored = db_conn.execute(
        "SELECT verdict, sessions, orders_placed FROM strategy_smoke_runs WHERE strategy_id = %s",
        (body["strategy_id"],),
    ).fetchone()
    assert stored == ("PASSED", 3, 1)


@pytest.mark.sandbox
def test_a_strategy_whose_universe_has_no_bars_is_rejected_and_stores_nothing(
    client,
    db_conn,  # noqa: ANN001
) -> None:
    """A stage-2 rejection reaches the caller as a verdict, and -- because
    `strategy_smoke_runs.strategy_id` is a NOT NULL FK -- leaves no row
    behind. Pins the consequence so it is a known limit, not a surprise."""
    symbol = "APINOBARS"
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s)",
        (symbol, f"NSE:CM:{symbol}"),
    )
    before = db_conn.execute("SELECT COUNT(*) FROM strategies").fetchone()[0]

    source = _source(
        f"""
        from decimal import Decimal
        from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


        class MyStrategy(Strategy):
            def configure(self):
                return StrategyManifest(
                    name="no-bars",
                    version="1.0.0",
                    universe=[
                        InstrumentRef(exchange="NSE", segment="CM", symbol="{symbol}"),
                    ],
                    data=DataRequest(bars="1m", history_bars=10),
                    capital=Decimal("1000000"),
                    base_currency="INR",
                )

            def initialize(self, ctx):
                pass

            def on_bar(self, ctx, bars):
                pass
        """
    )

    body = client.post(
        "/strategies", json={"name": "no-bars", "version": "1.0.0", "source": source}
    ).json()

    assert body["accepted"] is False
    assert body["verdict"] == "REJECTED"
    assert [f["code"] for f in body["findings"]] == ["NO_DATA"]
    assert db_conn.execute("SELECT COUNT(*) FROM strategies").fetchone()[0] == before


# --- GET /strategies/contract: the prompt kit's payload ------------------------


def test_the_contract_is_served_from_the_file_the_validator_enforces(client) -> None:  # noqa: ANN001
    """A prompt kit that hands out a stale contract is worse than one that
    hands out none: the agent writes against rules nobody checks any more,
    and the rejection reads as the agent's fault. Served from disk, not
    from a copy."""
    from pathlib import Path

    import trading.agent_contract as pkg
    from trading.agent_contract.registry import CONTRACT_VERSION

    response = client.get("/strategies/contract")

    assert response.status_code == 200
    body = response.json()
    on_disk = (
        Path(pkg.__file__).resolve().parents[3] / "docs" / "agent-contract" / "STRATEGY_CONTRACT.md"
    ).read_text()
    assert body["contract"] == on_disk
    assert body["sdk_stub"] == (Path(pkg.__file__).parent / "platform_sdk.py").read_text()
    # Read from the registry, never retyped: a hardcoded "0.1" here would
    # keep passing after the real version moved on.
    assert body["contract_version"] == CONTRACT_VERSION


def test_the_served_contract_is_substantial_enough_to_be_the_real_thing(client) -> None:  # noqa: ANN001
    # The vacuity guard: returning "" would satisfy an equality check
    # against a file this test also read as "".
    body = client.get("/strategies/contract").json()
    assert len(body["contract"]) > 10_000
    assert "## 9. Upload, validation, and the feedback loop" in body["contract"]
    assert "NotOnThisPlatform" in body["sdk_stub"]
