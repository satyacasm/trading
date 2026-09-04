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

from tests.agent_contract.conftest import VALID_SOURCE
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

    # The manifest stage 2 proved must survive stage 3. It was previously
    # dropped -- `register_strategy` accepts one and this endpoint never
    # passed it -- so every uploaded strategy stored NULL, discarding the
    # only record of what the run was actually configured to do.
    manifest = db_conn.execute(
        "SELECT manifest FROM strategies WHERE strategy_id = %s", (body["strategy_id"],)
    ).fetchone()[0]
    assert manifest is not None, "the manifest configure() returned was discarded at registration"
    assert manifest["data"]["bars"] == "1m"


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


def test_the_response_carries_the_money_as_strings_not_json_numbers(client) -> None:  # noqa: ANN001
    """JSON has no decimal type -- a number here would be parsed as a
    float by every client, which is the one representation this codebase
    refuses to let money take. Strings cross the wire; the frontend
    formats them for display only."""
    import json as jsonlib

    response = client.post(
        "/strategies",
        json={"name": "reaches-out", "version": "1.0.0", "source": REACHES_THE_NETWORK},
    )
    # A static rejection ran no strategy, so there is no summary to give.
    assert response.json()["summary"] is None

    raw = jsonlib.loads(response.text)
    assert "summary" in raw


def _passing_verdict(**overrides):  # noqa: ANN003, ANN201
    """A verdict built the way `build_verdict` builds one, so these tests
    exercise the real shape rather than a hand-rolled stand-in."""
    from trading.agent_contract.smoke import build_verdict

    outcome = {
        "ok": True,
        "bar_calls": 1875,
        "orders": [],
        "fills": 0,
        "rejections": [],
        "final_cash": "100000",
        "final_equity": "100000",
        "breaker_reason": None,
        "logs": [],
        "error": None,
        "crashed_at": None,
    }
    outcome.update(overrides)
    window = {
        "start": "2026-08-27T00:00:00+00:00",
        "end": "2026-09-02T00:00:00+00:00",
        "sessions": 5,
        "instruments": {},
        "bars": "1m",
        "interval_sec": 60,
    }
    return build_verdict(outcome, outcome, window, "runc", False)


def test_the_summary_names_why_orders_did_not_fill() -> None:
    """`orders: 12 · fills: 0` with no reason is the same class of
    half-truth as serving the wrong bar interval -- it reads as a strategy
    that chose not to trade when in fact every order bounced. The prose
    report already names the reason; the structured summary must too, or a
    client that branches on numbers draws the wrong conclusion.
    """
    from trading.agent_contract.api import _summary_of

    verdict = _passing_verdict(
        orders=[{"order_id": 1, "status": "REJECTED"}],
        rejections=["insufficient funds"],
    )

    summary = _summary_of(verdict)

    assert summary is not None
    assert summary.rejections == 1
    assert summary.rejection_reasons == ["insufficient funds"]


def test_a_tripped_breaker_reaches_the_summary_not_only_the_prose() -> None:
    from trading.agent_contract.api import _summary_of

    verdict = _passing_verdict(orders=[{"order_id": 1}], breaker_reason="max daily loss exceeded")

    summary = _summary_of(verdict)

    assert summary is not None
    assert summary.breaker_reason == "max daily loss exceeded"


def test_a_clean_run_claims_no_rejections_and_no_breaker() -> None:
    """The absent case must be empty rather than absent-shaped noise: a
    clean run reports zero and null, so a client never has to guess
    whether the field was omitted or the run was fine."""
    from trading.agent_contract.api import _summary_of

    summary = _summary_of(_passing_verdict(orders=[{"order_id": 1}], fills=1))

    assert summary is not None
    assert summary.rejections == 0
    assert summary.rejection_reasons == []
    assert summary.breaker_reason is None


def test_listing_strategies_is_empty_before_anything_is_registered(client) -> None:  # noqa: ANN001
    response = client.get("/strategies")

    assert response.status_code == 200, response.text
    assert response.json() == []


def test_a_registered_strategy_is_listed_with_the_run_that_justified_it(  # noqa: ANN001
    client,
    db_conn,
    local_user_id,
) -> None:
    """Until now the registry was write-only from the app's side: an
    upload wrote to `strategies` and `strategy_smoke_runs` and nothing
    could ever read either back. A registered version you cannot see is a
    version you cannot compare, which is what makes §9's immutability rule
    invisible in practice.
    """
    import json

    from trading.agent_contract.registry import register_strategy
    from trading.agent_contract.smoke import record_smoke_run

    registered = register_strategy(
        db_conn,
        user_id=local_user_id,
        name="listed-strategy",
        version="1.0.0",
        source=_source(
            """
            from decimal import Decimal


            class MyStrategy(Strategy):
                def configure(self):
                    return None

                def on_bar(self, ctx, bars):
                    ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="demo")
            """
        ),
    )
    # The manifest is written directly rather than through configure():
    # this test is about what the listing reports, and the round trip that
    # populates the column is covered end-to-end by the sandbox test above.
    db_conn.execute(
        "UPDATE strategies SET manifest = %s WHERE strategy_id = %s",
        (json.dumps({"data": {"bars": "1d"}, "base_currency": "INR"}), registered.strategy_id),
    )
    record_smoke_run(db_conn, registered.strategy_id, _passing_verdict(fills=2))

    body = client.get("/strategies").json()

    assert len(body) == 1
    listed = body[0]
    assert listed["strategy_id"] == registered.strategy_id
    assert listed["name"] == "listed-strategy"
    assert listed["version"] == "1.0.0"
    assert listed["bars"] == "1d"
    assert listed["latest_run"] is not None
    assert listed["latest_run"]["verdict"] == "PASSED_WITH_WARNINGS"
    assert listed["latest_run"]["sessions"] == 5
    assert listed["latest_run"]["kernel_isolated"] is False
    assert listed["latest_run"]["final_equity"] == "100000.0000"


def test_a_strategy_with_no_recorded_run_still_lists(client, db_conn, local_user_id) -> None:  # noqa: ANN001
    """`strategy_smoke_runs` is a separate table and a strategy can exist
    without a row in it. An inner join here would silently hide a
    registered version, which is worse than showing one with no run."""
    from trading.agent_contract.registry import register_strategy

    register_strategy(
        db_conn,
        user_id=local_user_id,
        name="runless",
        version="1.0.0",
        source=_source(
            """
            class MyStrategy(Strategy):
                def configure(self):
                    return None

                def on_bar(self, ctx, bars):
                    pass
            """
        ),
    )

    body = client.get("/strategies").json()

    assert [s["name"] for s in body] == ["runless"]
    assert body[0]["latest_run"] is None
    assert body[0]["bars"] is None


def test_strategies_are_listed_newest_first(client, db_conn, local_user_id) -> None:  # noqa: ANN001
    """Newest first for the same reason the blotter is: the version you
    just uploaded is the one you are looking for."""
    from trading.agent_contract.registry import register_strategy

    source = _source(
        """
        class MyStrategy(Strategy):
            def configure(self):
                return None

            def on_bar(self, ctx, bars):
                pass
        """
    )
    for version in ("1.0.0", "1.1.0", "1.2.0"):
        register_strategy(
            db_conn,
            user_id=local_user_id,
            name="ordered",
            version=version,
            source=source + f"# {version}\n",
        )

    body = client.get("/strategies").json()

    assert [s["version"] for s in body] == ["1.2.0", "1.1.0", "1.0.0"]


def test_a_backtest_for_an_unknown_strategy_is_404(client) -> None:  # noqa: ANN001
    """`get_strategy` raises KeyError for a missing row -- "a caller bug, not
    an empty result". The route has to turn that into a 404 rather than a
    500, because asking to backtest a strategy that was never registered is
    a perfectly ordinary client mistake."""
    response = client.post(
        "/strategies/99999999/backtests", json={"start": "2024-01-01", "end": "2024-03-01"}
    )
    assert response.status_code == 404
    # Not the vacuous 404 an unrouted path returns: the handler ran, looked,
    # and is naming what it could not find. Without this assertion the test
    # passes before the route exists at all.
    assert "99999999" in response.json()["detail"]


def test_a_backtest_refuses_an_uncovered_window_before_running_anything(
    client,  # noqa: ANN001
    db_conn,  # noqa: ANN001
    local_user_id,  # noqa: ANN001
    monkeypatch,  # noqa: ANN001
) -> None:
    """The pre-flight gate has to bite at the route, not just in isolation.

    The strategy is registered WITH a manifest on purpose. Registered
    without one, `backtest` falls back to running `configure()` in a
    container and refuses with MANIFEST_UNRESOLVABLE long before the gate --
    so a test asserting only that *some* finding came back would pass
    without the coverage check ever running. Assert the code.
    """
    from datetime import UTC, datetime

    from trading.agent_contract import smoke as smoke_mod
    from trading.agent_contract.registry import register_strategy

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','ROUTEGATE','INR',"
        "'ACTIVE','NSE:CM:ROUTEGATE') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for day in (2, 3, 4):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,100,100,100,10,1)",
            (instrument_id, datetime(2024, 1, day, 10, 0, tzinfo=UTC)),
        )

    registered = register_strategy(
        db_conn,
        user_id=local_user_id,
        name="route-gate-fixture",
        version="1.0.0",
        source=VALID_SOURCE,
        manifest={
            "name": "route-gate-fixture",
            "version": "1.0.0",
            "capital": "100000",
            "base_currency": "INR",
            "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "ROUTEGATE"}],
            "data": {"bars": "1d", "history_bars": 0},
        },
    )

    started: list[object] = []
    monkeypatch.setattr(smoke_mod, "run_smoke_in_sandbox", lambda *a, **k: started.append(a))

    response = client.post(
        f"/strategies/{registered.strategy_id}/backtests",
        json={"start": "2024-01-02", "end": "2026-12-31"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "REFUSED"
    codes = [f["code"] for f in body["findings"]]
    assert "BACKTEST_WINDOW_UNCOVERED" in codes, codes
    # The refusal names both ends, so the caller can act without reading source.
    message = next(
        f["message"] for f in body["findings"] if f["code"] == "BACKTEST_WINDOW_UNCOVERED"
    )
    assert "2024-01-04" in message and "2026-12-31" in message
    assert started == [], "nothing may be executed once the run is refused"


def test_the_backtest_route_is_not_a_coroutine() -> None:
    """C2: psycopg is synchronous and an async route running a blocking DB
    call on the event loop deadlocked this gateway permanently once already.
    This route also shells out to Docker for seconds at a time."""
    from trading.agent_contract.api import run_backtest

    assert not inspect.iscoroutinefunction(run_backtest)


def _registered_with_daily_bars(db_conn, local_user_id, *, symbol, last_day):  # noqa: ANN001, ANN201
    """A strategy registered WITH a manifest, over an instrument whose daily
    bars stop at `last_day` of 2024-01.

    The manifest matters: registered without one, `backtest` falls back to
    recovering it by running `configure()` in a container and refuses on
    MANIFEST_UNRESOLVABLE long before any gate -- so a test asserting only
    that a refusal happened would pass without the path under test running.
    """
    from datetime import UTC, datetime

    from trading.agent_contract.registry import register_strategy

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
        "RETURNING instrument_id",
        (symbol, f"NSE:CM:{symbol}"),
    ).fetchone()
    instrument_id = row[0]
    for day in range(2, last_day + 1):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,100,100,100,10,1)",
            (instrument_id, datetime(2024, 1, day, 10, 0, tzinfo=UTC)),
        )
    registered = register_strategy(
        db_conn,
        user_id=local_user_id,
        name=f"persist-{symbol.lower()}",
        version="1.0.0",
        source=VALID_SOURCE,
        manifest={
            "name": f"persist-{symbol.lower()}",
            "version": "1.0.0",
            "capital": "100000",
            "base_currency": "INR",
            "universe": [{"exchange": "NSE", "segment": "CM", "symbol": symbol}],
            "data": {"bars": "1d", "history_bars": 0},
        },
    )
    return registered.strategy_id


def test_a_refused_backtest_stores_no_run(client, db_conn, local_user_id) -> None:  # noqa: ANN001
    """Assert zero rows, not a status field.

    A gate that refuses and writes anyway would pass an assertion on the
    response alone, and the row would then claim a run happened -- which is
    exactly what "only executed runs are stored" exists to prevent.
    """
    strategy_id = _registered_with_daily_bars(
        db_conn, local_user_id, symbol="PERSISTREFUSE", last_day=4
    )
    before = db_conn.execute("SELECT count(*) FROM backtest_runs").fetchone()[0]

    response = client.post(
        f"/strategies/{strategy_id}/backtests",
        json={"start": "2024-01-02", "end": "2026-12-31"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "REFUSED"
    assert "BACKTEST_WINDOW_UNCOVERED" in [f["code"] for f in body["findings"]]
    assert body["backtest_run_id"] is None
    after = db_conn.execute("SELECT count(*) FROM backtest_runs").fetchone()[0]
    assert after == before


def _stored_run(db_conn, local_user_id, *, points):  # noqa: ANN001, ANN201
    """A stored run, written through the real write path rather than by
    hand, so these read tests cannot drift from what the writer produces."""
    from datetime import date

    from tests.agent_contract.test_backtest_persistence import _verdict
    from trading.agent_contract.persistence import record_backtest_run
    from trading.agent_contract.registry import register_strategy

    registered = register_strategy(
        db_conn,
        user_id=local_user_id,
        name="read-fixture",
        version="1.0.0",
        source=VALID_SOURCE,
    )
    run_id = record_backtest_run(
        db_conn,
        registered.strategy_id,
        _verdict(points),
        requested_start=date(2024, 1, 8),
        requested_end=date(2024, 1, 10),
        instrument_ids=[58607],
    )
    return registered.strategy_id, run_id


def test_the_list_route_omits_curves_and_the_detail_route_includes_them(
    client,  # noqa: ANN001
    db_conn,  # noqa: ANN001
    local_user_id,  # noqa: ANN001
) -> None:
    """The cheap list is the entire reason there are two routes, and nothing
    else enforces it: a list that embedded curves would transfer every point
    of every run to render a table of dates and final equities.
    """
    points = [
        {"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"},
        {"ts": "2024-01-09T10:00:00+00:00", "equity": "1000123.4567", "cash": "924286.2400"},
        {"ts": "2024-01-10T10:00:00+00:00", "equity": "999876.5433", "cash": "924286.2400"},
    ]
    strategy_id, run_id = _stored_run(db_conn, local_user_id, points=points)

    listed = client.get(f"/strategies/{strategy_id}/backtests")
    assert listed.status_code == 200
    rows = listed.json()
    assert [r["backtest_run_id"] for r in rows] == [run_id]
    assert "equity_curve" not in rows[0]
    assert rows[0]["status"] == "PASSED"

    detail = client.get(f"/backtests/{run_id}")
    assert detail.status_code == 200
    curve = detail.json()["equity_curve"]
    assert [p["ts"] for p in curve] == [p["ts"] for p in points]
    # Money as strings on the wire, for the reason RunSummary gives: JSON
    # numbers are IEEE 754 doubles.
    assert all(isinstance(p["equity"], str) for p in curve)
    assert curve[1]["equity"] == "1000123.4567"


def test_an_unknown_backtest_run_is_404(client) -> None:  # noqa: ANN001
    response = client.get("/backtests/99999999")
    assert response.status_code == 404
    # Not the vacuous 404 an unrouted path returns: the handler ran, looked,
    # and is naming what it could not find.
    assert "99999999" in response.json()["detail"]


def test_the_backtest_read_routes_are_not_coroutines() -> None:
    """psycopg is synchronous and an async route running a blocking DB call
    on the event loop deadlocked this gateway permanently once already."""
    from trading.agent_contract.api import get_backtest, list_backtests

    assert not inspect.iscoroutinefunction(get_backtest)
    assert not inspect.iscoroutinefunction(list_backtests)


def test_the_detail_route_carries_metrics_and_echoes_the_risk_free_rate(
    client,  # noqa: ANN001
    db_conn,  # noqa: ANN001
    local_user_id,  # noqa: ANN001
) -> None:
    """The rate is echoed because on an Indian platform assuming 0 is a
    systematically flattering lie: a 6% strategy reads as respectable and is
    worse than a G-Sec. A reader who never thought about it must be told
    what was assumed rather than left to infer zero.
    """
    points = [
        {"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"},
        {"ts": "2024-01-09T10:00:00+00:00", "equity": "1010000.0000", "cash": "1010000.0000"},
        {"ts": "2024-01-10T10:00:00+00:00", "equity": "990000.0000", "cash": "990000.0000"},
        {"ts": "2024-01-11T10:00:00+00:00", "equity": "1030000.0000", "cash": "1030000.0000"},
    ]
    _strategy_id, run_id = _stored_run(db_conn, local_user_id, points=points)

    detail = client.get(f"/backtests/{run_id}")
    assert detail.status_code == 200
    metrics = detail.json()["metrics"]

    assert metrics["risk_free"] == "0.065"
    # Money and ratios cross as strings: JSON has no decimal type.
    assert isinstance(metrics["total_return"], str)
    assert metrics["total_return"] == "0.03000000"
    # 1010000 -> 990000 against a running peak of 1010000 is -1.980198...%,
    # rendered at the fixed 8 dp every ratio uses.
    assert metrics["max_drawdown"]["depth"] == "-0.01980198"
    assert metrics["max_drawdown"]["recovered"] is True
    assert len(metrics["drawdown_curve"]) == 4

    override = client.get(f"/backtests/{run_id}?risk_free=0")
    assert override.status_code == 200
    assert override.json()["metrics"]["risk_free"] == "0"
    # A different rate must actually move the number it feeds.
    assert override.json()["metrics"]["sharpe"] != metrics["sharpe"]


def test_the_backtest_list_route_still_carries_no_metrics(
    client,  # noqa: ANN001
    db_conn,  # noqa: ANN001
    local_user_id,  # noqa: ANN001
) -> None:
    """Computing metrics for 50 runs means loading 50 curves, which is
    precisely the cost two routes exist to avoid."""
    strategy_id, _run_id = _stored_run(
        db_conn,
        local_user_id,
        points=[
            {"ts": "2024-01-08T10:00:00+00:00", "equity": "1000000.0000", "cash": "1000000.0000"},
            {"ts": "2024-01-09T10:00:00+00:00", "equity": "1010000.0000", "cash": "1010000.0000"},
        ],
    )
    row = client.get(f"/strategies/{strategy_id}/backtests").json()[0]
    assert "metrics" not in row
    assert "equity_curve" not in row
