# Charts + Watchlist Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Next.js web app with a live-updating watchlist and a candlestick chart, backed by new FastAPI endpoints (`/candles`, `/watchlist`, a reshaped `/instruments`) on the existing `stream_gateway` process, covering both crypto and NSE-equity instruments already flowing through this project's streaming pipeline.

**Architecture:** Backend-first: extend `stream_gateway` (FastAPI) with a new `market_data_api` router providing historical OHLCV (TimescaleDB `time_bucket`/`bars_daily`) and watchlist CRUD, then reshape `/instruments` and mount everything. Frontend is a standalone Next.js app in `web/` consuming that REST surface plus the existing `/ws` tick stream, with a client-side tick→candle merge for the live-updating chart.

**Tech Stack:** Python 3.12 (`uv`) · FastAPI · TimescaleDB (`time_bucket`, `first`/`last` aggregates) · psycopg (sync `Connection`, same pattern as `gateway.py`) · Next.js (App Router) + TypeScript + Tailwind · `lightweight-charts`

**Spec:** [`docs/superpowers/specs/2026-08-24-charts-watchlist-ui-design.md`](../specs/2026-08-24-charts-watchlist-ui-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Backend: Python 3.12 exactly, managed by `uv`.**
- **Backend lint/type gate every task:** `uv run ruff check . && uv run ruff format --check . && uv run mypy src` must pass before any commit.
- **Backend: every task ends with a passing `uv run pytest` (default invocation) and a commit.**
- **No `user_id`, no auth, anywhere in this sub-project.** V1 is single-user with no login (plan §12 Q1). The `watchlists` table has no `user_id` column — one global list, not a fake per-user row.
- **Decimal columns serialize to the frontend as plain JSON numbers** (Pydantic's default `Decimal` → JSON-number behavior) — acceptable here because this is a read-only display path and `lightweight-charts` itself only accepts JS `number`s for OHLC values. This does **not** relax the project's general "money is never a float" rule for any code that *parses* external decimal strings — nothing in this sub-project does that; it only re-serializes already-`Decimal` DB columns.
- **CORS is restricted to `http://localhost:3000` only** — this is a personal dev tool, not a public API.
- **`web/` is not added to `docker-compose.yml`.** It runs as a second ad-hoc `npm run dev` process, the same informal way `crypto_ingestor`/`upstox_ingestor`/`gateway` are run today.
- **No frontend automated test framework.** Frontend correctness is verified manually against the real running stack (Task 11), documented with a screenshot — this project's established convention for every prior live-data sub-project.
- **`lightweight-charts` is pinned to `^4.2.0`** in `package.json` — this plan's chart code uses `chart.addCandlestickSeries()`/`series.update()`, the v4 API shape. A later major version changed this API; pinning avoids the code in this plan silently breaking against whatever `latest` resolves to at install time.

---

## File Structure

```
migrations/versions/0005_watchlist.py            new: watchlists table

src/trading/streaming/db.py                       new: shared get_db_connection() (extracted from gateway.py)
src/trading/streaming/gateway.py                  modified: imports db.py, reshapes /instruments, adds CORS, mounts market_data_api router
src/trading/streaming/market_data_api.py          new: /candles, /watchlist endpoints
src/trading/streaming/static/proof.html           modified: one-line patch for /instruments' new list shape

tests/streaming/test_gateway.py                   modified: /instruments test updated for new shape, + CORS test
tests/streaming/test_watchlist_migration.py       new
tests/streaming/test_market_data_api.py           new

web/                                              new: Next.js app (scaffolded, not hand-authored file-by-file)
web/lib/api.ts                                    new: typed REST client
web/lib/useTickStream.ts                          new: shared WS hook (reconnect + subscribe/unsubscribe)
web/lib/candles.ts                                new: client-side tick→candle bucketing
web/app/page.tsx                                  new: watchlist dashboard
web/app/instrument/[id]/page.tsx                  new: candlestick chart page
```

Dependency order:

```
Task 1 (db.py + watchlists migration)
   │
   ├── Task 2 (watchlist CRUD)
   │
   ├── Task 3 (candles: sub-daily bucketing) ── Task 4 (candles: 1d branch)
   │
   └────────────────┴── Task 5 (gateway integration: /instruments reshape, CORS, mount)
                                │
                          Task 6 (scaffold web/)
                                │
                          Task 7 (watchlist dashboard, static)
                                │
                          Task 8 (WS hook + live wiring on dashboard)
                                │
                          Task 9 (chart page, static history)
                                │
                          Task 10 (live tick→candle merge)
                                │
                          Task 11 (end-to-end verification, controller-run)
```

**AI-tier delegation:** Tasks 1–4 are small, mechanical backend units against a complete spec — cheap tier. Task 5 is a small integration task — cheap tier. Tasks 6–10 are this repo's first-ever frontend code and involve real UI/UX judgment (layout, reconnect behavior, live-merge correctness) — standard tier. Task 11 is controller-run: it requires a human eye on a real browser against the real live stack, not something a subagent can verify unsupervised.

---

## Task 1: Shared `get_db_connection()` + `watchlists` migration

**Files:**
- Create: `src/trading/streaming/db.py`
- Modify: `src/trading/streaming/gateway.py`
- Create: `migrations/versions/0005_watchlist.py`
- Test: `tests/streaming/test_watchlist_migration.py`

**Interfaces:**
- Produces: `trading.streaming.db.get_db_connection` — a FastAPI dependency yielding one `psycopg.Connection` per request, committing on success / rolling back on exception (identical behavior to `gateway.py`'s current local definition, just relocated so `market_data_api.py` can import the same dependency without a circular import on `gateway.py`). A `watchlists` table.

- [ ] **Step 1: Extract `get_db_connection` into its own module**

Create `src/trading/streaming/db.py`:

```python
"""Shared FastAPI dependency: one Postgres connection per request.

Extracted out of `gateway.py` so `market_data_api.py`'s router can depend
on the exact same dependency object `gateway.py` uses -- tests override
this single dependency once (`app.dependency_overrides[get_db_connection]
= ...`) and every router mounted on `gateway.app` sees that override,
regardless of which module defines the route.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
from psycopg import Connection

from trading.config import get_settings


def get_db_connection() -> Iterator[Connection]:
    """A real connection per request. Tests override this dependency with
    their own `db_conn` fixture so a route's reads/writes happen inside the
    same rolled-back test transaction instead of committing a second, real
    connection."""
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

- [ ] **Step 2: Point `gateway.py` at the extracted dependency**

In `src/trading/streaming/gateway.py`, delete the local `get_db_connection` function definition (currently right after `_STATIC_ROOT`) and its `Iterator`/`psycopg` imports if they become unused, replacing with:

```python
from trading.streaming.db import get_db_connection
```

Keep this import at module level in `gateway.py` (not inside a function) so `from trading.streaming.gateway import get_db_connection` — which `tests/streaming/test_gateway.py` already does — keeps working unchanged; a module-level import re-exports the name under `trading.streaming.gateway`'s own namespace.

- [ ] **Step 3: Run the existing gateway suite to confirm this refactor is behavior-preserving**

Run: `uv run pytest tests/streaming/test_gateway.py -v`
Expected: all tests that passed before still pass — nothing about `get_db_connection`'s behavior changed, only its location.

- [ ] **Step 4: Write the failing migration test**

Create `tests/streaming/test_watchlist_migration.py`:

```python
from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.db


def test_watchlists_table_has_expected_columns(db_conn):
    rows = db_conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'watchlists'"
    ).fetchall()
    assert {row[0] for row in rows} == {"instrument_id", "added_at"}


def test_watchlists_instrument_id_references_instruments(db_conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db_conn.execute("INSERT INTO watchlists (instrument_id) VALUES (999999999)")
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `uv run pytest tests/streaming/test_watchlist_migration.py -v`
Expected: FAIL — `UndefinedTable: relation "watchlists" does not exist` (migration not yet written/applied).

- [ ] **Step 6: Write the migration**

Create `migrations/versions/0005_watchlist.py`:

```python
"""Add watchlists table for the charts + watchlist web UI.

Part of the charts+watchlist sub-project (docs/superpowers/specs/
2026-08-24-charts-watchlist-ui-design.md). No user_id column: V1 has
exactly one implicit user and no auth (implementation-plan.md Sec 12 Q1),
so a single global list is the honest shape here, not a user_id column
carrying a fake sentinel value for the only row that will ever exist.

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE watchlists (
            instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            added_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS watchlists")
```

Run: `uv run alembic upgrade head`
Expected: migration applies cleanly.

- [ ] **Step 7: Run the migration test to verify it passes**

Run: `uv run pytest tests/streaming/test_watchlist_migration.py -v`
Expected: 2 passed

- [ ] **Step 8: Full suite, lint/type gate, and commit**

Run: `uv run pytest`
Expected: all passing, no regressions.

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean.

```bash
git add src/trading/streaming/db.py src/trading/streaming/gateway.py \
        migrations/versions/0005_watchlist.py tests/streaming/test_watchlist_migration.py
git commit -m "feat(streaming): extract get_db_connection and add watchlists table"
```

---

## Task 2: Watchlist CRUD endpoints

**Files:**
- Create: `src/trading/streaming/market_data_api.py`
- Test: `tests/streaming/test_market_data_api.py`

**Interfaces:**
- Consumes: `trading.streaming.db.get_db_connection` (Task 1), `watchlists` table (Task 1).
- Produces: `router: APIRouter` (this task starts it; Tasks 3–4 append routes to the same `router`). `GET /watchlist`, `POST /watchlist`, `DELETE /watchlist/{instrument_id}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_market_data_api.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.market_data_api'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/market_data_api.py`:

```python
"""REST endpoints for the charts + watchlist web UI: historical candles and
the (single, global -- no auth, no user_id, see this plan's Global
Constraints) watchlist. Mounted onto `gateway.py`'s FastAPI app rather than
defined there directly, keeping that file from accumulating responsibilities
unrelated to WebSocket tick fan-out.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from pydantic import BaseModel

from trading.streaming.db import get_db_connection

router = APIRouter()


class WatchlistItem(BaseModel):
    instrument_id: int
    symbol: str
    asset_class: str
    exchange: str
    added_at: datetime
    last_price: Decimal | None
    last_ts: datetime | None


class AddWatchlistRequest(BaseModel):
    instrument_id: int


_GET_WATCHLIST_SQL = """
    SELECT
        w.instrument_id, i.symbol, i.asset_class, i.exchange, w.added_at,
        latest.close AS last_price, latest.ts AS last_ts
    FROM watchlists w
    JOIN instruments i ON i.instrument_id = w.instrument_id
    LEFT JOIN LATERAL (
        SELECT close, ts FROM bars_intraday b
        WHERE b.instrument_id = w.instrument_id
        ORDER BY b.ts DESC
        LIMIT 1
    ) latest ON true
    ORDER BY w.added_at
"""


@router.get("/watchlist", response_model=list[WatchlistItem])
def get_watchlist(conn: Connection = Depends(get_db_connection)) -> list[WatchlistItem]:  # noqa: B008
    rows = conn.execute(_GET_WATCHLIST_SQL).fetchall()
    return [
        WatchlistItem(
            instrument_id=row[0],
            symbol=row[1],
            asset_class=row[2],
            exchange=row[3],
            added_at=row[4],
            last_price=row[5],
            last_ts=row[6],
        )
        for row in rows
    ]


@router.post("/watchlist")
def add_to_watchlist(
    body: AddWatchlistRequest, conn: Connection = Depends(get_db_connection)  # noqa: B008
) -> dict[str, bool]:
    exists = conn.execute(
        "SELECT 1 FROM instruments WHERE instrument_id = %s", (body.instrument_id,)
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={body.instrument_id}"
        )
    conn.execute(
        "INSERT INTO watchlists (instrument_id) VALUES (%s) ON CONFLICT (instrument_id) DO NOTHING",
        (body.instrument_id,),
    )
    return {"ok": True}


@router.delete("/watchlist/{instrument_id}")
def remove_from_watchlist(
    instrument_id: int, conn: Connection = Depends(get_db_connection)  # noqa: B008
) -> dict[str, bool]:
    conn.execute("DELETE FROM watchlists WHERE instrument_id = %s", (instrument_id,))
    return {"ok": True}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: 7 passed

- [ ] **Step 5: Full suite, lint/type gate, and commit**

Run: `uv run pytest`
Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/market_data_api.py tests/streaming/test_market_data_api.py
git commit -m "feat(streaming): watchlist CRUD endpoints"
```

---

## Task 3: `/candles` — sub-daily bucketing (1m/5m/15m/1h)

**Files:**
- Modify: `src/trading/streaming/market_data_api.py` (append)
- Test: `tests/streaming/test_market_data_api.py` (append)

**Interfaces:**
- Produces: `GET /candles/{instrument_id}?interval={1m|5m|15m|1h}&limit=` → `CandlesResponse`. `Candle`, `CandlesResponse` Pydantic models. `_INTERVAL_BUCKETS`, `_VALID_INTERVALS` (does **not** include `"1d"` yet — Task 4 adds it), `_DEFAULT_LIMIT = 300`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_market_data_api.py`:

```python
from datetime import UTC, datetime


def _insert_bar(db_conn, instrument_id, ts, open_, high, low, close, volume):
    db_conn.execute(
        """
        INSERT INTO bars_intraday
            (instrument_id, ts, interval_sec, open, high, low, close, volume, source)
        VALUES (%s, %s, 60, %s, %s, %s, %s, %s, 6)
        """,
        (instrument_id, ts, open_, high, low, close, volume),
    )


def test_get_candles_buckets_1m_bars_into_a_5m_candle(client, db_conn, fixture_instrument_id):
    base = datetime(2026, 8, 24, 9, 15, tzinfo=UTC)
    _insert_bar(db_conn, fixture_instrument_id, base, 100, 102, 99, 101, 10)
    _insert_bar(db_conn, fixture_instrument_id, base.replace(minute=16), 101, 105, 100, 103, 5)
    _insert_bar(db_conn, fixture_instrument_id, base.replace(minute=17), 103, 104, 98, 99, 7)

    response = client.get(f"/candles/{fixture_instrument_id}?interval=5m")
    assert response.status_code == 200
    body = response.json()
    assert body["instrument_id"] == fixture_instrument_id
    assert body["interval"] == "5m"
    assert len(body["candles"]) == 1
    candle = body["candles"][0]
    assert float(candle["open"]) == 100.0
    assert float(candle["high"]) == 105.0
    assert float(candle["low"]) == 98.0
    assert float(candle["close"]) == 99.0
    assert float(candle["volume"]) == 22.0


def test_get_candles_returns_multiple_buckets_in_chronological_order(
    client, db_conn, fixture_instrument_id
):
    first_bucket = datetime(2026, 8, 24, 9, 15, tzinfo=UTC)
    second_bucket = datetime(2026, 8, 24, 9, 20, tzinfo=UTC)
    _insert_bar(db_conn, fixture_instrument_id, first_bucket, 100, 100, 100, 100, 1)
    _insert_bar(db_conn, fixture_instrument_id, second_bucket, 200, 200, 200, 200, 1)

    body = client.get(f"/candles/{fixture_instrument_id}?interval=5m").json()
    assert len(body["candles"]) == 2
    assert body["candles"][0]["ts"] < body["candles"][1]["ts"]
    assert float(body["candles"][0]["close"]) == 100.0
    assert float(body["candles"][1]["close"]) == 200.0


def test_get_candles_respects_limit(client, db_conn, fixture_instrument_id):
    for minute_offset, bucket_minute in enumerate((15, 20, 25)):
        ts = datetime(2026, 8, 24, 9, bucket_minute, tzinfo=UTC)
        _insert_bar(db_conn, fixture_instrument_id, ts, 1, 1, 1, 100 + minute_offset, 1)

    body = client.get(f"/candles/{fixture_instrument_id}?interval=5m&limit=2").json()
    assert len(body["candles"]) == 2
    # most recent 2 buckets, still returned oldest-first
    assert float(body["candles"][0]["close"]) == 101.0
    assert float(body["candles"][1]["close"]) == 102.0


def test_get_candles_404s_for_an_unknown_instrument(client):
    response = client.get("/candles/999999999?interval=5m")
    assert response.status_code == 404


def test_get_candles_400s_for_an_invalid_interval(client, fixture_instrument_id):
    response = client.get(f"/candles/{fixture_instrument_id}?interval=3m")
    assert response.status_code == 400
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: FAIL — `422 Unprocessable Entity` / `ImportError`, since the `/candles` route doesn't exist yet.

- [ ] **Step 3: Write minimal implementation**

Append to `src/trading/streaming/market_data_api.py`. Add `from fastapi import Query` to the existing `fastapi` import line.

```python
class Candle(BaseModel):
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


class CandlesResponse(BaseModel):
    instrument_id: int
    interval: str
    candles: list[Candle]


_INTERVAL_BUCKETS: dict[str, str] = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
}
_VALID_INTERVALS = frozenset(_INTERVAL_BUCKETS)
_DEFAULT_LIMIT = 300

_BUCKETED_CANDLES_SQL = """
    SELECT
        time_bucket(%s::interval, ts) AS bucket_ts,
        first(open, ts) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, ts) AS close,
        sum(volume) AS volume
    FROM bars_intraday
    WHERE instrument_id = %s AND interval_sec = 60
    GROUP BY bucket_ts
    ORDER BY bucket_ts DESC
    LIMIT %s
"""


def _fetch_bucketed_candles(
    conn: Connection, instrument_id: int, bucket: str, limit: int
) -> list[Candle]:
    rows = conn.execute(_BUCKETED_CANDLES_SQL, (bucket, instrument_id, limit)).fetchall()
    candles = [
        Candle(
            ts=ts, open=open_, high=high, low=low, close=close, volume=volume or Decimal(0)
        )
        for ts, open_, high, low, close, volume in rows
    ]
    return list(reversed(candles))


@router.get("/candles/{instrument_id}", response_model=CandlesResponse)
def get_candles(
    instrument_id: int,
    interval: str = Query(...),
    limit: int = _DEFAULT_LIMIT,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> CandlesResponse:
    if interval not in _VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid interval {interval!r}; expected one of {sorted(_VALID_INTERVALS)}",
        )
    exists = conn.execute(
        "SELECT 1 FROM instruments WHERE instrument_id = %s", (instrument_id,)
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )

    candles = _fetch_bucketed_candles(conn, instrument_id, _INTERVAL_BUCKETS[interval], limit)
    return CandlesResponse(instrument_id=instrument_id, interval=interval, candles=candles)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: 12 passed

- [ ] **Step 5: Full suite, lint/type gate, and commit**

Run: `uv run pytest`
Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/market_data_api.py tests/streaming/test_market_data_api.py
git commit -m "feat(streaming): /candles sub-daily bucketing"
```

---

## Task 4: `/candles` — `1d` branch (equities read `bars_daily`, crypto buckets `bars_intraday`)

**Files:**
- Modify: `src/trading/streaming/market_data_api.py`
- Test: `tests/streaming/test_market_data_api.py` (append)

**Interfaces:**
- Modifies: `_VALID_INTERVALS` gains `"1d"`. `get_candles` gains the asset-class branch.
- Produces: `_fetch_daily_candles(conn, instrument_id, limit) -> list[Candle]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_market_data_api.py`:

```python
@pytest.fixture
def fixture_equity_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, series, status, canonical_key)
        VALUES ('EQUITY', 'NSE', 'CM', 'RELIANCE', 'EQ', 'ACTIVE', 'NSE:CM:RELIANCE:EQ')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


def test_get_candles_1d_reads_bars_daily_for_an_equity(
    client, db_conn, fixture_equity_instrument_id
):
    db_conn.execute(
        """
        INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)
        VALUES (%s, '2026-08-21T00:00:00Z', 2900, 2950, 2890, 2940, 1000000, 1)
        """,
        (fixture_equity_instrument_id,),
    )
    # A 1-minute row that must NOT be used for this equity's 1d candle --
    # proves the equity branch reads bars_daily, not bucketed bars_intraday.
    db_conn.execute(
        """
        INSERT INTO bars_intraday
            (instrument_id, ts, interval_sec, open, high, low, close, volume, source)
        VALUES (%s, '2026-08-21T09:16:00Z', 60, 1, 1, 1, 1, 1, 7)
        """,
        (fixture_equity_instrument_id,),
    )

    body = client.get(f"/candles/{fixture_equity_instrument_id}?interval=1d").json()
    assert len(body["candles"]) == 1
    assert float(body["candles"][0]["close"]) == 2940.0


def test_get_candles_1d_buckets_bars_intraday_for_crypto(
    client, db_conn, fixture_instrument_id
):
    _insert_bar(db_conn, fixture_instrument_id, datetime(2026, 8, 24, 9, 15, tzinfo=UTC), 100, 110, 90, 105, 5)
    _insert_bar(db_conn, fixture_instrument_id, datetime(2026, 8, 24, 10, 0, tzinfo=UTC), 105, 108, 95, 99, 5)

    body = client.get(f"/candles/{fixture_instrument_id}?interval=1d").json()
    assert len(body["candles"]) == 1
    candle = body["candles"][0]
    assert float(candle["open"]) == 100.0
    assert float(candle["high"]) == 110.0
    assert float(candle["low"]) == 90.0
    assert float(candle["close"]) == 99.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v -k test_get_candles_1d`
Expected: FAIL — both currently 400 (`"1d"` not yet a valid interval).

- [ ] **Step 3: Write the fix**

In `src/trading/streaming/market_data_api.py`, change:

```python
_VALID_INTERVALS = frozenset(_INTERVAL_BUCKETS)
```

to:

```python
_VALID_INTERVALS = frozenset({*_INTERVAL_BUCKETS, "1d"})
```

Add the daily-candles query and helper, right after `_fetch_bucketed_candles`:

```python
_DAILY_CANDLES_SQL = """
    SELECT ts, open, high, low, close, volume
    FROM bars_daily
    WHERE instrument_id = %s
    ORDER BY ts DESC
    LIMIT %s
"""


def _fetch_daily_candles(conn: Connection, instrument_id: int, limit: int) -> list[Candle]:
    rows = conn.execute(_DAILY_CANDLES_SQL, (instrument_id, limit)).fetchall()
    candles = [
        Candle(
            ts=ts,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=Decimal(volume) if volume is not None else Decimal(0),
        )
        for ts, open_, high, low, close, volume in rows
    ]
    return list(reversed(candles))
```

Replace `get_candles`'s body with the asset-class branch (the existence check now also fetches `asset_class`):

```python
@router.get("/candles/{instrument_id}", response_model=CandlesResponse)
def get_candles(
    instrument_id: int,
    interval: str = Query(...),
    limit: int = _DEFAULT_LIMIT,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> CandlesResponse:
    if interval not in _VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid interval {interval!r}; expected one of {sorted(_VALID_INTERVALS)}",
        )
    row = conn.execute(
        "SELECT asset_class FROM instruments WHERE instrument_id = %s", (instrument_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )
    asset_class = row[0]

    if interval == "1d" and asset_class != "CRYPTO":
        candles = _fetch_daily_candles(conn, instrument_id, limit)
    else:
        bucket = _INTERVAL_BUCKETS.get(interval, "1 day")
        candles = _fetch_bucketed_candles(conn, instrument_id, bucket, limit)

    return CandlesResponse(instrument_id=instrument_id, interval=interval, candles=candles)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: 14 passed

- [ ] **Step 5: Full suite, lint/type gate, and commit**

Run: `uv run pytest`
Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/market_data_api.py tests/streaming/test_market_data_api.py
git commit -m "feat(streaming): /candles 1d branch (bars_daily for equities, bucketed bars_intraday for crypto)"
```

---

## Task 5: Gateway integration — reshape `/instruments`, CORS, mount `market_data_api`

**Files:**
- Modify: `src/trading/streaming/gateway.py`
- Modify: `src/trading/streaming/static/proof.html`
- Modify: `tests/streaming/test_gateway.py`

**Interfaces:**
- Modifies: `GET /instruments` — from `{symbol: instrument_id}` to `list[InstrumentSummary]` where `InstrumentSummary = {instrument_id, symbol, asset_class, exchange}`.
- Consumes: `trading.streaming.market_data_api.router` (Tasks 2–4), `trading.streaming.seed_upstox_instruments.seed_upstox_instrument_keys` (existing).

- [ ] **Step 1: Write the failing tests**

In `tests/streaming/test_gateway.py`, replace `test_instruments_endpoint_lists_the_seeded_pairs` with:

```python
from trading.streaming.seed_upstox_instruments import UPSTOX_WATCHLIST


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


def test_cors_allows_the_local_web_dev_origin(client: TestClient) -> None:
    response = client.get("/instruments", headers={"Origin": "http://localhost:3000"})
    assert response.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_market_data_routes_are_mounted_on_the_gateway_app(client: TestClient) -> None:
    response = client.get("/watchlist")
    assert response.status_code == 200
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_gateway.py -v`
Expected: FAIL — `/instruments` still returns the old dict shape, no CORS header set, `/watchlist` is a 404 (not yet mounted).

- [ ] **Step 3: Write the fix**

In `src/trading/streaming/gateway.py`, add these imports:

```python
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from trading.streaming import market_data_api
from trading.streaming.seed_upstox_instruments import seed_upstox_instrument_keys
```

Right after `app = FastAPI()`, add:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(market_data_api.router)
```

Replace the `/instruments` route:

```python
class InstrumentSummary(BaseModel):
    instrument_id: int
    symbol: str
    asset_class: str
    exchange: str


@app.get("/instruments", response_model=list[InstrumentSummary])
async def instruments(conn: Connection = Depends(get_db_connection)) -> list[InstrumentSummary]:  # noqa: B008
    # psycopg here is a synchronous, blocking call inside an async route --
    # an accepted simplification for this endpoint (called once per page
    # load, not a hot path), same as the original crypto-only version.
    crypto_ids = set(seed_crypto_instruments(conn).values())
    upstox_ids = set(seed_upstox_instrument_keys(conn).values())
    all_ids = list(crypto_ids | upstox_ids)
    if not all_ids:
        return []
    rows = conn.execute(
        "SELECT instrument_id, symbol, asset_class, exchange FROM instruments "
        "WHERE instrument_id = ANY(%s)",
        (all_ids,),
    ).fetchall()
    return [
        InstrumentSummary(instrument_id=row[0], symbol=row[1], asset_class=row[2], exchange=row[3])
        for row in rows
    ]
```

In `src/trading/streaming/static/proof.html`, update the instruments-loading block (inside `main()`) from:

```javascript
      const instruments = await (await fetch("/instruments")).json();
      const idToSymbol = {};
      for (const [symbol, instrumentId] of Object.entries(instruments)) {
        idToSymbol[instrumentId] = symbol;
```

to:

```javascript
      const instruments = await (await fetch("/instruments")).json();
      const idToSymbol = {};
      for (const inst of instruments.filter((i) => i.asset_class === "CRYPTO")) {
        idToSymbol[inst.instrument_id] = inst.symbol;
```

(This page stays a crypto-only smoke test, per this sub-project's spec — the filter keeps it that way now that `/instruments` covers both asset classes.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_gateway.py -v`
Expected: all passing (the two untouched tests — index, WS forwarding — plus the three new/changed ones).

- [ ] **Step 5: Full suite, lint/type gate, and commit**

Run: `uv run pytest`
Expected: all passing, no regressions.

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/gateway.py src/trading/streaming/static/proof.html tests/streaming/test_gateway.py
git commit -m "feat(streaming): reshape /instruments, add CORS, mount market_data_api"
```

---

## Task 6: Scaffold the Next.js app

**Files:**
- Create: `web/` (Next.js app, scaffolded via CLI)
- Create: `web/lib/api.ts`
- Create: `web/.env.local.example`

**Interfaces:**
- Produces: a running `npm run dev` dev server. `web/lib/api.ts` exports `InstrumentSummary`, `WatchlistItem`, `Candle`, `CandlesResponse`, `Interval` types and `fetchInstruments`, `fetchWatchlist`, `addToWatchlist`, `removeFromWatchlist`, `fetchCandles` functions — every later frontend task imports from here.

- [ ] **Step 1: Scaffold the app**

From the repo root:

```bash
npx create-next-app@latest web --typescript --tailwind --eslint --app --no-src-dir --import-alias "@/*" --use-npm
```

If prompted for anything the flags above don't cover, accept the default.

- [ ] **Step 2: Install the chart library, pinned per this plan's Global Constraints**

```bash
cd web && npm install lightweight-charts@^4.2.0 && cd ..
```

- [ ] **Step 3: Create the env file**

Create `web/.env.local.example`:

```
NEXT_PUBLIC_API_URL=http://localhost:8000
```

```bash
cp web/.env.local.example web/.env.local
```

(`.env.local` is already in `create-next-app`'s generated `.gitignore` — confirm with `cat web/.gitignore | grep env` before continuing; it must **not** be committed, though there's no secret in it, to keep the pattern consistent with how this repo already treats `.env.local` for the Python side.)

- [ ] **Step 4: Write the typed API client**

Create `web/lib/api.ts`:

```typescript
export type InstrumentSummary = {
  instrument_id: number;
  symbol: string;
  asset_class: string;
  exchange: string;
};

export type WatchlistItem = {
  instrument_id: number;
  symbol: string;
  asset_class: string;
  exchange: string;
  added_at: string;
  last_price: number | null;
  last_ts: string | null;
};

export type Candle = {
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type CandlesResponse = {
  instrument_id: number;
  interval: string;
  candles: Candle[];
};

export type Interval = "1m" | "5m" | "15m" | "1h" | "1d";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export async function fetchInstruments(): Promise<InstrumentSummary[]> {
  const res = await fetch(`${API_URL}/instruments`);
  if (!res.ok) throw new Error(`GET /instruments failed: ${res.status}`);
  return res.json();
}

export async function fetchWatchlist(): Promise<WatchlistItem[]> {
  const res = await fetch(`${API_URL}/watchlist`);
  if (!res.ok) throw new Error(`GET /watchlist failed: ${res.status}`);
  return res.json();
}

export async function addToWatchlist(instrumentId: number): Promise<void> {
  const res = await fetch(`${API_URL}/watchlist`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ instrument_id: instrumentId }),
  });
  if (!res.ok) throw new Error(`POST /watchlist failed: ${res.status}`);
}

export async function removeFromWatchlist(instrumentId: number): Promise<void> {
  const res = await fetch(`${API_URL}/watchlist/${instrumentId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`DELETE /watchlist/${instrumentId} failed: ${res.status}`);
}

export async function fetchCandles(
  instrumentId: number,
  interval: Interval,
  limit = 300
): Promise<CandlesResponse> {
  const res = await fetch(
    `${API_URL}/candles/${instrumentId}?interval=${interval}&limit=${limit}`
  );
  if (!res.ok) throw new Error(`GET /candles/${instrumentId} failed: ${res.status}`);
  return res.json();
}
```

- [ ] **Step 5: Verify the dev server boots**

```bash
cd web && npm run dev
```

Expected: the default `create-next-app` starter page loads at `http://localhost:3000` with no compile errors. Stop the server (Ctrl-C) once confirmed. This step doesn't require the backend running yet — it's only confirming the scaffold itself compiles.

- [ ] **Step 6: Commit**

```bash
git add web/
git commit -m "feat(web): scaffold Next.js app and typed API client"
```

---

## Task 7: Watchlist dashboard page (static)

**Files:**
- Create: `web/app/page.tsx`

**Interfaces:**
- Consumes: `web/lib/api.ts` (Task 6).
- Produces: the `/` route rendering the watchlist table with add/remove, no live prices yet (Task 8 adds those).

- [ ] **Step 1: Write the page**

Replace the generated `web/app/page.tsx` with:

```tsx
"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  InstrumentSummary,
  WatchlistItem,
  addToWatchlist,
  fetchInstruments,
  fetchWatchlist,
  removeFromWatchlist,
} from "@/lib/api";

export default function WatchlistPage() {
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [instruments, setInstruments] = useState<InstrumentSummary[]>([]);
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function reload() {
    try {
      const [wl, inst] = await Promise.all([fetchWatchlist(), fetchInstruments()]);
      setWatchlist(wl);
      setInstruments(inst);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    reload();
  }, []);

  const watchedIds = new Set(watchlist.map((w) => w.instrument_id));
  const matches = query
    ? instruments.filter(
        (i) =>
          !watchedIds.has(i.instrument_id) &&
          i.symbol.toLowerCase().includes(query.toLowerCase())
      )
    : [];

  async function handleAdd(instrumentId: number) {
    await addToWatchlist(instrumentId);
    setQuery("");
    await reload();
  }

  async function handleRemove(instrumentId: number) {
    await removeFromWatchlist(instrumentId);
    await reload();
  }

  return (
    <main className="p-8 max-w-3xl mx-auto">
      <h1 className="text-2xl font-bold mb-4">Watchlist</h1>
      {error && <p className="text-red-600 mb-4">{error}</p>}

      <div className="mb-6 relative">
        <input
          className="border rounded px-3 py-2 w-full"
          placeholder="Add a symbol..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        {matches.length > 0 && (
          <ul className="absolute z-10 bg-white border rounded w-full mt-1 max-h-48 overflow-auto">
            {matches.slice(0, 10).map((m) => (
              <li key={m.instrument_id}>
                <button
                  className="w-full text-left px-3 py-2 hover:bg-gray-100"
                  onClick={() => handleAdd(m.instrument_id)}
                >
                  {m.symbol} <span className="text-gray-400 text-sm">{m.asset_class}</span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <table className="w-full border-collapse">
        <thead>
          <tr className="text-left border-b">
            <th className="py-2">Symbol</th>
            <th>Asset</th>
            <th>Last price</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {watchlist.map((row) => (
            <tr key={row.instrument_id} id={`watchlist-row-${row.instrument_id}`} className="border-b">
              <td className="py-2">
                <Link href={`/instrument/${row.instrument_id}`}>{row.symbol}</Link>
              </td>
              <td>{row.asset_class}</td>
              <td id={`price-${row.instrument_id}`}>
                {row.last_price !== null ? row.last_price : "--"}
              </td>
              <td>
                <button className="text-red-600" onClick={() => handleRemove(row.instrument_id)}>
                  remove
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
```

- [ ] **Step 2: Verify Tailwind picks up `app/`**

Open `web/tailwind.config.ts` and confirm its `content` array includes `./app/**/*.{js,ts,jsx,tsx,mdx}` (the `create-next-app --tailwind` default already includes this — this step is just confirming, not necessarily editing).

- [ ] **Step 3: Manual verification against the real backend**

In one terminal: `docker compose up -d` (Postgres + Redis), then `uv run alembic upgrade head`, then `uvicorn trading.streaming.gateway:app --reload` from the repo root.
In another terminal: `cd web && npm run dev`.

In a browser at `http://localhost:3000`:
- Type "BTC" in the add box, click the `BTC-USDT` match — it appears in the table.
- Reload the page — it's still there (confirms the `watchlists` table persists it, not local state).
- Click "remove" — it disappears.

Expected: no console errors, add/remove/reload all behave as above.

- [ ] **Step 4: Commit**

```bash
git add web/app/page.tsx
git commit -m "feat(web): watchlist dashboard page"
```

---

## Task 8: Shared WS hook + live price wiring on the dashboard

**Files:**
- Create: `web/lib/useTickStream.ts`
- Modify: `web/app/page.tsx`

**Interfaces:**
- Produces: `useTickStream(instrumentIds: number[], onTick: (tick: Tick) => void): void` — manages one reconnecting WebSocket, diffing subscribe/unsubscribe as `instrumentIds` changes. `Tick` type.

- [ ] **Step 1: Write the hook**

Create `web/lib/useTickStream.ts`:

```typescript
"use client";

import { useEffect, useRef } from "react";

export type Tick = {
  instrument_id: number;
  ts: string;
  price: number;
  quantity: number;
  side?: string | null;
};

const WS_URL =
  (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000").replace(/^http/, "ws") + "/ws";
const MAX_BACKOFF_MS = 10_000;

export function useTickStream(instrumentIds: number[], onTick: (tick: Tick) => void): void {
  const wsRef = useRef<WebSocket | null>(null);
  const subscribedRef = useRef<Set<number>>(new Set());
  const onTickRef = useRef(onTick);
  const backoffRef = useRef(1000);
  const closedByUsRef = useRef(false);

  onTickRef.current = onTick;

  // One connection for the component's lifetime, reconnecting with backoff
  // on any drop -- a dashboard left open for hours across a laptop sleep
  // or network blip must recover on its own, not go silently stale.
  useEffect(() => {
    closedByUsRef.current = false;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        backoffRef.current = 1000;
        subscribedRef.current = new Set();
        for (const id of instrumentIdsRef.current) {
          ws.send(JSON.stringify({ action: "subscribe", instrument_id: id }));
          subscribedRef.current.add(id);
        }
      };

      ws.onmessage = (event) => {
        onTickRef.current(JSON.parse(event.data) as Tick);
      };

      ws.onclose = () => {
        if (closedByUsRef.current) return;
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, MAX_BACKOFF_MS);
        setTimeout(connect, delay);
      };
    }

    connect();

    return () => {
      closedByUsRef.current = true;
      wsRef.current?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const instrumentIdsRef = useRef<number[]>(instrumentIds);
  instrumentIdsRef.current = instrumentIds;

  // Diff subscriptions whenever the caller's instrument set changes (e.g.
  // adding/removing a watchlist row) without tearing down the connection.
  useEffect(() => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const next = new Set(instrumentIds);
    for (const id of next) {
      if (!subscribedRef.current.has(id)) {
        ws.send(JSON.stringify({ action: "subscribe", instrument_id: id }));
      }
    }
    for (const id of subscribedRef.current) {
      if (!next.has(id)) {
        ws.send(JSON.stringify({ action: "unsubscribe", instrument_id: id }));
      }
    }
    subscribedRef.current = next;
  }, [instrumentIds]);
}
```

- [ ] **Step 2: Wire it into the dashboard**

In `web/app/page.tsx`, add the import:

```tsx
import { Tick, useTickStream } from "@/lib/useTickStream";
```

Add state for live prices and the "live" freshness clock, and the hook call, inside `WatchlistPage`:

```tsx
  const [liveData, setLiveData] = useState<Record<number, { price: number; lastTickAt: number }>>(
    {}
  );
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const handleTick = (tick: Tick) => {
    setLiveData((prev) => ({
      ...prev,
      [tick.instrument_id]: { price: tick.price, lastTickAt: Date.now() },
    }));
  };

  useTickStream(
    watchlist.map((w) => w.instrument_id),
    handleTick
  );
```

Replace the price cell:

```tsx
              <td id={`price-${row.instrument_id}`}>
                {(() => {
                  const live = liveData[row.instrument_id];
                  const price = live ? live.price : row.last_price;
                  const isLive = live !== undefined && now - live.lastTickAt < 90_000;
                  return (
                    <>
                      <span className={isLive ? "text-green-600" : ""}>
                        {price !== null && price !== undefined ? price : "--"}
                      </span>
                      <span className="text-gray-400 text-xs ml-2">
                        {isLive ? "live" : row.last_ts ? `as of ${row.last_ts}` : ""}
                      </span>
                    </>
                  );
                })()}
              </td>
```

- [ ] **Step 3: Manual verification**

With the backend running (gateway + Postgres + Redis) plus `uv run python -m trading.streaming.crypto_ingestor` and `uv run python -m trading.streaming.bar_aggregator` also running:

- Add `BTC-USDT` to the watchlist in the browser.
- Confirm its price updates within a few seconds and shows the green "live" label.
- Stop `crypto_ingestor`. After ~90 seconds, confirm the label switches to "as of `<timestamp>`" and the green color goes away.

- [ ] **Step 4: Commit**

```bash
git add web/lib/useTickStream.ts web/app/page.tsx
git commit -m "feat(web): live price updates via shared WS hook"
```

---

## Task 9: Chart page with timeframe selector (static history)

**Files:**
- Create: `web/app/instrument/[id]/page.tsx`

**Interfaces:**
- Consumes: `fetchCandles`, `Interval` (`web/lib/api.ts`).
- Produces: the `/instrument/[id]` route, rendering a candlestick chart for the five supported intervals.

- [ ] **Step 1: Write the page**

Create `web/app/instrument/[id]/page.tsx`:

```tsx
"use client";

import { useEffect, useRef, useState } from "react";
import { useParams } from "next/navigation";
import { IChartApi, ISeriesApi, UTCTimestamp, createChart } from "lightweight-charts";
import { Candle, Interval, fetchCandles } from "@/lib/api";

const INTERVALS: Interval[] = ["1m", "5m", "15m", "1h", "1d"];

function toChartCandle(c: Candle) {
  return {
    time: (new Date(c.ts).getTime() / 1000) as UTCTimestamp,
    open: c.open,
    high: c.high,
    low: c.low,
    close: c.close,
  };
}

export default function InstrumentChartPage() {
  const params = useParams<{ id: string }>();
  const instrumentId = Number(params.id);

  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  const [interval, setInterval_] = useState<Interval>("1m");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 400,
    });
    const series = chart.addCandlestickSeries();
    chartRef.current = chart;
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetchCandles(instrumentId, interval)
      .then((res) => {
        if (cancelled || !seriesRef.current) return;
        seriesRef.current.setData(res.candles.map(toChartCandle));
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
  }, [instrumentId, interval]);

  return (
    <main className="p-8">
      <div className="mb-4 flex gap-2">
        {INTERVALS.map((iv) => (
          <button
            key={iv}
            className={`px-3 py-1 rounded border ${iv === interval ? "bg-black text-white" : ""}`}
            onClick={() => setInterval_(iv)}
          >
            {iv}
          </button>
        ))}
      </div>
      {error && <p className="text-red-600 mb-4">{error}</p>}
      <div ref={containerRef} />
    </main>
  );
}
```

- [ ] **Step 2: Manual verification**

With the backend running and at least one instrument in `bars_intraday` (e.g. `bar_aggregator` has been running for a few minutes against `BTC-USDT`):

- From the dashboard, click a symbol to navigate to `/instrument/<id>`.
- Confirm the candlestick chart renders with real history.
- Click through all five timeframe buttons; confirm the chart re-renders each time with visibly different bucket granularity (fewer, wider candles on `1h`/`1d`).

- [ ] **Step 3: Commit**

```bash
git add web/app/instrument/
git commit -m "feat(web): candlestick chart page with timeframe selector"
```

---

## Task 10: Live tick → candle merge on the chart

**Files:**
- Create: `web/lib/candles.ts`
- Modify: `web/app/instrument/[id]/page.tsx`

**Interfaces:**
- Produces: `ChartCandle` type, `bucketStart(epochSeconds, interval) -> UTCTimestamp`, `mergeTickIntoCandles(candles, price, tickTimeSeconds, interval) -> ChartCandle[]`.

- [ ] **Step 1: Write the bucketing helper**

Create `web/lib/candles.ts`:

```typescript
import { UTCTimestamp } from "lightweight-charts";

export type ChartCandle = {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
};

const INTERVAL_SECONDS: Record<string, number> = {
  "1m": 60,
  "5m": 300,
  "15m": 900,
  "1h": 3600,
  "1d": 86400,
};

export function bucketStart(epochSeconds: number, interval: string): UTCTimestamp {
  const width = INTERVAL_SECONDS[interval];
  return (Math.floor(epochSeconds / width) * width) as UTCTimestamp;
}

// Ticks are bucketed client-side rather than pre-bucketed by the backend:
// if the tick falls in the already-open (rightmost) candle, that candle's
// H/L/C update in place; if it starts a new bucket, a new O=H=L=C=price
// candle is appended. This matches lightweight-charts' own series.update()
// semantics (same time = replace last bar, later time = append).
export function mergeTickIntoCandles(
  candles: ChartCandle[],
  price: number,
  tickTimeSeconds: number,
  interval: string
): ChartCandle[] {
  const bucket = bucketStart(tickTimeSeconds, interval);
  const last = candles[candles.length - 1];

  if (last && last.time === bucket) {
    const updated: ChartCandle = {
      ...last,
      high: Math.max(last.high, price),
      low: Math.min(last.low, price),
      close: price,
    };
    return [...candles.slice(0, -1), updated];
  }

  if (last && bucket < last.time) {
    // An out-of-order/late tick -- ignore rather than corrupt the chart's
    // rightmost bar with a timestamp that goes backwards.
    return candles;
  }

  const fresh: ChartCandle = { time: bucket, open: price, high: price, low: price, close: price };
  return [...candles, fresh];
}
```

- [ ] **Step 2: Wire it into the chart page**

In `web/app/instrument/[id]/page.tsx`, add imports:

```tsx
import { ChartCandle, mergeTickIntoCandles } from "@/lib/candles";
import { useTickStream } from "@/lib/useTickStream";
```

Add a ref to track the current candle array (mirrors what's on the chart, updated by both the REST fetch and live ticks) and update the REST-fetch effect to populate it:

```tsx
  const candlesRef = useRef<ChartCandle[]>([]);
```

In the existing candles-fetch `useEffect`, after `seriesRef.current.setData(...)`, add:

```tsx
        candlesRef.current = res.candles.map(toChartCandle);
```

Add the live-tick subscription, after that effect:

```tsx
  useTickStream([instrumentId], (tick) => {
    if (tick.instrument_id !== instrumentId || !seriesRef.current) return;
    const tickTimeSeconds = new Date(tick.ts).getTime() / 1000;
    const merged = mergeTickIntoCandles(candlesRef.current, tick.price, tickTimeSeconds, interval);
    candlesRef.current = merged;
    seriesRef.current.update(merged[merged.length - 1]);
  });
```

- [ ] **Step 3: Manual verification**

With `crypto_ingestor`/`bar_aggregator` running against `BTC-USDT`:

- Open `/instrument/<btc-id>` on the `1m` interval.
- Watch the rightmost candle's wick/close move live as ticks arrive, with no page reload.
- Wait past a real minute boundary; confirm a new candle appends to the right rather than the previous one continuing to update.
- Switch to `5m`; confirm history re-fetches for the new granularity and live updates resume correctly there too.

- [ ] **Step 4: Commit**

```bash
git add web/lib/candles.ts web/app/instrument/
git commit -m "feat(web): live tick to candle merge on the chart"
```

---

## Task 11: End-to-end verification (controller-run)

**Files:** none — this task runs the shipped stack and records evidence, the same evidentiary standard every prior live-verification task in this project has held itself to (`demo-proof.png`, the bar-aggregator live-accumulation check, the intraday backfill's verification queries).

Controller-run, not delegated — it requires a human eye on a real browser watching real live data, which a subagent cannot do unsupervised.

- [ ] **Step 1:** Bring up the full stack: `docker compose up -d`, `uv run alembic upgrade head`, then in separate terminals: `uvicorn trading.streaming.gateway:app --reload`, `uv run python -m trading.streaming.crypto_ingestor`, `uv run python -m trading.streaming.bar_aggregator`, and — if it's currently an NSE trading session — `uv run python -m trading.streaming.upstox_ingestor` (if outside market hours, skip this and instead use it to verify the "closed" badge state, which is itself valid coverage per this plan's design). Then `cd web && npm run dev`.
- [ ] **Step 2:** In the browser, add at least 2 crypto symbols and 1 NSE equity symbol to the watchlist. Confirm crypto prices tick live; confirm the equity row shows either a live price (market open) or a "last close as of ..." badge (market closed) — never a blank or fake-ticking price.
- [ ] **Step 3:** Open a crypto instrument's chart page. Confirm history renders and all five timeframe buttons work. Confirm the `1m` chart's rightmost candle updates live from ticks.
- [ ] **Step 4:** Take a screenshot of the working dashboard and a screenshot of the working chart page.
- [ ] **Step 5:** Record in this plan's Completion Notes (append a new section below): which symbols were used, whether the NSE market was open or closed during the run and which state was observed, the screenshot paths, and any issues found (fixed inline if small, or filed as a new discovered-live task the same way Tasks 8/9/10 were added to the intraday-backfill plan if not).
