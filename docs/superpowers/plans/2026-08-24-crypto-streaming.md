# Crypto Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the Phase 1 live-data pipeline shape end to end — Binance WebSocket trade ticks flow through a normalizing ingestor, fan out over Redis, and render live in a browser — before adding Upstox, persistence, or UI polish.

**Architecture:** Two processes coupled only by Redis pub/sub. `crypto_ingestor` connects to Binance's public combined trade stream, parses each trade into a typed `Tick`, resolves it to an `instrument_id`, and `PUBLISH`es it to `ticks:{instrument_id}`. `stream_gateway` (FastAPI) holds browser WebSocket connections; each connection opens its own Redis subscription for exactly the instruments that connection asked for, and drops it the moment that connection unsubscribes or disconnects — no shared connection-manager, no leaked subscriptions. A disposable static HTML page is the only consumer.

> **Amendment (post-ship, 2026-08-24):** the shipped `stream_gateway` does not do per-instrument `SUBSCRIBE`/`UNSUBSCRIBE` as described above — see Task 5's amendment note below for what shipped and why.



**Tech Stack:** Python 3.12 (via `uv`) · FastAPI · Starlette WebSockets · `redis` (asyncio client) · `websockets` (already a dependency) · Pydantic v2 · pytest

**Spec:** [`docs/superpowers/specs/2026-08-24-crypto-streaming-design.md`](../specs/2026-08-24-crypto-streaming-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** exactly, managed by `uv`.
- **Money is never a float.** `Tick.price`/`Tick.quantity` are `Decimal`.
- **Timestamps are timezone-aware UTC.** `Tick.ts` must carry `tzinfo`; a naive datetime is a bug, not a convenience.
- **No network in tests** except tests marked `@pytest.mark.live` (already registered in `pyproject.toml`, excluded from the default run).
- **No mocks for infrastructure.** Tests run against a real Redis (`trading_redis`, already in `docker-compose.yml`) exactly like `db_conn` already runs against a real, rolled-back Postgres transaction — never a fake/mock Redis client.
- **Tests stay synchronous**, calling `asyncio.run(...)` around the async code under test — the existing convention in `tests/recorder/test_upstox_ws.py`. No new test-framework dependency (no `pytest-asyncio`).
- **A single malformed message is logged and skipped, never fatal** — the same rule `recorder/upstox_ws.py` already applies to a bad frame, extended here to a bad *parse*, not just a bad frame shape.
- **Nothing in this plan writes to TimescaleDB.** Persistence, bar aggregation, Upstox, and UI polish are out of scope per the design doc — don't add them even if a step would be easy.
- **Lint/type gate every task:** `ruff check . && ruff format --check . && mypy src` must pass before any commit.
- **Every task ends with a passing `pytest` run (default invocation, live tests excluded) and a commit.**

---

## File Structure

```
pyproject.toml                          + fastapi, uvicorn[standard], redis

src/trading/streaming/
  __init__.py
  models.py                             Tick
  seed_instruments.py                   CRYPTO_PAIRS, seed_crypto_instruments(), CLI
  binance_feed.py                       BinanceFeed protocol, LiveBinanceFeed, parse_trade_message()
  crypto_ingestor.py                    run_ingestion_loop(), CLI
  gateway.py                            FastAPI app, GET /, GET /instruments, WS /ws
  static/
    proof.html                          disposable browser proof page

tests/streaming/
  conftest.py                           redis_client fixture (real Redis)
  test_models.py
  test_seed_instruments.py
  test_binance_feed.py                  unit tests + one @pytest.mark.live shape check
  test_crypto_ingestor.py
  test_gateway.py
```

Dependency order: **Task 1** (Tick + deps) has no dependency on anything else in this plan. **Task 2** (seed) depends only on Task 1's package skeleton. **Task 3** (Binance feed/parser) depends only on Task 1's `Tick`. **Task 4** (ingestor loop) depends on Tasks 1 and 3. **Task 5** (gateway + proof page) depends on Tasks 1 and 2. **Task 6** (end-to-end verification) depends on everything.

```
Task 1 (Tick + deps)
  ├── Task 2 (seed) ──────────────┐
  ├── Task 3 (Binance feed) ── Task 4 (ingestor)
  └──────────────────────────────┴── Task 5 (gateway) ── Task 6 (e2e verification)
```

**AI-tier delegation:** every task below is well-specified enough for Sonnet-tier implementation — the architectural decisions (process split, Redis-as-only-coupling, per-connection subscription lifetime, no persistence) were already made and approved in the design doc, not left open here. Task 6 (manual end-to-end verification) is judgment-driven rather than test-driven and should be controller-run (Opus), the same way Phase 0's Task 17 backfill execution was.

---

## Task 1: `Tick` contract and project dependencies

**Files:**
- Modify: `pyproject.toml`
- Create: `src/trading/streaming/__init__.py`
- Create: `src/trading/streaming/models.py`
- Test: `tests/streaming/test_models.py`
- Test: `tests/streaming/__init__.py` (empty, makes the test package importable the same way `tests/recorder/` is)

**Interfaces:**
- Produces: `Tick` — `Tick(instrument_id: int, ts: datetime, price: Decimal, quantity: Decimal, side: str | None = None)`, a frozen Pydantic model with `.model_dump_json()` / `Tick.model_validate_json(...)` round-tripping. Every later task imports this from `trading.streaming.models`.

- [ ] **Step 1: Add dependencies**

Edit `pyproject.toml`'s `dependencies` list to add three entries (keep the rest of the file unchanged):

```toml
dependencies = [
    "polars>=1.0",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "psycopg[binary,pool]>=3.2",
    "httpx>=0.27",
    "alembic>=1.13",
    "sqlalchemy>=2.0",
    "structlog>=24.1",
    "websockets>=12.0",
    "fastapi>=0.110",
    "uvicorn[standard]>=0.30",
    "redis>=5.0",
]
```

Run: `uv sync`
Expected: dependencies install cleanly, `uv.lock` updates.

- [ ] **Step 2: Write the failing test**

Create `tests/streaming/__init__.py` (empty file).

Create `tests/streaming/test_models.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.streaming.models import Tick


def test_tick_round_trips_through_json() -> None:
    tick = Tick(
        instrument_id=42,
        ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        price=Decimal("65000.50"),
        quantity=Decimal("0.01"),
        side="buy",
    )

    restored = Tick.model_validate_json(tick.model_dump_json())

    assert restored.instrument_id == 42
    assert restored.price == Decimal("65000.50")
    assert restored.quantity == Decimal("0.01")
    assert restored.side == "buy"
    assert restored.ts == datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)


def test_tick_side_is_optional() -> None:
    tick = Tick(
        instrument_id=1,
        ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        price=Decimal("1.00"),
        quantity=Decimal("1.00"),
    )
    assert tick.side is None


def test_tick_rejects_a_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="tzinfo"):
        Tick(
            instrument_id=1,
            ts=datetime(2026, 8, 24, 12, 0, 0),  # no tzinfo
            price=Decimal("1.00"),
            quantity=Decimal("1.00"),
        )
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/streaming/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming'`

- [ ] **Step 4: Write minimal implementation**

Create `src/trading/streaming/__init__.py` (empty file).

Create `src/trading/streaming/models.py`:

```python
"""Contracts for the real-time streaming pipeline (Phase 1).

Distinct from `trading.contracts` (the six-stage EOD batch pipeline's
contracts): this is a live trade tick, not a canonical bar, and it never
touches TimescaleDB in this sub-project (see the design doc for why).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator


class Tick(BaseModel):
    """One trade, resolved to our instrument identity."""

    model_config = ConfigDict(frozen=True)

    instrument_id: int
    ts: datetime
    price: Decimal
    quantity: Decimal
    side: str | None = None

    @field_validator("ts")
    @classmethod
    def _ts_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Tick.ts must carry tzinfo (UTC in storage/wire format)")
        return value
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_models.py -v`
Expected: 3 passed

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean

```bash
git add pyproject.toml uv.lock src/trading/streaming/__init__.py src/trading/streaming/models.py \
        tests/streaming/__init__.py tests/streaming/test_models.py
git commit -m "feat(streaming): Tick contract and streaming package scaffold"
```

---

## Task 2: Crypto instrument seed

**Files:**
- Create: `src/trading/streaming/seed_instruments.py`
- Test: `tests/streaming/test_seed_instruments.py`

**Interfaces:**
- Consumes: `trading.contracts.InstrumentRef` (existing, `canonical_key` property), `trading.contracts.AssetClass` (existing, has `.CRYPTO`), `trading.config.get_settings()` (existing).
- Produces: `CRYPTO_PAIRS: tuple[str, ...]` — the fixed pair list, e.g. `("BTC-USDT", "ETH-USDT", "SOL-USDT")`. `seed_crypto_instruments(conn: Connection, pairs: Sequence[str] = CRYPTO_PAIRS) -> dict[str, int]` — idempotent, returns `{"BTC-USDT": 123, ...}`. Task 4 and Task 5 both call this.

Instruments get `exchange="BINANCE"`, `segment="SPOT"` (a new segment value — crypto spot pairs, distinct from the equity/derivative segments `CM`/`FO`/`MF` already in use), `asset_class="CRYPTO"`, `currency="USDT"`, `status="ACTIVE"`, `canonical_key` from `InstrumentRef.canonical_key` (no `series`/`expiry`/`strike`/`option_type` — a crypto spot pair has none of those). `tick_size` and `isin` stay `NULL`; nothing downstream in this plan reads them.

- [ ] **Step 1: Write the failing test**

Create `tests/streaming/test_seed_instruments.py`:

```python
from __future__ import annotations

import pytest

from trading.streaming.seed_instruments import CRYPTO_PAIRS, seed_crypto_instruments

pytestmark = pytest.mark.db


def test_seed_creates_one_instrument_per_pair(db_conn):
    result = seed_crypto_instruments(db_conn)

    assert set(result) == set(CRYPTO_PAIRS)
    for symbol, instrument_id in result.items():
        row = db_conn.execute(
            "SELECT asset_class, exchange, segment, symbol, currency, status "
            "FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        ).fetchone()
        assert row == ("CRYPTO", "BINANCE", "SPOT", symbol, "USDT", "ACTIVE")


def test_seed_is_idempotent(db_conn):
    first = seed_crypto_instruments(db_conn)
    second = seed_crypto_instruments(db_conn)

    assert first == second  # same instrument_ids, no duplicate rows

    count = db_conn.execute(
        "SELECT count(*) FROM instruments WHERE exchange = 'BINANCE'"
    ).fetchone()[0]
    assert count == len(CRYPTO_PAIRS)


def test_seed_accepts_a_custom_pair_list(db_conn):
    result = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])
    assert set(result) == {"BTC-USDT"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/streaming/test_seed_instruments.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.seed_instruments'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/seed_instruments.py`:

```python
"""CLI to seed the fixed crypto pair universe this streaming sub-project
watches, mirroring `trading.calendar.seed`'s directness for small, static
reference data.

Usage: uv run python -m trading.streaming.seed_instruments
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import AssetClass, InstrumentRef

CRYPTO_PAIRS: tuple[str, ...] = ("BTC-USDT", "ETH-USDT", "SOL-USDT")

_UPSERT = """
    INSERT INTO instruments
        (asset_class, exchange, segment, symbol, currency, status, canonical_key)
    VALUES (%s, 'BINANCE', 'SPOT', %s, 'USDT', 'ACTIVE', %s)
    ON CONFLICT (canonical_key) DO UPDATE SET updated_at = now()
    RETURNING instrument_id
"""


def seed_crypto_instruments(
    conn: Connection, pairs: Sequence[str] = CRYPTO_PAIRS
) -> dict[str, int]:
    """Idempotent upsert of CRYPTO/BINANCE/SPOT instrument rows.

    Returns `{"BTC-USDT": instrument_id, ...}`. Re-running with the same
    pair list returns the same instrument_ids every time.
    """
    result: dict[str, int] = {}
    for symbol in pairs:
        ref = InstrumentRef(exchange="BINANCE", segment="SPOT", symbol=symbol)
        row = conn.execute(
            _UPSERT, (AssetClass.CRYPTO.value, symbol, ref.canonical_key)
        ).fetchone()
        assert row is not None
        result[symbol] = int(row[0])
    return result


def main() -> None:
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        result = seed_crypto_instruments(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    for symbol, instrument_id in result.items():
        print(f"{symbol}: instrument_id={instrument_id}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_seed_instruments.py -v`
Expected: 3 passed

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/seed_instruments.py tests/streaming/test_seed_instruments.py
git commit -m "feat(streaming): seed the crypto instrument universe"
```

---

## Task 3: Binance feed protocol and trade-message parser

**Files:**
- Create: `src/trading/streaming/binance_feed.py`
- Test: `tests/streaming/test_binance_feed.py`

**Interfaces:**
- Consumes: `trading.streaming.models.Tick` (Task 1).
- Produces: `BinanceFeed` (Protocol: `async def connect(self) -> None`, `def __aiter__(self) -> AsyncIterator[str]`, `async def aclose(self) -> None`). `LiveBinanceFeed(pairs: Sequence[str])` — the real implementation. `parse_trade_message(raw: str, instrument_ids: dict[str, int]) -> Tick | None` — `instrument_ids` is keyed by **lowercase Binance symbol** (`"btcusdt"`), not our display symbol (`"BTC-USDT"`); Task 4 builds that mapping from Task 2's seed output. Returns `None` (never raises) for anything that isn't a trade event for a tracked symbol, or that doesn't parse.

Binance's public combined WebSocket stream needs no authentication and no runtime subscribe message — the pairs are baked into the connection URL itself (`wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade`), which is why `BinanceFeed` has no `authorize()`/`subscribe()` the way `recorder/upstox_ws.py`'s `UpstoxFeed` does — a deliberate, smaller protocol for a genuinely simpler handshake, not an oversight.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_binance_feed.py`:

```python
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest
from websockets.asyncio.client import connect as ws_connect

from trading.streaming.binance_feed import _stream_url, parse_trade_message


def test_stream_url_joins_multiple_pairs_lowercase_no_dash() -> None:
    url = _stream_url(["BTC-USDT", "ETH-USDT"])
    assert url == (
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
    )


def _envelope(**data_overrides: object) -> str:
    data = {
        "e": "trade",
        "s": "BTCUSDT",
        "p": "65000.50",
        "q": "0.01000000",
        "T": 1724500000000,
        "m": False,
    }
    data.update(data_overrides)
    return json.dumps({"stream": "btcusdt@trade", "data": data})


def test_parse_trade_message_builds_a_tick() -> None:
    tick = parse_trade_message(_envelope(), instrument_ids={"btcusdt": 501})

    assert tick is not None
    assert tick.instrument_id == 501
    assert tick.price == Decimal("65000.50")
    assert tick.quantity == Decimal("0.01000000")
    assert tick.side == "buy"  # m=False: taker bought
    assert tick.ts.year == 2024  # 1724500000000 ms


def test_parse_trade_message_maps_maker_flag_to_sell_side() -> None:
    tick = parse_trade_message(_envelope(m=True), instrument_ids={"btcusdt": 501})
    assert tick is not None
    assert tick.side == "sell"


def test_parse_trade_message_ignores_an_untracked_symbol() -> None:
    tick = parse_trade_message(_envelope(s="ETHUSDT"), instrument_ids={"btcusdt": 501})
    assert tick is None


def test_parse_trade_message_ignores_a_non_trade_event() -> None:
    tick = parse_trade_message(_envelope(e="aggTrade"), instrument_ids={"btcusdt": 501})
    assert tick is None


def test_parse_trade_message_returns_none_for_malformed_json() -> None:
    assert parse_trade_message("not json", instrument_ids={"btcusdt": 501}) is None


def test_parse_trade_message_returns_none_for_a_missing_field() -> None:
    raw = json.dumps({"stream": "btcusdt@trade", "data": {"e": "trade", "s": "BTCUSDT"}})
    assert parse_trade_message(raw, instrument_ids={"btcusdt": 501}) is None


@pytest.mark.live
def test_live_trade_message_matches_the_documented_shape() -> None:
    """One real message from Binance's public feed, shape-checked against
    what parse_trade_message expects. Excluded from the default run."""

    async def _probe() -> str:
        async with ws_connect(_stream_url(["BTC-USDT"])) as connection:
            return await asyncio.wait_for(connection.recv(), timeout=15)

    raw = asyncio.run(_probe())
    envelope = json.loads(raw)
    assert envelope["stream"] == "btcusdt@trade"
    data = envelope["data"]
    assert data["e"] == "trade"
    assert {"s", "p", "q", "T", "m"}.issubset(data)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/streaming/test_binance_feed.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.binance_feed'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/binance_feed.py`:

```python
"""Binance public trade-stream feed and parser.

Governing principle, same as `recorder/upstox_ws.py`: a single message we
can't interpret is logged and skipped, never fatal to the connection.
Unlike the recorder, this module parses live -- there is no raw-archive
leg in this sub-project (see the design doc for why).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

import structlog
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

from trading.streaming.models import Tick

log = structlog.get_logger(__name__)

_STREAM_BASE = "wss://stream.binance.com:9443/stream"


def _binance_symbol(pair: str) -> str:
    """'BTC-USDT' -> 'btcusdt' (Binance's wire symbol, no separator, lowercase)."""
    return pair.replace("-", "").lower()


def _stream_url(pairs: Sequence[str]) -> str:
    streams = "/".join(f"{_binance_symbol(p)}@trade" for p in pairs)
    return f"{_STREAM_BASE}?streams={streams}"


class BinanceFeed(Protocol):
    """One live connection to Binance's combined trade stream.

    No `authorize()`/`subscribe()` the way `UpstoxFeed` has them: Binance's
    public stream needs neither -- the pairs are chosen by the connection
    URL itself. Tests supply a fake that yields a scripted sequence of raw
    messages and, optionally, raises to simulate a dropped connection.
    """

    async def connect(self) -> None:
        """Open the connection. Raise on failure."""

    def __aiter__(self) -> AsyncIterator[str]:
        """Yield raw JSON message strings as they arrive. May raise to signal disconnect."""

    async def aclose(self) -> None:
        """Best-effort close; errors here must never propagate."""


def parse_trade_message(raw: str, instrument_ids: dict[str, int]) -> Tick | None:
    """Parse one combined-stream message into a `Tick`.

    `instrument_ids` is keyed by lowercase Binance symbol ("btcusdt"), not
    our display symbol. Returns None -- and logs why -- for anything that
    isn't a trade event for a tracked symbol, or that doesn't parse; never
    raises, so one bad message never kills the caller's loop.
    """
    try:
        envelope = json.loads(raw)
        data = envelope["data"]
        if data["e"] != "trade":
            return None
        binance_symbol = str(data["s"]).lower()
        instrument_id = instrument_ids.get(binance_symbol)
        if instrument_id is None:
            return None
        return Tick(
            instrument_id=instrument_id,
            ts=datetime.fromtimestamp(int(data["T"]) / 1000, tz=UTC),
            price=Decimal(data["p"]),
            quantity=Decimal(data["q"]),
            side="sell" if data["m"] else "buy",  # m: is the buyer the maker?
        )
    except (KeyError, TypeError, ValueError, InvalidOperation, json.JSONDecodeError) as exc:
        log.warning("binance_feed.malformed_message", reason=str(exc), raw=raw[:200])
        return None


class LiveBinanceFeed:
    """Real `BinanceFeed`: opens Binance's public combined trade stream.

    Does real network I/O and is therefore never exercised by the default
    test run (no network in tests, per the global constraints) -- only the
    single `@pytest.mark.live` test touches the real endpoint.
    """

    def __init__(self, pairs: Sequence[str]) -> None:
        self._pairs = pairs
        self._connection: ClientConnection | None = None

    async def connect(self) -> None:
        self._connection = await ws_connect(_stream_url(self._pairs))

    def __aiter__(self) -> AsyncIterator[str]:
        if self._connection is None:
            raise RuntimeError("iteration started before a successful connect()")
        return self._connection.__aiter__()

    async def aclose(self) -> None:
        if self._connection is not None:
            await self._connection.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_binance_feed.py -v`
Expected: 7 passed (the `@pytest.mark.live` test is excluded by the default `-m 'not live'` invocation).

Run once, separately, to actually verify the live shape (requires network access to `stream.binance.com`; if this environment can't reach it, note that in the task's completion report rather than skipping silently — same discipline Phase 0 used for NSE's anti-bot wall):
`uv run pytest tests/streaming/test_binance_feed.py -v -m live`
Expected: 1 passed, confirming the real feed matches the shape `parse_trade_message` assumes.

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/binance_feed.py tests/streaming/test_binance_feed.py
git commit -m "feat(streaming): Binance feed protocol and trade-message parser"
```

---

## Task 4: `crypto_ingestor` — the ingestion loop

**Files:**
- Create: `src/trading/streaming/crypto_ingestor.py`
- Create: `tests/streaming/conftest.py`
- Test: `tests/streaming/test_crypto_ingestor.py`

**Interfaces:**
- Consumes: `trading.streaming.binance_feed.BinanceFeed` / `LiveBinanceFeed` / `parse_trade_message` (Task 3), `trading.streaming.seed_instruments.seed_crypto_instruments` (Task 2), `trading.config.get_settings()` (existing).
- Produces: `run_ingestion_loop(redis, feed_factory, *, instrument_ids, initial_backoff_seconds=1.0, max_backoff_seconds=30.0, sleep=_default_sleep, max_ticks=None) -> None` (async). `max_ticks` is a test-only seam: `None` runs forever (production), an `int` stops after publishing that many ticks — crypto markets have no natural session close the way Upstox's recorder has, so this replaces that plan's `until`/`clock` pair with the closest equivalent. Every published tick goes to Redis channel `f"ticks:{tick.instrument_id}"` as `tick.model_dump_json()`.

- [ ] **Step 1: Add the shared Redis test fixture**

Create `tests/streaming/conftest.py`:

```python
"""Shared fixtures for the streaming test suite. Redis is real
(docker-compose's `trading_redis`), never mocked -- the same convention
`db_conn` already uses for Postgres."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis

from trading.config import get_settings


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        yield client
    finally:
        client.close()
```

- [ ] **Step 2: Write the failing tests**

Create `tests/streaming/test_crypto_ingestor.py`:

```python
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.crypto_ingestor import run_ingestion_loop


class ScriptedFeed:
    """A fake `BinanceFeed`: yields scripted raw messages, then optionally fails."""

    def __init__(
        self, messages: Sequence[str], *, fail_after: BaseException | None = None
    ) -> None:
        self.messages = list(messages)
        self.fail_after = fail_after
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def __aiter__(self) -> AsyncIterator[str]:
        for message in self.messages:
            yield message
        if self.fail_after is not None:
            raise self.fail_after

    async def aclose(self) -> None:
        self.closed = True


async def _no_sleep(seconds: float) -> None:
    return None


def _trade(symbol: str = "BTCUSDT", price: str = "65000.50") -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@trade",
            "data": {
                "e": "trade",
                "s": symbol,
                "p": price,
                "q": "0.01000000",
                "T": 1724500000000,
                "m": False,
            },
        }
    )


def test_run_ingestion_loop_publishes_parsed_ticks_to_redis(redis_client: redis.Redis) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)  # discard the subscribe confirmation

    feed = ScriptedFeed([_trade()])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    message = pubsub.get_message(timeout=2)
    assert message is not None and message["type"] == "message"
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501
    assert Decimal(str(payload["price"])) == Decimal("65000.50")


def test_run_ingestion_loop_skips_a_malformed_message_and_keeps_going(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedFeed(["not json", _trade()])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    # Only the second (valid) message ever reached Redis.
    message = pubsub.get_message(timeout=2)
    assert message is not None
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501


def test_run_ingestion_loop_reconnects_after_a_dropped_connection(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    failing_feed = ScriptedFeed([], fail_after=ConnectionError("dropped"))
    working_feed = ScriptedFeed([_trade()])
    feeds = iter([failing_feed, working_feed])

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: next(feeds),
                instrument_ids={"btcusdt": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert failing_feed.closed
    message = pubsub.get_message(timeout=2)
    assert message is not None
    payload = json.loads(message["data"])
    assert payload["instrument_id"] == 501
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/streaming/test_crypto_ingestor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.crypto_ingestor'`

- [ ] **Step 4: Write minimal implementation**

Create `src/trading/streaming/crypto_ingestor.py`:

```python
"""Entry point: `python -m trading.streaming.crypto_ingestor`.

Streams Binance trades, parses each into a Tick, and publishes it to
Redis. Reconnects with exponential backoff on any failure; a single
malformed message is logged and skipped, never fatal (same rule
`recorder/upstox_ws.py` applies to a bad frame).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import structlog
from redis.asyncio import Redis

from trading.config import get_settings
from trading.streaming.binance_feed import BinanceFeed, LiveBinanceFeed, parse_trade_message
from trading.streaming.seed_instruments import seed_crypto_instruments

log = structlog.get_logger(__name__)

FeedFactory = Callable[[], BinanceFeed]
Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def run_ingestion_loop(
    redis: Redis,
    feed_factory: FeedFactory,
    *,
    instrument_ids: dict[str, int],
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
    sleep: Sleeper = _default_sleep,
    max_ticks: int | None = None,
) -> None:
    """Stream ticks from `feed_factory()`, PUBLISHing each to Redis.

    Runs forever when `max_ticks` is None (production). Stops after
    publishing `max_ticks` ticks when it's an int -- a test seam standing
    in for the natural session-close boundary `recorder/upstox_ws.py`'s
    loop has and this one doesn't (crypto markets never close).
    """
    backoff = initial_backoff_seconds
    published = 0

    while max_ticks is None or published < max_ticks:
        feed = feed_factory()
        try:
            await feed.connect()
            backoff = initial_backoff_seconds

            async for raw in feed:
                tick = parse_trade_message(raw, instrument_ids)
                if tick is None:
                    continue
                try:
                    await redis.publish(f"ticks:{tick.instrument_id}", tick.model_dump_json())
                    published += 1
                except Exception as exc:  # noqa: BLE001 - a publish failure must not kill the socket
                    log.warning("crypto_ingestor.publish_failed", reason=str(exc))
                if max_ticks is not None and published >= max_ticks:
                    break
        except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect, not a crash
            log.warning("crypto_ingestor.disconnected", reason=str(exc))
            wait = min(backoff, max_backoff_seconds)
            await sleep(wait)
            backoff = min(backoff * 2, max_backoff_seconds)
        finally:
            try:
                await feed.aclose()
            except Exception:  # noqa: BLE001 - closing must never itself crash the loop
                log.debug("crypto_ingestor.close_failed", exc_info=True)


def main() -> None:
    settings = get_settings()

    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        symbol_to_id = seed_crypto_instruments(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    instrument_ids = {symbol.replace("-", "").lower(): iid for symbol, iid in symbol_to_id.items()}
    log.info("crypto_ingestor.starting", pairs=list(symbol_to_id))

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                redis, lambda: LiveBinanceFeed(list(symbol_to_id)), instrument_ids=instrument_ids
            )
        )
    except KeyboardInterrupt:
        log.info("crypto_ingestor.interrupted")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_crypto_ingestor.py -v`
Expected: 3 passed

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/crypto_ingestor.py tests/streaming/conftest.py \
        tests/streaming/test_crypto_ingestor.py
git commit -m "feat(streaming): crypto_ingestor loop publishing ticks to Redis"
```

---

## Task 5: `stream_gateway` and the proof page

**Files:**
- Create: `src/trading/streaming/gateway.py`
- Create: `src/trading/streaming/static/proof.html`
- Test: `tests/streaming/test_gateway.py`

**Interfaces:**
- Consumes: `trading.streaming.seed_instruments.seed_crypto_instruments`, `CRYPTO_PAIRS` (Task 2), `trading.config.get_settings()` (existing).
- Produces: `app` — the FastAPI instance (importable as `trading.streaming.gateway:app`, run via `uvicorn trading.streaming.gateway:app`). `get_db_connection() -> Iterator[Connection]` — a FastAPI dependency opening a real `psycopg` connection (committed on success, rolled back on error); tests override it with `app.dependency_overrides[get_db_connection] = lambda: db_conn` so `/instruments` reads through the *same* rolled-back test transaction instead of a second, separately-committed connection (this repo's `db_conn` fixture guarantees nothing a test does is ever persisted — a raw `db_conn.commit()` inside a test would break that for every later test run). `GET /` serves the proof page. `GET /instruments` returns `{"BTC-USDT": 123, ...}` (the current seed) as JSON — the proof page needs this because `instrument_id`s are database-assigned and can't be hardcoded into a static file. `WS /ws` accepts `{"action": "subscribe"|"unsubscribe", "instrument_id": int}` and forwards every `Tick` published to `ticks:{instrument_id}` verbatim (as the JSON text `crypto_ingestor` published) to that connection.

Each WebSocket connection owns its own Redis `pubsub()` object and subscribes only to the instruments *that connection* has asked for; the subscription for a given instrument is dropped the moment that connection unsubscribes or disconnects. This satisfies "no leaked subscriptions" at the connection level without needing a shared, refcounted subscription manager across every browser client — Redis handles many independent subscribers to the same channel cheaply, and a shared manager is unnecessary complexity for this sub-project's scope (YAGNI; revisit only if profiling ever shows per-connection Redis subscriptions are a real cost at scale).

> **Amendment (post-ship, 2026-08-24):** this section describes per-instrument `SUBSCRIBE`/`UNSUBSCRIBE`, held only while at least one connected client wants it. That is not what shipped. During Task 5's fix loop, a controller-approved ruling replaced it with one connection-lifetime `psubscribe("ticks:*")` per WebSocket connection plus in-process filtering against a local `subscribed: set[int]`. Reason: redis-py's `PubSub.subscribe()`/`unsubscribe()` are fire-and-forget — they write the command and return without waiting for Redis's confirmation — which raced against the test harness's WebSocket client (whose `send_json()` returns as soon as the message is queued for the ASGI app, not once the app has processed it) and against a real, separately-connected test publisher. A per-instrument design could lose ticks published before the matching `SUBSCRIBE` had actually reached Redis. The `psubscribe` redesign removes the race structurally instead of narrowing its window. This trades away only the literal per-channel-selectivity mechanism (which Redis command(s) are issued) — the connection-scoped-subscription design goal (each WebSocket owns its own subscription lifetime, torn down on disconnect, no shared manager) is unchanged and still holds. Full reasoning: `.superpowers/sdd/2026-08-24-crypto-streaming/progress.md`, Task 5 entries.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_gateway.py`:

```python
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


def test_instruments_endpoint_lists_the_seeded_pairs(client: TestClient, seeded_instrument_id: int) -> None:
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/streaming/test_gateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.gateway'`

- [ ] **Step 3: Write the proof page**

Create `src/trading/streaming/static/proof.html`:

```html
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Crypto stream proof</title>
  <style>
    body { font-family: monospace; padding: 2rem; }
    .price { font-size: 2rem; }
    .up { color: green; }
    .down { color: red; }
  </style>
</head>
<body>
  <h1>Live crypto ticks (Binance)</h1>
  <div id="rows"></div>
  <script>
    const rows = document.getElementById("rows");
    const lastPrice = {};

    async function main() {
      const instruments = await (await fetch("/instruments")).json();
      const idToSymbol = {};
      for (const [symbol, instrumentId] of Object.entries(instruments)) {
        idToSymbol[instrumentId] = symbol;
        const row = document.createElement("div");
        row.id = "row-" + instrumentId;
        row.innerHTML = symbol + ": <span class='price'>waiting...</span>";
        rows.appendChild(row);
      }

      const ws = new WebSocket("ws://" + location.host + "/ws");
      ws.onopen = () => {
        for (const instrumentId of Object.keys(idToSymbol)) {
          ws.send(JSON.stringify({ action: "subscribe", instrument_id: Number(instrumentId) }));
        }
      };
      ws.onmessage = (event) => {
        const tick = JSON.parse(event.data);
        const symbol = idToSymbol[tick.instrument_id];
        const row = document.getElementById("row-" + tick.instrument_id);
        const priceEl = row.querySelector(".price");
        const price = Number(tick.price);
        const prev = lastPrice[tick.instrument_id];
        priceEl.textContent = tick.price;
        priceEl.className = "price " + (prev === undefined ? "" : price >= prev ? "up" : "down");
        lastPrice[tick.instrument_id] = price;
      };
    }

    main();
  </script>
</body>
</html>
```

- [ ] **Step 4: Write minimal implementation**

Create `src/trading/streaming/gateway.py`:

```python
"""Entry point: `uvicorn trading.streaming.gateway:app`.

Holds browser WebSocket connections and fans out ticks published by
crypto_ingestor to whichever instruments each connection has asked for.
Each connection owns its own Redis subscription lifetime -- see the plan's
Task 5 for why that's the right scope, not a shared connection manager.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import psycopg
import structlog
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from psycopg import Connection
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
from trading.streaming.seed_instruments import seed_crypto_instruments

log = structlog.get_logger(__name__)

app = FastAPI()

_STATIC_ROOT = Path(__file__).parent / "static"


def get_db_connection() -> Iterator[Connection]:
    """A real connection per request. Tests override this dependency with
    their own `db_conn` fixture (`app.dependency_overrides[get_db_connection]
    = lambda: db_conn`) so `/instruments` reads inside the same rolled-back
    test transaction instead of committing a second, real connection."""
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_ROOT / "proof.html")


@app.get("/instruments")
async def instruments(conn: Connection = Depends(get_db_connection)) -> dict[str, int]:
    # psycopg here is a synchronous, blocking call inside an async route --
    # an accepted simplification for this endpoint (called once per page
    # load, not a hot path); see the design doc's scope notes.
    return seed_crypto_instruments(conn)


async def _forward_loop(pubsub: PubSub, websocket: WebSocket) -> None:
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        await websocket.send_text(message["data"])


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    redis: Redis = Redis.from_url(get_settings().redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    subscribed: set[int] = set()
    listener = asyncio.create_task(_forward_loop(pubsub, websocket))

    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")
            instrument_id = message.get("instrument_id")
            if instrument_id is None:
                continue
            channel = f"ticks:{instrument_id}"
            if action == "subscribe" and instrument_id not in subscribed:
                await pubsub.subscribe(channel)
                subscribed.add(instrument_id)
            elif action == "unsubscribe" and instrument_id in subscribed:
                await pubsub.unsubscribe(channel)
                subscribed.discard(instrument_id)
    except WebSocketDisconnect:
        pass
    finally:
        listener.cancel()
        try:
            await pubsub.unsubscribe()
            await pubsub.close()
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the handler
            log.debug("gateway.pubsub_cleanup_failed", exc_info=True)
        await redis.aclose()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_gateway.py -v`
Expected: 4 passed

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/gateway.py src/trading/streaming/static/proof.html \
        tests/streaming/test_gateway.py
git commit -m "feat(streaming): stream_gateway WebSocket service and proof page"
```

---

## Task 6: End-to-end manual verification

**Files:** none (verification only — nothing here should require a code change; if it does, that's a signal a step above missed something, not a step to improvise around).

This is the design doc's actual success criterion, and it's inherently a manual/visual check — nothing in Tasks 1-5 unit-tests "does a human watching a browser see live numbers." Controller-run (Opus), not delegated: judging "does this look right" against a live feed is a judgment call, not a scripted assertion.

- [ ] **Step 1: Confirm infrastructure is up**

Run: `docker compose ps`
Expected: `trading_tsdb` and `trading_redis` both healthy. If not: `docker compose up -d`.

- [ ] **Step 2: Seed the crypto instruments**

Run: `uv run python -m trading.streaming.seed_instruments`
Expected: three lines, `BTC-USDT: instrument_id=N`, `ETH-USDT: ...`, `SOL-USDT: ...`.

- [ ] **Step 3: Start the ingestor**

Run (separate terminal, leave running): `uv run python -m trading.streaming.crypto_ingestor`
Expected: `crypto_ingestor.starting` log line with the three pairs, then silence (ticks are being published, not logged individually).

- [ ] **Step 4: Start the gateway**

Run (separate terminal, leave running): `uv run uvicorn trading.streaming.gateway:app --port 8000`
Expected: uvicorn's normal startup log, "Application startup complete."

- [ ] **Step 5: Open the proof page and watch it**

Open `http://localhost:8000/` in a browser. Watch for at least 30 seconds.

Expected: all three pairs listed, each showing a price that updates (turning green on an uptick, red on a downtick) as real trades happen on Binance. If a pair's price never updates within a minute, that pair is genuinely low-frequency at that moment (rare for BTC/ETH; possible for a slower pair) — don't treat that alone as a failure, but if *none* of the three ever update, something upstream is broken; check the ingestor's terminal for `crypto_ingestor.disconnected`/`publish_failed` warnings first.

- [ ] **Step 6: Confirm the report**

Record in the task's completion notes: which pairs were observed updating, over what wall-clock window, and paste 2-3 example tick values with their timestamps as evidence — the same standard Phase 0's Task 17 report held itself to for live verification.

- [ ] **Step 7: Stop the processes**

Ctrl-C both the ingestor and uvicorn. No commit for this task — it verifies Tasks 1-5's commits, it doesn't add its own.
