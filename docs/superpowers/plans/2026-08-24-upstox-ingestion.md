# Upstox Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream real NSE equity trades from Upstox's V3 market-data feed into the same `ticks:{instrument_id}` Redis channels `crypto_ingestor` already publishes to, proving the "same pipeline, different feed adapter" claim — `bar_aggregator` and `stream_gateway` need zero changes to serve real Indian equity data.

**Architecture:** A new worker process, `upstox_ingestor`, reuses Phase 0's already-built `LiveUpstoxFeed` (auth + subscribe + frame iteration, `trading.recorder.upstox_ws`) and adds the one thing that module never needed: decoding Upstox's binary protobuf frames into `Tick`s. A one-time-generated, committed protobuf binding (`grpc_tools.protoc`, not a system `protoc` dependency) backs the parser.

**Tech Stack:** Python 3.12 (via `uv`) · `protobuf` (generated bindings) · `httpx`/`websockets` (via the existing `LiveUpstoxFeed`) · `redis` (asyncio client) · Pydantic v2 (reuses `trading.streaming.models.Tick`) · pytest

**Spec:** [`docs/superpowers/specs/2026-08-24-upstox-ingestion-design.md`](../specs/2026-08-24-upstox-ingestion-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** exactly, managed by `uv`.
- **Money is never a float.** `Tick.price`/`Tick.quantity` are `Decimal` — protobuf's `double` fields for `ltp`/`ltq` must be converted via `Decimal(str(...))`, never `Decimal(float_value)` directly (which would bake in binary-float imprecision).
- **Timestamps are timezone-aware UTC.** Upstox's `ltt` (last traded time) is epoch milliseconds; convert via `datetime.fromtimestamp(ltt / 1000, tz=UTC)`, same pattern `binance_feed.py` already uses for `T`.
- **No network in tests** except tests marked `@pytest.mark.live` (already registered in `pyproject.toml`, excluded from the default run).
- **No mocks for infrastructure.** Any Redis-touching test runs against a real Redis (`trading_redis`), any Postgres-touching test runs against real Postgres (`db_conn`) — never a fake client.
- **Tests stay synchronous**, calling `asyncio.run(...)` around async code under test — the convention every prior streaming task in this repo already follows.
- **A single malformed/unparseable frame is logged and skipped, never fatal** — same rule `crypto_ingestor`/`binance_feed.py` already apply.
- **Reconnect backoff resets only after a real message is consumed, not merely after `connect()`/`authorize()` succeeds** — this exact bug was found and fixed in `crypto_ingestor.run_ingestion_loop` earlier this session (a connect-then-immediately-drop failure mode reconnecting at a flat interval forever, risking an IP/rate-limit ban); apply the same `consumed_any` pattern here from the start rather than rediscovering it.
- **The generated protobuf module is committed to the repo, not regenerated on every build.** `grpcio-tools` is a dev-only dependency (regenerating after a schema change), never a runtime dependency of `upstox_ingestor` itself.
- **NSE market hours are 9:15–15:30 IST, not 24/7.** No scheduling logic is added in this plan — `upstox_ingestor` is started and stopped manually, exactly like `crypto_ingestor`. Task 5 (live verification) can only run while NSE is actually open.
- **`instruments.series` disambiguates same-symbol rows** (e.g. `RELIANCE` has both `series='EQ'` — the tradable equity line — and `series='BL'` — block-deal reporting, a different row with the same ISIN). Every watchlist query in this plan filters on `series = 'EQ'` explicitly; omitting it silently matches more than one row per symbol.
- **Lint/type gate every task:** `ruff check . && ruff format --check . && mypy src` must pass before any commit.
- **Every task ends with a passing `pytest` run (default invocation, live tests excluded) and a commit.**

---

## File Structure

```
pyproject.toml                                    + protobuf (runtime), grpcio-tools (dev group)

src/trading/streaming/upstox_proto/
  __init__.py
  MarketDataFeed.proto                             vendored schema (verbatim from Upstox)
  MarketDataFeed_pb2.py                             generated bindings (committed, not regenerated per-build)

src/trading/config.py                              + Settings.upstox_analytics_token

src/trading/streaming/
  seed_upstox_instruments.py                        UPSTOX_WATCHLIST, seed_upstox_instrument_keys()
  upstox_feed.py                                    parse_upstox_frame()
  upstox_ingestor.py                                run_ingestion_loop(), CLI

tests/streaming/
  test_upstox_proto.py                              generated-bindings round-trip sanity check
  test_seed_upstox_instruments.py
  test_upstox_feed.py
  test_upstox_ingestor.py
```

Dependency order: **Task 1** (proto + deps) has no dependency on anything else in this plan. **Task 2** (Settings + instrument seeding) depends only on existing `instruments` rows (Phase 0). **Task 3** (parser) depends on Task 1's generated bindings and the existing `Tick` model. **Task 4** (ingestor loop) depends on Tasks 2 and 3, and reuses Phase 0's existing `trading.recorder.upstox_ws.LiveUpstoxFeed`. **Task 5** (live verification) depends on everything, and additionally on NSE market hours being open.

```
Task 1 (proto + deps)
  ├── Task 3 (parser) ──────────────┐
Task 2 (Settings + instrument seed) ─┴── Task 4 (ingestor) ── Task 5 (live e2e, market hours only)
```

**AI-tier delegation:** Task 1 is mostly transcription (vendoring a fetched, verified-working schema) but is the first time this dependency type enters the repo — standard tier. Task 2 has a real correctness subtlety (the `series='EQ'` disambiguation) — standard tier. Task 3 has real judgment (protobuf `oneof`/map handling, a structurally different one-frame-many-instruments shape than Binance's one-message-one-trade) — standard tier. Task 4 reuses proven code but must apply an already-learned lesson correctly — standard tier. Task 5 is judgment-driven, time-gated, real-token verification — controller-run (Opus), same as every prior plan's live-verification task.

---

## Task 1: Vendor the protobuf schema, generate bindings, add dependencies

**Files:**
- Modify: `pyproject.toml`
- Create: `src/trading/streaming/upstox_proto/__init__.py`
- Create: `src/trading/streaming/upstox_proto/MarketDataFeed.proto`
- Create: `src/trading/streaming/upstox_proto/MarketDataFeed_pb2.py` (generated, then committed)
- Test: `tests/streaming/test_upstox_proto.py`

**Interfaces:**
- Produces: the generated module `trading.streaming.upstox_proto.MarketDataFeed_pb2`, exposing `FeedResponse`, `Feed`, `LTPC`, `Type` (enum with `initial_feed = 0`, `live_feed = 1`), and the other message types from the vendored schema. Task 3's parser imports `FeedResponse` from here.

This schema was fetched and verified working end-to-end in this session (compiled, serialized, and round-tripped a synthetic message successfully) — it is Upstox's real, current V3 `MarketDataFeed.proto`, not a guess.

- [ ] **Step 1: Add dependencies**

Edit `pyproject.toml`'s `dependencies` list to add `protobuf` (keep every other entry unchanged):

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
    "protobuf>=5.0",
]
```

And add `grpcio-tools` and `types-protobuf` to the dev group (keep every other entry unchanged):

```toml
[dependency-groups]
dev = ["pytest>=8.2", "pytest-cov>=5.0", "ruff>=0.5", "mypy>=1.10", "grpcio-tools>=1.60", "types-protobuf>=5.0"]
```

Run: `uv sync`
Expected: dependencies install cleanly, `uv.lock` updates.

- [ ] **Step 2: Vendor the schema**

Create `src/trading/streaming/upstox_proto/__init__.py` (empty file).

Create `src/trading/streaming/upstox_proto/MarketDataFeed.proto` with this exact content (Upstox's real, published V3 schema):

```protobuf
syntax = "proto3";
package com.upstox.marketdatafeeder.rpc.proto;

message LTPC {
  double ltp = 1;
  int64 ltt = 2;
  int64 ltq = 3;
  double cp = 4; //close price
}

message MarketLevel {
  repeated Quote bidAskQuote = 1;
  int64 lut = 2;
}

message MarketOHLC {
  repeated OHLC ohlc = 1;
}

message Quote {
  int32 bq = 1; //bid quantity
  double bp = 2; //bid price
  int32 bno = 3; //bid number of orders
  int32 aq = 4; // ask quantity
  double ap = 5; // ask price
  int32 ano = 6; // ask number of orders
  int64 bidQ = 7; //bid quantity
  int64 askQ = 8; // ask quantity
}

message OptionGreeks {
  double op = 1; // option price
  double up = 2; //underlying price
  double iv = 3; // implied volatility
  double delta = 4;
  double theta = 5;
  double gamma = 6;
  double vega = 7;
  double rho = 8;
}

message ExtendedFeedDetails {
  double atp = 1; //avg traded price
  double cp = 2; //close price
  int64 vtt = 3; //volume traded today
  double oi = 4; //open interest
  double changeOi = 5; //change oi
  double lastClose = 6;
  double tbq = 7; //total buy quantity
  double tsq = 8; //total sell quantity
  double close = 9;
  double lc = 10; //lower circuit
  double uc = 11; //upper circuit
  double yh = 12; //yearly high
  double yl = 13; //yearly low
  double fp = 14; //fill price
  int32 fv = 15; //fill volume
  int64 mbpBuy = 16; //mbp buy
  int64 mbpSell = 17; //mbp sell
  int64 tv = 18; //traded volume
  double dhoi = 19; //day high open interest
  double dloi = 20; //day low open interest
  double sp = 21; //spot price
  double poi = 22; //previous open interest
}

message OHLC {
  string interval = 1;
  double open = 2;
  double high = 3;
  double low = 4;
  double close = 5;
  int32 volume = 6;
  int64 ts = 7;
  int64 vol = 9;
}

enum Type {
  initial_feed = 0;
  live_feed = 1;
}

message MarketFullFeed {
  LTPC ltpc = 1;
  MarketLevel marketLevel = 2;
  OptionGreeks optionGreeks = 3;
  MarketOHLC marketOHLC = 4;
  ExtendedFeedDetails eFeedDetails = 5;
}

message IndexFullFeed {
  LTPC ltpc = 1;
  MarketOHLC marketOHLC = 2;
  double lastClose = 3;
  double yh = 4; //yearly high
  double yl = 5; //yearly low
}

message FullFeed {
  oneof FullFeedUnion {
    MarketFullFeed marketFF = 1;
    IndexFullFeed indexFF = 2;
  }
}

message OptionChain {
  LTPC ltpc = 1;
  Quote bidAskQuote = 2;
  OptionGreeks optionGreeks = 3;
  ExtendedFeedDetails eFeedDetails = 4;
}

message Feed {
  oneof FeedUnion {
    LTPC ltpc = 1;
    FullFeed ff = 2;
    OptionChain oc = 3;
  }
}

message FeedResponse {
  Type type = 1;
  map<string, Feed> feeds = 2;
  int64 currentTs = 3;
}
```

- [ ] **Step 3: Generate the bindings**

Run:
```bash
uv run python -m grpc_tools.protoc \
    -Isrc/trading/streaming/upstox_proto \
    --python_out=src/trading/streaming/upstox_proto \
    src/trading/streaming/upstox_proto/MarketDataFeed.proto
```
Expected: `src/trading/streaming/upstox_proto/MarketDataFeed_pb2.py` is created (a generated file — do not hand-edit it; regenerate with the same command if the `.proto` ever changes).

- [ ] **Step 4: Write the failing test**

Create `tests/streaming/test_upstox_proto.py`:

```python
from __future__ import annotations

from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


def test_feed_response_round_trips_an_ltpc_message() -> None:
    response = pb.FeedResponse()
    response.type = pb.live_feed
    response.currentTs = 1724500000000
    feed = pb.Feed()
    feed.ltpc.ltp = 65000.50
    feed.ltpc.ltt = 1724500000123
    feed.ltpc.ltq = 10
    response.feeds["NSE_EQ|INE002A01018"].CopyFrom(feed)

    raw = response.SerializeToString()
    restored = pb.FeedResponse()
    restored.ParseFromString(raw)

    assert restored.type == pb.live_feed
    assert restored.currentTs == 1724500000000
    entry = restored.feeds["NSE_EQ|INE002A01018"]
    assert entry.WhichOneof("FeedUnion") == "ltpc"
    assert entry.ltpc.ltp == 65000.50
    assert entry.ltpc.ltt == 1724500000123
    assert entry.ltpc.ltq == 10


def test_feed_response_reports_no_feed_type_for_an_empty_entry() -> None:
    response = pb.FeedResponse()
    response.feeds["NSE_EQ|UNSET"].CopyFrom(pb.Feed())

    assert response.feeds["NSE_EQ|UNSET"].WhichOneof("FeedUnion") is None
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/streaming/test_upstox_proto.py -v`
Expected: 2 passed (this schema was already verified working interactively in this session — this test locks that in as a regression guard, not exploratory debugging).

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean — `types-protobuf` (added above) supplies the stubs `mypy --strict` needs for the generated `_pb2.py` file; no per-file override needed.

```bash
git add pyproject.toml uv.lock src/trading/streaming/upstox_proto/ tests/streaming/test_upstox_proto.py
git commit -m "feat(streaming): vendor Upstox's MarketDataFeed protobuf schema and generated bindings"
```

---

## Task 2: `Settings.upstox_analytics_token` and watchlist instrument-key seeding

**Files:**
- Modify: `src/trading/config.py`
- Create: `src/trading/streaming/seed_upstox_instruments.py`
- Test: `tests/streaming/test_seed_upstox_instruments.py`

**Interfaces:**
- Consumes: existing `instruments` table rows (Phase 0 backfill; already has real ISINs for NSE equities).
- Produces: `Settings.upstox_analytics_token: str | None` (from `UPSTOX_ANALYTICS_TOKEN`). `UPSTOX_WATCHLIST: tuple[str, ...]` — the fixed symbol list. `seed_upstox_instrument_keys(conn: Connection, symbols: Sequence[str] = UPSTOX_WATCHLIST) -> dict[str, int]` — returns `{"NSE_EQ|INE002A01018": 58607, ...}` keyed by the derived Upstox instrument_key, valued by this codebase's existing `instrument_id` for that row. Idempotent (re-running produces the same mapping). Task 4's ingestor calls this to build its `instrument_ids` map.

`RELIANCE`/`TCS`/`INFY`/`HDFCBANK`/`ICICIBANK` all already exist in `instruments` as `exchange='NSE', segment='CM', asset_class='EQUITY'` rows, each with a real ISIN — but each symbol has **more than one row** (e.g. `RELIANCE` has both `series='EQ'`, the tradable line, and `series='BL'`, block-deal reporting — both share the same ISIN). Every query in this task filters `series = 'EQ'` explicitly; watchlist symbols not found with `series='EQ'` are a hard error, not silently skipped, since a wrong or missing instrument mapping is a correctness bug this plan should surface immediately rather than mask.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_seed_upstox_instruments.py`:

```python
from __future__ import annotations

import pytest

from trading.streaming.seed_upstox_instruments import (
    UPSTOX_WATCHLIST,
    seed_upstox_instrument_keys,
)

pytestmark = pytest.mark.db


def test_seed_maps_every_watchlist_symbol_to_its_eq_series_instrument(db_conn):
    result = seed_upstox_instrument_keys(db_conn)

    assert len(result) == len(UPSTOX_WATCHLIST)
    for upstox_key, instrument_id in result.items():
        assert upstox_key.startswith("NSE_EQ|")
        row = db_conn.execute(
            "SELECT series, source_bindings ->> 'upstox_instrument_key' "
            "FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        ).fetchone()
        assert row is not None
        series, stored_key = row
        assert series == "EQ"
        assert stored_key == upstox_key


def test_seed_is_idempotent(db_conn):
    first = seed_upstox_instrument_keys(db_conn)
    second = seed_upstox_instrument_keys(db_conn)
    assert first == second


def test_seed_accepts_a_custom_symbol_list(db_conn):
    result = seed_upstox_instrument_keys(db_conn, symbols=["RELIANCE"])
    assert set(result.values()) == {
        db_conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol = 'RELIANCE' "
            "AND exchange = 'NSE' AND segment = 'CM' AND series = 'EQ'"
        ).fetchone()[0]
    }


def test_seed_raises_a_clear_error_for_an_unknown_symbol(db_conn):
    with pytest.raises(ValueError, match="NOTASYMBOL"):
        seed_upstox_instrument_keys(db_conn, symbols=["NOTASYMBOL"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_seed_upstox_instruments.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.seed_upstox_instruments'`

- [ ] **Step 3: Add the Settings field**

Edit `src/trading/config.py`, adding one field alongside the existing Upstox settings (keep everything else unchanged):

```python
    upstox_api_key: str | None = None
    upstox_api_secret: str | None = None
    upstox_analytics_token: str | None = None
    dhan_client_id: str | None = None
```

- [ ] **Step 4: Write the seed module**

Create `src/trading/streaming/seed_upstox_instruments.py`:

```python
"""Populate `instruments.source_bindings` with Upstox instrument keys for a
small, fixed NSE equity watchlist -- mirroring `seed_instruments.py`'s
directness for small, static reference data, but updating existing rows
(Phase 0 already backfilled these instruments) rather than inserting new
ones the way the crypto seed does.

Usage: uv run python -m trading.streaming.seed_upstox_instruments
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg import Connection

from trading.config import get_settings

UPSTOX_WATCHLIST: tuple[str, ...] = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK")

_SELECT_EQ_ROW = """
    SELECT instrument_id, isin FROM instruments
    WHERE exchange = 'NSE' AND segment = 'CM' AND asset_class = 'EQUITY'
      AND series = 'EQ' AND symbol = %s
"""

_UPDATE_BINDING = """
    UPDATE instruments
    SET source_bindings = source_bindings || jsonb_build_object('upstox_instrument_key', %s)
    WHERE instrument_id = %s
"""


def seed_upstox_instrument_keys(
    conn: Connection, symbols: Sequence[str] = UPSTOX_WATCHLIST
) -> dict[str, int]:
    """Idempotent: derives each symbol's Upstox instrument_key from its
    stored ISIN (`NSE_EQ|<ISIN>`, Upstox's documented convention for NSE
    equities) and writes it into that row's `source_bindings`.

    Returns `{"NSE_EQ|<isin>": instrument_id, ...}`. Raises `ValueError`
    for any symbol that doesn't resolve to exactly one `series='EQ'` row --
    a missing or ambiguous mapping is a correctness bug worth surfacing
    immediately, not silently skipping.
    """
    result: dict[str, int] = {}
    for symbol in symbols:
        row = conn.execute(_SELECT_EQ_ROW, (symbol,)).fetchone()
        if row is None:
            raise ValueError(
                f"No exchange='NSE', segment='CM', asset_class='EQUITY', series='EQ' "
                f"instrument found for symbol {symbol!r}"
            )
        instrument_id, isin = row
        upstox_key = f"NSE_EQ|{isin}"
        conn.execute(_UPDATE_BINDING, (upstox_key, instrument_id))
        result[upstox_key] = int(instrument_id)
    return result


def main() -> None:
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        result = seed_upstox_instrument_keys(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    for upstox_key, instrument_id in result.items():
        print(f"{upstox_key}: instrument_id={instrument_id}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_seed_upstox_instruments.py -v`
Expected: 4 passed

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/config.py src/trading/streaming/seed_upstox_instruments.py \
        tests/streaming/test_seed_upstox_instruments.py
git commit -m "feat(streaming): seed Upstox instrument keys for the NSE equity watchlist"
```

---

## Task 3: `parse_upstox_frame()` — protobuf trade parser

**Files:**
- Create: `src/trading/streaming/upstox_feed.py`
- Test: `tests/streaming/test_upstox_feed.py`

**Interfaces:**
- Consumes: `trading.streaming.upstox_proto.MarketDataFeed_pb2.FeedResponse` (Task 1), `trading.streaming.models.Tick` (existing).
- Produces: `parse_upstox_frame(raw: bytes, instrument_ids: dict[str, int]) -> list[Tick]`. **Not** `Tick | None` like `binance_feed.parse_trade_message` — Upstox's `FeedResponse.feeds` is a `map<string, Feed>`, so **one WS frame can carry updates for multiple instruments at once**. Returns an empty list (never raises) for anything unparseable, or for a frame containing no tracked instrument's data. `instrument_ids` is keyed by the exact Upstox instrument_key string (e.g. `"NSE_EQ|INE002A01018"`) — Task 2's `seed_upstox_instrument_keys()` output is already in this shape, no case-folding or reformatting needed (unlike Binance's lowercase-symbol map).

For each `(instrument_key, feed)` entry in `FeedResponse.feeds`: skip if `instrument_key` isn't in `instrument_ids`; skip if `feed.WhichOneof("FeedUnion")` isn't `"ltpc"` (this task's ingestor subscribes in `"ltpc"` mode — see Task 4 — so a `full`/`option_chain` payload arriving would mean a subscribe-mode mismatch, not a message to silently coerce); otherwise build a `Tick` from `feed.ltpc.ltp`/`.ltq`/`.ltt`. **Do not filter on `FeedResponse.type` (`initial_feed` vs `live_feed`)** in this task — publish both uniformly; Task 5's live verification should specifically report whether `initial_feed` messages look like genuine ticks or redundant snapshots, since that's an empirical question this plan's design doc explicitly deferred, not something to guess at here.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_upstox_feed.py`:

```python
from __future__ import annotations

from decimal import Decimal

import pytest

from trading.streaming.upstox_feed import parse_upstox_frame
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


def _frame(entries: dict[str, tuple[float, int, int]], feed_type: int = pb.live_feed) -> bytes:
    """`entries` maps instrument_key -> (ltp, ltq, ltt)."""
    response = pb.FeedResponse()
    response.type = feed_type
    response.currentTs = 1724500000000
    for key, (ltp, ltq, ltt) in entries.items():
        feed = pb.Feed()
        feed.ltpc.ltp = ltp
        feed.ltpc.ltq = ltq
        feed.ltpc.ltt = ltt
        response.feeds[key].CopyFrom(feed)
    return response.SerializeToString()


def test_parse_upstox_frame_builds_a_tick_for_a_tracked_instrument() -> None:
    raw = _frame({"NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123)})

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.instrument_id == 501
    assert tick.price == Decimal("2500.5")
    assert tick.quantity == Decimal("10")
    assert tick.ts.year == 2024  # 1724500000123 ms


def test_parse_upstox_frame_builds_multiple_ticks_from_one_frame() -> None:
    raw = _frame(
        {
            "NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123),
            "NSE_EQ|INE467B01029": (3800.00, 5, 1724500000456),
        }
    )

    ticks = parse_upstox_frame(
        raw, instrument_ids={"NSE_EQ|INE002A01018": 501, "NSE_EQ|INE467B01029": 502}
    )

    assert {t.instrument_id for t in ticks} == {501, 502}


def test_parse_upstox_frame_ignores_an_untracked_instrument() -> None:
    raw = _frame({"NSE_EQ|UNTRACKED": (100.0, 1, 1724500000123)})

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert ticks == []


def test_parse_upstox_frame_includes_initial_feed_ticks_too() -> None:
    raw = _frame({"NSE_EQ|INE002A01018": (2500.50, 10, 1724500000123)}, feed_type=pb.initial_feed)

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert len(ticks) == 1


def test_parse_upstox_frame_ignores_a_tracked_instrument_with_no_ltpc_payload() -> None:
    response = pb.FeedResponse()
    response.feeds["NSE_EQ|INE002A01018"].CopyFrom(pb.Feed())  # oneof unset
    raw = response.SerializeToString()

    ticks = parse_upstox_frame(raw, instrument_ids={"NSE_EQ|INE002A01018": 501})

    assert ticks == []


def test_parse_upstox_frame_returns_empty_list_for_malformed_bytes() -> None:
    assert parse_upstox_frame(b"not a valid protobuf frame", instrument_ids={}) == []


@pytest.mark.live
def test_live_upstox_frame_matches_the_documented_ltpc_shape() -> None:
    """One real frame from Upstox's feed, shape-checked against what
    `parse_upstox_frame` assumes. Excluded from the default run. Only
    produces real data during NSE market hours (9:15-15:30 IST) -- outside
    that window this test will time out with no data, which is expected,
    not a failure to chase; run it during a trading session instead."""
    import asyncio

    from trading.config import get_settings
    from trading.recorder.upstox_ws import LiveUpstoxFeed

    token = get_settings().upstox_analytics_token
    if not token:
        pytest.skip("UPSTOX_ANALYTICS_TOKEN not set")

    async def _probe() -> bytes:
        feed = LiveUpstoxFeed(token)
        try:
            await feed.authorize()
            await feed.subscribe(["NSE_EQ|INE002A01018"])  # RELIANCE
            async for raw in feed:
                return raw
        finally:
            await feed.aclose()
        raise RuntimeError("feed closed with no frame received")

    raw = asyncio.run(asyncio.wait_for(_probe(), timeout=20))

    response = pb.FeedResponse()
    response.ParseFromString(raw)  # must not raise -- the real shape check
    assert response.type in (pb.initial_feed, pb.live_feed)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_feed.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.upstox_feed'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/upstox_feed.py`:

```python
"""Upstox V3 market-data protobuf frame parser.

Governing principle, same as `binance_feed.py`: a single message we can't
interpret is logged and skipped, never fatal. Unlike Binance's one-message-
one-trade shape, one Upstox `FeedResponse` frame can carry updates for
several instruments at once (`feeds` is a map), so this returns a list, not
`Tick | None`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import structlog
from google.protobuf.message import DecodeError

from trading.streaming.models import Tick
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb

log = structlog.get_logger(__name__)


def parse_upstox_frame(raw: bytes, instrument_ids: dict[str, int]) -> list[Tick]:
    """Parse one `FeedResponse` frame into zero or more `Tick`s.

    `instrument_ids` is keyed by the exact Upstox instrument_key string
    (e.g. "NSE_EQ|INE002A01018"). Never raises -- logs and returns an empty
    list for anything unparseable; skips (without logging, this is the
    expected common case) any entry for an untracked instrument or one with
    no `ltpc` payload set.
    """
    try:
        response = pb.FeedResponse()
        response.ParseFromString(raw)
    except DecodeError as exc:
        log.warning("upstox_feed.malformed_frame", reason=str(exc))
        return []

    ticks: list[Tick] = []
    for instrument_key, feed in response.feeds.items():
        instrument_id = instrument_ids.get(instrument_key)
        if instrument_id is None:
            continue
        if feed.WhichOneof("FeedUnion") != "ltpc":
            continue
        try:
            ticks.append(
                Tick(
                    instrument_id=instrument_id,
                    ts=datetime.fromtimestamp(feed.ltpc.ltt / 1000, tz=UTC),
                    price=Decimal(str(feed.ltpc.ltp)),
                    quantity=Decimal(str(feed.ltpc.ltq)),
                )
            )
        except (ValueError, InvalidOperation) as exc:
            log.warning(
                "upstox_feed.malformed_ltpc", instrument_key=instrument_key, reason=str(exc)
            )
    return ticks
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_feed.py -v`
Expected: 6 passed, 1 deselected (the `@pytest.mark.live` test is excluded by the default `-m 'not live'` invocation; it's exercised for real in Task 5, during market hours)

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_feed.py tests/streaming/test_upstox_feed.py
git commit -m "feat(streaming): parse Upstox protobuf frames into Ticks"
```

---

## Task 4: `upstox_ingestor` — the ingestion loop

**Files:**
- Create: `src/trading/streaming/upstox_ingestor.py`
- Test: `tests/streaming/test_upstox_ingestor.py`

**Interfaces:**
- Consumes: `trading.recorder.upstox_ws.{UpstoxFeed, LiveUpstoxFeed}` (existing, Phase 0), `trading.streaming.upstox_feed.parse_upstox_frame` (Task 3), `trading.streaming.seed_upstox_instruments.seed_upstox_instrument_keys` (Task 2), `trading.config.get_settings()` (existing).
- Produces: `run_ingestion_loop(redis, feed_factory, *, instrument_ids, initial_backoff_seconds=1.0, max_backoff_seconds=30.0, sleep=_default_sleep, max_ticks=None) -> None` (async). Same shape as `crypto_ingestor.run_ingestion_loop` — `feed_factory: Callable[[], UpstoxFeed]`, `instrument_ids: dict[str, int]` keyed by Upstox instrument_key. `max_ticks` is the same test seam crypto's ingestor uses.

The one structural difference from `crypto_ingestor`: this loop calls `await feed.authorize()` then `await feed.subscribe(list(instrument_ids))` before iterating frames — `UpstoxFeed`'s protocol (already defined in `trading.recorder.upstox_ws`) requires both; Binance's public stream needs neither. Subscribe mode is `"ltpc"` (`LiveUpstoxFeed.subscribe` currently hardcodes `"mode": "full"` in its request body — **do not modify `trading.recorder.upstox_ws.LiveUpstoxFeed`**, that module belongs to the raw-archival recorder sub-project and changing its subscribe mode would change Phase 0's already-shipped, already-reviewed behavior; instead this task's test double controls its own mode, and Task 5's live run will confirm empirically whether the shared `LiveUpstoxFeed.subscribe`'s hardcoded `"full"` mode is acceptable to reuse as-is for now — `parse_upstox_frame` already only extracts the `ltpc` portion regardless of what other fields a `"full"`-mode payload carries, so this is not a correctness blocker, just a bandwidth/scope note to verify live).

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_upstox_ingestor.py`:

```python
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.upstox_ingestor import run_ingestion_loop
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb


class ScriptedUpstoxFeed:
    """A fake `UpstoxFeed`: records authorize()/subscribe() calls, yields
    scripted raw frames, then optionally fails."""

    def __init__(self, frames: Sequence[bytes], *, fail_after: BaseException | None = None) -> None:
        self.frames = list(frames)
        self.fail_after = fail_after
        self.authorized = False
        self.subscribed_keys: list[str] | None = None
        self.closed = False

    async def authorize(self) -> None:
        self.authorized = True

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        self.subscribed_keys = instrument_keys
        return instrument_keys

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for frame in self.frames:
            yield frame
        if self.fail_after is not None:
            raise self.fail_after

    async def aclose(self) -> None:
        self.closed = True


async def _no_sleep(seconds: float) -> None:
    return None


def _ltpc_frame(instrument_key: str, ltp: float, ltq: int = 10, ltt: int = 1724500000123) -> bytes:
    response = pb.FeedResponse()
    response.type = pb.live_feed
    feed = pb.Feed()
    feed.ltpc.ltp = ltp
    feed.ltpc.ltq = ltq
    feed.ltpc.ltt = ltt
    response.feeds[instrument_key].CopyFrom(feed)
    return response.SerializeToString()


def test_run_ingestion_loop_authorizes_subscribes_and_publishes_parsed_ticks(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedUpstoxFeed([_ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert feed.authorized
    assert feed.subscribed_keys == ["NSE_EQ|INE002A01018"]
    message = pubsub.get_message(timeout=2)
    assert message is not None and message["type"] == "message"
    payload = message["data"]
    import json

    parsed = json.loads(payload)
    assert parsed["instrument_id"] == 501
    assert Decimal(str(parsed["price"])) == Decimal("2500.5")


def test_run_ingestion_loop_skips_a_malformed_frame_and_keeps_going(
    redis_client: redis.Redis,
) -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    feed = ScriptedUpstoxFeed([b"not a valid frame", _ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: feed,
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_no_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    message = pubsub.get_message(timeout=2)
    assert message is not None


def test_run_ingestion_loop_grows_backoff_on_repeated_connect_then_drop(
    redis_client: redis.Redis,
) -> None:
    """Same lesson already learned on crypto_ingestor: a connection that
    authorizes/subscribes successfully but yields zero frames before
    dropping must not reset backoff to the initial value -- only a
    connection that actually yields a message proves itself."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe("ticks:501")
    pubsub.get_message(timeout=1)

    failing_1 = ScriptedUpstoxFeed([], fail_after=ConnectionError("dropped"))
    failing_2 = ScriptedUpstoxFeed([], fail_after=ConnectionError("dropped"))
    working = ScriptedUpstoxFeed([_ltpc_frame("NSE_EQ|INE002A01018", 2500.50)])
    feeds = iter([failing_1, failing_2, working])

    sleeps: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                async_redis,
                lambda: next(feeds),
                instrument_ids={"NSE_EQ|INE002A01018": 501},
                sleep=_record_sleep,
                max_ticks=1,
            )
        )
    finally:
        asyncio.run(async_redis.aclose())

    assert sleeps == [1.0, 2.0]
    assert working.authorized and working.subscribed_keys == ["NSE_EQ|INE002A01018"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_ingestor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.upstox_ingestor'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/upstox_ingestor.py`:

```python
"""Entry point: `python -m trading.streaming.upstox_ingestor`.

Streams Upstox trades, parses each frame into zero or more Ticks, and
publishes them to Redis. Reconnects with exponential backoff on any
failure; a single malformed frame is logged and skipped, never fatal.

Reuses `trading.recorder.upstox_ws`'s `UpstoxFeed`/`LiveUpstoxFeed`
(Phase 0's already-built auth/subscribe/frame-iteration code) rather than
duplicating it -- this loop only adds parsing and Redis fan-out on top.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import structlog
from redis.asyncio import Redis

from trading.config import get_settings
from trading.recorder.upstox_ws import LiveUpstoxFeed, UpstoxFeed
from trading.streaming.seed_upstox_instruments import seed_upstox_instrument_keys
from trading.streaming.upstox_feed import parse_upstox_frame

log = structlog.get_logger(__name__)

FeedFactory = Callable[[], UpstoxFeed]
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
    publishing `max_ticks` ticks when it's an int -- a test seam, same
    shape as `crypto_ingestor.run_ingestion_loop`'s.
    """
    backoff = initial_backoff_seconds
    published = 0
    instrument_keys = list(instrument_ids)

    try:
        while max_ticks is None or published < max_ticks:
            feed = feed_factory()
            try:
                await feed.authorize()
                await feed.subscribe(instrument_keys)
                consumed_any = False

                async for raw in feed:
                    if not consumed_any:
                        # Only prove the connection by a real message, not merely a
                        # successful authorize()/subscribe() -- a connect-then-drop
                        # failure must still back off exponentially. Same lesson
                        # already learned on crypto_ingestor.run_ingestion_loop.
                        backoff = initial_backoff_seconds
                        consumed_any = True
                    for tick in parse_upstox_frame(raw, instrument_ids):
                        try:
                            await redis.publish(
                                f"ticks:{tick.instrument_id}", tick.model_dump_json()
                            )
                            published += 1
                        except Exception as exc:  # noqa: BLE001 - a publish failure must not kill the socket
                            log.warning("upstox_ingestor.publish_failed", reason=str(exc))
                        if max_ticks is not None and published >= max_ticks:
                            break
                    if max_ticks is not None and published >= max_ticks:
                        break
            except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect, not a crash
                log.warning("upstox_ingestor.disconnected", reason=str(exc))
                wait = min(backoff, max_backoff_seconds)
                await sleep(wait)
                backoff = min(backoff * 2, max_backoff_seconds)
            finally:
                try:
                    await feed.aclose()
                except Exception:  # noqa: BLE001 - closing must never itself crash the loop
                    log.debug("upstox_ingestor.close_failed", exc_info=True)
    finally:
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()
    token = settings.upstox_analytics_token
    if not token:
        raise RuntimeError(
            "UPSTOX_ANALYTICS_TOKEN is not set. Add it to .env (generated from the Upstox "
            "developer console's Analytics Access Token flow, not the daily OAuth token)."
        )

    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        instrument_ids = seed_upstox_instrument_keys(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    log.info("upstox_ingestor.starting", instrument_keys=list(instrument_ids))

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                redis,
                lambda: LiveUpstoxFeed(token),
                instrument_ids=instrument_ids,
            )
        )
    except KeyboardInterrupt:
        log.info("upstox_ingestor.interrupted")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_ingestor.py -v`
Expected: 3 passed

- [ ] **Step 5: Run the full suite once**

Run: `uv run pytest`
Expected: all passing, no regressions in any prior streaming task's tests or in `tests/recorder/test_upstox_ws.py` (this task imports from `trading.recorder.upstox_ws` but never modifies it).

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_ingestor.py tests/streaming/test_upstox_ingestor.py
git commit -m "feat(streaming): upstox_ingestor loop reusing Phase 0's LiveUpstoxFeed"
```

---

## Task 5: End-to-end live verification (NSE market hours only)

**Files:** none (verification only — nothing here should require a code change; if it does, that's a signal a step above missed something, not a step to improvise around).

Controller-run (Opus), not delegated — same reasoning as every prior plan's live-verification task: judging "does this look right against the real feed" is a judgment call. **This task can only run while NSE is open (9:15–15:30 IST, a trading day) — it is expected to be blocked at plan-execution time if run outside those hours, and should be explicitly deferred to the next market session rather than skipped or faked.**

- [ ] **Step 1: Confirm market hours**

Check the current time against NSE's 9:15–15:30 IST trading window on a trading day (not a weekend/holiday — `trading.calendar.trading_days.is_trading_day` can confirm the date if there's any doubt). If outside this window: stop here, report which commits (Tasks 1-4) are ready and waiting, and note the next trading session's open time. Do not attempt a partial or simulated live check as a substitute.

- [ ] **Step 2: Confirm infrastructure and token**

Run: `docker compose ps` — expect `trading_tsdb` and `trading_redis` both healthy.
Confirm `UPSTOX_ANALYTICS_TOKEN` is set: `uv run python -c "from trading.config import get_settings; print(bool(get_settings().upstox_analytics_token))"` should print `True` (never print the token itself).

- [ ] **Step 3: Start the ingestor**

Run (separate terminal, leave running): `uv run python -m trading.streaming.upstox_ingestor`
Expected: `upstox_ingestor.starting` log line listing the 5 watchlist instrument_keys, then either live activity or silence depending on trading volume. If `authorize()` fails, capture the exact error (Upstox's V3 authorize endpoint responds with a structured error payload) — this is exactly the kind of first-real-run failure this task exists to catch, not to route around.

- [ ] **Step 4: Start the aggregator (if not already running)**

Run (separate terminal, leave running, skip if already running): `uv run python -m trading.streaming.bar_aggregator`

- [ ] **Step 5: Watch and record**

Watch both terminals for at least 5 minutes. Then query, same shape as `bar_aggregator`'s own Task 4:

```bash
uv run python -c "
import psycopg
from trading.config import get_settings
conn = psycopg.connect(get_settings().database_url)
rows = conn.execute('''
    SELECT i.symbol, count(*) AS bars, max(b.ts) AS latest_bar,
           min(b.open) AS sample_open, max(b.close) AS sample_close
    FROM bars_intraday b JOIN instruments i ON i.instrument_id = b.instrument_id
    WHERE i.exchange = 'NSE' AND i.source_bindings ? 'upstox_instrument_key'
    GROUP BY i.symbol ORDER BY bars DESC
''').fetchall()
for row in rows:
    print(row)
"
```

Expected: real bars for at least the more liquid watchlist names (RELIANCE, TCS, HDFCBANK, ICICIBANK typically trade far more frequently than a thinner name), with sane, non-zero, non-null OHLC matching real market prices (cross-check one against a live quote source). Also specifically note, from the ingestor's log or a quick inspection: did any `initial_feed`-type message look like a duplicate/stale price versus the surrounding `live_feed` ticks? This answers the open question the design doc deferred — record the finding either way, it doesn't need to block anything regardless of the answer, but it does need Task 3's filtering decision revisited in a follow-up if `initial_feed` messages turn out to be noisy.

- [ ] **Step 6: Record the report**

Record in the task's completion notes: which watchlist names were observed updating, over what wall-clock window, 2-3 example tick/bar values with timestamps, whether `authorize()`/`subscribe()` worked against the real Analytics Token on the first attempt, and the `initial_feed` observation from Step 5 — the same evidentiary standard every prior live-verification task in this project has held itself to.

- [ ] **Step 7: Stop the processes**

Ctrl-C both processes (or leave running if continuing to observe). No commit for this task — it verifies Tasks 1-4's commits, it doesn't add its own.
