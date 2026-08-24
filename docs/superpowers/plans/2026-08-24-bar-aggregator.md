# Bar Aggregator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Subscribe to the live crypto ticks `crypto_ingestor` already publishes, bucket them into 1-minute OHLCV bars in memory, and write each closed bar into `bars_intraday` — the persistence and aggregation path the crypto-streaming plan explicitly deferred, and the data the replay service needs next.

**Architecture:** A third process, `bar_aggregator`, coupled to the existing two only by Redis. It `PSUBSCRIBE`s `ticks:*` (the same pattern-subscribe shape `stream_gateway` uses), maintains pure in-memory per-instrument minute buckets, and upserts each bar into `bars_intraday` the moment its window closes — never a partial bar. A periodic safety-net check flushes a bucket even if its instrument goes quiet exactly at a minute boundary.

**Tech Stack:** Python 3.12 (via `uv`) · `redis` (asyncio client) · `psycopg` · Pydantic v2 (reuses `trading.streaming.models.Tick`) · Alembic · pytest

**Spec:** [`docs/superpowers/specs/2026-08-24-bar-aggregator-design.md`](../specs/2026-08-24-bar-aggregator-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** exactly, managed by `uv`.
- **Money is never a float.** `OpenBar`/`ClosedBar` fields and every `bars_intraday` write use `Decimal`, matching `Tick.price`/`Tick.quantity`.
- **Timestamps are timezone-aware UTC.** `bucket_start()` always returns a UTC-anchored `datetime` regardless of the tick's own `tzinfo`.
- **No network in tests.** Ticks are published into Redis directly via the real `redis_client` fixture, never a real Binance connection.
- **No mocks for infrastructure.** Tests run against real Redis (`trading_redis`, docker-compose) and real Postgres (`db_conn`) — never a fake/mock client.
- **Tests stay synchronous** where they touch async code: `asyncio.run(...)` around the loop under test, no `pytest-asyncio`. The pure `BarAggregator` bucketing logic needs no `asyncio` at all — it's plain synchronous code, tested directly.
- **A single malformed message on `ticks:*` is logged and skipped, never fatal** — same rule `crypto_ingestor`/`stream_gateway` already apply to a bad frame.
- **Only 1-minute (`interval_sec=60`) bars are written in this sub-project.** `bars_intraday`'s primary key already supports other granularities; nothing here writes them.
- **A bar is only ever written once its minute window has fully closed — never a partial/in-progress bar.** On process shutdown, any still-open bucket is discarded, not force-flushed.
- **`bars_daily` is never touched by this plan.** Only `bars_intraday` and `data_sources`.
- **The core write path never calls `conn.commit()` itself.** Committing is the connection's responsibility: production opens its connection with `autocommit=True`; tests use `db_conn` (`autocommit=False`, rolled back at teardown), so nothing a test does is ever persisted — the same guarantee every other test in this repo relies on.
- **Lint/type gate every task:** `ruff check . && ruff format --check . && mypy src` must pass before any commit.
- **Every task ends with a passing `pytest` run (default invocation) and a commit.**

---

## File Structure

```
migrations/versions/
  0003_bars_intraday_volume_numeric.py   widens bars_intraday.volume, seeds BINANCE_WS

src/trading/contracts/enums.py           + DataSource.BINANCE_WS = 6

src/trading/streaming/
  bar_aggregator.py                      bucket_start(), OpenBar, ClosedBar, BarAggregator,
                                          write_closed_bar(), run_aggregation_loop(), CLI

tests/
  test_migrations.py                     + fractional-volume test, updated pinned-enum test
  streaming/test_bar_aggregator.py       unit tests (pure logic) + integration tests (Redis+PG)
```

Dependency order: **Task 1** (migration + enum) has no dependency on anything else in this plan. **Task 2** (pure bucketing logic + write path) depends on Task 1's `DataSource.BINANCE_WS` and widened `bars_intraday.volume`. **Task 3** (async loop + CLI) depends on Task 2's `BarAggregator`/`write_closed_bar`. **Task 4** (end-to-end verification) depends on everything.

```
Task 1 (migration + enum) -> Task 2 (pure logic + write path) -> Task 3 (async loop + CLI) -> Task 4 (e2e verification)
```

**AI-tier delegation:** Task 1 is transcription-level (complete SQL/code given verbatim) — cheapest tier. Task 2 is self-contained logic with real correctness subtlety (bucket math, out-of-order-tick handling) but no I/O — standard tier. Task 3 is where the prior crypto-streaming plan's real bugs lived (async Redis lifecycle, concurrent-task orchestration) — standard tier, reviewed carefully. Task 4 is judgment-driven manual verification — controller-run (Opus), same as the prior plan's Task 6.

---

## Task 1: Schema migration and `DataSource.BINANCE_WS`

**Files:**
- Create: `migrations/versions/0003_bars_intraday_volume_numeric.py`
- Modify: `src/trading/contracts/enums.py`
- Modify: `tests/test_migrations.py`

**Interfaces:**
- Produces: `bars_intraday.volume` as `NUMERIC(28,8)` (was `BIGINT`). `DataSource.BINANCE_WS` with value `6`, seeded into the `data_sources` table. Task 2's `write_closed_bar()` writes `DataSource.BINANCE_WS.value` into every `bars_intraday.source` it inserts, and writes `Decimal` volumes that only fit because of this migration.

- [ ] **Step 1: Write the failing tests**

Open `tests/test_migrations.py`. Find `test_data_source_values_are_pinned` (near the end of the file) and replace its body's expected dict to include the new member:

```python
def test_data_source_values_are_pinned():
    """These integers are persisted on every bar row (~250M at full backfill).

    Renumbering them would silently reattribute the provenance of all existing
    data with no error anywhere. This test is the guard rail: adding a member is
    fine, changing an existing member's value must break the build.
    """
    from trading.contracts import DataSource

    assert {s.name: s.value for s in DataSource} == {
        "NSE_CM_UDIFF": 1,
        "NSE_FO_UDIFF": 2,
        "BSE_CM_UDIFF": 3,
        "NSE_CM_LEGACY": 4,
        "AMFI_NAV": 5,
        "BINANCE_WS": 6,
    }
```

Then append a new test at the end of the same file:

```python
def test_bars_intraday_accepts_a_fractional_volume(db_conn):
    """Crypto trade quantities are fractional Decimals (e.g. 0.01000000 BTC)
    -- volume must not be a BIGINT. Migration 0003 widens it to NUMERIC."""
    from decimal import Decimal

    iid = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency,"
        " status, canonical_key) VALUES ('CRYPTO','BINANCE','SPOT','TESTUSDT','USDT',"
        "'ACTIVE','BINANCE:SPOT:TESTUSDT') RETURNING instrument_id"
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low,"
        " close, volume, trades, source) VALUES (%s, '2026-08-24T12:00:00Z', 60,"
        " 100, 105, 98, 102, %s, 4, 6)",
        (iid, Decimal("0.01000000")),
    )  # must not raise
    row = db_conn.execute(
        "SELECT volume FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row[0] == Decimal("0.01000000")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_migrations.py -v -k "pinned or fractional_volume"`
Expected: `test_data_source_values_are_pinned` FAILS (dict mismatch — `BINANCE_WS` not yet in the enum), `test_bars_intraday_accepts_a_fractional_volume` FAILS with a `psycopg.errors.InvalidTextRepresentation` or numeric-overflow error (volume is still `BIGINT` and can't hold a value with 8 decimal places, or the INSERT itself fails type coercion).

- [ ] **Step 3: Add the enum member**

Edit `src/trading/contracts/enums.py`:

```python
class DataSource(IntEnum):
    """Persisted provenance codes. Append only; never renumber."""

    NSE_CM_UDIFF = 1
    NSE_FO_UDIFF = 2
    BSE_CM_UDIFF = 3
    NSE_CM_LEGACY = 4
    AMFI_NAV = 5
    BINANCE_WS = 6
```

- [ ] **Step 4: Write the migration**

Create `migrations/versions/0003_bars_intraday_volume_numeric.py`:

```python
"""bars_intraday.volume widens to NUMERIC, DataSource.BINANCE_WS seeded.

Part of the bar-aggregator sub-project (docs/superpowers/specs/
2026-08-24-bar-aggregator-design.md). Crypto trade quantities are
fractional Decimals (e.g. 0.01000000 BTC) and cannot be represented in a
BIGINT column. Widens only `bars_intraday.volume` -- `bars_daily.volume`
is deliberately left untouched: this migration's caller never writes to
`bars_daily`, real NSE/BSE equity and F&O volumes are always whole-unit
integers, and that table is live and already covered by Phase 0's
closed-out reconcile/validation checks.

`bars_intraday` is empty at the time this migration is written (Task 6 of
the crypto-streaming plan never wrote to it -- that plan explicitly
deferred persistence), so this is a pure schema change, no data migration.
The downgrade path assumes the same: reverting after real fractional
volumes have been written would truncate them, but the table is empty as
of every migration currently in this repo's history.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE bars_intraday ALTER COLUMN volume TYPE NUMERIC(28,8)")
    op.execute("INSERT INTO data_sources (source_id, source_key) VALUES (6, 'BINANCE_WS')")


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 6")
    op.execute("ALTER TABLE bars_intraday ALTER COLUMN volume TYPE BIGINT")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_migrations.py -v`
Expected: all pass, including `test_data_sources_match_the_python_enum` (unmodified — it iterates the enum dynamically and needed no change) and the two you just touched.

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean

```bash
git add migrations/versions/0003_bars_intraday_volume_numeric.py \
        src/trading/contracts/enums.py tests/test_migrations.py
git commit -m "feat(streaming): widen bars_intraday.volume to NUMERIC, add DataSource.BINANCE_WS"
```

---

## Task 2: Bar bucketing logic and the Postgres write path

**Files:**
- Create: `src/trading/streaming/bar_aggregator.py` (this task writes the pure/sync portions only — Task 3 appends the async loop and CLI to the same file)
- Test: `tests/streaming/test_bar_aggregator.py` (this task writes the unit-test portions only — Task 3 appends integration tests to the same file)

**Interfaces:**
- Consumes: `trading.streaming.models.Tick` (existing), `trading.contracts.DataSource` (Task 1, has `.BINANCE_WS`).
- Produces: `bucket_start(ts: datetime, interval_seconds: int = 60) -> datetime`. `OpenBar` (dataclass: `open, high, low, close: Decimal`, `volume: Decimal`, `trades: int`; classmethod `OpenBar.start(tick: Tick) -> OpenBar`; method `.update(tick: Tick) -> None`). `ClosedBar` (dataclass: `instrument_id: int`, `bucket: datetime`, `bar: OpenBar`). `BarAggregator` (class: `__init__(self, interval_seconds: int = 60)`; `.ingest(tick: Tick) -> list[ClosedBar]`; `.flush_stale(now: datetime) -> list[ClosedBar]`). `write_closed_bar(conn: Connection, closed: ClosedBar, *, interval_seconds: int = 60) -> None`. `INTERVAL_SECONDS: int = 60` module constant. Task 3's `run_aggregation_loop` imports and uses all of these.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_bar_aggregator.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.streaming.bar_aggregator import (
    BarAggregator,
    ClosedBar,
    OpenBar,
    bucket_start,
    write_closed_bar,
)
from trading.streaming.models import Tick


def _tick(
    ts: datetime, price: str = "100.00", quantity: str = "1.00", instrument_id: int = 501
) -> Tick:
    return Tick(
        instrument_id=instrument_id, ts=ts, price=Decimal(price), quantity=Decimal(quantity)
    )


def test_bucket_start_floors_to_the_minute_in_utc() -> None:
    ts = datetime(2026, 8, 24, 12, 0, 45, tzinfo=UTC)
    assert bucket_start(ts) == datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)


def test_bucket_start_is_idempotent_on_an_already_floored_timestamp() -> None:
    ts = datetime(2026, 8, 24, 12, 1, 0, tzinfo=UTC)
    assert bucket_start(ts) == ts


def test_ingest_opens_a_new_bucket_and_returns_nothing_closed() -> None:
    aggregator = BarAggregator()
    closed = aggregator.ingest(_tick(datetime(2026, 8, 24, 12, 0, 10, tzinfo=UTC)))
    assert closed == []


def test_ingest_updates_high_low_close_within_the_same_bucket() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=1), price="100"))
    aggregator.ingest(_tick(base + timedelta(seconds=2), price="105"))
    aggregator.ingest(_tick(base + timedelta(seconds=3), price="98"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=4), price="102"))
    assert closed == []
    # Force the bucket closed by crossing into the next minute.
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert len(closed) == 1
    bar = closed[0].bar
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("105")
    assert bar.low == Decimal("98")
    assert bar.close == Decimal("102")
    assert bar.volume == Decimal("4.00")  # four 1.00-quantity ticks in the first bucket
    assert bar.trades == 4


def test_ingest_closes_the_previous_bucket_with_the_correct_bucket_start() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 5, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30)))
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1, seconds=1)))
    assert len(closed) == 1
    assert closed[0].bucket == base
    assert closed[0].instrument_id == 501


def test_ingest_treats_an_out_of_order_tick_as_an_update_to_the_current_bucket() -> None:
    """Ticks are assumed non-decreasing per instrument (one ordered WS
    connection -> one ordered Redis subscription). An out-of-order tick
    updates the currently-open bucket rather than reopening a closed one --
    an acknowledged simplification, not a crash or a silent data loss."""
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=30), price="100"))
    closed = aggregator.ingest(_tick(base + timedelta(seconds=10), price="999"))  # earlier ts
    assert closed == []  # no bucket was closed -- just folded into the open one
    closed = aggregator.ingest(_tick(base + timedelta(minutes=1), price="200"))
    assert closed[0].bar.close == Decimal("999")  # last-ingested tick, not last-in-time


def test_flush_stale_closes_a_bucket_whose_window_has_fully_elapsed() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert len(closed) == 1
    assert closed[0].bucket == base
    # Flushed buckets are removed -- a second flush at the same `now` finds nothing.
    assert aggregator.flush_stale(now=base + timedelta(seconds=60)) == []


def test_flush_stale_leaves_a_bucket_open_if_its_window_has_not_elapsed_yet() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10)))
    assert aggregator.flush_stale(now=base + timedelta(seconds=59)) == []


def test_flush_stale_tracks_multiple_instruments_independently() -> None:
    aggregator = BarAggregator()
    base = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    aggregator.ingest(_tick(base + timedelta(seconds=10), instrument_id=501))
    aggregator.ingest(_tick(base + timedelta(seconds=20), instrument_id=502))
    closed = aggregator.flush_stale(now=base + timedelta(seconds=60))
    assert {c.instrument_id for c in closed} == {501, 502}


def test_write_closed_bar_upserts_into_bars_intraday(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    closed = ClosedBar(
        instrument_id=iid,
        bucket=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        bar=OpenBar(
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("102"),
            volume=Decimal("0.01000000"),
            trades=4,
        ),
    )
    write_closed_bar(db_conn, closed)
    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, interval_sec, source"
        " FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (
        Decimal("100.0000"), Decimal("105.0000"), Decimal("98.0000"), Decimal("102.0000"),
        Decimal("0.01000000"), 4, 60, 6,
    )


def test_write_closed_bar_is_idempotent_on_conflict(db_conn) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    bucket = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)
    first = ClosedBar(
        instrument_id=iid, bucket=bucket,
        bar=OpenBar(open=Decimal("1"), high=Decimal("1"), low=Decimal("1"),
                    close=Decimal("1"), volume=Decimal("1"), trades=1),
    )
    second = ClosedBar(
        instrument_id=iid, bucket=bucket,
        bar=OpenBar(open=Decimal("1"), high=Decimal("9"), low=Decimal("1"),
                    close=Decimal("5"), volume=Decimal("3"), trades=3),
    )
    write_closed_bar(db_conn, first)
    write_closed_bar(db_conn, second)
    rows = db_conn.execute(
        "SELECT close, trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchall()
    assert len(rows) == 1  # upserted, not duplicated
    assert rows[0] == (Decimal("5.0000"), 3)  # second write's values won
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_bar_aggregator.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.bar_aggregator'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/bar_aggregator.py`:

```python
"""Bucketing logic and Postgres write path for the crypto bar aggregator.

Entry point (added in a later step of this plan, Task 3):
`python -m trading.streaming.bar_aggregator`. See the design doc
(docs/superpowers/specs/2026-08-24-bar-aggregator-design.md) for why this
exists: turning crypto_ingestor's live ticks into real 1-minute bars in
`bars_intraday` -- data the replay service needs next.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from psycopg import Connection

from trading.contracts import DataSource
from trading.streaming.models import Tick

INTERVAL_SECONDS = 60


def bucket_start(ts: datetime, interval_seconds: int = INTERVAL_SECONDS) -> datetime:
    """Floor `ts` to the start of its interval bucket, anchored to UTC
    regardless of `ts`'s own tzinfo (Tick.ts is always tz-aware, but this
    function doesn't assume which zone)."""
    epoch_seconds = int(ts.timestamp())
    floored = epoch_seconds - (epoch_seconds % interval_seconds)
    return datetime.fromtimestamp(floored, tz=UTC)


@dataclass
class OpenBar:
    """A bar still accumulating ticks. Never written to Postgres directly --
    only via a `ClosedBar` once its window has fully elapsed."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int

    @classmethod
    def start(cls, tick: Tick) -> OpenBar:
        return cls(
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.quantity,
            trades=1,
        )

    def update(self, tick: Tick) -> None:
        self.high = max(self.high, tick.price)
        self.low = min(self.low, tick.price)
        self.close = tick.price
        self.volume += tick.quantity
        self.trades += 1


@dataclass
class ClosedBar:
    instrument_id: int
    bucket: datetime
    bar: OpenBar


class BarAggregator:
    """Pure in-memory minute-bucketing -- no I/O. `ingest()`/`flush_stale()`
    are synchronous and return any bars that just closed as a result.

    Assumes ticks arrive in non-decreasing timestamp order per instrument
    (true for one ordered Binance WS connection feeding one ordered Redis
    subscription): an out-of-order tick updates the currently-open bucket
    rather than reopening an already-closed one. An acknowledged
    simplification for this proof-of-shape tier, not a silent bug.
    """

    def __init__(self, interval_seconds: int = INTERVAL_SECONDS) -> None:
        self._interval_seconds = interval_seconds
        self._open: dict[int, tuple[datetime, OpenBar]] = {}

    def ingest(self, tick: Tick) -> list[ClosedBar]:
        bucket = bucket_start(tick.ts, self._interval_seconds)
        current = self._open.get(tick.instrument_id)
        if current is None:
            self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
            return []
        current_bucket, bar = current
        if bucket <= current_bucket:
            bar.update(tick)
            return []
        self._open[tick.instrument_id] = (bucket, OpenBar.start(tick))
        return [ClosedBar(instrument_id=tick.instrument_id, bucket=current_bucket, bar=bar)]

    def flush_stale(self, now: datetime) -> list[ClosedBar]:
        """Close any bucket whose window has fully elapsed as of `now`, even
        with no new tick to trigger it via `ingest()`. Removes flushed
        buckets from internal state -- calling this twice at the same `now`
        returns the second time's results as empty."""
        closed: list[ClosedBar] = []
        for instrument_id, (bucket, bar) in list(self._open.items()):
            if now >= bucket + timedelta(seconds=self._interval_seconds):
                closed.append(ClosedBar(instrument_id=instrument_id, bucket=bucket, bar=bar))
                del self._open[instrument_id]
        return closed


_UPSERT_BAR = """
    INSERT INTO bars_intraday
        (instrument_id, ts, interval_sec, open, high, low, close, volume, trades, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume, trades = EXCLUDED.trades
"""


def write_closed_bar(
    conn: Connection, closed: ClosedBar, *, interval_seconds: int = INTERVAL_SECONDS
) -> None:
    """Upsert one closed bar. Never calls `conn.commit()` -- see this plan's
    Global Constraints for why (keeps this function test-safe against
    `db_conn`'s rollback-at-teardown; production commits via an
    `autocommit=True` connection instead)."""
    conn.execute(
        _UPSERT_BAR,
        (
            closed.instrument_id,
            closed.bucket,
            interval_seconds,
            closed.bar.open,
            closed.bar.high,
            closed.bar.low,
            closed.bar.close,
            closed.bar.volume,
            closed.bar.trades,
            DataSource.BINANCE_WS.value,
        ),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_bar_aggregator.py -v`
Expected: 11 passed

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean

```bash
git add src/trading/streaming/bar_aggregator.py tests/streaming/test_bar_aggregator.py
git commit -m "feat(streaming): bar bucketing logic and bars_intraday write path"
```

---

## Task 3: Async aggregation loop and CLI

**Files:**
- Modify: `src/trading/streaming/bar_aggregator.py` (append to the file Task 2 created)
- Test: `tests/streaming/test_bar_aggregator.py` (append to the file Task 2 created)

**Interfaces:**
- Consumes: `trading.streaming.bar_aggregator.{INTERVAL_SECONDS, BarAggregator, ClosedBar, write_closed_bar}` (Task 2), `trading.streaming.models.Tick` (existing), `trading.config.get_settings()` (existing).
- Produces: `run_aggregation_loop(redis: Redis, conn: Connection, *, interval_seconds: int = INTERVAL_SECONDS, flush_check_seconds: float = 5.0, sleep: Sleeper = _default_sleep, max_bars_written: int | None = None) -> None` (async). `main()` — CLI entry point, `python -m trading.streaming.bar_aggregator`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_bar_aggregator.py`. First, update the imports at the top of the file to add these:

```python
import asyncio
from collections.abc import Coroutine
from typing import Any

import redis
from redis.asyncio import Redis as AsyncRedis

from trading.config import get_settings
from trading.streaming.bar_aggregator import run_aggregation_loop
```

Then append these tests at the end of the file:

```python
async def _no_sleep(seconds: float) -> None:
    return None


def _tick_json(instrument_id: int, ts: str, price: str, quantity: str = "0.01000000") -> str:
    return Tick(
        instrument_id=instrument_id, ts=datetime.fromisoformat(ts),
        price=Decimal(price), quantity=Decimal(quantity),
    ).model_dump_json()


def test_run_aggregation_loop_writes_a_closed_bar_once_its_window_elapses(
    db_conn, redis_client: redis.Redis
) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = asyncio.ensure_future(
            run_aggregation_loop(async_redis, db_conn, sleep=_no_sleep, max_bars_written=1)
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)  # give psubscribe time to land before we publish
            redis_client.publish(
                channel, _tick_json(iid, "2026-08-24T12:00:10+00:00", "65000.00")
            )
            redis_client.publish(
                channel, _tick_json(iid, "2026-08-24T12:01:05+00:00", "65010.00")
            )

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, source"
        " FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (
        Decimal("65000.0000"), Decimal("65000.0000"), Decimal("65000.0000"),
        Decimal("65000.0000"), Decimal("0.01000000"), 1, 6,
    )


async def _run_both(loop_task: asyncio.Task[None], publisher: Coroutine[Any, Any, None]) -> None:
    await asyncio.gather(loop_task, publisher)


def test_run_aggregation_loop_skips_a_malformed_message_and_keeps_going(
    db_conn, redis_client: redis.Redis
) -> None:
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = asyncio.ensure_future(
            run_aggregation_loop(async_redis, db_conn, sleep=_no_sleep, max_bars_written=1)
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, "not json")
            redis_client.publish(
                channel, _tick_json(iid, "2026-08-24T12:00:10+00:00", "65000.00")
            )
            redis_client.publish(
                channel, _tick_json(iid, "2026-08-24T12:01:05+00:00", "65010.00")
            )

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)  # only the one valid tick before the bucket closed


def test_run_aggregation_loop_flushes_a_stale_bucket_via_the_periodic_safety_net(
    db_conn, redis_client: redis.Redis
) -> None:
    """No second tick ever arrives to trigger ingest()'s rollover-detection
    -- only the periodic flush can close this bucket. Uses a tick timestamped
    far in the past (not a real multi-minute wall-clock wait): the very
    first periodic check already finds the bucket's window elapsed."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    channel = f"ticks:{iid}"
    stale_ts = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()

    async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
    try:
        loop_task = asyncio.ensure_future(
            run_aggregation_loop(
                async_redis, db_conn, flush_check_seconds=0.05,
                sleep=asyncio.sleep, max_bars_written=1,
            )
        )

        async def _publish_after_subscribed() -> None:
            await asyncio.sleep(0.2)
            redis_client.publish(channel, _tick_json(iid, stale_ts, "65000.00"))

        asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
    finally:
        asyncio.run(async_redis.aclose())

    row = db_conn.execute(
        "SELECT trades FROM bars_intraday WHERE instrument_id = %s", (iid,)
    ).fetchone()
    assert row == (1,)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_bar_aggregator.py -v -k run_aggregation_loop`
Expected: FAIL — `ImportError: cannot import name 'run_aggregation_loop'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/trading/streaming/bar_aggregator.py`. First, add these imports to the top of the file, alongside the existing ones:

```python
import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import structlog
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from trading.config import get_settings
```

Then append the rest of the module:

```python
log = structlog.get_logger(__name__)

_TICK_PATTERN = "ticks:*"

Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _parse_tick(raw: str) -> Tick | None:
    try:
        return Tick.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001 - a malformed message is skipped, never fatal
        log.warning("bar_aggregator.malformed_message", reason=str(exc), raw=raw[:200])
        return None


async def run_aggregation_loop(
    redis: Redis,
    conn: Connection,
    *,
    interval_seconds: int = INTERVAL_SECONDS,
    flush_check_seconds: float = 5.0,
    sleep: Sleeper = _default_sleep,
    max_bars_written: int | None = None,
) -> None:
    """Subscribe to `ticks:*`, aggregate into bars, write each closed bar.

    Runs forever when `max_bars_written` is None (production). Stops once
    `max_bars_written` bars have been written when it's an int -- a test
    seam, the same shape as `crypto_ingestor.run_ingestion_loop`'s
    `max_ticks`.
    """
    aggregator = BarAggregator(interval_seconds)
    written = 0
    done = asyncio.Event()

    def _write_all(closed_bars: list[ClosedBar]) -> None:
        nonlocal written
        for closed in closed_bars:
            write_closed_bar(conn, closed, interval_seconds=interval_seconds)
            written += 1
        if max_bars_written is not None and written >= max_bars_written:
            done.set()

    async def _consume_ticks(pubsub: PubSub) -> None:
        async for message in pubsub.listen():
            if message["type"] != "pmessage":
                continue
            tick = _parse_tick(message["data"])
            if tick is None:
                continue
            _write_all(aggregator.ingest(tick))
            if done.is_set():
                return

    async def _periodic_flush() -> None:
        while not done.is_set():
            await sleep(flush_check_seconds)
            _write_all(aggregator.flush_stale(datetime.now(UTC)))

    pubsub = redis.pubsub()
    await pubsub.psubscribe(_TICK_PATTERN)
    consumer = asyncio.create_task(_consume_ticks(pubsub))
    flusher = asyncio.create_task(_periodic_flush())
    try:
        if max_bars_written is None:
            await asyncio.gather(consumer, flusher)
        else:
            await done.wait()
    finally:
        consumer.cancel()
        flusher.cancel()
        try:
            await pubsub.punsubscribe()
            # redis-py's PubSub.aclose (unlike Redis.aclose) ships with no
            # type annotations at all -- a real upstream stub gap, matching
            # the same suppression stream_gateway already carries.
            await pubsub.aclose()  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
            log.debug("bar_aggregator.pubsub_cleanup_failed", exc_info=True)
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop, so a
        # caller closing `redis` from a *different* asyncio.run() call later
        # (as short-lived test runs do) never hits a stale, cross-loop
        # connection.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    # autocommit=True: each closed bar is its own independent unit of work
    # over a long-running connection -- unlike seed_instruments.py's
    # one-shot atomic batch, there is no reason one bar's write should roll
    # back because a later bar's write fails. This is also what keeps
    # write_closed_bar() safe to call against `db_conn` in tests without any
    # special-casing: it never commits itself either way.
    conn = psycopg.connect(settings.database_url, autocommit=True)
    log.info("bar_aggregator.starting", interval_seconds=INTERVAL_SECONDS)
    try:
        asyncio.run(run_aggregation_loop(redis, conn))
    except KeyboardInterrupt:
        log.info("bar_aggregator.interrupted")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_bar_aggregator.py -v`
Expected: 14 passed

- [ ] **Step 5: Run the full suite once**

Run: `uv run pytest`
Expected: all passing, no regressions in `crypto_ingestor`/`gateway` tests (this task only adds a new consumer of the existing `ticks:*` channel, doesn't change either publisher).

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean

```bash
git add src/trading/streaming/bar_aggregator.py tests/streaming/test_bar_aggregator.py
git commit -m "feat(streaming): async bar_aggregator loop and CLI entry point"
```

---

## Task 4: End-to-end manual verification

**Files:** none (verification only — nothing here should require a code change; if it does, that's a signal a step above missed something, not a step to improvise around).

Controller-run (Opus), not delegated: same reasoning as the crypto-streaming plan's Task 6 — confirming real bars accumulate from a live feed is a judgment call, not a scripted assertion, even though the check itself is a SQL query rather than a browser.

- [ ] **Step 1: Confirm infrastructure is up**

Run: `docker compose ps`
Expected: `trading_tsdb` and `trading_redis` both healthy.

- [ ] **Step 2: Start (or confirm running) the ingestor**

Run (separate terminal, leave running, skip if already running from the crypto-streaming demo): `uv run python -m trading.streaming.crypto_ingestor`
Expected: `crypto_ingestor.starting` log line, then silence.

- [ ] **Step 3: Start the aggregator**

Run (separate terminal, leave running): `uv run python -m trading.streaming.bar_aggregator`
Expected: `bar_aggregator.starting interval_seconds=60` log line, then silence (closed bars are written, not logged individually).

- [ ] **Step 4: Wait, then query**

Wait at least 3 minutes of wall-clock time (long enough for at least 2 full minute boundaries to close for the actively-trading seeded pairs). Then run:

```bash
uv run python -c "
import psycopg
from trading.config import get_settings
conn = psycopg.connect(get_settings().database_url)
rows = conn.execute('''
    SELECT i.symbol, count(*) AS bars, max(b.ts) AS latest_bar,
           min(b.open) AS sample_open, max(b.close) AS sample_close
    FROM bars_intraday b JOIN instruments i ON i.instrument_id = b.instrument_id
    WHERE i.exchange = 'BINANCE'
    GROUP BY i.symbol ORDER BY bars DESC
''').fetchall()
for row in rows:
    print(row)
"
```

Expected: at least several pairs show `bars >= 2`, `latest_bar` within the last couple of minutes, and `sample_open`/`sample_close` are sane, non-zero, non-null prices matching the pair's real market price (cross-check one against the live proof page or Binance directly). If a pair shows 0 bars after 3+ minutes, check `bar_aggregator`'s terminal for `bar_aggregator.malformed_message` warnings before treating it as a failure — same "check the actual log before assuming breakage" discipline the crypto-streaming plan's Task 6 established. If literally every pair shows 0 bars, something upstream is broken (aggregator not actually subscribed, or `crypto_ingestor` not running).

- [ ] **Step 5: Confirm idempotency under a restart**

Stop `bar_aggregator` (Ctrl-C), wait ~10 seconds, restart it, wait another full minute, then re-run the query from Step 4. Expected: bar counts only ever increase (no duplicate rows for the same `(instrument_id, ts, interval_sec)` — the `ON CONFLICT DO UPDATE` from Task 2 is what guarantees this), and the small gap during the restart (the in-progress bucket discarded on shutdown, per this plan's Global Constraints) is visible as at most one missing minute per instrument, not a crash or a corrupted row.

- [ ] **Step 6: Record the report**

Record in the task's completion notes: which pairs were observed accumulating bars, over what wall-clock window, and paste 2-3 example rows from the Step 4 query as evidence — the same standard Phase 0's Task 17 report and the crypto-streaming plan's Task 6 both held themselves to.

- [ ] **Step 7: Stop the processes**

Ctrl-C both `bar_aggregator` and `crypto_ingestor` (unless you're leaving the crypto-streaming demo running for other reasons). No commit for this task — it verifies Tasks 1-3's commits, it doesn't add its own.
