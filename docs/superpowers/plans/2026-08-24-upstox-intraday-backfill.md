# Upstox Intraday Candle Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backfill `bars_intraday` with 1-minute NSE equity candles for the 5-symbol Upstox watchlist from Upstox's V3 historical-candle REST API, going back to January 2022 (the API's own floor for 1-minute data), so backtests have intraday history predating whenever `upstox_ingestor` first runs live.

**Architecture:** A new synchronous, one-shot CLI module walks each watchlist symbol's history in month-sized windows (the API's per-request cap for 1-minute candles), fetching, parsing, and upserting into the same `bars_intraday` table `bar_aggregator` writes to — same primary key, same upsert shape, distinct provenance (`DataSource.UPSTOX_HISTORICAL_CANDLE`). It does not reuse `bar_aggregator`'s tick-accumulation types (`OpenBar`/`ClosedBar`/`write_closed_bar`); a REST-sourced candle has no trade count, so this module gets its own small, honest upsert instead of stretching that abstraction.

**Tech Stack:** Python 3.12 (via `uv`) · `httpx` (already a dependency, sync `Client`) · `psycopg` (sync `Connection`) · Pydantic v2 Settings · pytest

**Spec:** [`docs/superpowers/specs/2026-08-24-upstox-intraday-backfill-design.md`](../specs/2026-08-24-upstox-intraday-backfill-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** exactly, managed by `uv`.
- **Money is never a float.** All `Decimal` conversions from the API's JSON numbers use `Decimal(str(value))`, never `Decimal(float_value)`.
- **Timestamps are timezone-aware UTC.** The historical-candle API returns ISO-8601 strings with a `+05:30` offset already applied (e.g. `"2024-01-02T09:15:00+05:30"`) — **not** epoch milliseconds like the WS feed's `ltt`. Convert via `datetime.fromisoformat(value).astimezone(UTC)`.
- **Synchronous throughout** — `httpx.Client` (not `AsyncClient`), `psycopg.Connection` (not the `redis.asyncio`-style async pattern `upstox_ingestor` uses). This is a one-shot sequential batch walk, not concurrent stream I/O.
- **`fetch_candles` takes an injected `httpx.Client`** so tests build one on `httpx.MockTransport` with canned responses — no real network in the default test run, except the one `@pytest.mark.live` test (already registered in `pyproject.toml`, excluded by default).
- **A malformed/unexpected API response shape is a loud `ValueError`, not a silent skip** — unlike the WS firehose's per-frame "log and skip," a REST response is one deliberate, retryable request; a shape mismatch means the API contract changed and every subsequent window would fail identically, so it should surface immediately.
- **A 401/403 response aborts the entire backfill immediately** — it is not a per-window transient failure to retry-then-skip; every remaining request would fail the same way.
- **A transient per-window failure (timeout, 5xx) retries once after a 2-second backoff, then is logged and skipped** (recorded in a report), and the walk continues to the next window.
- **`trades` is always written as `NULL`** for backfilled rows — the API gives no trade count; writing `0` would falsely claim "zero trades occurred."
- **Lint/type gate every task:** `ruff check . && ruff format --check . && mypy src` must pass before any commit.
- **Every task ends with a passing `pytest` run (default invocation, live tests excluded) and a commit.**

---

## File Structure

```
src/trading/contracts/enums.py                  + DataSource.UPSTOX_HISTORICAL_CANDLE = 7
migrations/versions/0004_upstox_historical_candle_source.py   new migration
src/trading/config.py                           + Settings.upstox_access_token

src/trading/streaming/upstox_intraday_backfill.py
  month_windows()
  BackfillCandle
  parse_candle_response()
  fetch_candles()
  write_backfill_candle()
  BackfillReport
  backfill_symbol()
  main()

tests/streaming/test_upstox_intraday_backfill.py
```

Dependency order: **Task 1** (enum + migration + Settings field) has no
dependency on anything else. **Task 2** (`month_windows`) and **Task 3**
(`BackfillCandle` + `parse_candle_response`) are both pure and independent
of each other and of Task 1. **Task 4** (`fetch_candles`) depends on
nothing but `httpx`. **Task 5** (`write_backfill_candle`) depends on
Task 1 (the enum + migrated `data_sources` row) and Task 3 (`BackfillCandle`).
**Task 6** (`backfill_symbol` + `main`) depends on Tasks 2, 3, 4, 5.
**Task 7** (running the real backfill) depends on everything and requires
a real `UPSTOX_ACCESS_TOKEN`.

```
Task 1 (enum + migration + settings)
Task 2 (month_windows)          ─┐
Task 3 (BackfillCandle + parse) ─┼── Task 5 (write_backfill_candle, needs Task 1 + Task 3)
Task 4 (fetch_candles)          ─┘
                                   Task 6 (backfill_symbol + main, needs 2,3,4,5) ── Task 7 (live run)
```

**AI-tier delegation:** Task 1 is small, mechanical schema/config plumbing — cheap tier. Tasks 2-4 are pure functions with complete specs — cheap tier. Task 5 is a small DB upsert with a clear, complete spec — cheap tier. Task 6 has real judgment (retry/abort error-handling branches, orchestration) — standard tier. Task 7 is controller-run: it performs real, cost-bearing writes against a real API with a real (currently unminted) token, and needs a human decision about when to actually run ~280 requests against production.

---

## Task 1: `DataSource.UPSTOX_HISTORICAL_CANDLE`, migration, `Settings.upstox_access_token`

**Files:**
- Modify: `src/trading/contracts/enums.py`
- Create: `migrations/versions/0004_upstox_historical_candle_source.py`
- Modify: `src/trading/config.py`
- Test: `tests/streaming/test_upstox_intraday_backfill_migration.py`

**Interfaces:**
- Produces: `DataSource.UPSTOX_HISTORICAL_CANDLE` (value `7`). A `data_sources` row `(7, 'UPSTOX_HISTORICAL_CANDLE')`. `Settings.upstox_access_token: str | None` (from env var `UPSTOX_ACCESS_TOKEN` — the same var `trading.auth.upstox` already mints and writes to `.env.local`; this task only exposes it on `Settings`, it doesn't change how it's minted).

- [ ] **Step 1: Add the enum value**

Edit `src/trading/contracts/enums.py`, appending after the existing values (keep everything else unchanged):

```python
class DataSource(IntEnum):
    """Persisted provenance codes. Append only; never renumber."""

    NSE_CM_UDIFF = 1
    NSE_FO_UDIFF = 2
    BSE_CM_UDIFF = 3
    NSE_CM_LEGACY = 4
    AMFI_NAV = 5
    BINANCE_WS = 6
    UPSTOX_HISTORICAL_CANDLE = 7
```

- [ ] **Step 2: Write the failing migration test**

Create `tests/streaming/test_upstox_intraday_backfill_migration.py`:

```python
from __future__ import annotations

import pytest

pytestmark = pytest.mark.db


def test_upstox_historical_candle_source_row_exists(db_conn):
    row = db_conn.execute(
        "SELECT source_key FROM data_sources WHERE source_id = 7"
    ).fetchone()
    assert row is not None
    assert row[0] == "UPSTOX_HISTORICAL_CANDLE"
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill_migration.py -v`
Expected: FAIL — no row with `source_id = 7` (migration not yet written/applied).

- [ ] **Step 4: Write the migration**

Create `migrations/versions/0004_upstox_historical_candle_source.py`:

```python
"""Seed data_sources with UPSTOX_HISTORICAL_CANDLE.

Part of the Upstox intraday backfill sub-project (docs/superpowers/specs/
2026-08-24-upstox-intraday-backfill-design.md). Mirrors migration 0003's
seeding of (6, 'BINANCE_WS') -- pure metadata insert, no table changes.

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-24
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "INSERT INTO data_sources (source_id, source_key) VALUES (7, 'UPSTOX_HISTORICAL_CANDLE') "
        "ON CONFLICT (source_id) DO NOTHING"
    )


def downgrade() -> None:
    op.execute("DELETE FROM data_sources WHERE source_id = 7")
```

Run: `uv run alembic upgrade head`
Expected: migration applies cleanly.

- [ ] **Step 5: Run the migration test to verify it passes**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill_migration.py -v`
Expected: 1 passed

- [ ] **Step 6: Add the Settings field**

Edit `src/trading/config.py`, adding one field alongside the existing Upstox settings (keep everything else unchanged):

```python
    upstox_api_key: str | None = None
    upstox_api_secret: str | None = None
    upstox_analytics_token: str | None = None
    upstox_access_token: str | None = None
    dhan_client_id: str | None = None
    dhan_access_token: str | None = None
```

- [ ] **Step 7: Run the full suite once, lint/type gate, and commit**

Run: `uv run pytest` (default invocation)
Expected: all passing, no regressions (this task adds one new test, touches no other module's behavior).

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`
Expected: all clean.

```bash
git add src/trading/contracts/enums.py migrations/versions/0004_upstox_historical_candle_source.py \
        src/trading/config.py tests/streaming/test_upstox_intraday_backfill_migration.py
git commit -m "feat(streaming): add UPSTOX_HISTORICAL_CANDLE data source and access-token setting"
```

---

## Task 2: `month_windows()` — pure date-range chunking

**Files:**
- Create: `src/trading/streaming/upstox_intraday_backfill.py` (this task starts the file; later tasks append to it)
- Test: `tests/streaming/test_upstox_intraday_backfill.py` (this task starts the file; later tasks append to it)

**Interfaces:**
- Produces: `month_windows(start: date, end: date) -> list[tuple[date, date]]`. Splits `[start, end]` into calendar-month-aligned `(from_date, to_date)` pairs, each pair's span never exceeding one calendar month, in chronological order. If `start > end`, returns `[]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_upstox_intraday_backfill.py`:

```python
from __future__ import annotations

from datetime import date

from trading.streaming.upstox_intraday_backfill import month_windows


def test_month_windows_single_full_month():
    windows = month_windows(date(2022, 1, 1), date(2022, 1, 31))
    assert windows == [(date(2022, 1, 1), date(2022, 1, 31))]


def test_month_windows_spans_multiple_months():
    windows = month_windows(date(2022, 1, 1), date(2022, 3, 15))
    assert windows == [
        (date(2022, 1, 1), date(2022, 1, 31)),
        (date(2022, 2, 1), date(2022, 2, 28)),
        (date(2022, 3, 1), date(2022, 3, 15)),
    ]


def test_month_windows_partial_first_month():
    windows = month_windows(date(2022, 1, 20), date(2022, 2, 10))
    assert windows == [
        (date(2022, 1, 20), date(2022, 1, 31)),
        (date(2022, 2, 1), date(2022, 2, 10)),
    ]


def test_month_windows_single_day():
    windows = month_windows(date(2024, 6, 15), date(2024, 6, 15))
    assert windows == [(date(2024, 6, 15), date(2024, 6, 15))]


def test_month_windows_empty_when_start_after_end():
    assert month_windows(date(2024, 1, 1), date(2023, 12, 31)) == []


def test_month_windows_handles_december_to_january_rollover():
    windows = month_windows(date(2022, 12, 15), date(2023, 1, 15))
    assert windows == [
        (date(2022, 12, 15), date(2022, 12, 31)),
        (date(2023, 1, 1), date(2023, 1, 15)),
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.upstox_intraday_backfill'`

- [ ] **Step 3: Write minimal implementation**

Create `src/trading/streaming/upstox_intraday_backfill.py`:

```python
"""Upstox V3 historical-candle backfill for the NSE equity watchlist.

One-shot, synchronous CLI: walks each watchlist symbol's history in
month-sized windows (Upstox's per-request cap for 1-minute candles) from
2022-01-01 (the API's own floor for 1-minute data) through yesterday,
upserting into `bars_intraday` under `DataSource.UPSTOX_HISTORICAL_CANDLE`.

Deliberately does not reuse `bar_aggregator`'s `OpenBar`/`ClosedBar`/
`write_closed_bar` -- those model tick accumulation (a running trade count),
which a pre-aggregated REST candle doesn't have. See this module's own
`write_backfill_candle` instead.

Usage: uv run python -m trading.streaming.upstox_intraday_backfill
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month-aligned (from_date, to_date)
    pairs, each spanning at most one calendar month -- Upstox's V3
    historical-candle API caps 1-minute candle requests at ~1 month per
    call. Empty list if start > end."""
    if start > end:
        return []

    windows: list[tuple[date, date]] = []
    window_start = start
    while window_start <= end:
        last_day_of_month = calendar.monthrange(window_start.year, window_start.month)[1]
        month_end = date(window_start.year, window_start.month, last_day_of_month)
        window_end = min(month_end, end)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 6 passed

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "feat(streaming): month_windows() date-range chunking for the intraday backfill"
```

---

## Task 3: `BackfillCandle` and `parse_candle_response()`

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py` (append to Task 2's file)
- Test: `tests/streaming/test_upstox_intraday_backfill.py` (append to Task 2's file)

**Interfaces:**
- Produces: `BackfillCandle` — a frozen dataclass: `instrument_id: int`, `ts: datetime` (UTC), `open: Decimal`, `high: Decimal`, `low: Decimal`, `close: Decimal`, `volume: Decimal`, `open_interest: int | None`. `parse_candle_response(payload: dict, instrument_id: int) -> list[BackfillCandle]` — parses Upstox's `{"status": ..., "data": {"candles": [[ts, o, h, l, c, v, oi], ...]}}` shape. Raises `ValueError` (not a silent skip) if `payload` doesn't have a `data.candles` list, or if any candle row doesn't have exactly 7 elements — a shape mismatch means the API contract changed, and every subsequent window in this backfill run would fail the same way, so it must surface immediately rather than be swallowed per-row. Returns `[]` for a response whose `candles` list is empty (a real, valid "no data in this window" case, distinct from a malformed response).

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
from datetime import UTC
from decimal import Decimal

import pytest

from trading.streaming.upstox_intraday_backfill import BackfillCandle, parse_candle_response


def test_parse_candle_response_builds_candles_from_a_valid_payload():
    payload = {
        "status": "success",
        "data": {
            "candles": [
                ["2024-01-02T09:15:00+05:30", 2456.5, 2460.0, 2455.0, 2458.25, 12345, 0],
                ["2024-01-02T09:16:00+05:30", 2458.25, 2459.0, 2457.0, 2457.5, 6789, 0],
            ]
        },
    }

    candles = parse_candle_response(payload, instrument_id=501)

    assert len(candles) == 2
    first = candles[0]
    assert first == BackfillCandle(
        instrument_id=501,
        ts=datetime(2024, 1, 2, 3, 45, tzinfo=UTC),
        open=Decimal("2456.5"),
        high=Decimal("2460.0"),
        low=Decimal("2455.0"),
        close=Decimal("2458.25"),
        volume=Decimal("12345"),
        open_interest=0,
    )


def test_parse_candle_response_converts_ist_offset_to_utc():
    payload = {
        "status": "success",
        "data": {"candles": [["2024-06-15T15:29:00+05:30", 100.0, 101.0, 99.0, 100.5, 1, 0]]},
    }

    candles = parse_candle_response(payload, instrument_id=1)

    assert candles[0].ts == datetime(2024, 6, 15, 9, 59, tzinfo=UTC)


def test_parse_candle_response_returns_empty_list_for_no_candles_in_window():
    payload = {"status": "success", "data": {"candles": []}}

    assert parse_candle_response(payload, instrument_id=1) == []


def test_parse_candle_response_raises_when_data_key_is_missing():
    with pytest.raises(ValueError, match="candles"):
        parse_candle_response({"status": "success"}, instrument_id=1)


def test_parse_candle_response_raises_when_a_candle_row_has_the_wrong_arity():
    payload = {"status": "success", "data": {"candles": [["2024-01-02T09:15:00+05:30", 1.0, 2.0]]}}

    with pytest.raises(ValueError, match="7"):
        parse_candle_response(payload, instrument_id=1)
```

Also add `from datetime import datetime` to this test file's existing `from datetime import UTC, date` import line (so it reads `from datetime import UTC, date, datetime`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: FAIL — `ImportError: cannot import name 'BackfillCandle'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/trading/streaming/upstox_intraday_backfill.py` (add these imports to the existing `from __future__ import annotations` block at the top, merging with Task 2's `import calendar` / `from datetime import date, timedelta`):

```python
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
```

Then append the new code:

```python
@dataclass(frozen=True)
class BackfillCandle:
    instrument_id: int
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_interest: int | None


def parse_candle_response(payload: dict, instrument_id: int) -> list[BackfillCandle]:
    """Parse one historical-candle API response into BackfillCandles.

    Raises ValueError for a response shape that doesn't match the
    documented contract -- not a silent skip. A REST response is one
    deliberate, retryable request; a shape mismatch means the API contract
    changed, and every subsequent window in this run would fail identically,
    so it must surface immediately rather than be swallowed row by row.
    """
    data = payload.get("data")
    if not isinstance(data, dict) or "candles" not in data:
        raise ValueError(f"response missing data.candles: {payload!r}")

    raw_candles = data["candles"]
    candles: list[BackfillCandle] = []
    for row in raw_candles:
        if len(row) != 7:
            raise ValueError(f"expected 7 elements per candle row, got {len(row)}: {row!r}")
        ts_str, open_, high, low, close, volume, open_interest = row
        candles.append(
            BackfillCandle(
                instrument_id=instrument_id,
                ts=datetime.fromisoformat(ts_str).astimezone(UTC),
                open=Decimal(str(open_)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=Decimal(str(volume)),
                open_interest=int(open_interest) if open_interest is not None else None,
            )
        )
    return candles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 11 passed (6 from Task 2 + 5 new)

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "feat(streaming): BackfillCandle and parse_candle_response() for the intraday backfill"
```

---

## Task 4: `fetch_candles()` — injectable HTTP client wrapper

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py` (append)
- Test: `tests/streaming/test_upstox_intraday_backfill.py` (append)

**Interfaces:**
- Produces: `fetch_candles(client: httpx.Client, instrument_key: str, from_date: date, to_date: date, token: str) -> dict`. Issues `GET https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/1/{to_date}/{from_date}` with header `Authorization: Bearer {token}`, `Accept: application/json`. Returns the parsed JSON body on a 2xx response. Raises `httpx.HTTPStatusError` on any non-2xx (via `response.raise_for_status()`) — callers (Task 6) distinguish 401/403 from other statuses by inspecting the exception's `response.status_code`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
from datetime import date

import httpx

from trading.streaming.upstox_intraday_backfill import fetch_candles


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_candles_issues_the_documented_request_and_returns_json():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"status": "success", "data": {"candles": []}})

    client = _mock_client(handler)

    result = fetch_candles(
        client,
        instrument_key="NSE_EQ|INE002A01018",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 31),
        token="tok123",
    )

    assert result == {"status": "success", "data": {"candles": []}}
    assert captured["auth"] == "Bearer tok123"
    assert (
        captured["url"]
        == "https://api.upstox.com/v3/historical-candle/NSE_EQ|INE002A01018/minutes/1/2024-01-31/2024-01-01"
    )


def test_fetch_candles_raises_on_non_2xx():
    client = _mock_client(lambda request: httpx.Response(401, json={"error": "invalid token"}))

    with pytest.raises(httpx.HTTPStatusError):
        fetch_candles(
            client,
            instrument_key="NSE_EQ|INE002A01018",
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 31),
            token="badtoken",
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: FAIL — `ImportError: cannot import name 'fetch_candles'`

- [ ] **Step 3: Write minimal implementation**

Add `import httpx` to the imports at the top of `src/trading/streaming/upstox_intraday_backfill.py`, then append:

```python
_BASE_URL = "https://api.upstox.com"


def fetch_candles(
    client: httpx.Client, instrument_key: str, from_date: date, to_date: date, token: str
) -> dict:
    """One GET against Upstox's V3 historical-candle endpoint for 1-minute
    candles. Raises httpx.HTTPStatusError on any non-2xx response --
    callers distinguish 401/403 (abort the whole backfill) from other
    statuses (retry-then-skip this window) via the exception's
    response.status_code."""
    url = (
        f"{_BASE_URL}/v3/historical-candle/{instrument_key}/minutes/1/"
        f"{to_date.isoformat()}/{from_date.isoformat()}"
    )
    response = client.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 13 passed

- [ ] **Step 5: Write and run the live shape-check test**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
@pytest.mark.live
def test_live_fetch_candles_matches_the_documented_response_shape():
    """One real request against Upstox's historical-candle API, confirming
    the response shape this module assumes. Excluded from the default run.
    Requires a real, currently-valid UPSTOX_ACCESS_TOKEN (minted via
    `uv run python -m trading.auth.upstox`) -- skips cleanly if unset."""
    from trading.config import get_settings

    token = get_settings().upstox_access_token
    if not token:
        pytest.skip("UPSTOX_ACCESS_TOKEN not set")

    with httpx.Client(timeout=10.0) as client:
        payload = fetch_candles(
            client,
            instrument_key="NSE_EQ|INE002A01018",  # RELIANCE
            from_date=date(2024, 1, 2),
            to_date=date(2024, 1, 2),
            token=token,
        )

    assert payload["status"] == "success"
    candles = payload["data"]["candles"]
    assert isinstance(candles, list)
    if candles:
        assert len(candles[0]) == 7
```

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -m live`
Expected: if `UPSTOX_ACCESS_TOKEN` is set in `.env`/`.env.local`, 1 passed, confirming the real response shape matches §2 of the spec. If unset, 1 skipped — **do not treat a skip here as a blocker**; report which outcome occurred and move on. (As of this plan's writing, `UPSTOX_ACCESS_TOKEN` is not yet minted — running `uv run python -m trading.auth.upstox` requires an interactive browser OAuth flow only the human operator can complete. If it's still unset when you reach this step, skip is the expected, correct outcome — not a task failure.)

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "feat(streaming): fetch_candles() Upstox historical-candle HTTP wrapper"
```

---

## Task 5: `write_backfill_candle()` — DB upsert

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py` (append)
- Test: `tests/streaming/test_upstox_intraday_backfill.py` (append)

**Interfaces:**
- Consumes: `BackfillCandle` (Task 3), `DataSource.UPSTOX_HISTORICAL_CANDLE` (Task 1), `bar_aggregator.INTERVAL_SECONDS` (existing, value `60`).
- Produces: `write_backfill_candle(conn: Connection, candle: BackfillCandle) -> None`. Upserts one row into `bars_intraday` keyed on `(instrument_id, ts, interval_sec)`, `source = DataSource.UPSTOX_HISTORICAL_CANDLE`, `trades = NULL` always. Never calls `conn.commit()` (test-safe against `db_conn`'s rollback-at-teardown; production commits via an `autocommit=True` connection in Task 6's `main()`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal

from trading.contracts import DataSource
from trading.streaming.upstox_intraday_backfill import BackfillCandle, write_backfill_candle


@pytest.fixture
def fixture_instrument_id(db_conn) -> int:
    """`db_conn` hands out a rolled-back transaction on a freshly migrated,
    otherwise-empty `trading_test` database (tests/conftest.py) -- there is
    no pre-seeded instrument row to reuse. This inserts one minimal, real
    `instruments` row so `bars_intraday`'s foreign key has something to
    resolve against, and returns its generated `instrument_id`."""
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('EQUITY', 'NSE', 'CM', 'RELIANCE', 'ACTIVE', 'NSE:CM:RELIANCE:EQ')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


@pytest.mark.db
def test_write_backfill_candle_inserts_a_row_with_null_trades_and_correct_source(
    db_conn, fixture_instrument_id
):
    candle = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 45, tzinfo=UTC),
        open=Decimal("100.5"),
        high=Decimal("101.0"),
        low=Decimal("99.5"),
        close=Decimal("100.75"),
        volume=Decimal("12345"),
        open_interest=0,
    )

    write_backfill_candle(db_conn, candle)

    row = db_conn.execute(
        "SELECT open, high, low, close, volume, trades, source, open_interest "
        "FROM bars_intraday WHERE instrument_id = %s AND ts = %s AND interval_sec = 60",
        (fixture_instrument_id, datetime(2024, 1, 2, 3, 45, tzinfo=UTC)),
    ).fetchone()

    assert row is not None
    assert row[0] == Decimal("100.5")
    assert row[4] == Decimal("12345")
    assert row[5] is None  # trades always NULL for backfilled rows
    assert row[6] == DataSource.UPSTOX_HISTORICAL_CANDLE
    assert row[7] == 0


@pytest.mark.db
def test_write_backfill_candle_upserts_rather_than_duplicates(db_conn, fixture_instrument_id):
    candle = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 46, tzinfo=UTC),
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("1"),
        open_interest=None,
    )
    write_backfill_candle(db_conn, candle)

    updated = BackfillCandle(
        instrument_id=fixture_instrument_id,
        ts=datetime(2024, 1, 2, 3, 46, tzinfo=UTC),
        open=Decimal("2"),
        high=Decimal("2"),
        low=Decimal("2"),
        close=Decimal("2"),
        volume=Decimal("2"),
        open_interest=None,
    )
    write_backfill_candle(db_conn, updated)

    rows = db_conn.execute(
        "SELECT close FROM bars_intraday WHERE instrument_id = %s AND ts = %s AND interval_sec = 60",
        (fixture_instrument_id, datetime(2024, 1, 2, 3, 46, tzinfo=UTC)),
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == Decimal("2")
```

`fixture_instrument_id` is defined once here (Task 5) and reused by Task 6's tests below — both tasks append to the same test file, so it's in scope throughout.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -m db`
Expected: FAIL — `ImportError: cannot import name 'write_backfill_candle'`

- [ ] **Step 3: Write minimal implementation**

Add these imports to the top of `src/trading/streaming/upstox_intraday_backfill.py`:

```python
from psycopg import Connection

from trading.contracts import DataSource
from trading.streaming.bar_aggregator import INTERVAL_SECONDS
```

Then append:

```python
_UPSERT_BACKFILL_CANDLE = """
    INSERT INTO bars_intraday
        (instrument_id, ts, interval_sec, open, high, low, close, volume, trades, open_interest, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        open_interest = EXCLUDED.open_interest, source = EXCLUDED.source
"""


def write_backfill_candle(conn: Connection, candle: BackfillCandle) -> None:
    """Upsert one backfilled candle into bars_intraday. trades is always
    written NULL -- the historical-candle API gives no trade count, and
    writing 0 would falsely claim zero trades occurred. Never commits; the
    caller controls transaction boundaries (see this module's main())."""
    conn.execute(
        _UPSERT_BACKFILL_CANDLE,
        (
            candle.instrument_id,
            candle.ts,
            INTERVAL_SECONDS,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.open_interest,
            DataSource.UPSTOX_HISTORICAL_CANDLE,
        ),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -m db`
Expected: 2 passed

Run the full file once: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 15 passed, 1 deselected (or 16 passed if the live test ran because a token was set)

- [ ] **Step 5: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "feat(streaming): write_backfill_candle() upsert into bars_intraday"
```

---

## Task 6: `backfill_symbol()` orchestration and `main()` CLI

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py` (append)
- Test: `tests/streaming/test_upstox_intraday_backfill.py` (append)

**Interfaces:**
- Consumes: `month_windows` (Task 2), `parse_candle_response`/`BackfillCandle` (Task 3), `fetch_candles` (Task 4), `write_backfill_candle` (Task 5), `seed_upstox_instrument_keys` (existing, from `trading.streaming.seed_upstox_instruments`), `get_settings` (existing).
- Produces: `BackfillReport` — a dataclass: `instrument_key: str`, `candles_written: int`, `skipped_windows: list[tuple[date, date]]`. `backfill_symbol(conn: Connection, client: httpx.Client, instrument_key: str, instrument_id: int, token: str, *, start: date = date(2022, 1, 1), end: date | None = None, sleep: Callable[[float], None] = time.sleep) -> BackfillReport`. `main() -> None` — CLI entry point.

`end=None` means "yesterday" (`date.today() - timedelta(days=1)`), computed once at call time — not baked into the default argument (a `date`-typed default would freeze at import time, which is wrong for a function meant to run on different days). `sleep` is a test seam (same pattern this project uses elsewhere for injecting a no-op or recording sleep function in tests) defaulting to real `time.sleep`, called with `REQUEST_DELAY_SECONDS` (`0.3`) between successful window fetches — not before the first request and not after a window that's about to abort the whole run.

Retry/abort logic for each window's `fetch_candles` call:
1. Try once. On success, `parse_candle_response` + `write_backfill_candle` each candle, sleep, continue to next window.
2. On `httpx.HTTPStatusError` with `status_code` 401 or 403: re-raise immediately (aborts `backfill_symbol`, and therefore `main()`'s whole run) — do not catch this at any outer level, it must propagate.
3. On any other `httpx.HTTPStatusError`, `httpx.TimeoutException`, or `httpx.TransportError`: sleep 2 seconds, retry once. If the retry also fails with a non-401/403 error, append `(from_date, to_date)` to `skipped_windows`, log a warning, and continue to the next window. If the retry fails with 401/403, re-raise (same as point 2).

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
from trading.streaming.upstox_intraday_backfill import BackfillReport, backfill_symbol


def test_backfill_symbol_writes_candles_across_windows_and_reports_count(
    db_conn, fixture_instrument_id
):
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {"candles": [["2022-01-15T09:15:00+05:30", 1.0, 1.0, 1.0, 1.0, 1, 0]]},
            },
        )

    client = _mock_client(handler)
    sleeps: list[float] = []

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 1, 31),
        sleep=sleeps.append,
    )

    assert call_count["n"] == 1  # single-month window
    assert report.candles_written == 1
    assert report.skipped_windows == []
    assert report.instrument_key == "NSE_EQ|INE002A01018"


def test_backfill_symbol_retries_once_then_skips_on_persistent_transient_failure(
    db_conn, fixture_instrument_id
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server error")

    client = _mock_client(handler)
    sleeps: list[float] = []

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 1, 31),
        sleep=sleeps.append,
    )

    assert report.candles_written == 0
    assert report.skipped_windows == [(date(2022, 1, 1), date(2022, 1, 31))]
    assert 2.0 in sleeps  # the retry backoff


def test_backfill_symbol_aborts_immediately_on_401(db_conn, fixture_instrument_id):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad token"})

    client = _mock_client(handler)

    with pytest.raises(httpx.HTTPStatusError):
        backfill_symbol(
            db_conn,
            client,
            instrument_key="NSE_EQ|INE002A01018",
            instrument_id=fixture_instrument_id,
            token="badtoken",
            start=date(2022, 1, 1),
            end=date(2022, 3, 31),
            sleep=lambda _: None,
        )


def test_backfill_symbol_continues_past_a_skipped_window_to_the_next(db_conn, fixture_instrument_id):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "2022-01-31" in str(request.url):
            return httpx.Response(500, text="server error")
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {"candles": [["2022-02-01T09:15:00+05:30", 1.0, 1.0, 1.0, 1.0, 1, 0]]},
            },
        )

    client = _mock_client(handler)

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2022, 1, 1),
        end=date(2022, 2, 28),
        sleep=lambda _: None,
    )

    assert report.candles_written == 1
    assert report.skipped_windows == [(date(2022, 1, 1), date(2022, 1, 31))]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: FAIL — `ImportError: cannot import name 'backfill_symbol'`

- [ ] **Step 3: Write minimal implementation**

Add these imports to the top of `src/trading/streaming/upstox_intraday_backfill.py`:

```python
import time
from collections.abc import Callable
from dataclasses import field

import psycopg
import structlog

from trading.streaming.seed_upstox_instruments import seed_upstox_instrument_keys
from trading.config import get_settings

log = structlog.get_logger(__name__)

REQUEST_DELAY_SECONDS = 0.3
_RETRY_BACKOFF_SECONDS = 2.0
```

Then append:

```python
@dataclass
class BackfillReport:
    instrument_key: str
    candles_written: int = 0
    skipped_windows: list[tuple[date, date]] = field(default_factory=list)


def backfill_symbol(
    conn: Connection,
    client: httpx.Client,
    instrument_key: str,
    instrument_id: int,
    token: str,
    *,
    start: date = date(2022, 1, 1),
    end: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> BackfillReport:
    """Backfill one symbol's history from `start` through `end` (default:
    yesterday). Retries a transient per-window failure once after a 2s
    backoff, then skips that window and continues. A 401/403 aborts the
    whole run immediately (propagates) -- every remaining request would
    fail identically, so retry-then-skip would be pointless."""
    window_end = end if end is not None else date.today() - timedelta(days=1)
    report = BackfillReport(instrument_key=instrument_key)

    for from_date, to_date in month_windows(start, window_end):
        payload = _fetch_window_with_retry(
            client, instrument_key, from_date, to_date, token, report, sleep
        )
        if payload is None:
            continue

        for candle in parse_candle_response(payload, instrument_id):
            write_backfill_candle(conn, candle)
            report.candles_written += 1
        sleep(REQUEST_DELAY_SECONDS)

    return report


def _fetch_window_with_retry(
    client: httpx.Client,
    instrument_key: str,
    from_date: date,
    to_date: date,
    token: str,
    report: BackfillReport,
    sleep: Callable[[float], None],
) -> dict | None:
    """One month-window fetch with one retry on a transient failure.
    Returns None (and records the window in `report.skipped_windows`) if
    both attempts fail transiently. Re-raises immediately on 401/403 --
    every remaining request in this backfill run would fail identically,
    so retry-then-skip would be pointless."""
    for attempt in (1, 2):
        try:
            return fetch_candles(client, instrument_key, from_date, to_date, token)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
                raise
        except (httpx.TimeoutException, httpx.TransportError):
            pass

        if attempt == 1:
            log.warning(
                "upstox_intraday_backfill.window_failed_retrying",
                instrument_key=instrument_key,
                from_date=str(from_date),
                to_date=str(to_date),
            )
            sleep(_RETRY_BACKOFF_SECONDS)
        else:
            log.warning(
                "upstox_intraday_backfill.window_skipped",
                instrument_key=instrument_key,
                from_date=str(from_date),
                to_date=str(to_date),
            )
            report.skipped_windows.append((from_date, to_date))

    return None


def main() -> None:
    settings = get_settings()
    token = settings.upstox_access_token
    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is not set. Run `uv run python -m trading.auth.upstox` to mint "
            "one (it expires daily, so do this right before running this backfill)."
        )

    conn = psycopg.connect(settings.database_url, autocommit=True)
    try:
        instrument_ids = seed_upstox_instrument_keys(conn)
        client = httpx.Client(timeout=30.0)
        try:
            reports: list[BackfillReport] = []
            for instrument_key, instrument_id in instrument_ids.items():
                log.info("upstox_intraday_backfill.starting_symbol", instrument_key=instrument_key)
                report = backfill_symbol(conn, client, instrument_key, instrument_id, token)
                reports.append(report)
                log.info(
                    "upstox_intraday_backfill.finished_symbol",
                    instrument_key=instrument_key,
                    candles_written=report.candles_written,
                    skipped_windows=len(report.skipped_windows),
                )
        finally:
            client.close()
    finally:
        conn.close()

    total_written = sum(r.candles_written for r in reports)
    total_skipped = sum(len(r.skipped_windows) for r in reports)
    print(f"Backfill complete: {total_written} candles written, {total_skipped} windows skipped")
    for report in reports:
        if report.skipped_windows:
            print(f"  {report.instrument_key}: skipped {report.skipped_windows}")

    if total_skipped:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
```

Note: this task's implementer should consolidate the imports added across
Tasks 2-6 into one clean import block at the top of the file (`calendar`,
`time`, stdlib `dataclasses`/`datetime`/`decimal`/`collections.abc`, then
third-party `httpx`/`structlog`/`psycopg`, then this repo's own modules) —
the step-by-step instructions above added them incrementally per task for
clarity, but the finished file should read as one coherent module, not a
patchwork. This is a normal expectation, not extra scope.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 19 passed, 1 deselected (or 20 passed if the live test ran)

- [ ] **Step 5: Run the full suite once**

Run: `uv run pytest`
Expected: all passing, no regressions anywhere else in the repo.

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "feat(streaming): backfill_symbol() orchestration and CLI entry point"
```

---

## Task 7: Run the real backfill (controller-run, requires a minted token)

**Files:** none (this task runs the shipped code against the real API and real database — nothing here should require a code change; if it does, that's a signal a step above missed something, not a step to improvise around).

Controller-run, not delegated — this performs real writes against the production database using a real, expiring access token, and burns real requests against Upstox's API (~280 requests across 5 symbols for the full 2022-01 → yesterday range). Unlike Task 5 of the upstox-ingestion plan, this does **not** need NSE market hours — historical data is available any time — but it does need a currently-valid `UPSTOX_ACCESS_TOKEN`, which is **not minted as of this plan's writing** (only `UPSTOX_ANALYTICS_TOKEN`, `UPSTOX_API_KEY`, `UPSTOX_API_SECRET` are set in `.env.local`).

- [ ] **Step 1: Mint a fresh access token**

Run: `uv run python -m trading.auth.upstox`
This opens a browser OAuth flow — **requires human interaction to log in and approve**, cannot be done autonomously. Confirms afterward: `uv run python -c "from trading.config import get_settings; print(bool(get_settings().upstox_access_token))"` should print `True`.

- [ ] **Step 2: Confirm infrastructure**

Run: `docker compose ps` — expect `trading_tsdb` healthy (this task doesn't need Redis).

- [ ] **Step 3: Run the backfill**

Run: `uv run python -m trading.streaming.upstox_intraday_backfill`
This will take a while (~280 requests × ~0.3-2s each ≈ several minutes to ~15 minutes, depending on retries). Watch the log output for `upstox_intraday_backfill.finished_symbol` lines per symbol.

- [ ] **Step 4: Verify and record**

Query, similar shape to the bar-aggregator and upstox-ingestion plans' own verification steps:

```bash
uv run python -c "
import psycopg
from trading.config import get_settings
conn = psycopg.connect(get_settings().database_url)
rows = conn.execute('''
    SELECT i.symbol, count(*) AS bars, min(b.ts) AS earliest, max(b.ts) AS latest
    FROM bars_intraday b JOIN instruments i ON i.instrument_id = b.instrument_id
    WHERE b.source = 7
    GROUP BY i.symbol ORDER BY bars DESC
''').fetchall()
for row in rows:
    print(row)
"
```

Expected: roughly 5 rows (one per watchlist symbol), each with hundreds of thousands of bars (a full trading day is ~375 one-minute bars; ~4.6 years × ~250 trading days/year × 375 ≈ 430k bars per symbol, though real coverage will be lower due to Upstox's actual data gaps), `earliest` at or near 2022-01, `latest` at or near yesterday.

Record: total candles written across all 5 symbols, the actual earliest/latest dates observed, and any skipped windows the CLI reported (from Task 6's `main()` output) — the same evidentiary standard every prior live/production task in this project has held itself to.

- [ ] **Step 5: Report**

No commit for this task (it runs shipped code, doesn't change any file). Report the verification numbers back in this plan's completion notes.

---

## Task 8 (discovered live, 2026-08-24): a single bad candle must not crash the whole backfill

**Why this task exists:** Running Task 7 for real surfaced a genuine gap Tasks 1-6 didn't anticipate. Upstox's historical-candle API can return a candle whose OHLC values violate `bars_intraday`'s own `ck_ohlc_order_intraday` CHECK constraint (`high >= low AND high >= open AND high >= close AND low <= open AND low <= close`, when `volume` is non-zero). Confirmed live and reproduced directly against the real API (not a parsing bug — this is Upstox's own verbatim response): `NSE_EQ|INE040A01034` (HDFCBANK), `2024-06-25T09:15:00+05:30` → `[open=835.6, high=839.8, low=837.4, close=839.6]`. `low (837.4) > open (835.6)` — a real vendor data quirk at the market-open candle, most likely the pre-open auction print leaking into the "open" field while the first minute's regular-session trades set a higher low. `write_backfill_candle` let `psycopg.errors.CheckViolation` propagate uncaught, which crashed `main()` mid-run (3 of 5 symbols had already finished and committed — `autocommit=True` means each write is its own transaction, so that data is safe — but the 4th symbol was left partially backfilled and the 5th never started).

This is the same class of problem Phase 0's `bars_daily` pipeline already solved for EOD data (quarantine bad rows, never let one row's defect kill the whole load) — but this backfill module deliberately doesn't participate in that heavier `ingest_jobs`/`quarantine` machinery (see this plan's spec §3: a small, dedicated write path was chosen specifically to avoid that heavier framework). The fix here stays proportionate to that choice: catch the failure at the single-candle level, log it with full detail, count it, and keep going — not wire this backfill into the full quarantine system.

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py`
- Modify: `tests/streaming/test_upstox_intraday_backfill.py`

**Interfaces:**
- Modifies: `BackfillReport` gains a new field `candles_rejected: int = 0`.
- Modifies: `backfill_symbol`'s per-candle write loop: each `write_backfill_candle` call is wrapped so a `psycopg.errors.CheckViolation` (and only that — not a bare `except Exception`, which would also swallow a genuine connection failure or a bug worth seeing) is caught, logged as a warning with the candle's full field values, increments `report.candles_rejected`, and the loop continues to the next candle in that window (not the next window — the rest of the window's candles are still good data and should still be written).
- `main()`'s final summary print gains a `candles_rejected` line alongside `candles_written`/`skipped_windows`, and a nonzero rejected count does **not** by itself trigger the `SystemExit(1)` that a skipped window does — a handful of known-dirty exchange prints is expected data-quality noise, not an operational failure the way a skipped network window is.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
def test_backfill_symbol_skips_a_check_violating_candle_and_continues(db_conn, fixture_instrument_id):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "candles": [
                        # A real, verbatim shape Upstox returned for HDFCBANK on
                        # 2024-06-25T09:15 IST -- low (837.4) > open (835.6) violates
                        # bars_intraday's ck_ohlc_order_intraday CHECK constraint.
                        ["2024-06-25T09:15:00+05:30", 835.6, 839.8, 837.4, 839.6, 370096, 0],
                        ["2024-06-25T09:16:00+05:30", 839.6, 840.8, 839.4, 840.0, 362254, 0],
                    ]
                },
            },
        )

    client = _mock_client(handler)

    report = backfill_symbol(
        db_conn,
        client,
        instrument_key="NSE_EQ|INE002A01018",
        instrument_id=fixture_instrument_id,
        token="tok",
        start=date(2024, 6, 1),
        end=date(2024, 6, 30),
        sleep=lambda _: None,
    )

    assert report.candles_written == 1
    assert report.candles_rejected == 1
    assert report.skipped_windows == []

    rows = db_conn.execute(
        "SELECT ts FROM bars_intraday WHERE instrument_id = %s ORDER BY ts",
        (fixture_instrument_id,),
    ).fetchall()
    assert len(rows) == 1  # only the valid candle landed
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -k check_violating`
Expected: FAIL — either `psycopg.errors.CheckViolation` propagates uncaught (crashing the test) or `AttributeError: 'BackfillReport' object has no attribute 'candles_rejected'`, depending on which line the test reaches first.

- [ ] **Step 3: Write the fix**

In `src/trading/streaming/upstox_intraday_backfill.py`, add `candles_rejected: int = 0` to `BackfillReport`:

```python
@dataclass
class BackfillReport:
    instrument_key: str
    candles_written: int = 0
    candles_rejected: int = 0
    skipped_windows: list[tuple[date, date]] = field(default_factory=list)
```

Add `from psycopg.errors import CheckViolation` to the imports.

In `backfill_symbol`, change the candle-writing loop from:

```python
        for candle in parse_candle_response(payload, instrument_id):
            write_backfill_candle(conn, candle)
            report.candles_written += 1
        sleep(REQUEST_DELAY_SECONDS)
```

to:

```python
        for candle in parse_candle_response(payload, instrument_id):
            try:
                write_backfill_candle(conn, candle)
                report.candles_written += 1
            except CheckViolation as exc:
                log.warning(
                    "upstox_intraday_backfill.candle_rejected",
                    instrument_key=instrument_key,
                    ts=candle.ts.isoformat(),
                    open=str(candle.open),
                    high=str(candle.high),
                    low=str(candle.low),
                    close=str(candle.close),
                    reason=str(exc).splitlines()[0],
                )
                report.candles_rejected += 1
        sleep(REQUEST_DELAY_SECONDS)
```

**Why `CheckViolation` and not a broader catch:** a bare `except Exception` here would also swallow a lost database connection, a schema drift, or a real bug in `write_backfill_candle` — all of which should crash loudly, not be silently counted as "one rejected candle." `CheckViolation` is the one specific, well-understood failure mode this task exists to handle: known-dirty exchange data hitting a known, intentional constraint. Note also that this catch must be scoped to `autocommit=True` connections (true for `main()`'s production connection) — under an explicit multi-statement transaction, a single failed statement would poison the whole transaction and every subsequent write would also fail until a rollback, which this narrow per-candle catch does not perform. `write_backfill_candle`'s docstring already documents that it never commits and the caller controls transaction boundaries; this task doesn't change that contract, it only relies on `main()`'s existing `autocommit=True` choice for this catch to be safe. (If `backfill_symbol` is ever called under an explicit-transaction connection — as this plan's own tests already do, via `db_conn` — a `CheckViolation` on one candle **will** poison that test's transaction for any subsequent write in the same test. This is fine for the specific test added in Step 1, which writes no candle after the rejected one within the same `db_conn` transaction, but is a real constraint worth knowing if a future test wants to assert on a successful write occurring *after* a rejected one within a single `db_conn`-backed test — it would need its own savepoint, which is out of scope here.)

Update `main()`'s summary print to include the rejected count:

```python
    total_written = sum(r.candles_written for r in reports)
    total_rejected = sum(r.candles_rejected for r in reports)
    total_skipped = sum(len(r.skipped_windows) for r in reports)
    print(
        f"Backfill complete: {total_written} candles written, {total_rejected} rejected "
        f"(bad OHLC data), {total_skipped} windows skipped"
    )
    for report in reports:
        if report.skipped_windows:
            print(f"  {report.instrument_key}: skipped {report.skipped_windows}")
        if report.candles_rejected:
            print(f"  {report.instrument_key}: {report.candles_rejected} candles rejected")

    if total_skipped:
        raise SystemExit(1)
```

(`total_rejected` alone does not trigger `SystemExit(1)` — see Interfaces above for why.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -k check_violating`
Expected: 1 passed

- [ ] **Step 5: Run the full test file, then the full repo suite**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 22 passed, 1 deselected (or 23 passed if the live test runs — `UPSTOX_ACCESS_TOKEN` is minted as of this task, so it should run and pass)

Run: `uv run pytest`
Expected: all passing, no regressions.

- [ ] **Step 6: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "fix(streaming): skip individual OHLC-invalid candles instead of crashing the backfill"
```

---

## Task 9: Re-run the real backfill for the symbols Task 7's crash left incomplete

**Superseded by Task 10 below before completion** — an attempted run of this task on 2026-08-24
22:01 IST surfaced a second live issue (a severe performance regression from Task 8's fix), which
is fixed by Task 10. Re-attempt this task's steps only after Task 10 is complete and reviewed.

**Files:** none (verification only, same as Task 7).

Task 7's run crashed partway through the 4th symbol (HDFCBANK) after successfully finishing RELIANCE, TCS/INFY (whichever the 2nd/3rd symbols were), and one more — 3 symbols fully written, committed, and safe (`autocommit=True`, each write its own transaction). Task 8's fix is now in place. Re-running `main()` from scratch is safe (idempotent upsert on every symbol, including the 3 already-complete ones — those just become cheap no-op upserts) and simpler than building a resume-from-symbol-N feature that was never in this plan's scope.

- [ ] **Step 1:** Run `uv run python -m trading.streaming.upstox_intraday_backfill` again.
- [ ] **Step 2:** Watch for any further crashes. A `candles_rejected` count in the log/summary output is expected and fine (that's Task 8's fix working as designed) — a crash is not.
- [ ] **Step 3:** Re-run this plan's original Task 7 Step 4 verification query. Report final totals: candles written per symbol, candles rejected per symbol, earliest/latest dates, any skipped windows.

---

## Task 10 (discovered live, 2026-08-24): the savepoint wrapper made every production write ~3x slower for no reason

**Why this task exists:** Attempting Task 9 surfaced a second gap. Task 8's fix wraps *every* candle write in `with conn.transaction():` (a savepoint) to stop a `CheckViolation` from poisoning the rest of a transaction — but that protection is only structurally necessary under an **explicit, non-autocommit transaction** (like this plan's own tests, via `db_conn`). Under `main()`'s production connection (`autocommit=True`), each statement is *already* its own independently committed/rolled-back unit at the Postgres protocol level — a failed statement there poisons nothing, so the savepoint wrapper was never doing useful work in production, only adding cost.

Confirmed by direct measurement against the real local Postgres (`autocommit=True` connection, 500 trivial statements): a bare `conn.execute(...)` averaged 0.21ms/statement; the same statement wrapped in `with conn.transaction():` averaged 0.60ms/statement — roughly **3x slower per write**. Applied across the ~2.15M candles this backfill writes (5 symbols × ~430k candles each), this is the direct cause of Task 9's re-run taking over 13 minutes on just the first symbol (RELIANCE) versus ~3 minutes for the same symbol in Task 7's original, unwrapped run. The fix must keep Task 8's correctness guarantee (tests still pass, `CheckViolation` is still caught and doesn't poison a test's transaction) while restoring production's original per-write cost.

**Files:**
- Modify: `src/trading/streaming/upstox_intraday_backfill.py`

**Interfaces:**
- Modifies: `backfill_symbol`'s per-candle write loop. The savepoint (`conn.transaction()`) is applied **only when `conn.autocommit` is `False`** (i.e., only when actually structurally necessary to protect a caller's explicit transaction — true for this plan's own tests via `db_conn`, never true for `main()`'s production connection). Under `conn.autocommit is True`, the write runs bare, exactly as it did before Task 8, restoring full write throughput. No test-visible behavior changes: `CheckViolation` is still caught, still logged, still counted in `report.candles_rejected`, and the existing `test_backfill_symbol_skips_a_check_violating_candle_and_continues` test (which runs under `db_conn`, `autocommit=False`) must still pass unmodified.

- [ ] **Step 1: Write a test confirming the bare-write path under autocommit**

Append to `tests/streaming/test_upstox_intraday_backfill.py`:

```python
def test_backfill_symbol_does_not_wrap_writes_in_a_savepoint_under_autocommit(
    db_url, fixture_instrument_id_factory
):
    """Regression test for the Task 9 re-run's ~3x slowdown: under an
    autocommit=True connection (production's shape), write_backfill_candle
    must be called directly, with no `with conn.transaction():` wrapper --
    that wrapper was only ever needed to protect an explicit, non-autocommit
    transaction (this plan's own tests) from CheckViolation poisoning it.
    Verified by monkeypatching Connection.transaction to fail loudly if
    called at all while a real autocommit connection runs backfill_symbol.
    """
    import psycopg
    from psycopg import Connection

    called = {"n": 0}
    original_transaction = Connection.transaction

    def _tracking_transaction(self, *args, **kwargs):
        called["n"] += 1
        return original_transaction(self, *args, **kwargs)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {"candles": [["2022-01-15T09:15:00+05:30", 1.0, 1.0, 1.0, 1.0, 1, 0]]},
            },
        )

    client = _mock_client(handler)
    autocommit_conn = psycopg.connect(db_url, autocommit=True)
    try:
        instrument_id = fixture_instrument_id_factory(autocommit_conn)
        Connection.transaction = _tracking_transaction
        backfill_symbol(
            autocommit_conn,
            client,
            instrument_key="NSE_EQ|INE002A01018",
            instrument_id=instrument_id,
            token="tok",
            start=date(2022, 1, 1),
            end=date(2022, 1, 31),
            sleep=lambda _: None,
        )
    finally:
        Connection.transaction = original_transaction
        autocommit_conn.execute("DELETE FROM bars_intraday WHERE instrument_id = %s", (instrument_id,))
        autocommit_conn.execute("DELETE FROM instruments WHERE instrument_id = %s", (instrument_id,))
        autocommit_conn.close()

    assert called["n"] == 0, "conn.transaction() must not be called under an autocommit connection"
```

This test needs a `fixture_instrument_id_factory` helper that does what `fixture_instrument_id` does but against an arbitrary connection (not just `db_conn`) and without relying on `db_conn`'s rollback for cleanup (since this test uses its own real `autocommit=True` connection, cleanup is manual — see the `finally` block above). Add this alongside the existing `fixture_instrument_id` fixture:

```python
@pytest.fixture
def fixture_instrument_id_factory():
    def _make(conn) -> int:
        row = conn.execute(
            """
            INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
            VALUES ('EQUITY', 'NSE', 'CM', 'RELIANCE', 'ACTIVE', 'NSE:CM:RELIANCE:EQ:AUTOCOMMIT_TEST')
            ON CONFLICT (canonical_key) DO UPDATE SET updated_at = now()
            RETURNING instrument_id
            """
        ).fetchone()
        return row[0]

    return _make
```

(The `ON CONFLICT ... DO UPDATE` is defensive: this fixture runs against a real, non-rolled-back connection, so a prior failed test run's leftover row with the same `canonical_key` must not crash this one with a unique-constraint violation.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -k does_not_wrap`
Expected: FAIL — `AssertionError: conn.transaction() must not be called under an autocommit connection` (`called["n"]` will be 1, since Task 8's current code always wraps).

- [ ] **Step 3: Write the fix**

In `src/trading/streaming/upstox_intraday_backfill.py`, add `import contextlib` to the imports.

Change the per-candle write loop from (Task 8's current unconditional wrap):

```python
        for candle in parse_candle_response(payload, instrument_id):
            try:
                # A savepoint (via conn.transaction()) rather than a bare
                # write: ...
                # [Task 8's existing comment block]
                with conn.transaction():
                    write_backfill_candle(conn, candle)
                report.candles_written += 1
            except CheckViolation as exc:
                ...
```

to:

```python
        for candle in parse_candle_response(payload, instrument_id):
            # A savepoint (via conn.transaction()) protects an explicit,
            # non-autocommit transaction from a CheckViolation poisoning
            # every subsequent write in the same transaction -- but that
            # protection is only necessary when one exists. Under an
            # autocommit=True connection (main()'s production shape), each
            # statement is already its own independently committed unit at
            # the protocol level; wrapping it in a savepoint anyway adds a
            # real ~3x per-write cost (measured: 0.21ms -> 0.60ms/statement
            # against local Postgres) for zero correctness benefit, which is
            # exactly what turned one production backfill run's first
            # symbol from ~3 minutes into 13+ minutes and counting before
            # this fix. Skip the wrapper entirely when conn.autocommit is
            # True; only pay for it when it's actually protecting something.
            transaction_ctx = contextlib.nullcontext() if conn.autocommit else conn.transaction()
            try:
                with transaction_ctx:
                    write_backfill_candle(conn, candle)
                report.candles_written += 1
            except CheckViolation as exc:
                log.warning(
                    "upstox_intraday_backfill.candle_rejected",
                    instrument_key=instrument_key,
                    ts=candle.ts.isoformat(),
                    open=str(candle.open),
                    high=str(candle.high),
                    low=str(candle.low),
                    close=str(candle.close),
                    reason=str(exc).splitlines()[0],
                )
                report.candles_rejected += 1
        sleep(REQUEST_DELAY_SECONDS)
```

(Keep Task 8's existing `CheckViolation` handling logic identical — only the transaction-wrapping decision changes.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v -k "does_not_wrap or check_violating"`
Expected: 2 passed (the new autocommit test, and Task 8's original savepoint-under-db_conn test — confirming both paths work correctly: no wrapper under autocommit, savepoint still applied under an explicit transaction).

Run the full file: `uv run pytest tests/streaming/test_upstox_intraday_backfill.py -v`
Expected: 23 passed (or 24 with the live test), no regressions in any earlier test.

- [ ] **Step 5: Run the full repo suite**

Run: `uv run pytest`
Expected: all passing, no regressions.

- [ ] **Step 6: A quick real-world timing sanity check (not a formal step, but do it)**

Before committing, it's worth a quick manual confirmation that this actually restores throughput: time a small real slice, e.g. `backfill_symbol` for one month-window against the real API with the real `autocommit=True` connection `main()` would use, and confirm it completes in roughly the same time as Task 7's original (pre-Task-8) run did for a comparable slice. This isn't a formal pytest step — just don't skip verifying the fix actually fixes the real-world symptom, not just the mocked unit test.

- [ ] **Step 7: Lint/type gate and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src`

```bash
git add src/trading/streaming/upstox_intraday_backfill.py tests/streaming/test_upstox_intraday_backfill.py
git commit -m "fix(streaming): skip the savepoint wrapper under autocommit connections (perf)"
```

---

## Task 11: Re-attempt Task 9 now that the fix is in

Identical to Task 9's steps, run again now that Task 10's fix is committed. This is the task whose results get reported as this plan's final completion evidence.

- [ ] **Step 1:** Run `uv run python -m trading.streaming.upstox_intraday_backfill` again.
- [ ] **Step 2:** Watch for crashes (none expected) and note the wall-clock time for comparison against Task 7's original per-symbol timings (~3 minutes/symbol) as confirmation the performance fix worked in the real, full-scale run, not just the timing sanity check.
- [ ] **Step 3:** Re-run this plan's original Task 7 Step 4 verification query. Report final totals: candles written per symbol, candles rejected per symbol, earliest/latest dates, any skipped windows.
