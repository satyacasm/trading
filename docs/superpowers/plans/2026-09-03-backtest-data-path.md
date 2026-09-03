# Backtest Data Path (Phase 3, sub-project 3a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `smoke_test()` — and everything that reuses it later in Phase 3 — serve the bar interval a strategy actually declared in its manifest, routing `"1d"` through the existing, tested, point-in-time corporate-action adjustment instead of silently handing over 1-minute bars.

**Architecture:** One pure function (`resolve_bar_interval`) maps the manifest's `data.bars` string to seconds. `select_window` and `fetch_bars` each grow a daily-bars branch alongside their existing `bars_intraday` path, selected by that resolved interval. The daily branch of `fetch_bars` calls the already-built `trading.corpactions.adjust.adjusted_bars` per instrument and converts its polars rows into the same `BarRecord` the 1-minute path already produces — `run_loop`, `BarRecord`, and `InMemoryBars` are untouched by this plan.

**Tech Stack:** Python 3.12, psycopg 3, polars (already a dependency via `trading.corpactions`), pytest.

**Spec:** `docs/superpowers/specs/2026-09-03-backtest-data-path-design.md` — read it before Task 1. Every decision below argues from a D3a-N number in that document, including a correction made while writing this plan (see Task 4's note on `_InvalidBarInterval`).

## Global Constraints

- **All money and quantities are `decimal.Decimal`, never `float`.** A `float` anywhere in a price, quantity, or volume path is a defect. `adjusted_bars` already returns polars `Decimal` columns; `.iter_rows(named=True)` yields native Python `Decimal` objects directly — no casting needed for price columns, only `volume` (an `int` from polars) needs `Decimal(...)` wrapping to match `BarRecord.volume`'s type.
- **No silent defaulting on invalid input.** An unrecognized `data.bars` value must raise, not default to `60`. This is the entire correctness point of the sub-project — verified directly (see spec's testing-section correction) that nothing validates this value before `smoke_test` uses it.
- **1-minute behavior must be provably unchanged.** Every existing caller of `select_window`/`fetch_bars` passes no `interval_sec`, so both functions default to `60` and must produce byte-identical output to before this plan for that default.
- Line length 100 (`[tool.ruff] line-length = 100`). `uv run ruff check src tests` and `uv run mypy src` must pass before every commit.
- Container tests carry `@pytest.mark.sandbox`; DB tests carry `@pytest.mark.db`. Default run is `-m 'not live and not golden'`.
- Run tests with `uv run pytest`.

---

### Task 1: `resolve_bar_interval` — read the manifest's declared interval, or raise

The pure function everything else in this plan routes on. No DB, no sandbox.

**Files:**
- Modify: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: nothing new — a plain `dict[str, Any]` manifest, the same shape `_describe_manifest` in `sandbox/runner.py` already produces (`manifest["data"]["bars"]` is a string, one of `"1m"`, `"5m"`, `"15m"`, `"1h"`, `"1d"`, when the field is set).
- Produces:
  - `resolve_bar_interval(manifest: dict[str, Any]) -> int` — returns seconds (`60`, `300`, `900`, `3600`, `86400`).
  - `_InvalidBarInterval` — an `Exception` subclass, raised (never defaulted) when `data.bars` is missing or not one of the five values. Task 4 catches this exactly like the existing `_UnresolvedUniverse`/`_MixedUniverse`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/agent_contract/test_smoke.py`:

```python
def test_resolve_bar_interval_maps_every_schema_value_to_seconds() -> None:
    from trading.agent_contract.smoke import resolve_bar_interval

    expected = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
    for bars, seconds in expected.items():
        manifest = {"data": {"bars": bars}}
        assert resolve_bar_interval(manifest) == seconds


def test_resolve_bar_interval_rejects_anything_else() -> None:
    """No silent default. A typo'd interval reaching this function
    unvalidated -- nothing schema-checks it before smoke_test calls this,
    see the spec's testing-section correction -- must be a loud failure,
    not a quiet 60.
    """
    from trading.agent_contract.smoke import _InvalidBarInterval, resolve_bar_interval

    for manifest in (
        {"data": {"bars": "2m"}},
        {"data": {"bars": None}},
        {"data": {}},
        {},
    ):
        with pytest.raises(_InvalidBarInterval):
            resolve_bar_interval(manifest)
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k resolve_bar_interval -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_bar_interval'`.

- [ ] **Step 3: Implement it**

Add to `src/trading/agent_contract/smoke.py`, near the other small marker exceptions (`_UnresolvedUniverse`, `_MixedUniverse`):

```python
_BAR_INTERVALS_SEC: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}


class _InvalidBarInterval(Exception):
    """The manifest's data.bars is not one of the five values §3 permits.

    Raised rather than defaulted: nothing schema-checks this value before
    smoke_test uses it -- api.py's pre-smoke-test validate_strategy() call
    has no manifest yet, and validate_manifest only runs inside
    register_strategy, after a passing smoke test. A silent default to
    60 here would be the exact silent-wrong-data failure this plan exists
    to remove, one function over.
    """


def resolve_bar_interval(manifest: dict[str, Any]) -> int:
    raw = (manifest.get("data") or {}).get("bars")
    try:
        return _BAR_INTERVALS_SEC[raw]
    except KeyError:
        raise _InvalidBarInterval(
            f"the manifest declares data.bars={raw!r}, which is not one of the five "
            f"values the contract permits: {sorted(_BAR_INTERVALS_SEC)}. "
            "See STRATEGY_CONTRACT.md §3."
        ) from None
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k resolve_bar_interval -v`
Expected: 2 passed.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
uv run pytest -q   # full suite, confirm nothing else broke
git add src/trading/agent_contract/smoke.py tests/agent_contract/test_smoke.py
git commit -m "feat(agent-contract): resolve_bar_interval reads DataRequest.bars, or raises

Nothing schema-checks a manifest's data.bars value before smoke_test
uses it -- verified directly, api.py's pre-smoke-test validate_strategy()
call has no manifest yet, and register_strategy's schema check only runs
after a passing smoke test. Raises rather than defaulting to 60, so a
typo'd interval fails loudly instead of silently running the wrong data."
```

---

### Task 2: `select_window`'s daily branch

`select_window` runs *before* `fetch_bars` in `smoke_test`'s pipeline. Fixing only `fetch_bars` (Task 3) without this would ship a fix that never activates: an instrument that exists only in `bars_daily` would have its window computed against zero `bars_intraday` sessions and return empty, never reaching the corrected `fetch_bars` at all. This is D3a-1's core finding from the spec.

**Files:**
- Modify: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: `resolve_bar_interval` (Task 1) — not called from inside `select_window` itself, but Task 4 will pass its result in as `interval_sec`.
- Produces: `select_window(conn, instrument_ids, sessions=5, *, interval_sec=60) -> dict[str, Any]` — same return shape as today (`{"start", "end", "sessions", "instruments"}`), new keyword-only `interval_sec` parameter, default `60` preserves every existing call site unchanged.

- [ ] **Step 1: Write the failing test**

Add to `tests/agent_contract/test_smoke.py`. This instrument has **only** `bars_daily` rows — no `bars_intraday` rows at all, matching the measured reality that 585,261 of 585,299 instruments in this database are daily-only:

```python
@pytest.mark.db
def test_select_window_finds_a_daily_only_instrument_when_asked_for_1d(db_conn) -> None:  # noqa: ANN001
    """585,261 of this database's 585,299 instruments have bars_daily rows
    and zero bars_intraday rows. select_window's default query would find
    nothing for one of them -- this is the gap Task 3's fetch_bars fix
    would otherwise ship silently inactive on.
    """
    from datetime import date

    from trading.agent_contract.smoke import select_window

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYONLY','INR',"
        "'ACTIVE','NSE:CM:DAILYONLY') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for day in (date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10)):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,101,99,100,10,1)",
            (instrument_id, day),
        )

    daily_window = select_window(db_conn, [instrument_id], sessions=5, interval_sec=86400)
    assert daily_window["sessions"] == 3

    intraday_window = select_window(db_conn, [instrument_id], sessions=5, interval_sec=60)
    assert intraday_window["sessions"] == 0
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k daily_only_instrument -v`
Expected: FAIL with `TypeError: select_window() got an unexpected keyword argument 'interval_sec'`.

- [ ] **Step 3: Implement it**

In `src/trading/agent_contract/smoke.py`, add a sibling SQL constant next to `_WINDOW_SQL`:

```python
_DAILY_WINDOW_SQL = """
    SELECT ts::date AS session
    FROM bars_daily
    WHERE instrument_id = ANY(%s)
    GROUP BY session
    HAVING COUNT(DISTINCT instrument_id) = %s
    ORDER BY session DESC
    LIMIT %s
"""
```

Change `select_window`'s signature and dispatch:

```python
def select_window(
    conn: Connection, instrument_ids: Sequence[int], sessions: int = 5, *, interval_sec: int = 60
) -> dict[str, Any]:
    """The most recent sessions EVERY instrument printed in.

    Intersection rather than union, deliberately: a window where half the
    universe has no bars would hand a strategy a market in which half its
    instruments silently do not exist, and the absent-not-carried-forward
    rule would make that indistinguishable from a quiet day.

    `bars_daily` has no `interval_sec` column -- it is one row per
    instrument per day, not multiplexed like `bars_intraday` -- so the
    daily branch's query has no equivalent filter to apply.
    """
    sql = _DAILY_WINDOW_SQL if interval_sec == 86400 else _WINDOW_SQL
    rows = conn.execute(sql, (list(instrument_ids), len(set(instrument_ids)), sessions)).fetchall()
    days = sorted(row[0] for row in rows)
    if not days:
        return {"start": None, "end": None, "sessions": 0, "instruments": {}}
    start = datetime.combine(days[0], time.min, tzinfo=UTC)
    end = datetime.combine(days[-1], time.max, tzinfo=UTC)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(days),
        "instruments": {},
    }
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k "daily_only_instrument or select_window" -v`
Expected: all pass, including the pre-existing `test_select_window_finds_the_latest_sessions_every_instrument_shares` (unmodified — confirms the `60` default keeps it working byte-for-byte).

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
uv run pytest -q
git add src/trading/agent_contract/smoke.py tests/agent_contract/test_smoke.py
git commit -m "feat(agent-contract): select_window finds sessions in bars_daily too

585,261 of 585,299 instruments in this database have bars_daily rows and
zero bars_intraday rows. select_window ran before fetch_bars and was
equally hardcoded to bars_intraday -- fixing fetch_bars alone would ship
a fix that never activates, since the window would already be empty for
any daily-only instrument. interval_sec=60 default keeps every existing
call site producing identical output."
```

---

### Task 3: `fetch_bars`'s daily branch — wire in the existing adjustment

The core of the sub-project: route `interval_sec == 86400` through `trading.corpactions.adjust.adjusted_bars`, which already exists, is tested, and is point-in-time correct. This task proves the wiring is correct, not the adjustment math (that's `tests/corpactions/test_adjust.py`'s job).

**Files:**
- Modify: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: `trading.corpactions.adjust.adjusted_bars(conn, instrument_id, start, end, *, as_of) -> pl.DataFrame` (columns: `ts: datetime`, `open/high/low/close/prev_close: Decimal`, `volume: int`, confirmed by direct inspection — `prev_close` is dropped, `BarRecord` has no such field).
- Produces: `fetch_bars(conn, instrument_ids, window, *, interval_sec=60) -> dict[int, tuple[BarRecord, ...]]` — same return shape, new keyword-only `interval_sec` parameter, default `60` preserves every existing call site.

- [ ] **Step 1: Write the failing test**

This is the regression the spec calls out by name: a synthetic 1:2 split, with the exact pre/post-split closes worked out by hand against `adjusted_bars`'s documented factor logic (`factor_for` scales a bar by every action whose `ex_date` is strictly *after* that bar's day — confirmed by direct inspection of `tests/corpactions/test_adjust.py`'s existing assertions). Add to `tests/agent_contract/test_smoke.py`:

```python
@pytest.mark.db
def test_fetch_bars_daily_branch_reconstructs_a_continuous_series_across_a_split(
    db_conn,  # noqa: ANN001
) -> None:
    """A 1:2 split printed on day 3. Unadjusted closes would show a ~50%
    drop between day 2 and day 3 -- the exact defect measured against real
    NSE splits in the spec (COLAB: -51.0%, VLL: -49.0%, NAVKARURB: -47.6%).
    With as_of set to the window's end (D3a-2: fixed for the whole run,
    after the split), the adjusted series must be continuous instead.
    """
    from datetime import date

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYSPLIT','INR',"
        "'ACTIVE','NSE:CM:DAILYSPLIT') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]

    # Day 3 is the ex-date. Its own bar is already printed post-split, per
    # adjusted_bars's factor_for: an action scales a bar only when the
    # ex_date is STRICTLY AFTER that bar's day.
    closes = {
        date(2024, 1, 8): 200,  # 2 days before ex-date
        date(2024, 1, 9): 210,  # 1 day before ex-date
        date(2024, 1, 10): 105,  # ex-date -- already post-split as printed
        date(2024, 1, 11): 106,  # 1 day after
        date(2024, 1, 12): 104,  # 2 days after
    }
    for day, close in closes.items():
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,%s,%s,%s,%s,10,1)",
            (instrument_id, day, close, close, close, close),
        )
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, "
        "ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2024-01-10',1,2,'test')",
        (instrument_id,),
    )

    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-12T23:59:59.999999+00:00",
        "sessions": 5,
        "instruments": {},
    }
    bars = fetch_bars(db_conn, [instrument_id], window, interval_sec=86400)

    series = [b.close for b in bars[instrument_id]]
    assert series == [
        Decimal("100.0000"),  # 200 * 0.5
        Decimal("105.0000"),  # 210 * 0.5
        Decimal("105.0000"),  # ex-date, unscaled
        Decimal("106.0000"),  # unscaled
        Decimal("104.0000"),  # unscaled
    ]
    # The regression itself: no jump anywhere near 50%.
    for a, b in zip(series, series[1:], strict=True):
        assert abs(b / a - 1) < Decimal("0.1"), (a, b)


@pytest.mark.db
def test_fetch_bars_daily_branch_uses_the_windows_end_as_as_of_not_some_other_date(
    db_conn,  # noqa: ANN001
) -> None:
    """Pins D3a-2's specific choice, not just that adjustment happens at
    all. `adjustment_factors`' query filters `ex_date <= as_of` -- an
    action whose ex_date is after `as_of` is not merely left unscaled, it
    is invisible entirely (confirmed by direct inspection of
    `_ACTION_QUERY`). So a window that ENDS before the split's ex-date
    must come back completely raw. If `_fetch_daily_bars` passed the
    wrong date here -- `date.today()`, the window's START, anything but
    `window["end"]` -- this is the test that would catch it: today's real
    calendar date is long after 2024, so a `date.today()` bug would make
    this test see the split as already known and silently pass adjusted
    values instead of raw ones.
    """
    from datetime import date

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYPRESPLIT','INR',"
        "'ACTIVE','NSE:CM:DAILYPRESPLIT') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for day, close in {date(2024, 1, 8): 200, date(2024, 1, 9): 210}.items():
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,%s,%s,%s,%s,10,1)",
            (instrument_id, day, close, close, close, close),
        )
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, "
        "ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2024-01-10',1,2,'test')",
        (instrument_id,),
    )

    # Window ends 2024-01-09 -- the DAY BEFORE the split's ex_date.
    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-09T23:59:59.999999+00:00",
        "sessions": 2,
        "instruments": {},
    }
    bars = fetch_bars(db_conn, [instrument_id], window, interval_sec=86400)

    series = [b.close for b in bars[instrument_id]]
    assert series == [Decimal("200.0000"), Decimal("210.0000")]  # raw, NOT halved


@pytest.mark.db
def test_fetch_bars_1m_path_is_unaffected_by_the_daily_branch(db_conn) -> None:  # noqa: ANN001
    """The default interval_sec=60 must produce identical output to before
    this plan -- the vacuity guard for every 1-minute strategy already on
    this platform."""
    from datetime import UTC, datetime

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','MINUTEONLY','INR',"
        "'ACTIVE','NSE:CM:MINUTEONLY') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for minute in range(3):
        db_conn.execute(
            "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, "
            "low, close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
            (instrument_id, datetime(2024, 1, 8, 9, 15 + minute, tzinfo=UTC)),
        )
    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-08T23:59:59.999999+00:00",
        "sessions": 1,
        "instruments": {},
    }

    bars = fetch_bars(db_conn, [instrument_id], window)  # interval_sec defaults to 60

    assert len(bars[instrument_id]) == 3
    assert all(b.interval_sec == 60 for b in bars[instrument_id])
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k "daily_branch or 1m_path_is_unaffected" -v`
Expected: the split test FAILS with `TypeError: fetch_bars() got an unexpected keyword argument 'interval_sec'`; the 1m test passes already (it is the pre-existing behavior, asserted explicitly here so a later change to the default cannot silently break it).

- [ ] **Step 3: Implement it**

Add the import and the daily-branch helper to `src/trading/agent_contract/smoke.py`:

```python
from trading.corpactions.adjust import adjusted_bars
```

```python
def _fetch_daily_bars(
    conn: Connection, instrument_ids: Sequence[int], window: dict[str, Any]
) -> dict[int, tuple[BarRecord, ...]]:
    """The `bars="1d"` path: one `adjusted_bars` call per instrument.

    `as_of` is the window's end date for every instrument and every bar in
    the run (D3a-2) -- one fixed factor set, so the series is continuous
    and returns are correct throughout the run, at the accepted cost that
    an early bar's absolute price level may not match what the exchange
    printed that day if a split lands later in the window.
    """
    start = datetime.fromisoformat(window["start"]).date()
    end = datetime.fromisoformat(window["end"]).date()
    series: dict[int, list[BarRecord]] = {}
    for instrument_id in instrument_ids:
        frame = adjusted_bars(conn, instrument_id, start, end, as_of=end)
        for row in frame.iter_rows(named=True):
            series.setdefault(instrument_id, []).append(
                BarRecord(
                    instrument_id=instrument_id,
                    ts=row["ts"],
                    interval_sec=86400,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=None if row["volume"] is None else Decimal(row["volume"]),
                )
            )
    window["instruments"] = {str(k): {"bars": len(v)} for k, v in series.items()}
    return {k: tuple(v) for k, v in series.items()}
```

Change `fetch_bars` to dispatch on the new parameter:

```python
def fetch_bars(
    conn: Connection,
    instrument_ids: Sequence[int],
    window: dict[str, Any],
    *,
    interval_sec: int = 60,
) -> dict[int, tuple[BarRecord, ...]]:
    if window["start"] is None:
        return {}
    if interval_sec == 86400:
        return _fetch_daily_bars(conn, instrument_ids, window)
    rows = conn.execute(
        _BARS_SQL,
        (
            list(instrument_ids),
            datetime.fromisoformat(window["start"]),
            datetime.fromisoformat(window["end"]),
        ),
    ).fetchall()
    series: dict[int, list[BarRecord]] = {}
    for (
        instrument_id,
        ts,
        interval_sec_row,
        open_,
        high,
        low,
        close,
        volume,
        trades,
        open_interest,
        oi_change,
    ) in rows:
        series.setdefault(instrument_id, []).append(
            BarRecord(
                instrument_id=instrument_id,
                ts=ts,
                interval_sec=interval_sec_row,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=None if volume is None else Decimal(volume),
                trades=trades,
                open_interest=open_interest,
                oi_change=oi_change,
            )
        )
    window["instruments"] = {str(k): {"bars": len(v)} for k, v in series.items()}
    return {k: tuple(v) for k, v in series.items()}
```

(The loop variable was renamed `interval_sec_row` only to avoid shadowing the new parameter — everything else in this branch is unchanged from before this plan.)

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k "daily_branch or 1m_path_is_unaffected or fetch_bars" -v`
Expected: all pass, including every pre-existing `fetch_bars` test.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
uv run pytest -q
git add src/trading/agent_contract/smoke.py tests/agent_contract/test_smoke.py
git commit -m "feat(agent-contract): fetch_bars routes bars=1d through adjusted_bars

The adjustment layer already existed (trading.corpactions.adjust, Phase
0, tested) and nothing outside that package called it. Wiring it in
here: a 1:2 split that would show as an unadjusted ~50% single-day drop
-- measured directly against three real NSE splits in the spec design
doc -- now reconstructs as a continuous series when as_of is the
backtest window's end date, fixed for the whole run (D3a-2).

interval_sec=60 stays the default, so every 1-minute call site --
including the smoke run every strategy uses today -- is unaffected."
```

---

### Task 4: Wire `resolve_bar_interval` into `smoke_test`, end to end

Threads Tasks 1–3 together in the one place that matters: the public entry point every upload goes through.

**Files:**
- Modify: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: `resolve_bar_interval` and `_InvalidBarInterval` (Task 1), `select_window(..., interval_sec=)` (Task 2), `fetch_bars(..., interval_sec=)` (Task 3).
- Produces: no new public interface — `smoke_test`'s signature is unchanged. This task only changes its body.

- [ ] **Step 1: Write the failing tests**

Two tests. The first proves a `bars="1d"` strategy actually works end to end through real containers; the second proves an invalid interval is caught as a finding rather than crashing the run or silently misbehaving. Add to `tests/agent_contract/test_smoke.py`:

```python
@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_serves_daily_bars_to_a_strategy_that_declares_them(db_conn) -> None:  # noqa: ANN001
    """End-to-end proof of the whole point of this plan: a strategy
    declaring bars="1d" gets real daily bars, not silently the 1-minute
    ones -- through a real container, a real manifest round trip, and the
    real adjustment layer."""
    import textwrap
    from datetime import date

    from trading.agent_contract.smoke import smoke_test

    symbol = "DAILYE2E"
    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
        "RETURNING instrument_id",
        (symbol, f"NSE:CM:{symbol}"),
    ).fetchone()
    instrument_id = row[0]

    for day in (date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10)):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,101,99,100,1000,1)",
            (instrument_id, day),
        )

    source = (
        textwrap.dedent(
            f"""
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="daily-e2e",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="{symbol}"),
                        ],
                        data=DataRequest(bars="1d", history_bars=5),
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
                            rationale="daily interval end-to-end",
                        )
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is True, verdict.as_agent_feedback()
    assert verdict.window["sessions"] == 3
    assert verdict.outcome is not None
    assert verdict.outcome["fills"] == 1


@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_reports_an_invalid_bar_interval_without_running_the_smoke_containers(
    db_conn,  # noqa: ANN001
) -> None:
    """DataRequest.bars is a Literal type hint, not a runtime-enforced one
    (platform_sdk.py: `BarInterval = Literal[...]`) -- a real strategy can
    genuinely pass a bad value, and this must surface as a finding after
    the configure() container alone, never reaching the two smoke
    containers."""
    import textwrap

    from trading.agent_contract.smoke import smoke_test

    source = (
        textwrap.dedent(
            """
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="bad-interval",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE"),
                        ],
                        data=DataRequest(bars="2m", history_bars=5),
                        capital=Decimal("1000000"),
                        base_currency="INR",
                    )

                def initialize(self, ctx):
                    pass

                def on_bar(self, ctx, bars):
                    pass
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is False
    assert [f.code for f in verdict.report.findings] == ["MANIFEST_UNRESOLVABLE"]
    assert "2m" in verdict.as_agent_feedback()
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k "serves_daily_bars or invalid_bar_interval" -v`
Expected, precisely, from tracing today's code:

- **The daily test fails** on `assert verdict.passed is True`. `DAILYE2E` is a
  freshly created instrument with only `bars_daily` rows; `smoke_test` still
  calls `select_window`/`fetch_bars` with their default `interval_sec=60`,
  which finds zero `bars_intraday` sessions for it, so `bars` comes back
  empty and `smoke_test` returns a `NO_DATA` finding — `verdict.passed` is
  actually `False`.
- **The invalid-interval test fails** on `assert verdict.passed is False`.
  Nothing in `smoke_test` reads `data.bars` yet, so the strategy proceeds on
  `RELIANCE`'s real `bars_intraday` data (that instrument has real 1-minute
  bars in this database, unaffected by the bogus `"2m"` declaration) — the
  smoke run completes as a `NO_ORDERS` **pass with a warning**, since the
  strategy places no orders, so `verdict.passed` is actually `True`. This is
  the exact silent-wrong-data defect the plan exists to remove: a bad
  interval is not merely unvalidated, it is silently ignored and the run
  proceeds anyway.

If either failure differs from this, stop and re-derive why before writing
Step 3 — a different failure than predicted means an assumption above is
wrong, and the fix should target the real cause.

- [ ] **Step 3: Implement it**

In `src/trading/agent_contract/smoke.py`, inside `smoke_test`, insert the interval resolution immediately after `manifest = configured.manifest` and before the existing `resolve_universe` call:

```python
    manifest = configured.manifest
    try:
        interval_sec = resolve_bar_interval(manifest)
    except _InvalidBarInterval as invalid:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="MANIFEST_UNRESOLVABLE",
                        message=str(invalid),
                        contract_section="§3",
                    ),
                )
            ),
            window={"start": None, "end": None, "sessions": 0, "instruments": {}},
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )
    try:
        instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
    except _UnresolvedUniverse as unresolved:
        ...  # unchanged
```

Then change the two lines that build `window` and `bars` to pass `interval_sec` through:

```python
    window = (
        select_window(conn, instrument_ids, interval_sec=interval_sec)
        if instrument_ids
        else {"start": None, "end": None, "sessions": 0, "instruments": {}}
    )
    bars = (
        fetch_bars(conn, instrument_ids, window, interval_sec=interval_sec)
        if instrument_ids
        else {}
    )
```

Everything else in `smoke_test` (the `_charge_key`/`load_schedules`/`SmokePayload`/two sandbox runs/`build_verdict` call) is unchanged — none of it depends on the interval.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k "serves_daily_bars or invalid_bar_interval" -v`
Expected: 2 passed.

- [ ] **Step 5: Run the whole `agent_contract` package, then the full suite**

```bash
uv run pytest tests/agent_contract -v
uv run pytest -q
```

Expected: every test passes, including all pre-existing `smoke_test` end-to-end tests (`test_smoke_test_runs_a_real_strategy_end_to_end`, the `NO_DATA` rejection test, etc.) — these exercise `bars="1m"` manifests and must be completely unaffected by this plan.

- [ ] **Step 6: Lint, type-check, format**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
```

- [ ] **Step 7: Update the docs and commit**

In `docs/STATUS.md`, note that 3a is complete and what it unblocks (3b, backtest runs at scale, can now assume `bars="1d"` is served correctly).

```bash
git add src/trading/agent_contract/smoke.py tests/agent_contract/test_smoke.py docs/STATUS.md
git commit -m "feat(agent-contract): smoke_test honors the manifest's declared bar interval

Closes Phase 3 sub-project 3a. resolve_bar_interval is computed once,
right after the manifest resolves, and threaded into both select_window
and fetch_bars -- the two functions that previously hardcoded
bars_intraday regardless of what a strategy asked for.

A bars='2m' typo is a real possibility, not a hypothetical: platform_sdk's
BarInterval is a Literal type hint with no runtime enforcement, and
nothing schema-checks the value before smoke_test uses it. It now
surfaces as MANIFEST_UNRESOLVABLE after the configure() container alone,
never reaching the two smoke containers.

A bars='1d' strategy now receives real daily bars, corporate-action
adjusted (D3a-2: as_of fixed to the backtest window's end date), through
the adjustment layer Phase 0 already built and this plan is the first
thing to actually call.

3b (backtest runs at scale + persistence) can now build on a data path
that serves what strategies declare, universe-sweep-sized, without
inheriting the phantom drawdowns an unadjusted bars_daily read would
produce."
```

---

## What this plan does not build

Named so they read as sequencing, matching the spec's §3:

- **Total-return series (dividends).** `adjusted_bars` deliberately excludes them; a distinct concept for a later sub-project.
- **Raising `SandboxLimits.memory`, sizing gates, result persistence, equity curve storage.** That is 3b.
- **Metrics, report UI, walk-forward, robustness suite** (3c–3e).
- **Any change to `run_loop`, `BarRecord`, `InMemoryBars`, or the sandbox runner.** The whole point of this plan is that they need none.
