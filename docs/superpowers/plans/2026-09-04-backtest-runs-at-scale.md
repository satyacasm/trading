# Backtest runs at scale, and the equity curve — Implementation Plan (Phase 3, 3b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the runtime run long, over daily bars, with a correct simulated
clock and an equity curve every later sub-project can consume.

**Architecture:** No new engine. `run_loop` already does ~481,000 bars/sec and
dispatches once per *timestamp*, so a daily decade is ~2,600 dispatches.
The work is (a) correcting the daily-bar clock, which is a money bug, (b)
emitting a series instead of a final scalar, (c) honouring `history_bars`,
and (d) refusing runs that will not fit before fetching anything.

**Tech Stack:** Python 3.12, psycopg 3 (sync), FastAPI, pytest, Docker/gVisor
sandbox, TimescaleDB.

**Spec:** `docs/superpowers/specs/2026-09-03-backtest-runs-at-scale-design.md`
(approved 2026-09-04, with one addition — see Global Constraints C7).

## Global Constraints

- **C1. Scope is daily bars.** `bars_daily`: 51,081,227 rows, 585,266
  instruments, 2016-01-01 → **2026-08-21**. `bars_intraday`: 2,237,083 rows,
  30 instruments. Multi-year intraday is out of scope and chunked delivery is
  not designed around.
- **C2. FastAPI routes are `def`, never `async def`.** psycopg is synchronous;
  an async route blocking the event loop deadlocked the gateway permanently.
  `test_no_route_is_a_coroutine_function` guards this. GETs must never write.
- **C3. Money crosses process boundaries as strings.** JSON numbers are IEEE
  754 doubles. Every monetary field in `RunOutcome` is already `str`; the
  equity curve follows without exception.
- **C4. `src/trading/runtime/` is copied into the sandbox image.** Nothing in
  that package may import `psycopg`, `docker`, or `trading.config`.
  `tests/agent_contract/test_image_contents.py` enforces it. Anything needing
  a database belongs in `trading.agent_contract.smoke`, on the host.
- **C5. The contract's *surface* is not touched.** No new `platform_sdk`
  field, no changed signature, nothing that would invalidate a strategy
  already written against it. Phase 2's acceptance bar stands at 1 of 3 model
  families and a structural change would invalidate the rounds already run.
  D3b-1a corrects one wrong *sentence*, which is a different thing.
- **C6. Verify by mutation, not by a green suite.** For each fix: prove the
  defect exists first, then break the fix deliberately and confirm the test
  goes red. This codebase's review history is that review, not tests, found
  nearly every real defect.
- **C7. (Added at approval.) A window past available data must be named, not
  silently run.** `bars_daily` ends 2026-08-21. A backtest requested through
  "today" would otherwise run on two weeks of nothing and report a flat tail
  as fact. Task 5 makes the sizing gate report coverage.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/trading/runtime/provider.py` | `BarRecord` gains `knowable_at`; `close_ts` stops deriving when set | 1 |
| `src/trading/runtime/payload.py` | Codec carries `knowable_at` across the container boundary | 1 |
| `src/trading/agent_contract/smoke.py` | Daily fetch stamps `knowable_at`; sizing + coverage gate; backtest orchestration | 1, 5, 7 |
| `src/trading/runtime/state.py` | `RunState` accumulates curve points | 3 |
| `src/trading/runtime/outcome.py` | `RunOutcome.equity_curve` | 3 |
| `src/trading/runtime/loop.py` | Samples the curve; `dispatch_from` | 3, 4 |
| `docs/agent-contract/STRATEGY_CONTRACT.md` | §5 `ts` wording for `1d` | 2 |
| `src/trading/agent_contract/sandbox.py` | `SandboxLimits.for_backtest()` profile | 6 |
| `src/trading/agent_contract/api.py` | `POST /strategies/{id}/backtests` | 7 |
| `src/trading/agent_contract/schemas.py` | Request/response models | 7 |

---

### Task 1: D3b-1 — the daily clock, and the money bug it causes

The one to test hardest. `BarRecord.close_ts` derives `ts + interval_sec`.
For a daily bar `ts` is already stored at the session close (every
`bars_daily` row is 10:00 UTC / 15:30 IST), so `close_ts` resolves to **the
next day's 15:30 IST**. `run_loop` sets its clock to it and then uses it for
the DP scrip-day key (`loop.py:288`), an order's `submitted_at`,
`decide_fill`'s anti-lookahead comparison, and the `day_open_equity` rollover.

**The trap:** the strategy runs *inside* the container, fed by
`decode_payload`. `_encode_series`/`_decode_series` carry nine fields.
If `knowable_at` is not added to **both**, the host stamps it, ships the bars,
and the container rebuilds every `BarRecord` without it — back to the broken
derivation, while every host-side test passes. Step 5 exists for exactly this.

**Files:**
- Modify: `src/trading/runtime/provider.py:20-49`
- Modify: `src/trading/runtime/payload.py:57-100`
- Modify: `src/trading/agent_contract/smoke.py:384-415` (`_fetch_daily_bars`)
- Test: `tests/runtime/test_loop.py`, `tests/runtime/test_provider.py`,
  `tests/runtime/test_payload.py`

**Interfaces:**
- Produces: `BarRecord(..., knowable_at: datetime | None = None)`;
  `BarRecord.close_ts -> datetime` returns `knowable_at` when set.
  Tasks 3, 4, 5, 7 all record timestamps produced by this and must land after it.

- [ ] **Step 1: Write the failing test — the strategy's clock is the session close**

**Corrected 2026-09-04.** The design asked for a DP scrip-day test. Probing it
first (as C6 requires) showed the claim is false: `ts + 86400` is an injective
shift on dates, so three sessions give three distinct DP keys under both
clocks, and `compute_charges` takes no date. A DP test would pass before and
after the fix. The real defect is that `ctx.now` — and therefore every
timestamp the run emits, including D3b-2's curve — is a full day late.

```python
def test_a_daily_bar_gives_the_strategy_the_session_close_as_its_clock() -> None:
    """bars_daily stores ts AT the session close (every row is 10:00 UTC /
    15:30 IST). Deriving close_ts as ts + 86400 hands the strategy a clock
    reading the NEXT day, so month-end, day-of-week and holiday logic are all
    wrong and the strategy cannot tell. Every timestamp the run emits -- an
    order's submitted_at, and the equity curve -- inherits the same lag.
    """
    session_close = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)  # 15:30 IST
    seen: list[datetime] = []

    class RecordsClock:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            seen.append(ctx.now)

    outcome = run_loop(
        strategy=RecordsClock(),
        bars=InMemoryBars({1: (_daily_bar(1, session_close, "100"),)}),
        schedules=(),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
    )

    assert outcome.ok, outcome.error
    assert seen == [session_close]
    assert seen[0].astimezone(_IST).date() == date(2026, 3, 2)
```

Add the helpers beside `_bar` in that file:

```python
def _daily_bar(instrument_id: int, ts: datetime, price: str) -> BarRecord:
    """A bars_daily row as stored: ts IS the session close (15:30 IST)."""
    return BarRecord(
        instrument_id=instrument_id,
        ts=ts,
        interval_sec=86400,
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        volume=Decimal("100"),
        knowable_at=ts,
    )
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/runtime/test_loop.py::test_daily_bar_sells_on_consecutive_sessions_are_billed_two_dp_days -v`
Expected: FAIL — `TypeError: BarRecord.__init__() got an unexpected keyword argument 'knowable_at'`.
That is the right first failure: the field does not exist yet.

- [ ] **Step 3: Add the field and stop deriving**

In `src/trading/runtime/provider.py`, add after `oi_change`:

```python
    # Set only where the interval's arithmetic cannot produce the answer.
    # A daily bar's row is stamped AT the session close, and an NSE session
    # is 6h15m of market time inside a 24-hour calendar interval, so
    # `ts + 86400` never lands on the close for any choice of `ts`.
    knowable_at: datetime | None = None
```

and replace `close_ts`:

```python
    @property
    def close_ts(self) -> datetime:
        """When this bar's values became knowable.

        For intraday bars `ts` marks the START of the interval (contract
        §5), so a clock set to `ts` while reading `close` would be reading
        the future, and `ts + interval_sec` is correct. A daily bar is not
        an interval in that sense: the row is stamped at the session close
        and the session is shorter than the calendar day it sits in, so the
        fetch path states the answer via `knowable_at` instead.
        """
        if self.knowable_at is not None:
            return self.knowable_at
        return self.ts + timedelta(seconds=self.interval_sec)
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/runtime/test_loop.py -v -k daily_bar_sells`
Expected: PASS. Then `uv run pytest tests/runtime/ -q` — all green.

- [ ] **Step 5: Write the failing codec test — the container must see it too**

Add to `tests/runtime/test_payload.py`:

```python
def test_knowable_at_survives_the_payload_round_trip() -> None:
    """The strategy runs inside the container, rebuilt by decode_payload.
    A knowable_at that does not cross the wire leaves the container deriving
    ts + 86400 again -- the fix would pass every host-side test and be absent
    exactly where charges are computed.
    """
    ts = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)
    bar = BarRecord(
        instrument_id=1, ts=ts, interval_sec=86400,
        open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"),
        knowable_at=ts,
    )
    decoded = decode_payload(encode_payload(SmokePayload(mode="smoke", source="x", bars={1: (bar,)})))
    assert decoded.bars[1][0].knowable_at == ts
    assert decoded.bars[1][0].close_ts == ts
```

- [ ] **Step 6: Run it and watch it fail**

Run: `uv run pytest tests/runtime/test_payload.py -v -k knowable_at`
Expected: FAIL — `assert None == datetime(...)`. The codec drops the field.

- [ ] **Step 7: Carry it through the codec**

In `_encode_series` add a tenth column:

```python
        # None for intraday (the arithmetic is right there); an ISO string
        # for daily. Encoded per bar rather than per series because a series
        # is not guaranteed homogeneous by anything but convention.
        "k": [None if bar.knowable_at is None else bar.knowable_at.isoformat() for bar in series],
```

In `_decode_series`, add `knowable_at` to the constructor and `column["k"]`
to the `zip`, keeping `strict=True`:

```python
            knowable_at=None if k is None else datetime.fromisoformat(k),
```

```python
        for ts, o, h, low, c, v, t, oi, oic, k in zip(
            column["ts"], column["o"], column["h"], column["l"], column["c"],
            column["v"], column["t"], column["oi"], column["oic"], column["k"],
            strict=True,
        )
```

- [ ] **Step 8: Run it and watch it pass**

Run: `uv run pytest tests/runtime/test_payload.py -q`
Expected: PASS, all payload tests green.

- [ ] **Step 9: Stamp it on the daily fetch path**

In `src/trading/agent_contract/smoke.py`, `_fetch_daily_bars`, add to the
`BarRecord(...)` construction:

```python
                    # bars_daily.ts IS the session close (verified: every row
                    # is 10:00 UTC / 15:30 IST), which is a fact about the
                    # session, not an arithmetic consequence of the interval.
                    knowable_at=row["ts"],
```

- [ ] **Step 10: Mutation check**

Temporarily restore `close_ts` to the unconditional derivation. Run
`uv run pytest tests/runtime/test_loop.py -k daily_bar_sells` and confirm it
goes RED. Then separately drop `"k"` from `_encode_series` and confirm the
payload test goes RED. Restore both.

- [ ] **Step 11: Full suite and commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
git add -A
git commit -m "fix(runtime): a daily bar's clock is the session close, not ts + 86400"
```

---

### Task 2: D3b-1a — the contract says what the runtime does, pinned by a test

Task 1 leaves contract §5 telling strategy authors that `ts` marks the START
of the interval, while for `1d` the runtime now deliberately treats it as the
session close. Same defect shape as the two dogfood rounds already forced
(`2fbcfc5`, `2daf103`): prose internally coherent and disagreeing with code,
invisible to a suite whose every strategy was written by someone who knew.
The **prose changes to match the runtime**, not the reverse.

**Files:**
- Modify: `docs/agent-contract/STRATEGY_CONTRACT.md` (§5, one paragraph)
- Test: `tests/agent_contract/test_contract_bundle.py`

**Interfaces:**
- Consumes: `BarRecord.knowable_at` from Task 1.

- [ ] **Step 1: Write the failing test**

Add to `tests/agent_contract/test_contract_bundle.py`, joining
`test_the_documented_OrderUpdate_matches_the_object_strategies_receive`:

```python
def test_the_contract_states_what_ts_means_for_daily_bars() -> None:
    """§5 documented `ts` as the interval START without qualification. For
    `1d` that is false: the row is stamped at the session close and the
    runtime treats it that way. A reader following the unqualified rule
    would place a daily bar 6h15m early and mis-time every decision.
    """
    text = (CONTRACT_PATH).read_text()
    section = _section(text, "§5")
    assert "session close" in section.lower()
    assert "1d" in section
```

Add `_section` if absent — split on the heading markers already used in that file.

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/agent_contract/test_contract_bundle.py -v -k daily_bars`
Expected: FAIL — the §5 text says nothing about the session close.

- [ ] **Step 3: Correct the one wrong sentence**

In `docs/agent-contract/STRATEGY_CONTRACT.md` §5, after the existing sentence
that `ts` marks the start of the interval, add:

```markdown
**Exception — `1d` bars.** For daily bars `ts` is the **session close**, not
the interval start. A daily row records a whole session, and an NSE session
is 6h15m of market time inside a 24-hour calendar day, so there is no
interval-start timestamp that `ts + 1 day` would turn into the close. The
platform stamps the close because that is when the bar became knowable, and
your `ctx.now` during `on_bar` is that same instant.
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/agent_contract/test_contract_bundle.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(contract): §5 says what ts means for 1d bars, pinned by a test"
```

---

### Task 3: D3b-2 — the equity curve is the breaker's number

`loop.py:340` already evaluates `ctx.portfolio.equity` every iteration for the
drawdown check and discards it. Record that same value at that same point.
Sampling there rather than recomputing is what makes "a drawdown drawn in 3e
and a breaker latch recorded in the same run cannot disagree" enforceable:
they are one number read once.

**Files:**
- Modify: `src/trading/runtime/state.py` (`RunState`)
- Modify: `src/trading/runtime/outcome.py` (`RunOutcome`)
- Modify: `src/trading/runtime/loop.py:~340` and both `RunOutcome(...)` returns
- Test: `tests/runtime/test_loop.py`

**Interfaces:**
- Produces: `RunOutcome.equity_curve: tuple[dict[str, str], ...]`, each point
  `{"ts": <iso>, "equity": <str>, "cash": <str>}`. 3c/3d/3e consume this exact shape.

- [ ] **Step 1: Write the failing test — the curve IS the breaker's number**

```python
def test_a_latched_breaker_equity_appears_as_the_final_curve_point() -> None:
    """D3b-2's claim is that the curve and the breaker cannot disagree,
    because they are the same read. Asserting equality is what makes that
    enforceable rather than aspirational.
    """
    outcome = _run_until_breaker_latches(max_daily_loss=Decimal("100"))
    assert outcome.breaker_reason is not None
    assert outcome.equity_curve
    assert outcome.equity_curve[-1]["equity"] == outcome.final_equity
    assert len(outcome.equity_curve) == outcome.bar_calls
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/runtime/test_loop.py -v -k latched_breaker_equity`
Expected: FAIL — `AttributeError: 'RunOutcome' object has no attribute 'equity_curve'`.

- [ ] **Step 3: Add the field to `RunState` and `RunOutcome`**

In `state.py`, on `RunState`:

```python
    # One (ts, equity, cash) triple per dispatched bar. Money as strings for
    # the reason outcome.py's docstring gives: JSON numbers are IEEE 754
    # doubles, and a curve of subtly wrong equity is worse than no curve.
    equity_curve: list[dict[str, str]] = field(default_factory=list)
```

In `outcome.py`, on `RunOutcome`, after `crashed_at`:

```python
    equity_curve: tuple[dict[str, str], ...] = ()
```

- [ ] **Step 4: Sample it where the breaker already reads equity**

In `loop.py`, immediately after `equity = ctx.portfolio.equity` in step 6:

```python
            # The breaker's own number, recorded rather than recomputed:
            # a second mark-to-market could drift from this one, and a
            # platform whose headline feature is an honest cost model
            # cannot have its curve disagree with its own breaker.
            state.equity_curve.append(
                {"ts": ts_iso, "equity": str(equity), "cash": str(state.cash)}
            )
```

Add `equity_curve=tuple(state.equity_curve),` to **both** `RunOutcome(...)`
constructions (the `_Crash` path and the success path) — a crashed run's
partial curve is evidence, not noise.

- [ ] **Step 5: Run it and watch it pass**

Run: `uv run pytest tests/runtime/test_loop.py -q`
Expected: PASS.

- [ ] **Step 6: Determinism must survive**

Run the existing two-pass comparison tests:
`uv run pytest tests/agent_contract/test_smoke.py -q -k determin`
Expected: PASS — equal runs produce equal curves. If this fails, the curve is
carrying a wall-clock or iteration-order artefact and must be fixed, not the test.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(runtime): RunOutcome carries the equity curve the breaker already computes"
```

---

### Task 4: D3b-3 — `history_bars` is honoured by widening the fetch

`DataRequest.history_bars` is declared in `platform_sdk.py`, constrained in
`schema.json`, documented in the contract — and **read by no code in `src/`**.
A strategy declaring `history_bars=200` and calling `ctx.data.bars(id, 200)`
in its first `on_bar` gets whatever exists, which for a backtest starting at
the window's first bar is nothing.

Warm-up does not mean dispatching `on_bar` early. It means the lookback API is
already populated when the first `on_bar` fires. Fetch from
`start − history_bars` sessions, ship those bars like any other, begin
*dispatch* at `start`.

**Files:**
- Modify: `src/trading/runtime/loop.py` (`run_loop` gains `dispatch_from`)
- Modify: `src/trading/agent_contract/smoke.py` (widen the window, read the manifest)
- Test: `tests/runtime/test_loop.py`, `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: Task 1's `knowable_at` (the widened bars are daily).
- Produces: `run_loop(..., dispatch_from: datetime | None = None)`. When set,
  bars whose `close_ts` is strictly before it are loaded into history and
  advance the cursor, but do not dispatch `on_bar`.

- [ ] **Step 1: Write the failing test — both halves, from inside a strategy**

Either half passing alone while the other breaks is the failure mode, so
assert both:

```python
def test_warm_up_populates_history_without_dispatching_early() -> None:
    """A strategy asking for 200 bars of warm-up must SEE 200 closed bars on
    its first on_bar, and that first on_bar must be at `start` -- not 200
    bars earlier. Warm-up populates the lookback; it does not move the run.
    """
    day = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)
    warm = [_daily_bar(1, day + timedelta(days=i), "100") for i in range(10)]
    live = [_daily_bar(1, day + timedelta(days=10 + i), "101") for i in range(3)]
    recorder = _RecordsFirstBarStrategy(instrument_id=1, lookback=10)
    outcome = run_loop(
        recorder,
        InMemoryBars({1: tuple(warm + live)}),
        (),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
        dispatch_from=live[0].close_ts,
    )
    assert outcome.ok, outcome.error
    assert recorder.first_ts == live[0].close_ts       # dispatch began at start
    assert recorder.history_len == 10                  # warm-up was visible
    assert outcome.bar_calls == 3                      # only the live bars ran
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/runtime/test_loop.py -v -k warm_up_populates`
Expected: FAIL — `TypeError: run_loop() got an unexpected keyword argument 'dispatch_from'`.

- [ ] **Step 3: Implement `dispatch_from`**

Add the parameter to `run_loop`'s signature:

```python
    dispatch_from: datetime | None = None,
```

Inside the `for close_ts, indexed in bars.indexed_groups():` loop, after
`state.now = close_ts` and the marks are applied, but **before** step 1
(pricing resting orders):

```python
            if dispatch_from is not None and close_ts < dispatch_from:
                # Warm-up: this bar is history the strategy may read, not an
                # event it experiences. No fills are priced against it (there
                # are no orders yet, and inventing them would be lookahead),
                # no handler is called, and the breaker is not evaluated --
                # but the cursor advances, which is what makes the bar
                # readable via ctx.data.bars() at the first real dispatch.
                for bar, index in indexed:
                    state.cursor[bar.instrument_id] = index + 1
                continue
```

Set the initial clock from the first *dispatched* bar rather than the first
bar overall, so `initialize` does not run at a warm-up timestamp: replace
the `first_ts` seeding with the first group at or after `dispatch_from` when
it is set.

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/runtime/test_loop.py -q`
Expected: PASS.

- [ ] **Step 5: Write the failing short-history test**

```python
def test_short_history_reports_the_shortfall_rather_than_running_quietly() -> None:
    """A strategy warmed on 40 of the 200 bars it asked for is a different
    experiment from the one requested, and must not be reported as the one
    requested."""
    plan = plan_backtest(conn, manifest_with(history_bars=200), [58607], start, end)
    assert plan.history_bars_requested == 200
    assert plan.history_bars_available < 200
```

- [ ] **Step 6: Run it, watch it fail, then widen the window in `smoke.py`**

Read `history_bars` from the manifest's `data` block, subtract that many
*sessions* (not calendar days) from `start` when selecting the window, and
record both the requested and available counts on the window dict.

- [ ] **Step 7: Full suite and commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
git add -A
git commit -m "feat(runtime): honour history_bars by widening the fetch, not by dispatching early"
```

---

### Task 5: D3b-4 + C7 — refuse by estimate, and never run past the data

Two refusals, one gate, both **before any bar is fetched**. Materialising five
million rows to learn they do not fit spends exactly the cost the gate exists
to avoid, and an OOM inside the container surfaces as `SMOKE_OOM`, which tells
an operator their strategy crashed when in fact their request was too big.

The coverage half is C7: `bars_daily` ends 2026-08-21, so a window through
"today" would run on two weeks of nothing and report a flat tail as fact —
the same silent-wrong-data defect 3a exists to eliminate.

**Files:**
- Modify: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Produces: **one** pre-flight seam, `plan_backtest(conn, manifest,
  instrument_ids, start, end, *, limits) -> BacktestPlan`. Task 4 adds its
  history fields; Task 5 adds sizing and coverage. One function because all
  three are pre-flight facts about the same request, established by the same
  window query — three functions would each re-ask it.
  `BacktestPlan` fields: `start`, `end`, `dispatch_from`,
  `history_bars_requested`, `history_bars_available`, `instruments`,
  `sessions`, `estimated_bars`, `data_start`, `data_end`,
  `findings: tuple[Finding, ...]`.
- Produces findings `BACKTEST_TOO_LARGE` and `BACKTEST_WINDOW_UNCOVERED`,
  joining `MANIFEST_UNRESOLVABLE` in `smoke.py`'s stable code list (line 72).

- [ ] **Step 1: Write the failing test — the gate refuses WITHOUT fetching**

A gate that refuses *after* fetching passes a naive assertion on the finding
code alone, so count queries rather than trusting the code:

```python
def test_the_sizing_gate_refuses_before_fetching_any_bar(monkeypatch, db_conn) -> None:
    fetches: list[Any] = []
    monkeypatch.setattr(smoke, "fetch_bars", lambda *a, **k: fetches.append(a))
    plan = smoke.plan_backtest(db_conn, _manifest(), _many(500), start=..., end=...)
    assert any(f.code == "BACKTEST_TOO_LARGE" for f in plan.findings)
    assert fetches == []          # nothing was materialised
```

- [ ] **Step 2: Run it and watch it fail**

Expected: FAIL — `AttributeError: module has no attribute 'plan_backtest'`.
(Task 4 introduced `plan_backtest` with its history fields; this task adds
the sizing and coverage findings to the same object.)

- [ ] **Step 3: Implement the estimate**

`COUNT(DISTINCT session_date)` over the window plus the instrument count; the
estimate is their product. The ceiling is **configuration derived from the
run's memory limit**, not an independent guess, so the two cannot drift:

```python
def _bar_ceiling(limits: SandboxLimits) -> int:
    """Bars that fit the run's decoded memory. Derived from the limit rather
    than guessed beside it -- one number moving with the other."""
    megabytes = int(limits.memory.rstrip("m"))
    return int(megabytes * _BARS_PER_MB)
```

Refuse with a finding naming the estimate, the ceiling, and the two levers:

```python
Finding(
    code="BACKTEST_TOO_LARGE",
    message=(
        f"{instruments} instruments x {sessions} sessions = ~{estimated:,} bars, "
        f"over the {ceiling:,}-bar ceiling for a {limits.memory} run. "
        "Narrow the universe or shorten the window."
    ),
    contract_section="§9",
)
```

- [ ] **Step 4: Run it and watch it pass**

- [ ] **Step 5: Write the failing coverage test (C7)**

```python
def test_a_window_extending_past_available_data_is_named_not_silently_run(db_conn) -> None:
    """bars_daily ends 2026-08-21. A backtest asked for a window through today
    must be told, not handed a flat tail it will read as a fact about the
    market."""
    plan = smoke.plan_backtest(db_conn, _manifest(), [58607],
                               start=date(2026, 1, 1), end=date(2026, 9, 4))
    finding = _only(f for f in plan.findings if f.code == "BACKTEST_WINDOW_UNCOVERED")
    assert "2026-08-21" in finding.message
    assert "2026-09-04" in finding.message
```

- [ ] **Step 6: Run it, watch it fail, then implement**

The same `COUNT` already knows `MAX(session_date)`. Compare it to the
requested `end` and emit the finding when the request runs past it, naming
both dates and how many sessions are missing.

- [ ] **Step 7: Full suite and commit**

```bash
git add -A
git commit -m "feat(agent-contract): refuse an oversized run by estimate, and never run past the data"
```

---

### Task 6: D3b-5 — a backtest profile, not a raised global default

`SandboxLimits` defaults are deliberately tight (256m, 30s) and its docstring
says why: "A strategy that legitimately needs more should say so and be
granted it explicitly, rather than every strategy inheriting the headroom the
greediest one needed." A backtest is that explicit grant. Raising the shared
default would hand every upload the backtester's headroom — exactly what that
docstring refuses.

**Files:**
- Modify: `src/trading/agent_contract/sandbox.py:90-114`
- Test: `tests/agent_contract/test_sandbox.py`

**Interfaces:**
- Produces: `SandboxLimits.for_backtest(base: SandboxLimits | None = None) -> SandboxLimits`.

- [ ] **Step 1: Write the failing test**

```python
def test_the_backtest_profile_raises_limits_without_moving_the_smoke_defaults() -> None:
    backtest = SandboxLimits.for_backtest()
    assert backtest.memory != SandboxLimits().memory
    assert backtest.timeout_seconds > SandboxLimits().timeout_seconds
    # The smoke path is untouched: the greediest run must not set the default.
    assert SandboxLimits().memory == "256m"
    assert SandboxLimits().timeout_seconds == 30.0


def test_the_backtest_profile_inherits_runtime_and_context() -> None:
    """A backtest must be confined by gVisor wherever a smoke run is."""
    base = SandboxLimits(runtime="runsc", docker_context="colima-sandbox")
    assert SandboxLimits.for_backtest(base).runtime == "runsc"
    assert SandboxLimits.for_backtest(base).docker_context == "colima-sandbox"
```

- [ ] **Step 2: Run it and watch it fail**

Expected: FAIL — `AttributeError: type object 'SandboxLimits' has no attribute 'for_backtest'`.

- [ ] **Step 3: Implement the profile**

```python
    @classmethod
    def for_backtest(cls, base: "SandboxLimits | None" = None) -> "SandboxLimits":
        """The explicit grant this class's docstring asks for.

        `runtime` and `docker_context` are inherited, never defaulted here:
        a backtest must be confined exactly as a smoke run is, and choosing
        one without the other yields a run confined differently than it claims.
        """
        base = base or cls()
        return replace(base, memory="2048m", timeout_seconds=600.0)
```

- [ ] **Step 4: Run it and watch it pass, then commit**

```bash
uv run pytest tests/agent_contract/test_sandbox.py -q
git add -A
git commit -m "feat(agent-contract): a backtest profile, not a raised global default"
```

---

### Task 7: D3b-6 — the route, against a registered version

Runs against a **registered version** rather than submitted source, following
the registry's existing rule that a registered version is immutable "because
results already attributed to that version must keep describing the code that
produced them." A backtest result is exactly such an attribution — so the
source is read from the row, and a backtest cannot run code that was never
validated. The window is the **caller's**, not the manifest's.

3b **returns** the result and does not store it. Persistence is 3c.

**Files:**
- Modify: `src/trading/agent_contract/api.py`
- Modify: `src/trading/agent_contract/schemas.py`
- Modify: `src/trading/agent_contract/smoke.py` (orchestration)
- Test: `tests/agent_contract/test_api.py`

**Interfaces:**
- Consumes: Tasks 1, 3, 4, 5, 6.
- Produces: `POST /strategies/{strategy_id}/backtests`, body
  `{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}`, returning the run summary,
  findings, and `equity_curve`.

- [ ] **Step 1: Write the failing route test**

```python
def test_backtest_runs_against_the_registered_source_not_a_submitted_one(client) -> None:
    """A result attributed to a version must describe the code that produced
    it, so the source comes from the registry row and the request cannot
    smuggle its own."""
    strategy_id = _register(client, _buy_and_hold_source(), version="1.0.0")
    response = client.post(
        f"/strategies/{strategy_id}/backtests",
        json={"start": "2026-01-01", "end": "2026-03-01"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["equity_curve"]
    assert body["bars"] == "1d"


def test_a_backtest_for_an_unknown_strategy_is_404(client) -> None:
    assert client.post("/strategies/999999/backtests",
                       json={"start": "2026-01-01", "end": "2026-03-01"}).status_code == 404
```

- [ ] **Step 2: Run it and watch it fail**

Expected: FAIL — 405/404 for an unrouted path.

- [ ] **Step 3: Add the schemas**

```python
class BacktestRequest(BaseModel):
    start: date
    end: date


class BacktestResponse(BaseModel):
    strategy_id: int
    status: str
    bars: str | None = None
    bar_calls: int | None = None
    fills: int | None = None
    final_cash: str | None = None
    final_equity: str | None = None
    breaker_reason: str | None = None
    equity_curve: list[dict[str, str]] = []
    findings: list[FindingOut] = []
```

- [ ] **Step 4: Add the route — `def`, never `async def` (C2)**

```python
@router.post("/strategies/{strategy_id}/backtests", response_model=BacktestResponse)
def run_backtest(
    strategy_id: int,
    request: BacktestRequest,
    conn: Connection = Depends(get_db_connection),
) -> BacktestResponse:
    """Blocks for the run, matching `POST /strategies`. Measured compute is
    ~2,600 dispatches for a daily decade against a loop that does 481,000
    bars/sec, so the honest expectation is seconds. `api.py` already documents
    why that is right for one operator and wrong for a queue; a job queue is
    not built for a wait that does not exist.
    """
```

Read the source and manifest from the registry row (404 when absent), run the
Task 5 gate, then execute with `SandboxLimits.for_backtest(...)`.

- [ ] **Step 5: Run it and watch it pass**

- [ ] **Step 6: Confirm the route-shape guard still holds**

Run: `uv run pytest tests/streaming/ -q -k coroutine`
Expected: PASS — `test_no_route_is_a_coroutine_function`.

- [ ] **Step 7: Full suite, lint, types, commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
git add -A
git commit -m "feat(agent-contract): POST /strategies/{id}/backtests, against a registered version"
```

---

## Verification before calling 3b done

- [ ] `uv run pytest tests/ -q -m "not golden and not sandbox and not live"` green
- [ ] `uv run pytest tests/ -q -m sandbox` green (spawns real containers)
- [ ] ruff, ruff format, mypy clean
- [ ] Each mutation check in C6 performed and restored
- [ ] A real backtest run end-to-end against the live stack, not only tests —
      the pattern 3a established, which is what caught the `"5m"` gap
- [ ] `docs/STATUS.md` updated: 3b state, and whether D3b-1's clock change
      alters any figure previously recorded from a `1d` run
