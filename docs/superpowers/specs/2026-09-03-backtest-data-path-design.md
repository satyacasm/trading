# Serving the bar interval a strategy actually declared — Phase 3, sub-project 3a

**Status:** design, approved 2026-09-03.
**Phase:** 3 (Backtesting + metrics), first sub-project.
**Precedes:** 3b (backtest runs at scale + persistence), which consumes this
unchanged — `run_loop` and `BarRecord` are untouched by this design.

---

## 1. What this is, and why Phase 3 starts here rather than with the engine

The implementation plan budgets Phase 3's hard part as "event-driven backtest
engine... *frontier tier*." That engine already exists: `trading.runtime.loop`
is the same loop the smoke run drives, and it was deliberately built as a pure
package with no psycopg or Docker import specifically so a later phase could
call it directly. Measured against Reliance's full 4.7-year 1-minute history
(430,274 bars) run outside the sandbox: **0.9 seconds**, ≈481,000 bars/sec,
correct final equity. Phase 3 does not need a new engine.

What it needs first is data the engine can trust, and two problems sit in
front of that:

**A live silent-wrong-data defect.** The contract advertises five bar
intervals (`1m`, `5m`, `15m`, `1h`, `1d`) in both §3 and `schema.json`, and a
strategy declares one via `DataRequest(bars=...)`. Nothing reads that field.
`trading.agent_contract.smoke.fetch_bars` hardcodes `bars_intraday` at
`interval_sec = 60` regardless of what the manifest asked for. A strategy
declaring `bars="1d"` today receives 1-minute bars and is told nothing —
exactly the class of defect this project has spent two phases refusing to
ship (`InvalidChargeSchedule`, the §9 dogfood fixes, the smoke-run
determinism check). It has gone unnoticed because every strategy written
against this platform so far declared `1m`.

**The universe a sweep needs lives in a different table.** `bars_intraday`
covers 30 instruments. A "run this over the NSE universe" backtest — the
shape of backtest you said you actually want — can only be served from
`bars_daily`: 51M rows, 585,266 instruments, 2016–2026. Any sub-project past
this one is aimed at `bars_daily` by necessity.

**And `bars_daily` prices are unadjusted, which silently poisons every
metric.** Measured directly: three real 1:2 stock splits in the corporate
actions table appear in `bars_daily` as single-day closes of −51.0%, −49.0%,
and −47.6%. No wealth was lost — holders received twice the shares — but a
naive backtest reads a catastrophic drawdown that never happened. With 1,686
splits and 1,938 bonuses recorded, a multi-year universe sweep is riddled
with phantom crashes, and every metric downstream (Sharpe, max drawdown,
CAGR) inherits the corruption invisibly. This is precisely the
"backtest-trust erosion if results are subtly wrong" risk the implementation
plan's own risk register names.

The good news, found while investigating: **the adjustment layer already
exists.** `trading.corpactions.adjust.adjusted_bars(conn, instrument_id,
start, end, *, as_of)` was built in Phase 0, is tested
(`tests/corpactions/test_adjust.py`), and is point-in-time correct by
design — it computes an adjusted *view* at read time rather than rewriting
storage, so a backtest run `as_of` any date sees exactly the corporate-action
knowledge that existed then (D10). Nothing outside `trading/corpactions/`
calls it. This sub-project's entire scope is wiring an existing, correct
function into the strategy data path — not designing a new one.

---

## 2. Settled decisions

### D3a-1. Read `DataRequest.bars` and route on it — in both places that ignore it

`resolve_bar_interval(manifest) -> int` maps the five schema values to
seconds (`"1m"→60`, `"5m"→300`, `"15m"→900`, `"1h"→3600`, `"1d"→86400`).

**Two functions hardcode `bars_intraday`, not one.** `smoke_test`'s pipeline
calls `select_window(conn, instrument_ids)` *before* `fetch_bars`, and
`select_window`'s `_WINDOW_SQL` is equally hardcoded to
`FROM bars_intraday ... WHERE interval_sec = 60`. Fixing only `fetch_bars`
would ship a defect that looks identical to the one it replaces: a `bars="1d"`
manifest naming an instrument that exists only in `bars_daily` — measured at
585,261 of 585,299 instruments, i.e. all but the 5 that carry both series —
would have its window computed against zero 1-minute sessions, return
`NO_DATA`, and never reach the corrected `fetch_bars` at all. Both functions
take the resolved interval as a
parameter; `smoke_test` computes it once, right after the manifest resolves,
and threads it into both calls.

`interval_sec < 86400` keeps today's `bars_intraday` path in both functions
completely unchanged (byte-identical output — this is the regression the
tests pin). `interval_sec == 86400` routes `select_window` through a
`bars_daily`-shaped sibling query (same session-intersection logic, no
`interval_sec` column to filter on — `bars_daily` is one row per
instrument per day, not multiplexed like `bars_intraday`) and routes
`fetch_bars` through `adjusted_bars` per instrument.

**Rejected: adjust `bars_intraday` too.** Corporate actions in this dataset
are dated (`ex_date`, a `date` column) and NSE does not split mid-session, so
intraday adjustment inside a single day is a non-problem, and adjusting
*across* days at 1-minute granularity is unneeded machinery this sub-project
doesn't have a use case for. Deferred, not designed away — if a future
multi-day intraday strategy needs it, `adjusted_bars`'s query shape extends
directly.

### D3a-2. `as_of` is the backtest window's end date, fixed for the whole run

One factor set is computed once and applied to every bar in the run. The
series is continuous and returns are correct throughout — which is what
every downstream metric (3b onward) needs. The accepted cost, already
implicit in `adjusted_bars`'s own docstring ("point-in-time... `as_of` any
date"): a bar from early in a long backtest reflects a split that happens
years later in simulated time, so its absolute price level does not match
what the exchange actually printed that day.

**Rejected: `as_of` = the simulated date, advancing with the run.** Prices
would always match what was really printed, which matters for anything
price-level dependent (limit orders, lot sizing, tick-size rounding, the
₹-denominated circuit breakers this platform already enforces). But the
series would jump at every ex-date, meaning `run_loop`'s per-bar return
calculations would need to read adjustment factors rather than raw closes —
a change to the runtime this sub-project's scope explicitly excludes (see
§3). Left for a future sub-project if backtests ever need price-level
fidelity over long horizons; flagged here so it is a known, deliberate
deferral rather than a discovered gap later.

**Rejected: split feed (raw prices to the strategy, factors to the
metrics).** Most faithful to reality, and the most machinery: `BarRecord`
would need to carry a factor, and every consumer of a `Bar` — the fill
model, the strategy, the metrics layer that doesn't exist yet — would need
to agree on what "the price" means in a given call. Revisit only if D3a-2's
approximation proves to matter empirically.

### D3a-3. `BarRecord`, not a new type

`adjusted_bars` returns a polars `DataFrame` (columns: `ts`, `open`, `high`,
`low`, `close`, `prev_close`, `volume`). `fetch_bars` converts each row into
the existing `BarRecord` dataclass — the same struct the 1-minute path
already produces and the only thing `run_loop`/`InMemoryBars` know how to
consume. `run_loop` is unmodified by this sub-project. `open_interest`,
`oi_change`, and `trades` are `None` for the daily path (not carried by
`bars_daily`), which `BarRecord` already permits as optional fields.

---

## 3. Explicitly out of scope

Named so they read as sequencing, not oversight:

- **Total-return series (dividends).** `adjusted_bars` deliberately excludes
  DIVIDEND actions — folding them in produces a different concept
  (total-return vs. price-adjusted) that a later sub-project owns. 38,742
  dividend records exist and are unused here.
- **Raising `SandboxLimits.memory` for backtest runs, sizing gates, and
  result persistence.** That is 3b. This sub-project only fixes what data a
  strategy receives; it does not change how much of it fits in a container
  or what happens to the outcome afterward.
- **Metrics, report UI, walk-forward, robustness suite** (3c–3e).
- **Any change to `run_loop`, `BarRecord`, `InMemoryBars`, or the sandbox
  runner.** The entire point of this sub-project is that the runtime Phase 2
  built needs no changes — only what feeds it does.

---

## 4. Testing

- **The regression this defect deserves:** a `bars="1d"` strategy run across
  a window containing a real recorded split (COLAB, VLL, or NAVKARURB, all
  confirmed 1:2 splits with `bars_daily` rows straddling the ex-date) must
  produce a *continuous* equity curve — no phantom ±50% jump on the ex-date.
  This is the test that would have caught today's defect on day one, and its
  absence is why it shipped unnoticed.
- **1-minute strategies are provably unaffected:** `fetch_bars` output for
  `interval_sec < 86400` is asserted byte-identical to pre-change behavior.
- **`resolve_bar_interval`** is unit-tested against all five schema values,
  plus the unmapped/invalid case (should surface as a validation-stage
  finding, not a runtime crash — stage 1 already validates `DataRequest`
  against `schema.json`'s enum, so this is confirming that guarantee holds
  rather than adding new defensive code).
- **`as_of` semantics:** a bar dated *before* a split shows the split's
  adjustment factor when `as_of` is after the split's `ex_date`, and does
  not when `as_of` is before it — pinning D3a-2 against the point-in-time
  guarantee `adjusted_bars` already provides.
- **`select_window`'s daily branch is exercised, not just `fetch_bars`'s.**
  Task 4's review already found one query-shape branch (`_describe_manifest`'s
  Query universe) that shipped untested in this exact package; the fix here
  is a second query-shape branch in the same shape of function, and the same
  miss is avoidable by naming it as a required test up front rather than
  discovering it in review. Covered case: an instrument that exists **only**
  in `bars_daily` (no `bars_intraday` rows at all) still produces a non-empty
  window when `bars="1d"` — the case that silently returns `NO_DATA` today.
