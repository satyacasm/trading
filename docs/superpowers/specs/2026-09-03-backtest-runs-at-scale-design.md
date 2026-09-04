# Backtest runs at scale, and the equity curve — Phase 3, sub-project 3b

**Status:** design, awaiting approval 2026-09-03.
**Phase:** 3 (Backtesting + metrics), second sub-project.
**Follows:** 3a (the backtest data path), which this consumes unchanged.
**Precedes:** 3c (result persistence + read API), 3d (metrics), 3e (report
UI), 3f (walk-forward + robustness) — every one of which consumes the
equity curve defined here, which is why its shape is settled now rather
than later.

---

## 1. What this is

3a made `smoke_test` serve the interval a strategy declared. It did not
make anything run *long*. A smoke run is five sessions chosen by the
platform; a backtest is a window chosen by the operator, over years.

Four things stand between those two, and they belong in one sub-project
because three of them are only reachable once runs get long:

1. A daily bar's simulated clock is wrong, in a way that misattributes
   money. Invisible over five sessions; systematic over ten years.
2. `RunOutcome` reports a final scalar. Every metric in 3d–3f needs a
   series, and nothing emits one.
3. `DataRequest.history_bars` is documented, schema-checked, and read by
   no code. A five-session smoke does not care. A backtest of a
   200-period strategy is meaningless without it.
4. Nothing refuses a run too large to hold, because nothing has been
   large enough to matter.

**What this is not is an engine.** The plan budgets Phase 3's hard part
as an "event-driven backtest engine... *frontier tier*." 3a measured the
existing one: `run_loop` against Reliance's full 4.7-year 1-minute
history — 430,274 bars — in **0.9 seconds**, ≈481,000 bars/sec, correct
final equity. `on_bar` dispatches once per *timestamp*, not once per bar
(`loop.py:323`), so a decade of daily bars over any universe width is
~2,600 dispatches. Compute is not the constraint here and no new engine
is needed. Memory is the constraint, and honesty about it is the work.

### Scope of "at scale", decided

**Daily bars, deep and wide.** `bars_daily` holds 51,081,227 rows across
585,266 instruments, 2016-01-01 to 2026-08-21. `bars_intraday` holds
2,237,083 rows across **30** instruments. The data itself therefore
imposes the trade-off: a wide backtest is a daily backtest, and an
intraday backtest is a narrow one. 3b targets the first. A 50-instrument
decade of daily bars is ~130,000 `BarRecord`s, which fits one payload
with a raised ceiling; multi-year intraday does not fit any, and the
architecture that would serve it (chunked or streamed delivery) is
deliberately deferred — see §3.

---

## 2. Settled decisions

### D3b-1. `close_ts` is wrong for daily bars, and it is a money bug — fix it first

`BarRecord.close_ts` is a derived property, `ts + interval_sec`,
documented as "when this bar's values became knowable." `run_loop` sets
the simulated clock to it (`loop.py:243`) and then uses it for four
things that matter: `decide_fill`'s anti-lookahead comparison, an order's
`submitted_at`, the DP-charge scrip-day key (`loop.py:288`), and the
`day_open_equity` rollover at the IST boundary.

That derivation is correct for contiguous intraday intervals and cannot
be correct for a daily bar. **An NSE session is 6h15m of market time
inside a 24-hour calendar interval**, so `ts + 86400` never lands on the
session close for any choice of `ts`. Compounding it, `bars_daily.ts` is
stored *at* the session close (verified: every row is 10:00 UTC / 15:30
IST), so today `close_ts` resolves to **the next day's 15:30 IST** — the
"uniform one-day lag" 3a carried forward, now located precisely.

The consequence is not cosmetic, but it is **not** the DP one this
paragraph originally claimed. **Corrected 2026-09-04, during
implementation, by probing it before writing the test the design asked
for:** `ts + 86400` is an *injective* shift on dates, so it maps every
session to a distinct day and the number of DP scrip-day keys is
invariant — three sessions give three keys under both clocks, and a
Friday/Monday pair gives two under both. `compute_charges` is handed
schedules already filtered by `_charge_key` on the host and takes no
date, so a day's shift cannot select a different rate either. There is no
DP mischarge, and a test asserting one would pass before and after the
fix.

What is actually wrong:

- **`ctx.now` is a full day ahead of reality.** A bar stored at
  2026-03-02 15:30 IST hands the strategy a clock reading 2026-03-03.
  Month-end, day-of-week and holiday logic are all simply wrong, and a
  strategy cannot detect it.
- **Every timestamp the run emits is a day late** — an order's
  `submitted_at`, and the equity curve D3b-2 introduces. A run over
  Jan 1 – Mar 1 reports its last curve point at Mar 2, outside the window
  that was asked for, and 3d would annualise over a range off by a day.

`day_open_equity` is **unaffected**: with daily bars every bar begins a
new IST day under either clock, so it rolls identically.

This is still the first thing to land, and for the reason the last
paragraph of this decision gives — everything below records timestamps
produced by it — but it is a correctness-of-the-clock fix, not a money
fix, and it should not be sold internally as the latter.

**Decision: stop deriving it.** `BarRecord` gains an optional explicit
`knowable_at: datetime | None`; `close_ts` returns it when set and falls
back to `ts + interval_sec` when not. The daily fetch path sets it to the
session close, which is a fact about the session rather than an arithmetic
consequence of the interval. Intraday bars set nothing and behave exactly
as today.

`bars_daily.ts` itself is left exactly as stored. For a daily bar it is
already the session close, so `knowable_at` equals `ts` and the loop's
clock becomes the moment the session actually ended — which is what a
daily strategy experiences.

Rejected alternative: re-stamping `bars_daily.ts` to the session open
(09:15 IST). It leaves `close_ts` at the next day's 09:15 — still wrong,
by 17h45m instead of 24h — because the flaw is the derivation, not the
column. Rejected alternative: re-stamping `ts` to the *previous* session's
close so the arithmetic works out. That makes `close_ts` right by making
`ts` a lie, and `ts` is what the strategy reads.

This lands first, and alone, because everything below records timestamps
produced by it. A curve persisted against the current clock would have to
be migrated.

### D3b-1a. The contract's `ts` wording is corrected for `1d`, and pinned by a test

D3b-1 leaves a documented rule disagreeing with the data. Contract §5 tells
strategy authors that `ts` marks the START of the interval; for `1d` bars
it is the session close, and after D3b-1 the runtime deliberately treats it
that way.

This is the same defect shape as the two the dogfood rounds have already
forced — §2 describing a class the runner refuses (`2fbcfc5`), `OrderUpdate`
never documented at all (`2daf103`) — prose internally coherent and
disagreeing with the code, invisible to a suite whose every strategy was
written by someone who already knew the rule. It is found here rather than
by round 4, and it is fixed the same way: the **prose changes to match the
runtime**, not the reverse, and a test reads the document and compares it to
the behaviour, joining
`test_the_documented_OrderUpdate_matches_the_object_strategies_receive`.

Scope of the edit is one paragraph in §5 stating that for `1d`, `ts` is the
session close because that is when a daily bar becomes knowable. No schema
change, no new field, no changed call signature — nothing a strategy already
written against the contract would have to be rewritten for. That is the
sense in which D3b-3 says the contract is untouched: its *surface* is
unchanged, while a sentence that was wrong is corrected.

### D3b-2. The equity curve is sampled where the breaker already computes equity

`loop.py:340` evaluates `ctx.portfolio.equity` on every iteration for the
breaker's drawdown check and discards it. The curve records that same
value at that same point: one `(ts, equity, cash)` triple per dispatched
bar, `ts` being the loop's clock (`close_ts`, per D3b-1), appended to
`RunState` and returned as a new `RunOutcome.equity_curve` field.

Sampling there rather than recomputing means a drawdown drawn in 3e and a
breaker latch recorded in the same run **cannot disagree** — they are the
same number read once.

Money crosses as strings, like every other field in `RunOutcome`, for the
reason that module's docstring already gives: JSON numbers are IEEE 754
doubles, and a curve of subtly wrong equity is worse than no curve.

One point per dispatch is ~2,600 points for a daily decade — small enough
that no downsampling policy is needed, and none is invented. When intraday
backtests arrive they will need one; that is 3f's problem, and the point
carries its own `ts` so downsampling stays possible without a format
change.

Rejected alternative: reconstructing the curve on the host from returned
fills. It requires a second mark-to-market implementation outside the
container, which can drift from the one inside it, in a platform whose
headline feature is an honest cost model. Rejected alternative: streaming
points on stdout. It replaces the single-JSON-envelope protocol
`_parse_runner_output` depends on, and buys nothing at this scale.

### D3b-3. `history_bars` is honoured by widening the fetch, not by changing the contract

`DataRequest.history_bars` is declared in `platform_sdk.py`, constrained
in `schema.json`, and documented in the contract as "bars of warm-up
before the first `on_bar`". **No code in `src/` reads it.** A strategy
declaring `history_bars=200` and calling `ctx.data.bars(id, 200)` in its
first `on_bar` receives whatever happens to exist — for a backtest
starting at the window's first bar, that is nothing.

Warm-up does not mean dispatching `on_bar` early. It means the lookback
API is already populated when the first `on_bar` fires.
`LiveDataAccess.bars` reads `InMemoryBars.history(instrument_id, closed)`,
which is clock-aware, so the implementation is:

- fetch from `start − history_bars` sessions rather than from `start`;
- ship those earlier bars in the payload like any other;
- begin *dispatch* at `start`.

The pre-start bars are then closed history at the first dispatch, and
`ctx.data.bars(id, 200)` returns 200 real prior sessions. `run_loop`
gains a `dispatch_from` argument; nothing else changes.

**The contract's surface is not touched** — no new field, no changed
signature, nothing that would invalidate a strategy already written against
it. That is deliberate: Phase 2's acceptance bar sits at 1 of 3 model
families, and a structural contract change now would invalidate the rounds
already run. (D3b-1a corrects one wrong *sentence*, which is a different
thing and is required for the same reason.)

If fewer than `history_bars` sessions exist before `start`, the run is not
silently shortened — the report says how many were available, because a
strategy warmed on 40 of the 200 bars it asked for is a different
experiment from the one requested.

### D3b-4. A pre-flight sizing gate refuses a run by estimate, before any bar is fetched

The ceiling is decoded container memory, not wire size. Columnar encoding
plus gzip handles transport well (`payload.py` notes ~10:1 on numeric
text); inside the container each bar is a `BarRecord` of `Decimal`s.

The gate estimates `instruments × sessions` with a `COUNT` against the
window **before fetching anything**, and refuses an oversized run with a
stable finding code — `BACKTEST_TOO_LARGE` — naming the estimate, the
ceiling, and the two levers (narrow the universe, shorten the window).

Estimating rather than discovering is the point. Materialising five
million rows to learn they do not fit spends the cost the gate exists to
avoid, and an OOM inside the container surfaces as `SMOKE_OOM`, which
would tell an operator their strategy crashed when in fact their request
was too big.

The ceiling is configuration, not a literal, and its default is derived
from the run's memory limit rather than guessed independently — one
number moving with the other, so they cannot drift into disagreeing.

### D3b-5. Backtest limits are a distinct profile, not a raised global default

`SandboxLimits` defaults are deliberately tight — 256m, 30s — and its
docstring states why: "A strategy that legitimately needs more should say
so and be granted it explicitly, rather than every strategy inheriting the
headroom the greediest one needed."

A backtest is that explicit grant. It gets its own profile with a raised
memory ceiling and timeout; the smoke path keeps today's values unchanged.
Raising the shared default would hand every upload the backtester's
headroom, which is exactly what that docstring refuses.

`runtime` and `docker_context` are inherited from settings as they are
now, so a backtest is confined by gVisor wherever a smoke run is.

### D3b-6. A backtest names a registered version and an explicit window; the request blocks

`POST /strategies/{strategy_id}/backtests`, with `start` and `end` in the
body. Plain `def`, like every route in this codebase.

Running against a **registered version** rather than submitted source
follows the registry's existing rule that a registered version is
immutable "because results already attributed to that version must keep
describing the code that produced them." A backtest result is exactly such
an attribution. The source is read from the row, so a backtest cannot run
code that was never validated.

The window is the **caller's**, not the manifest's. A strategy declares
what data it needs (`bars`, `history_bars`); an operator decides what
period to ask about. Defaulting to all available history is rejected: over
585,266 instruments it is a wildly different run from anything a caller
likely meant, and a default that expensive should be typed out.

**The request blocks**, matching `POST /strategies`. Measured compute is
~2,600 dispatches for a daily decade against a loop that does 481,000
bars/sec, so the honest expectation is seconds. `api.py` already documents
why that is acceptable for one operator and wrong for a queue; nothing
here changes that trade-off, and a job queue is not built for a wait that
does not exist.

This sub-project **returns** the result and does not store it. Persistence
is 3c.

---

## 3. Explicitly out of scope

Named so they read as sequencing, not oversight:

- **Persisting runs and curves, and any read API for them.** That is 3c.
  3b returns a result to its caller and forgets it.
- **Metrics of any kind** — returns, Sharpe, drawdown, exposure, turnover,
  post-tax P&L lens (§8). 3d consumes the curve this defines; it does not
  need the curve's producer to compute anything.
- **Report UI** (3e), **walk-forward and the robustness suite** (3f).
- **Chunked or streamed bar delivery, and multi-year intraday runs.** The
  scope decision in §1 is what makes the payload architecture survivable;
  when intraday backtests are wanted, that decision is what gets revisited,
  and `loop.py`'s docstring already anticipates being "fed from Timescale
  instead of from a payload" — which is only available outside the sandbox,
  where untrusted strategy code must not run.
- **Downsampling the curve.** Unnecessary at ~2,600 points; needed the day
  intraday runs land.
- **Total-return (dividend-adjusted) series.** Still 3a's deferral, still
  unclaimed: 38,742 dividend records exist and remain unused.
- **`BarRecord.end_ts`.** It is defined on the dataclass and read by
  nothing in `src/`. D3b-1 fixes `close_ts`, which *is* read; `end_ts`
  keeps its arithmetic definition and stays unused. It must not be given a
  consumer without revisiting the same session-versus-calendar problem.

---

## 4. Testing

The pattern 3a established holds: prove the defect exists before fixing
it, and verify each fix by mutation rather than by a green suite.

- **D3b-1 is the one to test hardest, because it is a money path.** A
  daily-bar sell on two consecutive sessions must produce two distinct DP
  scrip-day keys; under today's clock it does not. The test asserts the
  charge outcome, not the timestamp, so it fails for the reason that
  matters. Mutation check: restore the derived `close_ts` and confirm the
  test goes red.
- **The curve is the breaker's number.** A run whose breaker latches must
  show the latching equity as a point on its curve. Asserting equality
  between the two is what makes D3b-2's "cannot disagree" claim
  enforceable rather than aspirational.
- **D3b-1a is pinned document-to-runtime.** The test reads
  `STRATEGY_CONTRACT.md` and asserts what it claims about `1d` timestamps
  matches what a daily run actually does, so the two cannot drift apart
  again silently.
- **Warm-up is observable from inside a strategy.** A test strategy that
  records `len(ctx.data.bars(id, 200))` on its first `on_bar` asserts 200,
  and asserts the first dispatched timestamp equals `start` — the two
  halves of D3b-3, one of which would silently pass if the other broke.
- **The short-history case:** fewer available sessions than
  `history_bars` reports the shortfall rather than running quietly.
- **The sizing gate refuses without fetching.** Asserted by counting
  queries or by timing against a window that would take measurable time to
  materialise — a gate that refuses *after* fetching passes a naive
  assertion on the finding code alone.
- **Determinism survives.** The existing two-pass comparison must still
  hold with a curve in the outcome: equal runs produce equal curves, so
  the curve must not carry wall-clock or iteration-order artefacts.
- Container tests stay behind the `sandbox` marker.
