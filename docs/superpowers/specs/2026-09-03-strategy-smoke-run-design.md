# The smoke run — Agent Contract §9 stage 2

**Status:** design, approved 2026-09-03.
**Phase:** 2 (Agent Contract + strategy runtime).
**Precedes:** D4, the worked examples, which cannot be published until they
have been *executed*.

---

## 1. What this is, and why it is not a small addition

Stage 2 of the upload pipeline is described in the contract as "five
simulated days in a throwaway sandbox. Must not crash and must parse orders
correctly." That sentence understates it. Stages 1 and 3 — static
validation and registration — inspect strategy code. Stage 2 is the first
thing that **runs** it, which means it is the first appearance of the
strategy runtime itself: an event loop, a live `Context`, a fill path, and
a host↔container data boundary. None of those exist yet.

Everything downstream inherits this shape. Phase 3's backtester is the same
loop with a different data source; the forward paper runner is the same loop
on a real clock. So the design goal is not "get a smoke run working" but
**write the runtime once, and let the smoke run be its first caller.**

---

## 2. Settled decisions

Eight decisions, each with the reasoning that chose it.

### D-S1. The Context runs inside the sandbox; data is shipped in

The host resolves the manifest's universe, fetches the bars, and packs them
into the stdin payload alongside the source. A driver inside the container
runs the event loop against an in-memory `Context`.

**Rejected: RPC callback to the host.** It is more faithful — the strategy
would hit the platform's live code — but it costs a line protocol, replaces
the sandbox's one-shot `subprocess.run(..., capture_output=True)` with
streaming stdio, needs a deadline on every message, and lets a hung or
hostile strategy stall the host's loop. The sandbox's current shape is one
of its better properties: a single `docker run` with a host-enforced
timeout, where a strategy that misbehaves is a process the host kills. RPC
would trade that for fidelity we can get another way (D-S3).

**Rejected: skip the container for now.** Faster to the worked examples,
but it defers the boundary question rather than answering it, and the
answer changes the runtime's shape.

### D-S2. `platform_sdk.py` is reused unchanged; no second SDK

The stub's raising classes (`Context`, `DataAccess`, `PortfolioView`) are
precisely the ones a strategy **only ever receives** and never constructs.
The classes a strategy does instantiate — `StrategyManifest`, `Param`,
`InstrumentRef`, `Query`, `DataRequest`, and the `Strategy` base — are all
real working code today, with real validation in them.

Therefore the runtime needs no substitute module, no `sys.modules`
injection, and no de-stubbed copy. `trading.runtime.context.LiveContext`
**subclasses** `platform_sdk.Context` and overrides the raising methods. A
strategy imports the same `platform_sdk` it type-checked against offline,
and the offline stub cannot drift from the live runtime on shape, because
the live runtime is a subclass of it. Any method added to the stub and not
implemented live raises `NotOnThisPlatform` at exactly the call site — a
loud, correct failure rather than a silent divergence.

### D-S3. The real fill and cost code runs in the container

`trading.paper.fills.decide_fill`, `trading.paper.charges.compute_charges`,
and `trading.paper.breaker.{compute_equity,evaluate_breach}` are already
pure — no I/O, no clock, no DB — with their DB-backed loaders factored out
separately. That was done so Phase 3 could reuse them; it pays off here.

So `trading/paper/{fills,charges,models,enums,breaker}.py` is copied into
the sandbox image, and the host packs the `ChargeSchedule` rows into the
payload. The smoke run therefore produces **real fills at the real cost
model**: `on_order_update` fires, cash moves by actual NSE charges,
positions build, and the breaker evaluates.

This is what buys back the fidelity D-S1 gave up. The strategy does not
reach the platform over a socket; the platform's own arithmetic is standing
next to it in the container.

**Rejected: fills without charges.** A strategy tuned against a costless
smoke run looks profitable in exactly the way this platform's headline
feature exists to disprove.

**Rejected: accept and record orders without filling.** `on_order_update`
would never fire, so half the `Strategy` interface would go untested by the
gate whose entire job is catching generated code that does not work.

**Rejected: a simplified fill rule inside the driver.** That is a second
fill implementation, which is the divergence `decide_fill` was made pure to
prevent.

### D-S4. The data window is auto-selected and recorded

The host picks the most recent five sessions for which **every** resolved
instrument has bars, and writes the exact window into the result: start,
end, session count, and per-instrument bar counts and gap counts.

"Session" is asset-class specific and must not be left to inference: for an
exchange-traded instrument it is a trading day from the calendar the
platform already maintains, so weekends and holidays are skipped; for
crypto it is a 24-hour UTC day, since the market never closes. A universe
mixing the two takes the intersection of the days both have.

Always relevant to what the strategy actually trades, and it works for an
instrument listed last month. Reproducible after the fact because the
window is recorded on the smoke-run row.

The honest cost, stated so nobody misreads it: **a pass is a pass against a
stated window, not a permanent certificate.** The same strategy re-run next
week meets different bars.

**Rejected: a pinned historical window.** Stable and comparable across
strategies, but an instrument listed after the window has no data, and the
window ages away from current market behaviour until someone remembers to
move it.

**Rejected: synthetic bars.** Deterministic and always available, but real
markets produce gaps, halts, zero-volume sessions and limit moves, and
those are exactly the shapes that break generated strategies. A pass would
prove less than it appears to.

### D-S5. Zero orders is a warning, not a failure — but a recorded one

A strategy that runs five clean sessions and never orders **passes with
warnings**. Five arbitrary days genuinely may not trigger a selective
strategy's signal, and failing a crash-guard that only trades on 3-sigma
moves would be wrong.

But it passes loudly. The agent-facing report leads with `NO_ORDERS`,
naming how many times `on_bar` ran without an order and what usually causes
it. And the counts — orders placed, fills, rejections, breaker state — are
stored on the smoke-run row, so a strategy whose order path was never
exercised stays distinguishable from one that was proven, months later.

**Rejected: fail on zero orders.** The most common failure in generated
code is a signal that never fires, so the appeal is obvious — but it
refuses legitimate selective strategies, and a gate that is wrong in an
obvious way gets routed around.

**Rejected: let the manifest declare `expected_min_orders`.** An agent that
can set its own bar will set it to zero.

### D-S6. Every strategy is run twice and the order sequences compared

§2 of the contract makes determinism a rule, and until now nothing enforced
it. Static validation catches a literal `datetime.now()`, which misses set
iteration order, unseeded `random`, and anything dependent on dict hashing.

Two runs of an identical payload, compared order-for-order, catch all of
them. A difference is a `NONDETERMINISTIC` finding and a **hard fail**: a
strategy whose orders are not reproducible cannot be backtested, so every
number Phase 3 would report about it would be meaningless.

Cost is 2× wall clock, accepted.

### D-S7. The universe is not capped

If a manifest's `Query` resolves to 47 instruments, all 47 are fed.

This was chosen over a 10-instrument cap deliberately: a capped smoke run
tests a strategy at a breadth it will never actually run at, and
cross-instrument logic — the multi-asset rebalancer among the D4 examples
is exactly this — would go untested by the gate meant to test it.

**The cost is real and is not being hidden.** A 47-instrument equity
universe is roughly 88,000 bars. Two mitigations, and one residual risk:

- Bars are encoded **columnar** (`{"ts": [...], "o": [...], ...}`) rather
  than as per-bar objects, and the whole payload is **gzipped**, so stdin
  carries bytes rather than text. Columnar numeric JSON compresses about
  10:1, taking a ~19 MB payload to roughly 2 MB.
- The `SMOKE_TIMEOUT` finding reports **bars per second** alongside
  progress, so an agent can tell "my per-bar work is too slow" from "my
  universe is wide."

Residual risk, accepted: a legitimately wide universe can exhaust the 120s
limit and fail. If that turns out to bite in practice, the fix is a
per-instrument time budget, not a silent cap.

### D-S8. Two timeouts, for two different jobs

`configure` mode keeps the existing 30s. `smoke` mode gets 120s. A timeout
is a hard fail with `SMOKE_TIMEOUT`, which is useful signal rather than
mere enforcement: the same per-bar cost will run against years of bars in
Phase 3, where it is ~50× worse.

---

## 3. Architecture

### 3.1 Module layout

A new `src/trading/runtime/` package. It lives outside `agent_contract/`
because it has two callers and neither owns it: the smoke run today, the
backtester and forward runner in Phase 3.

```
src/trading/runtime/
  provider.py   BarProvider protocol; InMemoryBars
  context.py    LiveContext(platform_sdk.Context), LiveDataAccess, LivePortfolioView
  loop.py       EventLoop — pure: bars in, orders/fills/logs out
  payload.py    SmokePayload: encode (host) / decode (container)
  outcome.py    RunOutcome — what one loop execution produced

src/trading/agent_contract/
  smoke.py      host orchestration + verdict
```

`smoke.py` is the only module here that touches the database or Docker.
Everything under `runtime/` is pure and importable in the container.

### 3.2 What ships in the image

```
/opt/runner.py            driver: decode payload, build Context, run loop, emit result
/opt/platform_sdk.py      unchanged (D-S2)
/opt/trading/runtime/     provider, context, loop, payload, outcome
/opt/trading/paper/       fills, charges, models, enums, breaker
```

`trading/paper/models.py` imports pydantic, so pydantic joins numpy and
pandas in the image, pinned like they are.

**Drift is pinned by test, not by hope.** The image copies are the same
files, and `tests/agent_contract/test_image_contents.py` asserts the
Dockerfile's `COPY` set matches the modules the runner imports, so adding a
`trading.paper` import to the runtime without adding it to the image fails
the suite rather than failing at run time in a container. This is the same
trick `test_contract_bundle.py` already plays on `schema.json`.

### 3.3 The payload envelope

The runner's stdin contract changes from raw source text to a gzipped JSON
envelope:

```json
{
  "mode": "configure" | "smoke",
  "contract_version": "0.1",
  "source": "...",
  "window": {"start": "...", "end": "...", "sessions": 5},
  "bars": {"1401": {"ts": [...], "o": [...], "h": [...], "l": [...],
                    "c": [...], "v": [...], "interval_sec": 60}},
  "charge_schedules": [...],
  "config": {"starting_cash": "1000000.00", "slippage_bps": "5"}
}
```

`mode: "configure"` preserves today's cheap manifest-only run, so
`run_strategy_in_sandbox` keeps working for the fifteen existing isolation
tests and for a fast pre-check.

The envelope is built inside `run_strategy_in_sandbox`, not by its callers,
so the fifteen existing isolation tests keep calling it with a source string
and do not change at all. What must not happen is the **runner** sniffing
the payload shape to decide whether it received raw source or an envelope —
format detection by guessing is the compatibility shim that quietly breaks a
year later. The runner requires an envelope with an explicit `mode`; the
host is what constructs one.

### 3.4 The two-pass sequence, and why it is forced

The host cannot build the payload without the manifest — it needs the
universe to know which instruments to fetch and the interval to know which
bars — and the manifest is whatever `configure()` returns, which only runs
inside the container. That circularity is not incidental; it is what makes
`configure` mode structurally necessary rather than merely a cheap
pre-check:

1. **Pass 1 — `configure` mode, 30s.** Run `configure()` in the sandbox and
   return the manifest. The host validates it against `schema.json` and
   resolves its universe against the instrument table at the window's end
   date. A failure here is `MANIFEST_UNRESOLVABLE`, and no bars are ever
   fetched.
2. **The host** selects the window (D-S4), fetches bars, loads the
   `ChargeSchedule` rows in force across that window, and packs the
   payload. `starting_cash` is `manifest.capital` — the strategy's own
   declared figure, not a platform default.
3. **Passes 2 and 3 — `smoke` mode, 120s each.** The same payload, run
   twice, order sequences compared (D-S6).

So one upload costs three container runs. That is the price of both the
manifest being the strategy's own to declare and determinism being checked
rather than assumed.

### 3.5 Loop semantics

All instruments' bars merge into one timestamp-ordered stream. Ties break
by instrument id, so ordering is total and reproducible — a prerequisite
for D-S6 to mean anything.

Per timestamp:

1. Advance `ctx.now` to the bar's **close** (`ts + interval_sec`). A bar's
   timestamp marks the start of its interval but its values are only
   knowable at the end; setting the clock to the start would hand the
   strategy information from the future, which is the exact bias §4 of the
   contract promises is impossible.
2. Run resting orders through `decide_fill` against this bar.
3. For each state change: compute charges, apply to cash and positions,
   fire `on_order_update`.
4. Fire `on_bar` with **only the instruments that printed** in this
   interval. An instrument that did not trade is absent, not carried
   forward (contract §4).
5. Evaluate the breaker against the manifest's `max_daily_loss` and
   `max_drawdown_pct`.

`ctx.data.bars(...)` serves from the already-consumed prefix of the stream.
Lookahead is therefore prevented by construction — there is no argument a
strategy can pass that reaches unconsumed data, because the unconsumed data
is not in the structure being read.

Order submission runs the contract's §6 rules. A rejection is delivered
through `on_order_update`, never raised, so the smoke run tests that the
strategy survives one.

Every handler call is wrapped: an exception inside `on_bar` ends the run
with `SMOKE_CRASH` carrying the traceback, the bar index, and the simulated
timestamp, so an agent can see *when* it broke, not only that it did.

---

## 4. Verdict and reporting

Findings reuse `validation.Finding` and `ValidationReport`, so an agent
parses one report format across both stages and can branch on stable codes.

| Code | Verdict | Meaning |
|---|---|---|
| `SMOKE_CRASH` | FAIL | A handler raised. Traceback, bar index, sim timestamp. |
| `SMOKE_TIMEOUT` | FAIL | Exceeded 120s. Reports progress and bars/sec. |
| `SMOKE_OOM` | FAIL | Killed at the memory ceiling. |
| `NONDETERMINISTIC` | FAIL | The two runs' order sequences differ; names the first divergence. |
| `NO_DATA` | FAIL | The universe resolved to nothing, or to instruments with no common window. |
| `MANIFEST_UNRESOLVABLE` | FAIL | `configure()` returned a manifest the schema or the instrument table rejects. |
| `NO_ORDERS` | WARN | Five sessions, zero orders. The order path was never exercised. |
| `ALL_ORDERS_REJECTED` | WARN | Every order was refused. Names the most common rejection reason. |
| `BREAKER_TRIPPED` | WARN | The strategy hit its own declared limit inside five days. |

Warnings pass. The report is `PASSED`, `PASSED WITH WARNINGS`, or
`REJECTED`, and — as with stage 1 — **every finding is listed at once**,
never one per round trip.

## 5. Persistence

Migration **0011** adds `strategy_smoke_runs`, keyed to `(name, version)`
with many runs per version allowed:

- the window: start, end, session count, instrument ids, per-instrument bar counts
- the counts: `on_bar` calls, orders placed, fills, rejections, final cash and equity
- the verdict and the findings JSON
- `runtime` and `kernel_isolated`, carried forward from `SandboxResult`

That last pair is not decoration. The sandbox already refuses to let a run
be misread as better-isolated than it was, and a stored smoke run that
dropped the field would reintroduce exactly that ambiguity a month later.

Registration stays separate from smoking. `register_strategy` continues to
run stage 1 only, because the registry is DB-only while the smoke run needs
Docker, and a registry that cannot be written without a container daemon is
a worse registry. The pipeline is composed by the caller:
validate → smoke → register.

## 6. Testing

- **`runtime/loop.py` is pure**, so most behaviour is tested in-process
  without Docker: absent instruments, lookahead refusal, rejection
  delivery, partial fills, breaker trips, charge application.
- **A golden strategy** — SMA crossover on a fixed synthetic series with
  hand-computed expected orders — pins loop semantics against silent
  change.
- **Container tests** (marked, like the existing sandbox suite) run one
  real strategy end to end and assert the round trip: payload in, result
  out, fills present.
- **A deliberately non-deterministic strategy** must be caught by D-S6.
  Written as a test that would fail if the double-run were removed, so the
  check cannot rot into a no-op.
- **The image-contents drift test** described in §3.2.

Non-vacuity: as with the sandbox's isolation tests, each guard is verified
by breaking the thing it guards and watching the test fail.

## 7. What this explicitly does not do

- **No `ctx.intel`.** Phase 2.5. The stub keeps raising.
- **No tick routing.** `data.ticks=True` in a manifest is accepted but the
  smoke run feeds bars only, and the report says so rather than silently
  ignoring it.
- **No `on_expiry`.** F&O settlement needs the expiry calendar wired to the
  loop; out of scope here and noted in the report if a manifest's universe
  contains a derivative.
- **No multi-currency.** D6 already fixed one strategy to one portfolio to
  one currency for V1.
