# Running strategies forward against live prices — Phase 2, the live runtime

**Status:** design, approved 2026-09-04.
**Plan reference:** §166 (sandbox + supervisor), §254 (Phase 2: "forward paper-running
of strategies with monitoring dashboard").
**Depends on:** the backtest runtime (`run_loop`), the paper engine, the order API.

---

## 1. What this is

A registered strategy runs forward against live bars, places real paper
orders, and is watchable while it does. The plan calls this Phase 2's
"forward paper-running of strategies with monitoring dashboard"; the
backtester built it backwards, so most of the machinery already exists and
is pointed at history instead of the present.

**Most of the live path is already built.** `paper.engine` fills orders
from live ticks with the real Indian cost model, enforces the circuit
breaker, and emits Telegram alerts through the outbox. What is missing is
the piece that turns a strategy's `ctx.order()` into a row in `orders`.

## 2. The transport, and a documented divergence from §166

§166 specifies: *"Strategy I/O happens exclusively over a Unix socket RPC
to the runtime supervisor."* **That is not implementable on this machine,
and the alternative that is faithful to its shape is worse than the one
that is not.** Measured, not assumed:

| approach | reaches supervisor | reaches internet |
|---|---|---|
| `--network none` (today) | no | no |
| bind-mounted Unix socket | **no — `OSError 95`, unsupported across the macOS/Lima share** | no |
| default bridge + `host.docker.internal` | yes | **yes** |
| `--internal` bridge | only other containers on it | no |

The socket fails because Unix domain sockets do not work across the
macOS-to-VM filesystem share. The internal bridge blocks the internet but
cannot reach the host — so the supervisor would have to run *inside*
`colima-sandbox`, which holds no database. The data plane lives in the
`colima` VM, and that separation is deliberate: `STATUS.md` records it as
mattering more than gVisor does, because it removes 51M bars and the paper
ledger from a container escape's blast radius.

So §166's transport forces a choice between granting strategies internet
access and dissolving the VM split. Both are worse than the status quo.

**Decision: framed JSON over stdin/stdout.** The container keeps
`--network none` and no mounts, so all fifteen containment tests continue
to hold unchanged and the supervisor stays on the host beside the database.
The supervisor's *role* is exactly what §166 describes — it mediates every
byte, enforces the point-in-time data rule, and rate-limits orders. Only
the pipe differs, and it is the pipe that already carries the strategy's
source and its bar payload.

Recorded here rather than left implicit: the plan assumed a Linux host
where a Unix socket costs nothing, and this is a macOS/Lima machine where
it is unavailable.

## 3. Settled decisions

### D4-1. One dispatcher, driven at two speeds

The plan's claim that *"backtest and forward-paper behavior are bit-identical
by construction"* is only true if both run the same code. `run_loop` is a
`for` loop over a complete bar set; live needs the same body once per bar
close.

**`run_loop`'s per-bar body is extracted into `step`**, and both callers
drive it: the backtest in a tight loop, the supervisor once per closed bar.
Identical by sharing, not by discipline. A second loop written for live
would drift, and the drift would be invisible until a strategy behaved
differently in production than in its backtest — the worst failure this
platform could have.

The refactor is provable: the existing runtime suite passes unchanged, and
`test_two_identical_runs_produce_identical_order_snapshots` still holds.

### D4-2. A live strategy's orders are ordinary paper orders

`ctx.order()` becomes a row in `orders`, placed through the same path the
UI uses, against a real `portfolios` row. Nothing downstream is new: the
paper engine fills it against live ticks with the real cost model, the
circuit breaker watches the portfolio, the outbox alerts.

This is the whole reason the live path is small. It also means a live
strategy's trades appear in the ordinary blotter beside manual ones,
distinguishable by the `live_run_id` the order carries.

### D4-3. Bars, not ticks

The strategy contract's dispatch handler is `on_bar`, and the backtester
feeds bars. A live path that fed ticks would need a contract change and
would not be comparable to any backtest. The supervisor subscribes to the
aggregator's closed 1-minute bars (`bars:*`) and dispatches one `step` per
closed bar — the same unit the backtest dispatches.

Crypto bars arrive 24/7, so a live run is demonstrable outside NSE hours.

### D4-4. The supervisor owns the process, and the process is disposable

One container per running strategy, launched by the supervisor, fed bars on
stdin, emitting order intents on stdout. If it dies, the run is marked
`CRASHED` and stops; it is not silently restarted, because a strategy whose
in-memory state vanished mid-session is not the same strategy and its
subsequent orders would not follow from what it saw.

### D4-5. Rate limits are the supervisor's, not the strategy's

§166 puts order-rate limiting in the supervisor. A runaway strategy
emitting an order per bar on 25 instruments is a plausible bug, and the
paper engine would faithfully fill all of them. The supervisor caps orders
per run per minute and stops the run when the cap is breached, recording
why — the same posture the circuit breaker takes toward losses.

## 4. Out of scope for this increment

- **The monitoring UI.** It reads what this writes; designed once there is
  something to read.
- **Restart/resume across supervisor death.**
- **Tick-level dispatch** and any contract change.
- **Multiple portfolios per strategy**, or one portfolio shared by several
  strategies — D6's one-strategy-one-portfolio rule stands.

## 5. Testing

- **The step refactor changes nothing**: the whole runtime suite passes
  untouched, including the determinism comparison.
- **One bar in, one order intent out**, over the real framed protocol,
  against a real container.
- **A crashed strategy stops its run** and records `CRASHED` rather than
  being restarted.
- **The rate limiter stops a runaway** and says so.
- **A live order is an ordinary order**: it appears in `orders` with its
  `live_run_id`, and the paper engine fills it with charges identical to a
  manual order of the same shape.
