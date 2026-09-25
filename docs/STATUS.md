# Where this project stands

**Updated:** 2026-09-26, ~00:03 IST. Keep this file current — it is the
first thing to read when picking the work back up.

## Live-stack resilience plan merged (2026-09-25)

Design: `docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md`.
Every bug in its §1 table (silent `listen()` exit, no REST backfill,
in-memory-only delivery cursor, `ConnectionError` killing the supervisor,
blocking `readline()`, no staleness check, `ctx.state` never saved, no
deployment story) is fixed and tested, including now against real
infrastructure, not just fakes. What shipped:

- **Gap backfill** (`spot_backfill.py`): 1-minute bars are pulled from
  Binance REST spot klines on four triggers -- at aggregator startup,
  for the exact minute still open at that same startup once its
  tick-built bar is discarded as incomplete, after 90s of silence on a
  stream, and via a 5-minute sweep re-checking the last 30 minutes (the
  sweep stops one bucket short of `now` so it never races the tick
  path's own flush of the just-closed minute). A tick bar write waits
  (bounded, 30s) for any in-flight backfill of the same instrument, so
  Postgres still receives bars in ts order for that instrument; a
  backfill that still lands behind a newer bar is logged
  (`bar_aggregator.late_fill`) since a live run's cursor has by then
  already moved past it. Known limitation: ticks that arrive *during*
  the outage that closes the bar (both the bar mid-outage and the first
  bar after reconnection) are still built from whatever partial set this
  process actually saw and are never corrected afterward -- a backfilled
  bar for a minute a tick already wrote is skipped
  (`ON CONFLICT DO NOTHING`). The first bar after reconnection is traded
  on live, not just recorded.
- **Per-run delivery cursors** (`live_run_cursors`) replace pub/sub as
  the record of what a live run has seen: a `closed_bars:*` message is
  only a wake-up now, never the thing actually trusted, so a dropped
  message during a reconnect is harmless -- the next poll or sweep
  notices the gap on its own. Postgres is the record (design §2).
- **`ctx.state` persistence**: strategy state (capped at 64 KB) is saved
  and round-tripped through a container relaunch, so a strategy's memory
  survives a supervisor restart instead of starting over as if it were
  its first bar.
- **Catch-up bars and their enforcement**: the supervisor replays missed
  bars per run and flags each one `catchup = true` -- recomputed
  immediately before that bar is sent, not just once per replay pass, so
  a bar that was fresh when queried but went stale before its own turn
  to be dispatched (a long replay is many subprocess round trips) is
  still caught. This flag, checked by the supervisor itself before
  placing the order, is what actually blocks a catch-up bar from
  trading -- the paper API's own staleness guard checks the
  instrument's *latest* known bar, not each order's own reference bar,
  so it does not by itself catch a catch-up order once the feed is live
  again. Every refusal is logged (`live.catchup_refused`: `live_run_id`,
  `instrument_id`, `bar_ts`) and reflected in
  `live_runs.orders_refused`/`last_refusal`. Replay is capped at 24h; a
  skip past that cap is logged as `live.replay_gap` and stored in
  `live_runs.last_gap_note`.
- **Resilient Redis/Postgres reconnection** in every long-running
  process (`resilient_pubsub.py`, `ReconnectingConnection`): a
  disconnect backs off and resubscribes/reconnects instead of silently
  ending the consuming loop or killing the process -- including when
  the reconnect attempt itself fails because Redis or Postgres is still
  down, which used to raise straight through and crash the process; now
  it keeps backing off (capped at 30s) and retrying forever. The bar
  aggregator's own tick-path write connection reconnects the same way
  (it used to hold one connection for its whole life, so a Postgres
  restart broke every write until the process itself restarted) --
  fetched off the event loop, inside `to_thread`, since
  `ReconnectingConnection.get()` sleeps its backoff synchronously and
  can still raise; a fetch failure is logged
  (`bar_aggregator.db_unavailable`) and that one write batch is skipped
  rather than killing the loop, and a batch is only fetched at all when
  there is something in it to write (most ticks close nothing).
  Known limitations, not fixed by any of this:
  - `reconcile()` marks any strategy container exit `CRASHED`, a
    terminal status the supervisor never auto-relaunches -- a sandbox
    VM or Docker hiccup after the Mac sleeps (not a Redis/Postgres
    disconnect, which this fixes) ends the affected run(s), and they
    have to be restarted manually via the API.
  - The live supervisor's own pubsub reconnect (`SyncResilientPubSub`,
    used for the `closed_bars:*` wake-up) is synchronous and blocks its
    single-threaded loop for as long as Redis stays down: no delivery
    timer fires and `stop_run` isn't honoured until Redis is back and
    the reconnect returns. Since Redis runs locally, a Wi-Fi outage
    doesn't trigger this -- a Redis/colima restart does, and recovery
    is still bounded by the same 1s-30s backoff, just not concurrent
    with anything else the supervisor is doing meanwhile.
- **A real `select()`-based reply timeout** and a **stale-price guard**
  close off the blocking-`readline()` and no-staleness-check rows of the
  bug table.
- **Heartbeats and `GET /health`**: six `health:<name>` keys (30s TTL,
  refreshed every 10s) prove each process is alive. A heartbeat only
  proves the process is alive, not that its loop is making progress --
  for bar flow specifically, check that the latest row in
  `bars_intraday` is recent.
- **Deployment**: launchd agents for the whole live stack plus a Docker
  restart policy, in `deploy/` (`install-live-stack.sh`,
  `start-colima.sh`, `provision-sandbox-vm.sh`, and the plists). Every
  plist carries a PATH (installed from the operator's own PATH at
  install time) -- launchd's own default PATH is just
  `/usr/bin:/bin:/usr/sbin:/sbin`, which hid `colima` and `docker` from
  every agent that shells out to them. `provision-sandbox-vm.sh` is
  idempotent -- it merges `daemon.json` rather than overwriting it,
  backs up before writing, and prints "already provisioned" when
  `runsc` is already registered. None of this has been installed or run
  yet; the operator installs it.

**What remains:** the live drill itself. `docs/live-resilience-drill.md`
is the runbook (start the stack, start a run, confirm no gap, kill Wi-Fi
for 10 minutes, kick the supervisor, restart Redis) but it has not yet
been run for real against an actual outage. That is the next thing to
do -- everything above is proven by unit and integration tests (including
one that restarts the real `trading_redis_test` container mid-stream),
not yet by a live rehearsal.

---

## The one thing to do next

**Phase 3.5 task 3 — signed positions and margin** — which is blocked on
one operator action: a **read-only Binance API key**
(`BINANCE_API_KEY` / `BINANCE_API_SECRET` in `.env`, no trading or
withdrawal permission). Maintenance-margin tiers come from a signed
endpoint, and they are deliberately not hardcoded: an invented maintenance
rate liquidates at the wrong price, which is the failure
`MissingChargeSchedule` exists to refuse. `seed_perp_margin_tiers` says so
and writes nothing.

Also still true:

**Watch Monday morning.** Two things run unattended for the first time on
2026-09-07: the chain recorder at 09:10 (launchd), and the NSE half of
live bar delivery once real `bars:*` messages start flowing. Both are
verified by tests and by an out-of-hours smoke run; neither has yet seen a
trading session.

Then: **the news/announcements recorder**, which is on the same
irrecoverable clock as the chain recorder and has never been built at all.
Then **restart/resume across supervisor death**.

**Not a blocker, but wrong:** EOD bhavcopy ingestion last wrote on
2026-08-21. Nothing is scheduled to run it. The chain recorder no longer
depends on it (it anchors on Upstox's index close instead), but every
backtest is running against a database two weeks stale. A supervisor restart relaunches
every `RUNNING` row, which is right for the row and wrong for the strategy:
the container is new, so whatever the strategy held in memory is gone and
its next bar looks like its first. Nothing in `live_runs` records that this
happened, so the run's own history reads as continuous when it is not.

Then: tick-level dispatch, if ever wanted (it would need a contract
change), and the post-tax P&L lens.

---

## Phase 3.5 — crypto perpetuals (tasks 1-5 shipped, 2026-09-05/06)

Design: `docs/superpowers/specs/2026-09-05-crypto-perpetuals-design.md`.
A separate derivative core beside spot, not a flag on it: spot keeps its
`ck_no_negative_position`, its notional cash model and its charge model,
all untouched.

**Task 1 — contracts.** Eight USDT-margined perpetuals seeded as
`PERP / BINANCE_FUTURES / PERP` with fetched filters. `AssetClass.PERP` is
distinct because `load_schedules` and `_BROKER_BY_ASSET_CLASS` key on it —
reusing CRYPTO would silently apply spot's 10 bps to a perpetual fill.
`perp_contract_specs` is dated like `charge_schedules`. Filters vary far
more than anyone would guess: DOGE steps by a whole coin, BTC by 0.001,
min notionals run 5 / 20 / 50.

**Task 2 — data.** 18,895 daily bars from 2019-09-08 and 56,734 funding
settlements. Live marks and closed 1-minute bars by **polling**, because
the futures WebSocket is gated here: it connects, acks a SUBSCRIBE, and
sends nothing, while spot streams instantly and futures REST answers 200.
One `premiumIndex` call covers all 898 contracts.

**Worth not relearning:** BTC's mean funding over seven years is
0.0001059 per settlement — about **11.6% a year a long pays a short**.
That is the number whose omission makes every carry strategy backtest as
free money. And Binance's last kline is the interval still open: taking it
is lookahead arriving through the live feed.

**Task 3 — signed positions.** 85 margin tiers seeded from Binance's
signed endpoint. `perp_positions.quantity` is signed with no
`ck_no_negative_position`: the sign is the direction. A short can be
placed, filled and seen in equity. The gate that had to move was
`_require_sufficient_position` -- for a perpetual the sell IS the
position -- replaced by four checks: leverage declared, quantity on the
contract step, leverage inside the tier ceiling, margin actually free.

**Task 4 — funding.** Settles on the 00/08/16 UTC boundaries, half-open
`(since, until]` so adjacent windows partition the timeline and a process
asleep across two settles both. Its own `FUNDING` ledger type, because a
charge always costs the holder and funding pays one side.

**Task 5 — liquidation.** On the mark, never last traded. Solved from the
definition, so at the liquidation price remaining equity equals the
maintenance requirement exactly. The forced close goes through the
ordinary order and fill path, so it lands in the blotter with a rationale
naming both numbers. Beyond bankruptcy the fill is capped and the gap
recorded -- a real venue's insurance fund would have covered it.

**Worth not relearning:** at 20x, bankruptcy is exactly 5% from entry
(1/leverage) and liquidation sits ~0.4% inside it. That buffer is narrow,
and a test that picks a mark "well past the line" is testing the
bankruptcy cap, not liquidation.

**Open:** grade the liquidation price against Binance's own calculator,
the way Task 12 graded charges against Upstox's. Internal consistency is
necessary, not sufficient.

**Task 6 — contract and runtime.** `StrategyManifest.leverage`, declared
once per strategy, travelling manifest -> payload -> `LiveRun` -> order
body. `None` when absent, never 1: defaulting would make every
pre-existing strategy a perpetual trader.

**Task 7 — perpetuals can be backtested, with carry.** A strategy that
shorts BTC-USDT perpetual on daily bars at 3x passes smoke, registers,
and backtests over 964 sessions. The runtime models perpetual money:
cash moves on realised P&L rather than notional, equity counts
`qty x (mark - entry)`, and funding settles at every eight-hour boundary
a step crosses, each at its own published rate.

**The number that justifies the phase:** the same 2024-2026 short, run
with and without funding, differs by **+5,268.98 on 100,000 -- 5.27% of
the book over 2.6 years.** Without it the backtest understates a short's
return by that much, and overstates a long's by the same.

**Worth not relearning:** three `SmokePayload`s are constructed in
`smoke.py` -- smoke, backtest, and the stress rerun -- and wiring one is
wiring none. The first attempt applied funding in the smoke path only,
so the backtest ran against 2,889 published settlements and used zero of
them, producing a number identical to funding being switched off.

**Liquidation now applies inside a backtest too**, on the adverse price
(the low for a long, the high for a short) so a position liquidated
intra-bar cannot walk out of it because the market came back before the
close. `RunOutcome` carries `funding_paid` per instrument and every
liquidation event, naming the mark and the requirement it fell below.

**What that shows, on real BTC bars, shorting 0.5 from 2024-01-01:**

| declared leverage | outcome | final equity |
|---|---|---|
| 20x | liquidated 2024-01-09 at 47,312 | 98,645.45 |
| none (margin at 1x) | liquidated later, far worse price | 79,952.43 |

Higher leverage *lost less*, because it was closed early and stopped
bleeding while the barely-levered position carried on into a two-year
rally. Counterintuitive, correct, and the sort of thing this platform
exists to be able to show.

**A stale process ate most of an afternoon.** A perpetual backtest kept
returning a number with no liquidation in it, while the same payload run
against the same image by hand liquidated correctly. Image current,
payload complete, loop right -- and `lsof -ti:8000` listed **four**
listeners, with a gateway from hours earlier still answering.
`pkill -f uvicorn` had not matched it, and every "restart" since had
failed to bind and been ignored. Check what owns the port before
doubting the code.

**The report shows both** (0024). Funding lives apart from charges
because it is not one -- inside a cost total it could only read as an
expense, erasing the income half of every carry strategy. The column
negates the stored sign for display: the database stores what the run
*paid*, and a reader of a P&L column expects a gain to be positive.

A liquidation names what was left of the margin and the requirement it
fell below, and says when the fill was capped at the bankruptcy price.
"Liquidated" alone says something happened; the two numbers say why,
which is the question somebody has in March about a position that
vanished in January. The section renders only for runs that have them, so
every equity backtest looks exactly as it did.

**Phase 3.5 is complete.** All seven tasks shipped.

**The lesson of the phase, five times over:** an additive field only
exists where somebody remembers to add it. `perp` missing from the
sandbox copy list, `leverage` missing from `_describe_manifest`, funding
wired into one of three payload sites, and API fields landing on
`BacktestResponse` because `fills_ledger` appears in two models and the
edit matched the first. Every one passed the whole unit suite. The guards
that now catch these are the transitive image check and comparing two
runs numerically rather than trusting that code is called.

---

## The option-chain recorder — running daily from 2026-09-05 (merged)

Fires 09:10 on weekdays via launchd
(`deploy/install-chain-recorder.sh`); the exchange calendar decides
whether to record, so holidays are a quiet exit-0 rather than a second
copy of the holiday list. Records raw frames to `data/recordings` for
NIFTY and BANKNIFTY, 2 expiries, ±20 strikes -- 330 keys, well inside the
feed's cap.

**Why it had never run:** it demanded a literal list of Upstox instrument
keys, and nothing here could produce one. Our options carry symbols,
strikes and expiries; the feed speaks `NSE_FO|50917`. It now resolves its
own universe from Upstox's public instrument dump (no auth, archived like
any other source).

**The ATM anchor is Upstox's index close**, with the nearest futures close
as fallback. Those differed by 493 NIFTY points -- ten strikes -- the day
this was written, because EOD ingestion is stale; a window centred on the
futures close would have covered only ten strikes below the money. A stale
anchor is logged as a warning.

**Two bugs it only found by running.** The loop checked its deadline only
when a frame arrived, so a quiet feed held it open forever -- invisible on
a trading day, a stuck process per day under a scheduler. And a run
starting after 15:30 recorded five minutes of a shut market, which is what
a laptop opened in the evening would do every single night.

**Worth not relearning:** measure the strike step from the data. NIFTY
steps 50 and BANKNIFTY 100 *today*; an assumed constant does not fail
loudly when that changes, it silently records a narrower window. And
launchd, not cron, on a laptop: a missed `StartCalendarInterval` job runs
on wake, so a morning spent asleep is a partial recording rather than
none.

**Verified:** 45 seconds against the live feed on a closed Saturday
captured 70 frames, no gaps, no anomalies, clean exit on deadline; and
launchd fired the agent end-to-end (venv, `.env`, database, calendar,
exit 0).

---

## Live bar delivery — fixed 2026-09-05 (merged)

Two defects that each made live runs unusable.

**NSE strategies could never receive a bar.** The supervisor learns that a
minute closed from `closed_bars:*` and nowhere else, and the aggregator
published that channel only for bars it built from ticks. Upstox-bound
instruments are deliberately *excluded* from tick aggregation, so their
already-complete I1 bars went to `bars_intraday` and were announced to
nobody — an NSE run could sit `RUNNING` all session on zero bars. Both
kinds are announced now; `source` carries the distinction the separate
channel used to.

**A repeated bar killed a run that had been trading for hours.** Run 1
died at 01:32 IST on a bar it had already seen. The root cause is in
`BarAggregator`: `flush_stale` deletes the bucket it closes, so a late
tick found nothing open, started a fresh bar for a minute already
published, and had it closed a second time. A host that sleeps and wakes
with a backlog does this routinely — which is why a 7-minute watch of the
live stream found 169 messages, 169 distinct, 0 duplicates and proved
nothing. `_closed_through` remembers what has closed; dropped late ticks
are counted and logged.

Fixed at the runtime too, since transport is not the only way a bar can
repeat. `InMemoryBars.append` now has three cases where it had two: an
older bar is still refused, the same minute with *different* prices is
refused as a feed disagreeing with itself, and an identical bar returns
`None` — already known. The runner skips dispatch but still writes an
orders frame, because the supervisor reads exactly one per bar it feeds.

**Worth not relearning:** a "pure, no I/O" class that drops data needs a
counter, or the drop is invisible. And `sandbox/build.sh` must be re-run
after touching `runner.py` or anything under `src/trading/runtime/` — the
image carries its own copy.

---

## The live monitoring page — shipped 2026-09-04 (merged)

`/live` lists the runs; `/live/{id}` is one run — equity curve, P&L since
the run began, positions marked to the last bar, and each fill with the
rationale the strategy gave for it. Both poll every five seconds, the
supervisor's own reconcile cadence. A strategy page grows a portfolio
picker to start one.

**The curve is the run's, not the portfolio's.** It starts at `started_at`
and samples ~400 points from `portfolio_equity_snapshots` — the table the
circuit breaker reads, so the chart and the limit that would halt the run
cannot tell different stories.

**Refused orders are counted, and the reason kept** (`0018`). Found by
running the crypto probe against the INR portfolio: the currency gate
refused every order with a perfectly clear sentence, and the page showed
`0 orders` — indistinguishable from a strategy that had decided to sit
still. The gateway's own sentence is stored, not a code, because a code
would have to be translated back into that sentence somewhere else.

**Worth not relearning:** `formatMoney` in `web/lib/backtests.ts` drops the
sign on purpose — prose carries it ("lost 80,541 to charges"). A table
column has no prose, so a 9.92 loss rendered green as `9.92`. Signed
columns use `formatSignedMoney`. Equally: slicing an ISO string to show a
time throws away the offset that gave the digits meaning — a fill at 21:19
IST was displaying as 15:49.

---

## The live strategy runtime — working 2026-09-04

**A strategy ran forward against live prices and traded.** Verified outside
NSE hours on crypto: strategy 23 in a gVisor container took one closed
BTC-USDT bar, placed order 31 tagged `live_run_id=1`, and `paper.engine`
filled it a second later at 79,549.04 with 0.80 USDT of charges — the same
path, cost model and blotter a human order takes.

Run it with:

```bash
uv run python -m trading.live.supervisor
curl -X POST localhost:8000/strategies/{id}/live -d '{"portfolio_id":9}'
curl -X POST localhost:8000/live/{run_id}/stop
```

### How it works, and why not the way §166 says

**One dispatcher.** `run_loop`'s body is a `step` the supervisor drives once
per closed bar. §166 claims backtest and forward behaviour are
"bit-identical by construction"; `open_session` is that construction. A
second loop would drift invisibly until a strategy behaved differently in
production than in its backtest.

**Framed JSON over stdin/stdout, not a Unix socket.** Measured: a
bind-mounted socket is unreachable from the container (`OSError 95` across
the macOS/Lima share), the default bridge grants full internet, and the only
config that blocks the internet cannot reach the host — putting the
supervisor inside `colima-sandbox`, which holds no database. Pipes keep
`--network none` and all fifteen containment tests. Five minutes of spiking
saved a week.

**Orders go over HTTP.** The gateway owns the currency gate, market hours,
idempotency and the `orders:control` publish. Reaching past it would mean a
strategy's orders were validated differently from a human's.

**The aggregator now announces closed bars** on `closed_bars:*`. Until now
only it knew a bar had closed — the row went to the database and was
published to nobody, so crypto bars were invisible downstream. Separate from
`bars:*` (Upstox's raw I1 feed) on purpose: a subscriber must be able to
tell a bar that was *received* from one that was *computed*.

**A crashed strategy is not restarted.** One whose in-memory state vanished
mid-session is not the same strategy, and its next orders would not follow
from what it saw. Rate limiting lives in the supervisor per §166 and stops
the run rather than dropping orders — the breaker's posture toward losses.

**Watch out:** the payload is now length-prefixed, so the sandbox image and
the host must be deployed together. Rebuilding the image while an older
gateway was running broke uploads until it was restarted.

---

## Phase 3f — the robustness suite, shipped 2026-09-04 (merged)

§228's two checks, automatic on every backtest. Spec at
`docs/superpowers/specs/2026-09-04-robustness-design.md`.

**The rule this sub-project made explicit, and which 3c and 3d had already
been following:** anything that required running the world is **stored**;
anything that is arithmetic over what was stored is **computed**.

- The **2x cost-and-slippage rerun** is an observation — doubling slippage
  changes which fills happen — so it is executed and stored in
  `backtest_runs.stress` (migration `0014`). It needs **no runtime
  change**: the `ChargeSchedule`s are scaled host-side and the container
  runs unmodified code against a harsher world. `rate` and `cap` scale
  together, because a capped charge whose rate doubled alone would sit at
  its cap and exempt exactly the charges that dominate a large order.
- The **Monte Carlo reshuffle** is a pure function of the fill ledger and is
  computed on read, so improving it applies retroactively to every stored
  run. Seeded, 1,000 iterations.
- §6's overfitting guardrail is now a `COUNT`: `identical_run_count` over
  `(strategy_id, requested_start, requested_end)`.

**What it found on the real churn run (backtest 5):**

```
2x stress:  919,559.14 -> 843,788.14     doubling costs took another 75,771
reshuffle:  terminal equity p5 = p50 = p95 = 919,561.05   (identical, by design)
            max drawdown   p5 -10.62%   p50 -9.06%   p95 -8.15%
            actual         -8.24%
```

**The realised drawdown was lucky.** −8.24% sits nearer the *best* 5% than
the median, with a bad tail at −10.62% — the base report understates this
strategy's risk by roughly a fifth, purely through the order the trades
arrived in. That is the claim §228 wants visible and a single path cannot
make.

Terminal equity being identical across all 1,000 orderings is the built-in
correctness check: addition is commutative, so if those percentiles ever
diverged the accumulation would be wrong.

**Worth not relearning:** mutation caught a **vacuous determinism test** —
the second vacuous guard in one day. With six P&L values there are only 720
orderings and 1,000 samples saturates them, so the percentiles converged
identically whatever the seed and the test passed with seeding removed.
Widened to thirty values, where sampling actually matters. Same shape as
3c's `float` money guard: a property that holds anyway, mistaken for one
the code enforces.

---

## The fill ledger — shipped 2026-09-04 (merged)

`RunOutcome` now carries one record per fill with all nine charge
components itemised, migration `0013` stores them in `backtest_fills`,
`trading.metrics.trades` computes round trips and cost drag, and the
report has a **What it cost** panel.

`ChargeBreakdown`'s docstring had said why for months — "the cost-drag
report needs the breakdown and it cannot be reconstructed from a lump sum
afterwards" — while `run_loop` computed one per fill and kept only
`.total`.

**A trade is a FIFO round trip**, stated because win rate, profit factor
and expectancy all depend on it. A fill's charges split **proportionally
by quantity** when matched in parts. A position still open at the end is
counted neither way.

**`backtest_fills` uses an `ordinal`, not `(run_id, ts)`** — unlike the
equity curve. Two fills can genuinely share a bar, so the composite key
that is right for the curve would reject correct data here.

**Demonstrating it needed a strategy that trades.** Buy-and-hold pays two
sets of charges over six years and shows no drag; `phase3f-five-session-churn`
(strategy 18, run 3) round-trips RELIANCE every five sessions:

```
659 fills · 329 trades · win rate 43.16% · profit factor 0.72
gross P&L   -12,313.50
charges      68,227.36     <- 5.5x the gross loss
net P&L     -80,540.86
```

That is §8's "most sobering chart" made concrete, and it is unreachable
from a buy-and-hold run. Remove with
`DELETE FROM strategies WHERE name LIKE 'phase3f-%'`.

**Two things worth not relearning:**

1. **The real run found a hole no fixture would have.** `charges / gross`
   over a *negative* gross gives -5.54, and "costs took -554% of the gross
   result" is not a sentence — the guard only covered `gross == 0`. Drag is
   now undefined unless the gross result is positive, and the UI says the
   true thing instead. A hand-written fixture would have used a positive
   gross and never exposed it.
2. **Nothing leaves as a bare `str(Decimal)`.** Third instance in one day —
   the equity curve, the metric ratios, now trade money. `str(Decimal)`
   reports whatever precision the arithmetic produced, which is an
   implementation detail rather than data. **Money 4 dp, ratios and
   quantities 8 dp**, and the rule is now written into
   `trading/metrics/trades.py` rather than rediscovered a fourth time.

---

## Phase 3e — shipped 2026-09-04 (merged)

The backtest report, and a form to produce one. Spec at
`docs/superpowers/specs/2026-09-04-report-ui-design.md`.

- `/strategies/[id]` — the strategy, a **Run backtest** form, and its runs
  newest-first, reading the curve-free list route.
- `/backtests/[id]` — one report: the hurdle chart, the stat grid, the
  underwater chart, the monthly heatmap and rolling Sharpe.

**The report leads with the hurdle, not the return.** The equity curve is
drawn against a risk-free growth line from the same capital, and the page
says in words whether the strategy cleared it. For the real run it reads
*"This strategy returned less than a government bond over the same
period"* — +5.59% over 6.6 years is 0.82% a year against 6.50%.

No new dependencies: `lightweight-charts` already drives the price charts,
and the heatmap is a CSS grid using the `color-mix` technique
`globals.css` already uses for flash animations.

Two supporting changes: `GET /strategies/{strategy_id}` (same projection as
the list row, so the two cannot show different fields), and
**`CORS_ALLOW_ORIGINS`** is now configurable — the origin was hardcoded to
`:3000`, so no worktree could verify its own branch on a second port.

**Worth not relearning:** the sentences live in `lib/backtests.ts` and are
tested against their exact text. The linter caught that `describeHurdle`
was computed and the same wording then re-typed inline in JSX — the page
and its passing test would have drifted apart silently. That is the exact
failure the pure-lib pattern exists to prevent, and only an
unused-variable warning surfaced it.

Verified in a browser against the real stored run with zero console errors,
including the refusal path: a window past 2026-08-21 shows
`BACKTEST_WINDOW_UNCOVERED` and adds no run row.

---

## Phase 3d — shipped 2026-09-04 (merged)

Metrics over the stored curve, computed on read. Spec at
`docs/superpowers/specs/2026-09-04-backtest-metrics-design.md`.

`trading.metrics.curve` is a pure stdlib module: total return, CAGR,
volatility, Sharpe, Sortino, Calmar, max drawdown **depth and duration**,
VaR(95), worst period, drawdown curve, monthly returns, rolling 6-month
Sharpe. `GET /backtests/{id}` folds them in; the list route is unchanged.

**Scope was set by what the data supports, not by the plan's wish list.**
§178's trade metrics — win rate, profit factor, expectancy, turnover — and
the **cost-drag report** §8 calls the most sobering chart for a retail
options trader all need a per-fill ledger `run_loop` never emits:
`OrderSnapshot` stops at `status` and `submitted_at`, and `fills` is a bare
count. Alpha/beta need a benchmark series; `instruments` holds **zero index
rows**. Both are blocked on other work, not on metrics. **A per-fill ledger
in `RunOutcome` is the highest-value next increment after 3e** — it
unblocks the trade metrics, the cost-drag report and the post-tax lens in
one change.

**Three things worth not relearning:**

1. **`rf = 0` is a lie on this platform, and the numbers prove it.** The
   real 1,647-session RELIANCE buy-and-hold returned +5.59% over 6.6 years
   — a CAGR of 0.82%. At `rf = 0` its Sharpe is **+0.30** and it reads as a
   positive strategy; at the default 6.5% it is **−1.95** and correctly
   reads as worse than a government bond. Same curve. The rate is echoed in
   every response so nobody has to guess which one they are looking at.
2. **No float anywhere, including the statistics.** `Decimal.sqrt()` exists
   and `cagr` compounds through `ln`/`exp` because `Decimal.__pow__`
   refuses a non-integer exponent. Returns are money-derived; a carve-out
   for "statistics" is a boundary held by attention rather than by rule.
3. **Ratios cross the wire at a fixed 8 dp.** Full `Decimal` precision
   leaked the 28-digit context onto the wire, where it is unstable —
   reordering two mathematically identical operations shifts the last
   digits and every client sees a change that did not happen.

An undefined metric is `None`, never `0`: Sharpe on a flat curve, CAGR over
a zero-day window, a drawdown that never happened. And a drawdown still
open at the last point reports `recovered: false` rather than pretending it
closed — the real run's 526-session drawdown from 2024-07-08 never
recovered, and says so.

---

## Phase 3c — shipped 2026-09-04 (merged, `77a3e2e`)

Backtest runs and their equity curves are persisted and readable.
Spec at `docs/superpowers/specs/2026-09-04-backtest-persistence-design.md`,
plan at `docs/superpowers/plans/2026-09-04-backtest-persistence.md`.

**Migration `0012`** adds `backtest_runs` and `backtest_equity_points`.
The curve is a **table**, not a jsonb column: money belongs in
`numeric(18,4)` like every other money column here, and a curve in jsonb
would be a curve of strings — letting a transport constraint (JSON numbers
are IEEE 754 doubles) reach into storage. `(backtest_run_id, ts)` is the
primary key rather than a surrogate id, so a doubled point fails loudly
instead of reaching 3d as a wrong Sharpe.

Only runs that reached the container are stored — **crashes included, with
their partial curve**, because that artifact says where a run died.
Pre-flight refusals are returned and never stored.

Two read routes: `GET /strategies/{id}/backtests` (summaries, **no**
curves) and `GET /backtests/{run_id}` (one run **with** its curve).

**Two things worth not relearning:**

1. **A money guard was vacuous until mutation caught it.** Replacing
   `Decimal(str(raw))` with `float(raw)` left every persistence test green:
   psycopg binds the float, Postgres casts to `numeric(18,4)`, and around
   10^6 with four decimals a float64 is exact enough that the round trip is
   indistinguishable. Those tests were checking the *column's* behaviour,
   not the code's. The guard now asserts a value at the column's limit
   (`12345678901234.5678`, which float renders as `12345678901234.568`).
2. **`str(Decimal)` preserves scale, so the same run had two string
   forms.** A run reported `"1000000"` and read back `"1000000.0000"` —
   numerically identical, but a client caching or diffing them sees changes
   that never happened. Fixed at the source: `run_loop` now emits the 4 dp
   scale contract §5 declares, for curve points and `final_cash`/
   `final_equity` alike. Quantizing only the curve left `RunOutcome`
   internally inconsistent and 3b's own test caught it.

Verified live: `strategy_id=17` backtested 2020-01-01 → 2026-08-21 stored
as run 1 and run 2, 1,647 points each, and the POST response now compares
**byte-for-byte** with `GET /backtests/{id}`.

---

### Task 12 is done except for three things nobody but the operator can do
### Task 12 is done except for three things nobody but the operator can do
### Task 12 is done except for three things nobody but the operator can do

Executed 2026-09-04 against an open NSE session. **8 of 10 steps pass.** Full
evidence in `docs/paper-trading-live-verification.md`.

- **Step 3 — the charge comparison.** Still the only external grading the cost
  model gets, and still ungraded. Figures are recorded and waiting; three lines
  disagree with standard NSE rates and should be checked first:
  **IPFT is ₹0.0000 on every fill** (₹10/crore should give ₹0.1328 on a
  ₹132,766 turnover — the rate behaves like `1e-9`, 1000× low); **GST excludes
  the SEBI turnover fee from its base** (4.33 vs 4.36); and **exchange txn
  implies 0.00307%** where NSE's current cash rate may be 0.00297%. These
  figures are what unblocks `tests/paper/test_charges_golden.py`'s
  `REPLACE ME` guard.
- **Step 5 — the session sweep.** Order **30** (limit BUY 10 RELIANCE @ ₹1200)
  is resting `OPEN` and must go `EXPIRED` at 15:30 IST. The engine was
  restarted 10:54 on the fixed sweep, so this now verifies corrected code.
- **Step 6 — the circuit breaker.** Blocked: `TELEGRAM_BOT_TOKEN` and
  `TELEGRAM_CHAT_ID` are unset, so `run_alert_worker` idles and no alert can
  reach a phone.

**Step 9 answered in FU-1's favour:** `paper_engine.reconcile_adopted` fired
**0 times** across the 5 orders placed through the API this session, on top of
the 7 from 2026-09-02. FU-1 stays a follow-up.

### Two things Task 12 found that the test suite could not

**1. A `DAY` order silently became GTC across engine downtime. Fixed and
merged (`b59f98c`).** Order 24 (`RELIANCE BUY 2 MARKET DELIVERY`) was
submitted 2026-09-03 07:12 UTC and filled 2026-09-04 04:48 UTC — one second
after the engine was restarted, ~21 hours late, at a price ~₹26 from where it
was placed. `sweep_expired_day_orders` derived the session date from `now`
instead of from the order, so it asked "is *today's* session closed?" — which
at 10:18 IST it was not. The order therefore survived every sweep, including
the `startup_sweep` that exists precisely to catch orders orphaned by
downtime. A continuously-running engine hides this completely, which is why
every existing sweep test missed it: they submit and sweep inside one
simulated session.

**2. Live bar capture is only ~20–30% complete, and it is not a code bug.**
The machine had been on battery, deep-sleeping on a ~15-minute cycle with
41-second DarkWake windows; the ingestors only run while it is awake. Both
feeds gapping at *identical* minutes — despite two independent ingestor
processes and two unrelated venues — is what ruled out `bar_aggregator` and
pointed outside the codebase.

| | affected? |
|---|---|
| `bars_daily` (51M rows, 585,266 instruments, → 2026-08-21) | **No** — entirely Phase 0 backfill |
| `bars_intraday` backfill (`source=7`, → 2026-08-26) | No |
| `bars_intraday` live capture (`source` 6/8, last 11 days) | **Yes — 4%–48% per day, mostly 10–30%** |

3b's scope decision (daily bars) therefore sidesteps this entirely — but note
the corroboration: the passing dogfood example recorded "491 `on_bar` calls
over 5 sessions" against 5 × 375 = 1,875 expected, i.e. ~26%, squarely in this
band. **That example was already running on a quarter of reality and nothing
said so.** NSE holes are repairable with the existing
`upstox_intraday_backfill`; crypto has no REST backfill path in the repo.

Mitigation for a working session: `caffeinate -dimsu`, and keep the machine on
AC. Anything long-running deserves better than a laptop that sleeps.

---

## Phase status (implementation-plan.md §10)

| Phase | State |
|---|---|
| **Phase 0** — foundations | Complete 2026-08-24. 51M bars, 44,341 corporate actions, `docs/phase-0-closeout.md`. |
| **Phase 1** — streaming + manual paper trading | **Shipped.** Task 12 executed 2026-09-04, 8/10 steps pass; the three open items are operator actions, not code. |
| **Phase 2** — Agent Contract + strategy runtime | **Started.** Draft at `docs/agent-contract/STRATEGY_CONTRACT.md`. |
| **Phase 2.5** — intelligence layer | Not started. Recorders were meant to start in Phase 0 and compound; check whether the news/announcements recorder is actually running. |
| **Phase 3** — backtesting + metrics | **Sub-project 3a complete, and now visible in the UI.** `smoke_test` reads a strategy's declared `data.bars` and either serves it correctly (`"1m"`, `"1d"`) or rejects it with an honest finding naming the gap (see below); `/strategies` reports which interval a run actually received, and lists what is registered. **3b through 3f shipped and merged 2026-09-04**, and strategies now also run forward against live prices with a page to watch them on. Walk-forward analysis is the one piece of the original 3f scope still open. |

---

## Phase 3, sub-project 3a — the backtest data path (complete)

`smoke_test` (`trading.agent_contract.smoke`) resolves a manifest's
`data.bars` once, right after `configure()` returns, and threads the result
through `select_window`/`fetch_bars` instead of the two of them hardcoding
1-minute `bars_intraday` regardless of what a strategy asked for. `"1d"`
routes through `bars_daily` and the existing corporate-action adjustment
layer (`as_of` fixed to the backtest window's end date, D3a-2), so a strategy
declaring daily bars gets real, split-adjusted daily bars rather than
silently the wrong (1-minute) data. An interval outside the five the
contract permits (`platform_sdk.py`'s `BarInterval` is a `Literal` hint with
no runtime enforcement, so a typo like `"2m"` reaches this code for real) now
surfaces as `MANIFEST_UNRESOLVABLE` right after the `configure()` container,
before either smoke container runs, instead of silently proceeding on
whatever bars the old hardcoded path happened to find.

A whole-branch review caught that this was only half true at first landing:
`resolve_bar_interval` validated all five contract-legal values, but
`select_window`/`fetch_bars` only route `interval_sec == 86400` to
`bars_daily` -- `"5m"`/`"15m"`/`"1h"` (300/900/3600) fell through to the
same hardcoded `bars_intraday` path as `"1m"`, which holds ONLY 60-second
rows (confirmed directly against the database). A schema-legal `"5m"`
manifest was validated as fine and then silently served 1-minute bars
anyway -- the exact defect this plan exists to eliminate, for three of
five values instead of one. Fixed: `resolve_bar_interval` now also
distinguishes "not a recognized interval" from "recognized, but not yet
served," and `"5m"`/`"15m"`/`"1h"` raise `MANIFEST_UNRESOLVABLE` rather than
being silently misserved. Only `"1m"` and `"1d"` are served today; the
other three need real bar aggregation this platform does not yet have.

**Known limitation carried forward for 3b:** `bars_daily` rows are
timestamped at session CLOSE (verified: every row is 10:00 UTC / 15:30
IST), but the platform's `Bar.ts` contract defines the interval START --
a uniform, conservative one-day lag on the simulated clock for daily
strategies. Not a money or fill-order bug (every daily bar is affected
identically), but worth normalizing before a metrics layer persists
equity-curve timestamps built from it.

**Unblocks 3b:** backtest runs at scale (and the persistence/equity-curve
work that comes with it) can now assume `bars="1d"` is served correctly,
and that `bars="5m"/"15m"/"1h"` fails loudly rather than inheriting
phantom drawdowns from an unadjusted or wrong-interval read.

---

## What 3a looks like from the app (added 2026-09-03 evening)

3a was correct in the engine and invisible on screen: `select_window`
returned `start`/`end`/`sessions` and nothing else, so `/strategies` could
say "5 sessions" but could not say *which bars* — a distinction worth ~100×
in `on_bar` calls. Five changes closed that, and one of them was a real data
bug rather than a display gap.

- **The window now names its interval.** `select_window` stamps `bars` and
  `interval_sec` on both return paths, derived by inverting
  `_BAR_INTERVALS_SEC` so the label and the seconds cannot drift. Paths that
  resolved no interval (configure() produced no manifest; the manifest
  declared an unserved one) carry `None`, never a default of `60`/`"1m"` —
  the same refusal-to-default the rest of the module runs on.
- **The manifest survives registration.** `register_strategy` has always
  accepted a `manifest=`, and `POST /strategies` never passed one, so **every
  strategy registered before today stores `NULL`** and shows `--` in the new
  Bars column permanently. `SmokeVerdict` now carries the manifest
  `configure()` returned and the upload route hands it to stage 3. This was
  the only actual data loss in the set; the rest were presentation.
- **`GET /strategies` exists.** The registry was write-only from the app's
  side: an upload wrote `strategies` and `strategy_smoke_runs` and nothing
  could read either back, so §9's immutability rule (re-registering a version
  with different source is refused) was invisible — you could never see two
  versions side by side. Plain `def`, `LEFT JOIN LATERAL` to the newest run
  so a run-less strategy still lists, ordered `registered_at DESC,
  strategy_id DESC`. **Not scoped by `user_id`** — harmless with one seeded
  user and no auth, and the line to change when auth lands.
- **A run that filled nothing says why.** `RunSummary` gained `rejections`,
  `rejection_reasons` and `breaker_reason`, mirroring what `record_smoke_run`
  stores so the response and the row cannot disagree. "12 orders · 0 fills"
  previously read as a strategy that chose not to trade when in fact every
  order bounced — the same shape of half-truth as serving the wrong interval.
- **The page stops teaching the old world.** The starter template no longer
  claims 1-minute bars are a requirement, and a line above the upload button
  states that `1m`/`1d` are served while `5m`/`15m`/`1h` are contract-legal
  and rejected — so that rejection reads as a platform limit, not the agent's
  bug. Structured `findings` (code, §section, line) now render alongside the
  prose report, and the daily-clock caveat above appears on any `1d` run.

Verified against the live stack, not just tests: an upload returned `PASSED`
with `bars="1m"` under `runtime=runsc`/`kernel_isolated=true`, a `"5m"`
manifest was rejected `MANIFEST_UNRESOLVABLE §3` after the configure
container alone, and the page renders in a browser with no console errors.
`phase3a-interval-proof` 1.0.0 is that verification upload — it is the only
row with a populated manifest, and `DELETE FROM strategies WHERE name LIKE
'phase3a-%'` removes it.

Frontend presentation logic lives in `web/lib/strategies.ts` (pure, unit
tested against the exact sentences) rather than inside the page component.

---

## Recent merges on `main`

```
05470bb  Merge 'agent-contract-smoke-run': §9 stage 2, the smoke run
f49f069  Merge 'frontend-paper-trading': trade from the UI
ec6fa0c  Merge 'paper-trading-core': paper trading core (Phase 1)
```

`paper-trading-core` and `frontend-paper-trading` still exist as local refs.
Fully merged; safe to delete with `git branch -d`.

Test counts (2026-09-04, on `main`): **974 fast + 28 sandbox green**, mypy and
ruff clean. Earlier count for reference: **987 backend collected** — 952 green in the fast set (golden,
sandbox and live excluded); the 27 sandbox tests spawn real containers (the
smoke-run end-to-end ones spawn three each) and the 6 `live` tests need an
open NSE session, so they fail out of hours by design. **62 frontend** (was
36; `web/lib/strategies.ts` brought 17). ruff, mypy, eslint, tsc all clean.

---

## Phase 2 — where the contract draft stands

`docs/agent-contract/STRATEGY_CONTRACT.md` is at **v0.1 draft**. It is truthful
about the built platform (real field names, real enums, real charge figures
computed from the seeded schedules) and explicit that the runtime does not
exist.

**Five of the seven open decisions are settled** (D1, D2, D5, D6, D7 — each
with its reasoning in the contract's decisions table). Two remain:

- **D3 (sandbox limits)** — settled, and the environment turned out better
  than this file long claimed. See "Isolation" below: gVisor runs, for free,
  in a second Colima VM on this machine. No VPS is needed.
- **D4 (worked examples)** — unblocked and started. Stage 2 can execute them
  now, and the first one exists: `docs/agent-contract/dogfood/passing-buy-and-hold.py`
  is a cold agent's unedited output that PASSED (491 on_bar calls, 1 fill,
  +118.99 INR over 5 sessions). Three more archetypes to go before D4 closes.

**Dogfooding has started and it is finding real defects** — see
`docs/agent-contract/dogfood/RESULTS.md`. Two rounds, two contract bugs: §2
described a class shape the runner refuses (`2fbcfc5`), and `OrderUpdate` was
never documented at all (`2daf103`). Neither was findable by the test suite:
every strategy in this repo was written by someone who already knew the rule.
Round 3 passed first-try. **1 of the 3 model families §10 requires has
cleared**, and the other two must be run against the *current* contract.

Consequence of D6 worth remembering: **one strategy → one portfolio → one
currency**, so a single strategy cannot trade NSE equities and crypto together
in V1.

`schema.json` and `platform_sdk.py` are **written**, at
`src/trading/agent_contract/`. They live in the package rather than under
`docs/` so they fall under `mypy src` and the drift test — the schema's
enumerations are generated from `trading.paper.enums` and pinned by
`tests/agent_contract/test_contract_bundle.py`, so a value the schema accepts
is a value the order API accepts. Verified by mutation: appending a bogus
status to the schema fails the suite.

**Static validation is built** (`trading.agent_contract.validation`) — stage 1
of the §9 pipeline, plus stage 4's paste-back report. Manifest check against
the schema, import allowlist, AST scan for forbidden calls and escape-shaped
attribute access, wall-clock reads (a determinism rule, not a security one),
and structural checks. Stable finding codes so an agent can branch on them.

**Read the module docstring before extending it.** Static validation is *not*
the security boundary — an AST scan is bypassable by anyone trying, and
containment is the sandbox's job. It is a fast local filter for honest mistakes
in generated code. Both the module and the contract say so, and the "ACCEPTED"
report says so too, so nobody reads a pass as a proof of safety.

**Registration is built** (`trading.agent_contract.registry`, migration
`0010`) — §9 stage 3. Its load-bearing rule is that **a registered version is
immutable**: re-registering `(name, version)` with different source raises
`VersionConflict` rather than updating, because results already attributed to
that version must keep describing the code that produced them. Identical
source re-registers idempotently (a retry, not a change). Nothing that fails
static validation is stored, and the rejection carries the agent-facing report
so a caller can hand it straight back.

**The sandbox is built** (`trading.agent_contract.sandbox`, image in
`sandbox/`). Build it with `docker build -t trading-strategy-sandbox:0.1
sandbox/` — the tests need it.

### Isolation — corrected 2026-09-03, and verified

**Two earlier claims in this file were wrong, and both were wrong in the
pessimistic direction.** They said this machine runs Docker Desktop, which
ships `runc` only with no supported way to add gVisor, and concluded that a
paid Linux VPS would eventually be needed for isolation. Neither holds.

This machine runs **Colima**, not Docker Desktop. Colima is a Lima VM running
ordinary Ubuntu 24.04 that you have root in via `colima ssh` — so `runsc`
installs like any other package. The whole thing is free.

What is now set up and measured:

```bash
colima start --profile sandbox --cpu 2 --memory 4 --disk 20   # a SECOND VM
colima ssh --profile sandbox
  ARCH=$(uname -m)   # aarch64
  wget https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc
  wget https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/containerd-shim-runsc-v1
  chmod +x runsc containerd-shim-runsc-v1 && sudo mv runsc containerd-shim-runsc-v1 /usr/local/bin/
  sudo runsc install && sudo systemctl restart docker
```

`runsc release-20260817.0` is installed in the `sandbox` profile and works:
a container there reports kernel `4.19.0-gvisor`. Measured end to end,
`DOCKER_CONTEXT=colima-sandbox`, same strategy both ways:

| runtime | configure | smoke | `kernel_isolated` | final cash |
|---|---|---|---|---|
| `runc` | 0.38s | 0.31s | `False` | 98980.00 |
| `runsc` | 0.54s | 0.56s | **`True`** | 98980.00 |

numpy and pandas import and compute fine under gVisor, and the cost model
returns an identical figure. The overhead is ~0.2s per run — irrelevant
against a 120s smoke budget. All 18 container tests pass in that VM.

**The separate VM matters more than gVisor does right now.** The `default`
profile holds `trading_tsdb`, `trading_redis`, and `trading_redis_test`.
A container escape there never reaches macOS — Lima's hypervisor stops that —
but it lands in the same kernel as 51M bars of market data and the paper
ledger. Running strategies in a second VM removes the data plane from the
blast radius, and costs nothing but RAM.

**Not yet wired up.** `run_strategy_in_sandbox` still uses the ambient Docker
context and defaults to `runc`, so today's isolation gain is available rather
than automatic. Turning it on means pointing `SandboxLimits` at the sandbox
VM's socket and defaulting `runtime="runsc"`. Every `SandboxResult` already
records `runtime` and `kernel_isolated`, so a stored run can never be misread
as better isolated than it was.

Strategy source is piped over **stdin**, not bind-mounted: no host path is
exposed, and it sidesteps the VM's fixed share list, which excludes the system
temp directory (the first implementation failed on exactly that). That reason
survives the Docker Desktop correction above — every macOS Docker backend
shares only a configured set of host directories, Colima included.

15 tests attempt the forbidden thing and assert containment — socket, DNS,
writing outside `/tmp`, memory exhaustion, an infinite loop, running as root.
Verified non-vacuous by weakening the sandbox (network on, rootfs writable) and
watching them fail.

**§9 stage 2, the smoke run, is implemented** (`trading.agent_contract.smoke`,
`trading.runtime`). A submitted strategy is run against real bars, real fills,
and the real cost model inside the sandbox, and gets back a verdict written to
be pasted straight back into the agent that wrote it. One upload costs three
container runs: `configure` resolves the manifest — the host cannot fetch bars
until the strategy says which instruments it wants — then the smoke payload
runs **twice** and the two order sequences are compared. That comparison is
what finally enforces §2: determinism was a documented rule that nothing
checked, and a static scan that catches `datetime.now()` misses set iteration,
unseeded `random`, and dict-hash dependence. Runs are stored in
`strategy_smoke_runs` (migration 0011) — one row per run, many per version,
because the window moves even though the version does not.

Upload path, complete: validate → smoke → register, reachable over HTTP at
`POST /strategies` and from the UI at `/strategies`. The request blocks for the
whole run -- three containers, a few seconds -- which is right for one operator
and wrong for a queue. Set both `STRATEGY_SANDBOX_DOCKER_CONTEXT=colima-sandbox`
and `STRATEGY_SANDBOX_RUNTIME=runsc` (in `.env.local`) to get gVisor. **Both**
are required and neither is inferred: that daemon lists `runsc` among its
runtimes and still *defaults* to `runc`, so naming the daemon alone silently
records `kernel_isolated=false` while looking correctly configured. Unset on a
machine without gVisor -- there is no fallback, by design.

Next in Phase 2: **D4, the worked examples.** They were withheld because an
example in a contract is a promise the code runs, and nothing could run it.
Stage 2 is that runner, so the four examples can now be executed before they
are published.

**Acceptance bar** (plan §10): the contract is not done until *three different
frontier agents*, each given only that file, each produce a working strategy
first-try.

Useful sequencing thought: the contract-and-schema half is separable from the
sandbox half. Drafting and dogfooding the contract against three agents tests
the risky part (is the spec good?) before provisioning anything. The sandbox is
well-understood engineering; the contract is the bet.

---

## Open follow-ups

| # | What | Status |
|---|---|---|
| **FU-1** | After-commit callback registry on `get_db_connection`, so the `orders:control` `"new"` publish happens after the commit. The documented FastAPI fix does **not** exist in 0.141.1 — verified empirically, background tasks run before yield-dependency teardown. A 5s reconciliation sweep is the shipped backstop. | Task 12 Step 9 decides promotion. |
| **FU-2** | The paper engine is single-process **by design**, and DP-charge dedup correctness now depends on it. A second engine process on the same instrument would race the dedup SELECT and double-charge. | Gate any horizontal scaling on making that race-safe. |
| **FU-3** | No CHECK constraint ties `charge_schedules.basis` to `.charge_type`. `InvalidChargeSchedule` catches a malformed row at fill time; a constraint would refuse it at write time. | Open. |
| **FU-4** | Live bar capture is ~20–30% complete because the host sleeps. NSE holes are repairable via `upstox_intraday_backfill`; crypto needs a Binance REST klines backfill that does not exist. Anything reading live `bars_intraday` as if it were continuous is wrong. | Open. Does not block 3b (daily bars). |
| **FU-5** | Three suspected charge-model errors awaiting the Step 3 external grading: IPFT reads ₹0.0000 (rate behaves like `1e-9`; ₹10/crore implies `1e-6`), GST's base excludes the SEBI turnover fee, and exchange txn implies 0.00307%. | Blocked on Step 3. |
| **FU-6** | Three `RELIANCE` CM instrument rows exist — `58607` (NSE, Upstox-bound, the live one), `108061` (NSE, no binding, no bars), `101629` (BSE). Picking the wrong id yields an instrument that never ticks. | Open. |
| **FU-7** | `paper_engine` logs races, rejections and sweeps but **never a successful fill**, so the engine log cannot be used to follow trading activity. | Open; made Step 9 harder to trust. |
| — | Migrations `0007`, `0008`, `0009` reference `.superpowers/sdd/` paths in their docstrings — dangling once that scratch directory is deleted. Same class as the M-a fix, in three committed files. | Cosmetic. |

---

## Running the stack

Infra is `docker compose up -d` (timescaledb, redis on 6379, redis_test on
6380 — a separate *instance*, because Redis pub/sub ignores the db number).

**The database must be at migration `0010`.** `0009` adds `fills.tds` (an older
schema fails every fill insert); `0010` adds the strategy registry. Both
`trading` and `trading_test` are at `0010`.

```bash
uv run alembic upgrade head
uv run uvicorn trading.streaming.gateway:app --reload --port 8000
uv run python -m trading.streaming.crypto_ingestor      # Binance, 24/7
uv run python -m trading.streaming.bar_aggregator
uv run python -m trading.streaming.upstox_ingestor      # NSE session only
uv run python -m trading.paper.engine                   # the fill loop
uv run python -m trading.paper.alerts                   # outbox drain
cd web && npm run dev                                   # localhost:3000
```

Check for already-running processes before starting any of these — several
have been up for days, and **two crypto ingestors would double-publish every
tick**.

**Telegram is unconfigured**, so `run_alert_worker` idles and alerts queue in
`alert_deliveries` as `PENDING`. That is a deliberate configuration state, not
a fault. To switch it on, set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in
`.env`. Task 12 Step 6 wants an alert to actually reach the phone.

Demo portfolios from the 2026-09-02 session: **9** ("Crypto Demo", USDT) and
**10** ("INR Demo", INR).

---

## Two things about this codebase worth not relearning

**Review, not tests, has found nearly every real defect here** — six
quantization asymmetries, a fill-vs-cancel race, DP billed per fill instead of
per scrip per day, a missing currency gate (an INR portfolio could buy BTC-USDT
and be ~90× wrong). Each fix was then validated by *mutation* — flip the
operator, drop the exception from the catch tuple, select the wrong limit — and
several of those mutants survived a green suite. Use that on money paths.

**FastAPI routes must be `def`, never `async def`.** psycopg is synchronous;
an async route running a blocking DB call on the event loop deadlocked the
gateway permanently under concurrency. `test_no_route_is_a_coroutine_function`
guards it. GETs must never write.
