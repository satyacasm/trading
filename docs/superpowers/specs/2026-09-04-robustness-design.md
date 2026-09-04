# The robustness suite — Phase 3, sub-project 3f

**Status:** design, approved 2026-09-04.
**Follows:** the fill ledger, which the reshuffle reads.
**Scope:** Monte Carlo reshuffle, the 2x cost-and-slippage stress rerun, and
the repeated-run warning. **Walk-forward is deliberately not here** — see §3.

---

## 1. What this is

A backtest reports one path. §228 says that path is not enough: "an
automatic 2x slippage-and-cost stress rerun (if the edge dies at 2x, it was
never an edge), and a Monte Carlo trade-order reshuffle producing a
*distribution* of max drawdowns and terminal equities rather than the single
lucky path — with the 5th-percentile outcome displayed as prominently as the
mean."

Both run on **every** backtest, without being asked for. A robustness check
you have to request is one you skip when the base result looks good, which
is precisely when it matters.

## 2. Settled decisions

### D3f-1. Store what was observed; compute what can be derived

The two checks fall on opposite sides of a line 3c and 3d already drew.

- **The reshuffle is a pure function of the stored fill ledger.** Shuffling
  realised round-trip P&L and re-accumulating needs no container and no new
  data. It is therefore computed on read, exactly like every metric in 3d:
  nothing can drift from its source, and improving the method applies
  retroactively to every run ever stored rather than only to new ones.
- **The 2x stress is an observation.** Doubling slippage changes *which
  fills happen* — an order that filled at the real price may not fill at
  all — so the result cannot be re-derived from a base run's output. It has
  to be executed, and what is executed has to be stored.

Stated as a rule because it will keep coming up: **anything that required
running the world is stored; anything that is arithmetic over what was
stored is computed.**

### D3f-2. The 2x stress needs no runtime change

The obvious implementation plumbs a multiplier through `SmokePayload`, the
runner, `run_loop` and `compute_charges`. That is four boundaries, and this
sub-project has already learned what each one costs.

Instead the **schedules are scaled host-side** before the payload is built:
each `ChargeSchedule`'s `rate` and `cap` are doubled, and `slippage_bps` is
doubled. The container then runs unmodified code against a harsher cost
environment, which is both simpler and more honest — the stress *is* a
different cost environment, not a different calculation.

`cap` is doubled alongside `rate` deliberately. A capped charge whose rate
doubled but whose cap did not would simply stay at its cap, and the stress
would silently not apply to exactly the charges that dominate a large order.

### D3f-3. The reshuffle is seeded, and says what it destroys

**Seeded.** Unseeded, the same stored backtest would report different
robustness figures on every page load. Determinism is a rule this platform
enforces on strategies (§2, checked by the double-run comparison); a metrics
layer that violated it would be indefensible. A fixed seed makes the
distribution reproducible and comparable between runs.

**1,000 iterations**, which is plenty for a 5th percentile and costs
milliseconds over a few hundred trades.

**What it destroys, said out loud.** Reshuffling trade order removes serial
correlation: a strategy whose losses genuinely cluster (a trend follower in
a chop) will look better reshuffled than it was. The report says so rather
than presenting the distribution as a neutral fact. The question the
reshuffle actually answers is narrow and worth stating plainly: *how much of
this drawdown was the order the trades happened to arrive in?*

### D3f-4. Percentiles, with the 5th as prominent as the median

§228's instruction, and the substance of it: the mean outcome is the one
that flatters. The report shows the 5th, 50th and 95th percentiles of both
terminal equity and maximum drawdown, and the 5th is not smaller or greyer
than the 50th.

### D3f-5. The repeated-run warning is a query

§6 asks for "a gentle warning when a user re-runs the same strategy many
times on identical data". Now that 3c stores runs, that is
`COUNT(*) WHERE strategy_id = ? AND requested_start = ? AND requested_end = ?`.

Shown from the **fourth** run of an identical window onward. Three is
ordinary iteration; a fourth suggests tuning against one period, which is
how a backtest becomes a curve fit. The wording is a note, not a block —
"gentle" is the plan's word and the operator may have a good reason.

## 3. Explicitly out of scope

- **Walk-forward analysis.** It needs a splitting policy that is a design in
  itself — anchored or rolling, how many folds, what in-sample/out-of-sample
  ratio — plus N container runs per backtest and a report shape for
  comparing folds. Folding that decision into a sub-project about stress
  testing would bury it.
- **The post-tax P&L lens.** Own design; needs a tax model.
- **Parameter sweeps.** §6 mentions a vectorized "quick scan" mode as a
  clearly-labeled approximation, explicitly "later".

## 4. Testing

- **The reshuffle is deterministic**: the same fills produce the same
  percentiles across calls. Asserted directly, because an unseeded shuffle
  passes every other test in the suite.
- **A reshuffle of a single trade has zero dispersion** — the 5th and 95th
  percentiles coincide. The degenerate case a percentile implementation
  gets wrong.
- **Ordering genuinely changes drawdown**: a hand-built sequence whose
  losses cluster has a deeper max drawdown in its actual order than in its
  best reshuffle, which is the property that makes the check meaningful.
- **The stress run is harsher**: against the same strategy and window, the
  2x run's final equity is lower than the base run's, or the run is
  refused. Asserted end to end, because scaling the wrong field (`rate` but
  not `cap`) still produces a plausible-looking number.
- **Doubling reaches capped charges**: a schedule with a cap has both rate
  and cap doubled, verified directly rather than through a total.
- **The warning appears on the fourth identical window and not the third.**
