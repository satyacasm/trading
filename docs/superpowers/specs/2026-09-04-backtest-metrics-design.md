# Metrics over the stored equity curve — Phase 3, sub-project 3d

**Status:** design, approved 2026-09-04.
**Phase:** 3 (Backtesting + metrics), fourth sub-project.
**Follows:** 3c (persistence), which stores exactly what this reads.
**Precedes:** 3e (report UI), 3f (walk-forward + robustness).

---

## 1. What this is

3c stores an equity curve and computes nothing. 3d turns that curve into
the numbers a research platform exists to produce: how much it returned,
how much it risked, and how bad the worst stretch was.

It is a **pure function** — curve in, metrics out. No tables, no schema,
no migration, no writes.

## 2. Scope, and the two families that are not here

The implementation plan's metrics suite (§178) names far more than this
sub-project delivers. The cut is not arbitrary: it is exactly the line
between what the stored data supports and what it does not.

**Delivered — everything derivable from the curve:**

total return; annualized return (CAGR); volatility; Sharpe; Sortino;
Calmar; maximum drawdown **depth and duration**; VaR(95); worst period;
the drawdown curve; monthly returns; rolling 6-month Sharpe.

**Not delivered — trade-level metrics.** Win rate, profit factor, average
win/loss, expectancy, turnover, and the cost-drag report (gross versus
net-of-charges) all need a per-fill ledger: price, side, and the itemised
charges of each fill. `run_loop` does not emit one. `RunOutcome.orders`
carries `OrderSnapshot`, whose fields stop at `status` and `submitted_at`
-- no fill price, no charges -- and `fills` is a bare count. These are
therefore blocked on a **runtime** change, not a metrics one, and writing
them against what exists would mean inventing the inputs.

This is worth stating plainly because the cost-drag report is the single
metric §8 calls out as "often the most sobering chart we can show a retail
options trader". It is wanted. It is simply not reachable from an equity
curve, and pretending otherwise would produce a number with no basis.

**Not delivered — benchmark-relative metrics.** Alpha and beta versus
NIFTY 50 TRI need a benchmark series. This database holds **zero index
instruments**: `instruments` contains only OPTION, EQUITY, MF, FUTURE and
CRYPTO, and a search for an index row returns nothing. That is a
data-ingestion job, not a metrics one.

## 3. Settled decisions

### D3d-1. Computed on read, never stored

Metrics are a pure function of the stored curve, and folding ~1,650 points
is trivial. Three consequences, each of which is the reason:

- **Nothing can drift.** A stored metric is a second source of truth that
  can disagree with the curve it came from. Recomputing means the two
  cannot.
- **Adding or fixing a metric needs no migration.** A corrected Sharpe
  applies retroactively to every run ever stored, rather than only to runs
  computed after the fix -- which is the failure mode a metrics table
  guarantees.
- **No backfill.** The alternative requires one every time the set changes,
  and the set will change: 3f adds robustness figures on top of these.

If a list view ever proves too slow, that is the moment to add a cache --
measured, not assumed. The pure function stays the source either way.

### D3d-2. The arithmetic stays in `Decimal`

The reflex is numpy and float64; every metrics library does that.
`Decimal.sqrt()` exists, and at this size the cost is irrelevant.

The reason is not precision for its own sake -- a Sharpe wrong in the
fifteenth decimal changes nothing. It is that **returns are derived from
money**. Once a float enters the chain, the boundary between "money, which
must be exact" and "statistics, where error is harmless" is enforced by
nothing but attention, and this codebase's history is six quantization
defects found by review rather than by tests. One rule -- no float in the
chain -- is cheaper to hold than a rule with a carve-out, and it is the
same rule `numeric(18,4)`, `str(Decimal)` on the wire, and the payload
codec already follow.

Ratios are dimensionless and are rendered as fixed-precision strings, like
every other number crossing this API.

### D3d-3. Definitions, because "Sharpe" is ambiguous

Every one of these has more than one defensible definition, so the choice
is recorded rather than left to the implementation:

- **Period returns.** `r_t = E_t / E_{t-1} - 1` over consecutive curve
  points. A point where `E_{t-1}` is zero yields no return rather than a
  division error -- a portfolio at zero equity has no meaningful return.
- **Total return.** `E_last / E_first - 1`.
- **CAGR.** Compounded over **calendar days** between the first and last
  `ts`, not sessions: a year is a year regardless of how many times the
  exchange opened. `(E_last/E_first)^(365/days) - 1`.
- **Annualization factor.** 252 sessions per year for `1d`, derived from
  the run's stored `bars` value rather than hardcoded, so the day an
  intraday interval is served the factor is not silently wrong.
- **Volatility.** Sample standard deviation of period returns (`n-1`
  divisor), annualized by `sqrt(252)`.
- **Sharpe.** `(annualized return - risk_free) / volatility`. See D3d-4.
- **Sortino.** As Sharpe, but the denominator is downside deviation --
  the root-mean-square of returns below zero, annualized. Returns at or
  above zero contribute nothing rather than being excluded from the count,
  which is the definition that keeps Sortino comparable across runs.
- **Calmar.** `CAGR / |max drawdown depth|`.
- **Max drawdown depth.** `min over t of (E_t / running_peak_t - 1)`, a
  non-positive number.
- **Max drawdown duration.** The longest span from a running peak to the
  point that first recovers it, reported in **both sessions and calendar
  days**. If the curve never recovers, the span runs to the last point and
  the result is flagged `recovered: false` -- an unrecovered drawdown
  reported as if it had ended is the single most misleading thing this
  module could do.
- **VaR(95).** The historical 5th percentile of period returns, by
  nearest-rank on the sorted series. Not a parametric normal assumption:
  equity curves are not normal, and saying so costs nothing.
- **Worst period.** `min(r_t)`, with its timestamp.
- **Drawdown curve.** `E_t / running_peak_t - 1` at every point, so 3e can
  draw it without recomputing.
- **Monthly returns.** Compounded within each **IST** calendar month --
  the same timezone convention the DP scrip-day key and the breaker's
  day rollover already use.
- **Rolling Sharpe.** A 126-session window (six months), emitted only once
  the window is full rather than padded, because a Sharpe over eleven
  points is noise wearing the same name.

### D3d-4. The risk-free rate is stated, never assumed to be zero

Backtests conventionally use `rf = 0`, and on an Indian platform that is a
systematically flattering lie: the risk-free rate here is roughly 6-7%,
not roughly 0%. A strategy returning 6% a year reads as respectable at
`rf = 0` and is in truth worse than a government bond.

**The default is 6.5% annual, overridable per request, and the rate used
is echoed in the response next to every Sharpe and Sortino.** A reader who
disagrees with 6.5% can say so; a reader who never thought about it is
told what was assumed rather than left to infer zero.

### D3d-5. Metrics live on the detail route only

`GET /backtests/{run_id}` gains a `metrics` object. The list route keeps
its shape.

That is not an omission. Computing metrics for a list of 50 runs means
loading 50 curves -- roughly 80,000 points -- which is precisely the cost
D3c-5 created two routes to avoid. If 3e proves it needs headline figures
in a history table, the answer at that point is a stored summary, and it
should be decided with that requirement in hand rather than guessed now.

## 4. Explicitly out of scope

- **Trade-level metrics and the cost-drag report.** Blocked on a runtime
  fill ledger. Named in §2.
- **Benchmark-relative metrics.** Blocked on ingesting an index series.
- **The report UI.** 3e.
- **Robustness: 2x cost stress, Monte Carlo reshuffle, walk-forward.** 3f,
  and all three consume these metrics.
- **Post-tax P&L lens.** Needs the fill ledger too, plus a holding-period
  model; it belongs with the trade metrics.
- **Metrics over live paper portfolios.** The plan wants these for forward
  runs as well; that reads `portfolio_equity_snapshots`, a different
  source with a different shape, and folding both into one module now
  would couple two things that have not yet been shown to be the same.

## 5. Testing

Hand-computed fixtures with answers known independently of the code:

- **A flat curve** has zero volatility, zero return, and a Sharpe that is
  reported as undefined rather than as a division by zero.
- **A monotonically rising curve** has zero drawdown depth and no drawdown
  duration.
- **A V-shaped curve** has an exact, hand-computed drawdown depth *and* a
  duration in both sessions and calendar days.
- **A curve that never recovers** reports `recovered: false` and a
  duration running to its last point. This is the case a naive
  implementation gets wrong and reports as recovered.
- **Mutation:** change the annualization factor from 252, and change the
  standard-deviation divisor from `n-1` to `n`; both must redden a test.
  A metrics module whose tests survive those two mutations is not testing
  the arithmetic.
- **No float anywhere:** assert every intermediate is `Decimal`, and that
  the rendered output parses back to the same value.
