# The backtest report UI — Phase 3, sub-project 3e

**Status:** design, approved 2026-09-04.
**Follows:** 3d (metrics), which returns every number this draws.
**Precedes:** 3f (walk-forward + robustness).

---

## 1. What this is

3d returns a full metrics object and nothing draws it. A backtest is also
still unstartable from the product: it takes a `curl`. 3e closes both --
a strategy's run history with a form to start a run, and a report page for
one run.

## 2. Settled decisions

### D3e-1. Two routes, mirroring the two endpoints

- `/strategies/[id]` -- the strategy, a **Run backtest** form, and its runs
  newest-first. Reads `GET /strategies/{id}` and
  `GET /strategies/{id}/backtests`, the curve-free list.
- `/backtests/[id]` -- one report. Reads `GET /backtests/{id}`.

One-to-one with the API, so the cheap list stays cheap: a history table
never loads a curve. A report also gets a stable, linkable URL, which the
expandable-rows alternative would not.

Both are `"use client"` with `useParams`, following
`app/instrument/[id]/page.tsx` exactly rather than inventing a second
convention for dynamic routes in this app.

### D3e-1a. One small API addition: `GET /strategies/{strategy_id}`

The detail page needs a name and version. `registry.get_strategy` already
exists and raises `KeyError`; the route is a handful of lines and turns
that into a 404. The alternative -- fetching the whole list and filtering
client-side -- transfers every strategy to render one, and reads as an
oversight rather than a decision.

### D3e-2. The report leads with the hurdle, not the return

**This is the signature, and it is a deliberate refusal of the obvious.**

Every backtest report opens with a large green total return. This one
opens with the equity curve drawn **against a risk-free growth line from
the same starting capital**, with the area between them shaded: `--up`
when the strategy is ahead, `--down` when it is behind. That band *is* the
result.

The reason is the platform's own identity. Its distinguishing feature is
refusing comfortable numbers -- the honest Indian cost model, `recovered:
false` on an open drawdown, refusing to serve an interval it cannot serve
correctly. The real stored run makes the case: buy-and-hold RELIANCE
returned **+5.59% over 6.6 years**, a CAGR of **0.82%**, which a headline
return renders as a success and the hurdle renders as what it is -- a
widening red wedge under a 6.5% risk-free line.

It also puts the `risk_free` value the API already echoes on screen, where
a stats-table row would bury it.

### D3e-3. No new dependencies

`lightweight-charts` is already a dependency and already drives the price
charts; it draws the equity, risk-free, drawdown and rolling-Sharpe series.
The monthly-returns heatmap is a CSS grid shaded with
`color-mix(in oklab, var(--up) N%, transparent)` -- the technique
`globals.css` already uses for its flash animations. Adding a charting
library for one heatmap would be the wrong trade.

### D3e-4. It extends the existing design system, it does not invent one

`globals.css` defines semantic tokens (`--ground`, `--surface`, `--raised`,
`--line`, `--text`, `--muted`, `--up`, `--down`, `--live`) and three
typefaces with stated roles: Space Grotesk for identity, Inter for
labelling, JetBrains Mono for anything numeric via `.num` with tabular
figures. Every number this page renders uses `.num`; no new color is
introduced. A report that looked foreign to the rest of the terminal would
be worse than a plain one.

### D3e-5. Presentation logic is pure and unit-tested

Following `lib/strategies.ts`, which exists for exactly this reason: the
sentences a reader sees are tested against their exact text in
`lib/backtests.ts`, and the page component only wires them up. That is what
made 3a's "12 orders, 0 fills" phrasing testable, and the same applies to
"not recovered" and to a refusal's finding message.

### D3e-6. Copy names what happens

Active voice, sentence case, the same word through a flow: the button says
**Run backtest** and the result says **Ran**. An unrecovered drawdown says
**"not recovered"** in words rather than leaving a blank where a recovery
date would be. A refusal shows the finding message 3b already writes for
an agent -- "the window runs to 2026-12-31 but these instruments have no
bars after 2026-08-21" reads perfectly to a human, and rewriting it would
create two texts that can drift.

## 3. Out of scope

- **Trade-level panels** -- win rate, cost drag. Blocked on the fill ledger
  (3d §2), not on the UI.
- **Benchmark overlay against NIFTY.** No index instrument exists. The
  risk-free line is the hurdle this platform can draw honestly today.
- **Comparing two runs side by side.** 3f.
- **Editing or deleting runs.** A run is an immutable record of an event.

## 4. Testing

- `lib/backtests.ts` unit tests against exact sentences: the hurdle verdict
  ("0.82% CAGR against a 6.50% risk-free rate"), the drawdown line
  including the unrecovered case, and a refusal's rendering.
- A metrics object with `null` fields (a one-point curve, a flat curve)
  renders without crashing and without printing "null" -- undefined metrics
  are shown as "--", the same convention `/strategies` already uses.
- `npm run build` and `tsc` clean; eslint clean.
- Verified in a browser against the real stored run, with no console errors.
