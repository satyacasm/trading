# Phase 0 closeout

**Date:** 2026-08-24 · **Status: complete.**

Phase 0 (implementation-plan.md §10) was scoped as: instrument master
schema, TimescaleDB setup, EOD ingestion for NSE/BSE equity + F&O bhavcopy
with 10 years of backfill, corporate actions ingestion, AMFI NAVs, and the
week-1 empirical checks + self-recorders. All 18 planned tasks
(`docs/superpowers/plans/2026-08-14-phase-0-data-foundations.md`) landed;
this closes out Task 17's reconciliation gate (spec §8), the one that
decides whether the backfill can actually be trusted.

## What's in the warehouse

- **51,081,227 bars** across 5 sources (NSE CM UDiFF, NSE F&O UDiFF, BSE CM
  UDiFF, NSE CM legacy, AMFI NAV history), 2016-01-01 → present
- **585,273 instruments**, **44,341 corporate actions** (NSE + BSE)
- 6,330 ingest jobs, all resumable via the ledger

## The 11 verification checks, final state

| # | Check | Status | Note |
|---|---|---|---|
| 1 | calendar_completeness | FAIL (trivial) | today's date not yet ingested; self-resolves on the next daily run |
| 2 | known_values | PASS | 12 hand-verified values, byte-checked against raw archives |
| 3 | cross_source_agreement | FAIL → **fixed**, 6-row residual documented | `docs/cross-source-agreement-review.md` |
| 4 | continuity | FAIL, all buckets reviewed | `docs/continuity-spike-review.md`, `docs/continuity-step-review.md` |
| 5–9 | idempotency (×5 sources) | PASS | re-parsed archived bytes, zero drift |
| 10 | quarantine_rate | FAIL → documented exception | `docs/quarantine-rate-review.md` |
| 11 | recorder_liveness | NOT_APPLICABLE | Upstox self-recorder has never run (credentials blocked, see below) |

Per task-17-brief.md Step 6 — "a FAIL must be resolved as a bug fix or a
documented, justified exception; neither may be left silent" — every FAIL
above has now been through exactly that process:

- **Two real defects found and fixed**, both in how a corporate action's
  price adjustment finds the right instrument (a series-migrating stock
  getting its split attached to the wrong sibling `instrument_id`; a
  future's `underlying_price` compared against a same-symbol bond instead
  of the equity). Both live in `corpactions/adjust.py` and `reconcile.py`
  — one of them, the corp-actions fix, affects real backtest correctness
  (spec §4.3/D10), not just this report.
- **Everything else traced to real, verified data**, not defects: Rights
  Entitlements' normal volatility, a decade of well-documented penny-stock
  and crisis-era moves (IDEA, YESBANK, ADANIENT/Hindenburg, INDUSINDBK 2025,
  a market-wide silver-ETF shock), AMFI's segregated-portfolio schemes from
  the 2019–2021 debt-default wave, and exchange settlement-price mechanics
  that legitimately sit outside a session's traded range.
- **Two gaps knowingly deferred, not silently dropped**: ETF/mutual-fund
  unit splits are never ingested (Task 16 only covers equities; verifying
  the right feed needs live network access this environment doesn't have —
  `docs/continuity-step-review.md` Finding 3), and a 10-case residual where
  a stock's ISIN genuinely changed rather than just its series (needs an
  explicit identity-transition record, not a bigger join).

`recorder_liveness` stays `NOT_APPLICABLE`, not a pass — the Upstox
self-recorder (Task 15) has never run because Upstox reactivation
(UDAPI100058) is still blocked. This is an external dependency, not a Phase
0 build defect, and doesn't block Phase 1 starting (Phase 1 builds the
real-time ingestion the recorder depends on in the first place).

## Carried into Phase 1

- Upstox credential/reactivation blocker (self-recorder, real-time feed)
- ETF/fund-unit corporate-actions ingestion (needs live endpoint
  verification)
- 10-case ISIN-transition residual in continuity
- G1 from task-17-brief.md: NSE delivery-percentage data (`delivery_qty`/
  `delivery_pct`) still uningested — deferred to Phase 2.5 per the original
  plan, unaffected by this closeout
