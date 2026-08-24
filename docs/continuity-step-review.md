# Continuity check: review of the 1,530 steps

**Date:** 2026-08-24 · **Warehouse:** 51,081,227 bars, 2016-01-01 → 2026-08-24

`check_continuity` classifies every single-session equity move beyond 20% by
its shape. A **step** is a move that does *not* revert next session — the
shape of a real split-adjustment bug (spec §8 item 4), so it fails unless a
`corporate_actions` row with a matching `instrument_id` and exact `ex_date`
explains it. This is the review of all 1,530 the full warehouse produced.
(The companion 63 **spikes** were reviewed separately: `docs/continuity-spike-review.md`.)

## Conclusion

**No unadjusted-price defects — but two real gaps in how corporate actions
get *linked* to the price series they belong to, both now fixed.** Every
step is a faithfully recorded price; the database was never lying about what
the exchange printed. But ~140 of the 1,530 steps were cases where the right
corporate action already existed in `corporate_actions` and the lookup (both
this check, and — more importantly — the live `adjustment_factors()`
backtest-adjustment path in `src/trading/corpactions/adjust.py`) failed to
find it, for the two reasons detailed below (Findings 1–2). Both are fixed
as of this review: `check_continuity`'s FAIL count for `step` dropped from
1,530 to 1,390 (explained rose from 1,082 to 1,222) on re-run against the
full 51M-row warehouse. A small residual (10 cases, discussed under
Finding 1) needs a genuine identity change, not a linking bug, to resolve —
left as a documented exception.

Finding 3 (ETF/fund-unit splits) is a genuine ingestion gap, not a linking
bug, and is **not fixed** — see its section for why.

## Breakdown

| Class | Count | Disposition |
|---|---:|---|
| Rights entitlements (`-RE`, `-RE1`, `-RE2`, `-RE3`) | 747 | Normal — same finding as the spike review, extended to steps |
| **Corp action existed, wrong `instrument_id` / wrong `ex_date`** | **~140** | **Fixed — `adjust.py` and `check_continuity` now match by identity group + date window** |
| Same symbol, but a genuine ISIN change (not a linking bug) | 10 | Documented residual — see Finding 1's note |
| ETF / mutual-fund unit splits | 108 | **Gap — never ingested at all, not fixed (see Finding 3)** |
| Real market events (large/liquid) | ~300 | Genuine, verified — no defect |
| Illiquid / thin / near-zero volume | ~275 | Same class as the spike review — real trades, not meaningful signal |

Numbers overlap slightly at the edges (a handful of cases touch two
categories); after the fix, `step` fell from 1,530 to 1,390 (2026-08-24
re-run against the full 51,081,227-row warehouse).

## Finding 1 — series migration breaks the corp-action-to-price join (fixed)

`check_continuity` (and, critically, `adjustment_factors()` in
`corpactions/adjust.py`, which is what a real backtest uses to compute
adjusted prices) both match on exact `instrument_id`. Our instrument identity
key includes `series` (`EQ`, `BE`, ...), and a stock that migrates series
around the time of its split/bonus — a routine SEBI surveillance event,
unrelated to the corporate action itself — gets **two different
`instrument_id` rows for the same ISIN**, and the corporate action lands on
whichever one NSE/BSE's feed reports it against, which is not always the one
that was actually trading (and printing the step) on ex-date.

Concrete example, byte-for-byte from the database:

```
PCJEWELLER (ISIN INE785M01013, NSE):
  instrument_id=106261 (series BE): 2024-12-16 close drops -89.5%, the step
  instrument_id=60094  (series EQ, same ISIN, 2,490 historical bars): SPLIT 1:10
                                        ex_date=2024-12-16, attached here instead
```

The corporate action is *real* and its `ex_date` is *correct* — it's
attached to the wrong sibling `instrument_id` for that ISIN. The old
`check_continuity`/`adjustment_factors()` exact-instrument-id join could
never find it, no matter how wide the date tolerance.

**Scope:** confirmed 80 concrete instances in the equity continuity sample.
Database-wide, **6,923 ISIN groups have more than one `instrument_id`**
(series migrations across the 10-year history are common), and **1,102 of
those groups have at least one SPLIT/BONUS action attached to one of the
sibling instrument_ids** — that is the exposure ceiling for backtests reading
through `adjustment_factors()`, not just this check.

**Fixed 2026-08-24** in both `adjustment_factors()`
(`src/trading/corpactions/adjust.py`) and `check_continuity`
(`src/trading/reconcile.py`): both now look up a corporate action across
every `instrument_id` sharing the target's ISIN, exchange and segment (an
"identity group"), not just the exact one, deduplicating so an action
ingested once per sibling (confirmed live for PCJEWELLER's BSE split, filed
three times) is never double-applied. This was the more important of the two
fixes — `adjustment_factors()` is live production logic for point-in-time
price adjustment (spec §4.3/D10), so before this fix a backtest reading bars
under `instrument_id` 106261 got *unadjusted* prices with no warning, for
any symbol whose split/bonus landed on the sibling identity. Tests:
`tests/corpactions/test_adjust.py::test_a_split_recorded_against_a_sibling_series_still_adjusts_the_bars`,
`::test_a_duplicate_action_across_sibling_series_is_applied_only_once`,
and the `test_reconcile.py` equivalents.

**Residual — genuine ISIN changes (10 cases, not fixed).** AARTECH turned
out to be a *different* shape than PCJEWELLER: its two rows carry two
*different* ISINs (`INE01C001018` on the BE row that printed the step,
`INE01C001026` on the new EQ row the split landed on) — a real corporate
identity change, not just a series relabelling. An ISIN-keyed sibling match
correctly leaves these alone (merging on symbol alone risks conflating two
unrelated companies that happen to reuse a symbol years apart, which is rarer
but real). Re-running the full check after the fix found 10 such cases
database-wide. Documented, justified exception — resolving them needs an
explicit old-ISIN→new-ISIN linkage (e.g. a `SYMBOL_CHANGE`-shaped corporate
action recording the transition), not a bigger join.

## Finding 2 — illiquid names print their first post-action trade days after `ex_date` (fixed)

~20 steps (mostly BSE micro-caps: MINOLTAF, MOHITE, GETALONG, BPAGRI, LADDU,
HAMPS, TIMESGREEN, UEL, GCSL, HIIL, KEEPLEARN, GANHOLD, CAPRICORN,
ONIXSOLAR, BODHTREE, STEELCO) have a real, correctly-attached corporate
action within 1–7 calendar days of the step, with the move percentage
matching the ratio almost exactly (e.g. MINOLTAF −90.5% vs. a recorded
1:10 SPLIT, GETALONG −90.0% vs. 1:10, LADDU −81.0% vs. 1:5). These are
instruments that simply don't trade every session — the "previous bar" our
pair is built from predates the action, and the "current bar" is the first
trade *after* it, which can land several sessions later than the announced
`ex_date`. `check_continuity`'s exact-date match doesn't allow for that.

**Fixed 2026-08-24** in `check_continuity` (`src/trading/reconcile.py`):
widened the corp-action lookup to `ex_date BETWEEN row_date - max_gap_days
AND row_date` (the same window already used to decide whether two bars are
close enough to compare at all), landed in the same change as Finding 1.
Test: `test_reconcile.py::test_a_corporate_action_a_few_days_before_an_illiquid_step_still_explains_it`,
with a companion test pinning that the window stays bounded
(`test_a_corporate_action_more_than_max_gap_days_before_a_step_does_not_explain_it`).

## Finding 3 — ETF and mutual-fund unit splits are never ingested

108 steps are AMC-issued ETF/index-fund units — NIFTYBEES, BANKBEES,
GOLDBEES, every HDFC/ICICI/Kotak/Aditya Birla/DSP/Groww/Quantum/Invesco
sector and asset ETF — stepping by almost exactly −90% (occasionally −80%),
**clustered by fund house and date**: five Nippon "BEES" ETFs all step on
2019-12-19, four HDFC ETFs on 2021-02-17 and again five more on 2023-10-20,
four Aditya Birla Sun Life ETFs on 2021-11-25, six more across two exchanges
on 2024-05-10, and so on. That clustering — the whole product suite of one
AMC moving together on one date — is the signature of a unit face-value
split, not a market move or a data defect. Checked directly:
`corporate_actions` has **zero rows, ever**, for `NIFTYBEES` (NSE,
`instrument_id=58974`) or `GOLDBEES` (NSE, `instrument_id=58933`). Neither
NSE's nor BSE's corporate-action feed as currently ingested (Task 16) covers
fund-unit actions at all — a genuine coverage gap, not a linking bug like
Findings 1–2.

This doesn't corrupt anything today (no code currently adjusts ETF prices
for splits), but it means a Phase 3 backtest holding NIFTYBEES/GOLDBEES/etc.
across one of these dates would see an unadjusted −90% "loss" that never
happened.

**Not fixed — blocked, not deferred by choice.** This needs a genuinely new
ingestion path (a fund-unit corporate-actions feed, or classifier coverage
for one), and the codebase's own standard for that is a live-verified sample
(exactly how Task 16 built the existing NSE/BSE equity parsers) — a guessed
endpoint or a guessed category filter "would fail silently against the real
feed, which is worse than not parsing it at all" (`ingest.py`'s own words).
Checked what's actually reachable from this environment: `nseindia.com`
refuses every request here (its anti-bot layer rejects a bare client with no
prior browser session — confirmed, not assumed); `bseindia.com` is reachable,
and `ddlcategorys=E` is the documented equity filter `bse.py` already uses,
but `ddlcategorys=MF` returns the *same* unfiltered 11MB equity feed `E`
returns in miniature — evidence the parameter is being ignored, not a real
fund-unit filter, so encoding it would be exactly the kind of guess the
codebase forbids. Recommend scoping this as its own task, run somewhere with
real network access to nseindia.com (or BSE documentation naming the correct
category code), same shape as Task 16 originally was — not attempted further
here.

## Real market events — verified, no defect

The remaining ~300 liquid steps are dominated by a short list of repeat
names whose extreme single-day moves are independently, publicly documented:
**IDEA** (20 occurrences — a sub-₹15 stock for most of the decade, where
routine rupee moves are large percentages), **YESBANK** (10 — 2018–2020
banking crisis and reconstruction), **RCOM/RPOWER/DHFL/JETAIRWAYS**
(insolvency-era penny stocks), **ADANIENT** (7 — the Jan–Feb 2023 Hindenburg
report fallout and the Nov 2024 US DOJ indictment news), **INDUSINDBK** (the
March 2025 derivatives-accounting-discrepancy disclosure crash, plus the
March 2020 COVID week), **ZEEL** (the Zee–Sony merger collapse saga),
**INFIBEAM** (the 2018 fraud-allegation crash), and a cluster of large-caps
all sharing the March 2020 COVID crash week or the June 2024 Lok Sabha
election-result day. One standout: **46 instruments — every silver ETF/fund
on both NSE and BSE — stepped down 20-31% together on 2026-02-02**, the
signature of a real, single-day commodity price shock, not a data defect (a
bad print does not hit every silver-tracking instrument on two exchanges
simultaneously with consistent magnitude).

## Illiquid / thin volume — same disposition as the spike review

~275 steps are single-digit-to-low-thousands volume prints on obscure
small-caps, several with symbol names ending in numeric SME-listing codes
(e.g. `948SCL26A`, `990SCL26`). Same reading as the spike review's
near-zero-volume bucket: real, recorded trades, just not liquid enough to
carry a meaningful signal.

## Why this check cannot find a wrong *price*

As with the spike review: `check_idempotency` already proves every stored
price matches the archived exchange bytes byte-for-byte. Nothing above
implies any price in `bars_daily` is incorrect — the defect (where one
exists) is in which `instrument_id` a real corporate action is filed under,
or whether a fund-unit action was ingested at all.

## Recommended disposition

- **Rights entitlements (747), real market events (~300), illiquid/thin
  (~275):** documented, justified exception — no action.
- **Finding 1 (series-migration instrument-id mismatch) and Finding 2
  (illiquid-name date offset): fixed 2026-08-24.** `adjustment_factors()`
  (`src/trading/corpactions/adjust.py`) and `check_continuity`
  (`src/trading/reconcile.py`) both now match a corporate action across the
  target instrument's whole identity group (shared ISIN/exchange/segment,
  deduplicated) and within `[ex_date, ex_date + max_gap_days]`. 5 new tests
  (2 in `tests/corpactions/test_adjust.py`, 3 in `tests/test_reconcile.py`),
  full suite green (440 passed), `ruff`/`mypy` clean. `step` fell 1,530 →
  1,390 on re-run against the full warehouse.
- **10-case residual (genuine ISIN change, e.g. AARTECH):** documented,
  justified exception — needs an explicit identity-transition record to
  resolve, not a bigger join; left alone rather than risking a symbol-only
  fallback that could conflate unrelated companies reusing a symbol.
- **Finding 3 (ETF/fund splits, 108, zero ingestion): not fixed, blocked.**
  No network access to `nseindia.com` from this environment, and BSE's
  category filter can't be determined without guessing (which this
  codebase's own ingestion code explicitly refuses to do). Needs a
  Task-16-shaped follow-up run somewhere with real access to verify the
  actual feed/format, live, before any code is written.
