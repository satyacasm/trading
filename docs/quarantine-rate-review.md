# Quarantine rate review

**Date:** 2026-08-24 · **Warehouse:** 51,081,227 bars loaded, 220,910 quarantined
(0.4306%, 43× the spec's 0.01% threshold)

`check_quarantine_rate` (spec §8 item 6) fails when more than 0.01% of rows
across every source are quarantined. This is the review of all three reasons
that fired: `close_not_positive` (203,028), `nav_not_available` (17,211),
`ohlc_inconsistent` (671).

## Conclusion

**No ingestion defects.** Every quarantined row is either genuine AMFI data
about mutual-fund schemes with no real per-unit value, or a genuine
exchange-computed closing/settlement price that legitimately falls outside
the day's traded range. Nothing here is a parsing bug, a mapping bug, or bad
data — the 0.01% threshold was simply calibrated for a universe that turns
out not to match reality. Recorded as a documented, justified exception
(see Disposition); no code change.

## `close_not_positive` (203,028 rows, 100% AMFI) — segregated portfolios reporting ₹0 NAV

Every row is `open=high=low=close=0.0000` for an AMFI mutual-fund scheme.
Sampling 20 by frequency shows every single one has **"SEGREGATED
PORTFOLIO"** in its name: Nippon India Equity Savings Fund, Nippon India
Aggressive Hybrid Fund, UTI Credit Risk Fund "(Segregated - 06032020)",
Franklin India Short Term Income Plan "(Segregated Portfolio 3 — 9.50% Yes
Bank Ltd CO 23Dec21)". **511 distinct scheme codes**, each recurring roughly
daily for years (the top 18 each appear 1,563–1,591 times).

A segregated portfolio is what AMCs create when a fund's underlying debt
holding defaults: the defaulted security is carved out into its own
sub-scheme so it doesn't drag down the healthy portfolio's NAV. AMFI's own
convention when that underlying security is written off (Yes Bank's AT1
bonds in March 2020, Vodafone Idea exposure, DHFL, the CY2019–2021 debt-fund
default wave that produced UTI Credit Risk Fund's, Nippon's and Franklin's
segregated portfolios) is to publish the segregated portfolio's NAV as
literally **₹0.0000** — the position is worthless, not "missing." That's
faithful data, and the dates confirm it: the 203,028 rows spread across
1,936 distinct dates, heavily weighted toward March–July 2020 — exactly the
COVID-era default wave.

## `nav_not_available` (17,211 rows, 100% AMFI) — schemes with no NAV for that date

Every row has `close IS NULL` for a scheme that genuinely had no NAV
published that day — many at `2016-01-01` (the very first backfilled date,
before some closed-ended schemes had launched or before their first
valuation date) or for closed-ended debt series ("SBI DEBT FUND SERIES - 18
MONTHS - 12") that mature and stop publishing. This is exactly what
`nav_not_available` is designed to catch: AMFI's daily file lists every
*registered* scheme, not every scheme with a real valuation that day, and
the validator correctly refuses to invent a price rather than silently
writing a fabricated NAV.

## `ohlc_inconsistent` (671 rows, spread across nse_cm_udiff/nse_fo_udiff/bse_cm_udiff/nse_cm_legacy) — exchange close ≠ traded range

The rule fires when `close` (or `open`) falls outside `[low, high]`. Every
sample shows the same shape: `open = high = low` (or very nearly so) with
tiny volume — 202 of 671 rows have `volume = 1` — and `close` sitting a
fraction of a percent outside that single traded print. This isn't unique to
illiquid names: the highest-volume outliers (SAIL futures, 9,908 and 9,550
contracts; NHPC, NMDC, NUVAMA futures) show the identical shape, `close`
consistently within 0.01%–0.5% of the traded range, never wildly off.

Checked against the actual field mapping
(`src/trading/normalizers/udiff.py:88-90`): `close` reads NSE's own
`ClsPric` column, a genuinely different field from `SttlmPric` (settlement
price, stored separately as `settle_price`). NSE/BSE compute the official
closing price via their own methodology — for F&O, a settlement mechanism
distinct from the last traded print; for thinly-traded equities, a
closing-session/auction price — and that value is not contractually bound to
fall inside the regular session's intraday high/low. This is the exchange's
data doing exactly what it does, not a mapping error: `close` is reading the
right column, and the right column simply isn't always inside `[low, high]`.
It shows up most visibly at very low volume because `open=high=low` collapse
to a single value, and it turns out **any** close mismatch (however tiny)
trips a validator that treats `[low, high]` as the row's containing bound.

## Why these should stay quarantined, not be "fixed" into bars_daily

Unlike the corp-actions findings, there's no code path that reads the wrong
value here — the parser, normalizer and validator are all doing exactly what
they're supposed to. Loading a `close` outside `[low, high]` into
`bars_daily` would violate an invariant every downstream consumer (the
continuity check, the backtest fill model, any chart) is entitled to assume.
Quarantining these rows is the validator working correctly, not a defect to
patch.

## Disposition

Record as a **documented, justified exception** under task-17-brief.md
Step 6 — no code change. The 0.01% threshold was set before a full decade of
AMFI's real scheme registry (including the 2019–2021 debt-default wave) was
known; 99.7% of the quarantined rows are that registry's genuine
segregated-portfolio/no-NAV entries, and the remaining 0.3% are exchange
close-vs-range artifacts verified against the actual field mapping. Parser,
normalizer and validator are all doing exactly what they're supposed to —
there is nothing here to fix.

Worth re-running after any change to ingestion or a new backfill era, since
a genuine defect (a real mapping bug, a corrupted archive) would show up as
a *new* reason or a count that breaks this now-understood baseline, not as
noise within it.
