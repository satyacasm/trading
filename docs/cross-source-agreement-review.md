# Cross-source agreement review

**Date:** 2026-08-24 · **Warehouse:** 328,187 NSE stock-future rows carrying
`underlying_price`, compared against the NSE CM close on the same session
(spec §8 item 3, substituted per Ruling B5 for the unavailable NIFTY-spot
comparison)

`check_cross_source_agreement` fails when a future's stored `underlying_price`
disagrees with the NSE CM close by more than 0.5% (tolerance chosen from a
few days' sample, per the check's own docstring; "should be re-derived from
the full distribution once the backfill completes" — this is that
re-derivation). Full backfill produced 1,027 violations.

## Conclusion

**One real bug, fixed. Six genuine, benign outliers left, matching the
check's own already-documented rounding pattern.** The full distribution
made the shape obvious immediately: 99% of all 328,187 comparisons sit under
0.03% disagreement, but a distinct second cluster — 292 rows — sat above 5%,
topping out at 89%. That's not rounding noise; that was a bug.

## The bug: the CM join had no `series` filter

NSE's CM segment carries more than one instrument under the same `symbol`.
Sampling the worst offender, M&MFIN:

```
NSE/CM/M&MFIN instruments (all asset_class = EQUITY):
  instrument_id=57976  series=EQ  isin=INE774D01024   <- the actual equity
  instrument_id=139859 series=BL  isin=INE774D01024   <- block-deal reporting
  instrument_id=134112 series=N1  isin=INE774D08LU6   <- a listed NCD/bond
  instrument_id=134220 series=N2  isin=INE774D08MA6   <- a listed NCD/bond
  instrument_id=102161 series=N3  isin=INE774D08MG3   <- a listed NCD/bond
```

`BL` reports that day's block-deal trades under the *same* ISIN as the
equity but as a separate line with its own O/H/L/C — the same series
`check_continuity`'s own docstring already distrusts for `prev_close`
("METROPOLIS's block-deal row carried prev_close 1944.00 against a 564.00
close"). `N1`/`N2`/`N3` are genuinely different securities — listed NCDs —
that happen to reuse the equity's symbol string with a *different* ISIN
entirely, both incorrectly carrying `asset_class = 'EQUITY'` in the
instrument master (a separate, smaller classification issue not touched
here).

The join filtered on `cm.asset_class = 'EQUITY'` but not `cm.series`, so it
compared the future's `underlying_price` against whichever of these five
rows happened to have a bar on that session — for M&MFIN, that was
routinely the `N3` bond, trading around ₹250 against the real equity's
~₹2,100–2,280. Breaking the 1,027 violations down by which CM row they
actually joined against confirms it completely:

| CM series matched | Violations | What it is |
|---|---:|---|
| `BL` | 744 | block-deal reporting line, same ISIN, different session window |
| `N3` | 277 | a listed NCD/bond, different ISIN, different security entirely |
| `EQ` | 6 | the real equity — see below |

1,021 of 1,027 "disagreements" were never disagreements about the equity's
price at all; they were the check accidentally pricing a bond against a
stock.

**Fixed 2026-08-24** in `check_cross_source_agreement`
(`src/trading/reconcile.py`): the CM join now also requires `cm.series =
'EQ'` — the same canonical equity-series marker `parse_nse_corporate_actions`
already pins for the identical reason (Ruling S1, task-18-brief.md). Test:
`tests/test_reconcile.py::test_cross_source_agreement_ignores_a_non_equity_series_sharing_the_symbol`.
Re-run against the full warehouse: violations dropped from 1,027/328,187 to
**6/326,543** (the small drop in the denominator is the `BL`/`N3` rows no
longer entering the comparison at all, which is correct — they were never
the equity).

## The remaining 6 rows — the same rounding pattern the check already documents

All six are two events (each counted three times, once per expiry contract
trading that underlying that session): `ICICIBANK` on 2026-08-06
(underlying=1466.22, CM close=1457.50, 0.598%) and `ICICIPRULI` on
2026-06-29 (underlying=492.40, CM close=489.50, 0.592%). Both sit barely
above the 0.5% line — the same "`UndrlygPric` is a snapshot, not the CM
close, and the gap grows with price" phenomenon the check's own docstring
already documents and calibrates around (ABB at 0.2bp, 360ONE at 5.3bp,
PREMIERENE at 19.8bp). These two just landed a little further out on that
same distribution than the sample the original 50bp tolerance was drawn
from.

## Disposition

Record as a **documented, justified exception** under task-17-brief.md
Step 6 for the residual 6 rows — no further code change. The fix above
already resolved the one real defect (1,021 of 1,027 violations); the
remainder is the exact benign pattern the check exists to tolerate, just
two events past the current threshold. Worth revisiting only if a much
larger cluster of similar near-miss rounding gaps appears in a future
backfill era — at 6 rows out of 326,543 (0.0018%), there's no case for
retuning the tolerance on this evidence alone.
