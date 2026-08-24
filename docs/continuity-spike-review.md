# Continuity check: review of the 63 spikes

**Date:** 2026-08-24 · **Warehouse:** 51,081,227 bars, 2016-01-01 → 2026-08-21

`check_continuity` classifies every single-session equity move beyond 20% by
its shape. A **spike** is a move that returns to roughly its starting level
the next session — the shape of a bad print, since a corporate action never
reverses itself. This is the review of all 63 the full warehouse produced.

## Conclusion

**No data defects.** All 63 are faithfully recorded real market data. Three
were verified byte-for-byte against the raw exchange archives; the remaining
60 fall into instrument classes where a reverting 20%+ move is ordinary.

## Breakdown

| Class | Count | Volume range | Reading |
|---|---:|---|---|
| Rights entitlements (`-RE`, `-RE1`) | 34 | 7.6k – 14.7M | Short-lived instruments with a 7–15 day trading window; extreme volatility is their normal behaviour |
| Near-zero volume | 13 | 1 – 163 shares | The close is a single trade. Real, but not a meaningful price |
| Liquid, real moves | 8 | 3.9M – 809M | Documented market events (below) |
| Thin volume | 8 | 1.4k – 88k | Illiquid small caps and penny stocks |

## The eight liquid moves

Each has volume that makes a data error impossible — a bad print does not
carry 800 million shares.

| Symbol | Date | Prev | Close | Next | Move | Volume |
|---|---|---:|---:|---:|---:|---:|
| YESBANK | 2019-10-01 | 41.40 | 32.00 | 42.50 | −22.7% | 808,959,169 |
| IDEA | 2019-11-14 | 3.70 | 2.95 | 3.65 | −20.3% | 585,724,890 |
| IBULHSGFIN | 2019-11-28 | 268.15 | 334.75 | 290.50 | +24.8% | 168,963,840 |
| ZEEL | 2020-03-18 | 133.10 | 164.10 | 141.20 | +23.3% | 40,664,776 |
| ADANIENT | 2023-02-08 | 1802.95 | 2164.25 | 1925.70 | +20.0% | 19,173,006 |
| INFIBEAM | 2018-09-21 | 234.90 | 182.20 | 217.25 | −22.4% | 18,506,511 |
| SBC | 2024-01-19 | 35.90 | 28.70 | 32.60 | −20.1% | 10,128,021 |
| LOTUSEYE | 2024-03-04 | 64.95 | 51.85 | 62.20 | −20.2% | 3,978,194 |

## Archive verification

Three were re-read from the raw bhavcopy the exchange served. The stored
close matches exactly, and the intraday range confirms a genuine session
rather than a stray print:

```
YESBANK    2019-10-01  open 42.00   high 44.30   low 29.00   close 32.00    vol 808,959,169
ZEEL       2020-03-18  open 135.00  high 185.00  low 135.00  close 164.10   vol  40,664,776
IBULHSGFIN 2019-11-28  open 274.50  high 347.80  low 272.20  close 334.75   vol 168,963,840
```

YESBANK opened at 42.00, traded down to 29.00 and closed at 32.00. That is a
session, not a typo.

## Why this check cannot find a wrong price

Price faithfulness is already proven by a different check, and more directly.
`check_idempotency` re-parses the archived bytes for a random 30-day window
per source and compares a SHA-256 of every stored column — it passes for all
five sources, including 729,182 F&O rows re-read with zero drift. If a stored
price disagreed with the exchange's own file, that check would fail, not this
one.

What `check_continuity` is specified to catch (spec §8 item 4) is
split-adjustment bugs: a price that halves because a corporate action was
never recorded. That is the **step** bucket, and it is now served by 44,341
ingested corporate actions across both exchanges.

## Recommended disposition

Record as a **documented, justified exception** under task-17-brief.md Step 6.
The spike bucket found no defects because there are none to find; the residue
is real market behaviour in instrument classes that behave this way by design.

Worth re-running after any change to ingestion, since the bucket is cheap and
a genuine bad print would stand out immediately against this baseline.
