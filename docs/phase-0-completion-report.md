# Phase 0 completion report

Generated: 2026-08-24T06:06:28.304587+00:00

## Verification checks (spec §8)

| # | Check | Status | Detail |
|---|-------|--------|--------|
| 1 | calendar_completeness | FAIL | nse_cm_udiff: 1 gap(s), e.g. 2026-08-24; nse_fo_udiff: 1 gap(s), e.g. 2026-08-24; bse_cm_udiff: 1 gap(s), e.g. 2026-08-24; amfi_nav_history: 1 gap(s), e.g. 2026-08-24 |
| 2 | known_values | PASS | 12 hand-verified value(s) matched |
| 3 | cross_source_agreement | FAIL | 1027/328187 stock-future row(s) disagree with the NSE CM close by more than 0.50% (substituted per Ruling B5 for the unavailable NIFTY-spot comparison -- see task-17-report.md for the empirical finding that NSE's own UndrlygPric does not always equal the CM close): 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ADANIENSOL 2025-11-18 underlying=1026.7000 cm_close=1021.5500 (+1017 more) |
| 4 | continuity | FAIL | 8269341 pair(s) examined; 5672 beyond 20% (2997 tick, 1222 explained, 1390 step, 63 spike): NSE/CM/AARTECH 2024-08-09: -65.0% (step); NSE/CM/LOTUSEYE 2024-03-04: -20.2% then back (spike); NSE/CM/TATSILV 2026-02-02: -26.9% (step); NSE/CM/GRPLTD 2024-05-21: +25.9% (step); NSE/CM/SILVERAG 2026-02-02: -28.6% (step); NSE/CM/SEMAC 2024-05-23: -30.5% (step); NSE/CM/GIRIRAJ 2023-11-03: -79.0% (step); NSE/CM/SILVER1 2026-02-02: -23.3% (step); NSE/CM/SILVER1 2026-02-27: -89.7% (step); NSE/CM/PSUBANK 2026-07-10: -89.6% (step) (+1443 more) |
| 5 | idempotency[nse_cm_udiff] | PASS | re-loading 17 day(s) (57028 row(s)) in [2026-03-14, 2026-04-12] changed nothing |
| 6 | idempotency[nse_fo_udiff] | PASS | re-loading 18 day(s) (862124 row(s)) in [2026-03-13, 2026-04-11] changed nothing |
| 7 | idempotency[bse_cm_udiff] | PASS | re-loading 22 day(s) (99583 row(s)) in [2025-05-21, 2025-06-19] changed nothing |
| 8 | idempotency[nse_cm_legacy] | PASS | re-loading 22 day(s) (35883 row(s)) in [2016-05-16, 2016-06-14] changed nothing |
| 9 | idempotency[amfi_nav_history] | PASS | re-loading 21 day(s) (212924 row(s)) in [2019-06-03, 2019-07-02] changed nothing |
| 10 | quarantine_rate | FAIL | 220910/51302137 row(s) quarantined (0.4306%, threshold 0.0100%); reasons: close_not_positive=203028, nav_not_available=17211, ohlc_inconsistent=671 |
| 11 | recorder_liveness | NOT_APPLICABLE | no recorder session manifests found under data/recordings: the Upstox WebSocket recorder (Task 15) has never run in this environment because Upstox credentials do not exist yet (Ruling B4, task-17-addendum.md). This check cannot pass or fail until the recorder has run at least one session; it must never be counted as a pass. |

**6 passed, 4 failed, 1 not applicable, out of 11.**

A FAIL must be resolved as a bug fix or a documented, justified exception before Phase 0 is considered complete (task-17-brief.md Step 6). A NOT_APPLICABLE is never counted as a pass (task-17-addendum.md Ruling B4).

## Capacity (Ruling B7)

- Disk free: 207.8 GB of 494.3 GB total
- `data/raw` size: 4361.6 MB
- `bars_daily` rows: 51,081,227
- `instruments` rows: 585,273
- `ingest_jobs` rows: 6,330
