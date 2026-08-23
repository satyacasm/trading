# Phase 0 completion report

Generated: 2026-08-23T20:10:44.068410+00:00

## Verification checks (spec §8)

| # | Check | Status | Detail |
|---|-------|--------|--------|
| 1 | calendar_completeness | FAIL | nse_cm_udiff: 1 gap(s), e.g. 2026-08-24; nse_fo_udiff: 1 gap(s), e.g. 2026-08-24; bse_cm_udiff: 1 gap(s), e.g. 2026-08-24; amfi_nav_history: 1 gap(s), e.g. 2026-08-24 |
| 2 | known_values | PASS | 12 hand-verified value(s) matched |
| 3 | cross_source_agreement | FAIL | 1027/328187 stock-future row(s) disagree with the NSE CM close by more than 0.50% (substituted per Ruling B5 for the unavailable NIFTY-spot comparison -- see task-17-report.md for the empirical finding that NSE's own UndrlygPric does not always equal the CM close): 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; 360ONE 2025-10-27 underlying=1173.0000 cm_close=1179.7000; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-06-11 underlying=246.2500 cm_close=242.6500; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ABCAPITAL 2025-10-28 underlying=312.6000 cm_close=308.0000; ADANIENSOL 2025-11-18 underlying=1026.7000 cm_close=1021.5500 (+1017 more) |
| 4 | continuity | FAIL | 5672/8269341 examined pair(s) move beyond 20% with no matching corporate action: NSE/CM/AARTECH 2024-08-09: -65.0% (no matching corporate action); NSE/CM/ONEPOINT 2022-01-18: -86.0% (no matching corporate action); NSE/CM/LOTUSEYE 2024-03-04: -20.2% (no matching corporate action); NSE/CM/EMAMILTD 2018-06-21: -50.6% (no matching corporate action); NSE/CM/TTKPRESTIG 2021-12-14: -90.2% (no matching corporate action); NSE/CM/TATSILV 2026-02-02: -26.9% (no matching corporate action); NSE/CM/DANGEE 2022-09-06: -93.1% (no matching corporate action); NSE/CM/GRPLTD 2024-05-21: +25.9% (no matching corporate action); NSE/CM/ELGIEQUIP 2020-09-24: -49.4% (no matching corporate action); NSE/CM/SILVERAG 2026-02-02: -28.6% (no matching corporate action) (+5662 more) |
| 5 | idempotency[nse_cm_udiff] | PASS | re-loading 19 day(s) (62334 row(s)) in [2026-01-11, 2026-02-09] changed nothing |
| 6 | idempotency[nse_fo_udiff] | PASS | re-loading 21 day(s) (758884 row(s)) in [2025-04-21, 2025-05-20] changed nothing |
| 7 | idempotency[bse_cm_udiff] | PASS | re-loading 21 day(s) (92516 row(s)) in [2024-07-16, 2024-08-14] changed nothing |
| 8 | idempotency[nse_cm_legacy] | PASS | re-loading 22 day(s) (53984 row(s)) in [2023-07-06, 2023-08-04] changed nothing |
| 9 | idempotency[amfi_nav_history] | PASS | re-loading 20 day(s) (181888 row(s)) in [2018-11-01, 2018-11-30] changed nothing |
| 10 | quarantine_rate | FAIL | 220910/51302137 row(s) quarantined (0.4306%, threshold 0.0100%); reasons: close_not_positive=203028, nav_not_available=17211, ohlc_inconsistent=671 |
| 11 | recorder_liveness | NOT_APPLICABLE | no recorder session manifests found under data/recordings: the Upstox WebSocket recorder (Task 15) has never run in this environment because Upstox credentials do not exist yet (Ruling B4, task-17-addendum.md). This check cannot pass or fail until the recorder has run at least one session; it must never be counted as a pass. |

**6 passed, 4 failed, 1 not applicable, out of 11.**

A FAIL must be resolved as a bug fix or a documented, justified exception before Phase 0 is considered complete (task-17-brief.md Step 6). A NOT_APPLICABLE is never counted as a pass (task-17-addendum.md Ruling B4).

## Capacity (Ruling B7)

- Disk free: 215.1 GB of 494.3 GB total
- `data/raw` size: 4347.9 MB
- `bars_daily` rows: 51,081,227
- `instruments` rows: 585,266
- `ingest_jobs` rows: 6,330
