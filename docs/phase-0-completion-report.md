# Phase 0 completion report

Generated: 2026-08-23T18:28:10.276895+00:00

## Verification checks (spec §8)

| # | Check | Status | Detail |
|---|-------|--------|--------|
| 1 | calendar_completeness | FAIL | nse_cm_udiff: 527 gap(s), e.g. 2024-07-01, 2024-07-02, 2024-07-03, 2024-07-04, 2024-07-05, 2024-07-08, 2024-07-09, 2024-07-10, 2024-07-11, 2024-07-12 (+517 more); nse_fo_udiff: 529 gap(s), e.g. 2024-07-01, 2024-07-02, 2024-07-03, 2024-07-04, 2024-07-05, 2024-07-08, 2024-07-09, 2024-07-10, 2024-07-11, 2024-07-12 (+519 more); bse_cm_udiff: 532 gap(s), e.g. 2024-07-01, 2024-07-02, 2024-07-03, 2024-07-04, 2024-07-05, 2024-07-08, 2024-07-09, 2024-07-10, 2024-07-11, 2024-07-12 (+522 more); nse_cm_legacy: 2101 gap(s), e.g. 2016-01-01, 2016-01-04, 2016-01-05, 2016-01-06, 2016-01-07, 2016-01-08, 2016-01-11, 2016-01-12, 2016-01-13, 2016-01-14 (+2091 more); amfi_nav_history: 2633 gap(s), e.g. 2016-01-01, 2016-01-04, 2016-01-05, 2016-01-06, 2016-01-07, 2016-01-08, 2016-01-11, 2016-01-12, 2016-01-13, 2016-01-14 (+2623 more) |
| 2 | known_values | FAIL | NSE/CM/RELIANCE 2016-10-30 (close): not found in database (expected 1051.2, source data/raw/nse_cm_legacy/2016/10/2016-10-30.zip); NSE/CM/RELIANCE 2017-10-19 (close): not found in database (expected 909.9, source data/raw/nse_cm_legacy/2017/10/2017-10-19.zip); NSE/CM/RELIANCE 2018-11-07 (close): not found in database (expected 1110.7, source data/raw/nse_cm_legacy/2018/11/2018-11-07.zip); NSE/CM/RELIANCE 2019-10-27 (close): not found in database (expected 1434.25, source data/raw/nse_cm_legacy/2019/10/2019-10-27.zip); NSE/CM/RELIANCE 2020-02-01 (close): not found in database (expected 1383.35, source data/raw/nse_cm_legacy/2020/02/2020-02-01.zip); NSE/CM/RELIANCE 2024-01-20 (close): not found in database (expected 2713.30, source data/raw/nse_cm_udiff/2024/01/2024-01-20.zip); NSE/CM/RELIANCE 2024-11-01 (close): not found in database (expected 1338.65, source data/raw/nse_cm_udiff/2024/11/2024-11-01.zip); NSE/CM/RELIANCE 2025-02-01 (close): not found in database (expected 1264.60, source data/raw/nse_cm_udiff/2025/02/2025-02-01.zip); NSE/CM/RELIANCE 2025-10-21 (close): not found in database (expected 1465.20, source data/raw/nse_cm_udiff/2025/10/2025-10-21.zip); BSE/CM/RELIANCE 2026-08-13 (close): not found in database (expected 1316.45, source data/raw/_recon/bse_cm_udiff_20260813.csv) |
| 3 | cross_source_agreement | PASS | 1866 stock-future/underlying pair(s) agree within 0.50% |
| 4 | continuity | PASS | 13539 bar pair(s) examined in [2016-01-01, 2026-08-23]; 0 move(s) beyond 20%, all matched to a corporate action |
| 5 | idempotency[nse_cm_udiff] | PASS | re-loading 5 day(s) (17494 row(s)) in [2026-08-10, 2026-08-14] changed nothing |
| 6 | idempotency[nse_fo_udiff] | PASS | re-loading 3 day(s) (104434 row(s)) in [2026-08-12, 2026-08-14] changed nothing |
| 7 | idempotency[bse_cm_udiff] | NOT_APPLICABLE | no completed bse_cm_udiff job to sample a window from |
| 8 | idempotency[nse_cm_legacy] | NOT_APPLICABLE | no completed nse_cm_legacy job to sample a window from |
| 9 | idempotency[amfi_nav_history] | NOT_APPLICABLE | no completed amfi_nav_history job to sample a window from |
| 10 | quarantine_rate | PASS | 0/121928 row(s) quarantined (0.0000%, threshold 0.0100%); reasons: none |
| 11 | recorder_liveness | NOT_APPLICABLE | no recorder session manifests found under data/recordings: the Upstox WebSocket recorder (Task 15) has never run in this environment because Upstox credentials do not exist yet (Ruling B4, task-17-addendum.md). This check cannot pass or fail until the recorder has run at least one session; it must never be counted as a pass. |

**5 passed, 2 failed, 4 not applicable, out of 11.**

A FAIL must be resolved as a bug fix or a documented, justified exception before Phase 0 is considered complete (task-17-brief.md Step 6). A NOT_APPLICABLE is never counted as a pass (task-17-addendum.md Ruling B4).

## Capacity (Ruling B7)

- Disk free: 238.6 GB of 494.3 GB total
- `data/raw` size: 16.7 MB
- `bars_daily` rows: 121,928
- `instruments` rows: 39,118
- `ingest_jobs` rows: 8
