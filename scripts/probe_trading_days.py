#!/usr/bin/env python3
"""Re-verification probe for the trading calendar seed CSVs.

Probes every date in [--from, --to] against the live NSE archive, using the
same two-source method as the original Task 9 verification (see
`.superpowers/sdd/2026-08-14-phase-0-data-foundations/task-9-addendum.md`,
Ruling H2): `NseUdiffSource("cm")` first, falling back to `NseLegacyCmSource`
if that returns None, ~1s spacing between requests. Prints one
`date,classification` line per date, sorted, where classification is
`holiday` (both sources returned None) or `session` (either source returned
bytes) — diff this against `data/seed/nse_holidays.csv` and
`data/seed/nse_special_sessions.csv` to re-check any row marked `unverified`
once its date is in the past (see the `# MAINTENANCE:` note in both CSVs).

This is a standalone maintenance script, not a package module: nothing at
runtime imports it, and it does not import any calendar library
(`exchange_calendars` / `pandas_market_calendars`) — only the Task 5 fetch
sources, so it needs no dev-only extras to run.

Usage:
    uv run python scripts/probe_trading_days.py --from 2026-09-01 --to 2026-12-31
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

from trading.sources.nse_legacy import NseLegacyCmSource
from trading.sources.nse_udiff import NseUdiffSource

SLEEP_SECONDS = 1.0


def probe_range(
    start: date, end: date, *, sleep_seconds: float = SLEEP_SECONDS
) -> list[tuple[date, str]]:
    """Probe every date in [start, end] and classify it as 'holiday' or 'session'."""
    udiff = NseUdiffSource("cm")
    legacy = NseLegacyCmSource()
    results: list[tuple[date, str]] = []
    day = start
    while day <= end:
        udiff_result = udiff.fetch(day)
        time.sleep(sleep_seconds)
        if udiff_result is not None:
            results.append((day, "session"))
        else:
            legacy_result = legacy.fetch(day)
            time.sleep(sleep_seconds)
            results.append((day, "session" if legacy_result is not None else "holiday"))
        day += timedelta(days=1)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-probe the live NSE archive for a date range and classify each date."
    )
    parser.add_argument("--from", dest="start", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    if args.start > args.end:
        print("error: --from must not be after --to", file=sys.stderr)
        raise SystemExit(2)

    for day, classification in probe_range(args.start, args.end):
        print(f"{day.isoformat()},{classification}")


if __name__ == "__main__":
    main()
