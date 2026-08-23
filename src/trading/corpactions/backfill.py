"""Walk NSE's corporate-actions endpoint across a date range and ingest it.

Task 16 built the parser and the read-time adjustment but no way to actually
fetch history, so `corporate_actions` stayed empty and the reconcile
continuity check found 5,672 price moves beyond 20% that nothing could
explain -- a share split reads as a catastrophic loss to any backtest that
crosses it.

NSE's endpoint accepts `from_date`/`to_date` (verified live: 2,208 records
for 2020, 388 for Q1 2016, 29 for January 2020), so a decade is reachable one
window at a time rather than only as "what is coming up next week".

Run for real with:
    uv run python -m trading.corpactions --from 2016-01-01 --to 2026-08-21
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date

import psycopg
import structlog

from trading.config import get_settings
from trading.corpactions.ingest import (
    NSE_CORPORATE_ACTIONS_URL,
    ingest_corporate_actions,
    parse_nse_corporate_actions,
)
from trading.resolver.instruments import DbInstrumentResolver

log = structlog.get_logger(__name__)

# The listing page that sets the cookies the API requires; without it NSE
# answers the JSON endpoint with an HTML challenge.
NSE_PRIME_URL = "https://www.nseindia.com/companies-listing/corporate-filings-actions"

Fetch = Callable[[date, date], bytes | None]


@dataclass(frozen=True)
class BackfillResult:
    windows_fetched: int
    windows_failed: int
    ingested: int
    skipped: int

    def __str__(self) -> str:
        failed = (
            f", {self.windows_failed} window(s) returned nothing" if self.windows_failed else ""
        )
        return (
            f"{self.ingested} corporate action(s) ingested from "
            f"{self.windows_fetched} window(s), {self.skipped} subject(s) not recognised{failed}"
        )


def date_windows(start: date, end: date, *, years: int = 1) -> list[tuple[date, date]]:
    """Split a range into per-year windows, clipped to `end`.

    Whole calendar years rather than rolling spans so a resumed run re-fetches
    the same windows and the archived responses line up with what they cover.
    """
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        year_end = date(cursor.year + years - 1, 12, 31)
        windows.append((cursor, min(year_end, end)))
        cursor = date(cursor.year + years, 1, 1)
    return windows


def backfill_corporate_actions(
    conn: psycopg.Connection,
    fetch: Fetch,
    start: date,
    end: date,
    *,
    resolver: DbInstrumentResolver | None = None,
    on_window: Callable[[date, date, int, int], None] | None = None,
) -> BackfillResult:
    """Fetch, parse and upsert every window in `[start, end]`.

    A window that returns nothing is counted and skipped rather than aborting
    the run: one unavailable year should not cost the other ten.
    """
    _resolver = resolver if resolver is not None else DbInstrumentResolver()
    fetched = failed = ingested = skipped = 0

    for window_start, window_end in date_windows(start, end):
        payload = fetch(window_start, window_end)
        if payload is None:
            failed += 1
            log.warning("corpactions.window_empty", start=window_start, end=window_end)
            continue
        parsed = parse_nse_corporate_actions(payload, _resolver, conn)
        written = ingest_corporate_actions(conn, parsed.rows) if parsed.rows else 0
        fetched += 1
        ingested += written
        skipped += parsed.skipped
        if on_window is not None:
            on_window(window_start, window_end, written, parsed.skipped)

    return BackfillResult(fetched, failed, ingested, skipped)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _nse_fetch(delay: float) -> Fetch:
    from trading.sources.http import ArchivingClient

    client = ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(start: date, end: date) -> bytes | None:
        url = f"{NSE_CORPORATE_ACTIONS_URL}&from_date={start:%d-%m-%Y}&to_date={end:%d-%m-%Y}"
        name = f"nse_corporate_actions/{start.isoformat()}_{end.isoformat()}.json"
        result = client.get(url, archive_name=name, prime=NSE_PRIME_URL)
        time.sleep(delay)
        return result[0] if result is not None else None

    return fetch


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill NSE corporate actions.")
    parser.add_argument("--from", dest="start", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    windows = date_windows(args.start, args.end)
    if args.dry_run:
        print(f"DRY RUN: {len(windows)} window(s) would be fetched")
        for w_start, w_end in windows:
            print(f"  {w_start} .. {w_end}")
        return 0

    def progress(w_start: date, w_end: date, written: int, skipped: int) -> None:
        print(f"  {w_start} .. {w_end}: {written} ingested, {skipped} skipped", flush=True)

    with psycopg.connect(get_settings().database_url, autocommit=False) as conn:
        result = backfill_corporate_actions(
            conn, _nse_fetch(args.delay), args.start, args.end, on_window=progress
        )
        conn.commit()
    print(result)
    return 1 if result.windows_failed else 0


if __name__ == "__main__":
    sys.exit(main())
