"""CLI to seed the trading calendar for every (exchange, segment) pair the
backfill pipeline needs, from the committed holiday/special-session CSVs.

Usage: uv run python -m trading.calendar.seed [--from YYYY-MM-DD] [--to YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import psycopg

from trading.calendar.trading_days import seed_calendar
from trading.config import get_settings

# Task 9 addendum, Ruling H3: is_trading_day() raises on an unseeded date, and
# the pipeline covers NSE CM, NSE FO and BSE CM, so all three pairs are seeded
# from the same NSE-sourced holiday/special-session lists. BSE publishes the
# same trading-holiday list as NSE; a BSE-specific divergence (this module
# does not probe BSE's own archive) would surface in Task 17's reconciliation,
# which is the right place to catch it.
CALENDAR_PAIRS: tuple[tuple[str, str], ...] = (
    ("NSE", "CM"),
    ("NSE", "FO"),
    ("BSE", "CM"),
)

DEFAULT_START = date(2016, 1, 1)
DEFAULT_END = date(2026, 12, 31)

SEED_ROOT = Path(__file__).resolve().parents[3] / "data" / "seed"
HOLIDAYS_CSV = SEED_ROOT / "nse_holidays.csv"
SPECIAL_SESSIONS_CSV = SEED_ROOT / "nse_special_sessions.csv"

# Date the Task 9 addendum verification probe (and its Ruling H5 follow-up,
# task-9-fix-1.md) ran. Every holiday row dated after this in the CSVs above
# was not checked against the live archive and must be marked `unverified`;
# see tests/calendar/test_seed_csvs.py, which pins that invariant.
PROBE_DATE = date(2026, 8, 20)


def _load_dates(path: Path) -> set[date]:
    """Parse a `YYYY-MM-DD,description` CSV, skipping `#`-prefixed provenance lines."""
    out: set[date] = set()
    for raw_line in path.read_text().splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        ymd = stripped.split(",", 1)[0]
        out.add(date.fromisoformat(ymd))
    return out


def load_holidays(path: Path = HOLIDAYS_CSV) -> set[date]:
    return _load_dates(path)


def load_special_sessions(path: Path = SPECIAL_SESSIONS_CSV) -> set[date]:
    return _load_dates(path)


def seed_all(
    conn: psycopg.Connection,
    start: date,
    end: date,
    holidays: set[date],
    special_sessions: set[date],
) -> dict[tuple[str, str], int]:
    """Seed every pair in CALENDAR_PAIRS over [start, end]; returns rows written per pair."""
    return {
        (exchange, segment): seed_calendar(
            conn, exchange, segment, start, end, holidays, special_sessions
        )
        for exchange, segment in CALENDAR_PAIRS
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the trading calendar.")
    parser.add_argument("--from", dest="start", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, default=DEFAULT_END)
    args = parser.parse_args()

    holidays = load_holidays()
    special_sessions = load_special_sessions()

    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        counts = seed_all(conn, args.start, args.end, holidays, special_sessions)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    for (exchange, segment), n in counts.items():
        print(f"{exchange}/{segment}: {n} rows")


if __name__ == "__main__":
    main()
