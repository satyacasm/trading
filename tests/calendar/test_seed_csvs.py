"""Pins task-9-fix-1.md's Ruling H5 against regression: reads the committed
holiday CSV only (no network, no DB) and checks that every future-dated
holiday row is honestly marked `unverified`, and that no `unverified` row
sits in the Diwali/Muhurat window where the addendum's own evidence (ten
for ten probed years) says a special session is likely.
"""

from __future__ import annotations

from datetime import date

from trading.calendar.seed import HOLIDAYS_CSV, PROBE_DATE

MUHURAT_WINDOW_START = (10, 15)  # 15 October
MUHURAT_WINDOW_END = (11, 20)  # 20 November


def _in_muhurat_window(d: date) -> bool:
    return MUHURAT_WINDOW_START <= (d.month, d.day) <= MUHURAT_WINDOW_END


def _read_holiday_rows() -> list[tuple[date, str]]:
    rows = []
    for raw_line in HOLIDAYS_CSV.read_text().splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        ymd, _, description = stripped.partition(",")
        rows.append((date.fromisoformat(ymd), description))
    return rows


def test_future_holiday_rows_are_marked_unverified():
    rows = _read_holiday_rows()
    assert rows, "expected at least one holiday row"
    for d, description in rows:
        if d > PROBE_DATE:
            assert "unverified" in description, (
                f"{d} is after the probe date {PROBE_DATE} but its description "
                f"does not say 'unverified': {description!r}"
            )


def test_no_unverified_holiday_falls_in_muhurat_window():
    rows = _read_holiday_rows()
    unverified_in_window = [
        d for d, description in rows if "unverified" in description and _in_muhurat_window(d)
    ]
    assert unverified_in_window == [], (
        "unverified holiday rows must not fall in the Muhurat window "
        f"(15 Oct - 20 Nov): {unverified_in_window}"
    )
