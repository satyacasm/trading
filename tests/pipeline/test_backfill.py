from datetime import date

import pytest

pytestmark = pytest.mark.db


def test_missing_days_excludes_completed_and_holidays(db_conn, udiff_backfill):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(
        db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 14), holidays={date(2026, 8, 12)}
    )
    assert udiff_backfill.missing_days(db_conn, date(2026, 8, 10), date(2026, 8, 14)) == [
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 13),
        date(2026, 8, 14),
    ]


def test_a_completed_day_is_not_repeated(db_conn, udiff_backfill):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 13), date(2026, 8, 13), set())
    udiff_backfill.run(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert udiff_backfill.missing_days(db_conn, date(2026, 8, 13), date(2026, 8, 13)) == []
