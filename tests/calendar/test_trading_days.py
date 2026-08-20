from datetime import date

import pytest

from trading.calendar.trading_days import is_trading_day, seed_calendar, trading_days

pytestmark = pytest.mark.db


def test_weekends_are_not_trading_days(db_conn):
    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), holidays=set())
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 14)) is True  # Friday
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 15)) is False  # Saturday


def test_listed_holiday_is_not_a_trading_day(db_conn):
    seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays={date(2026, 8, 13)},
    )
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 13)) is False


def test_trading_days_returns_only_open_sessions_in_order(db_conn):
    seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays={date(2026, 8, 13)},
    )
    days = trading_days(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16))
    assert days == [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 14)]


def test_seeding_is_idempotent(db_conn):
    first = seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), set())
    second = seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), set())
    assert first == second
    # Scoped to the seeded range rather than an unconditional table count: the
    # brief's literal query assumed NSE/CM starts empty, which stops holding
    # once the real ten-year seed (task 9 addendum step 5) has been loaded.
    count = db_conn.execute(
        "SELECT count(*) FROM trading_calendar WHERE exchange='NSE' AND segment='CM' "
        "AND session_date BETWEEN %s AND %s",
        (date(2026, 8, 10), date(2026, 8, 16)),
    ).fetchone()[0]
    assert count == 7


# --- Task 9 addendum, Ruling H1: special sessions override weekday/holiday status ---


def test_muhurat_session_is_a_trading_day_despite_being_a_listed_holiday(db_conn):
    diwali = date(2026, 8, 13)  # a weekday, treated as a holiday below
    seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays={diwali},
        special_sessions={diwali},
    )
    assert is_trading_day(db_conn, "NSE", "CM", diwali) is True


def test_weekend_special_session_is_a_trading_day(db_conn):
    saturday = date(2026, 8, 15)
    seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays=set(),
        special_sessions={saturday},
    )
    assert is_trading_day(db_conn, "NSE", "CM", saturday) is True


def test_reseeding_does_not_flip_a_special_session(db_conn):
    saturday = date(2026, 8, 15)
    first = seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays=set(),
        special_sessions={saturday},
    )
    second = seed_calendar(
        db_conn,
        "NSE",
        "CM",
        date(2026, 8, 10),
        date(2026, 8, 16),
        holidays=set(),
        special_sessions={saturday},
    )
    assert first == second
    assert is_trading_day(db_conn, "NSE", "CM", saturday) is True
