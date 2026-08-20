from __future__ import annotations

from datetime import date, timedelta

from psycopg import Connection

SESSION_OPEN = "09:15"
SESSION_CLOSE = "15:30"


def seed_calendar(
    conn: Connection,
    exchange: str,
    segment: str,
    start: date,
    end: date,
    holidays: set[date],
    special_sessions: frozenset[date] | set[date] = frozenset(),
) -> int:
    """Insert one row per calendar day in range. Idempotent.

    A date in `special_sessions` is a trading day regardless of weekday or
    holiday membership (Muhurat sessions, weekend budget/special sessions),
    and wins over `holidays` if a date is in both (Ruling H1, task 9 addendum).
    """
    rows = []
    day = start
    while day <= end:
        is_special = day in special_sessions
        is_holiday = day.weekday() < 5 and day in holidays and not is_special
        is_open = is_special or (day.weekday() < 5 and day not in holidays)
        note = "special_session" if is_special else ("holiday" if is_holiday else None)
        rows.append(
            (
                exchange,
                segment,
                day,
                is_open,
                SESSION_OPEN if is_open else None,
                SESSION_CLOSE if is_open else None,
                note,
            )
        )
        day += timedelta(days=1)

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO trading_calendar "
            "(exchange, segment, session_date, is_trading_day, session_open, session_close, note) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (exchange, segment, session_date) DO UPDATE SET "
            "is_trading_day = EXCLUDED.is_trading_day, note = EXCLUDED.note",
            rows,
        )
    return len(rows)


def is_trading_day(conn: Connection, exchange: str, segment: str, d: date) -> bool:
    row = conn.execute(
        "SELECT is_trading_day FROM trading_calendar "
        "WHERE exchange=%s AND segment=%s AND session_date=%s",
        (exchange, segment, d),
    ).fetchone()
    if row is None:
        raise LookupError(f"calendar has no entry for {exchange}/{segment} {d}")
    return bool(row[0])


def trading_days(
    conn: Connection, exchange: str, segment: str, start: date, end: date
) -> list[date]:
    rows = conn.execute(
        "SELECT session_date FROM trading_calendar "
        "WHERE exchange=%s AND segment=%s AND session_date BETWEEN %s AND %s "
        "AND is_trading_day ORDER BY session_date",
        (exchange, segment, start, end),
    ).fetchall()
    return [r[0] for r in rows]
