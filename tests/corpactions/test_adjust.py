from datetime import date
from decimal import Decimal

import pytest

from trading.corpactions.adjust import adjusted_bars

pytestmark = pytest.mark.db


def test_a_split_scales_prices_before_the_ex_date(db_conn, seeded_instrument):
    """1:5 split on 2026-08-12 → the 08-11 close of 500 becomes 100."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500, date(2026, 8, 13): 100})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 10), date(2026, 8, 14), as_of=date(2026, 8, 14)
    )
    by_date = {r["ts"].date(): r["close"] for r in frame.to_dicts()}
    assert by_date[date(2026, 8, 11)] == Decimal("100.0000")
    assert by_date[date(2026, 8, 13)] == Decimal("100.0000")


def test_bars_after_the_ex_date_are_untouched(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 13): 100})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 13), date(2026, 8, 13), as_of=date(2026, 8, 14)
    )
    assert frame["close"][0] == Decimal("100.0000")


def test_volume_scales_inversely_to_price(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 11): 500}, volume=1000)
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 11), date(2026, 8, 11), as_of=date(2026, 8, 14)
    )
    assert frame["volume"][0] == 5000


def test_an_action_announced_after_as_of_is_ignored(db_conn, seeded_instrument):
    """Point-in-time: a backtest must not know about a split before announcement."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, ratio_from,"
        " ratio_to, announced_at, source)"
        " VALUES (%s,'SPLIT','2026-08-12',1,5,'2026-08-20T00:00:00Z','test')",
        (iid,),
    )
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 11), date(2026, 8, 11), as_of=date(2026, 8, 14)
    )
    assert frame["close"][0] == Decimal("500.0000")


def test_unadjusted_prices_remain_in_storage(db_conn, seeded_instrument):
    """D10: adjustment is a read-time view, never a rewrite."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    stored = db_conn.execute(
        "SELECT close FROM bars_daily WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert stored == Decimal("500.0000")


def test_query_is_immune_to_session_timezone(db_conn, seeded_instrument):
    """Ruling A2 (task-16 addendum): `ts::date BETWEEN ...` would shift a
    10:00Z bar to the previous calendar day under a session zone west of
    UTC by more than ten hours. Pin that the half-open `ts` range does not.
    """
    iid = seeded_instrument(closes={date(2026, 8, 11): 500, date(2026, 8, 13): 100})
    db_conn.execute("SET LOCAL TimeZone = 'America/Los_Angeles'")
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 10), date(2026, 8, 14), as_of=date(2026, 8, 14)
    )
    seen_dates = {r["ts"].date() for r in frame.to_dicts()}
    assert seen_dates == {date(2026, 8, 11), date(2026, 8, 13)}
