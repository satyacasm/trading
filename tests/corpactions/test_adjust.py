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


def test_a_split_recorded_against_a_sibling_series_still_adjusts_the_bars(
    db_conn, seeded_instrument
):
    """A stock migrating trading series (EQ<->BE, a routine surveillance
    event) around its own split date ends up with two `instrument_id` rows
    sharing one ISIN. NSE's/BSE's feed attaches the action to whichever one
    it resolves against -- confirmed live for AARTECH and PCJEWELLER
    (docs/continuity-step-review.md, Finding 1) the action can land on the
    *other* series' row, not the one whose bars actually need adjusting.
    `adjustment_factors` must still find it.
    """
    live_iid = seeded_instrument(closes={date(2026, 8, 11): 500}, symbol="SIBADJ")
    sibling_iid = db_conn.execute(
        "INSERT INTO instruments (exchange, segment, symbol, series, asset_class, status, "
        "canonical_key) VALUES ('NSE','CM','SIBADJ','BE','EQUITY','ACTIVE',"
        "'test_sibling_sibadj_be') RETURNING instrument_id"
    ).fetchone()[0]
    db_conn.execute(
        "UPDATE instruments SET isin='INE_SIBLING_ADJ_TEST' WHERE instrument_id IN (%s,%s)",
        (live_iid, sibling_iid),
    )
    # The SPLIT lands on the sibling instrument_id, not the one with the bars.
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (sibling_iid,),
    )

    frame = adjusted_bars(
        db_conn, live_iid, date(2026, 8, 11), date(2026, 8, 11), as_of=date(2026, 8, 14)
    )
    assert frame["close"][0] == Decimal("100.0000")


def test_a_duplicate_action_across_sibling_series_is_applied_only_once(db_conn, seeded_instrument):
    """Live-verified (PCJEWELLER, docs/continuity-step-review.md Finding 1):
    the same real-world SPLIT can be ingested once per sibling instrument_id
    (BSE alone carried three identical rows for PC Jeweller's 1:10 split).
    Widening the lookup to every sibling must not multiply the factor.
    """
    live_iid = seeded_instrument(closes={date(2026, 8, 11): 500}, symbol="SIBDUP")
    sibling_iid = db_conn.execute(
        "INSERT INTO instruments (exchange, segment, symbol, series, asset_class, status, "
        "canonical_key) VALUES ('NSE','CM','SIBDUP','BE','EQUITY','ACTIVE',"
        "'test_sibling_sibdup_be') RETURNING instrument_id"
    ).fetchone()[0]
    db_conn.execute(
        "UPDATE instruments SET isin='INE_SIBLING_DUP_TEST' WHERE instrument_id IN (%s,%s)",
        (live_iid, sibling_iid),
    )
    for iid in (live_iid, sibling_iid):
        db_conn.execute(
            "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
            " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
            (iid,),
        )

    frame = adjusted_bars(
        db_conn, live_iid, date(2026, 8, 11), date(2026, 8, 11), as_of=date(2026, 8, 14)
    )
    # 500 / 5 = 100 -- if the duplicate were double-applied it would be 20.
    assert frame["close"][0] == Decimal("100.0000")


def test_announced_at_is_also_immune_to_session_timezone(db_conn, seeded_instrument):
    """Ruling A6 (task-16 fix round 1): `announced_at::date` had the exact
    session-timezone defect Ruling A2 already fixed on `ts` -- left on its
    sibling column in the same query. An action announced
    2026-08-15T02:00:00Z, queried as_of=2026-08-14 (strictly before the
    announcement in UTC), must stay invisible under every session timezone,
    not just UTC -- under `America/Los_Angeles` the old `::date` cast made
    it VISIBLE instead, leaking a split the market had not yet announced.
    """
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, ratio_from,"
        " ratio_to, announced_at, source)"
        " VALUES (%s,'SPLIT','2026-08-12',1,5,'2026-08-15T02:00:00Z','test')",
        (iid,),
    )
    db_conn.execute("SET LOCAL TimeZone = 'America/Los_Angeles'")
    frame = adjusted_bars(
        db_conn, iid, date(2026, 8, 11), date(2026, 8, 11), as_of=date(2026, 8, 14)
    )
    assert frame["close"][0] == Decimal("500.0000")
