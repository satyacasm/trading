from datetime import date
from decimal import Decimal

import pytest

from trading.corpactions.ingest import CorporateActionRow, ingest_corporate_actions

pytestmark = pytest.mark.db


def _row(iid: int, *, ratio_to: str = "5", source: str = "test") -> CorporateActionRow:
    return CorporateActionRow(
        instrument_id=iid,
        action_type="SPLIT",
        ex_date=date(2026, 8, 12),
        record_date=None,
        ratio_from=Decimal("1"),
        ratio_to=Decimal(ratio_to),
        amount=None,
        new_symbol=None,
        announced_at=None,
        source=source,
        raw={"subject": "test"},
    )


def test_ingest_writes_a_row_and_returns_the_count(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    assert ingest_corporate_actions(db_conn, [_row(iid)]) == 1
    count = db_conn.execute(
        "SELECT count(*) FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert count == 1


def test_repeated_ingest_of_the_same_action_does_not_duplicate(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    ingest_corporate_actions(db_conn, [_row(iid)])
    ingest_corporate_actions(db_conn, [_row(iid)])
    count = db_conn.execute(
        "SELECT count(*) FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert count == 1


def test_a_changed_ratio_to_creates_a_distinct_row(db_conn, seeded_instrument):
    """ratio_to participates in uq_corp_action's expression: a restated
    ratio is a different action row, not an update to the old one."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    ingest_corporate_actions(db_conn, [_row(iid, ratio_to="5")])
    ingest_corporate_actions(db_conn, [_row(iid, ratio_to="4")])
    count = db_conn.execute(
        "SELECT count(*) FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert count == 2


def test_restating_a_non_key_field_updates_in_place(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    ingest_corporate_actions(db_conn, [_row(iid, source="first")])
    ingest_corporate_actions(db_conn, [_row(iid, source="restated")])
    row = db_conn.execute(
        "SELECT source FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()
    assert row[0] == "restated"


def test_two_conflicting_rows_in_one_batch_are_deduped(db_conn, seeded_instrument):
    """Postgres rejects ON CONFLICT DO UPDATE touching the same arbiter row
    twice in one command; ingest_corporate_actions must dedupe first,
    keeping the last occurrence."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    written = ingest_corporate_actions(db_conn, [_row(iid, source="a"), _row(iid, source="b")])
    assert written == 1
    row = db_conn.execute(
        "SELECT source FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()
    assert row[0] == "b"


def test_empty_rows_writes_nothing(db_conn):
    assert ingest_corporate_actions(db_conn, []) == 0
