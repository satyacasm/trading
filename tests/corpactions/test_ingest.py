import json
from datetime import date
from decimal import Decimal

import pytest

from trading.corpactions.ingest import (
    CorporateActionRow,
    ingest_corporate_actions,
    parse_nse_corporate_actions,
)
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db


def _row(
    iid: int,
    *,
    ratio_from: str = "1",
    ratio_to: str = "5",
    ex_date: date = date(2026, 8, 12),
    source: str = "test",
) -> CorporateActionRow:
    return CorporateActionRow(
        instrument_id=iid,
        action_type="SPLIT",
        ex_date=ex_date,
        record_date=None,
        ratio_from=Decimal(ratio_from),
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


def test_a_parsed_split_collides_with_its_canonical_equivalent(db_conn, seeded_instrument):
    """Ruling A7 (task-16 fix round 1): `parse_nse_corporate_actions` stores
    "From Rs 10/- To Rs 2/-" as the canonical (1, 5), not the raw face
    values (2, 10) -- so the same split entered a second time in the
    canonical form the rest of the system uses must collide with it, not
    hold a second row that gets applied twice.
    """
    iid = seeded_instrument(closes={date(2026, 8, 11): 500}, symbol="MCX")
    sample = json.dumps(
        [
            {
                "bcEndDate": "-",
                "bcStartDate": "-",
                "caBroadcastDate": None,
                "comp": "Multi Commodity Exchange of India Limited",
                "exDate": "02-Jan-2026",
                "faceVal": "2",
                "ind": "-",
                "isin": "INE745G01035",
                "ndEndDate": "-",
                "ndStartDate": "-",
                "recDate": "02-Jan-2026",
                "series": "EQ",
                "subject": (
                    "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share"
                ),
                "symbol": "MCX",
            }
        ]
    ).encode()
    parsed = parse_nse_corporate_actions(sample, DbInstrumentResolver(), db_conn)
    assert ingest_corporate_actions(db_conn, parsed.rows) == 1

    # Same split, same instrument, same ex_date -- but expressed directly in
    # the canonical (1, 5) form a second source (or a manual correction)
    # would use.
    ingest_corporate_actions(
        db_conn, [_row(iid, ratio_from="1", ratio_to="5", ex_date=date(2026, 1, 2))]
    )

    count = db_conn.execute(
        "SELECT count(*) FROM corporate_actions WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert count == 1
