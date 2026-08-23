"""Pins `parse_nse_corporate_actions` against real records captured from a
live GET against `NSE_CORPORATE_ACTIONS_URL` on 2026-08-20 -- see
docs/data-formats/eod-source-formats.md §5. No network is used here; these
are literal, verbatim-shaped records, not invented ones.
"""

import json
from datetime import date
from decimal import Decimal

import pytest

from trading.corpactions.ingest import parse_nse_corporate_actions
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db

_SAMPLE = json.dumps(
    [
        {
            "bcEndDate": "-",
            "bcStartDate": "-",
            "caBroadcastDate": None,
            "comp": "Best Agrolife Limited",
            "exDate": "16-Jan-2026",
            "faceVal": "1",
            "ind": "-",
            "isin": "INE052T01013",
            "ndEndDate": "-",
            "ndStartDate": "-",
            "recDate": "16-Jan-2026",
            "series": "EQ",
            "subject": "Bonus 1:2",
            "symbol": "BESTAGRO",
        },
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
        },
        {
            "bcEndDate": "-",
            "bcStartDate": "-",
            "caBroadcastDate": None,
            "comp": "SKM Egg Products Export (India) Limited",
            "exDate": "12-Jan-2026",
            "faceVal": "5",
            "ind": "-",
            "isin": "INE411D01015",
            "ndEndDate": "-",
            "ndStartDate": "-",
            "recDate": "12-Jan-2026",
            "series": "EQ",
            "subject": (
                "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share"
            ),
            "symbol": "SKMEGGPROD",
        },
        {
            "bcEndDate": "-",
            "bcStartDate": "-",
            "caBroadcastDate": None,
            "comp": "National Aluminium Company Limited",
            "exDate": "24-Aug-2026",
            "faceVal": "5",
            "ind": "-",
            "isin": "INE139A01026",
            "ndEndDate": "-",
            "ndStartDate": "-",
            "recDate": "24-Aug-2026",
            "series": "EQ",
            "subject": "Dividend - Re 1 Per Share",
            "symbol": "NATIONALUM",
        },
        {
            "bcEndDate": "-",
            "bcStartDate": "-",
            "caBroadcastDate": None,
            "comp": "Siyaram Silk Mills Limited",
            "exDate": "21-Aug-2026",
            "faceVal": "2",
            "ind": "-",
            "isin": "INE076B01010",
            "ndEndDate": "-",
            "ndStartDate": "-",
            "recDate": "22-Aug-2026",
            "series": "EQ",
            "subject": "Scheme Of Arrangement - Bonus Ncrps 4:1",
            "symbol": "SIYSIL",
        },
    ]
).encode()


def test_parses_the_recognised_subject_patterns(db_conn):
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    by_symbol = {r.raw["symbol"]: r for r in result.rows}
    assert by_symbol["BESTAGRO"].action_type == "BONUS"
    assert by_symbol["BESTAGRO"].ratio_from == Decimal("2")
    assert by_symbol["BESTAGRO"].ratio_to == Decimal("3")
    assert by_symbol["NATIONALUM"].action_type == "DIVIDEND"
    assert by_symbol["NATIONALUM"].amount == Decimal("1")


def test_split_ratios_are_stored_canonically_reduced_to_lowest_terms(db_conn):
    """Ruling A7 (task-16 fix round 1): the canonical share-count ratio,
    reduced to lowest terms -- not the raw face values. `ratio_to`
    participates in `uq_corp_action`'s uniqueness expression, so storing the
    unreduced face-value pair would let the same real-world split entered
    once here and once canonically by another source hold two rows and be
    applied twice. Pinned against two real subject strings from the live
    sample.
    """
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    by_symbol = {r.raw["symbol"]: r for r in result.rows}
    # "From Rs 10/- To Rs 2/-" -> (2, 10) unreduced -> (1, 5) canonical.
    assert by_symbol["MCX"].action_type == "SPLIT"
    assert by_symbol["MCX"].ratio_from == Decimal("1")
    assert by_symbol["MCX"].ratio_to == Decimal("5")
    # "From Rs 10/- To Rs 5/-" -> (5, 10) unreduced -> (1, 2) canonical.
    assert by_symbol["SKMEGGPROD"].action_type == "SPLIT"
    assert by_symbol["SKMEGGPROD"].ratio_from == Decimal("1")
    assert by_symbol["SKMEGGPROD"].ratio_to == Decimal("2")


def test_an_unrecognised_subject_is_skipped_not_guessed(db_conn):
    """'Scheme Of Arrangement - Bonus Ncrps 4:1' is not a plain equity bonus
    and this parser has not been shown a verified example of what its ratio
    means -- it must be skipped, not guessed."""
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    assert result.skipped == 1
    assert {r.raw["symbol"] for r in result.rows} == {
        "BESTAGRO",
        "MCX",
        "SKMEGGPROD",
        "NATIONALUM",
    }


def test_ex_dates_are_parsed_from_dd_mon_yyyy(db_conn):
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    by_symbol = {r.raw["symbol"]: r for r in result.rows}
    assert by_symbol["MCX"].ex_date == date(2026, 1, 2)
    assert by_symbol["NATIONALUM"].ex_date == date(2026, 8, 24)


def test_announced_at_is_null_when_ca_broadcast_date_is_null(db_conn):
    """Every record in the live sample carried `caBroadcastDate: null`."""
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    assert all(r.announced_at is None for r in result.rows)


def test_empty_feed_parses_to_nothing(db_conn):
    result = parse_nse_corporate_actions(b"[]", DbInstrumentResolver(), db_conn)
    assert result.rows == []
    assert result.skipped == 0
