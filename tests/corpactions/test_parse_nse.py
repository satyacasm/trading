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
    by_type = {r.action_type: r for r in result.rows}
    assert by_type["BONUS"].ratio_from == Decimal("2")
    assert by_type["BONUS"].ratio_to == Decimal("3")
    assert by_type["SPLIT"].ratio_from == Decimal("2")
    assert by_type["SPLIT"].ratio_to == Decimal("10")
    assert by_type["DIVIDEND"].amount == Decimal("1")


def test_an_unrecognised_subject_is_skipped_not_guessed(db_conn):
    """'Scheme Of Arrangement - Bonus Ncrps 4:1' is not a plain equity bonus
    and this parser has not been shown a verified example of what its ratio
    means -- it must be skipped, not guessed."""
    result = parse_nse_corporate_actions(_SAMPLE, DbInstrumentResolver(), db_conn)
    assert result.skipped == 1
    assert {r.raw["symbol"] for r in result.rows} == {"BESTAGRO", "MCX", "NATIONALUM"}


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
