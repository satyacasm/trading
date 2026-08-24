"""BSE corporate actions.

The continuity classifier showed ~878 of the 1,971 unexplained price steps
sit on BSE instruments -- and they could never be explained, because only
NSE's feed had ever been ingested. BSE publishes the same events in a
different shape, so it needs its own parser rather than a widened NSE one.

Every Purpose string here is copied verbatim from a live 2020 fetch.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from trading.contracts import InstrumentRef
from trading.corpactions.bse import parse_bse_corporate_actions
from trading.corpactions.ingest import ingest_corporate_actions
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db


def _seed_bse(conn, symbol: str, series: str = "A") -> int:
    resolver = DbInstrumentResolver()
    ref = InstrumentRef(exchange="BSE", segment="CM", symbol=symbol, series=series)
    return resolver.resolve({ref}, conn)[ref]


def _payload(*records: dict[str, object]) -> bytes:
    return json.dumps(list(records)).encode()


def _rec(symbol: str, purpose: str, ex: str = "02 Jan 2020") -> dict[str, object]:
    return {
        "scrip_code": 500001,
        "short_name": symbol,
        "long_name": f"{symbol} Limited",
        "Ex_date": ex,
        "RD_Date": ex,
        "Purpose": purpose,
    }


def test_bonus_issue_is_parsed(db_conn):
    _seed_bse(db_conn, "BSEBONUS")
    result = parse_bse_corporate_actions(_payload(_rec("BSEBONUS", "Bonus issue 1:1")), db_conn)
    assert len(result.rows) == 1
    row = result.rows[0]
    assert (row.action_type, row.ratio_from, row.ratio_to) == ("BONUS", Decimal(1), Decimal(2))
    assert row.ex_date == date(2020, 1, 2)


@pytest.mark.parametrize(
    ("purpose", "ratio_to"),
    [
        ("Stock  Split From Rs.10/- to Rs.5/-", Decimal(2)),
        ("Stock  Split From Rs.10/- to Rs.2/-", Decimal(5)),
        ("Stock Split From Rs.2/- to Rs.1/-", Decimal(2)),
    ],
)
def test_stock_split_spellings(db_conn, purpose: str, ratio_to: Decimal):
    """BSE writes "Stock" and "Split" with a doubled space in most rows."""
    _seed_bse(db_conn, "BSESPLIT")
    result = parse_bse_corporate_actions(_payload(_rec("BSESPLIT", purpose)), db_conn)
    assert len(result.rows) == 1
    assert result.rows[0].action_type == "SPLIT"
    assert result.rows[0].ratio_from == Decimal(1)
    assert result.rows[0].ratio_to == ratio_to


@pytest.mark.parametrize(
    ("purpose", "amount"),
    [
        ("Interim Dividend - Rs. - 1.0000", Decimal("1.0000")),
        ("Final Dividend - Rs. - 0.5000", Decimal("0.5000")),
        ("Dividend - Rs. - 2.5000", Decimal("2.5000")),
    ],
)
def test_dividend_spellings(db_conn, purpose: str, amount: Decimal):
    _seed_bse(db_conn, "BSEDIV")
    result = parse_bse_corporate_actions(_payload(_rec("BSEDIV", purpose)), db_conn)
    assert len(result.rows) == 1
    assert result.rows[0].action_type == "DIVIDEND"
    assert result.rows[0].amount == amount


@pytest.mark.parametrize("purpose", ["E.G.M.", "A.G.M.", "Buy Back of Shares"])
def test_non_price_events_are_skipped(db_conn, purpose: str):
    _seed_bse(db_conn, "BSENOOP")
    result = parse_bse_corporate_actions(_payload(_rec("BSENOOP", purpose)), db_conn)
    assert result.rows == []
    assert result.skipped == 1


def test_an_action_attaches_to_every_series_the_symbol_trades_in(db_conn):
    """A BSE company moves between series (A, B, T, XT) over a decade, and the
    corporate action belongs to the company, not to one series row."""
    first = _seed_bse(db_conn, "BSEMULTI", series="A")
    second = _seed_bse(db_conn, "BSEMULTI", series="B")

    result = parse_bse_corporate_actions(_payload(_rec("BSEMULTI", "Bonus issue 1:1")), db_conn)

    assert {r.instrument_id for r in result.rows} == {first, second}


def test_an_unknown_symbol_is_counted_never_created(db_conn):
    """Resolution looks up existing BSE instruments only. Creating one from a
    corporate-action feed would mint a phantom with a guessed series that no
    price row ever lands on."""
    before = db_conn.execute("SELECT count(*) FROM instruments").fetchone()
    result = parse_bse_corporate_actions(_payload(_rec("NOSUCHBSE", "Bonus issue 1:1")), db_conn)
    after = db_conn.execute("SELECT count(*) FROM instruments").fetchone()

    assert result.rows == []
    assert result.skipped == 1
    assert before == after


def test_parsed_rows_ingest_and_are_idempotent(db_conn):
    _seed_bse(db_conn, "BSEING")
    payload = _payload(_rec("BSEING", "Bonus issue 1:2"))

    first = ingest_corporate_actions(db_conn, parse_bse_corporate_actions(payload, db_conn).rows)
    ingest_corporate_actions(db_conn, parse_bse_corporate_actions(payload, db_conn).rows)

    assert first == 1
    stored = db_conn.execute(
        "SELECT count(*) FROM corporate_actions ca JOIN instruments i USING (instrument_id) "
        "WHERE i.symbol = 'BSEING'"
    ).fetchone()
    assert stored is not None and stored[0] == 1


# ---------------------------------------------------------------------------
# BSE spells several real price events differently from NSE, and they went
# unrecognised on the first run: 553 rights issues, 77 capital reductions and
# 52 share consolidations. None carries a ratio in the Purpose string, so each
# is recorded as the event it is, with no ratio invented.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("purpose", "action_type"),
    [
        ("Right Issue of Equity Shares", "RIGHTS"),
        ("Right Issue of Equity Shares with Warrants", "RIGHTS"),
        ("Reduction of Capital", "CAPITAL_REDUCTION"),
        ("Consolidation of Shares", "CONSOLIDATION"),
    ],
)
def test_bse_price_events_without_a_ratio(db_conn, purpose: str, action_type: str):
    """A consolidation is a reverse split -- fewer shares, higher price -- so
    it moves the price as surely as a split does."""
    _seed_bse(db_conn, "BSENORATIO")
    result = parse_bse_corporate_actions(_payload(_rec("BSENORATIO", purpose)), db_conn)

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.action_type == action_type
    assert (row.ratio_from, row.ratio_to, row.amount) == (None, None, None)


def test_a_rights_issue_with_a_ratio_still_keeps_it(db_conn):
    _seed_bse(db_conn, "BSERIGHTS")
    result = parse_bse_corporate_actions(_payload(_rec("BSERIGHTS", "Rights issue 2:5")), db_conn)
    row = result.rows[0]
    assert (row.action_type, row.ratio_from, row.ratio_to) == ("RIGHTS", Decimal(5), Decimal(7))


@pytest.mark.parametrize(
    "purpose",
    [
        # A unit-holder distribution from an InvIT/REIT, not a share dividend.
        "Income Distribution (InvIT)",
        "Income Distribution REITs",
        "InvIT - Return of Capital",
        # A trading-status event, not an action on the share count. It does
        # explain a price gap, but recording it as a corporate action would
        # conflate two different things.
        "Resolution Plan -Suspension",
        "Buy Back of Shares",
        "E.G.M.",
    ],
)
def test_bse_non_share_count_events_stay_unparsed(db_conn, purpose: str):
    _seed_bse(db_conn, "BSESKIP")
    result = parse_bse_corporate_actions(_payload(_rec("BSESKIP", purpose)), db_conn)
    assert result.rows == []
    assert result.skipped == 1
