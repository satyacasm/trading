from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import DataSource, assert_canonical
from trading.normalizers.amfi import AmfiNormalizer
from trading.parsers.amfi import AmfiNavParser
from trading.parsers.amfi_history import AmfiNavHistoryParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "amfi"


def _batch(fixture: str, source_key: str, parser, business_date: date = date(2026, 8, 13)):
    payload = make_payload(FIXTURES / fixture, source_key, business_date)
    return AmfiNormalizer().normalize(parser.parse(payload), payload)


def test_output_matches_the_canonical_contract_for_both_formats():
    assert_canonical(_batch("navall.txt", "amfi_nav", AmfiNavParser()).frame)
    assert_canonical(_batch("navhistory.txt", "amfi_nav_history", AmfiNavHistoryParser()).frame)


def test_asset_class_and_identity_mapping():
    frame = _batch("navall.txt", "amfi_nav", AmfiNavParser()).frame
    assert set(frame["asset_class"].unique()) == {"MF"}
    assert set(frame["exchange"].unique()) == {"AMFI"}
    assert set(frame["segment"].unique()) == {"MF"}
    assert frame["expiry"].null_count() == frame.height
    assert frame["strike"].null_count() == frame.height


def test_close_equals_nav_and_ohlc_collapse_to_it():
    """Golden row (scheme_code 119551) verified against the parser's fixture."""
    frame = _batch("navall.txt", "amfi_nav", AmfiNavParser()).frame
    row = frame.row(0, named=True)
    assert row["symbol"] == "119551"
    assert row["close"] == Decimal("107.2564")
    assert row["open"] == row["high"] == row["low"] == row["close"]
    assert row["isin"] == "INF209KA12Z1"
    assert row["name"].startswith("Aditya Birla Sun Life Banking & PSU Debt Fund")


def test_both_source_keys_map_to_the_same_data_source():
    """Ruling N2: one normalizer, one DataSource, for both AMFI parsers."""
    assert _batch("navall.txt", "amfi_nav", AmfiNavParser()).source is DataSource.AMFI_NAV
    assert (
        _batch("navhistory.txt", "amfi_nav_history", AmfiNavHistoryParser()).source
        is DataSource.AMFI_NAV
    )


def test_ts_is_built_per_row_from_nav_date_not_business_date():
    """Ruling N3: the latest-snapshot fixture is NOT single-dated.

    tests/fixtures/amfi/navall.txt carries rows at 13-Aug-2026, 14-Jun-2017 and
    18-May-2015. Every row's `ts` must come from its OWN nav_date, at the same
    15:30 IST -> UTC session-close convention, and must NOT be overwritten by
    payload.business_date.
    """
    frame = _batch("navall.txt", "amfi_nav", AmfiNavParser(), business_date=date(2026, 8, 13)).frame
    ts_dates = set(frame["ts"].dt.date().unique().to_list())
    assert {date(2026, 8, 13), date(2017, 6, 14), date(2015, 5, 18)} <= ts_dates

    stale = frame.filter(frame["ts"].dt.date() == date(2015, 5, 18))
    assert stale.height > 0
    assert stale["ts"][0] == datetime.fromisoformat("2015-05-18T10:00:00+00:00")


def test_nav_history_ts_matches_golden_date():
    frame = _batch("navhistory.txt", "amfi_nav_history", AmfiNavHistoryParser()).frame
    row = frame.row(0, named=True)
    assert row["symbol"] == "120373"
    assert row["ts"] == datetime.fromisoformat("2019-03-14T10:00:00+00:00")
    assert row["close"] == Decimal("74.2258")


def test_na_nav_yields_null_close_not_zero_or_dropped():
    """A `nav` of `N.A.` normalizes to a null close (quarantined downstream).

    Built by overwriting one row's nav on a fixture-parsed frame; the
    committed fixture itself is never modified.
    """
    payload = make_payload(FIXTURES / "navall.txt", "amfi_nav", date(2026, 8, 13))
    frame = AmfiNavParser().parse(payload)
    before_height = frame.height
    poisoned = frame.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("N.A."))
        .otherwise(pl.col("nav"))
        .alias("nav")
    )
    out = AmfiNormalizer().normalize(poisoned, payload).frame
    assert out.height == before_height  # never dropped
    assert out["close"][0] is None
    assert out["open"][0] is None


def test_isin_coalesce_drops_reinvest_isin_when_both_present():
    """Ruling N5 (task-10-fix-1.md, binding): pin the deliberate ISIN drop.

    Scheme code 119551 in the committed fixture carries TWO distinct, both-
    populated, real ISINs (isin_growth=INF209KA12Z1, isin_reinvest=INF209KA13Z9)
    -- the normal shape for an IDCW scheme, not an edge case. The parser layer
    must expose both; the normalizer must keep only isin_growth and drop
    isin_reinvest with no trace. If this ever silently changes -- e.g. the
    coalesce order flips, or a future edit tries to "recover" the second ISIN
    -- this test must fail.
    """
    payload = make_payload(FIXTURES / "navall.txt", "amfi_nav", date(2026, 8, 13))
    parsed = AmfiNavParser().parse(payload)
    raw_row = parsed.filter(pl.col("scheme_code") == "119551").row(0, named=True)
    assert raw_row["isin_growth"] == "INF209KA12Z1"
    assert raw_row["isin_reinvest"] == "INF209KA13Z9"
    assert raw_row["isin_growth"] != raw_row["isin_reinvest"]  # both real, both distinct

    normalized = AmfiNormalizer().normalize(parsed, payload).frame
    row = normalized.filter(pl.col("symbol") == "119551").row(0, named=True)
    assert row["isin"] == "INF209KA12Z1"  # isin_reinvest (INF209KA13Z9) is dropped


def test_unrecognised_source_key_raises_with_key_in_message():
    payload = make_payload(FIXTURES / "navall.txt", "not_a_real_source", date(2026, 8, 13))
    frame = AmfiNavParser().parse(payload)
    with pytest.raises(ValueError, match="not_a_real_source"):
        AmfiNormalizer().normalize(frame, payload)
