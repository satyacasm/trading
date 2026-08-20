from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import DataSource, assert_canonical
from trading.normalizers.nse_legacy import NseLegacyNormalizer
from trading.parsers.nse_legacy import NseLegacyCmParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "nse_legacy"


def _batch(fixture: str, source_key: str, business_date: date = date(2019, 3, 14)):
    payload = make_payload(FIXTURES / fixture, source_key, business_date)
    return NseLegacyNormalizer().normalize(NseLegacyCmParser().parse(payload), payload)


def test_output_matches_the_canonical_contract():
    assert_canonical(_batch("cm_legacy.zip", "nse_cm_legacy").frame)


def test_all_rows_map_to_equity_asset_class():
    frame = _batch("cm_legacy.zip", "nse_cm_legacy").frame
    assert set(frame["asset_class"].unique()) == {"EQUITY"}
    assert frame["expiry"].null_count() == frame.height
    assert frame["strike"].null_count() == frame.height
    assert frame["option_type"].null_count() == frame.height


def test_exchange_and_segment_are_literal():
    frame = _batch("cm_legacy.zip", "nse_cm_legacy").frame
    assert set(frame["exchange"].unique()) == {"NSE"}
    assert set(frame["segment"].unique()) == {"CM"}


def test_ts_is_session_close_in_utc_derived_from_uppercase_month():
    """TIMESTAMP is `14-MAR-2019`; %b parses the uppercase month (finding N4)."""
    frame = _batch("cm_legacy.zip", "nse_cm_legacy").frame
    assert frame["ts"][0] == datetime.fromisoformat("2019-03-14T10:00:00+00:00")
    assert frame["ts"].n_unique() == 1  # one bhavcopy, one trading day


def test_fields_absent_from_legacy_format_are_null():
    frame = _batch("cm_legacy.zip", "nse_cm_legacy").frame
    for column in (
        "open_interest",
        "oi_change",
        "settle_price",
        "underlying_price",
        "lot_size",
        "delivery_qty",
        "delivery_pct",
        "tick_size",
        "name",
    ):
        assert frame[column].null_count() == frame.height, column


def test_source_is_nse_cm_legacy():
    assert _batch("cm_legacy.zip", "nse_cm_legacy").source is DataSource.NSE_CM_LEGACY


def test_unrecognised_source_key_raises_with_key_in_message():
    payload = make_payload(FIXTURES / "cm_legacy.zip", "wrong_key", date(2019, 3, 14))
    frame = NseLegacyCmParser().parse(payload)
    with pytest.raises(ValueError, match="wrong_key"):
        NseLegacyNormalizer().normalize(frame, payload)
