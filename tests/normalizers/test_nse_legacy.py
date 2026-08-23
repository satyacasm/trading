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


# --- Task 18, Ruling S1: SERIES carries through as `series` ---


def test_series_is_carried_through_from_the_series_column():
    """The fixture carries EQ, BE, SM and BZ rows -- confirm SERIES maps
    straight through rather than being dropped (task-17-report.md finding
    F4, fixed by Ruling S1)."""
    frame = _batch("cm_legacy.zip", "nse_cm_legacy").frame
    by_symbol = dict(zip(frame["symbol"], frame["series"], strict=True))
    assert by_symbol["20MICRONS"] == "EQ"
    assert by_symbol["A2ZINFRA"] == "BE"
    assert by_symbol["AAKASH"] == "SM"
    assert frame["series"].null_count() == 0


# ---------------------------------------------------------------------------
# NSE served 2020-07-13's bhavcopy with a TWO-DIGIT year in TIMESTAMP
# ("13-Jul-20") inside a file named cm13JUL2020bhav.csv, while every other day
# uses "13-JUL-2020". polars' "%d-%b-%Y" parses "20" as the year 20 AD without
# complaint even under strict=True, so that day silently landed 2,001 rows at
# 0020-07-13 in the live warehouse -- found by min(ts) reading year 0020.
# ---------------------------------------------------------------------------

_LEGACY_HEADER = (
    "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,"
    "TIMESTAMP,TOTALTRADES,ISIN"
)


def _legacy_zip(tmp_path: Path, timestamp: str, name: str = "cm13JUL2020bhav.csv") -> Path:
    """A one-row legacy bhavcopy whose TIMESTAMP spelling is under test."""
    import zipfile

    row = (
        f"20MICRONS,EQ,32.85,33.85,31.85,33.45,33.85,32.3,187303,6187285.7,"
        f"{timestamp},1382,INE144J01027"
    )
    dest = tmp_path / "legacy.zip"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, f"{_LEGACY_HEADER}\n{row}\n")
    return dest


def _normalize_zip(archive: Path, business_date: date):
    payload = make_payload(archive, "nse_cm_legacy", business_date)
    return NseLegacyNormalizer().normalize(NseLegacyCmParser().parse(payload), payload)


def test_a_two_digit_year_resolves_to_the_full_year(tmp_path: Path):
    batch = _normalize_zip(_legacy_zip(tmp_path, "13-Jul-20"), date(2020, 7, 13))
    assert batch.frame["ts"].dt.date().to_list() == [date(2020, 7, 13)]


def test_a_four_digit_year_still_parses(tmp_path: Path):
    batch = _normalize_zip(_legacy_zip(tmp_path, "13-JUL-2020"), date(2020, 7, 13))
    assert batch.frame["ts"].dt.date().to_list() == [date(2020, 7, 13)]


def test_a_row_date_disagreeing_with_the_business_date_is_refused(tmp_path: Path):
    """The guard that would have caught the year-0020 corruption at ingest.
    One legacy bhavcopy covers exactly one session, so a row dated anything
    else means the date was misread or the wrong file was served."""
    with pytest.raises(ValueError, match="0020-07-13"):
        _normalize_zip(_legacy_zip(tmp_path, "13-Jul-0020"), date(2020, 7, 13))


def test_an_unparseable_timestamp_is_refused(tmp_path: Path):
    with pytest.raises(ValueError):
        _normalize_zip(_legacy_zip(tmp_path, "not-a-date"), date(2020, 7, 13))
