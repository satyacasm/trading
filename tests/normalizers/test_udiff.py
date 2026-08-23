from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import DataSource, assert_canonical
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.udiff import UdiffParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "udiff"


def _batch(fixture: str, source_key: str):
    payload = make_payload(FIXTURES / fixture, source_key, date(2026, 8, 13))
    return UdiffNormalizer().normalize(UdiffParser().parse(payload), payload)


def test_output_matches_the_canonical_contract():
    assert_canonical(_batch("nse_cm_udiff.zip", "nse_cm_udiff").frame)


def test_equity_rows_map_to_equity_asset_class():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert set(frame["asset_class"].unique()) == {"EQUITY"}
    assert frame["expiry"].null_count() == frame.height
    assert frame["strike"].null_count() == frame.height


def test_option_rows_carry_strike_expiry_and_type():
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    options = frame.filter(frame["asset_class"] == "OPTION")
    assert options.height > 0
    assert options["strike"].null_count() == 0
    assert options["expiry"].null_count() == 0
    assert set(options["option_type"].unique()) <= {"CE", "PE"}


def test_ts_is_session_close_in_utc():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert frame["ts"][0] == datetime.fromisoformat("2026-08-13T10:00:00+00:00")


def test_untraded_option_keeps_zero_ohlc_and_real_close():
    """Finding F2: must survive normalization, not be nulled or dropped."""
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    untraded = frame.filter((frame["volume"] == 0) & (frame["close"] > Decimal("0")))
    assert untraded.height > 0
    assert untraded["open"][0] == Decimal("0.0000")


def test_empty_strings_become_null_not_zero():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert frame["open_interest"].null_count() == frame.height  # CM has no OI


def test_lot_size_is_carried_through():
    """Finding F3: NewBrdLotQty feeds instrument_lot_history for free."""
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    assert frame["lot_size"].null_count() == 0
    assert (frame["lot_size"] > 0).all()


def test_source_is_tagged_per_segment():
    assert _batch("nse_fo_udiff.zip", "nse_fo_udiff").source is DataSource.NSE_FO_UDIFF
    assert _batch("bse_cm_udiff.csv", "bse_cm_udiff").source is DataSource.BSE_CM_UDIFF


def test_unmapped_fin_instrm_tp_raises():
    """Ruling N1: an unmapped FinInstrmTp must raise, never silently be EQUITY.

    Built by overwriting one row's FinInstrmTp on a fixture-parsed frame;
    the committed fixture itself is never modified.
    """
    payload = make_payload(FIXTURES / "nse_fo_udiff.zip", "nse_fo_udiff", date(2026, 8, 13))
    frame = UdiffParser().parse(payload)
    poisoned = frame.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("ZZZ"))
        .otherwise(pl.col("FinInstrmTp"))
        .alias("FinInstrmTp")
    )
    with pytest.raises(pl.exceptions.InvalidOperationError):
        UdiffNormalizer().normalize(poisoned, payload)


def test_unrecognised_source_key_raises_with_key_in_message():
    """Ruling N2: fail loudly, never fall back to a default source."""
    payload = make_payload(FIXTURES / "nse_cm_udiff.zip", "not_a_real_source", date(2026, 8, 13))
    frame = UdiffParser().parse(payload)
    with pytest.raises(ValueError, match="not_a_real_source"):
        UdiffNormalizer().normalize(frame, payload)


# --- Task 18, Ruling S1: SctySrs carries through as `series` ---


def test_cm_series_is_carried_through_from_sctysrs():
    """The CM fixture carries GB (gold bond) and EQ rows."""
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert set(frame["series"].unique()) == {"GB", "EQ"}
    assert frame["series"].null_count() == 0


def test_fo_series_is_null_because_sctysrs_is_blank_for_fo():
    """docs/data-formats/eod-source-formats.md: SctySrs is CM-only; the FO
    fixture's SctySrs is blank on every row, so series must come through as
    None rather than an empty string."""
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    assert frame["series"].null_count() == frame.height
