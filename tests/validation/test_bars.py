from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, NormalizedBatch, ValidationOutcome
from trading.validation.bars import BarValidator

TS = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


def _frame(**overrides: object) -> pl.DataFrame:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    row.update(
        exchange="NSE",
        segment="CM",
        symbol="TEST",
        asset_class="EQUITY",
        ts=TS,
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("95"),
        close=Decimal("105"),
        volume=1000,
    )
    row.update(overrides)
    return pl.DataFrame([row], schema=CANONICAL_BAR_SCHEMA)


def _validate(frame: pl.DataFrame) -> ValidationOutcome:
    return BarValidator().validate(
        NormalizedBatch(DataSource.NSE_CM_UDIFF, date(2026, 8, 13), frame)
    )


def test_a_good_row_passes() -> None:
    outcome = _validate(_frame())
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_untraded_option_with_zero_ohlc_passes() -> None:
    """Finding F2 — 60% of F&O rows look like this."""
    outcome = _validate(
        _frame(
            open=Decimal("0"), high=Decimal("0"), low=Decimal("0"), close=Decimal("19.45"), volume=0
        )
    )
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_traded_row_with_high_below_low_is_quarantined() -> None:
    outcome = _validate(_frame(high=Decimal("90"), low=Decimal("95")))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "ohlc_inconsistent"


def test_missing_close_is_quarantined() -> None:
    outcome = _validate(_frame(close=None))
    assert outcome.quarantined[0].reason == "close_missing"


def test_negative_volume_is_quarantined() -> None:
    outcome = _validate(_frame(volume=-5))
    assert outcome.quarantined[0].reason == "negative_volume"


def test_duplicate_keys_quarantine_the_later_row_only() -> None:
    frame = pl.concat([_frame(), _frame(close=Decimal("106"))])
    outcome = _validate(frame)
    assert outcome.valid.height == 1
    assert outcome.quarantined[0].reason == "duplicate_key"


def test_one_bad_row_does_not_fail_the_batch() -> None:
    """A 3-in-100k failure must not block a 2,500-day backfill."""
    frame = pl.concat([_frame(symbol="GOOD"), _frame(symbol="BAD", close=None)])
    outcome = _validate(frame)
    assert outcome.valid.height == 1
    assert len(outcome.quarantined) == 1


def test_mf_row_with_null_close_is_nav_not_available() -> None:
    """Ruling V1 — AMFI N.A. NAV is benign, not a parse failure."""
    outcome = _validate(_frame(asset_class="MF", close=None))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "nav_not_available"


def test_equity_row_with_null_close_stays_close_missing() -> None:
    """Ruling V1 — the MF carve-out must not swallow other asset classes."""
    outcome = _validate(_frame(asset_class="EQUITY", close=None))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "close_missing"


def test_null_volume_row_with_high_below_low_is_quarantined() -> None:
    """Ruling V4 — a null volume must not bypass the OHLC-consistency check."""
    outcome = _validate(_frame(volume=None, high=Decimal("90"), low=Decimal("95")))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "ohlc_inconsistent"


def test_mf_shaped_row_with_null_volume_passes() -> None:
    """Ruling V4 — AMFI rows carry null volume with open=high=low=close=NAV."""
    outcome = _validate(
        _frame(
            asset_class="MF",
            open=Decimal("19.45"),
            high=Decimal("19.45"),
            low=Decimal("19.45"),
            close=Decimal("19.45"),
            volume=None,
        )
    )
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_null_open_is_quarantined_as_ohlc_missing() -> None:
    """Ruling L1a — bars_daily declares open/high/low NOT NULL, same as close."""
    outcome = _validate(_frame(open=None))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "ohlc_missing"


def test_fully_populated_row_still_passes_ohlc_missing_check() -> None:
    """Ruling L1a — the new gate must not false-positive on a complete row."""
    outcome = _validate(_frame())
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_option_type_without_strike_is_quarantined() -> None:
    """Ruling L1b — instruments CHECK requires strike whenever option_type is set."""
    outcome = _validate(_frame(option_type="CE"))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "option_fields_incomplete"


def test_strike_without_option_type_passes() -> None:
    """Ruling L1b — this combination classifies as FUTURE/EQUITY and is not a violation."""
    outcome = _validate(_frame(strike=Decimal("24500")))
    assert outcome.valid.height == 1
    assert outcome.quarantined == []
