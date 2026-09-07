from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.volatility import atr, bollinger, realised_vol, true_ranges

_prices = st.lists(
    st.decimals(min_value=Decimal("1"), max_value=Decimal("10000"), places=2),
    min_size=25,
    max_size=60,
)


def test_true_range_uses_the_previous_close_when_the_bar_gaps_up() -> None:
    # Bar 2 gaps: high 20, low 18, previous close 10. The high-to-previous
    # -close distance (10) is the true range, not the 2-point bar range.
    highs = [Decimal(11), Decimal(20)]
    lows = [Decimal(9), Decimal(18)]
    closes = [Decimal(10), Decimal(19)]
    assert true_ranges(highs, lows, closes) == [Decimal(10)]


def test_true_ranges_has_one_fewer_entry_than_the_bars() -> None:
    bars = [Decimal(i) for i in range(1, 11)]
    assert len(true_ranges(bars, bars, bars)) == len(bars) - 1


def test_atr_of_constant_ranges_is_that_range() -> None:
    # Every bar spans exactly 2 with no gaps, so every true range is 2 and
    # Wilder smoothing of a constant is that constant.
    closes = [Decimal(10)] * 20
    highs = [Decimal(11)] * 20
    lows = [Decimal(9)] * 20
    assert atr(highs, lows, closes, period=14) == Decimal(2)


def test_atr_returns_none_without_enough_true_ranges() -> None:
    closes = [Decimal(10)] * 14
    assert atr(closes, closes, closes, period=14) is None


@given(_prices)
def test_atr_is_never_negative(closes: list[Decimal]) -> None:
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    result = atr(highs, lows, closes, period=14)
    if result is not None:
        assert result >= 0


def test_bollinger_mid_band_is_the_simple_moving_average() -> None:
    closes = [Decimal(2), Decimal(4), Decimal(6)]
    bands = bollinger(closes, period=3)
    assert bands is not None
    assert bands.mid == Decimal(4)


def test_bollinger_collapses_to_the_mean_when_the_series_is_flat() -> None:
    closes = [Decimal(10)] * 20
    bands = bollinger(closes, period=20)
    assert bands is not None
    assert bands.lower == bands.mid == bands.upper == Decimal(10)


def test_bollinger_bands_are_symmetric_around_the_mid() -> None:
    closes = [Decimal(i) for i in range(1, 21)]
    bands = bollinger(closes, period=20)
    assert bands is not None
    assert bands.upper - bands.mid == bands.mid - bands.lower


def test_bollinger_returns_none_with_too_few_bars() -> None:
    assert bollinger([Decimal(1), Decimal(2)], period=20) is None


def test_realised_vol_of_a_flat_series_is_zero() -> None:
    assert realised_vol([Decimal(10)] * 25, period=20) == Decimal(0)


def test_realised_vol_returns_none_when_a_reference_close_is_zero() -> None:
    closes = [Decimal(0)] + [Decimal(10)] * 24
    assert realised_vol(closes, period=24) is None


@given(_prices)
def test_realised_vol_is_never_negative(closes: list[Decimal]) -> None:
    result = realised_vol(closes, period=20)
    if result is not None:
        assert result >= 0
