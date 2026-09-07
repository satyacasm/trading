from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.trend import adx, ema, ema_series, macd, sma

_prices = st.lists(
    st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=2),
    min_size=1,
    max_size=60,
)


def test_sma_returns_none_when_there_are_fewer_bars_than_the_period() -> None:
    assert sma([Decimal(1), Decimal(2)], 3) is None


def test_sma_averages_only_the_last_period_bars() -> None:
    # The leading 100 must not be counted: mean(2, 4, 6) == 4.
    values = [Decimal(100), Decimal(2), Decimal(4), Decimal(6)]
    assert sma(values, 3) == Decimal(4)


def test_sma_rejects_a_non_positive_period() -> None:
    with pytest.raises(ValueError):
        sma([Decimal(1)], 0)


@given(_prices, st.integers(min_value=1, max_value=20))
def test_sma_of_a_constant_series_is_that_constant(values: list[Decimal], period: int) -> None:
    constant = values[0]
    series = [constant] * max(len(values), period)
    assert sma(series, period) == constant


def test_ema_seeds_with_the_sma_so_the_first_value_equals_it() -> None:
    # With exactly `period` bars there is nothing to smooth yet, so the
    # only EMA value is the seed.
    values = [Decimal(2), Decimal(4), Decimal(6)]
    assert ema(values, 3) == Decimal(4)


def test_ema_weights_the_newest_bar_by_alpha() -> None:
    # period 3 -> alpha = 2/4 = 0.5. Seed = mean(2, 4, 6) = 4.
    # Next bar 10 -> 0.5*10 + 0.5*4 = 7.
    values = [Decimal(2), Decimal(4), Decimal(6), Decimal(10)]
    assert ema(values, 3) == Decimal(7)


def test_ema_series_has_one_value_per_bar_after_the_seed() -> None:
    values = [Decimal(i) for i in range(1, 11)]
    assert len(ema_series(values, 4)) == len(values) - 4 + 1


def test_ema_returns_none_when_there_are_fewer_bars_than_the_period() -> None:
    assert ema([Decimal(1)], 5) is None


@given(_prices)
def test_ema_never_leaves_the_range_of_its_inputs(values: list[Decimal]) -> None:
    # A weighted average of the series cannot escape the series' bounds.
    result = ema(values, 3)
    if result is not None:
        assert min(values) <= result <= max(values)


def test_macd_of_a_flat_series_is_zero_on_every_leg() -> None:
    closes = [Decimal(10)] * 60
    result = macd(closes)
    assert result is not None
    assert result.line == Decimal(0)
    assert result.signal == Decimal(0)
    assert result.histogram == Decimal(0)


def test_macd_line_is_positive_while_price_rises() -> None:
    # The fast EMA sits above the slow one in an uptrend.
    closes = [Decimal(i) for i in range(1, 81)]
    result = macd(closes)
    assert result is not None
    assert result.line > 0


def test_macd_histogram_is_the_line_minus_its_signal() -> None:
    closes = [Decimal(i) for i in range(1, 81)]
    result = macd(closes)
    assert result is not None
    assert result.histogram == result.line - result.signal


def test_macd_returns_none_without_enough_bars() -> None:
    assert macd([Decimal(10)] * 20) is None


def test_macd_rejects_a_fast_period_that_is_not_faster() -> None:
    with pytest.raises(ValueError):
        macd([Decimal(10)] * 60, fast=26, slow=26)


def test_adx_of_a_flat_series_is_none() -> None:
    # No directional movement and no true range: DX is undefined at every
    # bar, so there is nothing to average.
    flat = [Decimal(10)] * 60
    assert adx(flat, flat, flat, period=14) is None


def test_adx_of_a_persistent_uptrend_is_high() -> None:
    # Every bar makes a higher high and higher low, so -DI is zero, DX is
    # pinned at 100, and its average is 100.
    closes = [Decimal(i) for i in range(1, 61)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    result = adx(highs, lows, closes, period=14)
    assert result is not None
    assert result == Decimal(100)


def test_adx_returns_none_without_enough_bars() -> None:
    closes = [Decimal(i) for i in range(1, 10)]
    assert adx(closes, closes, closes, period=14) is None
