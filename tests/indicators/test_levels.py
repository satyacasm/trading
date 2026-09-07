from __future__ import annotations

from decimal import Decimal

from trading.indicators.levels import pct_from_high, pct_from_low, range_position


def test_pct_from_high_is_zero_at_the_period_high() -> None:
    highs = [Decimal(10), Decimal(20), Decimal(15)]
    closes = [Decimal(9), Decimal(19), Decimal(20)]
    assert pct_from_high(closes, highs, period=3) == Decimal(0)


def test_pct_from_high_is_negative_below_the_period_high() -> None:
    highs = [Decimal(10), Decimal(20), Decimal(15)]
    closes = [Decimal(9), Decimal(19), Decimal(10)]
    # 10 against a 20 high is 50% below it.
    assert pct_from_high(closes, highs, period=3) == Decimal(-50)


def test_pct_from_low_is_positive_above_the_period_low() -> None:
    lows = [Decimal(10), Decimal(5), Decimal(8)]
    closes = [Decimal(11), Decimal(6), Decimal(10)]
    # 10 against a 5 low is 100% above it.
    assert pct_from_low(closes, lows, period=3) == Decimal(100)


def test_range_position_spans_zero_to_one_hundred() -> None:
    highs = [Decimal(20)] * 3
    lows = [Decimal(10)] * 3
    assert range_position([Decimal(0), Decimal(0), Decimal(15)], highs, lows, 3) == Decimal(50)
    assert range_position([Decimal(0), Decimal(0), Decimal(20)], highs, lows, 3) == Decimal(100)
    assert range_position([Decimal(0), Decimal(0), Decimal(10)], highs, lows, 3) == Decimal(0)


def test_range_position_is_none_when_the_range_is_flat() -> None:
    flat = [Decimal(10)] * 3
    assert range_position(flat, flat, flat, 3) is None


def test_levels_return_none_with_too_few_bars() -> None:
    one = [Decimal(10)]
    assert pct_from_high(one, one, period=5) is None
    assert pct_from_low(one, one, period=5) is None
    assert range_position(one, one, one, period=5) is None
