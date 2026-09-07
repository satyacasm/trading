from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.momentum import roc, rsi, stochastic_k

_prices = st.lists(
    st.decimals(min_value=Decimal("1"), max_value=Decimal("10000"), places=2),
    min_size=16,
    max_size=60,
)


def test_rsi_needs_one_more_bar_than_its_period() -> None:
    # `period` deltas require `period + 1` closes.
    assert rsi([Decimal(10)] * 14, period=14) is None
    assert rsi([Decimal(10)] * 15, period=14) is not None


def test_rsi_of_a_series_that_only_rises_is_one_hundred() -> None:
    closes = [Decimal(i) for i in range(1, 20)]
    assert rsi(closes, period=14) == Decimal(100)


def test_rsi_of_a_series_that_only_falls_is_zero() -> None:
    closes = [Decimal(i) for i in range(20, 1, -1)]
    assert rsi(closes, period=14) == Decimal(0)


def test_rsi_matches_a_hand_computed_wilder_example() -> None:
    # closes 10, 11, 10, 11, 10 with period 2.
    #   deltas          +1  -1  +1  -1
    #   gains            1   0   1   0
    #   losses           0   1   0   1
    #   seed (first 2)  avg_gain = 0.5   avg_loss = 0.5
    #   bar 3 (g=1,l=0) avg_gain = (0.5*1 + 1)/2 = 0.75
    #                   avg_loss = (0.5*1 + 0)/2 = 0.25
    #   bar 4 (g=0,l=1) avg_gain = (0.75*1 + 0)/2 = 0.375
    #                   avg_loss = (0.25*1 + 1)/2 = 0.625
    #   RS = 0.6 -> RSI = 100 - 100/1.6 = 37.5
    closes = [Decimal(10), Decimal(11), Decimal(10), Decimal(11), Decimal(10)]
    assert rsi(closes, period=2) == Decimal("37.5")


def test_a_truncated_rsi_differs_from_a_fully_warmed_one() -> None:
    # The whole reason `warmup` exists. Wilder smoothing carries the seed
    # forward indefinitely, so the same 15 final bars give a different RSI
    # depending on how much history preceded them. If this ever stops
    # being true, the warmup machinery is measuring nothing.
    closes = [Decimal(100) + Decimal((i * 7) % 13) for i in range(200)]
    truncated = rsi(closes[-15:], period=14)
    warmed = rsi(closes, period=14)
    assert truncated is not None and warmed is not None
    assert truncated != warmed


@given(_prices)
def test_rsi_always_lies_between_zero_and_one_hundred(closes: list[Decimal]) -> None:
    result = rsi(closes, period=14)
    if result is not None:
        assert Decimal(0) <= result <= Decimal(100)


def test_roc_is_the_percentage_change_over_the_period() -> None:
    # 100 -> 110 across 2 bars is +10%.
    closes = [Decimal(100), Decimal(105), Decimal(110)]
    assert roc(closes, period=2) == Decimal(10)


def test_roc_returns_none_when_the_reference_bar_is_zero() -> None:
    closes = [Decimal(0), Decimal(5), Decimal(10)]
    assert roc(closes, period=2) is None


def test_stochastic_k_is_zero_at_the_low_and_one_hundred_at_the_high() -> None:
    highs = [Decimal(10), Decimal(12), Decimal(14)]
    lows = [Decimal(5), Decimal(6), Decimal(7)]
    at_high = stochastic_k(highs, lows, [Decimal(9), Decimal(9), Decimal(14)], period=3)
    at_low = stochastic_k(highs, lows, [Decimal(9), Decimal(9), Decimal(5)], period=3)
    assert at_high == Decimal(100)
    assert at_low == Decimal(0)


def test_stochastic_k_is_none_when_the_range_is_flat() -> None:
    # A flat window has no position within it. Reporting 50 would invent
    # a midpoint that the data does not contain.
    flat = [Decimal(10)] * 3
    assert stochastic_k(flat, flat, flat, period=3) is None
