from __future__ import annotations

from trading.indicators.warmup import warmup_bars_for, warmup_bars_for_all


def test_warmup_is_never_less_than_fifty_bars() -> None:
    # Wilder smoothing converges slowly; a short period still needs a
    # meaningful run-up before its output is stable.
    assert warmup_bars_for(2) == 50


def test_warmup_scales_with_the_period_once_it_exceeds_the_floor() -> None:
    assert warmup_bars_for(14) == 70


def test_warmup_for_several_indicators_takes_the_largest() -> None:
    assert warmup_bars_for_all([2, 14, 26]) == 130


def test_warmup_for_no_indicators_is_zero() -> None:
    assert warmup_bars_for_all([]) == 0
