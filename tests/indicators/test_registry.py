from __future__ import annotations

from decimal import Decimal

import pytest

from trading.indicators import CATALOGUE, compute, parse, warmup_for


def test_parse_splits_a_token_into_a_name_and_a_period() -> None:
    request = parse("rsi14")
    assert request.name == "rsi"
    assert request.period == 14
    assert request.token == "rsi14"


def test_parse_falls_back_to_the_documented_default_period() -> None:
    assert parse("rsi").period == 14
    assert parse("ema").period == 20


def test_parse_is_case_insensitive_and_ignores_surrounding_space() -> None:
    assert parse("  RSI14 ").name == "rsi"


def test_parse_rejects_an_unknown_indicator_and_names_the_known_ones() -> None:
    with pytest.raises(ValueError) as excinfo:
        parse("supertrend9")
    assert "supertrend" in str(excinfo.value)
    assert "rsi" in str(excinfo.value)


def test_parse_rejects_a_period_on_macd() -> None:
    # MACD is three periods, not one; "macd12" cannot mean anything
    # unambiguous, so it is refused rather than silently reinterpreted.
    with pytest.raises(ValueError):
        parse("macd12")


def test_parse_rejects_a_zero_period() -> None:
    with pytest.raises(ValueError):
        parse("rsi0")


def test_every_catalogued_indicator_parses_and_computes() -> None:
    closes = [Decimal(i) for i in range(1, 121)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    for name in CATALOGUE:
        request = parse(name)
        result = compute(request, highs=highs, lows=lows, closes=closes)
        assert result is not None, f"{name} returned None on 120 clean bars"


def test_compute_returns_a_mapping_for_multi_valued_indicators() -> None:
    closes = [Decimal(i) for i in range(1, 121)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    bands = compute(parse("bb20"), highs=highs, lows=lows, closes=closes)
    assert isinstance(bands, dict)
    assert set(bands) == {"lower", "mid", "upper"}
    lines = compute(parse("macd"), highs=highs, lows=lows, closes=closes)
    assert isinstance(lines, dict)
    assert set(lines) == {"line", "signal", "histogram"}


def test_warmup_for_takes_the_largest_requirement() -> None:
    # rsi14 -> 70, macd -> 5*35 = 175.
    assert warmup_for([parse("rsi14"), parse("macd")]) == 175


def test_warmup_for_nothing_is_zero() -> None:
    assert warmup_for([]) == 0
