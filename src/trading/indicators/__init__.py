"""Technical indicators over bar series.

`Decimal` throughout, and shared rather than private to the MCP layer on
purpose: a strategy script and the live agent must compute RSI with the
same code. If they diverged, every lesson carried from a backtest into a
live decision would be measuring a subtly different thing, invisibly in
both places.

Callers name an indicator with a token -- `"rsi14"`, `"ema20"`, `"macd"`
-- rather than calling the functions directly, so that one request can
carry a list of them and the warmup requirement can be derived before any
data is fetched.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from trading.indicators.levels import pct_from_high, pct_from_low, range_position
from trading.indicators.momentum import roc, rsi, stochastic_k
from trading.indicators.trend import adx, ema, macd, sma
from trading.indicators.volatility import atr, bollinger, realised_vol
from trading.indicators.warmup import warmup_bars_for_all

__all__ = [
    "CATALOGUE",
    "IndicatorRequest",
    "compute",
    "parse",
    "warmup_for",
]

CATALOGUE: dict[str, str] = {
    "sma": "Simple moving average of close.",
    "ema": "Exponential moving average of close, seeded with the SMA.",
    "macd": "MACD line, signal and histogram. Fixed at 12/26/9.",
    "adx": "Wilder ADX: trend strength, not direction. 0-100.",
    "rsi": "Wilder RSI. 0-100.",
    "roc": "Percentage rate of change over the period.",
    "stoch": "Stochastic %K: close within the period's range. 0-100.",
    "atr": "Wilder Average True Range, in price units.",
    "bb": "Bollinger bands: lower, mid, upper.",
    "vol": "Per-bar standard deviation of simple returns. NOT annualised.",
    "pcthigh": "Percent of the last close below the period high. 0 or negative.",
    "pctlow": "Percent of the last close above the period low. 0 or positive.",
    "rangepos": "Position of the last close within the period range. 0-100.",
}

# The period a bare token means. `macd` carries its warmup requirement
# here (slow 26 + signal 9) even though it takes no period of its own.
_DEFAULT_PERIODS: dict[str, int] = {
    "sma": 20,
    "ema": 20,
    "macd": 35,
    "adx": 14,
    "rsi": 14,
    "roc": 12,
    "stoch": 14,
    "atr": 14,
    "bb": 20,
    "vol": 20,
    "pcthigh": 52,
    "pctlow": 52,
    "rangepos": 20,
}

# Indicators whose parameters are fixed, so a period in the token would be
# ambiguous rather than merely unused.
_FIXED_PARAMETER_INDICATORS = frozenset({"macd"})

_TOKEN = re.compile(r"([a-z]+)(\d*)")


@dataclass(frozen=True)
class IndicatorRequest:
    """One parsed indicator token."""

    token: str
    name: str
    period: int


def parse(token: str) -> IndicatorRequest:
    """Turn `"rsi14"` into a request, or raise with the known names.

    Raising rather than skipping an unknown token: an agent that misspells
    an indicator should be told, not handed a snapshot that quietly lacks
    the thing it asked for and looks complete.
    """
    cleaned = token.strip().lower()
    match = _TOKEN.fullmatch(cleaned)
    if match is None:
        raise ValueError(
            f"unparseable indicator token {token!r}; expected a name "
            f"optionally followed by a period, like 'rsi14'"
        )
    name, digits = match.group(1), match.group(2)
    if name not in _DEFAULT_PERIODS:
        known = ", ".join(sorted(_DEFAULT_PERIODS))
        raise ValueError(f"unknown indicator {name!r}; known indicators are: {known}")
    if digits and name in _FIXED_PARAMETER_INDICATORS:
        raise ValueError(
            f"{name!r} takes no period -- its parameters are fixed; use {name!r} alone"
        )
    period = int(digits) if digits else _DEFAULT_PERIODS[name]
    if period <= 0:
        raise ValueError(f"period must be positive, got {period} in {token!r}")
    return IndicatorRequest(token=cleaned, name=name, period=period)


def warmup_for(requests: Sequence[IndicatorRequest]) -> int:
    """Extra bars to fetch so every requested indicator converges."""
    return warmup_bars_for_all(request.period for request in requests)


def compute(
    request: IndicatorRequest,
    *,
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
) -> Decimal | dict[str, Decimal] | None:
    """Evaluate one parsed request against a bar series.

    `None` means the series was too short or the value is undefined; the
    caller reports that rather than substituting a number.
    """
    name, period = request.name, request.period
    if name == "sma":
        return sma(closes, period)
    if name == "ema":
        return ema(closes, period)
    if name == "macd":
        lines = macd(closes)
        return None if lines is None else lines._asdict()
    if name == "adx":
        return adx(highs, lows, closes, period)
    if name == "rsi":
        return rsi(closes, period)
    if name == "roc":
        return roc(closes, period)
    if name == "stoch":
        return stochastic_k(highs, lows, closes, period)
    if name == "atr":
        return atr(highs, lows, closes, period)
    if name == "bb":
        bands = bollinger(closes, period)
        return None if bands is None else bands._asdict()
    if name == "vol":
        return realised_vol(closes, period)
    if name == "pcthigh":
        return pct_from_high(closes, highs, period)
    if name == "pctlow":
        return pct_from_low(closes, lows, period)
    if name == "rangepos":
        return range_position(closes, highs, lows, period)
    raise ValueError(f"no implementation for catalogued indicator {name!r}")
