"""Volatility indicators: how far price moves, not which way."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import NamedTuple

from trading.indicators._shared import _require_positive_period


class Bands(NamedTuple):
    """A Bollinger envelope. `mid` is the simple moving average."""

    lower: Decimal
    mid: Decimal
    upper: Decimal


def true_ranges(
    highs: Sequence[Decimal], lows: Sequence[Decimal], closes: Sequence[Decimal]
) -> list[Decimal]:
    """True range per bar, from the second bar onwards.

    The previous close is part of the definition: a bar that gaps away
    from the last close has travelled the whole gap, and measuring only
    its own high-to-low would report a quiet bar after a violent move.
    """
    count = min(len(highs), len(lows), len(closes))
    out: list[Decimal] = []
    for index in range(1, count):
        previous_close = closes[index - 1]
        out.append(
            max(
                highs[index] - lows[index],
                abs(highs[index] - previous_close),
                abs(lows[index] - previous_close),
            )
        )
    return out


def atr(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Wilder's Average True Range."""
    _require_positive_period(period)
    ranges = true_ranges(highs, lows, closes)
    if len(ranges) < period:
        return None
    divisor = Decimal(period)
    value = sum(ranges[:period], Decimal(0)) / divisor
    for current in ranges[period:]:
        value = (value * (divisor - 1) + current) / divisor
    return value


def bollinger(
    closes: Sequence[Decimal], period: int = 20, num_std: Decimal = Decimal(2)
) -> Bands | None:
    """Bollinger bands around the simple moving average.

    Population standard deviation, dividing by `period` rather than
    `period - 1`: the window is the whole population being described, and
    it is what charting packages plot.
    """
    _require_positive_period(period)
    if len(closes) < period:
        return None
    window = closes[-period:]
    divisor = Decimal(period)
    mid = sum(window, Decimal(0)) / divisor
    variance = sum(((value - mid) ** 2 for value in window), Decimal(0)) / divisor
    deviation = variance.sqrt()
    return Bands(lower=mid - num_std * deviation, mid=mid, upper=mid + num_std * deviation)


def realised_vol(closes: Sequence[Decimal], period: int = 20) -> Decimal | None:
    """Per-bar standard deviation of simple returns.

    Deliberately NOT annualised. Periods-per-year cannot be derived from
    the bar interval alone -- an hourly bar is 8,760 a year on a 24/7
    crypto venue and about 1,575 across a 6h15m NSE session -- so
    annualising here would mean guessing the asset's calendar. The result
    would look like a volatility and not be one. A caller who knows the
    calendar can scale this itself.
    """
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None
    window = closes[-period - 1 :]
    returns: list[Decimal] = []
    for previous, current in zip(window, window[1:], strict=False):
        if previous == 0:
            return None
        returns.append(current / previous - Decimal(1))
    divisor = Decimal(len(returns))
    mean = sum(returns, Decimal(0)) / divisor
    variance = sum(((value - mean) ** 2 for value in returns), Decimal(0)) / divisor
    return variance.sqrt()
