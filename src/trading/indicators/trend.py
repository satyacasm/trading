"""Trend indicators: moving averages and what is built from them."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import NamedTuple

from trading.indicators._shared import _require_positive_period
from trading.indicators.volatility import true_ranges


def sma(values: Sequence[Decimal], period: int) -> Decimal | None:
    """Simple moving average of the last `period` values.

    `None` when there are not enough of them -- averaging whatever is
    available would return a different indicator under the same name.
    """
    _require_positive_period(period)
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window, Decimal(0)) / Decimal(period)


def ema_series(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Every EMA value, oldest first, seeded with the SMA of the first
    `period` bars.

    Seeding with the SMA rather than the first close is what makes this
    reproducible: seeding with a single bar leaves the whole series
    dependent on how far back the caller happened to fetch.

    Returns `[]` when there is not enough data, so callers can test it
    without a separate length check.
    """
    _require_positive_period(period)
    if len(values) < period:
        return []
    alpha = Decimal(2) / Decimal(period + 1)
    seed = sum(values[:period], Decimal(0)) / Decimal(period)
    out = [seed]
    for value in values[period:]:
        out.append(alpha * value + (Decimal(1) - alpha) * out[-1])
    return out


def ema(values: Sequence[Decimal], period: int) -> Decimal | None:
    """The latest exponential moving average, or `None`."""
    series = ema_series(values, period)
    return series[-1] if series else None


class Macd(NamedTuple):
    """Moving Average Convergence Divergence, all three legs."""

    line: Decimal
    signal: Decimal
    histogram: Decimal


def macd(closes: Sequence[Decimal], fast: int = 12, slow: int = 26, signal: int = 9) -> Macd | None:
    """MACD line, its signal line, and the histogram between them."""
    _require_positive_period(fast)
    _require_positive_period(slow)
    _require_positive_period(signal)
    if fast >= slow:
        raise ValueError(f"fast period {fast} must be shorter than slow period {slow}")

    fast_series = ema_series(closes, fast)
    slow_series = ema_series(closes, slow)
    if not fast_series or not slow_series:
        return None

    # Both series end on the same bar but the slow one starts later, so
    # align on the shorter tail. Zipping from the front would subtract
    # EMAs computed at different bars and produce a plausible, wrong line.
    overlap = min(len(fast_series), len(slow_series))
    line = [
        quick - slow_value
        for quick, slow_value in zip(fast_series[-overlap:], slow_series[-overlap:], strict=True)
    ]
    signal_series = ema_series(line, signal)
    if not signal_series:
        return None
    return Macd(line=line[-1], signal=signal_series[-1], histogram=line[-1] - signal_series[-1])


def adx(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Wilder's Average Directional Index: trend strength, not direction.

    `None` when no bar has a defined DX -- a series with no range and no
    directional movement has no trend strength to report, and zero would
    claim it measured one.
    """
    _require_positive_period(period)
    count = min(len(highs), len(lows), len(closes))
    if count < 2:
        return None

    # Directional movement starts at index 1, same as true_ranges: the
    # first bar has no previous bar to move from. Aligning against
    # true_ranges rather than re-deriving the range here keeps this in
    # step if that definition ever changes.
    plus_dm: list[Decimal] = []
    minus_dm: list[Decimal] = []
    for index in range(1, count):
        up_move = highs[index] - highs[index - 1]
        down_move = lows[index - 1] - lows[index]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else Decimal(0))
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else Decimal(0))

    ranges = true_ranges(highs, lows, closes)
    if len(ranges) < period:
        return None

    def wilder_sum(values: list[Decimal]) -> list[Decimal]:
        running = sum(values[:period], Decimal(0))
        out = [running]
        for value in values[period:]:
            running = running - running / Decimal(period) + value
            out.append(running)
        return out

    smoothed_range = wilder_sum(ranges)
    smoothed_plus = wilder_sum(plus_dm)
    smoothed_minus = wilder_sum(minus_dm)

    directional: list[Decimal] = []
    for total_range, up, down in zip(smoothed_range, smoothed_plus, smoothed_minus, strict=True):
        if total_range == 0:
            continue
        plus_di = Decimal(100) * up / total_range
        minus_di = Decimal(100) * down / total_range
        if plus_di + minus_di == 0:
            continue
        directional.append(Decimal(100) * abs(plus_di - minus_di) / (plus_di + minus_di))

    if len(directional) < period:
        return None
    divisor = Decimal(period)
    value = sum(directional[:period], Decimal(0)) / divisor
    for current in directional[period:]:
        value = (value * (divisor - 1) + current) / divisor
    return value
