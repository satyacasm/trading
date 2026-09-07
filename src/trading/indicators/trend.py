"""Trend indicators: moving averages and what is built from them."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")


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
