"""Where price sits relative to its own recent extremes."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from trading.indicators._shared import _require_positive_period


def pct_from_high(
    closes: Sequence[Decimal], highs: Sequence[Decimal], period: int
) -> Decimal | None:
    """How far the last close sits below the period's high, as a percentage.

    Zero at the high and negative below it, so the sign carries the
    meaning without the caller having to remember a convention.
    """
    _require_positive_period(period)
    if min(len(closes), len(highs)) < period:
        return None
    highest = max(highs[-period:])
    if highest == 0:
        return None
    return (closes[-1] - highest) / highest * Decimal(100)


def pct_from_low(closes: Sequence[Decimal], lows: Sequence[Decimal], period: int) -> Decimal | None:
    """How far the last close sits above the period's low, as a percentage."""
    _require_positive_period(period)
    if min(len(closes), len(lows)) < period:
        return None
    lowest = min(lows[-period:])
    if lowest == 0:
        return None
    return (closes[-1] - lowest) / lowest * Decimal(100)


def range_position(
    closes: Sequence[Decimal],
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    period: int,
) -> Decimal | None:
    """Position of the last close within the period's range, 0 to 100.

    `None` for a flat range, for the reason `stochastic_k` gives: a
    window with no range has no position within it.
    """
    _require_positive_period(period)
    if min(len(closes), len(highs), len(lows)) < period:
        return None
    highest = max(highs[-period:])
    lowest = min(lows[-period:])
    if highest == lowest:
        return None
    return (closes[-1] - lowest) / (highest - lowest) * Decimal(100)
