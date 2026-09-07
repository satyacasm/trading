"""Momentum indicators: rate and position of price change."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from trading.indicators._shared import _require_positive_period


def rsi(closes: Sequence[Decimal], period: int = 14) -> Decimal | None:
    """Wilder's Relative Strength Index over `closes`.

    Wilder smoothing, not a simple average of gains: the two disagree,
    and Wilder's is what every charting package means by "RSI". Needs
    `period + 1` closes, since `period` deltas require that many bars.

    A window with no losses is 100 rather than a division by zero -- the
    index is defined at that boundary and the caller should see it.
    """
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None

    gains: list[Decimal] = []
    losses: list[Decimal] = []
    for previous, current in zip(closes, closes[1:], strict=False):
        delta = current - previous
        gains.append(delta if delta > 0 else Decimal(0))
        losses.append(-delta if delta < 0 else Decimal(0))

    divisor = Decimal(period)
    avg_gain = sum(gains[:period], Decimal(0)) / divisor
    avg_loss = sum(losses[:period], Decimal(0)) / divisor
    for gain, loss in zip(gains[period:], losses[period:], strict=True):
        avg_gain = (avg_gain * (divisor - 1) + gain) / divisor
        avg_loss = (avg_loss * (divisor - 1) + loss) / divisor

    if avg_loss == 0:
        return Decimal(100) if avg_gain > 0 else Decimal(50)
    relative_strength = avg_gain / avg_loss
    return Decimal(100) - (Decimal(100) / (Decimal(1) + relative_strength))


def roc(closes: Sequence[Decimal], period: int = 12) -> Decimal | None:
    """Percentage rate of change over `period` bars."""
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None
    reference = closes[-period - 1]
    if reference == 0:
        return None
    return (closes[-1] - reference) / reference * Decimal(100)


def stochastic_k(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Where the last close sits in the period's range, as a percentage.

    `None` for a flat range rather than 50: a window with no range has no
    position within it, and a midpoint invented here would read as a
    fact about the market.
    """
    _require_positive_period(period)
    if min(len(highs), len(lows), len(closes)) < period:
        return None
    highest = max(highs[-period:])
    lowest = min(lows[-period:])
    if highest == lowest:
        return None
    return (closes[-1] - lowest) / (highest - lowest) * Decimal(100)
