"""How much run-up an indicator needs before its output means anything.

Wilder smoothing has no closed form: each value depends on the one
before it, so a 14-period RSI computed from 15 bars is simply not the
number the same RSI computed from 100 bars produces. Fetching only
`history + period` bars would hand a caller plausible values that
disagree with what a backtest computed over the same window.
"""

from __future__ import annotations

from collections.abc import Iterable

# Five periods of run-up puts the residual weight of the seed below 1% for
# the smoothings used here; the floor covers very short periods, where five
# periods is still only a handful of bars.
_WARMUP_MULTIPLE = 5
_MIN_WARMUP_BARS = 50


def warmup_bars_for(period: int) -> int:
    """Bars to fetch BEYOND the caller's requested history."""
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")
    return max(_WARMUP_MULTIPLE * period, _MIN_WARMUP_BARS)


def warmup_bars_for_all(periods: Iterable[int]) -> int:
    """The largest requirement across several indicators.

    Zero for an empty request: a caller asking for no indicators wants
    exactly the history it asked for.
    """
    return max((warmup_bars_for(p) for p in periods), default=0)
