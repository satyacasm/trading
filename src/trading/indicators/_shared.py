"""Private helpers shared across the indicators package.

Not part of the public surface -- everything here is an implementation
detail that every indicator module happens to need, not something an
importer outside this package should reach for.
"""

from __future__ import annotations


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")
