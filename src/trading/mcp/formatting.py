"""Shaping every value that crosses the boundary to an agent."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

# How old a bar may be before the series is called stale, as a multiple of
# its own interval. Three is chosen so a daily series survives a weekend:
# Friday's close read on Monday morning is about 2.7 days old, and an
# alarm every Monday is an alarm nobody reads.
_STALE_MULTIPLE = 3

_INTERVAL_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}


def money(value: object) -> str | None:
    """Any monetary value as exact text, or `None`.

    A string passes through untouched -- the gateway already sends money
    as text, and re-parsing it can only lose precision. A float is
    stringified rather than fed to `Decimal` directly, since
    `Decimal(0.1)` is 0.1000000000000000055… and `Decimal(str(0.1))` is
    the 0.1 that was meant.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return str(value)
    return str(Decimal(str(value)))


def refused(detail: str, **extra: object) -> dict[str, object]:
    """A business refusal, shaped as data rather than raised.

    The gateway's wording passes through verbatim. Its refusals already
    say what would satisfy them -- "order needs 500, portfolio has 100" --
    and flattening that into "order failed" would remove the only thing
    an agent could act on.
    """
    return {"status": "REFUSED", "reason": detail, **extra}


def freshness(last_ts: datetime | None, interval: str, now: datetime) -> dict[str, object]:
    """How current a series is, on every market-data response.

    Not an error, and never a refusal: an agent is entitled to trade a
    stale series if it decides to. It is not entitled to do so without
    being told, which is the failure this exists to prevent.
    """
    if last_ts is None:
        return {
            "as_of": None,
            "age_seconds": None,
            "stale": True,
            "warning": "no bars available for this instrument and interval",
        }
    age_seconds = int((now - last_ts).total_seconds())
    limit = _INTERVAL_SECONDS.get(interval, 86400) * _STALE_MULTIPLE
    stale = age_seconds > limit
    warning: str | None = None
    if stale:
        days, seconds = divmod(max(age_seconds, 0), 86400)
        span = f"{days} days" if days else f"{seconds // 3600} hours"
        warning = (
            f"last {interval} bar is {span} old; this series may not be current, "
            f"and any decision taken from it inherits that"
        )
    return {
        "as_of": last_ts.isoformat(),
        "age_seconds": age_seconds,
        "stale": stale,
        "warning": warning,
    }
