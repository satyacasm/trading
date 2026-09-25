"""One query, two callers: what price to trust for an instrument right
now, and whether it is too old to trust at all.

Both `_require_sufficient_cash` (paper/api.py) and `_load_marks`
(paper/engine.py) priced a MARKET order or a mark from the latest
`bars_intraday` close with no freshness check -- a feed that paused for
an hour still looked like a live price (design §6). This is the shared
check; the two callers disagree, correctly, on what to DO with a stale
answer -- one refuses, the other logs and carries on -- so that choice
stays with them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from psycopg import Connection

__all__ = ["StalePrice", "latest_reference_price"]


@dataclass(frozen=True)
class StalePrice:
    instrument_id: int
    ts: datetime
    age_seconds: float


def latest_reference_price(
    conn: Connection, instrument_id: int, *, now: datetime, max_age: timedelta
) -> tuple[Decimal, datetime] | StalePrice | None:
    """The latest `bars_intraday` close for `instrument_id`, or `None`
    if there has never been one, or a `StalePrice` if the latest is
    older than `max_age` as of `now`."""
    row = conn.execute(
        "SELECT close, ts FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
        (instrument_id,),
    ).fetchone()
    if row is None:
        return None
    close, ts = row
    age = (now - ts).total_seconds()
    if age > max_age.total_seconds():
        return StalePrice(instrument_id=instrument_id, ts=ts, age_seconds=age)
    return close, ts
