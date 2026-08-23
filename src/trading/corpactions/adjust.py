"""Read-time price adjustment for corporate actions (spec §4.3, D10).

Adjusted prices are never stored (D10): `bars_daily` holds only the
unadjusted prices the exchange actually printed. `adjusted_bars` computes a
point-in-time adjusted *view* over that storage at read time, so a backtest
run `as_of` any date sees exactly the corporate-action knowledge that
existed on that date, and storage never has to be rewritten when a new split
lands.

Only SPLIT and BONUS actions participate in this adjustment (see
`_ACTION_QUERY`). DIVIDEND is deliberately excluded: folding it in would
produce a total-return series, a distinct concept from a price-adjusted
series that a later phase is responsible for.

`announced_at IS NULL` is treated as "always known" rather than "never
known" -- an action with no recorded announcement timestamp is visible to
every `as_of` query rather than none. That is the safer default for a feed
(NSE's corporate-actions endpoint, see
docs/data-formats/eod-source-formats.md §5) that in practice never
populates this field; see `ingest.py`'s module docstring for the ingestion
side of the same decision.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import polars as pl
from psycopg import Connection

# Ruling A2 (task-16 addendum): filter on `ts` directly with a half-open
# range, never `ts::date BETWEEN ...`.
#
# 1. `ts::date` casts using the *session* TimeZone. Bars are stamped
#    10:00Z; under a session zone west of UTC by more than ten hours the
#    cast lands on the previous calendar day, silently shifting every bar.
# 2. Wrapping `ts` in a cast is non-sargable and defeats the
#    `(instrument_id, ts)` primary key on a table designed to hold 250
#    million rows.
_BAR_QUERY = """
    SELECT ts, open, high, low, close, prev_close, volume
    FROM bars_daily
    WHERE instrument_id = %s AND ts >= %s AND ts < %s
    ORDER BY ts
"""

# announced_at IS NULL is treated as "always known" (see module docstring).
_ACTION_QUERY = """
    SELECT ex_date, ratio_from, ratio_to
    FROM corporate_actions
    WHERE instrument_id = %s
      AND action_type IN ('SPLIT', 'BONUS')
      AND ex_date <= %s
      AND (announced_at IS NULL OR announced_at::date <= %s)
    ORDER BY ex_date
"""


def adjustment_factors(
    conn: Connection, instrument_id: int, *, as_of: date
) -> list[tuple[date, Decimal]]:
    """Every SPLIT/BONUS ex_date known as of `as_of`, with its price factor.

    Ruling A1 (task-16 addendum): the plan's Interfaces section declared this
    as `(conn, instrument_id, start, end) -> pl.DataFrame` with `ex_date`/
    `factor` columns, but its own Step 3 code implemented
    `(conn, instrument_id, start, end, as_of)` returning
    `list[tuple[date, Decimal]]` and never used `start`/`end`. Settled as
    `(conn, instrument_id, *, as_of)`, dropping the dead parameters and
    making `as_of` keyword-only so no caller can transpose it with a date
    bound.
    """
    rows = conn.execute(_ACTION_QUERY, (instrument_id, as_of, as_of)).fetchall()
    return [
        (ex_date, Decimal(ratio_from) / Decimal(ratio_to))
        for ex_date, ratio_from, ratio_to in rows
        if ratio_to
    ]


def adjusted_bars(
    conn: Connection, instrument_id: int, start: date, end: date, *, as_of: date
) -> pl.DataFrame:
    """Return bars in [start, end] scaled to `as_of` terms.

    Storage is never modified (D10). A bar at `t` is scaled by the product
    of every action's `ratio_from/ratio_to` whose `ex_date` falls in
    `(t, as_of]` -- i.e. a *reverse* cumulative product over ex-dates, since
    an action affects every bar strictly before it, never bars after it.
    """
    lower = datetime(start.year, start.month, start.day, tzinfo=UTC)
    upper = datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)
    bars = conn.execute(_BAR_QUERY, (instrument_id, lower, upper)).fetchall()
    frame = pl.DataFrame(
        bars,
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "open": pl.Decimal(18, 4),
            "high": pl.Decimal(18, 4),
            "low": pl.Decimal(18, 4),
            "close": pl.Decimal(18, 4),
            "prev_close": pl.Decimal(18, 4),
            "volume": pl.Int64(),
        },
        orient="row",
    )
    if frame.height == 0:
        return frame

    actions = adjustment_factors(conn, instrument_id, as_of=as_of)
    if not actions:
        return frame

    def factor_for(bar_day: date) -> Decimal:
        """Product of every action strictly AFTER this bar."""
        result = Decimal(1)
        for ex_date, factor in actions:
            if ex_date > bar_day:
                result *= factor
        return result

    factors = [factor_for(ts.date()) for ts in frame["ts"]]
    factor_col = pl.Series("factor", factors, dtype=pl.Decimal(18, 8))

    return (
        frame.with_columns(factor_col)
        .with_columns(
            *[
                (pl.col(c) * pl.col("factor")).cast(pl.Decimal(18, 4)).alias(c)
                for c in ("open", "high", "low", "close", "prev_close")
            ],
            (pl.col("volume") / pl.col("factor")).round(0).cast(pl.Int64).alias("volume"),
        )
        .drop("factor")
    )
