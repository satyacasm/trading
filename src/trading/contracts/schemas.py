from __future__ import annotations

import polars as pl

CANONICAL_BAR_SCHEMA: dict[str, pl.DataType] = {
    # identity (natural key)
    "exchange": pl.String(),
    "segment": pl.String(),
    "symbol": pl.String(),
    "asset_class": pl.String(),
    "expiry": pl.Date(),
    "strike": pl.Decimal(18, 4),
    "option_type": pl.String(),
    # descriptive
    "isin": pl.String(),
    "name": pl.String(),
    # time
    "ts": pl.Datetime("us", "UTC"),
    # prices
    "open": pl.Decimal(18, 4),
    "high": pl.Decimal(18, 4),
    "low": pl.Decimal(18, 4),
    "close": pl.Decimal(18, 4),
    "prev_close": pl.Decimal(18, 4),
    "settle_price": pl.Decimal(18, 4),
    "underlying_price": pl.Decimal(18, 4),
    # activity
    "volume": pl.Int64(),
    "turnover": pl.Decimal(22, 4),
    "trades": pl.Int32(),
    "open_interest": pl.Int64(),
    "oi_change": pl.Int64(),
    "delivery_qty": pl.Int64(),
    "delivery_pct": pl.Decimal(7, 4),
    # instrument attributes carried by the row (finding F3)
    "lot_size": pl.Int32(),
    "tick_size": pl.Decimal(12, 6),
}


def empty_canonical_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=CANONICAL_BAR_SCHEMA)


def assert_canonical(frame: pl.DataFrame) -> None:
    """Raise if a frame does not match the canonical contract exactly."""
    expected = set(CANONICAL_BAR_SCHEMA)
    actual = set(frame.columns)
    if missing := expected - actual:
        raise ValueError(f"missing canonical columns: {sorted(missing)}")
    if extra := actual - expected:
        raise ValueError(f"unexpected columns: {sorted(extra)}")
    for name, dtype in CANONICAL_BAR_SCHEMA.items():
        if frame.schema[name] != dtype:
            raise ValueError(f"column {name}: expected {dtype}, got {frame.schema[name]}")
