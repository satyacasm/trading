from __future__ import annotations

import polars as pl

from trading.contracts import NormalizedBatch, QuarantineRow, ValidationOutcome

# Natural key for duplicate detection. Validation runs BEFORE instrument
# resolution, so no instrument_id exists yet — this is the key the brief's
# prose calls "(instrument_id, ts)" but is actually the pre-resolution
# natural key plus ts (Ruling V2).
_KEY = ("exchange", "segment", "symbol", "expiry", "strike", "option_type", "ts")


class BarValidator:
    """Row-level invariants for canonical-schema bars. Never raises."""

    def validate(self, batch: NormalizedBatch) -> ValidationOutcome:
        frame = batch.frame.with_row_index("_row")

        frame = frame.with_columns(pl.col("_row").cum_count().over(list(_KEY)).gt(1).alias("_dup"))

        reason = (
            pl.when(pl.col("close").is_null() & (pl.col("asset_class") == "MF"))
            .then(pl.lit("nav_not_available"))
            .when(pl.col("close").is_null())
            .then(pl.lit("close_missing"))
            .when(pl.col("close") <= 0)
            .then(pl.lit("close_not_positive"))
            .when(pl.col("volume") < 0)
            .then(pl.lit("negative_volume"))
            .when(
                (pl.col("volume") > 0)
                & (
                    (pl.col("high") < pl.col("low"))
                    | (pl.col("high") < pl.col("open"))
                    | (pl.col("high") < pl.col("close"))
                    | (pl.col("low") > pl.col("open"))
                    | (pl.col("low") > pl.col("close"))
                )
            )
            .then(pl.lit("ohlc_inconsistent"))
            .when(pl.col("_dup"))
            .then(pl.lit("duplicate_key"))
            .otherwise(None)
            .alias("_reason")
        )

        frame = frame.with_columns(reason)

        bad = frame.filter(pl.col("_reason").is_not_null())
        good = frame.filter(pl.col("_reason").is_null()).drop("_row", "_dup", "_reason")

        quarantined = [
            QuarantineRow(reason=str(row.pop("_reason")), payload=row)
            for row in bad.drop("_row", "_dup").to_dicts()
        ]
        return ValidationOutcome(valid=good, quarantined=quarantined)
