from __future__ import annotations

import polars as pl

from trading.contracts import (
    CANONICAL_BAR_SCHEMA,
    AssetClass,
    DataSource,
    NormalizedBatch,
    RawPayload,
)

# Ruling N2 (task-10-addendum.md): both AMFI source keys map to the SAME
# DataSource. amfi_nav is the latest-NAV snapshot (AmfiNavParser), amfi_nav_history
# is the per-date historical file (AmfiNavHistoryParser); both parsers emit the
# identical column set on purpose so one normalizer serves both.
SOURCE_BY_KEY = {
    "amfi_nav": DataSource.AMFI_NAV,
    "amfi_nav_history": DataSource.AMFI_NAV,
}

# The AMFI convention for an unpublished NAV. cast(..., strict=False) turns this
# (and any other unparseable token) into null rather than raising.
_NAV = pl.col("nav").cast(pl.Decimal(18, 4), strict=False)


class AmfiNormalizer:
    """Normalizes AMFI NAV rows (both the latest-snapshot and historical formats)."""

    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        try:
            source = SOURCE_BY_KEY[payload.source_key]
        except KeyError as exc:
            raise ValueError(
                f"AmfiNormalizer: unrecognised source key {payload.source_key!r}"
            ) from exc

        out = frame.select(
            exchange=pl.lit("AMFI"),
            segment=pl.lit("MF"),
            symbol=pl.col("scheme_code"),
            asset_class=pl.lit(AssetClass.MF.value),
            expiry=pl.lit(None, dtype=pl.Date),
            strike=pl.lit(None, dtype=pl.Decimal(18, 4)),
            option_type=pl.lit(None, dtype=pl.String),
            # Growth-plan ISIN is populated far more often than the reinvestment
            # one (12 vs 30 nulls in the committed historical fixture); fall back
            # to isin_reinvest only when isin_growth is absent.
            isin=pl.coalesce(pl.col("isin_growth"), pl.col("isin_reinvest")),
            name=pl.col("scheme_name"),
            # Ruling N3: ts is built PER ROW from that row's own nav_date, never
            # from payload.business_date or a single first-row value. A stale
            # nav_date (a dead/suspended scheme) is a true fact and must survive.
            ts=(
                pl.col("nav_date")
                .str.to_date("%d-%b-%Y", strict=True)
                .cast(pl.Datetime("us"))
                .dt.offset_by("15h30m")
                .dt.replace_time_zone("Asia/Kolkata")
                .dt.convert_time_zone("UTC")
            ),
            open=_NAV,
            high=_NAV,
            low=_NAV,
            close=_NAV,
            prev_close=pl.lit(None, dtype=pl.Decimal(18, 4)),
            settle_price=pl.lit(None, dtype=pl.Decimal(18, 4)),
            underlying_price=pl.lit(None, dtype=pl.Decimal(18, 4)),
            volume=pl.lit(None, dtype=pl.Int64),
            turnover=pl.lit(None, dtype=pl.Decimal(22, 4)),
            trades=pl.lit(None, dtype=pl.Int32),
            open_interest=pl.lit(None, dtype=pl.Int64),
            oi_change=pl.lit(None, dtype=pl.Int64),
            delivery_qty=pl.lit(None, dtype=pl.Int64),
            delivery_pct=pl.lit(None, dtype=pl.Decimal(7, 4)),
            lot_size=pl.lit(None, dtype=pl.Int32),
            tick_size=pl.lit(None, dtype=pl.Decimal(12, 6)),
        ).select(list(CANONICAL_BAR_SCHEMA))

        return NormalizedBatch(source=source, business_date=payload.business_date, frame=out)
