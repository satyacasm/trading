from __future__ import annotations

import polars as pl

from trading.contracts import (
    CANONICAL_BAR_SCHEMA,
    AssetClass,
    DataSource,
    NormalizedBatch,
    RawPayload,
)

SOURCE_BY_KEY = {
    "nse_cm_legacy": DataSource.NSE_CM_LEGACY,
}


def _blank_to_null(name: str) -> pl.Expr:
    return pl.when(pl.col(name).str.strip_chars() == "").then(None).otherwise(pl.col(name))


def _dec(name: str, precision: int = 18, scale: int = 4) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Decimal(precision, scale))


def _int(name: str, dtype: type[pl.DataType] = pl.Int64) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Float64).cast(dtype)


def _session_close_utc(date_expr: pl.Expr) -> pl.Expr:
    """15:30 Asia/Kolkata -> UTC, the session-close convention (finding N4)."""
    return (
        date_expr.cast(pl.Datetime("us"))
        .dt.offset_by("15h30m")
        .dt.replace_time_zone("Asia/Kolkata")
        .dt.convert_time_zone("UTC")
    )


class NseLegacyNormalizer:
    """Normalizes NSE cash bhavcopy from before the UDiFF migration."""

    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        try:
            source = SOURCE_BY_KEY[payload.source_key]
        except KeyError as exc:
            raise ValueError(
                f"NseLegacyNormalizer: unrecognised source key {payload.source_key!r}"
            ) from exc

        out = frame.select(
            exchange=pl.lit("NSE"),
            segment=pl.lit("CM"),
            symbol=pl.col("SYMBOL"),
            # Ruling S1 (task-18-brief.md): the same SYMBOL can legitimately
            # appear more than once a day under different SERIES values
            # (e.g. DHFL EQ vs. several DHFL NCD series) -- each is a
            # distinct security, not a duplicate.
            series=_blank_to_null("SERIES"),
            asset_class=pl.lit(AssetClass.EQUITY.value),
            expiry=pl.lit(None, dtype=pl.Date),
            strike=pl.lit(None, dtype=pl.Decimal(18, 4)),
            option_type=pl.lit(None, dtype=pl.String),
            isin=_blank_to_null("ISIN"),
            name=pl.lit(None, dtype=pl.String),
            ts=_session_close_utc(_blank_to_null("TIMESTAMP").str.to_date("%d-%b-%Y", strict=True)),
            open=_dec("OPEN"),
            high=_dec("HIGH"),
            low=_dec("LOW"),
            close=_dec("CLOSE"),
            prev_close=_dec("PREVCLOSE"),
            settle_price=pl.lit(None, dtype=pl.Decimal(18, 4)),
            underlying_price=pl.lit(None, dtype=pl.Decimal(18, 4)),
            volume=_int("TOTTRDQTY"),
            turnover=_dec("TOTTRDVAL", 22, 4),
            trades=_int("TOTALTRADES", pl.Int32),
            open_interest=pl.lit(None, dtype=pl.Int64),
            oi_change=pl.lit(None, dtype=pl.Int64),
            delivery_qty=pl.lit(None, dtype=pl.Int64),
            delivery_pct=pl.lit(None, dtype=pl.Decimal(7, 4)),
            lot_size=pl.lit(None, dtype=pl.Int32),
            tick_size=pl.lit(None, dtype=pl.Decimal(12, 6)),
        ).select(list(CANONICAL_BAR_SCHEMA))

        return NormalizedBatch(source=source, business_date=payload.business_date, frame=out)
