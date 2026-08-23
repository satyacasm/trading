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


def _legacy_date(name: str) -> pl.Expr:
    """Parse legacy TIMESTAMP, whose year NSE spells with two or four digits.

    Live-verified: 2020-07-13's bhavcopy carries "13-Jul-20" while every
    neighbouring day carries "13-JUL-2020" -- inside a file named
    cm13JUL2020bhav.csv. polars' "%d-%b-%Y" accepts "20" as the year 20 AD
    without complaint even under strict=True, so that day silently wrote
    2,001 rows dated 0020-07-13. Branching on the width of the year token is
    deterministic; a coalesce of the two formats is not, because "%d-%b-%Y"
    succeeds (wrongly) on the two-digit spelling and would always win.

    Anything that matches neither becomes null and is caught by the
    business-date guard in `normalize`, never stored.
    """
    col = _blank_to_null(name).str.strip_chars()
    year_width = col.str.split("-").list.get(2, null_on_oob=True).str.len_chars()
    return (
        pl.when(year_width == 2)
        .then(col.str.to_date("%d-%b-%y", strict=False))
        .otherwise(col.str.to_date("%d-%b-%Y", strict=False))
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
            ts=_session_close_utc(_legacy_date("TIMESTAMP")),
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

        # One legacy bhavcopy covers exactly one session, so a row dated
        # anything else means the date was misread or the wrong file was
        # served. Refusing the day is right: the alternative is what actually
        # happened, which is a whole session landing in the year 20 AD where
        # only min(ts) would ever reveal it.
        session_dates = out["ts"].dt.convert_time_zone("Asia/Kolkata").dt.date()
        if session_dates.null_count() or (session_dates != payload.business_date).any():
            offenders = sorted(
                {str(d) for d in session_dates.unique().to_list() if d != payload.business_date}
            )
            raise ValueError(
                f"NseLegacyNormalizer: {payload.source_key} {payload.business_date} contains "
                f"row date(s) that are not the file's own session: {offenders or ['unparseable']}"
            )

        return NormalizedBatch(source=source, business_date=payload.business_date, frame=out)
