from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import polars as pl

from trading.contracts import (
    CANONICAL_BAR_SCHEMA,
    AssetClass,
    DataSource,
    NormalizedBatch,
    RawPayload,
)

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")
SESSION_CLOSE = time(15, 30)

# Ruling N1 (task-10-addendum.md): no `default=`. The five-entry map is complete
# for real data (STO/IDO/STF/IDF/STK counted across every live recon file), so an
# unmapped FinInstrmTp must raise rather than silently becoming EQUITY.
ASSET_CLASS_BY_TYPE = {
    "STK": AssetClass.EQUITY.value,
    "STO": AssetClass.OPTION.value,
    "IDO": AssetClass.OPTION.value,
    "STF": AssetClass.FUTURE.value,
    "IDF": AssetClass.FUTURE.value,
}

SOURCE_BY_KEY = {
    "nse_cm_udiff": DataSource.NSE_CM_UDIFF,
    "nse_fo_udiff": DataSource.NSE_FO_UDIFF,
    "bse_cm_udiff": DataSource.BSE_CM_UDIFF,
}


def _blank_to_null(name: str) -> pl.Expr:
    return pl.when(pl.col(name).str.strip_chars() == "").then(None).otherwise(pl.col(name))


def _dec(name: str, precision: int = 18, scale: int = 4) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Decimal(precision, scale))


def _int(name: str, dtype: type[pl.DataType] = pl.Int64) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Float64).cast(dtype)


class UdiffNormalizer:
    """Normalizes the UDiFF bhavcopy shared by NSE CM, NSE FO and BSE CM."""

    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        try:
            source = SOURCE_BY_KEY[payload.source_key]
        except KeyError as exc:
            raise ValueError(
                f"UdiffNormalizer: unrecognised source key {payload.source_key!r}"
            ) from exc

        trade_date = date.fromisoformat(frame["TradDt"][0])
        ts = datetime.combine(trade_date, SESSION_CLOSE, tzinfo=IST).astimezone(UTC)

        out = frame.select(
            exchange=pl.col("Src"),
            segment=pl.col("Sgmt"),
            symbol=pl.col("TckrSymb"),
            # Ruling S1 (task-18-brief.md): SctySrs carries the CM series
            # (EQ, BE, N2, GB, ...) and is empty for FO rows (verified in
            # docs/data-formats/eod-source-formats.md and against the FO
            # fixture), so this single expression correctly yields a real
            # series for CM and None for FO without segment-conditional
            # logic. The same symbol can legitimately appear more than once
            # a day under different CM series -- each is a distinct security.
            series=_blank_to_null("SctySrs"),
            # Ruling N1: replace_strict with no default -> raises on an unmapped
            # FinInstrmTp instead of silently mislabelling it EQUITY.
            asset_class=pl.col("FinInstrmTp").replace_strict(ASSET_CLASS_BY_TYPE),
            expiry=_blank_to_null("XpryDt").str.to_date("%Y-%m-%d", strict=False),
            strike=_dec("StrkPric"),
            option_type=_blank_to_null("OptnTp"),
            isin=_blank_to_null("ISIN"),
            name=_blank_to_null("FinInstrmNm"),
            ts=pl.lit(ts).cast(pl.Datetime("us", "UTC")),
            open=_dec("OpnPric"),
            high=_dec("HghPric"),
            low=_dec("LwPric"),
            close=_dec("ClsPric"),
            prev_close=_dec("PrvsClsgPric"),
            settle_price=_dec("SttlmPric"),
            underlying_price=_dec("UndrlygPric"),
            volume=_int("TtlTradgVol"),
            turnover=_dec("TtlTrfVal", 22, 4),
            trades=_int("TtlNbOfTxsExctd", pl.Int32),
            open_interest=_int("OpnIntrst"),
            oi_change=_int("ChngInOpnIntrst"),
            delivery_qty=pl.lit(None, dtype=pl.Int64),
            delivery_pct=pl.lit(None, dtype=pl.Decimal(7, 4)),
            lot_size=_int("NewBrdLotQty", pl.Int32),
            tick_size=pl.lit(None, dtype=pl.Decimal(12, 6)),
        ).select(list(CANONICAL_BAR_SCHEMA))

        return NormalizedBatch(source=source, business_date=payload.business_date, frame=out)
