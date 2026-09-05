from __future__ import annotations

from enum import IntEnum, StrEnum


class AssetClass(StrEnum):
    EQUITY = "EQUITY"
    INDEX = "INDEX"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    MF = "MF"
    CRYPTO = "CRYPTO"
    # A perpetual is not a CRYPTO row with a flag: `load_schedules` and
    # `_BROKER_BY_ASSET_CLASS` both key on asset_class, so sharing CRYPTO
    # would silently apply spot's brokerage to a perpetual fill.
    PERP = "PERP"
    COMMODITY = "COMMODITY"


class OptionType(StrEnum):
    CE = "CE"
    PE = "PE"


class DataSource(IntEnum):
    """Persisted provenance codes. Append only; never renumber."""

    NSE_CM_UDIFF = 1
    NSE_FO_UDIFF = 2
    BSE_CM_UDIFF = 3
    NSE_CM_LEGACY = 4
    AMFI_NAV = 5
    BINANCE_WS = 6
    UPSTOX_HISTORICAL_CANDLE = 7
    UPSTOX_WS = 8
    BINANCE_FUTURES_KLINE = 9
    BINANCE_FUTURES_WS = 10


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED_HOLIDAY = "SKIPPED_HOLIDAY"
    SKIPPED_NO_DATA = "SKIPPED_NO_DATA"
