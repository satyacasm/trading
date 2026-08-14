from __future__ import annotations

from enum import IntEnum, StrEnum


class AssetClass(StrEnum):
    EQUITY = "EQUITY"
    INDEX = "INDEX"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    MF = "MF"
    CRYPTO = "CRYPTO"
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


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED_HOLIDAY = "SKIPPED_HOLIDAY"
    SKIPPED_NO_DATA = "SKIPPED_NO_DATA"
