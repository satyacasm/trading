"""Enumerations for the paper-trading subsystem.

StrEnum throughout so values round-trip through Postgres `text` columns
and JSON without conversion, matching `trading.contracts.enums`.
"""

from __future__ import annotations

from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class Product(StrEnum):
    DELIVERY = "DELIVERY"
    INTRADAY = "INTRADAY"


class TimeInForce(StrEnum):
    DAY = "DAY"
    GTC = "GTC"


class ChargeType(StrEnum):
    BROKERAGE = "BROKERAGE"
    STT = "STT"
    EXCHANGE_TXN = "EXCHANGE_TXN"
    SEBI_FEE = "SEBI_FEE"
    STAMP_DUTY = "STAMP_DUTY"
    IPFT = "IPFT"
    GST = "GST"
    DP_CHARGES = "DP_CHARGES"
    TDS = "TDS"


class ChargeBasis(StrEnum):
    PERCENT_OF_TURNOVER = "PERCENT_OF_TURNOVER"
    FLAT_PER_ORDER = "FLAT_PER_ORDER"
    FLAT_PER_SCRIP_PER_DAY = "FLAT_PER_SCRIP_PER_DAY"
    PERCENT_OF_CHARGES = "PERCENT_OF_CHARGES"


class Rounding(StrEnum):
    NEAREST_RUPEE = "NEAREST_RUPEE"
    TWO_DECIMALS = "TWO_DECIMALS"


class EntryType(StrEnum):
    FILL = "FILL"
    CHARGE = "CHARGE"
    DEPOSIT = "DEPOSIT"
    # A perpetual's eight-hourly carry. Its own type, not a CHARGE: a
    # charge always costs the holder, and funding pays one side.
    FUNDING = "FUNDING"
    # A perpetual the exchange closed. Distinct from a breaker halt: the
    # breaker pauses a portfolio, this closes one position.
    LIQUIDATION = "LIQUIDATION"
