"""Pydantic models for the paper-trading subsystem.

Money is Decimal everywhere -- never float.

Models that cross the API boundary (Order, Position, Portfolio,
ChargeBreakdown) declare an explicit field serializer rendering Decimal as
a JSON number. Pydantic v2 renders Decimal as a *string* by default, which
is the defect Task 7b fixed for the market-data API and which silently
breaks arithmetic in any consumer.

ChargeSchedule and FillDecision deliberately carry no serializer: they are
process-internal and never leave this process. If a later task returns
either over HTTP, add the serializer there.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_serializer

from trading.paper.enums import (
    ChargeBasis,
    ChargeType,
    OrderStatus,
    OrderType,
    Product,
    Rounding,
    Side,
    TimeInForce,
)

_MONEY_FIELDS = (
    "brokerage",
    "stt",
    "exchange_txn",
    "sebi_fee",
    "stamp_duty",
    "ipft",
    "gst",
    "dp_charges",
)


class ChargeBreakdown(BaseModel):
    """Every statutory charge on one fill, itemised.

    Stored per component rather than as a total because §8's cost-drag
    report needs the breakdown and it cannot be reconstructed from a lump
    sum afterwards.
    """

    model_config = ConfigDict(frozen=True)

    brokerage: Decimal
    stt: Decimal
    exchange_txn: Decimal
    sebi_fee: Decimal
    stamp_duty: Decimal
    ipft: Decimal
    gst: Decimal
    dp_charges: Decimal

    @property
    def total(self) -> Decimal:
        return (
            self.brokerage
            + self.stt
            + self.exchange_txn
            + self.sebi_fee
            + self.stamp_duty
            + self.ipft
            + self.gst
            + self.dp_charges
        )

    @field_serializer(*_MONEY_FIELDS)
    def _money_as_number(self, v: Decimal) -> float:
        return float(v)


class ChargeSchedule(BaseModel):
    """One dated charge rule. Rates are data, not constants -- NSE cash
    transaction charges changed on 2026-03-01 and the backfill spans that
    boundary.

    Deliberately no money field serializer: this model is process-internal
    (loaded by Task 3's charge calculator) and never leaves this process.
    If a later task returns it over HTTP, add the serializer there.
    """

    model_config = ConfigDict(frozen=True)

    broker: str
    exchange: str
    asset_class: str
    product: Product
    charge_type: ChargeType
    basis: ChargeBasis
    applies_to_side: str
    rate: Decimal
    cap: Decimal | None
    rounding: Rounding
    gst_base_types: tuple[ChargeType, ...] = ()
    effective_from: date
    effective_to: date | None
    source_note: str


class Order(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: int
    portfolio_id: int
    instrument_id: int
    side: Side
    order_type: OrderType
    quantity: Decimal
    filled_quantity: Decimal
    limit_price: Decimal | None
    product: Product
    time_in_force: TimeInForce
    status: OrderStatus
    rationale: str
    submitted_at: datetime

    @property
    def remaining(self) -> Decimal:
        return self.quantity - self.filled_quantity

    @field_serializer("quantity", "filled_quantity")
    def _quantity_as_number(self, v: Decimal) -> float:
        return float(v)

    @field_serializer("limit_price")
    def _limit_price_as_number(self, v: Decimal | None) -> float | None:
        return float(v) if v is not None else None


class FillDecision(BaseModel):
    """The pure fill rules' output: fill this much at this price, caused by
    the price event at `tick_ts`.

    Deliberately no money field serializer: this model is process-internal
    (produced by Task 5's fill rules, consumed by the engine) and never
    leaves this process. If a later task returns it over HTTP, add the
    serializer there.
    """

    model_config = ConfigDict(frozen=True)

    quantity: Decimal
    price: Decimal
    tick_ts: datetime


class Position(BaseModel):
    model_config = ConfigDict(frozen=True)

    portfolio_id: int
    instrument_id: int
    quantity: Decimal
    avg_cost: Decimal
    realised_pnl: Decimal

    @field_serializer("quantity", "avg_cost", "realised_pnl")
    def _money_as_number(self, v: Decimal) -> float:
        return float(v)


class Portfolio(BaseModel):
    model_config = ConfigDict(frozen=True)

    portfolio_id: int
    user_id: int
    name: str
    base_currency: str
    initial_capital: Decimal
    cash_balance: Decimal
    status: str
    max_daily_loss: Decimal | None
    max_drawdown_pct: Decimal | None

    @field_serializer("initial_capital", "cash_balance")
    def _money_as_number(self, v: Decimal) -> float:
        return float(v)

    @field_serializer("max_daily_loss", "max_drawdown_pct")
    def _optional_money_as_number(self, v: Decimal | None) -> float | None:
        return float(v) if v is not None else None
