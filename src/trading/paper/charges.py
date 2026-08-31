"""The Indian cost model.

`compute_charges` is pure -- no DB access, no clock -- so it is
exhaustively testable and Phase 3's backtest engine can call it a million
times without touching Postgres. Schedules are loaded separately and
passed in.

Rates are data, not constants: NSE cash transaction charges were revised
0.00297% -> 0.00307% effective 2026-03-01, and the intraday backfill
spans that boundary, so a hardcoded rate would misprice most of the
historical period.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from psycopg import Connection

from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding, Side
from trading.paper.models import ChargeBreakdown, ChargeSchedule

_TWO_DP = Decimal("0.01")
_ONE = Decimal("1")


class MissingChargeSchedule(Exception):
    """No charge schedule covers this instrument, product, and date.

    Raised rather than defaulting to zero: a silently-zero charge produces
    a P&L that looks correct and is systematically optimistic, which is the
    most dangerous failure mode in this subsystem.
    """


def load_schedules(
    conn: Connection,
    broker: str,
    exchange: str,
    asset_class: str,
    product: Product,
    on: date,
) -> list[ChargeSchedule]:
    """Every charge rule in force for this combination on `on`."""
    rows = conn.execute(
        "SELECT broker, exchange, asset_class, product, charge_type, basis,"
        " applies_to_side, rate, cap, rounding, gst_base_types,"
        " effective_from, effective_to, source_note"
        " FROM charge_schedules"
        " WHERE broker=%s AND exchange=%s AND asset_class=%s AND product=%s"
        "   AND effective_from <= %s"
        "   AND (effective_to IS NULL OR effective_to > %s)",
        (broker, exchange, asset_class, product.value, on, on),
    ).fetchall()
    return [
        ChargeSchedule(
            broker=r[0],
            exchange=r[1],
            asset_class=r[2],
            product=Product(r[3]),
            charge_type=ChargeType(r[4]),
            basis=ChargeBasis(r[5]),
            applies_to_side=r[6],
            rate=r[7],
            cap=r[8],
            rounding=Rounding(r[9]),
            gst_base_types=tuple(ChargeType(t) for t in (r[10].split(",") if r[10] else [])),
            effective_from=r[11],
            effective_to=r[12],
            source_note=r[13],
        )
        for r in rows
    ]


def _round(value: Decimal, rounding: Rounding) -> Decimal:
    if rounding is Rounding.NEAREST_RUPEE:
        return value.quantize(_ONE, rounding=ROUND_HALF_UP)
    return value.quantize(_TWO_DP, rounding=ROUND_HALF_UP)


def _applies(schedule: ChargeSchedule, side: Side) -> bool:
    return schedule.applies_to_side in ("BOTH", side.value)


def compute_charges(
    schedules: Sequence[ChargeSchedule],
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> ChargeBreakdown:
    """Itemised charges for one fill. Pure.

    GST is computed last, over the named subset of charge types its own
    schedule row declares -- never as a multiplier on the total, because
    it excludes STT and stamp duty and the included set differs between
    brokers.
    """
    if not schedules:
        raise MissingChargeSchedule(
            "no charge schedule covers this fill; refusing to compute a silently-zero cost"
        )

    turnover = quantity * price
    amounts: dict[ChargeType, Decimal] = dict.fromkeys(ChargeType, Decimal("0"))
    gst_schedule: ChargeSchedule | None = None

    for s in schedules:
        if s.charge_type is ChargeType.GST:
            gst_schedule = s
            continue
        if not _applies(s, side):
            continue

        if s.basis is ChargeBasis.PERCENT_OF_TURNOVER:
            raw = turnover * s.rate
        elif s.basis in (
            ChargeBasis.FLAT_PER_ORDER,
            ChargeBasis.FLAT_PER_SCRIP_PER_DAY,
        ):
            raw = s.rate
        else:
            continue  # PERCENT_OF_CHARGES only ever applies to GST

        if s.cap is not None:
            raw = min(raw, s.cap)
        amounts[s.charge_type] = _round(raw, s.rounding)

    if gst_schedule is not None and _applies(gst_schedule, side):
        base = sum((amounts[t] for t in gst_schedule.gst_base_types), Decimal("0"))
        amounts[ChargeType.GST] = _round(base * gst_schedule.rate, gst_schedule.rounding)

    return ChargeBreakdown(
        brokerage=amounts[ChargeType.BROKERAGE],
        stt=amounts[ChargeType.STT],
        exchange_txn=amounts[ChargeType.EXCHANGE_TXN],
        sebi_fee=amounts[ChargeType.SEBI_FEE],
        stamp_duty=amounts[ChargeType.STAMP_DUTY],
        ipft=amounts[ChargeType.IPFT],
        gst=amounts[ChargeType.GST],
        dp_charges=amounts[ChargeType.DP_CHARGES],
    )
