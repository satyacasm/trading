"""Reference data a caller needs before it can size a perpetual order.

`paper.api._require_perp_order_is_tradable` refuses an order that is off
the contract's step or under its floors, which is right -- Binance would
refuse it too. But nothing served those numbers, so a caller could only
discover them by being rejected. DOGE steps by a whole coin, BTC by
0.001, and minimum notionals run 5 / 20 / 50: they are not guessable.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from pydantic import BaseModel

from trading.contracts.enums import AssetClass
from trading.streaming.db import get_db_connection

router = APIRouter()


class PerpContext(BaseModel):
    """Everything fixed about one perpetual, plus its latest funding print.

    Money and rates are strings for the reason `StringCandle` in
    `market_data_api` gives: JSON has no decimal type, and a step size
    read as a float is a step size that silently fails to divide.
    """

    instrument_id: int
    symbol: str
    step_size: str
    min_qty: str
    min_notional: str
    liquidation_fee: str | None
    max_leverage: str | None
    latest_funding_rate: str | None
    latest_funding_time: datetime | None
    latest_mark_price: str | None
    margin_tiers: list[dict[str, str]]


_INSTRUMENT = """
    SELECT symbol, asset_class FROM instruments WHERE instrument_id = %s
"""

_SPEC = """
    SELECT step_size, min_qty, min_notional, liquidation_fee
    FROM perp_contract_specs
    WHERE instrument_id = %s AND effective_to IS NULL
"""

_TIERS = """
    SELECT notional_floor, notional_cap, max_leverage, maintenance_rate, maintenance_amount
    FROM perp_margin_tiers WHERE instrument_id = %s ORDER BY notional_floor
"""

_LATEST_FUNDING = """
    SELECT funding_time, rate, mark_price FROM perp_funding
    WHERE instrument_id = %s ORDER BY funding_time DESC LIMIT 1
"""


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


@router.get("/perp-context/{instrument_id}", response_model=PerpContext)
def get_perp_context(
    instrument_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> PerpContext:
    instrument = conn.execute(_INSTRUMENT, (instrument_id,)).fetchone()
    if instrument is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )
    symbol, asset_class = instrument
    if asset_class != AssetClass.PERP.value:
        # Not an empty answer: empty filters read as "no constraints",
        # which is the opposite of the truth for anything else.
        raise HTTPException(
            status_code=404,
            detail=f"instrument_id={instrument_id} is not a perpetual (asset_class={asset_class})",
        )

    spec = conn.execute(_SPEC, (instrument_id,)).fetchone()
    if spec is None:
        # The same refusal the order path gives. A perpetual with no
        # current spec cannot be sized, and inventing one would invent a
        # trade the venue would never have accepted.
        raise HTTPException(
            status_code=404,
            detail=f"no current contract spec for instrument_id={instrument_id}",
        )
    step_size, min_qty, min_notional, liquidation_fee = spec

    tier_rows = conn.execute(_TIERS, (instrument_id,)).fetchall()
    tiers = [
        {
            "notional_floor": str(floor),
            "notional_cap": str(cap),
            "max_leverage": str(leverage),
            "maintenance_rate": str(rate),
            "maintenance_amount": str(amount),
        }
        for floor, cap, leverage, rate, amount in tier_rows
    ]

    funding = conn.execute(_LATEST_FUNDING, (instrument_id,)).fetchone()
    funding_time, funding_rate, mark_price = funding if funding is not None else (None, None, None)

    return PerpContext(
        instrument_id=instrument_id,
        symbol=symbol,
        step_size=str(step_size),
        min_qty=str(min_qty),
        min_notional=str(min_notional),
        liquidation_fee=_text(liquidation_fee),
        # The first tier's ceiling: leverage above it is refused for any
        # size, so it is the only one a caller can use without knowing
        # its notional yet.
        max_leverage=tiers[0]["max_leverage"] if tiers else None,
        latest_funding_rate=_text(funding_rate),
        latest_funding_time=funding_time,
        latest_mark_price=_text(mark_price),
        margin_tiers=tiers,
    )
