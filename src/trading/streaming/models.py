"""Contracts for the real-time streaming pipeline (Phase 1).

Distinct from `trading.contracts` (the six-stage EOD batch pipeline's
contracts): this is a live trade tick, not a canonical bar, and it never
touches TimescaleDB in this sub-project (see the design doc for why).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator


class Tick(BaseModel):
    """One trade, resolved to our instrument identity."""

    model_config = ConfigDict(frozen=True)

    instrument_id: int
    ts: datetime
    price: Decimal
    quantity: Decimal
    side: str | None = None

    @field_validator("ts")
    @classmethod
    def _ts_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Tick.ts must carry tzinfo (UTC in storage/wire format)")
        return value
