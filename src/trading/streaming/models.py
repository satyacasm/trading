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
    """One trade, resolved to our instrument identity.

    `price` must be strictly positive (IMP-6). This is not a theoretical
    edge case: `trading.paper.fills.decide_fill` would fill every resting
    market buy at `0.00` *and* cross every resting limit buy (`0 <=
    limit`), firing the entire resting buy book across every portfolio
    holding that instrument, silently, on a single bad `price=0` tick.

    `quantity` gets the weaker `>= 0` (only negative is rejected), not the
    same `> 0` as price -- a zero-quantity tick is not hypothetical, it is
    exactly what a live index feed sends: `test_parse_upstox_frame_handles_
    an_index_full_feed` synthesises a real Upstox `indexFF` frame (e.g.
    NIFTY 50) with `ltq=0`, because an index has no traded size, only a
    computed value. Rejecting that as malformed would silently drop every
    index tick in production. `decide_fill` never reads `Tick.quantity`
    anyway (a fill's quantity comes from `order.remaining`), so `0` here
    carries none of `price=0`'s hazard. A *negative* quantity has no
    legitimate source either way and is rejected.

    Every construction site (`upstox_feed.parse_upstox_frame`,
    `binance_feed.parse_trade_message`) already wraps `Tick(...)` in a
    `try/except (ValueError, ...)` that logs and drops the message rather
    than raising into the caller's loop -- `pydantic.ValidationError`
    subclasses `ValueError`, so these validators slot into that existing
    malformed-tick path for free.
    """

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

    @field_validator("price")
    @classmethod
    def _price_must_be_positive(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError(f"Tick.price must be positive, got {value}")
        return value

    @field_validator("quantity")
    @classmethod
    def _quantity_must_not_be_negative(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError(f"Tick.quantity must not be negative, got {value}")
        return value


class Bar(BaseModel):
    """One complete, authoritative minute bar as delivered whole by a
    provider (e.g. Upstox's `marketOHLC` `I1` entry) -- unlike `Tick`, this
    is never assembled by our own aggregation logic."""

    model_config = ConfigDict(frozen=True)

    instrument_id: int
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @field_validator("ts")
    @classmethod
    def _ts_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Bar.ts must carry tzinfo (UTC in storage/wire format)")
        return value
