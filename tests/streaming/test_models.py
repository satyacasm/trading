from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.streaming.models import Tick


def test_tick_round_trips_through_json() -> None:
    tick = Tick(
        instrument_id=42,
        ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        price=Decimal("65000.50"),
        quantity=Decimal("0.01"),
        side="buy",
    )

    restored = Tick.model_validate_json(tick.model_dump_json())

    assert restored.instrument_id == 42
    assert restored.price == Decimal("65000.50")
    assert restored.quantity == Decimal("0.01")
    assert restored.side == "buy"
    assert restored.ts == datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)


def test_tick_side_is_optional() -> None:
    tick = Tick(
        instrument_id=1,
        ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        price=Decimal("1.00"),
        quantity=Decimal("1.00"),
    )
    assert tick.side is None


def test_tick_rejects_a_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="tzinfo"):
        Tick(
            instrument_id=1,
            ts=datetime(2026, 8, 24, 12, 0, 0),  # no tzinfo
            price=Decimal("1.00"),
            quantity=Decimal("1.00"),
        )


# --- IMP-6: price/quantity must be positive --------------------------------


def test_tick_rejects_zero_price() -> None:
    """A price of 0 fills every resting market buy at 0.00 and crosses
    every resting limit buy (0 <= limit), firing the entire resting buy
    book across every portfolio on that instrument."""
    with pytest.raises(ValueError, match="price"):
        Tick(
            instrument_id=1,
            ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
            price=Decimal("0"),
            quantity=Decimal("1.00"),
        )


def test_tick_rejects_negative_price() -> None:
    with pytest.raises(ValueError, match="price"):
        Tick(
            instrument_id=1,
            ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
            price=Decimal("-1.00"),
            quantity=Decimal("1.00"),
        )


def test_tick_accepts_zero_quantity() -> None:
    """Unlike price, a zero quantity is not rejected: a live index feed
    (e.g. NIFTY 50) legitimately sends ltq=0 -- an index has no traded
    size, only a computed value -- see test_upstox_feed.py's
    test_parse_upstox_frame_handles_an_index_full_feed, which synthesises
    a real Upstox indexFF frame with ltq=0. decide_fill never reads
    Tick.quantity anyway (a fill's quantity comes from order.remaining),
    so 0 here carries none of price=0's hazard."""
    tick = Tick(
        instrument_id=1,
        ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
        price=Decimal("1.00"),
        quantity=Decimal("0"),
    )
    assert tick.quantity == Decimal("0")


def test_tick_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError, match="quantity"):
        Tick(
            instrument_id=1,
            ts=datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC),
            price=Decimal("1.00"),
            quantity=Decimal("-1.00"),
        )
