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
