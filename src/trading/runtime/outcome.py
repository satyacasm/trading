"""What one execution of a strategy produced.

Every money value is a **string**, not a Decimal and never a float. This
crosses a process boundary as JSON on its way out of the container, and
the same reasoning that makes the inbound payload text applies to the
result: a number here would be an IEEE 754 double, and a smoke run that
reported subtly wrong cash would be worse than one that reported none.

`OrderSnapshot` is the unit the determinism check compares (D-S6), which
is why it is frozen, fully ordered, and carries no object references --
two runs of the same payload must produce tuples that are equal or
unequal for reasons visible in the tuple itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["OrderSnapshot", "RunOutcome"]


@dataclass(frozen=True)
class OrderSnapshot:
    order_id: int
    instrument_id: int
    side: str
    order_type: str
    quantity: str
    limit_price: str | None
    status: str
    submitted_at: str


@dataclass(frozen=True)
class RunOutcome:
    ok: bool
    bar_calls: int
    orders: tuple[OrderSnapshot, ...]
    fills: int
    rejections: tuple[str, ...]
    final_cash: str
    final_equity: str
    breaker_reason: str | None = None
    logs: tuple[dict[str, Any], ...] = ()
    error: str | None = None
    crashed_at: dict[str, Any] | None = None
