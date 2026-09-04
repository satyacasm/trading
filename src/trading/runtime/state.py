"""Everything one run of a strategy accumulates.

Held in one mutable object rather than threaded through call signatures
because two collaborators need the same view of it: `LiveContext` writes
orders and logs into it, and `EventLoop` reads those out, fills them, and
writes cash and positions back. Passing it explicitly to both keeps the
sharing visible instead of hiding it in globals.

Every value inside is either a Decimal or a frozen pydantic model, so a
strategy handed a `Position` cannot rewrite the portfolio by mutating it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from trading.paper.models import Order, Position

__all__ = ["RunState"]


@dataclass
class RunState:
    now: datetime
    cash: Decimal
    starting_cash: Decimal
    positions: dict[int, Position] = field(default_factory=dict)
    orders: dict[int, Order] = field(default_factory=dict)
    # Submission order, kept separately from `orders` because dict
    # ordering is an implementation detail and D-S6 compares sequences.
    submissions: list[int] = field(default_factory=list)
    logs: list[dict[str, Any]] = field(default_factory=list)
    # How many of each instrument's bars have closed. `ctx.data` reads
    # strictly below this, which is the whole anti-lookahead mechanism.
    cursor: dict[int, int] = field(default_factory=dict)
    marks: dict[int, Decimal] = field(default_factory=dict)
    bar_calls: int = 0
    next_order_id: int = 1
    day_open_equity: Decimal | None = None
    peak_equity: Decimal | None = None
    breaker_reason: str | None = None
    # One (ts, equity, cash) triple per dispatched bar, appended where the
    # breaker already evaluates equity so the two can never disagree.
    # Money as strings for the reason outcome.py gives: JSON numbers are
    # IEEE 754 doubles, and a curve of subtly wrong equity is worse than
    # no curve.
    equity_curve: list[dict[str, str]] = field(default_factory=list)
    # One record per fill. Money as strings, like the curve, and every
    # charge component kept separately -- see RunOutcome.fill_ledger.
    fill_ledger: list[dict[str, str]] = field(default_factory=list)
