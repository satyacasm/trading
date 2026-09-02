"""Pure fill rules: given an order and one price event, fill or not.

No I/O, no clock, no DB. This is the module Phase 3's backtest engine
reuses unchanged -- it is fed bars instead of ticks, which is what §6's
"one engine, two clock speeds" means in practice.
"""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from trading.paper.enums import OrderStatus, OrderType, Side
from trading.paper.models import FillDecision, Order

_TWO_DP = Decimal("0.01")
_BPS = Decimal("10000")

_FILLABLE = (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING)


def decide_fill(
    order: Order,
    tick_price: Decimal,
    tick_ts: datetime,
    slippage_bps: Decimal,
) -> FillDecision | None:
    """Whether this price event fills this order, and at what price."""
    if order.status not in _FILLABLE:
        return None
    if order.remaining <= 0:
        return None

    # Anti-lookahead: a price that printed before the order existed can
    # never have filled it.
    if tick_ts < order.submitted_at:
        return None

    if order.order_type is OrderType.MARKET:
        # Slippage always moves against the order.
        drift = tick_price * slippage_bps / _BPS
        price = tick_price + drift if order.side is Side.BUY else tick_price - drift
        price = price.quantize(_TWO_DP, rounding=ROUND_HALF_UP)
    else:
        limit = order.limit_price
        if limit is None:
            return None
        crossed = tick_price <= limit if order.side is Side.BUY else tick_price >= limit
        if not crossed:
            return None
        # Fill at the limit, not at the better tick price. Real venues
        # sometimes grant improvement; assuming it here would flatter every
        # limit order and every backtest built on this engine.
        price = limit

    return FillDecision(quantity=order.remaining, price=price, tick_ts=tick_ts)
