"""The live `Context` -- what a strategy actually holds.

**It subclasses the offline stub rather than reimplementing it** (design
D-S2). The classes in `platform_sdk` that raise `NotOnThisPlatform` are
exactly the ones a strategy only ever *receives*: `Context`, `DataAccess`,
`PortfolioView`. The ones a strategy constructs -- `StrategyManifest`,
`Param`, `Query`, `DataRequest`, and the `Strategy` base -- are real
working code with real validation already in them.

So there is no second SDK, no `sys.modules` substitution, and no
de-stubbed copy to keep in step. A strategy imports the same
`platform_sdk` it type-checked against offline, and shape drift between
the stub and the runtime is impossible because the runtime is a subclass
of the stub. A method added to the stub and not implemented here raises
`NotOnThisPlatform` at exactly its call site -- loud and correct, rather
than silently diverging.

The order path deliberately splits its failures in two, following
contract §6. A **contract violation** -- a float quantity, an empty
rationale, a LIMIT without a price -- raises, because it is a bug in the
strategy's code that the author must see immediately and that the offline
stub would already have caught. A **business rejection** -- an unknown
instrument, a non-positive size -- does not raise: it produces a REJECTED
order delivered through `on_order_update`, because §6 makes a rejection a
normal outcome the strategy must survive, and a smoke run that crashed on
one would never test that it does.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal
from typing import Any

from trading.agent_contract import platform_sdk
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import Order, Position
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState

__all__ = ["SMOKE_PORTFOLIO_ID", "LiveContext"]

# The smoke run is one strategy against one portfolio (contract D6), and
# nothing is persisted, so the id is a constant rather than a lookup.
SMOKE_PORTFOLIO_ID = 1


class LiveDataAccess(platform_sdk.DataAccess):
    def __init__(self, state: RunState, bars: InMemoryBars) -> None:
        self._state = state
        self._bars = bars

    def bars(  # type: ignore[override]
        self, instrument_id: int, *, interval: str = "1m", count: int = 100
    ) -> list[BarRecord]:
        closed = self._state.cursor.get(instrument_id, 0)
        history = self._bars.history(instrument_id, closed)
        return list(history[-count:]) if count > 0 else []

    def last(self, instrument_id: int) -> BarRecord | None:  # type: ignore[override]
        recent = self.bars(instrument_id, count=1)
        return recent[-1] if recent else None

    def lot_size(self, instrument_id: int) -> int | None:
        # Point-in-time lot history is not shipped into the sandbox, and
        # inventing a lot size would size every F&O order wrong in a way
        # the strategy could not detect. None is the honest answer, and
        # it is the documented value for instruments with no lot concept.
        return None


class LivePortfolioView(platform_sdk.PortfolioView):
    def __init__(self, state: RunState) -> None:
        self._state = state

    @property
    def cash(self) -> Decimal:
        return self._state.cash

    @property
    def positions(self) -> dict[int, Position]:  # type: ignore[override]
        # A copy: the dict is the runtime's, and a strategy that cleared
        # it would silently erase its own holdings.
        return dict(self._state.positions)

    @property
    def equity(self) -> Decimal:
        total = self._state.cash
        for position in self._state.positions.values():
            if position.quantity == 0:
                continue
            mark = self._state.marks.get(position.instrument_id)
            if mark is not None:
                total += position.quantity * mark
        return total


class LiveContext(platform_sdk.Context):
    def __init__(
        self, state: RunState, bars: InMemoryBars, portfolio_id: int = SMOKE_PORTFOLIO_ID
    ) -> None:
        self._state = state
        self._bars = bars
        self._portfolio_id = portfolio_id
        self._universe = frozenset(bars.instruments())
        self.data = LiveDataAccess(state, bars)
        self.portfolio = LivePortfolioView(state)
        self.state: dict[str, Any] = {}

    @property
    def now(self):  # type: ignore[no-untyped-def]
        return self._state.now

    def _reject(self, order: Order, reason: str) -> int:
        rejected = order.model_copy(
            update={"status": OrderStatus.REJECTED, "rejection_reason": reason}
        )
        self._state.orders[rejected.order_id] = rejected
        return rejected.order_id

    def order(
        self,
        instrument_id: int,
        *,
        side: str,
        quantity: Decimal,
        rationale: str,
        order_type: str = "MARKET",
        limit_price: Decimal | None = None,
        product: str = "DELIVERY",
        time_in_force: str = "DAY",
    ) -> int:
        # Contract violations raise, via the stub's own checks: float
        # money, empty rationale, LIMIT without a price, MARKET with one.
        # Deliberately called for its validation, and its `NotOnThisPlatform`
        # is what tells us validation passed.
        with contextlib.suppress(platform_sdk.NotOnThisPlatform):
            super().order(
                instrument_id,
                side=side,  # type: ignore[arg-type]
                quantity=quantity,
                rationale=rationale,
                order_type=order_type,  # type: ignore[arg-type]
                limit_price=limit_price,
                product=product,  # type: ignore[arg-type]
                time_in_force=time_in_force,  # type: ignore[arg-type]
            )

        order_id = self._state.next_order_id
        self._state.next_order_id += 1
        self._state.submissions.append(order_id)

        order = Order(
            order_id=order_id,
            portfolio_id=self._portfolio_id,
            instrument_id=instrument_id,
            side=Side(side),
            order_type=OrderType(order_type),
            quantity=quantity,
            filled_quantity=Decimal("0"),
            limit_price=limit_price,
            product=Product(product),
            time_in_force=TimeInForce(time_in_force),
            status=OrderStatus.OPEN,
            rationale=rationale,
            submitted_at=self._state.now,
        )

        if instrument_id not in self._universe:
            return self._reject(
                order,
                f"instrument_id={instrument_id} is not in this run's universe; "
                "a strategy may only trade what its manifest declared",
            )
        if quantity <= 0:
            return self._reject(order, f"quantity must be positive, got {quantity}")

        self._state.orders[order_id] = order
        return order_id

    def cancel(self, order_id: int) -> None:
        order = self._state.orders.get(order_id)
        if order is None:
            return
        if order.status not in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED):
            return
        self._state.orders[order_id] = order.model_copy(update={"status": OrderStatus.CANCELLED})

    def log(self, event: str, **fields: Any) -> None:
        self._state.logs.append({"ts": self._state.now.isoformat(), "event": event, **fields})
