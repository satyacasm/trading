"""Typed no-op stubs of the strategy runtime, shipped in the Agent Contract
bundle.

An external agent writes a strategy against `STRATEGY_CONTRACT.md`. Before
uploading it, the author should be able to import it, type-check it, and
run a linter over it locally -- catching a misspelled method, a float where
a Decimal belongs, or a call that does not exist, without a round trip
through the platform. That is what this module is for.

**Every stub raises.** None of them returns a plausible-looking value,
because the failure mode that matters here is an agent's local dry run
appearing to succeed: an empty bar list and a zero cash balance would let a
strategy "run" locally, produce no orders, and look fine. An import error
or a loud `NotOnThisPlatform` is a far better outcome than a green run that
proves nothing. The stub's job is to make shape errors visible, not to
simulate anything.

The three checks it *does* perform eagerly -- non-empty rationale, Decimal
money, and no wall clock -- are the contract rules an agent is most likely
to break, and each is cheaper to discover here than as a 422 after upload.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Protocol

__all__ = [
    "Bar",
    "Context",
    "ExpiryEvent",
    "InstrumentId",
    "NotOnThisPlatform",
    "Order",
    "OrderId",
    "OrderUpdate",
    "Param",
    "Position",
    "Strategy",
    "StrategyManifest",
    "Tick",
]

InstrumentId = int
OrderId = int

BarInterval = Literal["1m", "5m", "15m", "1h", "1d"]
SideName = Literal["BUY", "SELL"]
OrderTypeName = Literal["MARKET", "LIMIT"]
ProductName = Literal["DELIVERY", "INTRADAY"]
TimeInForceName = Literal["DAY", "GTC"]


class NotOnThisPlatform(RuntimeError):
    """Raised by every stub that would need real platform state.

    Seeing this locally means the code reached a genuine runtime call and
    the shape was right -- it is the expected outcome of a local dry run,
    not a defect in the strategy.
    """


def _stub(what: str) -> NotOnThisPlatform:
    return NotOnThisPlatform(
        f"{what} needs the platform runtime; this is the offline SDK stub. "
        "Type-check and lint against it, then upload the strategy to run it."
    )


def _require_decimal(name: str, value: object) -> Decimal:
    """Money and quantities are Decimal end to end, never float.

    Checked here rather than left to the server because a float quantity is
    the single most likely mistake in generated code, and binary floating
    point in a money path is precisely what this platform's cost model
    exists to avoid.
    """
    if isinstance(value, float):
        raise TypeError(
            f"{name} must be a Decimal, not a float -- money and quantities are "
            f"Decimal end to end on this platform. Use Decimal({value!r}) "
            "(from a string, e.g. Decimal('1.5'), never Decimal(1.5))."
        )
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal, got {type(value).__name__}")
    return value


# --- Data structures ---------------------------------------------------------
#
# Protocols rather than dataclasses: a strategy reads these, never
# constructs them, and a Protocol documents the readable surface without
# implying the runtime hands back this exact class.


class Bar(Protocol):
    instrument_id: InstrumentId
    ts: datetime
    """UTC, and marks the START of the interval."""
    interval_sec: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    trades: int | None
    open_interest: int | None
    oi_change: int | None


class Tick(Protocol):
    instrument_id: InstrumentId
    ts: datetime
    price: Decimal
    """Strictly positive."""
    quantity: Decimal
    """May be zero -- an index tick has no traded size."""
    side: str | None


class Order(Protocol):
    order_id: OrderId
    instrument_id: InstrumentId
    side: SideName
    order_type: OrderTypeName
    quantity: Decimal
    filled_quantity: Decimal
    limit_price: Decimal | None
    product: ProductName
    time_in_force: TimeInForceName
    status: str
    rationale: str
    rejection_reason: str | None


class Position(Protocol):
    instrument_id: InstrumentId
    quantity: Decimal
    avg_cost: Decimal
    realised_pnl: Decimal


class OrderUpdate(Protocol):
    order: Order
    """The order in its new state."""
    previous_status: str


class ExpiryEvent(Protocol):
    instrument_id: InstrumentId
    settlement_price: Decimal
    quantity: Decimal


# --- Manifest ----------------------------------------------------------------


class Param:
    """One tunable parameter, with bounds a sweep can respect."""

    def __init__(self, type_: type, *, default: Any, bounds: tuple[Any, Any] | None = None) -> None:
        self.type = type_
        self.default = default
        self.bounds = bounds


class InstrumentRef:
    """One instrument named explicitly."""

    def __init__(self, *, exchange: str, segment: str, symbol: str) -> None:
        self.exchange = exchange
        self.segment = segment
        self.symbol = symbol


class Query:
    """A universe resolved point-in-time.

    Resolution happens against `listed_on`/`delisted_on` at `ctx.now`, so a
    backtest over 2023 sees the instruments that existed in 2023 --
    including ones since delisted. That is the survivorship-bias guarantee;
    it is why a query exists at all rather than only an explicit list.
    """

    def __init__(
        self,
        *,
        asset_class: str | None = None,
        exchange: str | None = None,
        index: str | None = None,
    ) -> None:
        self.asset_class = asset_class
        self.exchange = exchange
        self.index = index


class DataRequest:
    def __init__(self, *, bars: BarInterval, ticks: bool = False, history_bars: int = 100) -> None:
        self.bars = bars
        self.ticks = ticks
        self.history_bars = history_bars


class StrategyManifest:
    def __init__(
        self,
        *,
        name: str,
        version: str,
        universe: list[InstrumentRef] | Query,
        data: DataRequest,
        capital: Decimal,
        base_currency: str,
        params: dict[str, Param] | None = None,
        max_daily_loss: Decimal | None = None,
        max_drawdown_pct: Decimal | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self.universe = universe
        self.data = data
        self.capital = _require_decimal("capital", capital)
        self.base_currency = base_currency
        self.params = params or {}
        self.max_daily_loss = (
            None if max_daily_loss is None else _require_decimal("max_daily_loss", max_daily_loss)
        )
        self.max_drawdown_pct = (
            None
            if max_drawdown_pct is None
            else _require_decimal("max_drawdown_pct", max_drawdown_pct)
        )


# --- Context -----------------------------------------------------------------


class DataAccess:
    """Point-in-time history. Cannot return anything later than `ctx.now`."""

    def bars(
        self, instrument_id: InstrumentId, *, interval: BarInterval = "1m", count: int = 100
    ) -> list[Bar]:
        raise _stub("ctx.data.bars")

    def last(self, instrument_id: InstrumentId) -> Bar | None:
        raise _stub("ctx.data.last")

    def lot_size(self, instrument_id: InstrumentId) -> int | None:
        """The lot size in force at `ctx.now`, or None where the instrument
        has no lot concept (equity cash, crypto).

        A method rather than a field on the instrument record, deliberately:
        lot sizes are revised over time, so this is point-in-time data. A
        static field would invite caching a 2026 lot size into a 2022
        backtest and sizing every F&O order wrong.
        """
        raise _stub("ctx.data.lot_size")


class PortfolioView:
    @property
    def cash(self) -> Decimal:
        raise _stub("ctx.portfolio.cash")

    @property
    def positions(self) -> dict[InstrumentId, Position]:
        raise _stub("ctx.portfolio.positions")

    @property
    def equity(self) -> Decimal:
        raise _stub("ctx.portfolio.equity")


class Context:
    """Everything a strategy can reach. There is nothing else."""

    def __init__(self) -> None:
        self.data = DataAccess()
        self.portfolio = PortfolioView()
        self.state: dict[str, Any] = {}

    @property
    def now(self) -> datetime:
        """The simulation clock, UTC. The only clock available -- never
        `datetime.now()`, which the sandbox does not provide."""
        raise _stub("ctx.now")

    def order(
        self,
        instrument_id: InstrumentId,
        *,
        side: SideName,
        quantity: Decimal,
        rationale: str,
        order_type: OrderTypeName = "MARKET",
        limit_price: Decimal | None = None,
        product: ProductName = "DELIVERY",
        time_in_force: TimeInForceName = "DAY",
    ) -> OrderId:
        _require_decimal("quantity", quantity)
        if limit_price is not None:
            _require_decimal("limit_price", limit_price)
        if not rationale.strip():
            raise ValueError(
                "rationale must be non-empty: every order on this platform records why "
                "it was placed, so the trade log can be read back later"
            )
        if order_type == "LIMIT" and limit_price is None:
            raise ValueError("a LIMIT order requires limit_price")
        if order_type == "MARKET" and limit_price is not None:
            raise ValueError("a MARKET order must not carry limit_price")
        raise _stub("ctx.order")

    def cancel(self, order_id: OrderId) -> None:
        raise _stub("ctx.cancel")

    def log(self, event: str, **fields: Any) -> None:
        raise _stub("ctx.log")


# --- The interface a strategy implements -------------------------------------


class Strategy:
    """Subclass this. `configure` and `initialize` are required; every event
    handler is optional -- implement only what the strategy reacts to."""

    def configure(self) -> StrategyManifest:
        raise NotImplementedError("every strategy must implement configure()")

    def initialize(self, ctx: Context) -> None:
        """Called once, after the manifest is accepted."""

    def on_bar(self, ctx: Context, bars: dict[InstrumentId, Bar]) -> None:
        """One completed interval. An instrument that did not trade in the
        interval is ABSENT from `bars` rather than present with carried-
        forward values -- the platform will not invent a trade that did not
        happen. Use `ctx.data.last()` for the last known price."""

    def on_tick(self, ctx: Context, tick: Tick) -> None:
        """Only routed when the manifest sets `data.ticks=True`."""

    def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
        """Fires on EVERY state change, including each partial fill -- a
        GTC limit can rest partially filled indefinitely, and a strategy
        sizing its next order from `filled_quantity` needs to see that as it
        happens rather than only at terminal state.

        A rejection arrives here too. It is a normal outcome, not an
        exception, and a strategy must survive one."""

    def on_expiry(self, ctx: Context, event: ExpiryEvent) -> None:
        """F&O settlement."""
