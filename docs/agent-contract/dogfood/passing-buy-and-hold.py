from decimal import Decimal

from platform_sdk import (
    Bar,
    Context,
    InstrumentId,
    InstrumentRef,
    OrderUpdate,
    Param,
    Strategy,
    StrategyManifest,
    DataRequest,
)


class BuyAndHold(Strategy):
    """Buy a fixed quantity of one instrument on the first bar, then hold.

    A deliberately boring control strategy: it exists to prove the upload,
    validation, smoke-run, and order path work end to end.
    """

    def configure(self) -> StrategyManifest:
        return StrategyManifest(
            name="buy-and-hold",
            version="1.0.0",
            universe=[
                InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE"),
            ],
            data=DataRequest(
                bars="1m",
                ticks=False,
                history_bars=1,
            ),
            capital=Decimal("1000000"),
            base_currency="INR",
            params={
                "quantity": Param(int, default=10, bounds=(1, 1000)),
            },
        )

    def initialize(self, ctx: Context) -> None:
        # One entry, ever. `submitted` guards against a second order if
        # on_bar fires again before the first order reaches a terminal state.
        self.submitted = False
        self.entry_order_id = None
        self.quantity = Decimal("10")
        ctx.log("initialized", quantity=str(self.quantity))

    def on_bar(self, ctx: Context, bars: dict[InstrumentId, Bar]) -> None:
        if self.submitted:
            return

        # The manifest names exactly one instrument, but an instrument that did
        # not trade is absent from the dict, so take whichever bar arrived.
        if not bars:
            return

        # Dicts are insertion-ordered, so this is deterministic; sets are not.
        instrument_id = next(iter(bars))
        bar = bars[instrument_id]

        self.submitted = True
        self.entry_order_id = ctx.order(
            instrument_id,
            side="BUY",
            quantity=self.quantity,
            order_type="MARKET",
            product="DELIVERY",
            time_in_force="DAY",
            rationale=(
                "Buy-and-hold control: entering a fixed 10-share delivery "
                "position on the first bar of the run and holding it "
                "untouched thereafter, to exercise the order path end to end."
            ),
        )
        ctx.log(
            "entry_submitted",
            instrument_id=instrument_id,
            quantity=str(self.quantity),
            close=str(bar.close),
        )

    def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
        # A rejection is a normal outcome here, not an exception. Everything
        # about the order lives one level down, on `update.order`.
        order = update.order
        if order.status == "REJECTED":
            ctx.log(
                "entry_rejected",
                order_id=order.order_id,
                reason=order.rejection_reason,
            )
            return

        ctx.log(
            "order_update",
            order_id=order.order_id,
            previous_status=update.previous_status,
            status=order.status,
            filled_quantity=str(order.filled_quantity),
        )
