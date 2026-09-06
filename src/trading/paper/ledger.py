"""The one atomic write: a fill and everything it implies.

`fills` and `ledger_entries` are the source of truth; `cash_balance` and
`positions` are caches maintained here in the same transaction. The cache
is only acceptable because `replay_portfolio` can prove it never drifted.

`apply_fill` deliberately does NOT commit. The caller owns the transaction
boundary -- that is what lets the engine commit fill, ledger, position,
cash, and order status as one unit, and what lets tests roll back.

The final order-status write is guarded by an optimistic-concurrency
check (`WHERE status IN ('OPEN','PARTIALLY_FILLED','PENDING')`, rowcount
checked): a fill is decided against a snapshot of `order` that can go
stale before this transaction's last statement runs -- concretely, the
paper_engine holds orders in memory and reacts to a Redis `cancel`
message that can outrace the API's own commit of `CANCELLED` (see
`trading.streaming.db.get_db_connection`, which commits only after the
route body returns). Without this guard, that race lets a fill silently
overwrite a user's cancellation. `apply_fill` raises
`OrderNoLongerFillable` when the guard trips, rather than silently
no-op'ing, so the caller's `except` rolls back the whole transaction --
the fill row, cash update, ledger entry, and position update this
function already wrote must never survive an order that turned out not
to be fillable after all.

`avg_cost`, `realised_pnl`, and `cash_balance` are quantized to the same
four decimal places as their `NUMERIC(18,4)` columns, at every mutation,
using the same `ROUND_HALF_UP` convention `trading.paper.charges` uses.
Without this, a computation that doesn't terminate in four decimal places
would be written to Postgres already rounded by the column's declared
scale, while `replay_portfolio`'s pure-Python recomputation kept full
precision -- a silent mismatch `hypothesis` catches almost immediately
across enough examples. `avg_cost`/`realised_pnl` need this because a
weighted-average division rarely terminates exactly (e.g. three buys
totalling 7.00 over quantity 3, 7/3 = 2.333...); `cash_balance` needs it
for a different reason -- `orders.quantity`/`fills.quantity` are
`NUMERIC(18,8)` because crypto fills are fractional, so a notional like
`0.00000001 * 79090.0100` carries twelve decimal places even though every
input was exact. `apply_fill` never computes this in Python: Postgres
rounds `cash_balance = cash_balance + %s` to 4dp *as it stores each row*,
so `replay_portfolio` must quantize its running cash total after every
single fill (not once at the end) to replicate that same sequence of
roundings -- quantizing only the final total would still diverge whenever
intermediate roundings compound differently than one final rounding
would. Quantizing in Python at each step, identically in both functions,
makes the two computations byte-for-byte reproducible rather than merely
"close."

**Precondition on `decision.price`, made explicit rather than assumed:**
`replay_portfolio` only ever sees a fill's price after it has round-tripped
through `fills.price`, a `NUMERIC(18,4)` column -- so its recomputation is
structurally rounded to 4dp even when it doesn't call `quantize_money`
directly. `apply_fill`, by contrast, computes notional and `avg_cost` from
`decision.price` *before* that round trip. If `decision.price` ever
carried more than four decimal places, the two would diverge -- the
identical bug class as the cash defect above, on the price axis instead
of the quantity axis. Today it's unreachable only because
`trading.paper.fills.decide_fill` quantizes market prices to 2dp and
passes limit prices through from a `NUMERIC(18,4)` column already -- a
fact this module must not rely on silently. `apply_fill` therefore
quantizes `decision.price` itself as its first step -- but only
*defensively*, as a real precondition check on its own arithmetic
(notional, `avg_cost`, the `fills.price` insert): `decision.model_copy`
rebinds a local name, and `FillDecision` is frozen, so this cannot and
does not change what the *caller* sees. `decision.price` as the caller
holds it, and everything the caller derives from it (charges, an alert
payload, a Redis publish) *before* ever calling `apply_fill`, is
untouched by this quantization (IMP-5 -- see the caller,
`trading.paper.engine._process_fill`, which quantizes once, at the
source, before deriving anything else, so every downstream consumer -- not
just this function -- shares one value). `quantize_money` is exported
(not just used internally) precisely so that caller can do so, mirroring
`trading.paper.breaker.quantize_money`'s identical fix for the same shape.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from psycopg import Connection

from trading.contracts import AssetClass
from trading.paper.enums import EntryType, OrderStatus, Side
from trading.paper.models import ChargeBreakdown, FillDecision, Order, Position
from trading.paper.perp import PerpPosition, apply_perp_fill, initial_margin

_MONEY_DP = Decimal("0.0001")


def quantize_money(value: Decimal) -> Decimal:
    """To 4dp (`NUMERIC(18,4)`'s scale), `ROUND_HALF_UP`. Exported (not
    just used internally by `apply_fill`/`replay_portfolio`) so
    `trading.paper.engine._process_fill` can quantize `decision.price`
    once, at the source, before deriving charges, an alert payload, or a
    Redis publish from it -- see the module docstring's "Precondition on
    decision.price" section (IMP-5). Mirrors `trading.paper.breaker.
    quantize_money`'s identical fix for the identical shape.
    """
    return value.quantize(_MONEY_DP, rounding=ROUND_HALF_UP)


class MissingLeverage(Exception):
    """A perpetual fill reached the ledger with no leverage on its order.

    Raised rather than defaulted: leverage decides how much margin the
    position locks up, and assuming one would silently reserve an amount
    the trader never chose.
    """


class OrderNoLongerFillable(Exception):
    """`apply_fill`'s final order-status UPDATE found the order was no
    longer OPEN/PARTIALLY_FILLED/PENDING -- something else (a `cancel`
    that committed after this fill's decision was made, most concretely)
    changed its status concurrently, between the caller reading it into
    memory and this transaction's final write.

    Raised, not silently ignored, specifically so the caller's `except`
    block rolls back the *whole* transaction: the fill row, cash update,
    ledger entry, and position update this function already wrote (all
    still uncommitted at this point) must never survive if the order they
    belong to turned out to no longer be fillable. This is optimistic
    concurrency control, not an error condition -- the caller should treat
    it as "the system did the right thing," not as a failure to log and
    retry.
    """


def apply_fill(
    conn: Connection,
    order: Order,
    decision: FillDecision,
    charges: ChargeBreakdown,
) -> int:
    """Record a fill and update ledger, position, cash, and order status."""
    # Quantize defensively to the same 4dp fills.price will hold once
    # stored -- see the module docstring's "Precondition on decision.price"
    # section. Every use of decision.price below (notional, avg_cost, the
    # fills insert) reads this already-quantized value.
    decision = decision.model_copy(update={"price": quantize_money(decision.price)})

    notional = decision.quantity * decision.price
    total_charges = charges.total
    # A perpetual moves cash on realised P&L and fees, never on notional:
    # opening one reserves margin rather than spending money. Applying the
    # spot formula would credit a short with the whole notional it never
    # received. Decided before anything is written so both paths share the
    # one order-status update and its optimistic-concurrency guard.
    is_perp = _is_perp(conn, order.instrument_id)
    if is_perp:
        realised = _perp_realised(conn, order, decision)
        delta = realised - total_charges
    else:
        # Charges always leave the account, whichever side the trade is.
        delta = -(notional + total_charges) if order.side is Side.BUY else notional - total_charges

    fill_row = conn.execute(
        "INSERT INTO fills (order_id, quantity, price, filled_at, tick_ts,"
        " brokerage, stt, exchange_txn, sebi_fee, stamp_duty, ipft, gst,"
        " dp_charges, tds, total_charges)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        " RETURNING fill_id",
        (
            order.order_id,
            decision.quantity,
            decision.price,
            decision.tick_ts,
            charges.brokerage,
            charges.stt,
            charges.exchange_txn,
            charges.sebi_fee,
            charges.stamp_duty,
            charges.ipft,
            charges.gst,
            charges.dp_charges,
            charges.tds,
            total_charges,
        ),
    ).fetchone()
    assert fill_row is not None
    fill_id = int(fill_row[0])

    # The ck_no_negative_cash constraint enforces the floor in the database,
    # so an over-spend raises here rather than silently going negative.
    cash_row = conn.execute(
        "UPDATE portfolios SET cash_balance = cash_balance + %s"
        " WHERE portfolio_id = %s RETURNING cash_balance",
        (delta, order.portfolio_id),
    ).fetchone()
    assert cash_row is not None
    balance_after = cash_row[0]

    conn.execute(
        "INSERT INTO ledger_entries (portfolio_id, ts, entry_type, amount,"
        " fill_id, balance_after) VALUES (%s, now(), %s, %s, %s, %s)",
        (order.portfolio_id, EntryType.FILL.value, delta, fill_id, balance_after),
    )

    if is_perp:
        _apply_perp_position(conn, order, decision)
    else:
        _apply_position(conn, order, decision)

    filled = order.filled_quantity + decision.quantity
    status = OrderStatus.FILLED if filled >= order.quantity else OrderStatus.PARTIALLY_FILLED
    # Optimistic concurrency control: this fill was decided against a
    # snapshot of `order` that may already be stale by the time this
    # transaction reaches its final write -- most concretely, a `cancel`
    # that committed on another connection in between. The status guard
    # below is what actually closes that race (an in-memory recheck in the
    # caller is best-effort only; it cannot see a commit made by another
    # process). A 0 rowcount means the order changed underneath us.
    cursor = conn.execute(
        "UPDATE orders SET filled_quantity = %s, status = %s, updated_at = now()"
        " WHERE order_id = %s"
        "   AND status IN (%s, %s, %s)",
        (
            filled,
            status.value,
            order.order_id,
            OrderStatus.OPEN.value,
            OrderStatus.PARTIALLY_FILLED.value,
            OrderStatus.PENDING.value,
        ),
    )
    if cursor.rowcount == 0:
        raise OrderNoLongerFillable(
            f"order {order.order_id} is no longer OPEN/PARTIALLY_FILLED/PENDING; "
            "refusing to overwrite its status with a fill decided before that change"
        )
    return fill_id


def _is_perp(conn: Connection, instrument_id: int) -> bool:
    """Whether this instrument settles as a derivative rather than a holding.

    Read from the instrument rather than passed in by the caller: every
    call site would otherwise have to remember, and a caller that forgot
    would silently apply spot's cash mechanics to a perpetual -- the one
    mistake in this file that produces plausible numbers.
    """
    row = conn.execute(
        "SELECT asset_class FROM instruments WHERE instrument_id = %s", (instrument_id,)
    ).fetchone()
    return row is not None and row[0] == AssetClass.PERP.value


def _perp_state(conn: Connection, order: Order) -> tuple[PerpPosition, Decimal, Decimal]:
    """The open perpetual position, its realised total, and its leverage."""
    row = conn.execute(
        "SELECT quantity, entry_price, leverage, realised_pnl FROM perp_positions"
        " WHERE portfolio_id=%s AND instrument_id=%s FOR UPDATE",
        (order.portfolio_id, order.instrument_id),
    ).fetchone()
    leverage = _order_leverage(conn, order)
    if row is None:
        return PerpPosition(Decimal("0"), Decimal("0"), leverage), Decimal("0"), leverage
    # An existing position keeps the leverage it was opened at; a later
    # order cannot silently re-lever exposure already taken.
    return PerpPosition(row[0], row[1], row[2]), row[3], row[2]


def _order_leverage(conn: Connection, order: Order) -> Decimal:
    """The leverage this order was placed at.

    Read from the model where present -- the API now surfaces it, so the
    common path costs no query -- and from the row otherwise, which keeps
    callers that build an `Order` by hand working.
    """
    if order.leverage is not None:
        return order.leverage
    row = conn.execute(
        "SELECT leverage FROM orders WHERE order_id = %s", (order.order_id,)
    ).fetchone()
    if row is None or row[0] is None:
        raise MissingLeverage(
            f"order {order.order_id} is for a perpetual but carries no leverage; "
            "refusing to assume one -- the margin it locks up depends on it"
        )
    return Decimal(row[0])


def _perp_realised(conn: Connection, order: Order, decision: FillDecision) -> Decimal:
    """What this fill closes, in money. Reads state; writes nothing."""
    position, _total, _leverage = _perp_state(conn, order)
    _after, realised = apply_perp_fill(
        position, side=order.side.value, quantity=decision.quantity, price=decision.price
    )
    return realised


def _apply_perp_position(conn: Connection, order: Order, decision: FillDecision) -> None:
    """Signed position keeping, and the margin it locks up.

    Margin is recomputed from the resulting position rather than adjusted
    by a delta: a delta has to be right on every path -- open, add, reduce,
    close, reverse -- and being wrong on one of them leaks margin that no
    position explains. Recomputing is correct on all five by construction.
    """
    position, realised_total, leverage = _perp_state(conn, order)
    after, realised = apply_perp_fill(
        position, side=order.side.value, quantity=decision.quantity, price=decision.price
    )
    reserved = (
        Decimal("0")
        if after.is_flat
        else initial_margin(after.quantity, price=after.entry_price, leverage=leverage)
    )
    conn.execute(
        "INSERT INTO perp_positions (portfolio_id, instrument_id, quantity, entry_price,"
        " leverage, reserved_margin, realised_pnl)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s)"
        " ON CONFLICT (portfolio_id, instrument_id) DO UPDATE SET"
        "   quantity = EXCLUDED.quantity, entry_price = EXCLUDED.entry_price,"
        "   reserved_margin = EXCLUDED.reserved_margin,"
        "   realised_pnl = perp_positions.realised_pnl + %s",
        (
            order.portfolio_id,
            order.instrument_id,
            after.quantity,
            after.entry_price,
            leverage,
            reserved,
            realised_total + realised,
            realised,
        ),
    )


def _apply_position(conn: Connection, order: Order, decision: FillDecision) -> None:
    """Weighted-average cost on increase; realised P&L on decrease.

    Long-only in this slice, so a sell can only reduce an existing position
    -- the API rejects a sell with no position behind it.
    """
    row = conn.execute(
        "SELECT quantity, avg_cost, realised_pnl FROM positions"
        " WHERE portfolio_id=%s AND instrument_id=%s FOR UPDATE",
        (order.portfolio_id, order.instrument_id),
    ).fetchone()

    if order.side is Side.BUY:
        if row is None:
            conn.execute(
                "INSERT INTO positions (portfolio_id, instrument_id, quantity,"
                " avg_cost, realised_pnl) VALUES (%s,%s,%s,%s,0)",
                (
                    order.portfolio_id,
                    order.instrument_id,
                    decision.quantity,
                    quantize_money(decision.price),
                ),
            )
            return
        qty, avg, _ = row
        new_qty = qty + decision.quantity
        # Weighted average over the *gross* traded price. Charges are a cash
        # cost, not part of the position's cost basis -- folding them in here
        # would double-count them against realised P&L on the way out.
        new_avg = quantize_money(((qty * avg) + (decision.quantity * decision.price)) / new_qty)
        conn.execute(
            "UPDATE positions SET quantity=%s, avg_cost=%s"
            " WHERE portfolio_id=%s AND instrument_id=%s",
            (new_qty, new_avg, order.portfolio_id, order.instrument_id),
        )
        return

    assert row is not None, "sell with no position; the API must reject this"
    qty, avg, realised = row
    new_qty = qty - decision.quantity
    gain = quantize_money((decision.price - avg) * decision.quantity)
    conn.execute(
        "UPDATE positions SET quantity=%s, realised_pnl=%s"
        " WHERE portfolio_id=%s AND instrument_id=%s",
        (new_qty, realised + gain, order.portfolio_id, order.instrument_id),
    )


def replay_portfolio(conn: Connection, portfolio_id: int) -> tuple[Decimal, dict[int, Position]]:
    """Recompute cash and positions from `fills` alone.

    The invariant that earns the caches their place: this must reproduce
    `portfolios.cash_balance` and every `positions` row exactly. Mirrors
    `apply_fill`/`_apply_position`'s quantization step for step -- see the
    module docstring for why that is required, not cosmetic, and in
    particular why cash must be quantized after *every* fill rather than
    once at the end.
    """
    initial = conn.execute(
        "SELECT initial_capital FROM portfolios WHERE portfolio_id=%s",
        (portfolio_id,),
    ).fetchone()
    assert initial is not None
    # initial_capital is already NUMERIC(18,4); quantize anyway so the
    # running total starts from the same representation apply_fill's
    # first UPDATE would have started from.
    cash = quantize_money(Decimal(initial[0]))

    rows = conn.execute(
        "SELECT o.instrument_id, o.side, f.quantity, f.price, f.total_charges"
        " FROM fills f JOIN orders o ON o.order_id = f.order_id"
        " WHERE o.portfolio_id = %s ORDER BY f.fill_id",
        (portfolio_id,),
    ).fetchall()

    positions: dict[int, Position] = {}
    for instrument_id, side, quantity, price, total_charges in rows:
        notional = quantity * price
        if side == Side.BUY:
            # Quantized after every fill, not once at the end -- see the
            # module docstring. Postgres rounds cash_balance to 4dp on
            # every UPDATE apply_fill issues, so replaying the fills in
            # full precision and rounding only the final sum would
            # reproduce a different number whenever intermediate
            # roundings compound differently than one final rounding.
            cash = quantize_money(cash - (notional + total_charges))
            held = positions.get(instrument_id)
            if held is None:
                positions[instrument_id] = Position(
                    portfolio_id=portfolio_id,
                    instrument_id=instrument_id,
                    quantity=quantity,
                    avg_cost=quantize_money(price),
                    realised_pnl=Decimal("0"),
                )
            else:
                new_qty = held.quantity + quantity
                positions[instrument_id] = held.model_copy(
                    update={
                        "quantity": new_qty,
                        "avg_cost": quantize_money(
                            ((held.quantity * held.avg_cost) + (quantity * price)) / new_qty
                        ),
                    }
                )
        else:
            cash = quantize_money(cash + (notional - total_charges))
            held = positions[instrument_id]
            positions[instrument_id] = held.model_copy(
                update={
                    "quantity": held.quantity - quantity,
                    "realised_pnl": held.realised_pnl
                    + quantize_money((price - held.avg_cost) * quantity),
                }
            )

    return cash, positions
