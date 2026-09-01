"""The one atomic write: a fill and everything it implies.

`fills` and `ledger_entries` are the source of truth; `cash_balance` and
`positions` are caches maintained here in the same transaction. The cache
is only acceptable because `replay_portfolio` can prove it never drifted.

`apply_fill` deliberately does NOT commit. The caller owns the transaction
boundary -- that is what lets the engine commit fill, ledger, position,
cash, and order status as one unit, and what lets tests roll back.

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
structurally rounded to 4dp even when it doesn't call `_quantize`
directly. `apply_fill`, by contrast, computes notional and `avg_cost` from
`decision.price` *before* that round trip. If `decision.price` ever
carried more than four decimal places, the two would diverge -- the
identical bug class as the cash defect above, on the price axis instead
of the quantity axis. Today it's unreachable only because
`trading.paper.fills.decide_fill` quantizes market prices to 2dp and
passes limit prices through from a `NUMERIC(18,4)` column already -- a
fact this module must not rely on silently. `apply_fill` therefore
quantizes `decision.price` itself, defensively, as its first step, so the
value it uses for notional, `avg_cost`, and the `fills.price` insert is
provably the same value `replay_portfolio` will later read back,
regardless of what a future `FillDecision` producer does or doesn't
round.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from psycopg import Connection

from trading.paper.enums import EntryType, OrderStatus, Side
from trading.paper.models import ChargeBreakdown, FillDecision, Order, Position

_MONEY_DP = Decimal("0.0001")


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_MONEY_DP, rounding=ROUND_HALF_UP)


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
    decision = decision.model_copy(update={"price": _quantize(decision.price)})

    notional = decision.quantity * decision.price
    total_charges = charges.total
    # Charges always leave the account, whichever side the trade is.
    delta = -(notional + total_charges) if order.side is Side.BUY else notional - total_charges

    fill_row = conn.execute(
        "INSERT INTO fills (order_id, quantity, price, filled_at, tick_ts,"
        " brokerage, stt, exchange_txn, sebi_fee, stamp_duty, ipft, gst,"
        " dp_charges, total_charges)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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

    _apply_position(conn, order, decision)

    filled = order.filled_quantity + decision.quantity
    status = OrderStatus.FILLED if filled >= order.quantity else OrderStatus.PARTIALLY_FILLED
    conn.execute(
        "UPDATE orders SET filled_quantity = %s, status = %s, updated_at = now()"
        " WHERE order_id = %s",
        (filled, status.value, order.order_id),
    )
    return fill_id


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
                    _quantize(decision.price),
                ),
            )
            return
        qty, avg, _ = row
        new_qty = qty + decision.quantity
        # Weighted average over the *gross* traded price. Charges are a cash
        # cost, not part of the position's cost basis -- folding them in here
        # would double-count them against realised P&L on the way out.
        new_avg = _quantize(((qty * avg) + (decision.quantity * decision.price)) / new_qty)
        conn.execute(
            "UPDATE positions SET quantity=%s, avg_cost=%s"
            " WHERE portfolio_id=%s AND instrument_id=%s",
            (new_qty, new_avg, order.portfolio_id, order.instrument_id),
        )
        return

    assert row is not None, "sell with no position; the API must reject this"
    qty, avg, realised = row
    new_qty = qty - decision.quantity
    gain = _quantize((decision.price - avg) * decision.quantity)
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
    cash = _quantize(Decimal(initial[0]))

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
            cash = _quantize(cash - (notional + total_charges))
            held = positions.get(instrument_id)
            if held is None:
                positions[instrument_id] = Position(
                    portfolio_id=portfolio_id,
                    instrument_id=instrument_id,
                    quantity=quantity,
                    avg_cost=_quantize(price),
                    realised_pnl=Decimal("0"),
                )
            else:
                new_qty = held.quantity + quantity
                positions[instrument_id] = held.model_copy(
                    update={
                        "quantity": new_qty,
                        "avg_cost": _quantize(
                            ((held.quantity * held.avg_cost) + (quantity * price)) / new_qty
                        ),
                    }
                )
        else:
            cash = _quantize(cash + (notional - total_charges))
            held = positions[instrument_id]
            positions[instrument_id] = held.model_copy(
                update={
                    "quantity": held.quantity - quantity,
                    "realised_pnl": held.realised_pnl
                    + _quantize((price - held.avg_cost) * quantity),
                }
            )

    return cash, positions
