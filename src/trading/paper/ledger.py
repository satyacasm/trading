"""The one atomic write: a fill and everything it implies.

`fills` and `ledger_entries` are the source of truth; `cash_balance` and
`positions` are caches maintained here in the same transaction. The cache
is only acceptable because `replay_portfolio` can prove it never drifted.

`apply_fill` deliberately does NOT commit. The caller owns the transaction
boundary -- that is what lets the engine commit fill, ledger, position,
cash, and order status as one unit, and what lets tests roll back.

`avg_cost` and `realised_pnl` are quantized to the same four decimal
places as the `positions` table's `NUMERIC(18,4)` columns, at every
mutation, using the same `ROUND_HALF_UP` convention `trading.paper.charges`
uses. Without this, a weighted-average division that doesn't terminate in
four decimal places (e.g. three buys totalling 7.00 over quantity 3,
7/3 = 2.333...) would be written to Postgres already rounded by the
column's declared scale, while `replay_portfolio`'s pure-Python
recomputation kept full precision -- a silent mismatch `hypothesis`
catches almost immediately across enough examples. Quantizing in Python
at each step, identically in both functions, makes the two computations
byte-for-byte reproducible rather than merely "close."
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
    `_apply_position`'s quantization step for step -- see the module
    docstring for why that is required, not cosmetic.
    """
    initial = conn.execute(
        "SELECT initial_capital FROM portfolios WHERE portfolio_id=%s",
        (portfolio_id,),
    ).fetchone()
    assert initial is not None
    cash = Decimal(initial[0])

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
            cash -= notional + total_charges
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
            cash += notional - total_charges
            held = positions[instrument_id]
            positions[instrument_id] = held.model_copy(
                update={
                    "quantity": held.quantity - quantity,
                    "realised_pnl": held.realised_pnl
                    + _quantize((price - held.avg_cost) * quantity),
                }
            )

    return cash, positions
