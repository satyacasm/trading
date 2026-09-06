"""When the exchange closes a perpetual position, and at what price.

A perpetual can lose more than it cost. Isolated margin bounds that: the
position is collateralised by what was posted for it, and once what
remains falls below the exchange's maintenance requirement the position is
closed whether or not the holder agrees. Modelling that is not optional --
a simulator without it lets every over-levered strategy survive a move
that would have ended it, which is the most flattering possible lie about
leverage.

**Liquidation is not a circuit-breaker halt.** The breaker is this
platform protecting a portfolio from its own strategy, and it pauses
trading. A liquidation is the market closing one position because it ran
out of collateral. Conflating them would let a liquidated strategy read as
a paused one, which is the difference between "you were stopped out" and
"you can resume when ready".

**Mark price, never last traded.** The mark is index-derived and
deliberately resistant to a single venue's wick. Liquidating on last price
would close positions on prints the mark never reached, and report
blow-ups that did not happen.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import structlog
from psycopg import Connection

from trading.paper.enums import EntryType, OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import ChargeBreakdown, FillDecision, Order

# The arithmetic lives in `perp`, which imports nothing. The strategy
# runtime needs it and is copied into a container with no database, no
# structlog and no psycopg -- a pure function in an I/O module is a pure
# function the sandbox cannot have. Re-exported so callers of this module
# keep working.
from trading.paper.perp import (
    PerpPosition,
    Tier,
    bankruptcy_price,
    liquidation_price,
    maintenance_margin,
    position_equity,
    should_liquidate,
    unrealised_pnl,
)

log = structlog.get_logger(__name__)

__all__ = [
    "Liquidated",
    "bankruptcy_price",
    "liquidate_open_positions",
    "liquidation_price",
    "position_equity",
    "should_liquidate",
]


@dataclass(frozen=True)
class Liquidated:
    """One position the exchange closed, and what it cost."""

    portfolio_id: int
    instrument_id: int
    quantity: Decimal
    mark: Decimal
    fill_price: Decimal
    fee: Decimal
    # What the market took beyond the collateral posted. Non-zero only when
    # the mark gapped past the bankruptcy price -- the loss a real venue's
    # insurance fund would have absorbed.
    shortfall: Decimal


_OPEN_PERPS = """
    SELECT p.portfolio_id, p.instrument_id, p.quantity, p.entry_price, p.leverage,
           p.reserved_margin, f.cash_balance
    FROM perp_positions p
    JOIN portfolios f ON f.portfolio_id = p.portfolio_id
    WHERE p.quantity <> 0 AND f.status = 'ACTIVE'
"""

_TIERS = """
    SELECT notional_floor, notional_cap, maintenance_rate, maintenance_amount
    FROM perp_margin_tiers WHERE instrument_id = %s ORDER BY notional_floor
"""


def _liquidation_fee_rate(conn: Connection, instrument_id: int) -> Decimal:
    row = conn.execute(
        "SELECT liquidation_fee FROM perp_contract_specs"
        " WHERE instrument_id = %s AND effective_to IS NULL",
        (instrument_id,),
    ).fetchone()
    if row is None:
        raise LookupError(
            f"no contract spec for instrument_id={instrument_id}; the liquidation fee is "
            "published per contract and must not be assumed"
        )
    return Decimal(row[0])


def liquidate_open_positions(
    conn: Connection, marks: Mapping[int, Decimal], *, now: datetime
) -> list[Liquidated]:
    """Close every perpetual position whose collateral has run out.

    The forced close goes through the ordinary order and fill path -- a
    real order row, a real fill, the normal position and margin update --
    so it appears in the blotter beside every other trade rather than as a
    position that silently vanished. Its rationale says what happened,
    which is the record a trader will actually go looking for.

    A position whose instrument has no mark is skipped: liquidating on a
    stale price is worse than not liquidating this pass, and the next mark
    is seconds away.
    """
    from trading.paper.ledger import apply_fill  # circular at module scope

    closed: list[Liquidated] = []
    for row in conn.execute(_OPEN_PERPS).fetchall():
        portfolio_id, instrument_id, quantity, entry_price, leverage, margin, cash = row
        mark = marks.get(instrument_id)
        if mark is None:
            continue

        position = PerpPosition(quantity, entry_price, leverage)
        tiers = [tuple(t) for t in conn.execute(_TIERS, (instrument_id,)).fetchall()]
        if not tiers or not should_liquidate(position, margin=margin, mark=mark, tiers=tiers):
            continue

        # Cap the fill at bankruptcy: past it the position has consumed
        # everything posted, and filling further would drive cash below
        # zero against `ck_no_negative_cash` -- and would charge the trader
        # for a loss a real venue's insurance fund would have taken.
        bankrupt = bankruptcy_price(position, margin=margin)
        assert bankrupt is not None
        gapped = (quantity > 0 and mark < bankrupt) or (quantity < 0 and mark > bankrupt)
        fill_price = bankrupt if gapped else mark
        shortfall = (
            abs(unrealised_pnl(position, mark=mark) - unrealised_pnl(position, mark=bankrupt))
            if gapped
            else Decimal("0")
        )

        side = Side.SELL if quantity > 0 else Side.BUY
        size = abs(quantity)
        fee = (size * fill_price * _liquidation_fee_rate(conn, instrument_id)).quantize(
            Decimal("0.0001")
        )
        order = _forced_order(
            conn, portfolio_id, instrument_id, side, size, leverage, mark, margin, position, tiers
        )
        apply_fill(
            conn,
            order,
            FillDecision(quantity=size, price=fill_price, tick_ts=now),
            _fee_only(fee),
        )

        if shortfall > 0:
            _record_shortfall(conn, portfolio_id, now, shortfall)
        log.warning(
            "liquidation.closed",
            portfolio_id=portfolio_id,
            instrument_id=instrument_id,
            quantity=str(quantity),
            mark=str(mark),
            fill_price=str(fill_price),
            shortfall=str(shortfall),
        )
        closed.append(
            Liquidated(portfolio_id, instrument_id, quantity, mark, fill_price, fee, shortfall)
        )
    return closed


def _forced_order(
    conn: Connection,
    portfolio_id: int,
    instrument_id: int,
    side: Side,
    size: Decimal,
    leverage: Decimal,
    mark: Decimal,
    margin: Decimal,
    position: PerpPosition,
    tiers: Sequence[Tier],
) -> Order:
    required = maintenance_margin(position.quantity, mark=mark, tiers=tiers)
    equity = position_equity(position, margin=margin, mark=mark)
    rationale = (
        f"liquidated at {mark}: position equity {equity.quantize(Decimal('0.01'))} fell below "
        f"the maintenance requirement of {required.quantize(Decimal('0.01'))}"
    )
    row = conn.execute(
        "INSERT INTO orders (portfolio_id, instrument_id, side, order_type, quantity,"
        " product, time_in_force, status, rationale, leverage, idempotency_key)"
        " VALUES (%s,%s,%s,'MARKET',%s,'INTRADAY','GTC','OPEN',%s,%s,%s)"
        " RETURNING order_id, submitted_at",
        (
            portfolio_id,
            instrument_id,
            side.value,
            size,
            rationale,
            leverage,
            f"liquidation-{uuid4()}",
        ),
    ).fetchone()
    assert row is not None
    return Order(
        order_id=int(row[0]),
        portfolio_id=portfolio_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=size,
        filled_quantity=Decimal("0"),
        limit_price=None,
        product=Product.INTRADAY,
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.OPEN,
        rationale=rationale,
        submitted_at=row[1],
        leverage=leverage,
    )


def _fee_only(fee: Decimal) -> ChargeBreakdown:
    zero = Decimal("0")
    return ChargeBreakdown(
        brokerage=fee,
        stt=zero,
        exchange_txn=zero,
        sebi_fee=zero,
        stamp_duty=zero,
        ipft=zero,
        gst=zero,
        dp_charges=zero,
        tds=zero,
    )


def _record_shortfall(
    conn: Connection, portfolio_id: int, now: datetime, shortfall: Decimal
) -> None:
    """Say what the gap cost, without charging it.

    A zero-amount entry, because the money did not move -- the fill was
    capped at bankruptcy. What this row records is the loss a real venue's
    insurance fund would have absorbed, which is information a trader
    needs and a number this platform must not quietly pretend away.
    """
    row = conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()
    assert row is not None
    conn.execute(
        "INSERT INTO ledger_entries (portfolio_id, ts, entry_type, amount, balance_after)"
        " VALUES (%s, %s, %s, %s, %s)",
        (portfolio_id, now, EntryType.LIQUIDATION.value, Decimal("0"), row[0]),
    )
    log.warning(
        "liquidation.shortfall",
        portfolio_id=portfolio_id,
        shortfall=str(shortfall),
        detail="the mark gapped past bankruptcy; a real venue's insurance fund would "
        "have covered this",
    )
