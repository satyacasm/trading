"""Signed position keeping for perpetual futures.

Spot's `_apply_position` is long-only by construction: a sell can only
reduce something already held, and `ck_no_negative_position` makes a short
unrepresentable. A perpetual inverts that. The sign of `quantity` carries
the direction, there is no side column, and every operation here has to be
correct in both directions -- which is most of why this lives in its own
module rather than as a branch inside the spot path.

Everything here is pure. Cash, margin reservation and the database live in
the caller; what this module owns is the arithmetic that decides how much
was made, how much is still at risk, and when the exchange would close the
position.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

__all__ = [
    "SETTLEMENT_HOURS",
    "PerpPosition",
    "funding_payment",
    "apply_perp_fill",
    "initial_margin",
    "maintenance_margin",
    "bankruptcy_price",
    "liquidation_price",
    "position_equity",
    "settlements_between",
    "should_liquidate",
    "unrealised_pnl",
]

# Binance settles funding at 00:00, 08:00 and 16:00 UTC.
SETTLEMENT_HOURS = (0, 8, 16)

# A maintenance tier: (notional_floor, notional_cap, rate, deduction).
Tier = tuple[Decimal, Decimal, Decimal, Decimal]


@dataclass(frozen=True)
class PerpPosition:
    """One contract's open exposure.

    `quantity` is signed: positive is long, negative is short, zero is
    flat. One-way mode -- a position cannot be long and short at once, so
    a single signed number says everything a side column would.
    """

    quantity: Decimal
    entry_price: Decimal
    leverage: Decimal

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0

    def notional(self, mark: Decimal) -> Decimal:
        return abs(self.quantity) * mark


def unrealised_pnl(position: PerpPosition, *, mark: Decimal) -> Decimal:
    """What the position would realise if closed at `mark`.

    Signed by direction rather than by price movement: a short gains when
    the mark falls, which is exactly the case spot's `quantity x mark`
    formula cannot express.
    """
    if position.is_flat:
        return Decimal("0")
    return position.quantity * (mark - position.entry_price)


def initial_margin(quantity: Decimal, *, price: Decimal, leverage: Decimal) -> Decimal:
    """What opening this exposure locks up. Reserved, not spent."""
    if leverage <= 0:
        raise ValueError(f"leverage must be positive, got {leverage}")
    return abs(quantity) * price / leverage


def maintenance_margin(quantity: Decimal, *, mark: Decimal, tiers: Sequence[Tier]) -> Decimal:
    """The equity floor below which the exchange closes the position.

        maintenance = notional x rate - deduction

    The deduction (Binance's `cum`) is what makes the ladder continuous:
    without it, a position sitting on a tier boundary has two different
    requirements depending on which row you read, and every tier above the
    first overstates the requirement -- liquidating positions that were
    never near the line.

    Risk is symmetric, so the sign of `quantity` does not matter here; a
    short of five is exactly as demanding as a long of five.

    Raises rather than falling back to the top tier: a notional above every
    tier is a position the exchange would not have permitted, and quietly
    applying the last rate would invent a margin requirement for a trade
    that could not exist.
    """
    notional = abs(quantity) * mark
    for floor, cap, rate, deduction in tiers:
        if floor <= notional <= cap:
            return notional * rate - deduction
    raise LookupError(
        f"no maintenance tier covers a notional of {notional}; the largest tier ends at "
        f"{max((cap for _f, cap, _r, _d in tiers), default=0)}"
    )


def apply_perp_fill(
    position: PerpPosition, *, side: str, quantity: Decimal, price: Decimal
) -> tuple[PerpPosition, Decimal]:
    """The position after a fill, and what that fill realised.

    Three cases, and the third is the one that is easy to get wrong:

    - **Increasing** exposure in the direction already held averages the
      entry price and realises nothing, because nothing was closed.
    - **Reducing** it realises the difference between entry and fill on the
      part closed, and leaves the entry price of the remainder alone.
      Re-averaging on a partial close would silently rewrite the basis of
      exposure that is still open.
    - **Crossing through flat** -- selling three against a long of one --
      closes the old position and opens a new one the other way. The
      realised part covers only what was actually closed, and the new
      position's entry is the fill price, never a blend of a long's basis
      with a short's.
    """
    if quantity <= 0:
        raise ValueError(f"fill quantity must be positive, got {quantity}")
    signed = quantity if side == "BUY" else -quantity
    held = position.quantity

    if held == 0:
        return replace(position, quantity=signed, entry_price=price), Decimal("0")

    same_direction = (held > 0) == (signed > 0)
    if same_direction:
        total = held + signed
        averaged = (held * position.entry_price + signed * price) / total
        return replace(position, quantity=total, entry_price=averaged), Decimal("0")

    closed = min(abs(signed), abs(held))
    # Signed by the direction being closed: closing a long realises
    # (price - entry), closing a short realises (entry - price).
    direction = Decimal("1") if held > 0 else Decimal("-1")
    realised = closed * (price - position.entry_price) * direction

    remaining = held + signed
    if remaining == 0:
        return replace(position, quantity=Decimal("0"), entry_price=Decimal("0")), realised
    if (remaining > 0) == (held > 0):
        # Partially closed; the rest keeps its original basis.
        return replace(position, quantity=remaining), realised
    # Crossed through flat: what is left is new exposure at the fill price.
    return replace(position, quantity=remaining, entry_price=price), realised


def funding_payment(quantity: Decimal, *, mark: Decimal, rate: Decimal) -> Decimal:
    """What the holder of `quantity` pays at this settlement.

    Positive means the holder pays; negative means the holder is paid. One
    signed expression covers all four cases -- long or short, rate positive
    or negative -- because the sign of the position and the sign of the
    rate multiply out exactly as the transfer does.

    Charged on notional, not on margin. That is why leverage compounds a
    carry cost: the same collateral carries a far larger funding bill.
    """
    return quantity * mark * rate


def settlements_between(since: datetime, until: datetime) -> list[datetime]:
    """Every settlement boundary in the half-open window `(since, until]`.

    Half-open on purpose. A poll landing exactly on 08:00 must not settle a
    boundary the previous poll's window already closed, and only
    `(since, until]` makes a sequence of adjacent windows partition the
    timeline rather than overlap at every edge.
    """
    if until < since:
        raise ValueError(
            f"funding window runs backwards: {since.isoformat()} to {until.isoformat()}; "
            "a clock that moved back is worth hearing about, not silently settling nothing"
        )
    found: list[datetime] = []
    cursor = since.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end = until.astimezone(UTC)
    while cursor <= end:
        if cursor.hour in SETTLEMENT_HOURS and cursor > since:
            found.append(cursor)
        cursor += timedelta(hours=1)
    return found


def position_equity(position: PerpPosition, *, margin: Decimal, mark: Decimal) -> Decimal:
    """What is left of the collateral behind this position.

    The margin posted, plus whatever the move has done since entry. This
    is the number the maintenance requirement is compared against, and it
    is deliberately *this position's* equity rather than the portfolio's:
    isolated margin means one position's loss cannot reach another's
    collateral.
    """
    return margin + unrealised_pnl(position, mark=mark)


def should_liquidate(
    position: PerpPosition, *, margin: Decimal, mark: Decimal, tiers: Sequence[Tier]
) -> bool:
    """Whether the exchange would close this position at `mark`.

    Strictly below, matching the circuit breaker's convention: a position
    landing exactly on its requirement is still adequately margined, and
    a boundary that liquidates on equality closes positions the exchange
    would have left alone.
    """
    if position.is_flat:
        return False
    required = maintenance_margin(position.quantity, mark=mark, tiers=tiers)
    return position_equity(position, margin=margin, mark=mark) < required


def liquidation_price(
    position: PerpPosition, *, margin: Decimal, tiers: Sequence[Tier]
) -> Decimal | None:
    """The mark at which this position would be closed, or None if flat.

    Solved from the definition rather than pattern-matched from a
    published formula, so it stays right when the inputs change. At the
    liquidation price, remaining equity equals the requirement:

        margin + quantity x (m - entry) = |quantity| x m x rate - deduction

    Rearranged for a long (quantity positive), where `q` is the size:

        m = (margin - q x entry + deduction) / (q x (rate - 1))

    and for a short, where the unrealised term flips sign:

        m = (margin + q x entry + deduction) / (q x (rate + 1))

    The tier is chosen at the position's *entry* notional. A liquidating
    position is usually near the tier it opened in, and solving for the
    tier that the solution itself selects would need an iteration whose
    only effect, at these sizes, is to move the answer by less than a tick.
    Worth knowing rather than worth hiding: a position opened close to a
    tier boundary can liquidate a little off this estimate.
    """
    if position.is_flat:
        return None

    size = abs(position.quantity)
    entry_notional = size * position.entry_price
    rate, deduction = _tier_for(entry_notional, tiers)

    if position.quantity > 0:
        return (margin - size * position.entry_price + deduction) / (size * (rate - 1))
    return (margin + size * position.entry_price + deduction) / (size * (rate + 1))


def _tier_for(notional: Decimal, tiers: Sequence[Tier]) -> tuple[Decimal, Decimal]:
    for floor, cap, rate, deduction in tiers:
        if floor <= notional <= cap:
            return rate, deduction
    raise LookupError(
        f"no maintenance tier covers a notional of {notional}; a position the exchange "
        "would not have permitted has no liquidation price either"
    )


def bankruptcy_price(position: PerpPosition, *, margin: Decimal) -> Decimal | None:
    """The mark at which this position has consumed exactly its margin.

    Beyond the liquidation price, and the difference between them is the
    maintenance buffer -- the room the exchange keeps so it can close the
    position while something is still left to close it with.

    A real venue closes between the two and its insurance fund covers any
    gap when the market moves faster than that. This platform has no fund,
    so a fill beyond here is capped at this price and the shortfall is
    recorded rather than silently absorbed: pretending the loss stopped at
    the margin would understate what leverage actually did.
    """
    if position.is_flat:
        return None
    size = abs(position.quantity)
    per_unit = margin / size
    if position.quantity > 0:
        return position.entry_price - per_unit
    return position.entry_price + per_unit
