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
from decimal import Decimal

__all__ = [
    "PerpPosition",
    "apply_perp_fill",
    "initial_margin",
    "maintenance_margin",
    "unrealised_pnl",
]

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
