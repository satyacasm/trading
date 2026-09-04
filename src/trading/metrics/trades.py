"""Round trips and cost drag, from a run's per-fill ledger.

Pure, like `curve`: fills in, numbers out. No database, no float.

A **trade** here is a FIFO round trip: each sell closes the oldest open buy
on the same instrument. That definition has to be stated because win rate,
profit factor, average win/loss and expectancy all depend on it, and other
conventions (LIFO, average-cost) give different answers on the same fills.

A position still open when the run ends is **not** a trade. Counting it as
a loss would understate win rate for every strategy that ends holding
something -- which is most of them -- and counting it as a win would be
worse.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

__all__ = ["RoundTrip", "cost_drag", "round_trips", "trade_metrics"]

# Nothing leaves this module as a bare `str(Decimal)`. That reports whatever
# precision the arithmetic happened to produce -- an implementation detail,
# not data -- and it has bitten this codebase three times: the equity curve,
# the metric ratios, and trade money. Money is 4 dp, ratios 8 dp.
_MONEY = Decimal("0.0001")
_RATIO = Decimal("0.00000001")


def _money(value: Decimal | None) -> str | None:
    return None if value is None else str(value.quantize(_MONEY))


def _ratio(value: Decimal | None) -> str | None:
    return None if value is None else str(value.quantize(_RATIO))


@dataclass(frozen=True)
class RoundTrip:
    instrument_id: str
    opened_ts: str
    closed_ts: str
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    gross_pnl: Decimal
    charges: Decimal

    @property
    def net_pnl(self) -> Decimal:
        return self.gross_pnl - self.charges


@dataclass
class _Open:
    ts: str
    quantity: Decimal
    price: Decimal
    charges: Decimal
    original_quantity: Decimal


def round_trips(fills: list[dict[str, Any]]) -> list[RoundTrip]:
    """FIFO round trips, in the order they closed.

    A fill's charges are split **proportionally by quantity** when it is
    matched in parts. Attributing an entry's whole charge to the first exit
    would make that round trip look worse than it was and the next one
    better, and every trade metric reads those numbers.
    """
    open_by_instrument: dict[str, list[_Open]] = {}
    trips: list[RoundTrip] = []

    for fill in fills:
        instrument = str(fill["instrument_id"])
        quantity = Decimal(fill["quantity"])
        price = Decimal(fill["price"])
        charges = Decimal(fill["total_charges"])
        if quantity <= 0:
            continue

        if fill["side"] == "BUY":
            open_by_instrument.setdefault(instrument, []).append(
                _Open(
                    ts=str(fill["ts"]),
                    quantity=quantity,
                    price=price,
                    charges=charges,
                    original_quantity=quantity,
                )
            )
            continue

        remaining = quantity
        queue = open_by_instrument.get(instrument, [])
        while remaining > 0 and queue:
            entry = queue[0]
            matched = min(remaining, entry.quantity)
            entry_share = entry.charges * (matched / entry.original_quantity)
            exit_share = charges * (matched / quantity)
            trips.append(
                RoundTrip(
                    instrument_id=instrument,
                    opened_ts=entry.ts,
                    closed_ts=str(fill["ts"]),
                    quantity=matched,
                    entry_price=entry.price,
                    exit_price=price,
                    gross_pnl=(price - entry.price) * matched,
                    charges=entry_share + exit_share,
                )
            )
            entry.quantity -= matched
            remaining -= matched
            if entry.quantity == 0:
                queue.pop(0)
        # A sell with nothing open against it closes nothing. It cannot
        # happen through the order API (short selling is not supported), and
        # silently inventing a trade for it would be worse than ignoring it.

    return trips


def trade_metrics(fills: list[dict[str, Any]]) -> dict[str, Any]:
    """Win rate, profit factor, average win/loss and expectancy, on net P&L.

    Net, not gross: a trade that made money before charges and lost it after
    is a loss, and this platform's whole point is that the difference is
    real.
    """
    trips = round_trips(fills)
    if not trips:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "profit_factor": None,
            "average_win": None,
            "average_loss": None,
            "expectancy": None,
        }

    wins = [t.net_pnl for t in trips if t.net_pnl > 0]
    losses = [t.net_pnl for t in trips if t.net_pnl < 0]
    total = Decimal(len(trips))
    gross_win = sum(wins, Decimal(0))
    gross_loss = -sum(losses, Decimal(0))

    average_win = (gross_win / Decimal(len(wins))) if wins else None
    average_loss = (sum(losses, Decimal(0)) / Decimal(len(losses))) if losses else None
    return {
        "trades": len(trips),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": _ratio(Decimal(len(wins)) / total),
        # None rather than infinity when nothing was lost: a profit factor
        # with no denominator is not a large number, it is undefined.
        "profit_factor": None if gross_loss == 0 else _ratio(gross_win / gross_loss),
        "average_win": _money(average_win),
        "average_loss": _money(average_loss),
        "expectancy": _money(sum((t.net_pnl for t in trips), Decimal(0)) / total),
    }


def cost_drag(fills: list[dict[str, Any]]) -> dict[str, Any]:
    """How much of the gross result the Indian cost stack ate.

    §8 calls this the most sobering chart we can show a retail options
    trader, and it is the reason the ledger stores components rather than a
    total. `drag` is charges over the gross result: 0.4 means costs took
    40% of what the strategy made before them.

    `None` unless the gross result is positive. A strategy that made
    nothing -- or lost money -- before charges has no edge for costs to take
    a share of, and `charges / gross` over a negative gross yields a
    negative ratio that reads as nonsense: a real run produced -5.54, i.e.
    "costs took -554%". The caller is left to say the true thing instead,
    which is that costs turned a small gross loss into a large net one.
    """
    trips = round_trips(fills)
    charges = sum((Decimal(f["total_charges"]) for f in fills), Decimal(0))
    gross = sum((t.gross_pnl for t in trips), Decimal(0))
    return {
        "total_charges": _money(charges),
        "gross_pnl": _money(gross),
        "net_pnl": _money(gross - charges),
        "drag": None if gross <= 0 else _ratio(charges / gross),
    }
