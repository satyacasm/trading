"""Funding: the carry that makes a perpetual track spot without an expiry.

A dated future converges on spot because it settles on a date. A perpetual
has no date, so something else has to pull it back, and that something is
funding: every eight hours the two sides of the market pay each other in
proportion to their notional. When the perpetual trades above spot the
rate is positive and longs pay shorts, which makes holding a long
progressively expensive until the premium closes.

**It is a transfer, not a fee**, and that is the distinction this module
exists to keep. A fee always costs the holder. Funding costs one side and
pays the other, reverses sign when the market flips, and is the entire
return of a carry strategy. Modelling it as a charge would erase an income
stream and invert a whole family of strategies.

Nor is it small. BTC-USDT's mean settlement across 2019-2026 is 0.0001059,
which is roughly 11.6% a year that a long pays a short. A backtest without
it shows every carry strategy earning free money.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal

import structlog
from psycopg import Connection

from trading.paper.enums import EntryType

# The arithmetic lives in `perp`, which imports nothing -- the strategy
# runtime needs these two and is copied into a container with no database,
# no structlog and no psycopg. A pure function in an I/O module is a pure
# function the sandbox cannot have.
from trading.paper.perp import SETTLEMENT_HOURS, funding_payment, settlements_between

__all__ = [
    "SETTLEMENT_HOURS",
    "FundingSettled",
    "funding_payment",
    "settle_funding",
    "settlements_between",
]

log = structlog.get_logger(__name__)


class FundingSettled:
    """What one settlement did, for the log and for the caller's report."""

    def __init__(self, positions: int, total_paid: Decimal) -> None:
        self.positions = positions
        self.total_paid = total_paid


_OPEN_POSITIONS = """
    SELECT p.portfolio_id, p.instrument_id, p.quantity
    FROM perp_positions p
    JOIN portfolios f ON f.portfolio_id = p.portfolio_id
    WHERE p.quantity <> 0 AND f.status = 'ACTIVE'
"""


def settle_funding(
    conn: Connection,
    at: datetime,
    rates: Mapping[int, tuple[Decimal, Decimal]],
) -> FundingSettled:
    """Apply one settlement to every open perpetual position.

    `rates` maps instrument_id to `(rate, mark)`. A position whose
    instrument has no rate is skipped and logged rather than settled at
    zero: settling at zero is indistinguishable in the ledger from a
    genuine zero-rate settlement, and the difference matters when someone
    later asks why a carry strategy earned less than the funding series
    says it should have.

    Every payment becomes a `FUNDING` ledger entry. A P&L a trader cannot
    trace to a row is a P&L they are right not to trust -- and this one
    accrues silently three times a day, which is exactly the kind that
    goes unexplained.
    """
    settled = 0
    total = Decimal("0")
    for portfolio_id, instrument_id, quantity in conn.execute(_OPEN_POSITIONS).fetchall():
        entry = rates.get(instrument_id)
        if entry is None:
            log.warning(
                "funding.no_rate",
                instrument_id=instrument_id,
                portfolio_id=portfolio_id,
                at=at.isoformat(),
            )
            continue
        rate, mark = entry
        paid = funding_payment(quantity, mark=mark, rate=rate)
        if paid == 0:
            continue

        row = conn.execute(
            "UPDATE portfolios SET cash_balance = cash_balance - %s"
            " WHERE portfolio_id = %s RETURNING cash_balance",
            (paid, portfolio_id),
        ).fetchone()
        assert row is not None
        conn.execute(
            "INSERT INTO ledger_entries (portfolio_id, ts, entry_type, amount, balance_after)"
            " VALUES (%s, %s, %s, %s, %s)",
            (portfolio_id, at, EntryType.FUNDING.value, -paid, row[0]),
        )
        conn.execute(
            "UPDATE perp_positions SET funding_paid = funding_paid + %s"
            " WHERE portfolio_id = %s AND instrument_id = %s",
            (paid, portfolio_id, instrument_id),
        )
        settled += 1
        total += paid
    return FundingSettled(settled, total)


def rates_at(
    conn: Connection, at: datetime, instrument_ids: Sequence[int]
) -> dict[int, tuple[Decimal, Decimal]]:
    """The settlement Binance published at `at`, per instrument.

    Read from `perp_funding` -- the series backfilled from Binance -- so a
    live settlement and a backtested one use the same number from the same
    source. A settlement we have no row for is absent from the result,
    which `settle_funding` treats as "skip and say so".
    """
    found: dict[int, tuple[Decimal, Decimal]] = {}
    for instrument_id in instrument_ids:
        row = conn.execute(
            "SELECT rate, mark_price FROM perp_funding"
            " WHERE instrument_id = %s AND funding_time = %s",
            (instrument_id, at),
        ).fetchone()
        if row is None or row[1] is None:
            continue
        found[instrument_id] = (row[0], row[1])
    return found
