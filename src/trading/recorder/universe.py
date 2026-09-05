"""Which contracts the chain recorder subscribes to on a given morning.

The recorder exists because tagged intraday option history is a commercial
product and self-recording is the free floor (plan §3.1). Every trading day
it does not run is a day nobody can sell back, so the selection here is
deliberately generous rather than clever: it is cheaper to record strikes
that turn out not to matter than to discover in March that the window was
too narrow in September.

Selection is a pure function of the instrument dump plus an anchor price,
so it is testable without a network, a token, or a market. Where the anchor
comes from -- and how stale it is -- is the caller's problem, and
`ChainSelection` carries the answer so a run can say what it assumed.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

if TYPE_CHECKING:
    from psycopg import Connection

_UPSTOX_BASE_URL = "https://api.upstox.com"

__all__ = [
    "Anchor",
    "ChainSelection",
    "UpstoxInstrument",
    "anchor_from_futures",
    "anchor_from_index",
    "flatten_keys",
    "select_chain",
]


@dataclass(frozen=True)
class UpstoxInstrument:
    """One tradable contract as Upstox's public instrument dump describes it.

    `instrument_key` is the only field the feed itself needs; the rest is
    what selection reasons over and what a later parser will need to make
    sense of the recorded frames.
    """

    instrument_key: str
    segment: str
    underlying_symbol: str
    underlying_key: str
    instrument_type: str
    expiry: date | None
    strike: Decimal | None
    lot_size: int | None
    trading_symbol: str


@dataclass(frozen=True)
class ChainSelection:
    underlying: str
    expiries: tuple[date, ...]
    strike_step: Decimal
    atm: Decimal
    anchor: Decimal
    anchor_date: date
    contracts: tuple[UpstoxInstrument, ...]
    underlying_key: str

    @property
    def instrument_keys(self) -> tuple[str, ...]:
        """What to subscribe to: the underlying first, then its options.

        The underlying leads because it is the one key whose absence cannot
        be repaired later -- a chain without its spot series cannot be used
        to reconstruct a held position's moneyness or to solve for implied
        volatility, which is most of why intraday option history is worth
        recording at all.
        """
        return (self.underlying_key, *(c.instrument_key for c in self.contracts))


def _modal_step(strikes: list[Decimal]) -> Decimal:
    """The strike interval, measured rather than assumed.

    NIFTY steps 50 and BANKNIFTY 100 today, and exchanges revise both. A
    hard-coded constant does not fail loudly when that happens; it quietly
    records a window a fraction of the intended width.
    """
    ordered = sorted(set(strikes))
    if len(ordered) < 2:
        raise LookupError("cannot infer a strike step from fewer than two distinct strikes")
    gaps = Counter(b - a for a, b in zip(ordered, ordered[1:], strict=False))
    return gaps.most_common(1)[0][0]


def select_chain(
    instruments: list[UpstoxInstrument],
    *,
    underlying: str,
    anchor: Decimal,
    anchor_date: date,
    today: date,
    strikes: int,
    expiries: int,
) -> ChainSelection:
    """The contracts to record for one underlying.

    `anchor` is the last known price of the underlying and `strikes` the
    number of steps to take either side of the nearest strike to it. An
    anchor is always somewhat stale -- the selection is made before the
    session opens -- which is the reason to be generous with `strikes`
    rather than to pretend the anchor is the open.

    Raises `LookupError` rather than returning an empty selection: a
    recorder that subscribes to nothing produces a session file that looks
    exactly like a quiet market, and the day is gone by the time anyone
    reads it.
    """
    live = [
        i
        for i in instruments
        if i.underlying_symbol == underlying
        and i.instrument_type in ("CE", "PE")
        and i.expiry is not None
        and i.strike is not None
        and i.expiry >= today
    ]
    if not live:
        raise LookupError(
            f"no live option contracts for {underlying!r} on or after {today.isoformat()}"
        )

    wanted = sorted({i.expiry for i in live if i.expiry is not None})[:expiries]
    in_window = [i for i in live if i.expiry in wanted]

    step = _modal_step([i.strike for i in in_window if i.strike is not None])
    # The strike nearest the anchor, not the anchor rounded down: an anchor
    # a rupee above a strike belongs to that strike, not to the next one up.
    atm = (anchor / step).quantize(Decimal("1")) * step
    low, high = atm - step * strikes, atm + step * strikes

    contracts = tuple(
        sorted(
            (i for i in in_window if i.strike is not None and low <= i.strike <= high),
            key=lambda i: (i.expiry or today, i.strike or Decimal(0), i.instrument_type),
        )
    )
    return ChainSelection(
        underlying=underlying,
        expiries=tuple(wanted),
        strike_step=step,
        atm=atm,
        anchor=anchor,
        anchor_date=anchor_date,
        contracts=contracts,
        underlying_key=contracts[0].underlying_key if contracts else "",
    )


@dataclass(frozen=True)
class Anchor:
    """A last known price for an underlying, and the day it is from.

    The date travels with the price because it is the part that decides
    whether the recorded window was wide enough. An anchor is never today's
    open -- the selection happens before the session -- so the question is
    never "is this stale" but "how stale, and did we allow for it".
    """

    price: Decimal
    as_of: date

    def age_days(self, on: date) -> int:
        return (on - self.as_of).days

    def is_stale(self, on: date, *, tolerance_days: int) -> bool:
        return self.age_days(on) > tolerance_days


_NEAREST_FUTURE_CLOSE = """
    SELECT b.close, b.ts::date
    FROM bars_daily b JOIN instruments i USING (instrument_id)
    WHERE i.asset_class = 'FUTURE' AND i.symbol = %s AND i.expiry >= %s
    ORDER BY b.ts DESC, i.expiry ASC
    LIMIT 1
"""


def anchor_from_futures(conn: Connection, underlying: str, *, on: date) -> Anchor:
    """The most recent close of the nearest unexpired future.

    The index itself is not in the instrument master -- our options carry no
    `underlying_id` -- so the near-month future is the closest thing we hold
    to spot. Its basis is a few tenths of a percent, which is nothing beside
    a window of twenty strikes either side.

    Raises rather than defaulting: a recorder that guessed an anchor would
    subscribe to a plausible-looking window centred on the wrong price, and
    the recording would be useless in a way nothing later could detect.
    """
    row = conn.execute(_NEAREST_FUTURE_CLOSE, (underlying, on)).fetchone()
    if row is None:
        raise LookupError(
            f"no unexpired futures close for {underlying!r} as of {on.isoformat()}; "
            "cannot anchor the strike window"
        )
    return Anchor(price=Decimal(str(row[0])), as_of=row[1])


def flatten_keys(selections: list[ChainSelection]) -> list[str]:
    """Every key to subscribe to, once each, in a stable order.

    The feed caps how many instruments one connection may carry, so a
    duplicate key is a slot spent on data already arriving. Order is
    preserved rather than sorted: the underlyings lead, and a truncated
    subscription (if a cap is ever hit) then loses strikes furthest from the
    money rather than losing a spot series.
    """
    seen: dict[str, None] = {}
    for selection in selections:
        for key in selection.instrument_keys:
            seen.setdefault(key, None)
    return list(seen)


def anchor_from_index(
    client: httpx.Client, index_key: str, *, token: str, on: date, lookback_days: int = 10
) -> Anchor:
    """The index's own most recent daily close.

    Preferred over `anchor_from_futures`: this is spot itself rather than a
    proxy carrying basis, and its freshness depends on Upstox rather than on
    our EOD ingestion having run. The day this was written those differed by
    fifteen days and 493 NIFTY points, which is ten strikes -- enough to
    leave a twenty-strike window covering only ten strikes below the money.

    `lookback_days` spans weekends and holiday clusters. Raises rather than
    returning a default so the caller can fall back to the futures proxy.
    """
    url = (
        f"{_UPSTOX_BASE_URL}/v3/historical-candle/{quote(index_key, safe='')}/days/1/"
        f"{on.isoformat()}/{(on - timedelta(days=lookback_days)).isoformat()}"
    )
    response = client.get(
        url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}
    )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    candles = payload.get("data", {}).get("candles") or []
    if not candles:
        raise LookupError(f"no daily candles for {index_key!r} in the {lookback_days} days to {on}")
    # The API returns newest first. Each candle is
    # [ts, open, high, low, close, volume, oi].
    newest = candles[0]
    return Anchor(
        price=Decimal(str(newest[4])),
        as_of=datetime.fromisoformat(newest[0]).date(),
    )
