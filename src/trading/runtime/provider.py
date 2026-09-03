"""Bars, and the only shape the runtime reads them through.

This package is copied into the strategy sandbox image, so nothing here
may import psycopg, docker, or `trading.config` -- the container has no
database and no network. `tests/agent_contract/test_image_contents.py`
enforces that.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import groupby

__all__ = ["BarRecord", "InMemoryBars"]


@dataclass(frozen=True)
class BarRecord:
    """One completed interval. Structurally a `platform_sdk.Bar`.

    Frozen because the same record is handed to the strategy and kept in
    the history the strategy reads back; a mutable bar would let a
    strategy rewrite its own past.
    """

    instrument_id: int
    ts: datetime
    interval_sec: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    trades: int | None = None
    open_interest: int | None = None
    oi_change: int | None = None

    @property
    def close_ts(self) -> datetime:
        """When this bar's values became knowable.

        `ts` marks the START of the interval (contract §5), so a clock set
        to `ts` while reading `close` would be reading the future. The loop
        advances to `close_ts` instead.
        """
        return self.ts + timedelta(seconds=self.interval_sec)


class InMemoryBars:
    """Every bar the smoke run will feed, held in memory.

    The sandbox implementation of what Phase 3 will serve from Timescale.
    The interface is deliberately narrow -- group iteration and a history
    prefix -- because those are the only two things the event loop and
    `ctx.data` need, and a wider one would invite a strategy-visible query
    that could reach past `ctx.now`.
    """

    def __init__(self, bars: Mapping[int, Sequence[BarRecord]]) -> None:
        self._bars = {
            instrument_id: tuple(sorted(series, key=lambda b: b.ts))
            for instrument_id, series in bars.items()
        }
        # Index of each bar within its own instrument's series, so
        # `history` can be answered without a scan during the loop.
        flat = [(bar, index) for series in self._bars.values() for index, bar in enumerate(series)]
        # Total ordering: by close time, then instrument id. Ties must
        # break deterministically or the double-run check (D-S6) would
        # report false divergences.
        flat.sort(key=lambda pair: (pair[0].close_ts, pair[0].instrument_id))
        self._flat = flat

    def instruments(self) -> tuple[int, ...]:
        return tuple(sorted(self._bars))

    def total_bars(self) -> int:
        return len(self._flat)

    def groups(self) -> Iterator[tuple[datetime, tuple[BarRecord, ...]]]:
        """Bars grouped by the instant they all became knowable.

        An instrument that did not print in an interval is simply absent
        from its group -- never carried forward. Contract §4: the platform
        will not invent a trade that did not happen.
        """
        for close_ts, pairs in groupby(self._flat, key=lambda pair: pair[0].close_ts):
            yield close_ts, tuple(pair[0] for pair in pairs)

    def indexed_groups(self) -> Iterator[tuple[datetime, tuple[tuple[BarRecord, int], ...]]]:
        """`groups()`, but each bar paired with its index in its own series."""
        for close_ts, pairs in groupby(self._flat, key=lambda pair: pair[0].close_ts):
            yield close_ts, tuple(pairs)

    def history(self, instrument_id: int, upto_index: int) -> tuple[BarRecord, ...]:
        """Bars STRICTLY BEFORE `upto_index`.

        Strict, not inclusive: the bar currently being processed has not
        closed from the strategy's point of view until the loop has
        dispatched it, and returning it here is the lookahead the contract
        promises is impossible.
        """
        return self._bars.get(instrument_id, ())[:upto_index]
