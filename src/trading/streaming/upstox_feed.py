"""Upstox V3 market-data protobuf frame parser.

Governing principle, same as `binance_feed.py`: a single message we can't
interpret is logged and skipped, never fatal. Unlike Binance's one-message-
one-trade shape, one Upstox `FeedResponse` frame can carry updates for
several instruments at once (`feeds` is a map), so this returns a list, not
`Tick | None`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import structlog
from google.protobuf.message import DecodeError

from trading.streaming.models import Bar, Tick
from trading.streaming.upstox_proto import MarketDataFeed_pb2 as pb

log = structlog.get_logger(__name__)


def parse_upstox_frame(raw: bytes, instrument_ids: dict[str, int]) -> list[Tick]:
    """Parse one `FeedResponse` frame into zero or more `Tick`s.

    `instrument_ids` is keyed by the exact Upstox instrument_key string
    (e.g. "NSE_EQ|INE002A01018"). Never raises -- logs and returns an empty
    list for anything unparseable; skips (without logging, this is the
    expected common case) any entry for an untracked instrument or one with
    no `ltpc` payload set.
    """
    try:
        response = pb.FeedResponse()  # type: ignore[attr-defined]
        response.ParseFromString(raw)
    except DecodeError as exc:
        log.warning("upstox_feed.malformed_frame", reason=str(exc))
        return []

    ticks: list[Tick] = []
    for instrument_key, feed in response.feeds.items():
        instrument_id = instrument_ids.get(instrument_key)
        if instrument_id is None:
            continue
        ltpc = _extract_ltpc(feed)
        if ltpc is None:
            continue
        try:
            ticks.append(
                Tick(
                    instrument_id=instrument_id,
                    ts=datetime.fromtimestamp(ltpc.ltt / 1000, tz=UTC),
                    price=Decimal(str(ltpc.ltp)),
                    quantity=Decimal(str(ltpc.ltq)),
                )
            )
        except (ValueError, InvalidOperation) as exc:
            log.warning(
                "upstox_feed.malformed_ltpc", instrument_key=instrument_key, reason=str(exc)
            )
    return ticks


_I1_INTERVAL = "I1"


def parse_upstox_bars(raw: bytes, instrument_ids: dict[str, int]) -> list[Bar]:
    """Parse one `FeedResponse` frame into zero or more complete `Bar`s --
    the `I1` (previous completed minute) entry from each tracked
    instrument's authoritative `marketOHLC.ohlc` list.

    Same never-raise contract as `parse_upstox_frame`: malformed bytes are
    logged and produce an empty list. Skips silently (no log, the expected
    common case) an untracked instrument, an unset `FeedUnion`/
    `FullFeedUnion` member, or a frame carrying no `I1` entry at all (mode
    "ltpc" frames and instruments not yet past their first minute never
    have one).
    """
    try:
        response = pb.FeedResponse()  # type: ignore[attr-defined]
        response.ParseFromString(raw)
    except DecodeError as exc:
        log.warning("upstox_feed.malformed_frame", reason=str(exc))
        return []

    bars: list[Bar] = []
    for instrument_key, feed in response.feeds.items():
        instrument_id = instrument_ids.get(instrument_key)
        if instrument_id is None:
            continue
        market_ohlc = _extract_market_ohlc(feed)
        if market_ohlc is None:
            continue
        i1 = next((o for o in market_ohlc.ohlc if o.interval == _I1_INTERVAL), None)
        if i1 is None:
            continue
        try:
            bars.append(
                Bar(
                    instrument_id=instrument_id,
                    ts=datetime.fromtimestamp(i1.ts / 1000, tz=UTC),
                    open=Decimal(str(i1.open)),
                    high=Decimal(str(i1.high)),
                    low=Decimal(str(i1.low)),
                    close=Decimal(str(i1.close)),
                    volume=Decimal(str(i1.volume)),
                )
            )
        except (ValueError, InvalidOperation) as exc:
            log.warning(
                "upstox_feed.malformed_ohlc", instrument_key=instrument_key, reason=str(exc)
            )
    return bars


def _extract_market_ohlc(feed: pb.Feed) -> pb.MarketOHLC | None:  # type: ignore[name-defined]
    """Find the `MarketOHLC` payload wherever this `Feed` carries it, same
    structural approach as `_extract_ltpc`: it only ever lives inside mode
    "full"'s `feed.ff`, one level down inside `FullFeed`'s own oneof --
    `marketFF` for equities, `indexFF` for indices. Returns `None`
    (skip, no log) for anything else, including bare `feed.ltpc` (mode
    "ltpc" carries no OHLC payload at all)."""
    which = feed.WhichOneof("FeedUnion")
    if which != "ff":
        return None
    ff_which = feed.ff.WhichOneof("FullFeedUnion")
    if ff_which == "marketFF":
        return feed.ff.marketFF.marketOHLC
    if ff_which == "indexFF":
        return feed.ff.indexFF.marketOHLC
    return None


def _extract_ltpc(feed: pb.Feed) -> pb.LTPC | None:  # type: ignore[name-defined]
    """Find the `LTPC` payload wherever this `Feed` carries it.

    In mode "ltpc" it sits directly at `feed.ltpc`. In mode "full" (which is
    what this pipeline subscribes with) it never does -- every tick arrives
    as `feed.ff`, one level down inside `FullFeed`'s own oneof: `marketFF`
    for equities, `indexFF` for indices. Returns `None` (skip, no log) for
    an unset/unknown union member at either level.
    """
    which = feed.WhichOneof("FeedUnion")
    if which == "ltpc":
        return feed.ltpc
    if which == "ff":
        ff_which = feed.ff.WhichOneof("FullFeedUnion")
        if ff_which == "marketFF":
            return feed.ff.marketFF.ltpc
        if ff_which == "indexFF":
            return feed.ff.indexFF.ltpc
        return None
    return None
