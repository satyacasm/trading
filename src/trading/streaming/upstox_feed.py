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

from trading.streaming.models import Tick
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
        if feed.WhichOneof("FeedUnion") != "ltpc":
            continue
        try:
            ticks.append(
                Tick(
                    instrument_id=instrument_id,
                    ts=datetime.fromtimestamp(feed.ltpc.ltt / 1000, tz=UTC),
                    price=Decimal(str(feed.ltpc.ltp)),
                    quantity=Decimal(str(feed.ltpc.ltq)),
                )
            )
        except (ValueError, InvalidOperation) as exc:
            log.warning(
                "upstox_feed.malformed_ltpc", instrument_key=instrument_key, reason=str(exc)
            )
    return ticks
