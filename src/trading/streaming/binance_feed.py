"""Binance public trade-stream feed and parser.

Governing principle, same as `recorder/upstox_ws.py`: a single message we
can't interpret is logged and skipped, never fatal to the connection.
Unlike the recorder, this module parses live -- there is no raw-archive
leg in this sub-project (see the design doc for why).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol, cast

import structlog
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

from trading.streaming.models import Tick

log = structlog.get_logger(__name__)

_STREAM_BASE = "wss://stream.binance.com:9443/stream"


def _binance_symbol(pair: str) -> str:
    """'BTC-USDT' -> 'btcusdt' (Binance's wire symbol, no separator, lowercase)."""
    return pair.replace("-", "").lower()


def _stream_url(pairs: Sequence[str]) -> str:
    streams = "/".join(f"{_binance_symbol(p)}@trade" for p in pairs)
    return f"{_STREAM_BASE}?streams={streams}"


class BinanceFeed(Protocol):
    """One live connection to Binance's combined trade stream.

    No `authorize()`/`subscribe()` the way `UpstoxFeed` has them: Binance's
    public stream needs neither -- the pairs are chosen by the connection
    URL itself. Tests supply a fake that yields a scripted sequence of raw
    messages and, optionally, raises to simulate a dropped connection.
    """

    async def connect(self) -> None:
        """Open the connection. Raise on failure."""

    def __aiter__(self) -> AsyncIterator[str]:
        """Yield raw JSON message strings as they arrive. May raise to signal disconnect."""

    async def aclose(self) -> None:
        """Best-effort close; errors here must never propagate."""


def parse_trade_message(raw: str, instrument_ids: dict[str, int]) -> Tick | None:
    """Parse one combined-stream message into a `Tick`.

    `instrument_ids` is keyed by lowercase Binance symbol ("btcusdt"), not
    our display symbol. Returns None -- and logs why -- for anything that
    isn't a trade event for a tracked symbol, or that doesn't parse; never
    raises, so one bad message never kills the caller's loop.
    """
    try:
        envelope = json.loads(raw)
        data = envelope["data"]
        if data["e"] != "trade":
            return None
        binance_symbol = str(data["s"]).lower()
        instrument_id = instrument_ids.get(binance_symbol)
        if instrument_id is None:
            return None
        return Tick(
            instrument_id=instrument_id,
            ts=datetime.fromtimestamp(int(data["T"]) / 1000, tz=UTC),
            price=Decimal(data["p"]),
            quantity=Decimal(data["q"]),
            side="sell" if data["m"] else "buy",  # m: is the buyer the maker?
        )
    except (KeyError, TypeError, ValueError, InvalidOperation, json.JSONDecodeError) as exc:
        log.warning("binance_feed.malformed_message", reason=str(exc), raw=raw[:200])
        return None


class LiveBinanceFeed:
    """Real `BinanceFeed`: opens Binance's public combined trade stream.

    Does real network I/O and is therefore never exercised by the default
    test run (no network in tests, per the global constraints) -- only the
    single `@pytest.mark.live` test touches the real endpoint.
    """

    def __init__(self, pairs: Sequence[str]) -> None:
        self._pairs = pairs
        self._connection: ClientConnection | None = None

    async def connect(self) -> None:
        self._connection = await ws_connect(_stream_url(self._pairs))

    def __aiter__(self) -> AsyncIterator[str]:
        if self._connection is None:
            raise RuntimeError("iteration started before a successful connect()")
        return cast(AsyncIterator[str], self._connection.__aiter__())

    async def aclose(self) -> None:
        if self._connection is not None:
            await self._connection.close()
