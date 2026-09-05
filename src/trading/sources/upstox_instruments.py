"""Upstox's public instrument dump: the map from our contracts to their keys.

The market-data feed addresses everything by `instrument_key`
(`NSE_FO|50917`), which is Upstox's own token and is not derivable from
anything in our instrument master -- our options carry exchange symbols,
strikes and expiries, not Upstox tokens. This file is the only bridge, and
it is published without authentication, so the chain recorder can decide
what to subscribe to before it holds a token.

Fetched through `ArchivingClient` like every other source: the raw bytes are
kept so a parser fixed in month four can be re-run against the dump as it
was in month one, rather than against a dump where the contracts of interest
have long since expired and been dropped.
"""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import structlog

from trading.config import get_settings
from trading.contracts import FetchError
from trading.recorder.universe import UpstoxInstrument
from trading.sources.http import ArchivingClient

__all__ = ["URL", "fetch_instruments", "parse_instruments"]

log = structlog.get_logger(__name__)

URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
IST = ZoneInfo("Asia/Kolkata")


def _expiry_date(raw: object) -> date | None:
    """An expiry epoch (ms) as the IST calendar day it names.

    The dump stamps expiry at 23:59:59 IST on the expiry day. Read in UTC
    that is 18:29 the same day, which happens to agree -- but a stamp at
    00:30 IST would not, and a contract retired a day early is the kind of
    error that shows up as an unexplained gap in a backtest months later.
    """
    if raw is None:
        return None
    if not isinstance(raw, (int, float, str)):
        raise TypeError(f"expiry must be an epoch, got {type(raw).__name__}")
    return datetime.fromtimestamp(int(raw) / 1000, IST).date()


def _decimal(raw: object) -> Decimal | None:
    if raw is None:
        return None
    # str() first: Decimal(float) would carry the float's binary error into
    # a value used for equality comparison against other strikes.
    return Decimal(str(raw))


def parse_instruments(raw: bytes) -> list[UpstoxInstrument]:
    """Every row of the dump we can make sense of.

    A row that will not parse is logged and skipped rather than raising:
    this is 118,000 rows a day from a source we do not control, and one bad
    expiry must not cost the whole subscription universe.
    """
    rows = json.loads(gzip.decompress(raw))
    parsed: list[UpstoxInstrument] = []
    skipped = 0
    for row in rows:
        try:
            parsed.append(
                UpstoxInstrument(
                    instrument_key=str(row["instrument_key"]),
                    segment=str(row.get("segment", "")),
                    underlying_symbol=str(row.get("underlying_symbol", "")),
                    underlying_key=str(row.get("underlying_key", "")),
                    instrument_type=str(row.get("instrument_type", "")),
                    expiry=_expiry_date(row.get("expiry")),
                    strike=_decimal(row.get("strike_price")),
                    lot_size=None if row.get("lot_size") is None else int(row["lot_size"]),
                    trading_symbol=str(row.get("trading_symbol", "")),
                )
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            skipped += 1
    if skipped:
        log.warning("upstox_instruments.rows_skipped", count=skipped, total=len(rows))
    return parsed


def fetch_instruments(client: ArchivingClient | None = None) -> list[UpstoxInstrument]:
    """Today's dump, archived then parsed.

    Named by the IST date the recorder would call today: the dump is a
    snapshot with no date of its own, and a file archived under the wrong
    day is worse than no archive, since it silently answers for a session
    it was not taken during.
    """
    client = client or ArchivingClient(root=get_settings().raw_archive_root)
    on = datetime.now(IST).date()
    name = f"upstox_instruments/{on:%Y}/{on:%m}/{on.isoformat()}.json.gz"
    result = client.get(URL, archive_name=name, prime=None)
    if result is None:
        raise FetchError(f"{URL} returned 404; no instrument dump to record against")
    body, _path, _digest = result
    return parse_instruments(body)
