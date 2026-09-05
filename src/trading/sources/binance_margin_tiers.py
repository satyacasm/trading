"""Binance USDⓈ-M maintenance-margin brackets.

What liquidation reads. A position is liquidated when its equity falls
below the maintenance requirement for its notional tier:

    maintenance = |qty| x mark x maintenance_rate - maintenance_amount

`maintenance_amount` (Binance's `cum`) is not optional decoration: without
it the tiered formula overstates the requirement at every tier above the
first, and the simulator liquidates positions that were never near the
line.

**This endpoint is signed**, unlike every other Binance source here, so it
needs an API key and secret. A read-only key is enough -- no trading
permission, no funds at risk. Until one exists these tiers cannot be
fetched, and they are deliberately not hardcoded from memory: a maintenance
rate invented from recollection is exactly the class of quietly-wrong cost
model that `MissingChargeSchedule` exists to refuse. A perpetual with no
tier is a perpetual that cannot be ordered, which is the honest state.

Like `LiveUpstoxFeed`, the network call here is not exercised by the test
suite (no network in tests). The parsing and the signing are; the request
itself is unverified until an operator runs it with real credentials.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

__all__ = ["URL", "MarginTier", "fetch_margin_tiers", "parse_margin_tiers", "signed_query"]

log = structlog.get_logger(__name__)

URL = "https://fapi.binance.com/fapi/v1/leverageBracket"


@dataclass(frozen=True)
class MarginTier:
    symbol: str
    notional_floor: Decimal
    notional_cap: Decimal
    max_leverage: Decimal
    maintenance_rate: Decimal
    maintenance_amount: Decimal


def signed_query(params: dict[str, Any], *, secret: str) -> str:
    """The query string with Binance's HMAC-SHA256 signature appended.

    The signature covers the whole query, and the signature parameter must
    come last -- Binance verifies against everything preceding it.
    """
    query = urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def parse_margin_tiers(raw: bytes) -> list[MarginTier]:
    """Every bracket in the payload, flattened across symbols."""
    payload = json.loads(raw)
    tiers: list[MarginTier] = []
    for entry in payload:
        symbol = str(entry["symbol"])
        for bracket in entry.get("brackets", []):
            tiers.append(
                MarginTier(
                    symbol=symbol,
                    notional_floor=Decimal(str(bracket["notionalFloor"])),
                    notional_cap=Decimal(str(bracket["notionalCap"])),
                    max_leverage=Decimal(str(bracket["initialLeverage"])),
                    maintenance_rate=Decimal(str(bracket["maintMarginRatio"])),
                    maintenance_amount=Decimal(str(bracket["cum"])),
                )
            )
    return tiers


def fetch_margin_tiers(
    api_key: str, api_secret: str, *, timestamp_ms: int, client: httpx.Client | None = None
) -> list[MarginTier]:
    """Every symbol's brackets. Requires a Binance API key and secret.

    `timestamp_ms` is passed in rather than read from the clock so the
    caller owns the value that gets signed -- Binance rejects a request
    whose timestamp drifts more than `recvWindow` from its own clock, and a
    caller that can see the timestamp can report that clearly instead of
    surfacing an opaque -1021.
    """
    owned = client is None
    client = client or httpx.Client(timeout=20.0)
    try:
        response = client.get(
            f"{URL}?{signed_query({'timestamp': timestamp_ms}, secret=api_secret)}",
            headers={"X-MBX-APIKEY": api_key},
        )
        response.raise_for_status()
        return parse_margin_tiers(response.content)
    finally:
        if owned:
            client.close()
