"""Binance USDⓈ-M contract specifications, from the public exchange info.

`exchangeInfo` is unauthenticated and authoritative: it carries the tick
size, quantity step, minimum notional and liquidation fee that order
validation has to enforce. Today `instruments.tick_size` is NULL for every
crypto row, which is survivable for spot -- where a fill is priced by a
trade that already happened -- and is not survivable for a derivative,
where an order that does not respect the step is an order the venue would
have rejected and we would have filled.

Filters are read strictly. A contract missing one is skipped rather than
defaulted: a step size of zero accepts any quantity and a minimum notional
of zero accepts dust, and both are worse than the contract not existing.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import structlog

from trading.config import get_settings
from trading.contracts import FetchError
from trading.sources.http import ArchivingClient

__all__ = [
    "FUNDING_URL",
    "KLINES_URL",
    "URL",
    "FundingSettlement",
    "PerpBar",
    "PerpContractSpec",
    "fetch_contract_specs",
    "fetch_funding_history",
    "fetch_klines",
    "parse_contract_specs",
    "parse_funding_history",
    "parse_klines",
]

log = structlog.get_logger(__name__)

_BASE = "https://fapi.binance.com/fapi/v1"
URL = f"{_BASE}/exchangeInfo"
FUNDING_URL = f"{_BASE}/fundingRate"
KLINES_URL = f"{_BASE}/klines"

# Both history endpoints cap a page well below the `limit` they accept:
# fundingRate returns at most 500 rows, klines at most 1500. Paging is
# driven by the last row's timestamp rather than by an offset, so a page
# that comes back short is the end of history, not a dropped request.
FUNDING_PAGE = 500
KLINES_PAGE = 1500
UTC_ZONE = ZoneInfo("UTC")

# Quarterlies (`CURRENT_QUARTER`, `NEXT_QUARTER`) arrive from the same
# endpoint. They expire and settle, which needs the expiry machinery this
# phase deliberately does not build.
_PERPETUAL = "PERPETUAL"
# A portfolio is single-currency (CRIT-1), so a contract margined in
# anything but USDT could never be ordered from the USDT portfolios that
# exist. Seeding it would add an instrument nothing can ever trade.
_MARGIN_ASSET = "USDT"


@dataclass(frozen=True)
class PerpContractSpec:
    """One perpetual contract's tradable shape."""

    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    liquidation_fee: Decimal


def _filters(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {f["filterType"]: f for f in row.get("filters", [])}


def parse_contract_specs(raw: bytes) -> list[PerpContractSpec]:
    """Every live, USDT-margined perpetual in the payload."""
    payload = json.loads(raw)
    specs: list[PerpContractSpec] = []
    skipped = 0
    for row in payload.get("symbols", []):
        if row.get("contractType") != _PERPETUAL:
            continue
        if row.get("status") != "TRADING":
            continue
        if row.get("marginAsset") != _MARGIN_ASSET:
            continue
        found = _filters(row)
        try:
            price, lot, notional = (
                found["PRICE_FILTER"],
                found["LOT_SIZE"],
                found["MIN_NOTIONAL"],
            )
            specs.append(
                PerpContractSpec(
                    symbol=str(row["symbol"]),
                    base_asset=str(row["baseAsset"]),
                    quote_asset=str(row["quoteAsset"]),
                    tick_size=Decimal(str(price["tickSize"])),
                    step_size=Decimal(str(lot["stepSize"])),
                    min_qty=Decimal(str(lot["minQty"])),
                    min_notional=Decimal(str(notional["notional"])),
                    liquidation_fee=Decimal(str(row["liquidationFee"])),
                )
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            skipped += 1
            log.warning("binance_futures.contract_skipped", symbol=row.get("symbol"))
    if skipped:
        log.warning("binance_futures.contracts_skipped", count=skipped)
    return specs


def fetch_contract_specs(client: ArchivingClient | None = None) -> list[PerpContractSpec]:
    """Today's specifications, archived then parsed.

    Archived like every other source: Binance revises filters, and a
    reconstruction of why an order was accepted in March needs March's
    filters, not today's.
    """
    client = client or ArchivingClient(root=get_settings().raw_archive_root)
    on = datetime.now(UTC_ZONE).date()
    name = f"binance_futures_exchange_info/{on:%Y}/{on:%m}/{on.isoformat()}.json"
    result = client.get(URL, archive_name=name, prime=None)
    if result is None:
        raise FetchError(f"{URL} returned 404; no contract specifications to seed from")
    body, _path, _digest = result
    return parse_contract_specs(body)


@dataclass(frozen=True)
class FundingSettlement:
    """One funding settlement: what longs paid shorts, and when.

    `mark_price` is optional because Binance's earliest 2019 rows carry an
    empty one. Those settlements happened and a position held then really
    paid them, so dropping the row would lose money that moved; inventing
    a mark would be worse than admitting we do not have it.
    """

    symbol: str
    funding_time: datetime
    rate: Decimal
    mark_price: Decimal | None


@dataclass(frozen=True)
class PerpBar:
    """One kline. `ts` is the START of the interval, per contract §5."""

    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trades: int


def _epoch_ms(raw: object) -> datetime:
    if not isinstance(raw, (int, float, str)):
        raise TypeError(f"expected an epoch, got {type(raw).__name__}")
    return datetime.fromtimestamp(int(raw) / 1000, UTC_ZONE)


def parse_funding_history(raw: bytes) -> list[FundingSettlement]:
    settlements: list[FundingSettlement] = []
    for row in json.loads(raw):
        try:
            mark = str(row.get("markPrice", "")).strip()
            settlements.append(
                FundingSettlement(
                    symbol=str(row["symbol"]),
                    funding_time=_epoch_ms(row["fundingTime"]),
                    rate=Decimal(str(row["fundingRate"])),
                    mark_price=Decimal(mark) if mark else None,
                )
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            log.warning("binance_futures.funding_row_skipped", row=row)
    return settlements


def parse_klines(raw: bytes) -> list[PerpBar]:
    bars: list[PerpBar] = []
    for row in json.loads(raw):
        try:
            bars.append(
                PerpBar(
                    ts=_epoch_ms(row[0]),
                    open=Decimal(str(row[1])),
                    high=Decimal(str(row[2])),
                    low=Decimal(str(row[3])),
                    close=Decimal(str(row[4])),
                    volume=Decimal(str(row[5])),
                    quote_volume=Decimal(str(row[7])),
                    trades=int(row[8]),
                )
            )
        except (IndexError, KeyError, TypeError, ValueError, InvalidOperation):
            log.warning("binance_futures.kline_row_skipped")
    return bars


def _paged(
    client: httpx.Client,
    url: str,
    params: dict[str, Any],
    *,
    start_ms: int,
    page_size: int,
    parse: Callable[[bytes], list[Any]],
    timestamp_of: Callable[[Any], datetime],
) -> list[Any]:
    """Walk a Binance history endpoint forward until it stops giving rows.

    Paged by the last row's timestamp plus a millisecond rather than by an
    offset: these endpoints have no cursor, and an offset would silently
    skip or repeat rows whenever history changed under the walk.
    """
    collected: list[Any] = []
    cursor = start_ms
    while True:
        response = client.get(url, params={**params, "startTime": cursor, "limit": page_size})
        response.raise_for_status()
        page = parse(response.content)
        if not page:
            return collected
        collected.extend(page)
        if len(page) < page_size:
            return collected
        cursor = int(timestamp_of(page[-1]).timestamp() * 1000) + 1


def fetch_funding_history(
    symbol: str, *, start_ms: int, client: httpx.Client | None = None
) -> list[FundingSettlement]:
    """Every funding settlement for `symbol` from `start_ms` onward."""
    owned = client is None
    client = client or httpx.Client(timeout=30.0)
    try:
        return _paged(
            client,
            FUNDING_URL,
            {"symbol": symbol},
            start_ms=start_ms,
            page_size=FUNDING_PAGE,
            parse=parse_funding_history,
            timestamp_of=lambda s: s.funding_time,
        )
    finally:
        if owned:
            client.close()


def fetch_klines(
    symbol: str, interval: str, *, start_ms: int, client: httpx.Client | None = None
) -> list[PerpBar]:
    """Every kline for `symbol` at `interval` from `start_ms` onward."""
    owned = client is None
    client = client or httpx.Client(timeout=30.0)
    try:
        return _paged(
            client,
            KLINES_URL,
            {"symbol": symbol, "interval": interval},
            start_ms=start_ms,
            page_size=KLINES_PAGE,
            parse=parse_klines,
            timestamp_of=lambda b: b.ts,
        )
    finally:
        if owned:
            client.close()
