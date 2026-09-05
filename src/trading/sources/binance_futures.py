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
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from trading.config import get_settings
from trading.contracts import FetchError
from trading.sources.http import ArchivingClient

__all__ = ["URL", "PerpContractSpec", "fetch_contract_specs", "parse_contract_specs"]

log = structlog.get_logger(__name__)

URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
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
