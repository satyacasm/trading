"""Parsing Binance's public USDⓈ-M contract specifications."""

from __future__ import annotations

import json
from decimal import Decimal

from trading.sources.binance_futures import parse_contract_specs

_BTC = {
    "symbol": "BTCUSDT",
    "contractType": "PERPETUAL",
    "status": "TRADING",
    "baseAsset": "BTC",
    "quoteAsset": "USDT",
    "marginAsset": "USDT",
    "liquidationFee": "0.012500",
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "556.80"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        {"filterType": "MIN_NOTIONAL", "notional": "50"},
    ],
}
_DELIVERY = {**_BTC, "symbol": "BTCUSDT_260626", "contractType": "CURRENT_QUARTER"}
_DELISTED = {**_BTC, "symbol": "OLDCOIN", "status": "SETTLING"}
_COIN_MARGINED = {**_BTC, "symbol": "BTCBUSD", "quoteAsset": "BUSD", "marginAsset": "BUSD"}


def _payload(symbols: list[dict[str, object]]) -> bytes:
    return json.dumps({"symbols": symbols}).encode()


def test_a_perpetual_becomes_a_spec_with_every_filter_it_needs() -> None:
    (spec,) = parse_contract_specs(_payload([_BTC]))
    assert spec.symbol == "BTCUSDT"
    # Decimal throughout: these are the numbers order validation compares
    # against, and a float step of 0.001 does not divide a quantity cleanly.
    assert spec.tick_size == Decimal("0.10")
    assert spec.step_size == Decimal("0.001")
    assert spec.min_qty == Decimal("0.001")
    assert spec.min_notional == Decimal("50")
    assert spec.liquidation_fee == Decimal("0.012500")
    assert isinstance(spec.tick_size, Decimal)


def test_only_perpetuals_are_taken() -> None:
    """Binance serves dated quarterlies from the same endpoint. They expire,
    settle, and need the F&O machinery this phase is deliberately not
    building yet."""
    specs = parse_contract_specs(_payload([_BTC, _DELIVERY]))
    assert [s.symbol for s in specs] == ["BTCUSDT"]


def test_only_trading_contracts_are_taken() -> None:
    specs = parse_contract_specs(_payload([_BTC, _DELISTED]))
    assert [s.symbol for s in specs] == ["BTCUSDT"]


def test_only_usdt_margined_contracts_are_taken() -> None:
    """A portfolio is single-currency (CRIT-1), so a BUSD-margined contract
    could never be traded from a USDT portfolio anyway -- seeding it would
    put an instrument in the master that nothing can ever order."""
    specs = parse_contract_specs(_payload([_BTC, _COIN_MARGINED]))
    assert [s.symbol for s in specs] == ["BTCUSDT"]


def test_a_contract_missing_a_filter_is_skipped_not_defaulted() -> None:
    """A missing step size defaulted to zero would accept any quantity; a
    missing minNotional defaulted to zero would accept dust. Both are worse
    than the contract simply not existing."""
    broken = {**_BTC, "symbol": "NOFILTERS", "filters": []}
    specs = parse_contract_specs(_payload([broken, _BTC]))
    assert [s.symbol for s in specs] == ["BTCUSDT"]
