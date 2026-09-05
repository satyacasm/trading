"""Parsing Upstox's public instrument dump."""

from __future__ import annotations

import gzip
import json
from datetime import date
from decimal import Decimal

from trading.sources.upstox_instruments import parse_instruments

_OPTION_ROW = {
    "segment": "NSE_FO",
    "name": "NIFTY",
    "exchange": "NSE",
    # 2026-10-27 23:59:59 IST -- the expiry day, stamped at end of day.
    "expiry": 1793125799000,
    "instrument_type": "PE",
    "underlying_symbol": "NIFTY",
    "underlying_key": "NSE_INDEX|Nifty 50",
    "instrument_key": "NSE_FO|50917",
    "lot_size": 75,
    "strike_price": 24500.0,
    "trading_symbol": "NIFTY 24500 PE 27 OCT 26",
}
_EQUITY_ROW = {
    "segment": "NSE_EQ",
    "name": "RELIANCE INDUSTRIES LTD",
    "exchange": "NSE",
    "instrument_type": "EQ",
    "instrument_key": "NSE_EQ|INE002A01018",
    "lot_size": 1,
    "trading_symbol": "RELIANCE",
}


def _dump(rows: list[dict[str, object]]) -> bytes:
    return gzip.compress(json.dumps(rows).encode())


def test_an_option_row_becomes_a_dated_contract() -> None:
    (contract,) = [i for i in parse_instruments(_dump([_OPTION_ROW])) if i.instrument_type == "PE"]
    assert contract.instrument_key == "NSE_FO|50917"
    # The epoch is 23:59:59 in IST on expiry day. Read in any other zone it
    # can land on the day before, which would retire a contract a day early.
    assert contract.expiry == date(2026, 10, 27)
    # Decimal, not float: a strike is a price, and 24500.0 is the one number
    # in this row that later arithmetic will compare for equality.
    assert contract.strike == Decimal("24500")
    assert isinstance(contract.strike, Decimal)
    assert contract.lot_size == 75
    assert contract.underlying_key == "NSE_INDEX|Nifty 50"


def test_a_row_without_expiry_or_strike_still_parses() -> None:
    """Equities share the dump with derivatives; the fields that only make
    sense for a contract are absent, not zero."""
    (equity,) = parse_instruments(_dump([_EQUITY_ROW]))
    assert equity.expiry is None
    assert equity.strike is None
    assert equity.underlying_symbol == ""


def test_a_malformed_row_is_skipped_rather_than_killing_the_dump() -> None:
    """118,000 rows arrive daily from a source we do not control. One row
    with a bad expiry must not cost the other 118,387."""
    broken = {**_OPTION_ROW, "expiry": "not-an-epoch"}
    parsed = parse_instruments(_dump([broken, _EQUITY_ROW]))
    assert [i.instrument_key for i in parsed] == ["NSE_EQ|INE002A01018"]
