"""Seed Binance USDⓈ-M perpetuals into the instrument master.

A perpetual is a different tradable thing from its spot pair: different
price, different costs, different position, different risk. `BTC-USDT`
spot and `BTC-USDT` perp therefore get different `instrument_id`s, kept
apart by exchange and segment in the canonical key.

The universe is a deliberate list rather than everything Binance lists.
There are roughly 500 USDⓈ-M perpetuals; this is a single-user platform
that will trade a handful, and every illiquid contract seeded is one more
row every instrument query scans forever.

Usage: uv run python -m trading.streaming.seed_perp_instruments
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime

import psycopg
import structlog
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import AssetClass, InstrumentRef
from trading.sources.binance_futures import PerpContractSpec, fetch_contract_specs

log = structlog.get_logger(__name__)

EXCHANGE = "BINANCE_FUTURES"
SEGMENT = "PERP"

# The majors we already carry spot for, so a strategy can compare the two,
# plus nothing else. Extending this list is a deliberate act.
PERP_UNIVERSE: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
)

_UPSERT_INSTRUMENT = """
    INSERT INTO instruments
        (asset_class, exchange, segment, symbol, currency, status, canonical_key)
    VALUES (%s, %s, %s, %s, 'USDT', 'ACTIVE', %s)
    ON CONFLICT (canonical_key) DO UPDATE SET updated_at = now()
    RETURNING instrument_id
"""

_CURRENT_SPEC = """
    SELECT effective_from, tick_size, step_size, min_qty, min_notional, liquidation_fee
    FROM perp_contract_specs
    WHERE instrument_id = %s AND effective_to IS NULL
"""

_CLOSE_SPEC = """
    UPDATE perp_contract_specs SET effective_to = %s
    WHERE instrument_id = %s AND effective_from = %s
"""

_INSERT_SPEC = """
    INSERT INTO perp_contract_specs
        (instrument_id, effective_from, tick_size, step_size, min_qty,
         min_notional, liquidation_fee, source_note)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, effective_from) DO NOTHING
"""


def platform_symbol(binance_symbol: str) -> str:
    """`BTCUSDT` as `BTC-USDT`.

    Every other instrument in this database is hyphenated; Binance is not.
    One convention, applied at the boundary rather than at each read.
    """
    for quote in ("USDT",):
        if binance_symbol.endswith(quote):
            return f"{binance_symbol.removesuffix(quote)}-{quote}"
    return binance_symbol


def perp_canonical_keys(symbols: Sequence[str] = PERP_UNIVERSE) -> list[str]:
    """Canonical keys for the perpetual universe, without touching the
    database -- the same read-only escape hatch `crypto_canonical_keys`
    provides for spot."""
    return [
        InstrumentRef(
            exchange=EXCHANGE, segment=SEGMENT, symbol=platform_symbol(symbol)
        ).canonical_key
        for symbol in symbols
    ]


def _sync_spec(conn: Connection, instrument_id: int, spec: PerpContractSpec, on: date) -> None:
    """Keep one open spec row per contract, closing the old one when a
    filter actually changes.

    Rewriting in place would make an order accepted under March's tick size
    unreconstructable against March's rules, which is the same reason
    `charge_schedules` and `instrument_lot_history` are dated.
    """
    current = conn.execute(_CURRENT_SPEC, (instrument_id,)).fetchone()
    incoming = (
        spec.tick_size,
        spec.step_size,
        spec.min_qty,
        spec.min_notional,
        spec.liquidation_fee,
    )
    if current is not None:
        effective_from, *held = current
        if all(a == b for a, b in zip(held, incoming, strict=True)):
            return
        if effective_from >= on:
            # Same day, changed filters: replace rather than open a second
            # row that would collide on the primary key.
            conn.execute(
                "DELETE FROM perp_contract_specs WHERE instrument_id=%s AND effective_from=%s",
                (instrument_id, effective_from),
            )
        else:
            conn.execute(_CLOSE_SPEC, (on, instrument_id, effective_from))
    conn.execute(
        _INSERT_SPEC,
        (instrument_id, on, *incoming, f"binance exchangeInfo {on.isoformat()}"),
    )


def seed_perp_instruments(
    conn: Connection,
    specs: Sequence[PerpContractSpec],
    *,
    on: date | None = None,
    universe: Sequence[str] = PERP_UNIVERSE,
) -> dict[str, int]:
    """Idempotent upsert of PERP instrument rows and their dated filters.

    Returns `{"BTC-USDT": instrument_id, ...}`. A spec outside `universe`
    raises: seeding is how the master stays a deliberate list, and a caller
    passing the whole exchange should hear about it rather than quietly
    adding 500 contracts.
    """
    on = on or datetime.now(UTC).date()
    allowed = set(universe)
    result: dict[str, int] = {}
    for spec in specs:
        if spec.symbol not in allowed:
            raise ValueError(
                f"{spec.symbol} is not in the seeded universe; add it to PERP_UNIVERSE "
                "deliberately rather than seeding whatever the exchange lists"
            )
        symbol = platform_symbol(spec.symbol)
        ref = InstrumentRef(exchange=EXCHANGE, segment=SEGMENT, symbol=symbol)
        row = conn.execute(
            _UPSERT_INSTRUMENT,
            (AssetClass.PERP.value, EXCHANGE, SEGMENT, symbol, ref.canonical_key),
        ).fetchone()
        assert row is not None
        instrument_id = int(row[0])
        _sync_spec(conn, instrument_id, spec, on)
        result[symbol] = instrument_id
    return result


def main() -> None:
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    specs = [s for s in fetch_contract_specs() if s.symbol in set(PERP_UNIVERSE)]
    missing = set(PERP_UNIVERSE) - {s.symbol for s in specs}
    if missing:
        log.warning("seed_perp.contracts_absent", symbols=sorted(missing))
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        seeded = seed_perp_instruments(conn, specs)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    log.info("seed_perp.done", seeded=len(seeded), symbols=sorted(seeded))


if __name__ == "__main__":
    main()
