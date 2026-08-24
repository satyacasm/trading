"""CLI to seed the fixed crypto pair universe this streaming sub-project
watches, mirroring `trading.calendar.seed`'s directness for small, static
reference data.

Usage: uv run python -m trading.streaming.seed_instruments
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import AssetClass, InstrumentRef

CRYPTO_PAIRS: tuple[str, ...] = (
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "BNB-USDT",
    "XRP-USDT",
    "ADA-USDT",
    "DOGE-USDT",
    "AVAX-USDT",
    "DOT-USDT",
    "LINK-USDT",
    "POL-USDT",
    "LTC-USDT",
    "TRX-USDT",
    "ATOM-USDT",
    "UNI-USDT",
    "ETC-USDT",
    "XLM-USDT",
    "NEAR-USDT",
    "APT-USDT",
    "ARB-USDT",
    "OP-USDT",
    "FIL-USDT",
    "ICP-USDT",
    "SUI-USDT",
    "INJ-USDT",
)

_UPSERT = """
    INSERT INTO instruments
        (asset_class, exchange, segment, symbol, currency, status, canonical_key)
    VALUES (%s, 'BINANCE', 'SPOT', %s, 'USDT', 'ACTIVE', %s)
    ON CONFLICT (canonical_key) DO UPDATE SET updated_at = now()
    RETURNING instrument_id
"""


def seed_crypto_instruments(
    conn: Connection, pairs: Sequence[str] = CRYPTO_PAIRS
) -> dict[str, int]:
    """Idempotent upsert of CRYPTO/BINANCE/SPOT instrument rows.

    Returns `{"BTC-USDT": instrument_id, ...}`. Re-running with the same
    pair list returns the same instrument_ids every time.
    """
    result: dict[str, int] = {}
    for symbol in pairs:
        ref = InstrumentRef(exchange="BINANCE", segment="SPOT", symbol=symbol)
        row = conn.execute(_UPSERT, (AssetClass.CRYPTO.value, symbol, ref.canonical_key)).fetchone()
        assert row is not None
        result[symbol] = int(row[0])
    return result


def main() -> None:
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        result = seed_crypto_instruments(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    for symbol, instrument_id in result.items():
        print(f"{symbol}: instrument_id={instrument_id}")


if __name__ == "__main__":
    main()
