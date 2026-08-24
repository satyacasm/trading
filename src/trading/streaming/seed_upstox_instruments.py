"""Populate `instruments.source_bindings` with Upstox instrument keys for a
small, fixed NSE equity watchlist -- mirroring `seed_instruments.py`'s
directness for small, static reference data, but updating existing rows
(Phase 0 already backfilled these instruments) rather than inserting new
ones the way the crypto seed does.

Usage: uv run python -m trading.streaming.seed_upstox_instruments
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg import Connection

from trading.config import get_settings

UPSTOX_WATCHLIST: tuple[str, ...] = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK")

_SELECT_EQ_ROW = """
    SELECT instrument_id, isin FROM instruments
    WHERE exchange = 'NSE' AND segment = 'CM' AND asset_class = 'EQUITY'
      AND series = 'EQ' AND symbol = %s
"""

_UPDATE_BINDING = """
    UPDATE instruments
    SET source_bindings = source_bindings || jsonb_build_object('upstox_instrument_key', %s::text)
    WHERE instrument_id = %s
"""


def seed_upstox_instrument_keys(
    conn: Connection, symbols: Sequence[str] = UPSTOX_WATCHLIST
) -> dict[str, int]:
    """Idempotent: derives each symbol's Upstox instrument_key from its
    stored ISIN (`NSE_EQ|<ISIN>`, Upstox's documented convention for NSE
    equities) and writes it into that row's `source_bindings`.

    Returns `{"NSE_EQ|<isin>": instrument_id, ...}`. Raises `ValueError`
    for any symbol that doesn't resolve to exactly one `series='EQ'` row --
    a missing or ambiguous mapping is a correctness bug worth surfacing
    immediately, not silently skipping.
    """
    result: dict[str, int] = {}
    for symbol in symbols:
        row = conn.execute(_SELECT_EQ_ROW, (symbol,)).fetchone()
        if row is None:
            raise ValueError(
                f"No exchange='NSE', segment='CM', asset_class='EQUITY', series='EQ' "
                f"instrument found for symbol {symbol!r}"
            )
        instrument_id, isin = row
        upstox_key = f"NSE_EQ|{isin}"
        conn.execute(_UPDATE_BINDING, (upstox_key, instrument_id))
        result[upstox_key] = int(instrument_id)
    return result


def main() -> None:
    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        result = seed_upstox_instrument_keys(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    for upstox_key, instrument_id in result.items():
        print(f"{upstox_key}: instrument_id={instrument_id}")


if __name__ == "__main__":
    main()
