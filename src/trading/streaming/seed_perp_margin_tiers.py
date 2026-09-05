"""Seed maintenance-margin tiers for the seeded perpetual universe.

Refuses rather than guesses. Binance's bracket endpoint is signed, so
without `BINANCE_API_KEY` / `BINANCE_API_SECRET` this exits saying so and
writes nothing. The alternative -- hardcoding maintenance rates from
memory -- would produce a liquidation model that looks precise and
liquidates at the wrong price, which is worse than one that visibly does
not exist yet.

Usage: uv run python -m trading.streaming.seed_perp_margin_tiers
"""

from __future__ import annotations

import sys
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime

import psycopg
import structlog
from psycopg import Connection

from trading.config import get_settings
from trading.sources.binance_margin_tiers import MarginTier, fetch_margin_tiers
from trading.streaming.seed_perp_instruments import PERP_UNIVERSE, platform_symbol

log = structlog.get_logger(__name__)

_INSTRUMENT_ID = """
    SELECT instrument_id FROM instruments
    WHERE asset_class='PERP' AND exchange='BINANCE_FUTURES' AND symbol=%s
"""

_UPSERT_TIER = """
    INSERT INTO perp_margin_tiers
        (instrument_id, notional_floor, effective_from, notional_cap,
         max_leverage, maintenance_rate, maintenance_amount, source_note)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, notional_floor, effective_from) DO UPDATE SET
        notional_cap = EXCLUDED.notional_cap,
        max_leverage = EXCLUDED.max_leverage,
        maintenance_rate = EXCLUDED.maintenance_rate,
        maintenance_amount = EXCLUDED.maintenance_amount
"""


def seed_margin_tiers(
    conn: Connection,
    tiers: Sequence[MarginTier],
    *,
    on: date | None = None,
    universe: Sequence[str] = PERP_UNIVERSE,
) -> int:
    """Write tiers for instruments already in the master. Returns the count.

    Tiers for symbols outside the seeded universe are skipped silently:
    the endpoint returns every contract Binance lists, and that is not a
    reason to widen what this platform trades.
    """
    on = on or datetime.now(UTC).date()
    allowed = set(universe)
    written = 0
    for tier in tiers:
        if tier.symbol not in allowed:
            continue
        row = conn.execute(_INSTRUMENT_ID, (platform_symbol(tier.symbol),)).fetchone()
        if row is None:
            log.warning("seed_perp_tiers.instrument_absent", symbol=tier.symbol)
            continue
        conn.execute(
            _UPSERT_TIER,
            (
                int(row[0]),
                tier.notional_floor,
                on,
                tier.notional_cap,
                tier.max_leverage,
                tier.maintenance_rate,
                tier.maintenance_amount,
                f"binance leverageBracket {on.isoformat()}",
            ),
        )
        written += 1
    return written


def main() -> None:
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    settings = get_settings()
    if not settings.binance_api_key or not settings.binance_api_secret:
        sys.exit(
            "BINANCE_API_KEY and BINANCE_API_SECRET are not set. Binance's "
            "leverageBracket endpoint is signed, and maintenance-margin rates are "
            "not guessable -- a wrong one liquidates at the wrong price. Create a "
            "read-only key (no trading, no withdrawal permission) at "
            "https://www.binance.com/en/my/settings/api-management and add both to .env."
        )
    tiers = fetch_margin_tiers(
        settings.binance_api_key,
        settings.binance_api_secret,
        timestamp_ms=int(time.time() * 1000),
    )
    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        written = seed_margin_tiers(conn, tiers)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    log.info("seed_perp_tiers.done", tiers_written=written)


if __name__ == "__main__":
    main()
