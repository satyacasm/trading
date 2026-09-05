"""Backfill perpetual price and funding history from Binance.

Unlike everything else this platform records, perpetual history is not on
a clock. NSE intraday option chains are a commercial product, which is why
the chain recorder accrues one irreplaceable day at a time; tagged Indian
news is the same. Binance serves perpetual klines and funding settlements
back to 2019 for free, forever, so this is a job that can be run whenever
and re-run without loss.

Both walks resume from the newest row already stored. Seven years is
sixteen pages of funding per contract; a re-run should cost one.

Usage:
    uv run python -m trading.streaming.perp_backfill              # daily bars + funding
    uv run python -m trading.streaming.perp_backfill --interval 1m --symbol BTCUSDT
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, datetime

import psycopg
import structlog
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import DataSource
from trading.sources.binance_futures import (
    FundingSettlement,
    PerpBar,
    fetch_funding_history,
    fetch_klines,
)
from trading.streaming.seed_perp_instruments import PERP_UNIVERSE, platform_symbol

log = structlog.get_logger(__name__)

# 2019-09-08: Binance's first USDⓈ-M perpetual kline. Starting earlier
# just costs an empty page.
GENESIS_MS = 1567900800000

_INSTRUMENT_ID = """
    SELECT instrument_id FROM instruments
    WHERE asset_class='PERP' AND exchange='BINANCE_FUTURES' AND symbol=%s
"""

# `trades` is the count Binance reports. `turnover` takes the quote volume:
# for a USDT-margined perpetual the quote asset is the settlement currency,
# so quote volume is turnover in the portfolio's own money.
_UPSERT_BAR = """
    INSERT INTO bars_daily
        (instrument_id, ts, open, high, low, close, volume, turnover, trades, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, ts) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        turnover = EXCLUDED.turnover, trades = EXCLUDED.trades
"""

_UPSERT_INTRADAY = """
    INSERT INTO bars_intraday
        (instrument_id, ts, interval_sec, open, high, low, close, volume,
         turnover, trades, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        turnover = EXCLUDED.turnover, trades = EXCLUDED.trades
"""

_UPSERT_FUNDING = """
    INSERT INTO perp_funding (instrument_id, funding_time, rate, mark_price)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (instrument_id, funding_time) DO UPDATE SET
        rate = EXCLUDED.rate, mark_price = EXCLUDED.mark_price
"""

_INTERVAL_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}


def resume_from(
    conn: Connection, instrument_id: int, table: str, column: str, default_ms: int
) -> int:
    """One millisecond past the newest row stored, or `default_ms`.

    A millisecond past, not at: Binance's `startTime` is inclusive, so
    resuming exactly at the stored row re-reads it every run.
    """
    if table not in {"perp_funding", "bars_daily", "bars_intraday"}:
        raise ValueError(f"refusing to interpolate an unknown table name: {table!r}")
    row = conn.execute(
        f"SELECT max({column}) FROM {table} WHERE instrument_id = %s",  # noqa: S608
        (instrument_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return default_ms
    return int(row[0].timestamp() * 1000) + 1


def write_bars(
    conn: Connection, instrument_id: int, bars: Sequence[PerpBar], interval: str = "1d"
) -> int:
    """Upsert klines. Returns the number written.

    Upsert rather than insert because Binance revises a kline while its
    interval is still open: a backfill re-run that includes today must
    correct the row it wrote an hour ago.
    """
    source = DataSource.BINANCE_FUTURES_KLINE.value
    seconds = _INTERVAL_SECONDS.get(interval)
    for bar in bars:
        values = (
            instrument_id,
            bar.ts,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.quote_volume,
            bar.trades,
        )
        if seconds is None:
            conn.execute(_UPSERT_BAR, (*values, source))
        else:
            conn.execute(
                _UPSERT_INTRADAY,
                (instrument_id, bar.ts, seconds, *values[2:], source),
            )
    return len(bars)


def write_funding(
    conn: Connection, instrument_id: int, settlements: Sequence[FundingSettlement]
) -> int:
    for settlement in settlements:
        conn.execute(
            _UPSERT_FUNDING,
            (instrument_id, settlement.funding_time, settlement.rate, settlement.mark_price),
        )
    return len(settlements)


def _instrument_id(conn: Connection, binance_symbol: str) -> int | None:
    row = conn.execute(_INSTRUMENT_ID, (platform_symbol(binance_symbol),)).fetchone()
    return None if row is None else int(row[0])


def backfill(
    conn: Connection,
    symbols: Sequence[str],
    *,
    interval: str,
    with_funding: bool,
) -> None:
    for symbol in symbols:
        instrument_id = _instrument_id(conn, symbol)
        if instrument_id is None:
            log.warning("perp_backfill.instrument_absent", symbol=symbol)
            continue

        table = "bars_daily" if interval == "1d" else "bars_intraday"
        start = resume_from(conn, instrument_id, table, "ts", GENESIS_MS)
        bars = fetch_klines(symbol, interval, start_ms=start)
        written = write_bars(conn, instrument_id, bars, interval)
        conn.commit()
        log.info(
            "perp_backfill.bars",
            symbol=symbol,
            interval=interval,
            written=written,
            since=datetime.fromtimestamp(start / 1000, UTC).date().isoformat(),
        )

        if not with_funding:
            continue
        start = resume_from(conn, instrument_id, "perp_funding", "funding_time", GENESIS_MS)
        settlements = fetch_funding_history(symbol, start_ms=start)
        written = write_funding(conn, instrument_id, settlements)
        conn.commit()
        log.info("perp_backfill.funding", symbol=symbol, written=written)


def main() -> None:
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="1d", choices=["1d", *_INTERVAL_SECONDS])
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--no-funding", action="store_true")
    args = parser.parse_args()

    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    backfill(
        conn,
        args.symbols or list(PERP_UNIVERSE),
        interval=args.interval,
        with_funding=not args.no_funding,
    )


if __name__ == "__main__":
    main()
