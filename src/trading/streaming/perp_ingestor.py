"""Live perpetual marks and closed bars, by polling.

**Why polling and not a WebSocket.** Every other live feed here streams.
The futures socket does not work from this jurisdiction: it connects,
accepts a SUBSCRIBE and returns `{"result":null,"id":1}` -- a success ack
-- and then delivers no market data at all, on both the combined `/stream`
and raw `/ws` forms, while the spot socket from the same machine delivers
in under a second and futures REST returns 200 throughout. Binance Futures
is not offered to Indian users and its streaming data is gated
accordingly; the public REST market-data endpoints are not. Verified
2026-09-05.

Polling turns out to suit this shape well. One `premiumIndex` call returns
the mark price, funding rate and next funding time for all 898 listed
contracts, so the whole universe costs one request rather than eight
subscriptions. Closed bars come from the same `klines` endpoint the
backfill uses, which means a live bar and a backfilled bar are the same
row from the same source rather than two series that have to agree.

Usage: uv run python -m trading.streaming.perp_ingestor
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

import httpx
import psycopg
import structlog
from psycopg import Connection
from redis import Redis

from trading.config import get_settings
from trading.contracts import DataSource
from trading.sources.binance_futures import KLINES_URL, PerpBar, parse_klines
from trading.streaming.perp_backfill import write_bars
from trading.streaming.seed_perp_instruments import PERP_UNIVERSE, platform_symbol

log = structlog.get_logger(__name__)

PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

MARK_CHANNEL = "perp_marks"
MARK_KEY = "perp_mark"
# The aggregator's channel, spoken deliberately: the live supervisor learns
# that a minute closed from `closed_bars:*` and nowhere else, so a perpetual
# has to arrive on the same channel an equity does or a strategy trading it
# would never be dispatched a bar.
CLOSED_BAR_CHANNEL = "closed_bars"

INTERVAL = "1m"
INTERVAL_SECONDS = 60


class MarkSink(Protocol):
    """The two Redis calls this module makes, and no more.

    Parameter names mirror redis-py's own (`message`, `name`) rather than
    reading naturally, because a Protocol is matched by keyword too: a
    prettier name here would make the real client fail to satisfy it.
    """

    def publish(self, channel: str, message: str) -> Any: ...
    def set(self, name: str, value: str) -> Any: ...


@dataclass(frozen=True)
class Mark:
    """One contract's mark price and the funding it is accruing."""

    symbol: str
    instrument_id: int
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time: datetime

    def as_json(self) -> str:
        return json.dumps(
            {
                "instrument_id": self.instrument_id,
                "symbol": self.symbol,
                # Strings, like every other number that crosses a process
                # boundary here: a mark is what a liquidation check compares
                # against, and JSON numbers are doubles.
                "mark_price": str(self.mark_price),
                "index_price": str(self.index_price),
                "funding_rate": str(self.funding_rate),
                "next_funding_time": self.next_funding_time.isoformat(),
            }
        )


def marks_for(client: httpx.Client, instrument_ids: Mapping[str, int]) -> list[Mark]:
    """Marks for the contracts we actually seeded.

    One request covers every listed contract, so the filtering happens here
    rather than in the URL -- 898 come back and eight are wanted.
    """
    response = client.get(PREMIUM_INDEX_URL, timeout=15.0)
    response.raise_for_status()
    marks: list[Mark] = []
    for row in response.json():
        instrument_id = instrument_ids.get(str(row.get("symbol")))
        if instrument_id is None:
            continue
        try:
            marks.append(
                Mark(
                    symbol=str(row["symbol"]),
                    instrument_id=instrument_id,
                    mark_price=Decimal(str(row["markPrice"])),
                    index_price=Decimal(str(row["indexPrice"])),
                    funding_rate=Decimal(str(row["lastFundingRate"])),
                    next_funding_time=datetime.fromtimestamp(
                        int(row["nextFundingTime"]) / 1000, UTC
                    ),
                )
            )
        except (KeyError, TypeError, ValueError, ArithmeticError):
            log.warning("perp_ingestor.mark_unparseable", symbol=row.get("symbol"))
    return marks


def publish_mark(redis: MarkSink, mark: Mark) -> None:
    """Announce the mark, and leave it where it can be read back.

    Both, because they answer different questions. A subscriber listening
    now wants the change; a liquidation check waking between publishes
    wants the last known value, and pub/sub cannot answer that.
    """
    payload = mark.as_json()
    redis.publish(f"{MARK_CHANNEL}:{mark.instrument_id}", payload)
    redis.set(f"{MARK_KEY}:{mark.instrument_id}", payload)


def closed_klines(
    client: httpx.Client, symbol: str, *, now: datetime, limit: int = 3
) -> list[PerpBar]:
    """The finished bars among the last `limit`.

    Binance's final element is the interval still in progress: its close
    moves every second until the minute ends. Taking it would hand a
    strategy a bar that has not happened yet -- lookahead arriving through
    the live feed, which is the one place the point-in-time rule cannot
    protect against it.
    """
    response = client.get(
        KLINES_URL, params={"symbol": symbol, "interval": INTERVAL, "limit": limit}, timeout=15.0
    )
    response.raise_for_status()
    bars = parse_klines(response.content)
    cutoff = now.timestamp()
    return [b for b in bars if b.ts.timestamp() + INTERVAL_SECONDS <= cutoff]


def publish_closed_bar(redis: MarkSink, instrument_id: int, bar: PerpBar) -> None:
    redis.publish(
        f"{CLOSED_BAR_CHANNEL}:{instrument_id}",
        json.dumps(
            {
                "instrument_id": instrument_id,
                "ts": bar.ts.isoformat(),
                "interval_sec": INTERVAL_SECONDS,
                "open": str(bar.open),
                "high": str(bar.high),
                "low": str(bar.low),
                "close": str(bar.close),
                "volume": str(bar.volume),
                "source": DataSource.BINANCE_FUTURES_KLINE.value,
            }
        ),
    )


def _seeded_instruments(conn: Connection, universe: Sequence[str]) -> dict[str, int]:
    found: dict[str, int] = {}
    for symbol in universe:
        row = conn.execute(
            "SELECT instrument_id FROM instruments WHERE asset_class='PERP'"
            " AND exchange='BINANCE_FUTURES' AND symbol=%s",
            (platform_symbol(symbol),),
        ).fetchone()
        if row is None:
            log.warning("perp_ingestor.instrument_absent", symbol=symbol)
            continue
        found[symbol] = int(row[0])
    return found


def high_water_marks(conn: Connection, instrument_ids: Mapping[str, int]) -> dict[int, datetime]:
    """The newest stored 1-minute bar per contract.

    Seeds the loop's in-memory mark so a restarted ingestor does not
    republish what the previous one already announced. A restart is
    precisely when a live strategy least wants to be handed a bar it has
    already acted on.
    """
    marks: dict[int, datetime] = {}
    for instrument_id in instrument_ids.values():
        row = conn.execute(
            "SELECT max(ts) FROM bars_intraday WHERE instrument_id=%s AND interval_sec=%s",
            (instrument_id, INTERVAL_SECONDS),
        ).fetchone()
        if row is not None and row[0] is not None:
            marks[instrument_id] = row[0]
    return marks


def run_ingestion_loop(
    conn: Connection,
    redis: MarkSink,
    client: httpx.Client,
    instrument_ids: Mapping[str, int],
    *,
    mark_interval_seconds: float = 2.0,
    bar_interval_seconds: float = 30.0,
    iterations: int | None = None,
    sleep: Any = time.sleep,
    clock: Any = None,
) -> None:
    """Poll marks often and bars on the minute, until `iterations` run out.

    `iterations` is a test seam; in production it is None and this runs
    until killed.
    """
    clock = clock or (lambda: datetime.now(UTC))
    last_bar_poll = 0.0
    seen = high_water_marks(conn, instrument_ids)
    done = 0

    while iterations is None or done < iterations:
        try:
            for mark in marks_for(client, instrument_ids):
                publish_mark(redis, mark)
        except Exception as exc:  # noqa: BLE001 - a poll failure must not kill the loop
            log.warning("perp_ingestor.mark_poll_failed", reason=str(exc))

        if time.monotonic() - last_bar_poll >= bar_interval_seconds:
            last_bar_poll = time.monotonic()
            for symbol, instrument_id in instrument_ids.items():
                try:
                    for bar in closed_klines(client, symbol, now=clock()):
                        # `<=`, not `!=`. A page carries several finished
                        # bars, so equality against the newest leaves every
                        # older one comparing unequal and republished on
                        # every poll, forever.
                        newest = seen.get(instrument_id)
                        if newest is not None and bar.ts <= newest:
                            continue
                        write_bars(conn, instrument_id, [bar], INTERVAL)
                        conn.commit()
                        publish_closed_bar(redis, instrument_id, bar)
                        seen[instrument_id] = bar.ts
                except Exception as exc:  # noqa: BLE001 - same reasoning
                    log.warning("perp_ingestor.bar_poll_failed", symbol=symbol, reason=str(exc))

        done += 1
        if iterations is None or done < iterations:
            sleep(mark_interval_seconds)


def main() -> None:
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    settings = get_settings()
    conn = psycopg.connect(settings.database_url, autocommit=False)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    instrument_ids = _seeded_instruments(conn, PERP_UNIVERSE)
    if not instrument_ids:
        raise SystemExit("no perpetual instruments seeded; run seed_perp_instruments first")
    log.info("perp_ingestor.starting", contracts=sorted(instrument_ids))
    with httpx.Client() as client:
        try:
            run_ingestion_loop(conn, redis, client, instrument_ids)
        except KeyboardInterrupt:
            log.info("perp_ingestor.interrupted")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
