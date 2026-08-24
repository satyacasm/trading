"""Entry point: `python -m trading.streaming.crypto_ingestor`.

Streams Binance trades, parses each into a Tick, and publishes it to
Redis. Reconnects with exponential backoff on any failure; a single
malformed message is logged and skipped, never fatal (same rule
`recorder/upstox_ws.py` applies to a bad frame).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import structlog
from redis.asyncio import Redis

from trading.config import get_settings
from trading.streaming.binance_feed import BinanceFeed, LiveBinanceFeed, parse_trade_message
from trading.streaming.seed_instruments import seed_crypto_instruments

log = structlog.get_logger(__name__)

FeedFactory = Callable[[], BinanceFeed]
Sleeper = Callable[[float], Awaitable[None]]


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def run_ingestion_loop(
    redis: Redis,
    feed_factory: FeedFactory,
    *,
    instrument_ids: dict[str, int],
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 30.0,
    sleep: Sleeper = _default_sleep,
    max_ticks: int | None = None,
) -> None:
    """Stream ticks from `feed_factory()`, PUBLISHing each to Redis.

    Runs forever when `max_ticks` is None (production). Stops after
    publishing `max_ticks` ticks when it's an int -- a test seam standing
    in for the natural session-close boundary `recorder/upstox_ws.py`'s
    loop has and this one doesn't (crypto markets never close).
    """
    backoff = initial_backoff_seconds
    published = 0

    try:
        while max_ticks is None or published < max_ticks:
            feed = feed_factory()
            try:
                await feed.connect()
                backoff = initial_backoff_seconds

                async for raw in feed:
                    tick = parse_trade_message(raw, instrument_ids)
                    if tick is None:
                        continue
                    try:
                        await redis.publish(f"ticks:{tick.instrument_id}", tick.model_dump_json())
                        published += 1
                    except Exception as exc:  # noqa: BLE001 - a publish failure must not kill the socket
                        log.warning("crypto_ingestor.publish_failed", reason=str(exc))
                    if max_ticks is not None and published >= max_ticks:
                        break
            except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect, not a crash
                log.warning("crypto_ingestor.disconnected", reason=str(exc))
                wait = min(backoff, max_backoff_seconds)
                await sleep(wait)
                backoff = min(backoff * 2, max_backoff_seconds)
            finally:
                try:
                    await feed.aclose()
                except Exception:  # noqa: BLE001 - closing must never itself crash the loop
                    log.debug("crypto_ingestor.close_failed", exc_info=True)
    finally:
        # Release any pooled connection(s) opened during this run before
        # control returns to the caller's event loop. Without this, a
        # connection created here stays bound to this coroutine's loop; if
        # the caller later closes `redis` from a *different* `asyncio.run()`
        # call (as short-lived test runs do), closing that stale connection
        # raises `RuntimeError: Event loop is closed`. Disconnecting here,
        # inside the loop that created it, avoids that -- and is harmless
        # in production, where the pool simply reopens connections on the
        # next publish.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()

    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        symbol_to_id = seed_crypto_instruments(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    instrument_ids = {symbol.replace("-", "").lower(): iid for symbol, iid in symbol_to_id.items()}
    log.info("crypto_ingestor.starting", pairs=list(symbol_to_id))

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                redis, lambda: LiveBinanceFeed(list(symbol_to_id)), instrument_ids=instrument_ids
            )
        )
    except KeyboardInterrupt:
        log.info("crypto_ingestor.interrupted")


if __name__ == "__main__":
    main()
