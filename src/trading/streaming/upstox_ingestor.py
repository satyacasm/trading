"""Entry point: `python -m trading.streaming.upstox_ingestor`.

Streams Upstox trades, parses each frame into zero or more Ticks, and
publishes them to Redis. Reconnects with exponential backoff on any
failure; a single malformed frame is logged and skipped, never fatal.

Reuses `trading.recorder.upstox_ws`'s `UpstoxFeed`/`LiveUpstoxFeed`
(Phase 0's already-built auth/subscribe/frame-iteration code) rather than
duplicating it -- this loop only adds parsing and Redis fan-out on top.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import psycopg
import structlog
from redis.asyncio import Redis

from trading.config import get_settings
from trading.recorder.upstox_ws import LiveUpstoxFeed, UpstoxFeed
from trading.streaming.seed_upstox_instruments import seed_upstox_instrument_keys
from trading.streaming.upstox_feed import parse_upstox_frame

log = structlog.get_logger(__name__)

FeedFactory = Callable[[], UpstoxFeed]
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
    publishing `max_ticks` ticks when it's an int -- a test seam, same
    shape as `crypto_ingestor.run_ingestion_loop`'s.
    """
    backoff = initial_backoff_seconds
    published = 0
    instrument_keys = list(instrument_ids)

    try:
        while max_ticks is None or published < max_ticks:
            feed = feed_factory()
            try:
                await feed.authorize()
                await feed.subscribe(instrument_keys)
                consumed_any = False

                async for raw in feed:
                    if not consumed_any:
                        # Only prove the connection by a real message, not merely a
                        # successful authorize()/subscribe() -- a connect-then-drop
                        # failure must still back off exponentially. Same lesson
                        # already learned on crypto_ingestor.run_ingestion_loop.
                        backoff = initial_backoff_seconds
                        consumed_any = True
                    if not isinstance(raw, bytes):
                        type_name = type(raw).__name__
                        log.warning("upstox_ingestor.unexpected_frame_type", type_name=type_name)
                        continue
                    for tick in parse_upstox_frame(raw, instrument_ids):
                        try:
                            await redis.publish(
                                f"ticks:{tick.instrument_id}", tick.model_dump_json()
                            )
                            published += 1
                        except Exception as exc:  # noqa: BLE001 - a publish failure must not kill the socket
                            log.warning("upstox_ingestor.publish_failed", reason=str(exc))
                        if max_ticks is not None and published >= max_ticks:
                            break
                    if max_ticks is not None and published >= max_ticks:
                        break
            except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect, not a crash
                log.warning("upstox_ingestor.disconnected", reason=str(exc))
                wait = min(backoff, max_backoff_seconds)
                await sleep(wait)
                backoff = min(backoff * 2, max_backoff_seconds)
            finally:
                try:
                    await feed.aclose()
                except Exception:  # noqa: BLE001 - closing must never itself crash the loop
                    log.debug("upstox_ingestor.close_failed", exc_info=True)
    finally:
        # Same reasoning as crypto_ingestor.run_ingestion_loop's identical
        # finally block: release any pooled connection(s) opened during this
        # run before control returns to the caller's event loop.
        await redis.connection_pool.disconnect()


def main() -> None:
    settings = get_settings()
    token = settings.upstox_analytics_token
    if not token:
        raise RuntimeError(
            "UPSTOX_ANALYTICS_TOKEN is not set. Add it to .env (generated from the Upstox "
            "developer console's Analytics Access Token flow, not the daily OAuth token)."
        )

    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        instrument_ids = seed_upstox_instrument_keys(conn)
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

    log.info("upstox_ingestor.starting", instrument_keys=list(instrument_ids))

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        asyncio.run(
            run_ingestion_loop(
                redis,
                lambda: LiveUpstoxFeed(token),
                instrument_ids=instrument_ids,
            )
        )
    except KeyboardInterrupt:
        log.info("upstox_ingestor.interrupted")


if __name__ == "__main__":
    main()
