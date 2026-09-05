"""Entry point: `python -m trading.recorder`.

Records one trading day of Upstox option-chain frames to disk. Designed to
be fired unconditionally every weekday morning by a scheduler and to decide
for itself whether today is a day worth recording -- the exchange calendar
already knows, and a scheduler that encodes holidays is a second copy of
that knowledge which will drift.

Governing principle (spec 5.6): record raw, parse later. Nothing here
interprets a frame. The one interpretation this file does make is *which*
contracts to subscribe to, and it records that decision in the session
manifest so a later parser can tell a strike that was never subscribed to
from one that was subscribed to and never traded.

Environment:
  UPSTOX_ACCESS_TOKEN / UPSTOX_ANALYTICS_TOKEN  either is accepted
  UPSTOX_RECORDER_UNDERLYINGS   default "NIFTY,BANKNIFTY"
  UPSTOX_RECORDER_STRIKES       strikes either side of ATM, default 20
  UPSTOX_RECORDER_EXPIRIES      how many expiries forward, default 2
  UPSTOX_RECORDER_UNIVERSE      explicit keys, bypassing resolution entirely
  UPSTOX_RECORDER_ALLOW_AFTER_CLOSE  record even once the session has shut
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

import httpx
import psycopg
import structlog

from trading.calendar.trading_days import is_trading_day
from trading.config import get_settings
from trading.recorder.session import RecordingSession
from trading.recorder.universe import (
    Anchor,
    ChainSelection,
    UpstoxInstrument,
    anchor_from_futures,
    anchor_from_index,
    flatten_keys,
    select_chain,
)
from trading.recorder.upstox_ws import LiveUpstoxFeed, run_recording_loop
from trading.sources.upstox_instruments import fetch_instruments

log = structlog.get_logger(__name__)

SOURCE_KEY = "upstox_chain"
ACCESS_TOKEN_ENV_VAR = "UPSTOX_ACCESS_TOKEN"
ANALYTICS_TOKEN_ENV_VAR = "UPSTOX_ANALYTICS_TOKEN"
UNIVERSE_ENV_VAR = "UPSTOX_RECORDER_UNIVERSE"
UNDERLYINGS_ENV_VAR = "UPSTOX_RECORDER_UNDERLYINGS"
STRIKES_ENV_VAR = "UPSTOX_RECORDER_STRIKES"
EXPIRIES_ENV_VAR = "UPSTOX_RECORDER_EXPIRIES"
AFTER_CLOSE_ENV_VAR = "UPSTOX_RECORDER_ALLOW_AFTER_CLOSE"

DEFAULT_UNDERLYINGS = "NIFTY,BANKNIFTY"
DEFAULT_STRIKES = 20
DEFAULT_EXPIRIES = 2
# Beyond this the strike window was centred on a price the market has long
# since left. Recording still happens -- a wrongly centred recording beats
# none -- but it is said out loud.
ANCHOR_TOLERANCE_DAYS = 5

IST = ZoneInfo("Asia/Kolkata")


class _HasUpstoxTokens(Protocol):
    upstox_access_token: str | None
    upstox_analytics_token: str | None


def resolve_token(settings: _HasUpstoxTokens, environ: Mapping[str, str]) -> str | None:
    """The Upstox token, from the environment or from `.env`.

    Four names because two already exist in this codebase for the same
    secret (`upstox_ingestor` reads the analytics one, the recorder was
    written against the access one) and an unattended job that cannot find
    a token it has is the worst way to learn which is which.
    """
    for name in (ACCESS_TOKEN_ENV_VAR, ANALYTICS_TOKEN_ENV_VAR):
        value = environ.get(name)
        if value:
            return value
    return settings.upstox_access_token or settings.upstox_analytics_token or None


def _csv_from_env(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def _flag_from_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes"}


def _int_from_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or not raw.strip() else int(raw)


def session_close(started_at: datetime, *, allow_after_close: bool) -> datetime | None:
    """When to stop, or `None` if there is nothing left of today to record.

    NSE/BSE cash close is 15:30 IST, i.e. 10:00 UTC. A scheduler fires the
    missed morning job when the machine wakes, so a laptop opened in the
    evening would otherwise record five minutes of a shut market and leave
    a session file that looks like a real one until somebody opens it.
    """
    close = started_at.replace(hour=10, minute=0, second=0, microsecond=0)
    if close > started_at:
        return close
    return started_at + timedelta(minutes=5) if allow_after_close else None


def _anchor_for(
    conn: psycopg.Connection,
    client: httpx.Client,
    underlying: str,
    instruments: list[UpstoxInstrument],
    *,
    token: str,
    today: date,
) -> Anchor:
    """Spot if we can get it, the futures proxy if we cannot.

    The index close is both fresher and more correct, but it needs a live
    token and a reachable API at 09:10 on a morning nobody is watching. The
    futures close needs only the database. Preferring the first and falling
    back to the second means a token that expired overnight costs the
    recording some precision in where its window is centred, rather than
    costing the recording.
    """
    index_key = next(
        (i.underlying_key for i in instruments if i.underlying_symbol == underlying),
        "",
    )
    if index_key:
        try:
            return anchor_from_index(client, index_key, token=token, on=today)
        except (httpx.HTTPError, LookupError, ValueError, KeyError) as exc:
            log.warning(
                "recorder.index_anchor_failed",
                underlying=underlying,
                index_key=index_key,
                reason=str(exc),
                detail="falling back to the nearest futures close",
            )
    return anchor_from_futures(conn, underlying, on=today)


def resolve_universe(conn: psycopg.Connection, *, token: str) -> list[ChainSelection]:
    """The chains to record today, one selection per configured underlying.

    Fails rather than recording a partial universe: a session missing
    BANKNIFTY is indistinguishable, months later, from a day BANKNIFTY did
    not trade.
    """
    underlyings = _csv_from_env(os.environ.get(UNDERLYINGS_ENV_VAR, DEFAULT_UNDERLYINGS))
    strikes = _int_from_env(STRIKES_ENV_VAR, DEFAULT_STRIKES)
    expiries = _int_from_env(EXPIRIES_ENV_VAR, DEFAULT_EXPIRIES)
    today = datetime.now(IST).date()

    instruments = fetch_instruments()
    log.info("recorder.instrument_dump_read", rows=len(instruments))

    selections = []
    with httpx.Client(timeout=20.0) as client:
        for underlying in underlyings:
            anchor = _anchor_for(conn, client, underlying, instruments, token=token, today=today)
            selection = select_chain(
                instruments,
                underlying=underlying,
                anchor=anchor.price,
                anchor_date=anchor.as_of,
                today=today,
                strikes=strikes,
                expiries=expiries,
            )
            log.info(
                "recorder.chain_selected",
                underlying=underlying,
                expiries=[e.isoformat() for e in selection.expiries],
                strike_step=str(selection.strike_step),
                atm=str(selection.atm),
                anchor=str(anchor.price),
                anchor_as_of=anchor.as_of.isoformat(),
                anchor_age_days=anchor.age_days(today),
                contracts=len(selection.contracts),
            )
            if anchor.is_stale(today, tolerance_days=ANCHOR_TOLERANCE_DAYS):
                log.warning(
                    "recorder.anchor_is_stale",
                    underlying=underlying,
                    anchor_age_days=anchor.age_days(today),
                    detail="the strike window is centred on an old close; both the index "
                    "API and EOD ingestion are behind",
                )
            selections.append(selection)
    return selections


def main() -> None:
    settings = get_settings()
    access_token = resolve_token(settings, os.environ)
    if not access_token:
        sys.exit(
            f"no Upstox token: set {ACCESS_TOKEN_ENV_VAR} or {ANALYTICS_TOKEN_ENV_VAR} in the "
            "environment or in .env; cannot authorise against the feed."
        )

    now = datetime.now(UTC)
    today = datetime.now(IST).date()
    conn = psycopg.connect(settings.database_url, autocommit=True)

    # Fired every weekday by the scheduler; the calendar decides. Exit 0 so
    # a holiday is a quiet no-op rather than a failure notification that
    # trains you to ignore failure notifications.
    if not is_trading_day(conn, "NSE", "FO", today):
        log.info("recorder.not_a_trading_day", session_date=today.isoformat())
        return

    explicit = _csv_from_env(os.environ.get(UNIVERSE_ENV_VAR, ""))
    universe = explicit if explicit else flatten_keys(resolve_universe(conn, token=access_token))
    if not universe:
        sys.exit("resolved an empty universe; nothing to record.")

    session = RecordingSession(
        root=settings.recordings_root, source_key=SOURCE_KEY, session_date=today
    )
    session.open()
    until = session_close(now, allow_after_close=_flag_from_env(AFTER_CLOSE_ENV_VAR))
    if until is None:
        log.info(
            "recorder.session_already_closed",
            session_date=today.isoformat(),
            detail=f"set {AFTER_CLOSE_ENV_VAR}=1 to record anyway",
        )
        return
    log.info(
        "recorder.starting",
        instruments=len(universe),
        session_date=today.isoformat(),
        until=until.isoformat(),
    )

    try:
        asyncio.run(
            run_recording_loop(
                session,
                lambda: LiveUpstoxFeed(access_token),
                universe=universe,
                until=until,
            )
        )
    except KeyboardInterrupt:
        log.info("recorder.interrupted")
        session.close()


if __name__ == "__main__":
    main()
