"""Entry point: `python -m trading.recorder`.

Reads the Upstox access token and the instrument universe to subscribe to
from the environment (see the operator section of the Task 15 report for
exactly which variables), opens a `RecordingSession` rooted at
`settings.recordings_root`, and runs the capture loop until the session's
close time or the process is killed.

`trading.config.get_settings()` currently has no field for either the
access token or the instrument universe -- see the Task 15 report for why
this reads them from the environment directly instead.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime, timedelta

import structlog

from trading.config import get_settings
from trading.recorder.session import RecordingSession
from trading.recorder.upstox_ws import LiveUpstoxFeed, run_recording_loop

log = structlog.get_logger(__name__)

SOURCE_KEY = "upstox_chain"
ACCESS_TOKEN_ENV_VAR = "UPSTOX_ACCESS_TOKEN"
UNIVERSE_ENV_VAR = "UPSTOX_RECORDER_UNIVERSE"


def _universe_from_env(raw: str) -> list[str]:
    return [key.strip() for key in raw.split(",") if key.strip()]


def _session_close(started_at: datetime) -> datetime:
    """NSE/BSE cash close is 15:30 IST, i.e. 10:00 UTC."""
    close = started_at.replace(hour=10, minute=0, second=0, microsecond=0)
    if close <= started_at:
        # Started after today's close (e.g. manual run for testing):
        # record briefly rather than exiting immediately.
        close = started_at + timedelta(minutes=5)
    return close


def main() -> None:
    settings = get_settings()
    access_token = os.environ.get(ACCESS_TOKEN_ENV_VAR)
    universe = _universe_from_env(os.environ.get(UNIVERSE_ENV_VAR, ""))

    if not access_token:
        sys.exit(f"{ACCESS_TOKEN_ENV_VAR} is not set; cannot authorise against the Upstox feed.")
    if not universe:
        sys.exit(
            f"{UNIVERSE_ENV_VAR} is not set (comma-separated instrument keys); nothing to record."
        )

    now = datetime.now(UTC)
    session = RecordingSession(
        root=settings.recordings_root, source_key=SOURCE_KEY, session_date=now.date()
    )
    session.open()
    until = _session_close(now)
    log.info(
        "recorder.starting",
        universe=universe,
        session_date=now.date().isoformat(),
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
