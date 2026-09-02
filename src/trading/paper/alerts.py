"""Entry point: `python -m trading.paper.alerts`.

Telegram alerts via a transactional outbox (Task 11).

**The engine (and the breaker) must never call the Telegram API.** A
direct HTTP call from inside the fill path would put a third-party
outage -- or merely a slow response -- directly in the way of recording a
trade. Instead, `enqueue_alert` writes a `PENDING` row to
`alert_deliveries` *inside the same transaction* as the event it reports
(a fill, a circuit-breaker trip, an order rejection), and this module's
`run_alert_worker` drains that table from a separate process, on its own
schedule. The worst case of Telegram being down becomes a late
notification, never a stalled or corrupted ledger -- and the table itself
is a durable record of what was sent, what failed, and why.

**Decimal payloads.** JSON has no `Decimal` type. Money/percentage values
in an alert payload (a FILL's `quantity`/`price`, a BREACH's
`equity`/`threshold`) are serialised as *strings*, not floats --
deliberately different from `trading.paper.models`' HTTP-facing field
serialisers, which render `Decimal` as a JSON *number* for a numeric API
consumer to parse back into arithmetic. An alert payload's only consumers
are a human reading a Telegram message and, later, whoever inspects
`alert_deliveries.payload` as an audit trail; nothing here deserialises it
back into a computation, so a string that preserves every digit is
strictly safer than a float that can silently round a money value (e.g.
`positions.quantity`'s eight fractional digits). `_json_default` is the
one place this decision lives -- every `enqueue_alert` call site passes
`Decimal` values straight through rather than hand-rolling `str(...)` at
each site, and any other non-JSON-native type raises `TypeError` instead
of being silently dropped or coerced (this project's no-silent-fallbacks
rule).

**Connection lifecycle differs from `trading.paper.engine`'s.** `run_engine`
opens and closes a fresh connection per fill, for isolation. `run_alert_worker`
instead calls `conn_factory` exactly once and never closes what it
returns -- the connection's lifecycle belongs to the caller. `main` below
opens one connection at process startup and closes it at shutdown, handing
the worker a factory that always returns that same connection; the tests
that prove a sender failure can never roll back the fill that enqueued it
do the same with a rolled-back-at-teardown `db_conn`, specifically so they
can inspect that *same* transaction afterward. Either caller would break
if this module closed the connection itself.

**Known limitation: no reconnect on a dropped connection.** If the one
connection `run_alert_worker` holds dies mid-run, every subsequent batch's
`_drain_pending` raises, is caught, logged, and the loop continues rather
than crashing (see the outer `try`/`except` below, including the nested
guard around `conn.rollback()` itself -- a dead connection can raise
*there* too, and that must not propagate either). But nothing reconnects:
the worker will keep failing every batch, forever, until its process is
restarted. This mirrors an identical, already-accepted gap in this
project's Redis consumers (`trading.streaming.crypto_ingestor` et al.)
and is deliberately out of scope here -- these processes need a
supervisor (e.g. systemd `Restart=on-failure`) rather than in-process
reconnect logic, which is a larger change than this task owns.

**Retry/backoff shape.** `alert_deliveries` (migration 0007) carries
`attempts` and `last_error` but no per-row "next retry at" timestamp, so
there is no per-row elapsed-time backoff to compute here. Instead,
`run_alert_worker` polls in fixed-interval batches (`poll_interval_seconds`,
default 5s, matching the breaker's own periodic-check cadence in
`trading.paper.engine`): each batch attempts every currently-`PENDING`
row exactly once: on success it becomes `SENT`; on failure `attempts` is
incremented and it stays `PENDING` (to be retried on the next batch,
which is at least `poll_interval_seconds` away) unless that increment
reaches `max_attempts`, in which case it becomes `FAILED` and is never
retried again. This is deliberately simple -- appropriate for the
personal, single-user, low-volume system this brief describes, not a
high-throughput delivery system.

**An unconfigured bot is a configuration state, not an error.** This is a
personal single-user system and the user may simply not want Telegram.
`build_telegram_sender` returns `None` when `telegram_bot_token`/
`telegram_chat_id` aren't both set; `run_alert_worker` treats a `None`
sender as a signal to log once and idle, never touching
`alert_deliveries` and never crashing.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
import psycopg
import structlog
from psycopg import Connection

from trading.config import get_settings

if TYPE_CHECKING:
    from trading.config import Settings

log = structlog.get_logger(__name__)

ConnFactory = Callable[[], Connection]
Sender = Callable[[str], None]

_STATUS_PENDING = "PENDING"
_STATUS_SENT = "SENT"
_STATUS_FAILED = "FAILED"

_DEFAULT_MAX_ATTEMPTS = 5
_DEFAULT_POLL_INTERVAL_SECONDS = 5.0
_TELEGRAM_TIMEOUT_SECONDS = 10.0

_TELEGRAM_API_ROOT = "https://api.telegram.org"


def _json_default(value: Any) -> str:
    """`json.dumps`'s `default` hook: `Decimal` becomes an exact string
    (see the module docstring's "Decimal payloads" section); anything
    else raises rather than being silently dropped or coerced.

    Formatted with `format(value, "f")`, not `str(value)` -- `Decimal`'s
    own `__str__` switches to scientific notation for a small-enough
    exponent (`str(Decimal("0.00000001"))` is `"1E-8"`, not
    `"0.00000001"`), which would still be exact but is a needless trap
    for a human reading a Telegram message or the raw `payload` column.
    `format(..., "f")` always renders fixed-point.
    """
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(
        f"alert payload contains a non-JSON-serialisable {type(value).__name__} "
        f"({value!r}) -- serialise it explicitly before calling enqueue_alert"
    )


def enqueue_alert(conn: Connection, kind: str, payload: Mapping[str, Any]) -> None:
    """Write a `PENDING` `alert_deliveries` row. No network call, ever --
    this function only ever executes one INSERT. Deliberately does not
    commit: the caller owns the transaction (matching `trading.paper.
    ledger.apply_fill` and `trading.paper.breaker.trip`/`record_snapshot`'s
    convention), which is what lets a fill/trip/rejection and the alert
    that reports it land in the ledger and the outbox atomically.
    """
    conn.execute(
        "INSERT INTO alert_deliveries (kind, payload, status, attempts) VALUES (%s, %s, %s, 0)",
        (kind, json.dumps(dict(payload), default=_json_default), _STATUS_PENDING),
    )


def _format_message(kind: str, payload_json: str) -> str:
    return f"[{kind}] {payload_json}"


def _drain_pending(conn: Connection, sender: Sender, max_attempts: int) -> None:
    """One pass over every currently-`PENDING` row, oldest first (matches
    migration 0007's `ix_alerts_pending` index on `(status, created_at)`).

    Each row is committed individually, not as a batch: a later row's
    failure must never roll back an earlier row's already-recorded
    success, and -- the load-bearing case -- a row's failure must never
    roll back whatever the *caller* of `enqueue_alert` already wrote on
    this same connection, in this same still-open transaction, before
    this worker ever ran (see `test_telegram_failure_never_affects_the_
    fill`). `sender`'s exception is caught here, not allowed to propagate,
    for exactly that reason.
    """
    rows = conn.execute(
        "SELECT delivery_id, kind, payload, attempts FROM alert_deliveries"
        " WHERE status = %s ORDER BY created_at",
        (_STATUS_PENDING,),
    ).fetchall()

    for delivery_id, kind, payload, attempts in rows:
        try:
            sender(_format_message(kind, payload))
        except Exception as exc:  # noqa: BLE001 - a sender failure is exactly what the
            # outbox exists to absorb; it must never propagate out of this
            # function (see the docstring above).
            new_attempts = attempts + 1
            status = _STATUS_FAILED if new_attempts >= max_attempts else _STATUS_PENDING
            conn.execute(
                "UPDATE alert_deliveries SET attempts=%s, status=%s, last_error=%s"
                " WHERE delivery_id=%s",
                (new_attempts, status, str(exc), delivery_id),
            )
            conn.commit()
            log.warning(
                "alerts.delivery_failed",
                delivery_id=delivery_id,
                kind=kind,
                attempts=new_attempts,
                status=status,
                reason=str(exc),
            )
            continue

        conn.execute(
            "UPDATE alert_deliveries SET status=%s, sent_at=now() WHERE delivery_id=%s",
            (_STATUS_SENT, delivery_id),
        )
        conn.commit()


def _idle(
    max_batches: int | None, poll_interval_seconds: float, sleep: Callable[[float], None]
) -> None:
    batches = 0
    while max_batches is None or batches < max_batches:
        sleep(poll_interval_seconds)
        batches += 1


def run_alert_worker(
    conn_factory: ConnFactory,
    sender: Sender | None,
    *,
    max_batches: int | None = None,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Drain `PENDING` `alert_deliveries` rows forever (`max_batches=None`,
    production) or for exactly `max_batches` passes -- a test seam, the
    same shape as `trading.streaming.crypto_ingestor.run_ingestion_loop`'s
    `max_ticks`.

    `sender is None` means an unconfigured bot (see `build_telegram_sender`)
    -- logs once, then idles for `max_batches` (or forever), touching
    neither the database nor the network, and never crashing. See the
    module docstring's "unconfigured bot" and "retry/backoff shape"
    sections for the rest of this function's behaviour, and "connection
    lifecycle" for why `conn_factory` is called exactly once and the
    connection it returns is never closed here.
    """
    if sender is None:
        # Logged once, here -- before the idle loop, not inside it -- so a
        # long production run doesn't spam this warning on every poll.
        log.warning("alerts.worker_idle_no_bot_token")
        _idle(max_batches, poll_interval_seconds, sleep)
        return

    conn = conn_factory()
    batches = 0
    while max_batches is None or batches < max_batches:
        try:
            _drain_pending(conn, sender, max_attempts)
        except Exception as exc:  # noqa: BLE001 - one batch's failure (e.g. a transient
            # DB error) must never kill a long-running worker.
            log.warning("alerts.batch_failed", reason=str(exc))
            try:
                conn.rollback()
            except Exception as rollback_exc:  # noqa: BLE001 - a connection that is
                # already dead (the common cause of the original failure --
                # a dropped socket, say) can itself raise on rollback().
                # Failing to roll back a dead connection is not, on its
                # own, worth killing the process over: this is deliberately
                # swallowed rather than re-raised, so the loop can still
                # reach the next `sleep`/batch instead of propagating out
                # of run_alert_worker entirely. (No reconnect logic here by
                # design -- see the module docstring's "known limitation"
                # note; a dead connection just keeps failing every
                # subsequent batch, logged each time, until the process is
                # restarted by its supervisor.)
                log.warning("alerts.rollback_failed", reason=str(rollback_exc))
        batches += 1
        if max_batches is not None and batches >= max_batches:
            break
        sleep(poll_interval_seconds)


def build_telegram_sender(
    settings: Settings, *, transport: httpx.BaseTransport | None = None
) -> Sender | None:
    """`None` when `telegram_bot_token`/`telegram_chat_id` aren't both
    set -- the signal `run_alert_worker` treats as "idle, don't crash".
    When both are set, returns a closure that POSTs to the Telegram Bot
    API's `sendMessage` endpoint via `httpx`, matching this codebase's
    established HTTP client (`trading.auth.upstox`, `trading.sources.
    http`). `transport` is the same test seam those modules use --
    `httpx.MockTransport` in tests, never a real socket.
    """
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return None
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id

    def _send(text: str) -> None:
        with httpx.Client(transport=transport, timeout=_TELEGRAM_TIMEOUT_SECONDS) as client:
            response = client.post(
                f"{_TELEGRAM_API_ROOT}/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
            )
            response.raise_for_status()

    return _send


def main() -> None:
    settings = get_settings()
    sender = build_telegram_sender(settings)
    log.info("alerts.starting", enabled=sender is not None)
    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        run_alert_worker(lambda: conn, sender)
    except KeyboardInterrupt:
        log.info("alerts.interrupted")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
