"""Runs strategies forward against live bars.

One container per running strategy, launched here, fed closed bars on
stdin, emitting order intents on stdout. The supervisor turns an intent
into an ordinary paper order and lets the rest of the platform do what it
already does: `paper.engine` fills it against live ticks with the real cost
model, the circuit breaker watches the portfolio, the outbox alerts.

**Orders go over HTTP, not straight to the database.** The gateway's
`POST /orders` is where the currency gate, the market-hours check, the
idempotency rule and the `orders:control` publish live. Reaching past it
would mean a live strategy's orders were validated differently from a
human's -- and "a live strategy's orders are ordinary orders" is the whole
reason the live path is small.

**Rate limiting is here, per §166.** A strategy emitting an order every bar
across a wide universe is a plausible bug, and the engine would faithfully
fill all of them. The cap stops the run and records why, the same posture
the breaker takes toward losses.

Run it with `python -m trading.live.supervisor`.
"""

from __future__ import annotations

import contextlib
import json
import os
import select
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import redis
import structlog
from psycopg import Connection
from psycopg.types.json import Jsonb

from trading.agent_contract.sandbox import SandboxLimits, _docker_args
from trading.agent_contract.smoke import _resolve_limits
from trading.config import get_settings
from trading.db import ReconnectingConnection
from trading.live.cursors import advance_cursor, pending_bars
from trading.live.protocol import (
    FRAME_BAR,
    FRAME_ERROR,
    FRAME_ORDERS,
    FRAME_STOP,
    decode_frame,
    encode_frame,
)
from trading.runtime.payload import MODE_LIVE, SmokePayload, encode_payload
from trading.streaming.heartbeat import start_heartbeat_thread
from trading.streaming.resilient_pubsub import SyncResilientPubSub

log = structlog.get_logger(__name__)

# The aggregator's own output, not Upstox's raw I1 feed on `bars:*`.
# Conflating them would leave a subscriber unable to tell a bar that was
# received from one that was computed.
_BAR_CHANNEL_PATTERN = "closed_bars:*"

# §166 puts order-rate limiting in the supervisor. Generous enough that no
# sane strategy notices, tight enough that a runaway is stopped within a
# minute rather than after a session.
MAX_ORDERS_PER_MINUTE = 60


@dataclass
class LiveRun:
    """One strategy running forward."""

    live_run_id: int
    strategy_id: int
    portfolio_id: int
    process: subprocess.Popen[bytes]
    instrument_ids: set[int]
    runtime: str
    kernel_isolated: bool
    started_at: datetime
    bars_seen: int = 0
    orders_placed: int = 0
    # A refused order is information, not a failure -- but a run refused on
    # every bar reads as an idle one unless the count is kept.
    orders_refused: int = 0
    last_refusal: str | None = None
    last_gap_note: str | None = None
    # What the manifest declared, or None for a strategy that trades
    # nothing levered. Sent on every order this run places: the gateway
    # requires it for a perpetual and refuses it as meaningless otherwise.
    leverage: Decimal | None = None
    _order_times: list[float] = field(default_factory=list)
    # Bytes read from stdout past the frame `read_frames` last returned
    # on. A single os.read() can pull in more than one frame's worth of
    # bytes at once -- without carrying the remainder to the next call,
    # a frame already drained from the kernel pipe would be lost rather
    # than merely delayed.
    _stdout_buffer: bytes = b""

    def over_rate_limit(self) -> bool:
        cutoff = time.monotonic() - 60
        self._order_times = [t for t in self._order_times if t >= cutoff]
        return len(self._order_times) > MAX_ORDERS_PER_MINUTE

    def note_order(self) -> None:
        self._order_times.append(time.monotonic())


def start_run(
    conn: Connection,
    strategy_id: int,
    portfolio_id: int,
    source: str,
    instrument_ids: list[int],
    schedules: Any,
    starting_cash: Decimal,
    slippage_bps: Decimal,
    limits: SandboxLimits | None = None,
    leverage: Decimal | None = None,
) -> LiveRun:
    """Launch a strategy's container and record the run.

    The row is written before the process starts. A container that dies
    immediately must still leave a trace saying it was attempted -- a run
    that vanished without a row is indistinguishable from one that was
    never requested.
    """
    resolved = _resolve_limits(limits)
    row = conn.execute(
        "INSERT INTO live_runs (strategy_id, portfolio_id, status, runtime, kernel_isolated)"
        " VALUES (%s,%s,'RUNNING',%s,%s) RETURNING live_run_id, started_at",
        (
            strategy_id,
            portfolio_id,
            resolved.runtime or "runc",
            (resolved.runtime or "runc") in {"runsc"},
        ),
    ).fetchone()
    assert row is not None
    live_run_id, started_at = int(row[0]), row[1]

    payload = encode_payload(
        SmokePayload(
            mode=MODE_LIVE,
            source=source,
            starting_cash=starting_cash,
            slippage_bps=slippage_bps,
            charge_schedules=tuple(schedules),
            leverage=leverage,
            state_max_bytes=get_settings().live_state_max_bytes,
        )
    )
    process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        _docker_args(resolved, f"live-{live_run_id}"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    process.stdin.write(b"%d\n" % len(payload) + payload)
    process.stdin.flush()

    log.info(
        "live.started",
        live_run_id=live_run_id,
        strategy_id=strategy_id,
        portfolio_id=portfolio_id,
        runtime=resolved.runtime or "runc",
    )
    return LiveRun(
        live_run_id=live_run_id,
        strategy_id=strategy_id,
        portfolio_id=portfolio_id,
        process=process,
        instrument_ids=set(instrument_ids),
        runtime=resolved.runtime or "runc",
        kernel_isolated=(resolved.runtime or "runc") in {"runsc"},
        started_at=started_at,
        leverage=leverage,
    )


def stop_run(conn: Connection, run: LiveRun, status: str, reason: str | None) -> None:
    """End a run and say why.

    The reason is the point. A run stopped by its breaker, by the rate
    limit, by a crashed container, or by an operator are four different
    things, and a bare "stopped" would lose the distinction that makes the
    record worth keeping.
    """
    if run.process.poll() is None:
        try:
            if run.process.stdin is not None:
                run.process.stdin.write(encode_frame(FRAME_STOP).encode())
                run.process.stdin.flush()
                run.process.stdin.close()
            run.process.wait(timeout=10)
        except Exception:  # noqa: BLE001 - a container that will not stop is killed
            run.process.kill()
    conn.execute(
        "UPDATE live_runs SET status=%s, stopped_reason=%s, stopped_at=now(),"
        " bars_seen=%s, orders_placed=%s, orders_refused=%s, last_refusal=%s"
        " WHERE live_run_id=%s",
        (
            status,
            reason,
            run.bars_seen,
            run.orders_placed,
            run.orders_refused,
            run.last_refusal,
            run.live_run_id,
        ),
    )
    log.info("live.stopped", live_run_id=run.live_run_id, status=status, reason=reason)


def feed_bar(run: LiveRun, bar: dict[str, Any]) -> None:
    """Hand one closed bar to a running strategy."""
    if run.process.stdin is None or run.process.poll() is not None:
        return
    run.process.stdin.write(encode_frame(FRAME_BAR, **bar).encode())
    run.process.stdin.flush()
    run.bars_seen += 1


def read_frames(run: LiveRun, timeout: float = 30.0) -> list[dict[str, Any]]:
    """Frames the strategy emitted for the bar just fed, read via
    select() against a real deadline -- not a blocking readline(),
    whose 30s bound used to be checked only BETWEEN reads, so a hung
    container froze every run indefinitely (design §6).

    Reads until an `orders`/`error` frame arrives, which the runner
    emits exactly once per dispatched bar, so the supervisor stays in
    lockstep with the strategy rather than guessing how long a bar
    takes. On the deadline with nothing conclusive yet, returns
    whatever frames were parsed so far (often none).

    A single `os.read()` can return more bytes than one frame's line --
    e.g. a READY frame and the ORDERS frame that follows it, flushed
    together. Anything past the line that ends this call is next bar's
    data, not garbage, so it is carried on `run._stdout_buffer` to the
    next call rather than read again from (and re-lost from) the pipe.
    """
    frames: list[dict[str, Any]] = []
    if run.process.stdout is None:
        return frames
    fd = run.process.stdout.fileno()
    deadline = time.monotonic() + timeout
    buffer = run._stdout_buffer
    while True:
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            frame = decode_frame(line.decode("utf-8", "replace"))
            if frame is None:
                continue
            frames.append(frame)
            if frame["type"] in {FRAME_ORDERS, FRAME_ERROR}:
                run._stdout_buffer = buffer
                return frames
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            run._stdout_buffer = buffer
            return frames
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            run._stdout_buffer = buffer
            return frames
        chunk = os.read(fd, 65536)
        if not chunk:
            run._stdout_buffer = buffer
            return frames  # EOF -- the process closed its stdout
        buffer += chunk


def _refusal_text(detail: str) -> str:
    """The gateway's sentence, unwrapped from its JSON envelope.

    Showing a person `{"detail":"portfolio 10 has base_currency=..."}` puts
    a layer of transport between them and an explanation that was already
    written for them. A body that is not the expected envelope is passed
    through as-is rather than discarded -- an unexpected shape is still the
    only evidence of what went wrong.
    """
    try:
        parsed = json.loads(detail)
    except ValueError:
        return detail
    if isinstance(parsed, dict):
        inner = parsed.get("detail")
        if isinstance(inner, str):
            return inner
    return detail


def place_order(api_url: str, run: LiveRun, intent: dict[str, Any], seq: int) -> bool:
    """Turn an intent into an ordinary paper order, over HTTP.

    The gateway is where validation, the currency gate, market hours and
    the `orders:control` publish live. Writing straight to the table would
    mean a strategy's orders were checked differently from a human's.

    The idempotency key is derived from the run and the order's position in
    it, so a supervisor that retries cannot double-place.
    """
    import urllib.error
    import urllib.request

    body = json.dumps(
        {
            "portfolio_id": run.portfolio_id,
            "instrument_id": intent["instrument_id"],
            "side": intent["side"],
            "order_type": intent["order_type"],
            "quantity": intent["quantity"],
            "limit_price": intent["limit_price"],
            "product": intent["product"],
            "rationale": intent["rationale"],
            "leverage": None if run.leverage is None else str(run.leverage),
            "idempotency_key": f"live-{run.live_run_id}-{seq}",
            "live_run_id": run.live_run_id,
        }
    ).encode()
    request = urllib.request.Request(  # noqa: S310 - fixed localhost gateway
        f"{api_url}/orders", data=body, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
            status: int = response.status
            return 200 <= status < 300
    except urllib.error.HTTPError as exc:
        # A refused order is information, not a failure: the currency gate
        # or the market-hours check doing its job is exactly what a live
        # strategy should experience, and the run continues. The gateway's
        # own sentence is kept on the run so the monitor can answer "why is
        # nothing happening" without anyone opening this log.
        detail = exc.read()[:300].decode("utf-8", "replace")
        run.orders_refused += 1
        run.last_refusal = _refusal_text(detail)
        log.warning(
            "live.order_refused",
            live_run_id=run.live_run_id,
            status=exc.code,
            detail=detail,
        )
        return False
    except Exception as exc:  # noqa: BLE001 - the gateway being down is not the strategy's fault
        log.warning("live.order_failed", live_run_id=run.live_run_id, reason=str(exc))
        return False


def handle_bar(conn: Connection, api_url: str, run: LiveRun, bar: dict[str, Any]) -> bool:
    """One bar, end to end. False means the run should stop.

    `bar["catchup"]` (default False, so every pre-cursors caller and
    test is unaffected) marks a bar delivered late. The runtime still
    updates ctx.state and indicators on it -- only the ORDER is refused,
    by this process rather than by the untrusted container, because the
    enforcing check has to sit outside what it is enforcing against.
    """
    catchup = bool(bar.get("catchup", False))
    feed_bar(run, bar)
    timeout = get_settings().live_reply_timeout_seconds
    frames = read_frames(run, timeout=timeout)
    if not frames:
        # A process already dead is not worth failing over.
        with contextlib.suppress(Exception):
            run.process.kill()
        stop_run(conn, run, "CRASHED", f"no reply in {timeout}s")
        return False

    state: dict[str, Any] | None = None
    for frame in frames:
        if frame["type"] == FRAME_ERROR:
            stop_run(conn, run, "CRASHED", str(frame.get("error", ""))[:2000])
            return False
        if frame["type"] != FRAME_ORDERS:
            continue
        state = frame.get("state", state)
        for intent in frame.get("orders", []):
            # Refused before the rate limit sees it: a catch-up replay
            # dispatches many bars in seconds, and counting orders that
            # are never placed would stop the very run being recovered.
            if catchup:
                run.orders_refused += 1
                run.last_refusal = "catch-up bar: price no longer tradeable"
                log.info(
                    "live.catchup_refused",
                    live_run_id=run.live_run_id,
                    instrument_id=bar.get("instrument_id"),
                    bar_ts=bar.get("ts"),
                )
                continue
            run.note_order()
            if run.over_rate_limit():
                stop_run(
                    conn,
                    run,
                    "STOPPED",
                    f"order-rate limit: more than {MAX_ORDERS_PER_MINUTE} orders in a minute",
                )
                return False
            if place_order(api_url, run, intent, run.orders_placed):
                run.orders_placed += 1
        if not frame.get("alive", True):
            stop_run(conn, run, "STOPPED", frame.get("breaker_reason") or "the breaker latched")
            return False

    with conn.transaction():
        if "instrument_id" in bar and "ts" in bar:
            advance_cursor(
                conn, run.live_run_id, int(bar["instrument_id"]), datetime.fromisoformat(bar["ts"])
            )
        conn.execute(
            "UPDATE live_runs SET bars_seen=%s, orders_placed=%s, orders_refused=%s,"
            " last_refusal=%s, last_gap_note=COALESCE(%s, last_gap_note),"
            " strategy_state=COALESCE(%s, strategy_state) WHERE live_run_id=%s",
            (
                run.bars_seen,
                run.orders_placed,
                run.orders_refused,
                run.last_refusal,
                run.last_gap_note,
                None if state is None else Jsonb(state),
                run.live_run_id,
            ),
        )
    return True


def deliver_pending(
    conn: Connection,
    api_url: str,
    run: LiveRun,
    now: datetime,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> bool:
    """Everything design §4 means by "on any notification (or a timer),
    the supervisor sends everything after the cursor, oldest first" --
    the single mechanism covering a missed closed_bars:* message, an
    aggregator restart, and a supervisor restart. Returns False the
    moment any bar's handle_bar says the run should stop.

    `pending_bars` fixes each bar's `catchup` flag once, at query time
    (`now`). A long replay dispatches many bars in a row, each one a real
    subprocess round trip -- a bar that was fresh when queried can go
    stale by the time its own turn to be sent comes up, and the paper
    API's own guard only checks the latest bar, not this one (I3). So
    `catchup` is recomputed here, immediately before each `handle_bar`,
    against `clock()` -- real wall time by default, injectable for tests
    -- and only ever escalated: a bar the query already flagged stays
    flagged, it is never un-flagged by a stale-but-lucky recompute.
    """
    settings = get_settings()
    pending, gap_note = pending_bars(
        conn,
        run.live_run_id,
        run.instrument_ids,
        run.started_at,
        now=now,
        catchup_after=timedelta(seconds=settings.live_catchup_after_seconds),
        replay_cap=timedelta(hours=settings.live_replay_cap_hours),
    )
    if gap_note is not None:
        run.last_gap_note = gap_note
        log.warning("live.replay_gap", live_run_id=run.live_run_id, note=gap_note)
        if not pending:
            # Nothing will reach handle_bar's own write this cycle --
            # persist the note now rather than losing it until the next
            # bar that happens to arrive.
            conn.execute(
                "UPDATE live_runs SET last_gap_note=%s WHERE live_run_id=%s",
                (gap_note, run.live_run_id),
            )
    catchup_after = timedelta(seconds=settings.live_catchup_after_seconds)
    for item in pending:
        frame = item.frame
        bar_close = datetime.fromisoformat(frame["ts"]) + timedelta(
            seconds=frame["interval_sec"]
        )
        if (clock() - bar_close) > catchup_after:
            frame["catchup"] = True
        if not handle_bar(conn, api_url, run, frame):
            return False
    return True


_SELECT_RUNNING = """
    SELECT r.live_run_id, r.strategy_id, r.portfolio_id, s.source, s.manifest,
           p.cash_balance, r.started_at, r.bars_seen, r.orders_placed,
           r.orders_refused, r.last_refusal, r.strategy_state
    FROM live_runs r
    JOIN strategies s ON s.strategy_id = r.strategy_id
    JOIN portfolios p ON p.portfolio_id = r.portfolio_id
    WHERE r.status = 'RUNNING'
"""


def reconcile(conn: Connection, runs: dict[int, LiveRun]) -> None:
    """Start what the database says should be running, stop what it does not.

    The supervisor polls rather than being told. A control channel would be
    one more thing that can be missed while a container is being launched,
    and the table is the truth either way: a row is RUNNING or it is not.
    This also means a supervisor restarted after a crash converges on the
    intended state instead of needing to be re-driven.
    """
    from trading.agent_contract.smoke import resolve_universe
    from trading.paper.charges import load_schedules
    from trading.paper.enums import Product

    wanted: dict[int, tuple[Any, ...]] = {
        int(row[0]): row for row in conn.execute(_SELECT_RUNNING).fetchall()
    }

    for live_run_id, run in list(runs.items()):
        if live_run_id not in wanted:
            stop_run(conn, run, "STOPPED", "stopped by the operator")
            runs.pop(live_run_id, None)
        elif run.process.poll() is not None:
            stop_run(conn, run, "CRASHED", "the container exited")
            runs.pop(live_run_id, None)

    for live_run_id, row in wanted.items():
        if live_run_id in runs:
            continue
        (
            _,
            strategy_id,
            portfolio_id,
            source,
            manifest,
            cash,
            started_at,
            bars_seen,
            orders_placed,
            orders_refused,
            last_refusal,
            strategy_state,
        ) = row
        if manifest is None:
            stop_run_row(conn, live_run_id, "CRASHED", "this version stores no manifest")
            continue
        try:
            instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
            declared_leverage = _manifest_leverage(manifest)
            broker, exchange, asset_class = _charge_key_for(conn, instrument_ids)
            schedules = load_schedules(
                conn, broker, exchange, asset_class, Product.DELIVERY, datetime.now(UTC).date()
            )
        except Exception as exc:  # noqa: BLE001 - an unresolvable run is a stopped run
            stop_run_row(conn, live_run_id, "CRASHED", f"could not resolve the run: {exc}")
            continue
        # The row already exists, so adopt it rather than inserting another.
        runs[live_run_id] = _launch(
            conn,
            live_run_id,
            strategy_id,
            portfolio_id,
            source,
            instrument_ids,
            schedules,
            Decimal(str(cash)),
            started_at=started_at,
            leverage=declared_leverage,
            bars_seen=bars_seen,
            orders_placed=orders_placed,
            orders_refused=orders_refused,
            last_refusal=last_refusal,
            strategy_state=strategy_state,
        )


def _manifest_leverage(manifest: Any) -> Decimal | None:
    """What the manifest declared, as a Decimal.

    Read as a string first: the manifest is stored as JSON, and a leverage
    that arrived as a float would carry binary error into the margin the
    order reserves.
    """
    raw = (manifest or {}).get("leverage")
    return None if raw is None else Decimal(str(raw))


def stop_run_row(conn: Connection, live_run_id: int, status: str, reason: str) -> None:
    """End a run that never got a process."""
    conn.execute(
        "UPDATE live_runs SET status=%s, stopped_reason=%s, stopped_at=now() WHERE live_run_id=%s",
        (status, reason, live_run_id),
    )
    log.warning("live.not_started", live_run_id=live_run_id, reason=reason)


def _charge_key_for(conn: Connection, instrument_ids: list[int]) -> tuple[str, str, str]:
    from trading.agent_contract.smoke import _charge_key

    return _charge_key(conn, instrument_ids)


def _launch(
    conn: Connection,
    live_run_id: int,
    strategy_id: int,
    portfolio_id: int,
    source: str,
    instrument_ids: list[int],
    schedules: Any,
    starting_cash: Decimal,
    *,
    started_at: datetime,
    limits: SandboxLimits | None = None,
    leverage: Decimal | None = None,
    bars_seen: int = 0,
    orders_placed: int = 0,
    orders_refused: int = 0,
    last_refusal: str | None = None,
    strategy_state: dict[str, Any] | None = None,
) -> LiveRun:
    """Start a container for a run row that already exists. Restores the
    counters and ctx.state a fresh LiveRun would otherwise reset to
    zero/None -- without this, a relaunch's idempotency keys
    (`live-{id}-{seq}`) collide with ones already used before the
    restart, and the strategy loses everything it had learned."""
    resolved = _resolve_limits(limits)
    payload = encode_payload(
        SmokePayload(
            mode=MODE_LIVE,
            source=source,
            starting_cash=starting_cash,
            slippage_bps=Decimal(str(get_settings().paper_slippage_bps)),
            charge_schedules=tuple(schedules),
            leverage=leverage,
            strategy_state=strategy_state,
            state_max_bytes=get_settings().live_state_max_bytes,
        )
    )
    process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        _docker_args(resolved, f"live-{live_run_id}"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    process.stdin.write(b"%d\n" % len(payload) + payload)
    process.stdin.flush()
    conn.execute(
        "UPDATE live_runs SET runtime=%s, kernel_isolated=%s WHERE live_run_id=%s",
        (resolved.runtime or "runc", (resolved.runtime or "runc") == "runsc", live_run_id),
    )
    log.info("live.started", live_run_id=live_run_id, instruments=sorted(instrument_ids))
    return LiveRun(
        live_run_id=live_run_id,
        strategy_id=strategy_id,
        portfolio_id=portfolio_id,
        process=process,
        instrument_ids=set(instrument_ids),
        runtime=resolved.runtime or "runc",
        kernel_isolated=(resolved.runtime or "runc") == "runsc",
        started_at=started_at,
        bars_seen=bars_seen,
        orders_placed=orders_placed,
        orders_refused=orders_refused,
        last_refusal=last_refusal,
        leverage=leverage,
    )


def run_supervisor(stop: threading.Event | None = None) -> None:
    """Poll `closed_bars:*` for a wake-up, and on a timer regardless,
    then ask every run to deliver everything it hasn't seen yet (design
    §4). The message's own payload is never read past its type -- the
    cursor mechanism (Task 9) already knows what each run needs, so a
    wake-up for ANY instrument is reason enough to check every run.
    """
    settings = get_settings()
    api_url = settings.gateway_url
    db = ReconnectingConnection(settings.database_url, autocommit=True)
    pubsub = SyncResilientPubSub(
        lambda: redis.Redis.from_url(settings.redis_url, decode_responses=True),
        patterns=[_BAR_CHANNEL_PATTERN],
    )
    runs: dict[int, LiveRun] = {}
    log.info("live.supervisor_started", channel=_BAR_CHANNEL_PATTERN)
    last_reconcile = 0.0
    last_delivery = 0.0

    while stop is None or not stop.is_set():
        try:
            conn = db.get()
        except psycopg.OperationalError as exc:
            # The DB is still down after ReconnectingConnection's own
            # backoff sleep and reconnect attempt -- skip this pass
            # rather than let the whole supervisor process die on an
            # outage that will recover on its own.
            log.warning("live.db_unavailable", reason=str(exc))
            continue
        if time.monotonic() - last_reconcile > 5.0:
            try:
                reconcile(conn, runs)
            except Exception as exc:  # noqa: BLE001 - a bad row must not kill the loop
                log.warning("live.reconcile_failed", reason=str(exc))
            last_reconcile = time.monotonic()

        message = pubsub.get_message(timeout=1.0)
        woken = message is not None and message.get("type") == "pmessage"
        due = time.monotonic() - last_delivery > settings.live_delivery_timer_seconds
        if not woken and not due:
            continue
        last_delivery = time.monotonic()
        now = datetime.now(UTC)
        for live_run_id, run in list(runs.items()):
            try:
                if not deliver_pending(conn, api_url, run, now):
                    runs.pop(live_run_id, None)
            except Exception:  # noqa: BLE001 - one run's failure must not stop this pass for the rest
                log.warning("live.deliver_failed", live_run_id=live_run_id, exc_info=True)


def main() -> int:
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    log.info("live.starting_process")
    settings = get_settings()
    start_heartbeat_thread(
        lambda: redis.Redis.from_url(settings.redis_url, decode_responses=True),
        "live_supervisor",
        ttl=settings.heartbeat_ttl_seconds,
        every=settings.heartbeat_refresh_seconds,
    )
    run_supervisor()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
