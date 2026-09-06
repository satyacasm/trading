"""The in-container entrypoint: import a strategy, call `configure()`,
report what happened as one JSON object on stdout.

This runs *inside* the sandbox, as the unprivileged `strategy` user, with
no network and a read-only filesystem. It is baked into the image rather
than mounted so a caller cannot replace it with something that skips the
reporting contract below.

Its single job is to be **boring and total**: every outcome -- success, an
import that fails, a `configure()` that raises, a strategy class that is
missing -- comes back as the same JSON shape, so the host never has to
parse a traceback out of stderr to find out what happened. Anything this
script lets escape becomes an opaque non-zero exit on the host side, which
is exactly the failure mode the structured result exists to avoid.

Note on trust: the host does not rely on this file for containment. If a
strategy subverts it, the container's limits still hold. This is a
reporter, not a guard.
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any

# The source arrives on stdin, not as a mounted file. That removes the
# bind mount entirely: no host path is exposed to the container, nothing
# depends on which directories the Docker daemon happens to share (macOS
# only shares a configured set, so a temp dir under /var/folders is
# invisible to it), and the same call works unchanged on a Linux CI host.
# It also means the strategy source never touches a filesystem the
# strategy can reach.
SOURCE_NAME = "strategy.py"


def _read_payload_bytes() -> bytes:
    """Exactly the payload, leaving anything after it on the stream.

    Length-prefixed: a decimal byte count on its own line, then that many
    bytes. `read()` to EOF would be simpler and is what this did while every
    run was a batch -- but a live run sends bar frames down the same pipe
    after the payload, and reading to EOF would swallow them and then block
    forever waiting for an EOF that had already happened.

    A header rather than relying on gzip being self-delimiting: a buffered
    reader may pull bytes past the end of the gzip member into its own
    buffer, which would eat the first frame in a way that depends on
    buffer sizes rather than on anything in the protocol.
    """
    header = sys.stdin.buffer.readline().strip()
    length = int(header)
    remaining = length
    chunks: list[bytes] = []
    while remaining > 0:
        chunk = sys.stdin.buffer.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _emit(result: dict[str, Any]) -> None:
    # A single line on stdout, and nothing else ever written there, so the
    # host can parse the last line without heuristics even if the strategy
    # printed during import.
    #
    # Written through the raw buffer in an explicit loop rather than with
    # `sys.stdout.write`, because a short write here loses the tail of the
    # result silently.
    #
    # The image sets PYTHONUNBUFFERED=1, which makes `sys.stdout` write
    # through to a raw `FileIO`. A write() to a pipe returns once it has
    # accepted at most the pipe's capacity -- 64 KiB -- and a raw writer
    # does not loop on that short return the way a BufferedWriter would.
    # Under runc the write completes anyway; under gVisor it does not, and
    # the remainder is dropped with no error and exit status 0. The host
    # then sees truncated JSON and reports "the sandbox produced no
    # structured result": a run that finished perfectly, reported as a
    # crash. Measured: 65,537 of 200,001 bytes under runsc, all 200,001
    # under runc.
    #
    # It bit the first time a result exceeded 64 KiB, which was an equity
    # curve of 1,667 daily points -- so it is latent for every large `logs`
    # payload too, and worse under the isolated runtime than the plain one.
    data = ("\n__SANDBOX_RESULT__" + json.dumps(result) + "\n").encode("utf-8")
    stream = sys.stdout.buffer
    view = memoryview(data)
    while view:
        written = stream.write(view)
        if not written:
            # A stream accepting nothing would spin forever; stop and let
            # the host report a truncated result rather than hang.
            break
        view = view[written:]
    stream.flush()


def _describe_manifest(manifest: Any) -> dict[str, Any] | None:
    """Best-effort JSON view of whatever `configure()` returned.

    Deliberately tolerant: `configure()` returns a `StrategyManifest`
    object, and this stage is a smoke test, not a validator -- the manifest
    is checked properly against `schema.json` back on the host, where the
    schema lives. Returning None rather than raising keeps a manifest we
    cannot serialise from turning a successful run into a failed one.
    """
    if manifest is None:
        return None
    fields: dict[str, Any] = {}
    for name in (
        "name",
        "version",
        "base_currency",
        "capital",
        "max_daily_loss",
        "max_drawdown_pct",
    ):
        value = getattr(manifest, name, None)
        if value is not None:
            fields[name] = str(value)
    data = getattr(manifest, "data", None)
    if data is not None:
        fields["data"] = {
            "bars": getattr(data, "bars", None),
            "ticks": bool(getattr(data, "ticks", False)),
            "history_bars": getattr(data, "history_bars", None),
        }

    universe = getattr(manifest, "universe", None)
    if universe is not None:
        if isinstance(universe, list):
            # Explicit instruments, named one by one.
            fields["universe"] = [
                {
                    "exchange": getattr(ref, "exchange", None),
                    "segment": getattr(ref, "segment", None),
                    "symbol": getattr(ref, "symbol", None),
                }
                for ref in universe
            ]
        else:
            # A Query, resolved point-in-time on the host against
            # listed_on/delisted_on -- which is why only its criteria
            # cross back, never a resolved list the container guessed at.
            fields["universe"] = {
                "asset_class": getattr(universe, "asset_class", None),
                "exchange": getattr(universe, "exchange", None),
                "index": getattr(universe, "index", None),
            }

    return fields or None


def _install_sdk_alias() -> None:
    """Make one module answer to both names.

    A strategy writes `from platform_sdk import Strategy`; the runtime
    writes `from trading.agent_contract import platform_sdk`. Two import
    paths to one file produce two distinct module objects in Python, with
    two distinct `Strategy` and `Context` classes -- and the subclass
    relationship the whole SDK decision rests on would silently stop being
    one. Aliasing before any strategy source is executed means there is
    exactly one module, under two names.
    """
    from trading.agent_contract import platform_sdk

    sys.modules.setdefault("platform_sdk", platform_sdk)


def _load_strategy_class(source: str) -> tuple[type | None, dict[str, Any] | None]:
    namespace: dict[str, Any] = {"__name__": "strategy"}
    try:
        exec(compile(source, SOURCE_NAME, "exec"), namespace)  # noqa: S102
    except BaseException:  # noqa: BLE001 - every failure is a reportable outcome
        return None, {"ok": False, "stage": "import", "error": traceback.format_exc(limit=20)}
    candidates = [
        obj
        for name, obj in namespace.items()
        if isinstance(obj, type)
        and name != "Strategy"
        and any(base.__name__ == "Strategy" for base in obj.__mro__[1:])
    ]
    if not candidates:
        return None, {
            "ok": False,
            "stage": "discover",
            "error": "no class inheriting Strategy was defined at module level",
        }
    return candidates[0], None


def main() -> int:
    from trading.runtime.payload import MODE_LIVE, MODE_SMOKE, decode_payload

    _install_sdk_alias()

    try:
        payload = decode_payload(_read_payload_bytes())
    except Exception:  # noqa: BLE001
        _emit({"ok": False, "stage": "payload", "error": traceback.format_exc(limit=20)})
        return 0

    strategy_cls, failure = _load_strategy_class(payload.source)
    if failure is not None:
        _emit(failure)
        return 0
    assert strategy_cls is not None

    try:
        instance = strategy_cls()
        manifest = instance.configure()
    except BaseException:  # noqa: BLE001
        _emit(
            {
                "ok": False,
                "stage": "configure",
                "strategy_class": strategy_cls.__name__,
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    if payload.mode == MODE_LIVE:
        return _run_live(payload, instance, manifest, strategy_cls)

    if payload.mode != MODE_SMOKE:
        _emit(
            {
                "ok": True,
                "stage": "configure",
                "strategy_class": strategy_cls.__name__,
                "manifest": _describe_manifest(manifest),
            }
        )
        return 0

    from dataclasses import asdict

    from trading.runtime.loop import run_loop
    from trading.runtime.provider import InMemoryBars

    try:
        outcome = run_loop(
            strategy=instance,
            bars=InMemoryBars(payload.bars),
            schedules=payload.charge_schedules,
            starting_cash=payload.starting_cash,
            slippage_bps=payload.slippage_bps,
            # The caller's limits when given, else whatever the strategy
            # declared. An operator asking "what would this have done with a
            # wider stop?" is asking a different question from the one the
            # manifest answers, and editing the strategy to ask it would
            # create a version whose results are attributed separately.
            max_daily_loss=(
                payload.max_daily_loss
                if payload.max_daily_loss is not None
                else getattr(manifest, "max_daily_loss", None)
            ),
            max_drawdown_pct=(
                payload.max_drawdown_pct
                if payload.max_drawdown_pct is not None
                else getattr(manifest, "max_drawdown_pct", None)
            ),
            # None for a smoke run, which dispatches every bar it is given.
            dispatch_from=payload.dispatch_from,
            perp_instruments=payload.perp_instruments,
        )
    except BaseException:  # noqa: BLE001 - the loop itself failing is still an outcome
        _emit(
            {
                "ok": False,
                "stage": "smoke",
                "strategy_class": strategy_cls.__name__,
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    _emit(
        {
            "ok": outcome.ok,
            "stage": "smoke",
            "strategy_class": strategy_cls.__name__,
            "manifest": _describe_manifest(manifest),
            "outcome": asdict(outcome),
            "error": outcome.error,
        }
    )
    return 0


def _run_live(payload, instance, manifest, strategy_cls):  # noqa: ANN001, ANN202
    """Drive the strategy a bar at a time, from frames on stdin.

    The same `step` the backtester drives, so forward and historical
    behaviour are identical by sharing rather than by discipline. The
    process stays alive between bars, which is what keeps the strategy's
    own state -- `self.` attributes as much as `ctx.state` -- intact across
    dispatches. Re-invoking a fresh container per bar would silently reset
    anything not written to `ctx.state`, and most strategies use both.

    Frames in: `bar` carries one closed bar; `stop` ends the run. Frames
    out: `ready` once initialised, `orders` after each bar with whatever the
    strategy submitted, `error` if a handler raised.

    Orders are reported as intents, not placed. This process has no network
    and no database by design; the supervisor turns an intent into a real
    paper order, which is also where §166 puts the rate limit.
    """
    import sys
    from decimal import Decimal

    from trading.live.protocol import (
        FRAME_BAR,
        FRAME_ERROR,
        FRAME_ORDERS,
        FRAME_READY,
        FRAME_STOP,
        decode_frame,
        encode_frame,
    )
    from trading.runtime.loop import open_session
    from trading.runtime.provider import BarRecord, InMemoryBars

    bars = InMemoryBars({})
    session = open_session(
        instance,
        bars,
        payload.charge_schedules,
        payload.starting_cash,
        payload.slippage_bps,
        max_daily_loss=(
            payload.max_daily_loss
            if payload.max_daily_loss is not None
            else getattr(manifest, "max_daily_loss", None)
        ),
        max_drawdown_pct=(
            payload.max_drawdown_pct
            if payload.max_drawdown_pct is not None
            else getattr(manifest, "max_drawdown_pct", None)
        ),
        perp_instruments=payload.perp_instruments,
    )

    def _write(line: str) -> None:
        # The same explicit write loop `_emit` uses, for the same reason: a
        # raw unbuffered stream can accept a short write and drop the rest.
        data = line.encode("utf-8")
        stream = sys.stdout.buffer
        view = memoryview(data)
        while view:
            written = stream.write(view)
            if not written:
                break
            view = view[written:]
        stream.flush()

    initialised = False
    submitted_before = 0
    alive = True

    for raw in sys.stdin:
        frame = decode_frame(raw)
        if frame is None:
            continue
        if frame["type"] == FRAME_STOP:
            break
        if frame["type"] != FRAME_BAR:
            continue

        try:
            record = BarRecord(
                instrument_id=int(frame["instrument_id"]),
                ts=_parse_ts(frame["ts"]),
                interval_sec=int(frame["interval_sec"]),
                open=Decimal(frame["open"]),
                high=Decimal(frame["high"]),
                low=Decimal(frame["low"]),
                close=Decimal(frame["close"]),
                volume=None if frame.get("volume") is None else Decimal(frame["volume"]),
                knowable_at=None
                if frame.get("knowable_at") is None
                else _parse_ts(frame["knowable_at"]),
            )
            appended = bars.append(record)
            if appended is None:
                # A redelivery of a bar the strategy has already acted on.
                # Dispatching it again would place the same orders twice.
                # The supervisor reads exactly one orders frame per bar it
                # feeds, so the frame is still written -- staying silent
                # here would stall it until its read timed out.
                _write(
                    encode_frame(
                        FRAME_ORDERS,
                        ts=record.close_ts.isoformat(),
                        orders=[],
                        breaker_reason=session.state.breaker_reason,
                        alive=alive,
                        duplicate=True,
                    )
                )
                continue
            bar, index = appended
            if not initialised:
                session.initialize(bar.close_ts)
                initialised = True
                _write(encode_frame(FRAME_READY, strategy_class=strategy_cls.__name__))
            alive = session.step(bar.close_ts, ((bar, index),))
        except BaseException:  # noqa: BLE001 - any failure is still an outcome
            _write(encode_frame(FRAME_ERROR, error=traceback.format_exc(limit=20)))
            break

        # Only what this bar produced. `state.submissions` is cumulative, so
        # replaying it whole would re-place every earlier order.
        new_orders = session.state.submissions[submitted_before:]
        submitted_before = len(session.state.submissions)
        _write(
            encode_frame(
                FRAME_ORDERS,
                ts=bar.close_ts.isoformat(),
                orders=[_order_intent(session.state.orders[oid]) for oid in new_orders],
                breaker_reason=session.state.breaker_reason,
                alive=alive,
            )
        )
        if not alive:
            break

    _emit({"ok": True, "stage": "live", "outcome": _as_dict(session.outcome())})
    return 0


def _parse_ts(raw):  # noqa: ANN001, ANN202
    from datetime import datetime

    return datetime.fromisoformat(raw)


def _order_intent(order):  # noqa: ANN001, ANN202
    """What the supervisor needs to place a real order. Money as strings."""
    return {
        "instrument_id": order.instrument_id,
        "side": order.side.value,
        "order_type": order.order_type.value,
        "quantity": str(order.quantity),
        "limit_price": None if order.limit_price is None else str(order.limit_price),
        "product": order.product.value,
        "rationale": order.rationale,
    }


def _as_dict(outcome):  # noqa: ANN001, ANN202
    from dataclasses import asdict

    return asdict(outcome)


if __name__ == "__main__":
    sys.exit(main())
