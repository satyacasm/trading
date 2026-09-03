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


def _emit(result: dict[str, Any]) -> None:
    # A single line on stdout, and nothing else ever written there, so the
    # host can parse the last line without heuristics even if the strategy
    # printed during import.
    sys.stdout.write("\n__SANDBOX_RESULT__" + json.dumps(result) + "\n")
    sys.stdout.flush()


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
    from trading.runtime.payload import MODE_SMOKE, decode_payload

    _install_sdk_alias()

    try:
        payload = decode_payload(sys.stdin.buffer.read())
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
            max_daily_loss=getattr(manifest, "max_daily_loss", None),
            max_drawdown_pct=getattr(manifest, "max_drawdown_pct", None),
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


if __name__ == "__main__":
    sys.exit(main())
