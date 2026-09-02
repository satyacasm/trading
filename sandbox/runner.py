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
    return fields or None


def main() -> int:
    source = sys.stdin.read()
    namespace: dict[str, Any] = {"__name__": "strategy"}
    try:
        # compile() names the unit so tracebacks read "strategy.py, line N"
        # and line numbers match what the author submitted.
        exec(compile(source, SOURCE_NAME, "exec"), namespace)  # noqa: S102
    except BaseException:  # noqa: BLE001 - every failure is a reportable outcome
        _emit(
            {
                "ok": False,
                "stage": "import",
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    candidates = [
        obj
        for name, obj in namespace.items()
        if isinstance(obj, type)
        and name != "Strategy"
        and any(base.__name__ == "Strategy" for base in obj.__mro__[1:])
    ]
    if not candidates:
        _emit(
            {
                "ok": False,
                "stage": "discover",
                "error": "no class inheriting Strategy was defined at module level",
            }
        )
        return 0

    strategy_cls = candidates[0]
    try:
        instance = strategy_cls()
        manifest = instance.configure()
    except BaseException:  # noqa: BLE001 - same reasoning as above
        _emit(
            {
                "ok": False,
                "stage": "configure",
                "strategy_class": strategy_cls.__name__,
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    _emit(
        {
            "ok": True,
            "stage": "configure",
            "strategy_class": strategy_cls.__name__,
            "manifest": _describe_manifest(manifest),
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
