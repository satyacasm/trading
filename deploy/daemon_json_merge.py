"""Merge logic for provision-sandbox-vm.sh: registers the runsc OCI
runtime in a Docker daemon.json without discarding anything else an
operator has set there. Kept in its own module, separate from the
shell script's ssh/backup/restart plumbing, so it is a plain function
the test suite can import and check directly -- no VM, no subprocess.

Usage from the shell script: the current daemon.json content (or '{}'
if the file does not exist yet) is piped in on stdin; the merged,
canonically-sorted JSON is written to stdout.
"""

from __future__ import annotations

import copy
import json
import sys
from typing import Any

RUNSC_RUNTIME = {"path": "/usr/local/bin/runsc"}


def merge_runsc(data: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of `data` with runtimes.runsc set to RUNSC_RUNTIME,
    preserving every other key -- including any other entries already
    under `runtimes`. Pure and idempotent: merging an already-merged
    dict returns an equal dict."""
    merged = copy.deepcopy(data)
    runtimes = dict(merged.get("runtimes", {}))
    runtimes["runsc"] = dict(RUNSC_RUNTIME)
    merged["runtimes"] = runtimes
    return merged


def main() -> None:
    current = json.load(sys.stdin)
    merged = merge_runsc(current)
    json.dump(merged, sys.stdout, sort_keys=True)


if __name__ == "__main__":
    main()
