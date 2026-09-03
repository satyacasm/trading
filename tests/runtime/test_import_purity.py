"""`trading.runtime` is copied into a container with no database and no
network, whose image installs numpy and pandas only. An import that
merely *works* on the host proves nothing about that."""

from __future__ import annotations

import subprocess
import sys

FORBIDDEN = ("psycopg", "httpx", "structlog", "trading.config")


def test_importing_the_runtime_pulls_in_nothing_the_sandbox_lacks() -> None:
    probe = (
        "import sys, json;"
        "import trading.runtime.loop, trading.runtime.context, trading.runtime.payload;"
        f"print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in {FORBIDDEN!r}"
        f" or m in {FORBIDDEN!r})))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    leaked = result.stdout.strip()
    assert leaked == "[]", f"trading.runtime transitively imports: {leaked}"
