"""The image must contain what the runner imports.

`tests/runtime/test_import_purity.py` already guards the other direction
-- that no `trading.runtime` module reaches for something the sandbox
lacks -- transitively and in a subprocess, which is the only way that
guarantee means anything (a direct-import AST check would only ever see
the first hop and would have missed the real psycopg leak it was meant to
catch). What is left to guard here is drift in `sandbox/build.sh`: the
Dockerfile copies the assembled tree wholesale (`COPY trading
/opt/trading`), so build.sh -- which selects WHICH modules get assembled
-- is the real drift surface. Asserting against the Dockerfile would
either fail on the parent copy or pass vacuously.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNTIME = REPO / "src" / "trading" / "runtime"
BUILD_SCRIPT = REPO / "sandbox" / "build.sh"


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def test_every_trading_module_the_runtime_needs_is_assembled_into_the_image() -> None:
    needed: set[str] = set()
    for path in sorted(RUNTIME.glob("*.py")):
        needed.update(m for m in _imported_modules(path) if m.startswith("trading."))
    build = BUILD_SCRIPT.read_text(encoding="utf-8")
    missing = []
    for module in sorted(needed):
        leaf = module.rsplit(".", 1)[-1]
        package_dir = "/".join(module.split(".")[:-1])
        # build.sh copies either a brace-expanded set of leaf names out of
        # a package, or that package's *.py wholesale.
        copied_wholesale = f"src/{package_dir}/*.py" in build
        copied_by_name = f"src/{package_dir}/" in build and leaf in build
        if not (copied_wholesale or copied_by_name):
            missing.append(module)
    assert missing == [], f"imported by trading.runtime but never assembled: {missing}"


def test_the_runtime_package_itself_is_assembled() -> None:
    # Guards the case where build.sh copies trading.paper correctly but
    # forgets the package this whole plan adds.
    assert "src/trading/runtime/*.py" in BUILD_SCRIPT.read_text(encoding="utf-8")
