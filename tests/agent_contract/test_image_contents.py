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
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNTIME = REPO / "src" / "trading" / "runtime"
BUILD_SCRIPT = REPO / "sandbox" / "build.sh"


def _imported_modules(path: Path, *, module_level_only: bool = False) -> set[str]:
    """Modules this file imports.

    `module_level_only` follows just the imports that run when the module
    is imported. A `from trading.config import ...` inside a function body
    never executes on the sandbox path -- `paper.charges` has several --
    and treating one as a dependency would demand the image carry a module
    it never touches, which is a false alarm that trains people to widen
    the copy list until it stops meaning anything.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = tree.body if module_level_only else list(ast.walk(tree))
    found: set[str] = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


_BRACE_COPY = re.compile(r"^cp\s+src/(?P<package>[\w/]+)/\{(?P<names>[^}]+)\}\.py", re.M)
_GLOB_COPY = re.compile(r"^cp\s+src/(?P<package>[\w/]+)/\*\.py", re.M)
# build.sh creates empty package markers rather than copying them: an
# `__init__.py` with imports in it would drag the whole host package into
# a container that wants three modules from it.
_TOUCHED = re.compile(r"touch\s+(?P<paths>.+)$", re.M)


def _is_assembled(module: str, build: str) -> bool:
    """Whether build.sh actually copies this module.

    Parsed, not substring-matched. `leaf in build` was the original check
    and it passes on any mention anywhere in the file -- including the
    comment explaining why the module matters, which is exactly the line
    somebody writes while forgetting to add it to the list. It reported
    `perp` as assembled when the copy list did not contain it.
    """
    as_path = module.replace(".", "/")
    for match in _TOUCHED.finditer(build):
        if f"sandbox/{as_path}/__init__.py" in match.group("paths"):
            return True
    package_dir = "/".join(module.split(".")[:-1])
    leaf = module.rsplit(".", 1)[-1]
    for match in _GLOB_COPY.finditer(build):
        if match.group("package") == package_dir:
            return True
    for match in _BRACE_COPY.finditer(build):
        if match.group("package") == package_dir:
            names = {n.strip() for n in match.group("names").split(",")}
            if leaf in names:
                return True
    return False


def _needed_transitively() -> set[str]:
    """Every `trading.*` module the runtime reaches, at any depth.

    Direct imports are not enough, and this test learned that the
    expensive way. `breaker` began importing `paper.perp` when equity
    gained a perpetual term -- a second hop, invisible to a scan of
    `runtime/*.py`. `perp` imports nothing forbidden, so the purity test
    stayed green too, and every live container died at import for three
    tasks before one was actually run.
    """
    seen: set[str] = set()
    queue = [f"trading.runtime.{path.stem}" for path in sorted(RUNTIME.glob("*.py"))]
    while queue:
        module = queue.pop()
        if module in seen:
            continue
        seen.add(module)
        path = REPO / "src" / Path(*module.split("."))
        source = path.with_suffix(".py")
        if not source.exists():
            source = path / "__init__.py"
        if not source.exists():
            continue
        for found in _imported_modules(source, module_level_only=True):
            if found.startswith("trading.") and found not in seen:
                queue.append(found)
    return {m for m in seen if not m.startswith("trading.runtime.")}


def test_every_trading_module_the_runtime_needs_is_assembled_into_the_image() -> None:
    needed = _needed_transitively()
    build = BUILD_SCRIPT.read_text(encoding="utf-8")
    missing = []
    for module in sorted(needed):
        if not _is_assembled(module, build):
            missing.append(module)
    assert missing == [], f"imported by trading.runtime but never assembled: {missing}"


def test_the_runtime_package_itself_is_assembled() -> None:
    # Guards the case where build.sh copies trading.paper correctly but
    # forgets the package this whole plan adds.
    assert "src/trading/runtime/*.py" in BUILD_SCRIPT.read_text(encoding="utf-8")
