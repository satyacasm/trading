"""Static validation -- stage 1 of the upload pipeline (contract §9).

**This is not the security boundary.** Say it plainly, because a module
that greps for `eval` and `socket` invites being mistaken for one. An AST
scan is bypassable by anyone actually trying: `getattr(__builtins__, "e"
+ "val")`, a name assembled at run time, a payload decoded from base64.
Containment is the sandbox's job -- gVisor, no network namespace,
read-only filesystem, hard resource limits -- and none of it depends on
this file.

What this stage *is*: a fast, local first filter that catches the mistakes
an AI agent actually makes when writing against `STRATEGY_CONTRACT.md`,
and hands back a report that agent can act on without a round trip
through the sandbox. Every finding names a line, a code, and the section
of the contract it comes from, because §9's feedback loop is the point of
the whole exercise: a rejection is only useful if pasting it back to the
agent produces a fix.

So the checks here are tuned for *helpfulness on honest code*, not for
adversarial resistance. `open()` is flagged because a generated strategy
that reads a CSV is a real and common mistake, not because flagging it
stops a hostile one.

`validate_source` never executes, compiles, or imports the code it is
given -- only `ast.parse`, which builds a tree without running anything.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Any

from jsonschema import Draft202012Validator

from trading.agent_contract.schemas import load_schemas

__all__ = [
    "ALLOWED_IMPORTS",
    "Finding",
    "ValidationReport",
    "validate_manifest",
    "validate_source",
    "validate_strategy",
]

# Third-party analysis libraries the contract promises, plus the standard
# library a strategy legitimately needs for arithmetic and bookkeeping.
# Deliberately absent: anything that reaches the filesystem, the network,
# another process, or the import system (`os`, `sys`, `pathlib`, `socket`,
# `subprocess`, `importlib`, `ctypes`, `pickle`, `shutil`).
#
# `time` is absent for a different reason -- it is a clock, and the only
# clock a strategy may read is `ctx.now` (§2, determinism).
ALLOWED_IMPORTS: frozenset[str] = frozenset(
    {
        # analysis
        "numpy",
        "pandas",
        "talib",
        "scipy",
        "statistics",
        "math",
        "decimal",
        "fractions",
        # bookkeeping
        "collections",
        "itertools",
        "functools",
        "operator",
        "dataclasses",
        "enum",
        "typing",
        "abc",
        "re",
        "json",
        "heapq",
        "bisect",
        "copy",
        "uuid",
        # dates: the types are fine, reading the clock is not (see _WALL_CLOCK)
        "datetime",
        "calendar",
        "zoneinfo",
        # the contract's own SDK
        "platform_sdk",
        "trading",
    }
)

# Builtins with no legitimate use inside a strategy. `open` earns its place
# by being a common honest mistake (an agent reading a CSV of signals);
# the rest are escape-shaped.
_FORBIDDEN_CALLS: frozenset[str] = frozenset(
    {
        "open",
        "eval",
        "exec",
        "compile",
        "__import__",
        "input",
        "breakpoint",
        "globals",
        "locals",
        "vars",
        "memoryview",
    }
)

# Attribute names that only appear when walking the object graph toward
# something a strategy should not reach. Ordinary dunder *methods*
# (`__init__`, `__len__`, `__repr__`) are normal Python and are not here.
_FORBIDDEN_ATTRIBUTES: frozenset[str] = frozenset(
    {
        "__class__",
        "__bases__",
        "__mro__",
        "__subclasses__",
        "__globals__",
        "__builtins__",
        "__code__",
        "__closure__",
        "__dict__",
        "__getattribute__",
        "__reduce__",
    }
)

# (module, attribute) pairs that read real time. A strategy that reads the
# wall clock cannot be replayed, so a backtest of it proves nothing --
# this is a determinism rule, not a security one.
_WALL_CLOCK: frozenset[tuple[str, str]] = frozenset(
    {
        ("datetime", "now"),
        ("datetime", "today"),
        ("datetime", "utcnow"),
        ("date", "today"),
        ("time", "time"),
        ("time", "time_ns"),
        ("time", "monotonic"),
        ("time", "perf_counter"),
    }
)


@dataclass(frozen=True)
class Finding:
    """One reason the upload was rejected, phrased for the agent that wrote
    the code: a stable `code` to branch on, a `message` naming the offending
    symbol, the `line` to look at, and the contract section that explains
    the rule."""

    code: str
    message: str
    line: int | None = None
    contract_section: str = ""


@dataclass(frozen=True)
class ValidationReport:
    findings: tuple[Finding, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_agent_feedback(self) -> str:
        """§9's feedback loop: the text to paste straight back into the
        agent that generated the strategy.

        Every finding is listed, not just the first. One defect per round
        trip would make the agent iterate needlessly when it could fix
        everything at once.
        """
        if self.ok:
            return (
                "ACCEPTED: static validation passed.\n"
                "  No forbidden imports, calls, or wall-clock reads were found, and the\n"
                "  structure matches the contract.\n"
                "  This is a static check for honest mistakes, not a proof of safety -- the\n"
                "  sandbox is what actually contains strategy code at run time."
            )

        lines = ["REJECTED: static validation found "]
        lines[0] += f"{len(self.findings)} problem{'s' if len(self.findings) != 1 else ''}.\n"
        for finding in self.findings:
            where = f"line {finding.line}" if finding.line is not None else "file"
            lines.append(f"  [{finding.code}] {where}: {finding.message}")
            if finding.contract_section:
                lines.append(f"      See STRATEGY_CONTRACT.md {finding.contract_section}.")
        lines.append("\nFix these and resubmit. All findings are listed above, not only the first.")
        return "\n".join(lines)


def validate_manifest(manifest: dict[str, Any]) -> list[Finding]:
    """Check a manifest against the published schema.

    Errors are sorted by path so the report is stable between runs -- an
    agent diffing two rejections should see what changed, not a reshuffle.
    """
    schemas = load_schemas()
    validator = Draft202012Validator(
        {"$ref": "#/$defs/StrategyManifest", "$defs": schemas["$defs"]}
    )
    findings: list[Finding] = []
    for error in sorted(validator.iter_errors(manifest), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(part) for part in error.absolute_path) or "(root)"
        findings.append(
            Finding(
                code="MANIFEST_INVALID",
                message=f"{path}: {error.message}",
                contract_section="§3",
            )
        )
    return findings


@dataclass
class _Scanner(ast.NodeVisitor):
    """Walks the tree once, collecting findings.

    A single pass rather than several: an agent gets one complete list, and
    the traversal order keeps findings roughly in source order without a
    sort.
    """

    findings: list[Finding] = field(default_factory=list)
    has_strategy_class: bool = False
    strategy_has_configure: bool = False

    def _add(self, code: str, message: str, node: ast.AST, section: str) -> None:
        self.findings.append(
            Finding(
                code=code,
                message=message,
                line=getattr(node, "lineno", None),
                contract_section=section,
            )
        )

    def _check_module(self, name: str, node: ast.AST) -> None:
        # Allowlist by top-level package: `numpy.linalg` is allowed because
        # `numpy` is. An agent reaching for a submodule of a permitted
        # library is doing nothing wrong.
        root = name.split(".")[0]
        if root not in ALLOWED_IMPORTS:
            self._add(
                "IMPORT_NOT_ALLOWED",
                f"imports {name!r}, which is not on the allowlist. A strategy reaches "
                "data only through `ctx`; there is no network and no filesystem.",
                node,
                "§8",
            )

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._check_module(alias.name, node)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        # `from . import x` has no module name; relative imports cannot
        # resolve in a single-file upload anyway.
        if node.module is None:
            self._add(
                "IMPORT_NOT_ALLOWED",
                "uses a relative import; a strategy is a single module with no package around it.",
                node,
                "§8",
            )
        else:
            self._check_module(node.module, node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name) and func.id in _FORBIDDEN_CALLS:
            self._add(
                "FORBIDDEN_CALL",
                f"calls {func.id}(), which is not available to strategies.",
                node,
                "§8",
            )
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            pair = (func.value.id, func.attr)
            if pair in _WALL_CLOCK:
                self._add(
                    "WALL_CLOCK",
                    f"calls {func.value.id}.{func.attr}(), which reads the real clock. "
                    "Use ctx.now -- a strategy that reads wall-clock time cannot be "
                    "replayed, so its backtest would prove nothing.",
                    node,
                    "§2",
                )
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _FORBIDDEN_ATTRIBUTES:
            self._add(
                "FORBIDDEN_ATTRIBUTE",
                f"accesses {node.attr!r}, which walks the object graph outside the "
                "strategy. Strategies use only what `ctx` exposes.",
                node,
                "§8",
            )
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # "A strategy" is any class inheriting something named Strategy, or
        # named Strategy itself. Deliberately loose: the runtime resolves
        # the real class, and this stage only checks the shape is present.
        inherits_strategy = any(
            (isinstance(base, ast.Name) and base.id == "Strategy")
            or (isinstance(base, ast.Attribute) and base.attr == "Strategy")
            for base in node.bases
        )
        if inherits_strategy or node.name == "Strategy":
            self.has_strategy_class = True
            if any(
                isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)
                and item.name == "configure"
                for item in node.body
            ):
                self.strategy_has_configure = True
        self.generic_visit(node)


def validate_source(source: str) -> list[Finding]:
    """Every static finding in one strategy module.

    Parses only. Nothing here executes, compiles, or imports the source.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [
            Finding(
                code="SYNTAX_ERROR",
                message=f"does not parse as Python: {exc.msg}",
                line=exc.lineno,
                contract_section="§2",
            )
        ]

    scanner = _Scanner()
    scanner.visit(tree)

    if not scanner.has_strategy_class:
        scanner.findings.append(
            Finding(
                code="NO_STRATEGY_CLASS",
                message="defines no class inheriting `Strategy`. A strategy is a single "
                "class named or deriving from Strategy.",
                contract_section="§2",
            )
        )
    elif not scanner.strategy_has_configure:
        scanner.findings.append(
            Finding(
                code="MISSING_CONFIGURE",
                message="the Strategy class does not implement `configure()`, which "
                "declares the universe, data needs, and capital.",
                contract_section="§3",
            )
        )

    return scanner.findings


def validate_strategy(source: str, manifest: dict[str, Any] | None = None) -> ValidationReport:
    """Stage 1 end to end: source plus, when supplied, the manifest.

    The manifest is optional because it is normally produced by *running*
    `configure()`, which needs the sandbox. When a caller already has it,
    checking it here means one rejection covering both halves rather than
    two round trips.
    """
    findings = list(validate_source(source))
    if manifest is not None:
        findings.extend(validate_manifest(manifest))
    return ValidationReport(findings=tuple(findings))
