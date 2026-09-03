"""Static validation: the first stage of the upload pipeline (contract §9).

What this stage is for, and what it is emphatically not for: it catches
honest mistakes in generated code -- a wrong import, a wall-clock read, a
missing `configure` -- and returns a report an agent can act on. It is
**not** the security boundary. An AST scan is bypassable by anyone trying
(`getattr` chains, `eval` of a built string, encoded names), and the
sandbox is what actually contains strategy code. Tests here therefore
assert *helpfulness on plausible generated code*, not resistance to a
determined attacker, because claiming the latter would be a false
assurance.
"""

from __future__ import annotations

import textwrap

from trading.agent_contract.validation import (
    ValidationReport,
    validate_manifest,
    validate_source,
    validate_strategy,
)


def src(text: str) -> str:
    return textwrap.dedent(text).strip() + "\n"


CONFORMING = src(
    """
    from decimal import Decimal

    from platform_sdk import Context, DataRequest, InstrumentRef, Strategy, StrategyManifest


    class MyStrategy(Strategy):
        def configure(self) -> StrategyManifest:
            return StrategyManifest(
                name="demo",
                version="1.0.0",
                universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")],
                data=DataRequest(bars="1m"),
                capital=Decimal("1000000"),
                base_currency="INR",
            )

        def initialize(self, ctx: Context) -> None:
            self.held = False

        def on_bar(self, ctx, bars) -> None:
            ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="demo")
    """
)


def codes(findings) -> set[str]:
    return {f.code for f in findings}


_SHELL = src(
    """
    class MyStrategy(Strategy):
        def configure(self):
            return None
    """
)


def with_shell(snippet: str) -> str:
    """A snippet plus the minimal class every real strategy file has.

    Appended *after* the snippet, not before, so the snippet keeps its own
    line numbers and a finding's reported line stays checkable. Without
    this, every scanner test would also trip NO_STRATEGY_CLASS and assert
    against input no agent would ever submit.
    """
    return snippet + "\n\n" + _SHELL


# --- the happy path ----------------------------------------------------------


def test_a_conforming_strategy_produces_no_findings() -> None:
    assert validate_source(CONFORMING) == []


def test_a_conforming_strategy_passes_end_to_end() -> None:
    report = validate_strategy(CONFORMING)
    assert report.ok
    assert report.findings == ()


# --- syntax ------------------------------------------------------------------


def test_unparseable_source_is_reported_with_its_line() -> None:
    """A syntax error must come back as a finding, not an exception: the
    caller is a pipeline stage, and an agent needs the line number."""
    findings = validate_source("def broken(:\n    pass\n")
    assert codes(findings) == {"SYNTAX_ERROR"}
    assert findings[0].line == 1


# --- imports -----------------------------------------------------------------


def test_an_import_outside_the_allowlist_is_rejected() -> None:
    findings = validate_source(with_shell(src("import requests")))
    assert codes(findings) == {"IMPORT_NOT_ALLOWED"}
    assert "requests" in findings[0].message


def test_a_from_import_outside_the_allowlist_is_rejected() -> None:
    findings = validate_source(with_shell(src("from socket import socket")))
    assert "IMPORT_NOT_ALLOWED" in codes(findings)
    assert "socket" in findings[0].message


def test_a_submodule_of_an_allowed_package_is_allowed() -> None:
    """`import pandas.testing` must not be rejected because the allowlist
    names `pandas` -- an agent writing `numpy.linalg` is doing nothing
    wrong."""
    assert validate_source(with_shell(src("import numpy.linalg"))) == []


def test_the_allowed_scientific_stack_passes() -> None:
    assert validate_source(with_shell(src("import numpy as np\nimport pandas as pd"))) == []


def test_the_finding_names_the_line_the_import_is_on() -> None:
    findings = validate_source(with_shell(src("import numpy\nimport os")))
    assert len(findings) == 1
    assert findings[0].line == 2


# --- forbidden builtins ------------------------------------------------------


def test_open_is_rejected() -> None:
    findings = validate_source(with_shell(src("data = open('/etc/passwd').read()")))
    assert "FORBIDDEN_CALL" in codes(findings)


# NOTE on the `eval`/`exec`/`open` strings below: they are inert test data --
# source text handed to a *parser* so we can assert it is rejected. Nothing in
# this module executes, compiles, or imports any of it; `validate_source` only
# ever calls `ast.parse`, which builds a tree without running anything.
def test_eval_and_exec_are_rejected() -> None:
    findings = validate_source(with_shell(src("eval('1+1')\nexec('x=1')")))
    assert codes(findings) == {"FORBIDDEN_CALL"}
    assert len(findings) == 2


def test_dunder_import_is_rejected() -> None:
    findings = validate_source(with_shell(src("__import__('os')")))
    assert "FORBIDDEN_CALL" in codes(findings)


# --- escape-shaped attribute access ------------------------------------------


def test_dunder_attribute_access_is_reported() -> None:
    """`().__class__.__bases__` is the canonical sandbox-escape opening.
    Reported because it has no legitimate place in a strategy -- not
    because reporting it constitutes containment."""
    findings = validate_source(with_shell(src("x = ().__class__.__bases__")))
    assert "FORBIDDEN_ATTRIBUTE" in codes(findings)


def test_ordinary_dunder_methods_are_not_flagged() -> None:
    """Defining __init__ or calling super().__init__() is normal Python and
    must not be swept up by the escape check."""
    assert (
        validate_source(
            with_shell(
                src(
                    """
                    class A:
                        def __init__(self) -> None:
                            super().__init__()
                    """
                )
            )
        )
        == []
    )


# --- determinism -------------------------------------------------------------


def test_reading_the_wall_clock_is_rejected() -> None:
    """Determinism rule, not a security rule: a strategy that reads
    datetime.now() cannot be replayed, so a backtest of it means nothing."""
    findings = validate_source(
        with_shell(
            src(
                """
                from datetime import datetime

                now = datetime.now()
                """
            )
        )
    )
    assert "WALL_CLOCK" in codes(findings)


def test_time_time_is_rejected() -> None:
    findings = validate_source(with_shell(src("import time\nt = time.time()")))
    assert "WALL_CLOCK" in codes(findings)


def test_ctx_now_is_not_flagged() -> None:
    """The sanctioned way to read time must obviously survive the check."""
    assert validate_source(with_shell(src("def f(ctx):\n    return ctx.now"))) == []


# --- structure ---------------------------------------------------------------


def test_source_without_a_strategy_class_is_rejected() -> None:
    findings = validate_source(src("x = 1"))
    assert "NO_STRATEGY_CLASS" in codes(findings)


def test_a_strategy_class_without_configure_is_rejected() -> None:
    findings = validate_source(
        src(
            """
            class MyStrategy(Strategy):
                def on_bar(self, ctx, bars) -> None:
                    pass
            """
        )
    )
    assert "MISSING_CONFIGURE" in codes(findings)


# --- manifest ----------------------------------------------------------------


def test_a_conforming_manifest_produces_no_findings() -> None:
    manifest = {
        "name": "demo",
        "version": "1.0.0",
        "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}],
        "data": {"bars": "1m"},
        "capital": "1000000",
        "base_currency": "INR",
    }
    assert validate_manifest(manifest) == []


def test_a_manifest_with_a_bad_interval_is_rejected_naming_the_field() -> None:
    manifest = {
        "name": "demo",
        "version": "1.0.0",
        "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}],
        "data": {"bars": "30s"},
        "capital": "1000000",
        "base_currency": "INR",
    }
    findings = validate_manifest(manifest)
    assert codes(findings) == {"MANIFEST_INVALID"}
    assert "data.bars" in findings[0].message


def test_a_manifest_with_float_capital_is_rejected() -> None:
    """Money crosses the wire as a string. A JSON number would already have
    been through binary floating point by the time we saw it."""
    manifest = {
        "name": "demo",
        "version": "1.0.0",
        "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}],
        "data": {"bars": "1m"},
        "capital": 1000000.5,
        "base_currency": "INR",
    }
    assert codes(validate_manifest(manifest)) == {"MANIFEST_INVALID"}


# --- the agent-facing report -------------------------------------------------


def test_report_is_ok_only_when_there_are_no_findings() -> None:
    assert ValidationReport(findings=()).ok
    assert not validate_strategy(with_shell(src("import os"))).ok


def test_feedback_names_the_file_line_code_and_contract_section() -> None:
    """§9's whole point: the rejection is written to be pasted straight back
    into the agent that produced the code, so it must carry everything
    needed to fix it without the platform in front of you."""
    report = validate_strategy(with_shell(src("import numpy\nimport requests")))
    feedback = report.as_agent_feedback()

    assert "REJECTED" in feedback
    assert "line 2" in feedback
    assert "requests" in feedback
    assert "IMPORT_NOT_ALLOWED" in feedback
    assert "STRATEGY_CONTRACT.md" in feedback
    assert "§8" in feedback


def test_feedback_lists_every_finding_not_only_the_first() -> None:
    """One round trip per defect would make the agent loop needlessly. All
    of them, every time."""
    report = validate_strategy(with_shell(src("import os\nimport socket\neval('1')")))
    feedback = report.as_agent_feedback()
    assert "os" in feedback
    assert "socket" in feedback
    assert "eval" in feedback


def test_feedback_on_a_clean_strategy_says_so() -> None:
    assert "ACCEPTED" in validate_strategy(CONFORMING).as_agent_feedback()


def test_feedback_states_that_static_validation_is_not_the_sandbox() -> None:
    """Anyone reading a passing report -- human or agent -- should not come
    away believing the code has been proven safe. It has been checked for
    honest mistakes; containment is the sandbox's job."""
    feedback = validate_strategy(CONFORMING).as_agent_feedback()
    assert "sandbox" in feedback.lower()


# --- the class the runner will actually look for -------------------------------


def test_a_class_merely_named_Strategy_is_rejected_as_the_runner_would() -> None:
    """Found by dogfooding: an agent given only the contract wrote
    `class Strategy:` -- exactly what §2 told it to -- and this stage waved
    it through, because the rule was `inherits_strategy or name ==
    "Strategy"`. `sandbox/runner.py` then refused it: it selects on
    `name != "Strategy" and any(base.__name__ == "Strategy" ...)`, so a
    class named Strategy is doubly disqualified.

    A stage-1 rule looser than the runtime's is worse than no rule. The
    entire justification for this stage is catching in milliseconds what
    would otherwise cost three containers, and it was passing through the
    single most likely misreading of the contract.
    """
    findings = validate_source(
        src(
            """
            class Strategy:
                def configure(self):
                    return None

                def on_bar(self, ctx, bars) -> None:
                    pass
            """
        )
    )

    assert "NO_STRATEGY_CLASS" in codes(findings)
    message = next(f.message for f in findings if f.code == "NO_STRATEGY_CLASS")
    # Naming the actual mistake, not just the absence: "no strategy class"
    # reads as nonsense to someone looking at the class they just wrote.
    assert "subclass" in message.lower()


def test_subclassing_under_your_own_name_is_what_passes() -> None:
    # The vacuity guard for the test above: rejecting everything would
    # satisfy it.
    assert (
        validate_source(
            src(
                """
            class MyStrategy(Strategy):
                def configure(self):
                    return None
            """
            )
        )
        == []
    )


def test_shadowing_the_base_class_name_is_also_rejected() -> None:
    # `class Strategy(Strategy)` inherits correctly and still fails the
    # runner's `name != "Strategy"` guard.
    assert "NO_STRATEGY_CLASS" in codes(
        validate_source(
            src(
                """
                class Strategy(Strategy):
                    def configure(self):
                        return None
                """
            )
        )
    )
