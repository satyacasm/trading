"""The strategy upload endpoint -- §9's three stages behind one POST.

Mounted onto `gateway.py` rather than defined there, matching
`market_data_api.py` and `paper/api.py`. Kept out of `paper/api.py`
deliberately: that module is portfolios and orders, and an upload shares
no request shape, no table, and no failure mode with either.

**One request, three stages, in order.** Stage 1 (`validate_strategy`)
runs first even though `register_strategy` validates again internally:
an AST scan costs milliseconds and the smoke run costs three containers,
so discovering a forbidden import inside the sandbox would burn seconds
to learn what a grep already knew. Stage 2 only runs on a clean stage 1,
stage 3 only on a passing stage 2.

**Every route is a plain `def`, never `async def`.** psycopg is
synchronous and this route additionally blocks on `docker run` for
seconds at a time; FastAPI dispatches plain `def` to a threadpool but
runs `async def` on the event loop, where either would be fatal. Commit
`5d03a2e` fixed exactly that deadlock elsewhere in this codebase.
`test_no_route_is_a_coroutine_function` guards it here.

**The request is slow by construction and that is not hidden.** Three
containers run before it returns -- `configure` to resolve the manifest,
then the smoke payload twice so the two order sequences can be compared.
A five-session window is seconds. The caller holds a threadpool worker
and a database connection for the whole of it, which is acceptable for
one operator uploading strategies and would not be for a queue of them.

**A rejection stores nothing at all.** `strategy_smoke_runs.strategy_id`
is a NOT NULL FK to `strategies`, and a strategy that fails either stage
is never registered, so there is no row to hang its run on. Rejected
runs are therefore reported, never recorded -- a known limit of the
0011 schema rather than an oversight here.

**Rejections return 200.** The verdict *is* the response: §9 exists to
hand an agent a report it can act on, and a 4xx would split one outcome
across two channels, forcing every client to read both the status and
the body to learn the same fact. It also cannot coexist with persistence
-- `get_db_connection` rolls back on any exception, so raising to signal
"your code is bad" would discard the record of having judged it. This
departs from `POST /orders`, which uses 400 for domain rejections; the
difference is that an order rejection is one sentence, and this one is a
structured report an agent iterates against.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from psycopg import Connection
from pydantic import BaseModel, Field

from trading.agent_contract.registry import CONTRACT_VERSION, register_strategy
from trading.agent_contract.smoke import SmokeVerdict, record_smoke_run, smoke_test
from trading.agent_contract.validation import Finding, ValidationReport, validate_strategy
from trading.streaming.db import get_db_connection

router = APIRouter()

# migration 0007 seeds exactly one local user; there is no auth layer yet,
# so an upload that does not name one is attributed to it rather than
# inventing an owner or refusing.
_LOCAL_USER_EMAIL = "local@paper.trading"

_PACKAGE_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_ROOT.parents[2]
_CONTRACT_PATH = _REPO_ROOT / "docs" / "agent-contract" / "STRATEGY_CONTRACT.md"
_SDK_STUB_PATH = _PACKAGE_ROOT / "platform_sdk.py"


class UploadStrategyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    version: str = Field(min_length=1, max_length=40)
    source: str = Field(min_length=1)
    user_id: int | None = None


class FindingOut(BaseModel):
    code: str
    message: str
    line: int | None = None
    contract_section: str = ""


class UploadStrategyResponse(BaseModel):
    """What the agent that wrote the code reads back.

    `feedback` is the whole report as prose, meant to be pasted straight
    into the next prompt; `findings` is the same information structured
    so a client can branch on stable codes without parsing it.
    """

    accepted: bool
    verdict: str
    strategy_id: int | None
    feedback: str
    findings: list[FindingOut]
    window: dict[str, Any] | None
    runtime: str | None
    kernel_isolated: bool | None


def _findings_of(report: ValidationReport) -> list[FindingOut]:
    return [
        FindingOut(
            code=f.code,
            message=f.message,
            line=f.line,
            contract_section=f.contract_section,
        )
        for f in report.findings
    ]


def _static_rejection(report: ValidationReport) -> UploadStrategyResponse:
    """Stage 1 failed, so no container ever started.

    `window`, `runtime` and `kernel_isolated` are null rather than
    defaulted: reporting `runtime="runc"` for a run that did not happen
    would be a claim about isolation nobody made.
    """
    return UploadStrategyResponse(
        accepted=False,
        verdict="REJECTED",
        strategy_id=None,
        feedback=report.as_agent_feedback(),
        findings=_findings_of(report),
        window=None,
        runtime=None,
        kernel_isolated=None,
    )


def _from_verdict(verdict: SmokeVerdict, strategy_id: int | None) -> UploadStrategyResponse:
    label = (
        "REJECTED"
        if not verdict.passed
        else ("PASSED_WITH_WARNINGS" if verdict.warnings_only else "PASSED")
    )
    return UploadStrategyResponse(
        accepted=verdict.passed,
        verdict=label,
        strategy_id=strategy_id,
        feedback=verdict.as_agent_feedback(),
        findings=_findings_of(verdict.report),
        window=verdict.window,
        runtime=verdict.runtime,
        kernel_isolated=verdict.kernel_isolated,
    )


def _resolve_user_id(conn: Connection, requested: int | None) -> int:
    if requested is not None:
        return requested
    row = conn.execute(
        "SELECT user_id FROM users WHERE email = %s", (_LOCAL_USER_EMAIL,)
    ).fetchone()
    if row is None:  # pragma: no cover - migration 0007 seeds this user
        raise RuntimeError(f"no user_id given and no {_LOCAL_USER_EMAIL} to fall back on")
    return int(row[0])


@router.post("/strategies", response_model=UploadStrategyResponse)
def upload_strategy(
    request: UploadStrategyRequest,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> UploadStrategyResponse:
    report = validate_strategy(request.source)
    if not report.ok:
        return _static_rejection(report)

    verdict = smoke_test(conn, request.source)
    if not verdict.passed:
        return _from_verdict(verdict, strategy_id=None)

    registered = register_strategy(
        conn,
        user_id=_resolve_user_id(conn, request.user_id),
        name=request.name,
        version=request.version,
        source=request.source,
    )
    record_smoke_run(conn, registered.strategy_id, verdict)
    return _from_verdict(verdict, strategy_id=registered.strategy_id)


class ContractBundle(BaseModel):
    """Everything an agent needs before it writes a line.

    The contract is the prompt; the stub is what a capable agent can
    import to check its own work offline. Both travel together because
    handing over one without the other is the common way a round trip
    gets wasted.
    """

    contract: str
    sdk_stub: str
    contract_version: str


@router.get("/strategies/contract", response_model=ContractBundle)
def get_contract() -> ContractBundle:
    """The contract and SDK stub, read from disk on every request.

    Deliberately uncached. A prompt kit that hands out a stale contract is
    worse than one that hands out none: the agent writes against rules
    nothing enforces any more, the upload is rejected, and the rejection
    reads as the agent's fault rather than the platform's. Two file reads
    are cheap next to the three containers the sibling route spawns.
    """
    return ContractBundle(
        contract=_CONTRACT_PATH.read_text(encoding="utf-8"),
        sdk_stub=_SDK_STUB_PATH.read_text(encoding="utf-8"),
        contract_version=CONTRACT_VERSION,
    )


__all__ = [
    "ContractBundle",
    "Finding",
    "UploadStrategyRequest",
    "UploadStrategyResponse",
    "router",
]
