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

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import Connection
from psycopg.errors import UniqueViolation
from pydantic import BaseModel, Field

from trading.agent_contract.persistence import (
    get_backtest_run,
    list_backtest_runs,
    record_backtest_run,
)
from trading.agent_contract.registry import CONTRACT_VERSION, get_strategy, register_strategy
from trading.agent_contract.smoke import (
    BacktestVerdict,
    SmokeVerdict,
    backtest,
    charge_lookup_date,
    earliest_schedule_date,
    record_smoke_run,
    smoke_test,
)
from trading.agent_contract.validation import Finding, ValidationReport, validate_strategy
from trading.metrics.curve import summarize
from trading.metrics.robustness import reshuffle
from trading.metrics.trades import cost_drag, round_trips, trade_metrics
from trading.paper.enums import Product
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


class RunSummary(BaseModel):
    """What the run was worth, for a caller that wants numbers not prose.

    Every money field is a string. JSON has no decimal type, so a number
    here would reach every client as a float -- the one representation
    this codebase refuses to let money take (contract §5). The frontend
    parses these for display only, never for arithmetic.
    """

    bar_calls: int
    orders: int
    fills: int
    starting_cash: str | None
    final_cash: str | None
    final_equity: str | None
    pnl: str | None
    pnl_pct: str | None
    currency: str
    # "orders: 12 · fills: 0" with no reason reads as a strategy that chose
    # not to trade, when in fact every order bounced. rejections/
    # rejection_reasons/breaker_reason close that gap the same way the
    # prose report already does, so a client that branches on these
    # numbers cannot draw the wrong conclusion.
    rejections: int
    rejection_reasons: list[str]
    breaker_reason: str | None


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
    summary: RunSummary | None


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
        summary=None,
    )


def _summary_of(verdict: SmokeVerdict) -> RunSummary | None:
    outcome = verdict.outcome
    if outcome is None:
        return None
    # Mirrors record_smoke_run exactly (count = len(reasons), the reasons
    # list stored verbatim) so the API response and the stored row can
    # never disagree about how many orders bounced or why.
    rejections = list(outcome.get("rejections") or [])
    return RunSummary(
        bar_calls=int(outcome.get("bar_calls", 0)),
        orders=len(outcome.get("orders", [])),
        fills=int(outcome.get("fills", 0)),
        starting_cash=None if verdict.starting_cash is None else str(verdict.starting_cash),
        final_cash=_as_str(outcome.get("final_cash")),
        final_equity=_as_str(outcome.get("final_equity")),
        pnl=None if verdict.pnl is None else str(verdict.pnl),
        pnl_pct=None if verdict.pnl_pct is None else str(verdict.pnl_pct),
        currency=verdict.currency,
        rejections=len(rejections),
        rejection_reasons=rejections,
        breaker_reason=outcome.get("breaker_reason"),
    )


def _as_str(value: Any) -> str | None:
    return None if value is None else str(value)


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
        summary=_summary_of(verdict),
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
        manifest=verdict.manifest,
    )
    record_smoke_run(conn, registered.strategy_id, verdict)
    return _from_verdict(verdict, strategy_id=registered.strategy_id)


class BacktestRequest(BaseModel):
    """The window is the caller's, not the manifest's.

    Defaulting to all available history is deliberately not offered: over
    585,266 instruments that is a wildly different run from anything a
    caller likely meant, and a default that expensive should be typed out.
    """

    start: date
    end: date
    # An operator asking "what would this have done with 50,000?" is asking
    # a different question from the one the manifest answers. Overriding it
    # here rather than editing the strategy keeps that from creating a new
    # version whose results are attributed separately.
    starting_cash: Decimal | None = None
    # Risk limits for this run only. `None` uses the strategy's own -- a
    # run halted early is only explicable next to the limit it hit, so
    # whichever applied is recorded on the result.
    max_daily_loss: Decimal | None = None
    max_drawdown_pct: Decimal | None = None


class BacktestResponse(BaseModel):
    """One backtest, or the reason it was refused.

    `equity_curve` is a list of `{ts, equity, cash}` with money as strings,
    for the reason `RunSummary` gives: JSON has no decimal type, and a curve
    of subtly wrong equity is worse than no curve.
    """

    strategy_id: int
    status: str
    # None when the run was refused before it reached the container: a
    # refusal is not a run and is deliberately not stored.
    backtest_run_id: int | None = None
    bars: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    dispatch_from: str | None = None
    sessions: int | None = None
    history_bars_requested: int | None = None
    history_bars_available: int | None = None
    bar_calls: int | None = None
    fills: int | None = None
    final_cash: str | None = None
    final_equity: str | None = None
    breaker_reason: str | None = None
    equity_curve: list[dict[str, str]]
    # Every fill with its charges itemised. The list route carries neither
    # this nor the curve, for the same reason.
    fills_ledger: list[dict[str, Any]] = []
    findings: list[FindingOut] = []
    # Approximations worth knowing about -- a charge schedule that does
    # not reach back to the window's start, say. Not refusals: the run
    # happened, and the reader should know what was assumed.
    notes: list[str] = []
    runtime: str | None = None
    kernel_isolated: bool | None = None


def _backtest_response(
    strategy_id: int, verdict: BacktestVerdict, run_id: int | None = None
) -> BacktestResponse:
    outcome = verdict.outcome or {}
    plan = verdict.plan
    return BacktestResponse(
        strategy_id=strategy_id,
        status="PASSED" if verdict.passed else "REFUSED",
        backtest_run_id=run_id,
        bars=verdict.bars,
        window_start=plan.start.isoformat() if plan else None,
        window_end=plan.end.isoformat() if plan else None,
        dispatch_from=plan.dispatch_from.isoformat() if plan else None,
        sessions=plan.sessions if plan else None,
        history_bars_requested=plan.history_bars_requested if plan else None,
        history_bars_available=plan.history_bars_available if plan else None,
        bar_calls=outcome.get("bar_calls"),
        fills=outcome.get("fills"),
        final_cash=_as_str(outcome.get("final_cash")),
        final_equity=_as_str(outcome.get("final_equity")),
        breaker_reason=outcome.get("breaker_reason"),
        equity_curve=list(outcome.get("equity_curve") or []),
        findings=_findings_of(verdict.report),
        notes=list(getattr(verdict, "notes", ())),
        runtime=verdict.runtime,
        kernel_isolated=verdict.kernel_isolated,
    )


@router.post("/strategies/{strategy_id}/backtests", response_model=BacktestResponse)
def run_backtest(
    strategy_id: int,
    request: BacktestRequest,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> BacktestResponse:
    """Run a registered version over an operator-chosen window.

    Blocks for the whole run, matching `POST /strategies`. Measured compute
    is ~2,600 dispatches for a daily decade against a loop that does
    ~481,000 bars/sec, so the honest expectation is seconds; this module
    already documents why that is right for one operator and wrong for a
    queue, and a job queue is not built for a wait that does not exist.

    Plain `def`: psycopg is synchronous, and this shells out to Docker.
    """
    try:
        verdict = backtest(
            conn,
            strategy_id,
            start=request.start,
            end=request.end,
            starting_cash=request.starting_cash,
            max_daily_loss=request.max_daily_loss,
            max_drawdown_pct=request.max_drawdown_pct,
        )
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"no strategy with strategy_id={strategy_id}"
        ) from None

    run_id: int | None = None
    if verdict.plan is not None and verdict.outcome is not None:
        # It reached the container, so it is a run: PASSED or FAILED, both
        # stored, a crash with its partial curve. A pre-flight refusal has
        # no outcome and is deliberately not recorded -- it is a
        # deterministic function of the request, costs a COUNT to
        # re-derive, and a row would imply to a later reader that a run
        # happened.
        run_id = record_backtest_run(
            conn,
            strategy_id,
            verdict,
            requested_start=request.start,
            requested_end=request.end,
            instrument_ids=verdict.instrument_ids,
        )
    return _backtest_response(strategy_id, verdict, run_id)


class BacktestSummary(BaseModel):
    """One stored run, WITHOUT its curve.

    The omission is the whole reason there are two read routes: a history
    view that embedded curves would transfer every point of every run to
    render a table of dates and final equities.
    """

    backtest_run_id: int
    strategy_id: int
    status: str
    requested_start: str
    requested_end: str
    fetch_start: str
    dispatch_from: str
    sessions: int
    instruments: list[int]
    history_bars_requested: int
    history_bars_available: int
    bars: str | None
    bar_calls: int
    orders_placed: int
    fills: int
    final_cash: str | None
    final_equity: str | None
    breaker_reason: str | None
    error: str | None
    findings: list[dict[str, Any]]
    runtime: str
    kernel_isolated: bool
    contract_version: str
    ran_at: str
    # The limits this run was constrained by, when the caller chose them.
    # None means the strategy's own applied.
    max_daily_loss: str | None = None
    max_drawdown_pct: str | None = None


class BacktestDetail(BacktestSummary):
    """One stored run WITH its curve and fills. Money as strings."""

    equity_curve: list[dict[str, str]]
    # Every fill with its charges itemised. The list route carries neither
    # this nor the curve, for the same reason: a history table would drag
    # every fill of every run behind it.
    fills_ledger: list[dict[str, Any]] = []
    # What the run paid (positive) or was paid (negative) in funding, per
    # instrument. Separate from the cost report on purpose: funding is a
    # signed transfer, not a charge, and folding it into cost drag would
    # make an income stream read as an expense.
    funding: list[dict[str, Any]] = []
    # Positions the exchange closed, each naming the mark and the
    # maintenance requirement its equity fell below. Empty for every
    # non-perpetual run, which is most of them.
    liquidations: list[dict[str, Any]] = []
    # Computed on read from the curve, never stored: a stored metric is a
    # second source of truth that can drift from the series it came from,
    # and recomputing means a corrected metric applies retroactively to
    # every run rather than only to runs computed after the fix.
    metrics: dict[str, Any] | None = None
    # Approximations worth knowing about. Recomputed on read rather than
    # stored: the charge-schedule note is a pure function of the window's
    # end and the earliest schedule date, so storing it would be a second
    # source of truth that could drift from the schedules themselves.
    notes: list[str] = []
    # The 2x cost-and-slippage rerun, as executed and stored. An
    # observation: doubling slippage changes which fills happen, so it
    # cannot be re-derived from this run's output.
    stress: dict[str, Any] | None = None
    # The trade-order reshuffle, computed on read from `fills_ledger` --
    # arithmetic over stored data, so improving it applies retroactively.
    reshuffle: dict[str, Any] | None = None


@router.get("/strategies/{strategy_id}/backtests", response_model=list[BacktestSummary])
def list_backtests(
    strategy_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> list[BacktestSummary]:
    """A strategy's run history, newest first, without curves.

    Plain `def`, and a GET that never writes. Not scoped by `user_id`,
    matching `GET /strategies` -- harmless with one seeded user and no auth,
    and the same single line to change when auth lands.
    """
    return [BacktestSummary(**row) for row in list_backtest_runs(conn, strategy_id)]


# 6.5% rather than the conventional 0. On an Indian platform a zero
# risk-free rate is a systematically flattering lie: a strategy returning 6%
# a year reads as respectable and is in truth worse than a government bond.
# The value used is echoed inside `metrics`, so a reader who never
# considered the question is told what was assumed.
_DEFAULT_RISK_FREE = Decimal("0.065")
_RISK_FREE_DESCRIPTION = (
    "Annual risk-free rate for Sharpe and Sortino. Defaults to 6.5%, reflecting "
    "Indian G-Sec reality rather than the conventional 0. Echoed inside `metrics`."
)


@router.get("/backtests/{backtest_run_id}", response_model=BacktestDetail)
def get_backtest(
    backtest_run_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
    risk_free: Annotated[Decimal, Query(description=_RISK_FREE_DESCRIPTION)] = _DEFAULT_RISK_FREE,
) -> BacktestDetail:
    """One stored run, with its equity curve and metrics computed from it.

    Plain `def`, and a GET that never writes.
    """
    try:
        run = get_backtest_run(conn, backtest_run_id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"no backtest run with backtest_run_id={backtest_run_id}"
        ) from None

    points = [
        (datetime.fromisoformat(p["ts"]), Decimal(p["equity"]), Decimal(p["cash"]))
        for p in run["equity_curve"]
    ]
    metrics = summarize(points, run["bars"], risk_free) if points else None
    if metrics is not None:
        ledger = run.get("fills_ledger") or []
        # Trade metrics and cost drag read the fill ledger, not the curve:
        # a curve cannot say what a strategy paid, which is exactly why the
        # ledger exists.
        metrics["trades"] = trade_metrics(ledger)
        metrics["cost_drag"] = cost_drag(ledger)
        # Computed here rather than stored: the reshuffle is arithmetic over
        # the stored fills, so improving it applies retroactively to every
        # run ever recorded instead of only to new ones.
        run["reshuffle"] = reshuffle(
            [trip.net_pnl for trip in round_trips(ledger)],
            starting_equity=points[0][1],
        )
    run["metrics"] = metrics

    # Same derivation the run itself used, so the page and the POST response
    # cannot disagree about what was assumed.
    from datetime import date as _date

    requested_end = _date.fromisoformat(run["requested_end"])
    earliest = earliest_schedule_date(conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY)
    _, note = charge_lookup_date(requested_end, earliest)
    run["notes"] = [] if note is None else [note]

    return BacktestDetail(**run)


class LiveRunRequest(BaseModel):
    """Which portfolio the strategy trades. One strategy, one portfolio."""

    portfolio_id: int


class LiveRunOut(BaseModel):
    live_run_id: int
    strategy_id: int
    portfolio_id: int
    status: str
    stopped_reason: str | None
    runtime: str | None
    kernel_isolated: bool | None
    bars_seen: int
    orders_placed: int
    orders_refused: int
    last_refusal: str | None
    started_at: str
    stopped_at: str | None


# Named, because the joined detail query appends columns after these and
# hard-coded offsets into that row are exactly what breaks when a column is
# added in the middle. Ask the list where a column is; never count by eye.
_LIVE_RUN_COLUMN_LIST = (
    "live_run_id",
    "strategy_id",
    "portfolio_id",
    "status",
    "stopped_reason",
    "runtime",
    "kernel_isolated",
    "bars_seen",
    "orders_placed",
    "orders_refused",
    "last_refusal",
    "started_at",
    "stopped_at",
)
_LIVE_RUN_COLUMNS = ", ".join(_LIVE_RUN_COLUMN_LIST)
_LIVE_RUN_COLUMN_COUNT = len(_LIVE_RUN_COLUMN_LIST)


def _live_run_out(row: tuple[Any, ...]) -> LiveRunOut:
    return LiveRunOut(
        live_run_id=row[0],
        strategy_id=row[1],
        portfolio_id=row[2],
        status=row[3],
        stopped_reason=row[4],
        runtime=row[5],
        kernel_isolated=row[6],
        bars_seen=row[7],
        orders_placed=row[8],
        orders_refused=row[9],
        last_refusal=row[10],
        started_at=row[11].isoformat(),
        stopped_at=None if row[12] is None else row[12].isoformat(),
    )


@router.post("/strategies/{strategy_id}/live", response_model=LiveRunOut)
def start_live_run(
    strategy_id: int,
    request: LiveRunRequest,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> LiveRunOut:
    """Ask the supervisor to run this strategy forward.

    Writes the intent and returns; the supervisor reconciles against this
    table and launches the container. Deliberately not a synchronous start:
    the gateway does not own the supervisor's process table, and a route
    that waited for a container would fail differently depending on which
    machine it ran on.

    A partial unique index enforces one live run per portfolio, so a second
    start against a busy portfolio is refused by the database rather than
    by a check that could race.
    """
    try:
        get_strategy(conn, strategy_id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"no strategy with strategy_id={strategy_id}"
        ) from None
    try:
        row = conn.execute(
            f"INSERT INTO live_runs (strategy_id, portfolio_id, status)"
            f" VALUES (%s,%s,'RUNNING') RETURNING {_LIVE_RUN_COLUMNS}",
            (strategy_id, request.portfolio_id),
        ).fetchone()
    except UniqueViolation:
        raise HTTPException(
            status_code=409,
            detail=(
                f"portfolio {request.portfolio_id} already has a live run; "
                "one strategy per portfolio at a time"
            ),
        ) from None
    assert row is not None
    return _live_run_out(row)


@router.post("/live/{live_run_id}/stop", response_model=LiveRunOut)
def stop_live_run(
    live_run_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> LiveRunOut:
    """Ask the supervisor to stop a run.

    Marks it stopped; the supervisor sees the row is no longer RUNNING and
    shuts the container down. Stopping an already-stopped run is not an
    error -- it is the state the caller asked for.
    """
    row = conn.execute(
        f"UPDATE live_runs SET status='STOPPED',"
        f" stopped_reason=coalesce(stopped_reason,'stopped by the operator'),"
        f" stopped_at=coalesce(stopped_at, now())"
        f" WHERE live_run_id=%s RETURNING {_LIVE_RUN_COLUMNS}",
        (live_run_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no live run with live_run_id={live_run_id}")
    return _live_run_out(row)


class LivePosition(BaseModel):
    instrument_id: int
    symbol: str
    quantity: str
    avg_cost: str
    last_price: str | None
    market_value: str | None
    unrealised_pnl: str | None


class LiveFill(BaseModel):
    order_id: int
    symbol: str
    side: str
    quantity: str
    price: str
    total_charges: str
    rationale: str
    filled_at: str


class LiveRunDetail(LiveRunOut):
    """One run, with everything needed to watch it.

    Equity comes from `portfolio_equity_snapshots`, which the breaker
    already writes on every tick -- the same number that would pause the
    run, so the chart and the limit cannot disagree. That is the same
    reasoning D3b-2 used for the backtest curve, and it applies with more
    force here: a monitoring page that drew a different equity from the one
    being enforced would be actively misleading.
    """

    strategy_name: str
    strategy_version: str
    portfolio_name: str
    base_currency: str
    cash_balance: str
    equity: str | None
    peak_equity: str | None
    drawdown_pct: str | None
    positions: list[LivePosition] = []
    recent_fills: list[LiveFill] = []
    equity_curve: list[dict[str, str]] = []


# The run's own columns, qualified: `strategy_id` and `portfolio_id` are
# ambiguous once the joins are in, and an unqualified list fails only at
# runtime.
_LIVE_RUN_COLUMNS_QUALIFIED = ", ".join(f"r.{column}" for column in _LIVE_RUN_COLUMN_LIST)

_LIVE_DETAIL_SQL = f"""
    SELECT {_LIVE_RUN_COLUMNS_QUALIFIED}, s.name, s.version, p.name, p.base_currency,
           p.cash_balance
    FROM live_runs r
    JOIN strategies s ON s.strategy_id = r.strategy_id
    JOIN portfolios p ON p.portfolio_id = r.portfolio_id
    WHERE r.live_run_id = %s
"""

_LIVE_POSITIONS_SQL = """
    SELECT pos.instrument_id, i.symbol, pos.quantity, pos.avg_cost,
           (SELECT b.close FROM bars_intraday b WHERE b.instrument_id = pos.instrument_id
            ORDER BY b.ts DESC LIMIT 1) AS last_price
    FROM positions pos JOIN instruments i ON i.instrument_id = pos.instrument_id
    WHERE pos.portfolio_id = %s AND pos.quantity <> 0
    ORDER BY i.symbol
"""

_LIVE_FILLS_SQL = """
    SELECT o.order_id, i.symbol, o.side, f.quantity, f.price, f.total_charges,
           o.rationale, f.filled_at
    FROM fills f
    JOIN orders o ON o.order_id = f.order_id
    JOIN instruments i ON i.instrument_id = o.instrument_id
    WHERE o.live_run_id = %s
    ORDER BY f.filled_at DESC LIMIT 50
"""

# Sampled, not every row: the breaker writes a snapshot per tick, so a
# day's run is tens of thousands of points and a chart cannot use them all.
# Every Nth row by ordinal keeps the shape without pretending to a
# resolution the eye could read.
_LIVE_EQUITY_SQL = """
    SELECT ts, equity FROM (
        SELECT ts, equity, row_number() OVER (ORDER BY ts) AS rn,
               count(*) OVER () AS total
        FROM portfolio_equity_snapshots
        WHERE portfolio_id = %s AND ts >= %s
    ) sampled
    WHERE rn %% greatest(1, total / 400) = 0
    ORDER BY ts
"""


@router.get("/live/{live_run_id}", response_model=LiveRunDetail)
def get_live_run(
    live_run_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> LiveRunDetail:
    """One live run and the portfolio it is trading. A GET that never writes."""
    row = conn.execute(_LIVE_DETAIL_SQL, (live_run_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no live run with live_run_id={live_run_id}")

    base = _live_run_out(row[:_LIVE_RUN_COLUMN_COUNT])
    joined = row[_LIVE_RUN_COLUMN_COUNT:]
    # The curve is the run's, not the portfolio's: a portfolio that has been
    # traded by hand for a week would otherwise open with a week of history
    # this run had nothing to do with.
    started_at = row[_LIVE_RUN_COLUMN_LIST.index("started_at")]
    positions = []
    for instrument_id, symbol, quantity, avg_cost, last_price in conn.execute(
        _LIVE_POSITIONS_SQL, (row[2],)
    ).fetchall():
        value = None if last_price is None else last_price * quantity
        positions.append(
            LivePosition(
                instrument_id=instrument_id,
                symbol=symbol,
                quantity=str(quantity),
                avg_cost=str(avg_cost),
                last_price=None if last_price is None else str(last_price),
                market_value=None if value is None else str(value),
                unrealised_pnl=None if value is None else str(value - avg_cost * quantity),
            )
        )

    fills = [
        LiveFill(
            order_id=o,
            symbol=sym,
            side=side,
            quantity=str(qty),
            price=str(price),
            total_charges=str(charges),
            rationale=rationale,
            filled_at=filled_at.isoformat(),
        )
        for o, sym, side, qty, price, charges, rationale, filled_at in conn.execute(
            _LIVE_FILLS_SQL, (live_run_id,)
        ).fetchall()
    ]

    curve = [
        {"ts": ts.isoformat(), "equity": str(equity)}
        for ts, equity in conn.execute(_LIVE_EQUITY_SQL, (row[2], started_at)).fetchall()
    ]
    latest = conn.execute(
        "SELECT equity, peak_equity, drawdown_pct FROM portfolio_equity_snapshots"
        " WHERE portfolio_id = %s ORDER BY ts DESC LIMIT 1",
        (row[2],),
    ).fetchone()

    return LiveRunDetail(
        **base.model_dump(),
        strategy_name=joined[0],
        strategy_version=joined[1],
        portfolio_name=joined[2],
        base_currency=joined[3],
        cash_balance=str(joined[4]),
        equity=None if latest is None else str(latest[0]),
        peak_equity=None if latest is None else str(latest[1]),
        drawdown_pct=None if latest is None else str(latest[2]),
        positions=positions,
        recent_fills=fills,
        equity_curve=curve,
    )


@router.get("/live", response_model=list[LiveRunOut])
def list_live_runs(
    conn: Annotated[Connection, Depends(get_db_connection)],
    limit: int = Query(default=50, ge=1, le=200),
) -> list[LiveRunOut]:
    """Every live run, newest first. A GET that never writes."""
    rows = conn.execute(
        f"SELECT {_LIVE_RUN_COLUMNS} FROM live_runs"
        f" ORDER BY started_at DESC, live_run_id DESC LIMIT %s",
        (limit,),
    ).fetchall()
    return [_live_run_out(row) for row in rows]


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


class LatestRun(BaseModel):
    """The most recent smoke run for one strategy version, as stored by
    `record_smoke_run` -- not re-derived from a live verdict, so this is
    exactly what a future reader of the row would see."""

    smoke_run_id: int
    verdict: str
    window_start: str | None
    window_end: str | None
    sessions: int
    bar_calls: int
    orders_placed: int
    fills: int
    rejections: int
    final_equity: str | None
    breaker_reason: str | None
    runtime: str
    kernel_isolated: bool
    contract_version: str
    ran_at: str


class StrategySummary(BaseModel):
    """One row of `GET /strategies`: a registered version plus the smoke
    run that justified it, if one has been recorded."""

    strategy_id: int
    name: str
    version: str
    status: str
    contract_version: str
    registered_at: str
    bars: str | None
    latest_run: LatestRun | None


_LIST_STRATEGIES_SQL = """
    SELECT
        s.strategy_id, s.name, s.version, s.status, s.contract_version,
        s.registered_at, s.manifest -> 'data' ->> 'bars' AS bars,
        r.smoke_run_id, r.verdict, r.window_start, r.window_end, r.sessions,
        r.bar_calls, r.orders_placed, r.fills, r.rejections, r.final_equity,
        r.breaker_reason, r.runtime, r.kernel_isolated, r.contract_version,
        r.ran_at
    FROM strategies s
    LEFT JOIN LATERAL (
        SELECT *
        FROM strategy_smoke_runs sr
        WHERE sr.strategy_id = s.strategy_id
        ORDER BY sr.ran_at DESC
        LIMIT 1
    ) r ON true
    ORDER BY s.registered_at DESC, s.strategy_id DESC
    LIMIT %s
"""


def _strategy_summary_from_row(row: tuple[Any, ...]) -> StrategySummary:
    (
        strategy_id,
        name,
        version,
        status,
        contract_version,
        registered_at,
        bars,
        smoke_run_id,
        verdict,
        window_start,
        window_end,
        sessions,
        bar_calls,
        orders_placed,
        fills,
        rejections,
        final_equity,
        breaker_reason,
        runtime,
        kernel_isolated,
        run_contract_version,
        ran_at,
    ) = row
    # The LEFT JOIN LATERAL leaves every r.* column NULL when a strategy
    # has no recorded run -- smoke_run_id is the one column that can never
    # be NULL for a real run (it is the table's primary key), so it is
    # what decides whether latest_run is None rather than any nullable
    # field on the run itself (final_equity, breaker_reason, ... are all
    # legitimately NULL on a real row too).
    latest_run = (
        None
        if smoke_run_id is None
        else LatestRun(
            smoke_run_id=smoke_run_id,
            verdict=verdict,
            window_start=None if window_start is None else window_start.isoformat(),
            window_end=None if window_end is None else window_end.isoformat(),
            sessions=sessions,
            bar_calls=bar_calls,
            orders_placed=orders_placed,
            fills=fills,
            rejections=rejections,
            final_equity=None if final_equity is None else str(final_equity),
            breaker_reason=breaker_reason,
            runtime=runtime,
            kernel_isolated=kernel_isolated,
            contract_version=run_contract_version,
            ran_at=ran_at.isoformat(),
        )
    )
    return StrategySummary(
        strategy_id=strategy_id,
        name=name,
        version=version,
        status=status,
        contract_version=contract_version,
        registered_at=registered_at.isoformat(),
        bars=bars,
        latest_run=latest_run,
    )


@router.get("/strategies", response_model=list[StrategySummary])
def list_strategies(
    limit: int = Query(default=50, ge=1, le=200),
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> list[StrategySummary]:
    """Every registered strategy, newest first, with its latest smoke run.

    A plain `def`, like every route in this module: psycopg is
    synchronous, and this is a GET, so it must never write -- it only
    reads `strategies` and `strategy_smoke_runs`.

    The join to the latest run is a LEFT JOIN LATERAL, not an inner join:
    a strategy can be registered with zero rows in `strategy_smoke_runs`
    (see `record_smoke_run`'s module docstring on rejections storing
    nothing), and an inner join would silently hide that registered
    version instead of showing it with `latest_run: null`.
    """
    rows = conn.execute(_LIST_STRATEGIES_SQL, (limit,)).fetchall()
    return [_strategy_summary_from_row(row) for row in rows]


_GET_STRATEGY_SQL = _LIST_STRATEGIES_SQL.replace(
    "ORDER BY s.registered_at DESC, s.strategy_id DESC\n    LIMIT %s",
    "WHERE s.strategy_id = %s",
)


@router.get("/strategies/{strategy_id}", response_model=StrategySummary)
def get_strategy_summary(
    strategy_id: int,
    conn: Annotated[Connection, Depends(get_db_connection)],
) -> StrategySummary:
    """One registered strategy, shaped exactly like a row of `GET /strategies`.

    The same projection, deliberately: a detail page and a list row showing
    different fields for the same strategy is how the two drift. Fetching
    the whole list and filtering client-side would transfer every strategy
    to render one.

    Plain `def`, and a GET that never writes.
    """
    row = conn.execute(_GET_STRATEGY_SQL, (strategy_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"no strategy with strategy_id={strategy_id}")
    return _strategy_summary_from_row(row)


__all__ = [
    "ContractBundle",
    "LatestRun",
    "RunSummary",
    "StrategySummary",
    "Finding",
    "UploadStrategyRequest",
    "UploadStrategyResponse",
    "router",
]
