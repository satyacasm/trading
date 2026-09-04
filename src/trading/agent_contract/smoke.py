"""Stage 2 of the upload pipeline: the smoke run (contract §9).

Stages 1 and 3 inspect strategy code. This one **runs** it, which makes it
the first caller of the strategy runtime -- and the runtime, not this
module, is the part Phase 3 inherits. What lives here is only what stage 2
needs and a backtest does not: choosing a window, packing a payload,
driving the container twice, and turning what came back into something an
agent can act on.

**The sequence is forced to two passes.** The host cannot build a payload
without the manifest -- it needs the universe to know which instruments to
fetch -- and the manifest is whatever `configure()` returns, which only
runs inside the container. So `configure` mode is not an optimisation, it
is step one. Then bars are fetched, and the smoke payload is run **twice**
and the order sequences compared, because contract §2 makes determinism a
rule and nothing until now enforced it: static validation catches a
literal `datetime.now()` and misses set iteration order, unseeded
`random`, and dict-hash dependence.

**Warnings pass, and pass loudly.** A strategy that runs five clean
sessions without ordering is not obviously broken -- five arbitrary days
may not trigger a selective signal -- so failing it would refuse
legitimate strategies, and a gate that is wrong in an obvious way gets
routed around. But it passes with the fact stated first in the report and
recorded on the row, so "never exercised" stays distinguishable from
"proven" months later.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

from psycopg import Connection

from trading.agent_contract.registry import CONTRACT_VERSION
from trading.agent_contract.sandbox import (
    SandboxLimits,
    SandboxResult,
    run_smoke_in_sandbox,
    run_strategy_in_sandbox,
)
from trading.agent_contract.validation import Finding, ValidationReport
from trading.config import get_settings
from trading.corpactions.adjust import adjusted_bars
from trading.paper.charges import load_schedules
from trading.paper.enums import Product
from trading.runtime.payload import MODE_SMOKE, SmokePayload
from trading.runtime.provider import BarRecord

__all__ = [
    "SmokeVerdict",
    "build_verdict",
    "fetch_bars",
    "record_smoke_run",
    "resolve_bar_interval",
    "resolve_universe",
    "select_window",
    "smoke_test",
]

_FAIL_CODES = frozenset(
    {
        "SMOKE_CRASH",
        "SMOKE_TIMEOUT",
        "SMOKE_OOM",
        "NO_DATA",
        "MANIFEST_UNRESOLVABLE",
        "NONDETERMINISTIC",
    }
)


@dataclass(frozen=True)
class SmokeVerdict:
    passed: bool
    warnings_only: bool
    report: ValidationReport
    window: dict[str, Any]
    outcome: dict[str, Any] | None
    runtime: str
    kernel_isolated: bool
    notes: tuple[str, ...] = ()
    # The manifest's declared capital, and the currency it is denominated
    # in. Optional because a verdict can exist without one -- a crash
    # before configure() resolved, or a caller that did not supply it --
    # and `None` must stay distinguishable from zero: defaulting the
    # baseline to 0 would report the whole closing equity as profit.
    starting_cash: Decimal | None = None
    currency: str = ""

    @property
    def final_equity(self) -> Decimal | None:
        raw = (self.outcome or {}).get("final_equity")
        return None if raw is None else Decimal(str(raw))

    @property
    def pnl(self) -> Decimal | None:
        """Closing equity less declared capital, or None if either is unknown."""
        equity = self.final_equity
        if equity is None or self.starting_cash is None:
            return None
        return (equity - self.starting_cash).quantize(Decimal("0.01"))

    @property
    def pnl_pct(self) -> Decimal | None:
        """P&L as a percentage of capital. None on zero capital -- a return
        on nothing is undefined, not infinite, and not zero."""
        pnl = self.pnl
        if pnl is None or not self.starting_cash:
            return None
        return (pnl / self.starting_cash * 100).quantize(Decimal("0.01"))

    def as_agent_feedback(self) -> str:
        if not self.passed:
            header = f"REJECTED: the smoke run found {len(self.report.findings)} problem(s)."
        elif self.warnings_only:
            header = "PASSED WITH WARNINGS: the strategy ran five sessions without crashing."
        else:
            header = "PASSED: the strategy ran five sessions cleanly."

        lines = [header, ""]
        window = self.window
        lines.append(
            f"  Window: {window.get('start')} to {window.get('end')} "
            f"({window.get('sessions')} sessions)."
        )
        if self.outcome:
            lines.append(
                f"  on_bar calls: {self.outcome['bar_calls']:,}   "
                f"orders: {len(self.outcome['orders'])}   fills: {self.outcome['fills']}"
            )
            equity, pnl = self.final_equity, self.pnl
            if equity is not None:
                money = f"  Final equity: {equity:,.2f} {self.currency}".rstrip()
                if pnl is not None:
                    pct = "" if self.pnl_pct is None else f" ({self.pnl_pct:+.2f}%)"
                    money += f"   P&L: {pnl:+,.2f}{pct}"
                lines.append(money)
        lines.append("")

        for finding in self.report.findings:
            lines.append(f"  [{finding.code}] {finding.message}")
            if finding.contract_section:
                lines.append(f"      See STRATEGY_CONTRACT.md {finding.contract_section}.")

        lines.append("")
        lines.append("  What this run did NOT prove:")
        for note in self.notes:
            lines.append(f"    - {note}")
        if self.kernel_isolated:
            lines.append(
                f"    - Confined by runtime={self.runtime}: syscalls are mediated by a "
                "user-space kernel."
            )
        else:
            lines.append(
                f"    - Confined by runtime={self.runtime}: namespace/cgroup/seccomp only -- "
                "the host kernel is shared."
            )
        lines.append(
            "    - This window is not permanent. Re-running next week meets different bars."
        )
        return "\n".join(lines)


def _last_line(outcome: dict[str, Any]) -> str:
    """The final line of a traceback -- the exception, without the frames.

    Sliced with `[-1]` rather than `[-1:]`: the latter is a list, and an
    f-string renders a list as `['ValueError: boom']`, which reads to an
    agent as a fault in the platform rather than one in its strategy.
    """
    lines = (outcome.get("error") or "").strip().splitlines()
    return lines[-1].strip() if lines else "no error text was captured"


def _order_key(order: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(order.get(field) for field in sorted(order))


def build_verdict(
    first: dict[str, Any],
    second: dict[str, Any],
    window: dict[str, Any],
    runtime: str,
    kernel_isolated: bool,
    *,
    starting_cash: Decimal | None = None,
    currency: str = "",
) -> SmokeVerdict:
    findings: list[Finding] = []

    if not first.get("ok"):
        crashed_at = first.get("crashed_at") or {}
        where = crashed_at.get("handler", "the strategy")
        when = crashed_at.get("ts", "an unknown point")
        findings.append(
            Finding(
                # A timeout and an OOM kill never produced a RunOutcome at
                # all; `_outcome_of` translates them into this shape and
                # names the code, so §4's table stays true rather than
                # collapsing three distinct failures into SMOKE_CRASH.
                code=str(first.get("code", "SMOKE_CRASH")),
                message=(
                    f"{where} raised at simulated time {when}, after "
                    f"{first.get('bar_calls', 0):,} on_bar calls.\n"
                    f"      {_last_line(first)}"[:600]
                ),
                contract_section="§2",
            )
        )
    elif not second.get("ok"):
        # Only the second pass died. That is not merely a crash -- it is
        # proof the two passes disagreed, which is the deeper fault: a
        # strategy that fails intermittently cannot be backtested at all,
        # and the crash is the evidence rather than the finding. Reported
        # here instead of leaving it to the order comparison below, which
        # sees [] == [] whenever neither pass got far enough to order.
        crashed_at = second.get("crashed_at") or {}
        findings.append(
            Finding(
                code="NONDETERMINISTIC",
                message=(
                    "two runs of identical input disagreed: pass 1 completed "
                    f"{first.get('bar_calls', 0):,} on_bar calls, pass 2 raised in "
                    f"{crashed_at.get('handler', 'the strategy')} at simulated time "
                    f"{crashed_at.get('ts') or 'an unknown point'}.\n"
                    f"      {_last_line(second)}\n"
                    "      A backtest of this strategy would not be reproducible. Common "
                    "causes: random without a seed, iterating a set, or depending on dict "
                    "insertion order that varies."
                )[:800],
                contract_section="§2",
            )
        )
    if first.get("ok"):
        first_orders = [_order_key(o) for o in first.get("orders", [])]
        second_orders = [_order_key(o) for o in second.get("orders", [])]
        if second.get("ok") and first_orders != second_orders:
            index = next(
                (
                    i
                    for i, (a, b) in enumerate(zip(first_orders, second_orders, strict=False))
                    if a != b
                ),
                min(len(first_orders), len(second_orders)),
            )
            findings.append(
                Finding(
                    code="NONDETERMINISTIC",
                    message=(
                        f"two runs of identical input produced different orders; they first "
                        f"differ at order {index + 1}. A backtest of this strategy would not be "
                        "reproducible. Common causes: iterating a set, random without a seed, "
                        "or depending on dict insertion order that varies."
                    ),
                    contract_section="§2",
                )
            )

        orders = first.get("orders", [])
        if not orders:
            findings.append(
                Finding(
                    code="NO_ORDERS",
                    message=(
                        f"{window.get('sessions')} sessions, 0 orders placed. on_bar ran "
                        f"{first.get('bar_calls', 0):,} times without ordering, so nothing about "
                        "your order path was tested. Likely a threshold never crossed or a "
                        "condition inverted -- check your ctx.log output."
                    ),
                    contract_section="§4",
                )
            )
        elif all(o.get("status") == "REJECTED" for o in orders):
            reasons = first.get("rejections") or ["no reason recorded"]
            findings.append(
                Finding(
                    code="ALL_ORDERS_REJECTED",
                    message=(
                        f"all {len(orders)} orders were rejected. Most common reason: {reasons[0]}"
                    ),
                    contract_section="§6",
                )
            )

        if first.get("breaker_reason"):
            findings.append(
                Finding(
                    code="BREAKER_TRIPPED",
                    message=(
                        "the strategy hit its own declared limit inside the window: "
                        f"{first['breaker_reason']}"
                    ),
                    contract_section="§3",
                )
            )

    report = ValidationReport(findings=tuple(findings))
    failed = any(f.code in _FAIL_CODES for f in findings)
    notes = [
        "Partial fills: the fill model fills an order's full remaining quantity, so "
        "PARTIALLY_FILLED never occurred and your handling of it is untested.",
        "Ticks, on_expiry, and ctx.intel are not routed by a smoke run.",
    ]
    return SmokeVerdict(
        passed=not failed,
        warnings_only=not failed and bool(findings),
        report=report,
        window=window,
        outcome=first if first.get("ok") else None,
        runtime=runtime,
        kernel_isolated=kernel_isolated,
        notes=tuple(notes),
        starting_cash=starting_cash,
        currency=currency,
    )


_WINDOW_SQL = """
    SELECT ts::date AS session
    FROM bars_intraday
    WHERE instrument_id = ANY(%s) AND interval_sec = 60
    GROUP BY session
    HAVING COUNT(DISTINCT instrument_id) = %s
    ORDER BY session DESC
    LIMIT %s
"""

_DAILY_WINDOW_SQL = """
    SELECT ts::date AS session
    FROM bars_daily
    WHERE instrument_id = ANY(%s)
    GROUP BY session
    HAVING COUNT(DISTINCT instrument_id) = %s
    ORDER BY session DESC
    LIMIT %s
"""

_BARS_SQL = """
    SELECT instrument_id, ts, interval_sec, open, high, low, close, volume, trades,
           open_interest, oi_change
    FROM bars_intraday
    WHERE instrument_id = ANY(%s) AND interval_sec = 60
      AND ts >= %s AND ts < %s
    ORDER BY instrument_id, ts
"""


def select_window(
    conn: Connection, instrument_ids: Sequence[int], sessions: int = 5, *, interval_sec: int = 60
) -> dict[str, Any]:
    """The most recent sessions EVERY instrument printed in.

    Intersection rather than union, deliberately: a window where half the
    universe has no bars would hand a strategy a market in which half its
    instruments silently do not exist, and the absent-not-carried-forward
    rule would make that indistinguishable from a quiet day.

    `bars_daily` has no `interval_sec` column -- it is one row per
    instrument per day, not multiplexed like `bars_intraday` -- so the
    daily branch's query has no equivalent filter to apply.
    """
    sql = _DAILY_WINDOW_SQL if interval_sec == 86400 else _WINDOW_SQL
    rows = conn.execute(sql, (list(instrument_ids), len(set(instrument_ids)), sessions)).fetchall()
    days = sorted(row[0] for row in rows)
    if not days:
        return {"start": None, "end": None, "sessions": 0, "instruments": {}}
    start = datetime.combine(days[0], time.min, tzinfo=UTC)
    end = datetime.combine(days[-1], time.max, tzinfo=UTC)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(days),
        "instruments": {},
    }


def _fetch_daily_bars(
    conn: Connection, instrument_ids: Sequence[int], window: dict[str, Any]
) -> dict[int, tuple[BarRecord, ...]]:
    """The `bars="1d"` path: one `adjusted_bars` call per instrument.

    `as_of` is the window's end date for every instrument and every bar in
    the run (D3a-2) -- one fixed factor set, so the series is continuous
    and returns are correct throughout the run, at the accepted cost that
    an early bar's absolute price level may not match what the exchange
    printed that day if a split lands later in the window.
    """
    start = datetime.fromisoformat(window["start"]).date()
    end = datetime.fromisoformat(window["end"]).date()
    series: dict[int, list[BarRecord]] = {}
    for instrument_id in instrument_ids:
        frame = adjusted_bars(conn, instrument_id, start, end, as_of=end)
        for row in frame.iter_rows(named=True):
            series.setdefault(instrument_id, []).append(
                BarRecord(
                    instrument_id=instrument_id,
                    ts=row["ts"],
                    interval_sec=86400,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=None if row["volume"] is None else Decimal(row["volume"]),
                    # bars_daily.ts IS the session close -- verified against
                    # the table, every row is 10:00 UTC / 15:30 IST. That is a
                    # fact about the session, not an arithmetic consequence of
                    # the interval, so it is stated rather than derived.
                    knowable_at=row["ts"],
                )
            )
    window["instruments"] = {str(k): {"bars": len(v)} for k, v in series.items()}
    return {k: tuple(v) for k, v in series.items()}


def fetch_bars(
    conn: Connection,
    instrument_ids: Sequence[int],
    window: dict[str, Any],
    *,
    interval_sec: int = 60,
) -> dict[int, tuple[BarRecord, ...]]:
    if window["start"] is None:
        return {}
    if interval_sec == 86400:
        return _fetch_daily_bars(conn, instrument_ids, window)
    rows = conn.execute(
        _BARS_SQL,
        (
            list(instrument_ids),
            datetime.fromisoformat(window["start"]),
            datetime.fromisoformat(window["end"]),
        ),
    ).fetchall()
    series: dict[int, list[BarRecord]] = {}
    for (
        instrument_id,
        ts,
        interval_sec_row,
        open_,
        high,
        low,
        close,
        volume,
        trades,
        open_interest,
        oi_change,
    ) in rows:
        series.setdefault(instrument_id, []).append(
            BarRecord(
                instrument_id=instrument_id,
                ts=ts,
                interval_sec=interval_sec_row,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=None if volume is None else Decimal(volume),
                trades=trades,
                open_interest=open_interest,
                oi_change=oi_change,
            )
        )
    window["instruments"] = {str(k): {"bars": len(v)} for k, v in series.items()}
    return {k: tuple(v) for k, v in series.items()}


class _UnresolvedUniverse(Exception):
    """A declared instrument has no row at `as_of`."""


_BAR_INTERVALS_SEC: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}

# Which of the five contract-legal intervals this platform can actually
# serve today. select_window/fetch_bars route interval_sec == 86400
# through bars_daily and everything else through bars_intraday -- but
# bars_intraday holds ONLY 60-second rows (confirmed directly against
# this database: every row has interval_sec = 60, no other value exists).
# Before this check existed, a schema-legal "5m"/"15m"/"1h" manifest
# resolved cleanly and was then silently served 1-minute bars anyway --
# exactly the defect this whole file exists to remove, just for three of
# five values instead of one. Serving them for real needs genuine 5m/
# 15m/1h aggregation or a resampling path -- real work, not a one-line
# change -- so for now, an unserved value raises rather than lies.
_SERVED_INTERVALS_SEC = frozenset({60, 86400})


class _InvalidBarInterval(Exception):
    """The manifest's data.bars is not recognized, or not yet served.

    Raised rather than defaulted in both cases: nothing schema-checks
    this value before smoke_test uses it -- api.py's pre-smoke-test
    validate_strategy() call has no manifest yet, and validate_manifest
    only runs inside register_strategy, after a passing smoke test. A
    silent default to 60 -- or silently serving 1-minute bars for a
    schema-legal value this platform cannot yet honor -- would both be
    the exact silent-wrong-data failure this plan exists to remove.
    """


def resolve_bar_interval(manifest: dict[str, Any]) -> int:
    data = manifest.get("data")
    raw = data.get("bars") if isinstance(data, dict) else None
    try:
        interval_sec = _BAR_INTERVALS_SEC[raw]  # type: ignore[index]
    except (KeyError, TypeError):
        raise _InvalidBarInterval(
            f"the manifest declares data.bars={raw!r}, which is not one of the five "
            f"values the contract permits: {sorted(_BAR_INTERVALS_SEC)}. "
            "See STRATEGY_CONTRACT.md §3."
        ) from None
    if interval_sec not in _SERVED_INTERVALS_SEC:
        raise _InvalidBarInterval(
            f"data.bars={raw!r} is one of the contract's five permitted values, but "
            'this platform does not yet serve it. Only "1m" and "1d" are served '
            "today -- serving 5m/15m/1h needs bar aggregation this platform does not "
            "yet have. See STRATEGY_CONTRACT.md §3."
        )
    return interval_sec


def resolve_universe(conn: Connection, manifest: dict[str, Any], as_of: date) -> list[int]:
    """Instrument ids for a manifest's universe, point-in-time.

    Resolution is against `listed_on`/`delisted_on` at `as_of`, not against
    what exists today -- the same survivorship-bias defence the data layer
    keeps, applied here so a smoke run cannot quietly include an instrument
    that had not listed yet.
    """
    universe = manifest.get("universe")
    if isinstance(universe, list):
        ids, missing = [], []
        for ref in universe:
            row = conn.execute(
                "SELECT instrument_id FROM instruments WHERE exchange=%s AND segment=%s "
                "AND symbol=%s AND (listed_on IS NULL OR listed_on <= %s) "
                "AND (delisted_on IS NULL OR delisted_on > %s)",
                (ref["exchange"], ref["segment"], ref["symbol"], as_of, as_of),
            ).fetchone()
            if row is None:
                missing.append(f"{ref['exchange']}:{ref['segment']}:{ref['symbol']}")
            else:
                ids.append(row[0])
        if missing:
            listed = "\n  ".join(missing)
            raise _UnresolvedUniverse(
                f"your universe declares {len(universe)} instrument(s); {len(missing)} "
                f"do(es) not resolve at {as_of.isoformat()}:\n  {listed}\n"
                "Check the exchange/segment/symbol triple, or the listing date if this "
                "instrument is newly listed. Dropping it and running on the rest would "
                "report a pass for a universe you did not ask for."
            )
        return ids
    if not isinstance(universe, dict):
        universe = {}
    clauses, params = (
        ["(listed_on IS NULL OR listed_on <= %s)", "(delisted_on IS NULL OR delisted_on > %s)"],
        [as_of, as_of],
    )
    for column in ("asset_class", "exchange"):
        if universe.get(column):
            clauses.append(f"{column} = %s")
            params.append(universe[column])
    rows = conn.execute(
        f"SELECT instrument_id FROM instruments WHERE {' AND '.join(clauses)} "
        "ORDER BY instrument_id",
        params,
    ).fetchall()
    return [row[0] for row in rows]


_BROKER_FOR_ASSET_CLASS = {"EQUITY": "UPSTOX", "CRYPTO": "BINANCE"}


class _MixedUniverse(Exception):
    """The universe spans more than one charge regime."""


def _charge_key(conn: Connection, instrument_ids: Sequence[int]) -> tuple[str, str, str]:
    """The (broker, exchange, asset_class) whose schedules price this run.

    A universe spanning two regimes is refused rather than priced with one
    of them. Contract D6 already fixes a strategy to one portfolio and one
    currency, so a mixed universe is a manifest that cannot mean what it
    says -- and pricing NSE equity fills with Binance's schedule would
    produce a cost model that is quietly, confidently wrong, which is the
    exact failure this platform exists to avoid.
    """
    rows = conn.execute(
        "SELECT DISTINCT asset_class, exchange FROM instruments WHERE instrument_id = ANY(%s)",
        (list(instrument_ids),),
    ).fetchall()
    if len(rows) != 1:
        found = ", ".join(f"{a}/{e}" for a, e in sorted(rows))
        raise _MixedUniverse(
            f"the universe spans {len(rows)} charge regimes ({found}). A strategy trades one "
            "portfolio in one currency (see the contract's D6), and charges differ per "
            "exchange and asset class -- pricing them all with one schedule would be wrong "
            "rather than approximate. Split this into one strategy per regime."
        )
    asset_class, exchange = rows[0]
    broker = _BROKER_FOR_ASSET_CLASS.get(asset_class)
    if broker is None:
        raise _MixedUniverse(
            f"no charge schedule is seeded for asset_class={asset_class!r}; "
            f"known: {sorted(_BROKER_FOR_ASSET_CLASS)}"
        )
    return broker, exchange, asset_class


def _resolve_limits(limits: SandboxLimits | None) -> SandboxLimits:
    """The ceilings for this run, with the daemon and runtime from settings.

    Both halves come from one place because either alone is a trap.
    `SandboxLimits.runtime=None` means "the daemon's default", and a
    daemon with gVisor installed still defaults to `runc`, so naming the
    daemon without naming the runtime silently gets namespaces. Naming the
    runtime without the daemon is worse: it asks for `runsc` from a daemon
    that may not have it.

    Resolving both here rather than at each call site also means the
    `configure` pass cannot end up weaker than the smoke passes -- the
    failure mode worth designing out, since `configure` is the first thing
    that runs the strategy's code.

    An explicit `limits` wins untouched: settings supply a default, they
    do not override a caller who asked for something specific.
    """
    if limits is not None:
        return limits
    settings = get_settings()
    return SandboxLimits(
        runtime=settings.strategy_sandbox_runtime,
        docker_context=settings.strategy_sandbox_docker_context,
    )


def smoke_test(
    conn: Connection, source: str, *, limits: SandboxLimits | None = None
) -> SmokeVerdict:
    """The whole of stage 2: configure, fetch, run twice, judge."""
    limits = _resolve_limits(limits)
    configured = run_strategy_in_sandbox(source, limits)
    if not configured.ok or configured.manifest is None:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="MANIFEST_UNRESOLVABLE",
                        message=(
                            "configure() did not return a usable manifest: "
                            f"{(configured.error or 'no manifest was produced').strip()[:400]}"
                        ),
                        contract_section="§3",
                    ),
                )
            ),
            window={"start": None, "end": None, "sessions": 0},
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )

    manifest = configured.manifest
    try:
        interval_sec = resolve_bar_interval(manifest)
    except _InvalidBarInterval as invalid:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="MANIFEST_UNRESOLVABLE",
                        message=str(invalid),
                        contract_section="§3",
                    ),
                )
            ),
            window={"start": None, "end": None, "sessions": 0, "instruments": {}},
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )
    try:
        instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
    except _UnresolvedUniverse as unresolved:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="MANIFEST_UNRESOLVABLE",
                        message=str(unresolved),
                        contract_section="§3",
                    ),
                )
            ),
            window={"start": None, "end": None, "sessions": 0, "instruments": {}},
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )
    window = (
        select_window(conn, instrument_ids, interval_sec=interval_sec)
        if instrument_ids
        else {"start": None, "end": None, "sessions": 0, "instruments": {}}
    )
    bars = (
        fetch_bars(conn, instrument_ids, window, interval_sec=interval_sec)
        if instrument_ids
        else {}
    )
    if not bars:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="NO_DATA",
                        message=(
                            f"the manifest's universe resolved to {len(instrument_ids)} "
                            "instrument(s), and no window exists where all of them have "
                            "recorded bars at the declared interval. A smoke run needs "
                            "recorded data for every instrument it will feed."
                        ),
                        contract_section="§3",
                    ),
                )
            ),
            window=window,
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )

    try:
        broker, exchange, asset_class = _charge_key(conn, instrument_ids)
    except _MixedUniverse as mixed:
        return SmokeVerdict(
            passed=False,
            warnings_only=False,
            report=ValidationReport(
                findings=(
                    Finding(
                        code="MANIFEST_UNRESOLVABLE", message=str(mixed), contract_section="§3"
                    ),
                )
            ),
            window=window,
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
        )
    schedules = load_schedules(
        conn,
        broker,
        exchange,
        asset_class,
        Product.DELIVERY,
        datetime.fromisoformat(window["end"]).date(),
    )
    payload = SmokePayload(
        mode=MODE_SMOKE,
        source=source,
        contract_version=CONTRACT_VERSION,
        window=window,
        bars=bars,
        charge_schedules=tuple(schedules),
        starting_cash=Decimal(str(manifest.get("capital", "0"))),
        slippage_bps=Decimal("0"),
    )
    first = run_smoke_in_sandbox(payload, limits)
    second = run_smoke_in_sandbox(payload, limits)
    return build_verdict(
        _outcome_of(first),
        _outcome_of(second),
        window,
        first.runtime,
        first.kernel_isolated,
        starting_cash=payload.starting_cash,
        currency=str(manifest.get("base_currency", "")),
    )


def _outcome_of(result: SandboxResult) -> dict[str, Any]:
    """A SandboxResult as the dict `build_verdict` reads.

    A timeout or an OOM kill never produced a RunOutcome at all, so they
    are translated into the same crash shape rather than left as a None
    the verdict logic would have to special-case.
    """
    if result.outcome is not None:
        return result.outcome
    stage = {"timeout": "SMOKE_TIMEOUT", "killed": "SMOKE_OOM"}.get(result.stage, "SMOKE_CRASH")
    return {
        "ok": False,
        "code": stage,
        "bar_calls": 0,
        "orders": [],
        "fills": 0,
        "rejections": [],
        "final_cash": "0",
        "final_equity": "0",
        "breaker_reason": None,
        "logs": [],
        "error": f"[{stage}] {result.error or 'the sandbox produced no outcome'}",
        "crashed_at": {"handler": stage, "ts": ""},
    }


_INSERT_SMOKE_RUN = """
    INSERT INTO strategy_smoke_runs (
        strategy_id, verdict, window_start, window_end, sessions, instruments,
        bar_calls, orders_placed, fills, rejections, rejection_reasons, final_cash,
        final_equity, breaker_reason, findings, runtime, kernel_isolated, contract_version
    ) VALUES (
        %(strategy_id)s, %(verdict)s, %(window_start)s, %(window_end)s, %(sessions)s,
        %(instruments)s, %(bar_calls)s, %(orders_placed)s, %(fills)s, %(rejections)s,
        %(rejection_reasons)s, %(final_cash)s, %(final_equity)s, %(breaker_reason)s,
        %(findings)s, %(runtime)s, %(kernel_isolated)s, %(contract_version)s
    ) RETURNING smoke_run_id
"""


def _money(raw: Any) -> Decimal | None:
    """A money field from the container's JSON, as a Decimal.

    The runtime serialises cash and equity as strings precisely so no
    float ever exists between the loop and this row (§5). Postgres would
    cast the string for us on the way into NUMERIC, but doing it here
    keeps the "no float in a money path" rule checkable in Python rather
    than resting on an implicit database cast.
    """
    return None if raw is None else Decimal(str(raw))


def _timestamp(raw: Any) -> datetime | None:
    return None if raw is None else datetime.fromisoformat(str(raw))


def record_smoke_run(conn: Connection, strategy_id: int, verdict: SmokeVerdict) -> int:
    """Store one smoke run against a registered strategy.

    Following this codebase's convention, nothing here commits: the caller
    owns the transaction boundary, so a registration and the smoke run
    that justified it can land as one unit.
    """
    outcome = verdict.outcome or {}
    label = (
        "REJECTED"
        if not verdict.passed
        else ("PASSED_WITH_WARNINGS" if verdict.warnings_only else "PASSED")
    )
    rejections = list(outcome.get("rejections") or [])
    row = conn.execute(
        _INSERT_SMOKE_RUN,
        {
            "strategy_id": strategy_id,
            "verdict": label,
            "window_start": _timestamp(verdict.window.get("start")),
            "window_end": _timestamp(verdict.window.get("end")),
            "sessions": verdict.window.get("sessions", 0),
            "instruments": json.dumps(verdict.window.get("instruments", {})),
            "bar_calls": outcome.get("bar_calls", 0),
            "orders_placed": len(outcome.get("orders", [])),
            "fills": outcome.get("fills", 0),
            "rejections": len(rejections),
            "rejection_reasons": json.dumps(rejections),
            "final_cash": _money(outcome.get("final_cash")),
            "final_equity": _money(outcome.get("final_equity")),
            "breaker_reason": outcome.get("breaker_reason"),
            "findings": json.dumps(
                [
                    {
                        "code": f.code,
                        "message": f.message,
                        "line": f.line,
                        "contract_section": f.contract_section,
                    }
                    for f in verdict.report.findings
                ]
            ),
            "runtime": verdict.runtime,
            "kernel_isolated": verdict.kernel_isolated,
            "contract_version": CONTRACT_VERSION,
        },
    ).fetchone()
    if row is None:  # pragma: no cover - RETURNING on INSERT always yields a row
        raise RuntimeError("the smoke run insert returned no row")
    return int(row[0])
