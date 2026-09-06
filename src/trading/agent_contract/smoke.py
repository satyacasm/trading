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
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

from psycopg import Connection

from trading.agent_contract.registry import CONTRACT_VERSION, get_strategy
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
from trading.paper.models import ChargeSchedule
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
    # The limits the caller chose, if any. `None` means the strategy's own
    # were used -- a distinction worth keeping, since one is a decision
    # about this run and the other is not.
    max_daily_loss: Decimal | None = None
    max_drawdown_pct: Decimal | None = None
    # The manifest's declared capital, and the currency it is denominated
    # in. Optional because a verdict can exist without one -- a crash
    # before configure() resolved, or a caller that did not supply it --
    # and `None` must stay distinguishable from zero: defaulting the
    # baseline to 0 would report the whole closing equity as profit.
    starting_cash: Decimal | None = None
    currency: str = ""
    # The manifest configure() returned, so a caller downstream of the
    # verdict (registration, the listing route) can see what stage 2
    # actually resolved without re-running the sandbox. None on every
    # path that returns before configure() produced one -- there is
    # nothing here to carry.
    manifest: dict[str, Any] | None = None

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
    bars_label = _BAR_LABELS_BY_SEC.get(interval_sec)
    if not days:
        return {
            "start": None,
            "end": None,
            "sessions": 0,
            "instruments": {},
            "bars": bars_label,
            "interval_sec": interval_sec,
        }
    start = datetime.combine(days[0], time.min, tzinfo=UTC)
    end = datetime.combine(days[-1], time.max, tzinfo=UTC)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(days),
        "instruments": {},
        "bars": bars_label,
        "interval_sec": interval_sec,
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

# Built once by inverting _BAR_INTERVALS_SEC rather than hardcoding a
# second literal mapping -- two copies of the same five pairs can drift,
# and a window naming the wrong bar label would be exactly the kind of
# quiet lie this module exists to remove.
_BAR_LABELS_BY_SEC: dict[int, str] = {sec: label for label, sec in _BAR_INTERVALS_SEC.items()}

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


_BROKER_FOR_ASSET_CLASS = {"EQUITY": "UPSTOX", "CRYPTO": "BINANCE", "PERP": "BINANCE"}


def _perp_instruments(conn: Connection, instrument_ids: Sequence[int]) -> tuple[int, ...]:
    """Which of this run's instruments settle as derivatives.

    Resolved here, where there is a database, because the container has
    none -- and whether an instrument is a perpetual decides how its fills
    move cash.
    """
    rows = conn.execute(
        "SELECT instrument_id FROM instruments"
        " WHERE instrument_id = ANY(%s) AND asset_class = 'PERP'",
        (list(instrument_ids),),
    ).fetchall()
    return tuple(int(r[0]) for r in rows)


def _funding_rates(
    conn: Connection, instrument_ids: Sequence[int], window: dict[str, str]
) -> tuple[dict[str, str], ...]:
    """Every published settlement inside the run's window.

    Strings on the wire, like every other number crossing into the
    container. Empty for a run with no perpetuals, which costs nothing.
    """
    rows = conn.execute(
        "SELECT instrument_id, funding_time, rate FROM perp_funding"
        " WHERE instrument_id = ANY(%s) AND funding_time >= %s AND funding_time <= %s"
        " ORDER BY funding_time",
        (list(instrument_ids), window["start"], window["end"]),
    ).fetchall()
    return tuple(
        {"instrument_id": str(r[0]), "ts": r[1].isoformat(), "rate": str(r[2])} for r in rows
    )


def _manifest_leverage(manifest: dict[str, Any]) -> Decimal | None:
    raw = manifest.get("leverage")
    return None if raw is None else Decimal(str(raw))


def product_for_asset_class(asset_class: str) -> Product:
    """Which product's charges price this asset class.

    A perpetual is never delivered -- it has no expiry to deliver at -- so
    its schedule is seeded under INTRADAY. Asking for DELIVERY finds
    nothing and refuses a run the platform could have priced perfectly
    well, which is a confusing way to learn that a contract has no
    settlement date.
    """
    return Product.INTRADAY if asset_class == "PERP" else Product.DELIVERY


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
            window={
                "start": None,
                "end": None,
                "sessions": 0,
                "bars": None,
                "interval_sec": None,
            },
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
            window={
                "start": None,
                "end": None,
                "sessions": 0,
                "instruments": {},
                "bars": None,
                "interval_sec": None,
            },
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
            manifest=manifest,
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
            window={
                "start": None,
                "end": None,
                "sessions": 0,
                "instruments": {},
                "bars": None,
                "interval_sec": None,
            },
            outcome=None,
            runtime=configured.runtime,
            kernel_isolated=configured.kernel_isolated,
            manifest=manifest,
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
            manifest=manifest,
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
            manifest=manifest,
        )
    schedules = load_schedules(
        conn,
        broker,
        exchange,
        asset_class,
        product_for_asset_class(asset_class),
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
        perp_instruments=_perp_instruments(conn, instrument_ids),
        funding_rates=_funding_rates(conn, instrument_ids, window),
        leverage=_manifest_leverage(manifest),
    )
    first = run_smoke_in_sandbox(payload, limits)
    second = run_smoke_in_sandbox(payload, limits)
    verdict = build_verdict(
        _outcome_of(first),
        _outcome_of(second),
        window,
        first.runtime,
        first.kernel_isolated,
        starting_cash=payload.starting_cash,
        currency=str(manifest.get("base_currency", "")),
    )
    # build_verdict's signature stays free of a manifest parameter -- it is
    # also called directly by tests with a hand-built outcome and no
    # manifest in scope -- so the manifest this real run resolved is
    # attached here instead, after the fact.
    return replace(verdict, manifest=manifest)


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


_HISTORY_SESSIONS_SQL = """
    SELECT DISTINCT ts::date AS session_date
    FROM bars_daily
    WHERE instrument_id = ANY(%s) AND ts::date < %s
    ORDER BY session_date DESC
    LIMIT %s
"""


_WINDOW_COVERAGE_SQL = """
    SELECT
        count(DISTINCT ts::date) FILTER (WHERE ts::date BETWEEN %s AND %s) AS sessions,
        min(ts::date) AS data_start,
        max(ts::date) AS data_end
    FROM bars_daily
    WHERE instrument_id = ANY(%s)
"""

# Bars per megabyte of decoded container memory. A `BarRecord` of Decimals
# costs far more than its wire form -- `payload.py` notes ~10:1 on gzipped
# numeric text -- so this is deliberately conservative and measured against
# the decoded object, which is what actually has to fit.
_BARS_PER_MB = 2_000


def bar_ceiling(limits: SandboxLimits) -> int:
    """How many bars fit the run's memory limit.

    Derived from `limits.memory` rather than guessed beside it: a raised
    backtest profile lifts the ceiling automatically, and the two numbers
    cannot drift into disagreeing about what fits.
    """
    return int(int(limits.memory.rstrip("m")) * _BARS_PER_MB)


@dataclass(frozen=True)
class BacktestPlan:
    """Everything decided about a backtest *before* a single bar is fetched.

    One object rather than three functions because widening for warm-up,
    estimating size, and checking coverage are all facts about the same
    request, established by queries over the same window -- three callers
    would each re-ask it.

    `start` is where bars are fetched from; `dispatch_from` is where the run
    begins. They differ by exactly the warm-up the manifest asked for, which
    is the whole of D3b-3: warm-up populates the lookback, it does not move
    the experiment.
    """

    start: datetime
    end: datetime
    dispatch_from: datetime
    history_bars_requested: int
    history_bars_available: int
    instruments: int = 0
    sessions: int = 0
    estimated_bars: int = 0
    data_start: date | None = None
    data_end: date | None = None
    findings: tuple[Finding, ...] = ()


def plan_backtest(
    conn: Connection,
    manifest: dict[str, Any],
    instrument_ids: Sequence[int],
    *,
    start: date,
    end: date,
    limits: SandboxLimits | None = None,
) -> BacktestPlan:
    """Pre-flight a backtest: widen for warm-up, and report any shortfall.

    `history_bars` is counted in **sessions**, not calendar days -- a
    strategy asking for 200 bars wants 200 prints, and subtracting 200 days
    would hand it roughly 138 over a weekend-bearing window.

    A shortfall is reported rather than silently accepted: a strategy warmed
    on 40 of the 200 bars it asked for is a different experiment from the one
    requested, and reporting it as the requested one is the same class of
    silent-wrong-data failure that 3a exists to eliminate.
    """
    data = manifest.get("data")
    requested = data.get("history_bars", 100) if isinstance(data, dict) else 100
    requested = int(requested)

    rows = conn.execute(_HISTORY_SESSIONS_SQL, (list(instrument_ids), start, requested)).fetchall()
    available_days = sorted(row[0] for row in rows)

    dispatch_from = datetime.combine(start, time.min, tzinfo=UTC)
    fetch_start = (
        datetime.combine(available_days[0], time.min, tzinfo=UTC)
        if available_days
        else dispatch_from
    )

    # One query answers both remaining questions: how big the run is, and
    # whether the window is covered at all. Neither fetches a bar.
    row = conn.execute(_WINDOW_COVERAGE_SQL, (start, end, list(instrument_ids))).fetchone()
    sessions = int(row[0] or 0) if row else 0
    data_start = row[1] if row else None
    data_end = row[2] if row else None

    instruments = len(set(instrument_ids))
    estimated = instruments * (sessions + len(available_days))
    ceiling = bar_ceiling(limits or SandboxLimits())

    findings: list[Finding] = []
    if estimated > ceiling:
        findings.append(
            Finding(
                code="BACKTEST_TOO_LARGE",
                message=(
                    f"{instruments} instruments x {sessions + len(available_days)} sessions "
                    f"= ~{estimated:,} bars, over the {ceiling:,}-bar ceiling for a "
                    f"{(limits or SandboxLimits()).memory} run. "
                    "Narrow the universe, or shorten the window."
                ),
                contract_section="§9",
            )
        )
    if data_end is not None and end > data_end:
        missing = (end - data_end).days
        findings.append(
            Finding(
                code="BACKTEST_WINDOW_UNCOVERED",
                message=(
                    f"the window runs to {end.isoformat()} but these instruments have no "
                    f"bars after {data_end.isoformat()} -- the last {missing} day(s) of the "
                    "request have no data. Running anyway would report a flat tail as a "
                    "fact about the market. Shorten the window, or backfill the gap."
                ),
                contract_section="§9",
            )
        )

    return BacktestPlan(
        start=fetch_start,
        end=datetime.combine(end, time.max, tzinfo=UTC),
        dispatch_from=dispatch_from,
        history_bars_requested=requested,
        history_bars_available=len(available_days),
        instruments=instruments,
        sessions=sessions,
        estimated_bars=estimated,
        data_start=data_start,
        data_end=data_end,
        findings=tuple(findings),
    )


# §228: "an automatic 2x slippage-and-cost stress rerun (if the edge dies at
# 2x, it was never an edge)."
STRESS_MULTIPLIER = Decimal("2")

_EARLIEST_SCHEDULE_SQL = """
    SELECT min(effective_from) FROM charge_schedules
    WHERE broker = %s AND exchange = %s AND asset_class = %s AND product = %s
"""


def earliest_schedule_date(
    conn: Connection, broker: str, exchange: str, asset_class: str, product: Product
) -> date | None:
    """The first date any charge rule exists for this combination."""
    row = conn.execute(
        _EARLIEST_SCHEDULE_SQL, (broker, exchange, asset_class, product.value)
    ).fetchone()
    return None if row is None else row[0]


def charge_lookup_date(window_end: date, earliest: date | None) -> tuple[date, str | None]:
    """Which date to price this run's charges at, and what to say about it.

    `load_schedules` filters `effective_from <= on`, so a window ending
    before any schedule exists finds none and `compute_charges` refuses --
    correctly, since a silently-zero cost is the one thing it must never
    produce. But it refused *inside the container*, after two container
    runs, for a condition answerable by one query beforehand.

    It was also inconsistent. A 2020-2026 window already prices its 2020
    fills at rates effective from 2024-10-01, because the lookup uses the
    window's end; only the short window crashed. Clamping to the earliest
    available schedule makes both cases work and makes the approximation
    visible in both -- the alternative is a platform that silently
    backdates rates on long windows and crashes on short ones.
    """
    if earliest is None or window_end >= earliest:
        return window_end, None
    return earliest, (
        f"this window ends {window_end.isoformat()}, before any charge schedule exists "
        f"({earliest.isoformat()} is the earliest). Charges are computed at the "
        f"{earliest.isoformat()} rates, so costs for this period are approximate. "
        "A window that ends after that date already prices its early fills the same "
        "way -- the lookup uses the window's end."
    )


def stress_schedules(
    schedules: Sequence[ChargeSchedule], *, multiplier: Decimal = STRESS_MULTIPLIER
) -> tuple[ChargeSchedule, ...]:
    """The same charge structure in a harsher world.

    Scaling the schedules host-side rather than plumbing a multiplier
    through `SmokePayload`, the runner, `run_loop` and `compute_charges`:
    that is four boundaries, and the container then runs unmodified code.
    It is also the more honest model -- a stress test *is* a different cost
    environment, not a different calculation.

    `cap` is scaled alongside `rate`, deliberately. A capped charge whose
    rate doubled but whose cap did not would simply stay at its cap, and the
    stress would silently fail to apply to exactly the charges that dominate
    a large order. `None` means no cap and stays `None`.
    """
    return tuple(
        schedule.model_copy(
            update={
                "rate": schedule.rate * multiplier,
                "cap": None if schedule.cap is None else schedule.cap * multiplier,
            }
        )
        for schedule in schedules
    )


@dataclass(frozen=True)
class BacktestVerdict:
    """What one backtest produced, or why it was refused."""

    passed: bool
    report: ValidationReport
    plan: BacktestPlan | None
    bars: str | None
    outcome: dict[str, Any] | None
    runtime: str | None = None
    kernel_isolated: bool = False
    # The universe as resolved point-in-time at the window's end. Carried on
    # the verdict so the store records what was actually traded rather than
    # re-resolving it later against a different `as_of` and getting a
    # different answer.
    instrument_ids: tuple[int, ...] = ()
    # §228's 2x cost-and-slippage rerun. An observation, not a derivation:
    # doubling slippage changes which fills happen, so it cannot be
    # re-derived from the base run's output and has to be executed.
    stress: dict[str, Any] | None = None
    # Warnings that do not make a run wrong -- an approximation the reader
    # should know about, not a refusal. Separate from `findings`, which
    # mean the run did not happen.
    notes: tuple[str, ...] = ()
    # The limits the caller chose, if any. `None` means the strategy's
    # own were used -- a distinction worth keeping, since one is a
    # decision about this run and the other is not.
    max_daily_loss: Decimal | None = None
    max_drawdown_pct: Decimal | None = None


def _refused(*findings: Finding, plan: BacktestPlan | None = None) -> BacktestVerdict:
    return BacktestVerdict(
        passed=False,
        report=ValidationReport(findings=findings),
        plan=plan,
        bars=None,
        outcome=None,
    )


def backtest(
    conn: Connection,
    strategy_id: int,
    *,
    start: date,
    end: date,
    limits: SandboxLimits | None = None,
    starting_cash: Decimal | None = None,
    max_daily_loss: Decimal | None = None,
    max_drawdown_pct: Decimal | None = None,
) -> BacktestVerdict:
    """Run a registered strategy over an operator-chosen window.

    Runs the **registered source**, read from the row, rather than anything
    the caller supplied: the registry's rule is that a version is immutable
    "because results already attributed to that version must keep describing
    the code that produced them", and a backtest result is exactly such an
    attribution. A backtest therefore cannot execute code that never passed
    stage 1.

    The window is the caller's, not the manifest's -- a strategy declares
    what data it needs, an operator decides what period to ask about.

    Raises `KeyError` if `strategy_id` does not exist; the route turns that
    into a 404.
    """
    record = get_strategy(conn, strategy_id)
    resolved = SandboxLimits.for_backtest(_resolve_limits(limits))

    manifest = record.manifest
    if manifest is None:
        # Registered before the manifest was persisted. Recover it the only
        # honest way -- by asking the strategy, in the sandbox -- rather than
        # assuming a default universe on its behalf.
        configured = run_strategy_in_sandbox(record.source, resolved)
        if not configured.ok or configured.manifest is None:
            return _refused(
                Finding(
                    code="MANIFEST_UNRESOLVABLE",
                    message=(
                        "this version stores no manifest and configure() did not return "
                        f"a usable one: {(configured.error or 'no manifest').strip()[:400]}"
                    ),
                    contract_section="§3",
                )
            )
        manifest = configured.manifest

    try:
        interval_sec = resolve_bar_interval(manifest)
    except _InvalidBarInterval as invalid:
        return _refused(
            Finding(code="MANIFEST_UNRESOLVABLE", message=str(invalid), contract_section="§3")
        )

    if interval_sec != 86400:
        # Scope, stated rather than approximated. `bars_daily` holds 51M rows
        # across 585,266 instruments; `bars_intraday` holds 2.2M across 30.
        # A backtest at scale is a daily one, and serving this request from
        # the intraday table would silently be a different, far narrower
        # experiment than the caller asked for.
        return _refused(
            Finding(
                code="BACKTEST_INTERVAL_UNSUPPORTED",
                message=(
                    f"this strategy declares data.bars={manifest.get('data', {}).get('bars')!r}; "
                    'backtests currently run on daily bars only ("1d"). Multi-year intraday '
                    "does not fit one payload and chunked delivery is not built yet."
                ),
                contract_section="§3",
            )
        )

    try:
        # `as_of` is the window's END, matching D3a-2's fixed factor set: one
        # point-in-time universe for the whole run, not one that drifts.
        instrument_ids = resolve_universe(conn, manifest, end)
    except _UnresolvedUniverse as unresolved:
        return _refused(
            Finding(code="MANIFEST_UNRESOLVABLE", message=str(unresolved), contract_section="§3")
        )

    plan = plan_backtest(conn, manifest, instrument_ids, start=start, end=end, limits=resolved)
    if plan.findings:
        # Refused on the estimate, before a single bar was materialised.
        return _refused(*plan.findings, plan=plan)

    window: dict[str, Any] = {
        "start": plan.start.isoformat(),
        "end": plan.end.isoformat(),
        "sessions": plan.sessions,
        "bars": "1d",
        "interval_sec": interval_sec,
        "instruments": {},
    }
    bars = (
        fetch_bars(conn, instrument_ids, window, interval_sec=interval_sec)
        if instrument_ids
        else {}
    )
    if not bars:
        return _refused(
            Finding(
                code="NO_DATA",
                message=(
                    f"the manifest's universe resolved to {len(instrument_ids)} instrument(s) "
                    f"and no daily bars exist between {start.isoformat()} and {end.isoformat()}."
                ),
                contract_section="§3",
            ),
            plan=plan,
        )

    try:
        broker, exchange, asset_class = _charge_key(conn, instrument_ids)
    except _MixedUniverse as mixed:
        return _refused(
            Finding(code="MANIFEST_UNRESOLVABLE", message=str(mixed), contract_section="§3"),
            plan=plan,
        )

    product = product_for_asset_class(asset_class)
    earliest = earliest_schedule_date(conn, broker, exchange, asset_class, product)
    priced_on, charge_note = charge_lookup_date(end, earliest)
    schedules = load_schedules(conn, broker, exchange, asset_class, product, priced_on)
    payload = SmokePayload(
        mode=MODE_SMOKE,
        source=record.source,
        contract_version=CONTRACT_VERSION,
        window=window,
        bars=bars,
        charge_schedules=tuple(schedules),
        # The caller's capital when given, else what the strategy declared.
        # An operator asking "what would this have done with 50,000?" is
        # asking a different question from the one the manifest answers, and
        # editing the strategy to ask it would create a new version whose
        # results are attributed separately.
        starting_cash=(
            Decimal(str(manifest.get("capital", "0"))) if starting_cash is None else starting_cash
        ),
        slippage_bps=Decimal("0"),
        # Warm-up bars were fetched above; dispatch still begins where the
        # caller asked.
        dispatch_from=plan.dispatch_from,
        max_daily_loss=max_daily_loss,
        max_drawdown_pct=max_drawdown_pct,
    )
    # Once, not twice: determinism was proved at upload by stage 2's
    # double run, and re-proving it here would double the cost of every
    # backtest to re-answer a settled question about the same source.
    result = run_smoke_in_sandbox(payload, resolved)
    outcome = _outcome_of(result)
    passed = bool(outcome.get("ok"))

    # The stress pass, run only when the base run succeeded: stressing a run
    # that already crashed answers nothing, and costs a container to say so.
    stress: dict[str, Any] | None = None
    if passed:
        stressed = run_smoke_in_sandbox(
            SmokePayload(
                mode=MODE_SMOKE,
                source=record.source,
                contract_version=CONTRACT_VERSION,
                window=window,
                bars=bars,
                charge_schedules=stress_schedules(schedules),
                starting_cash=payload.starting_cash,
                slippage_bps=payload.slippage_bps * STRESS_MULTIPLIER,
                dispatch_from=plan.dispatch_from,
                # The stress run must be constrained identically, or it is
                # measuring two changes at once.
                max_daily_loss=max_daily_loss,
                max_drawdown_pct=max_drawdown_pct,
            ),
            resolved,
        )
        stressed_outcome = _outcome_of(stressed)
        stress = {
            "multiplier": str(STRESS_MULTIPLIER),
            "ok": bool(stressed_outcome.get("ok")),
            "fills": stressed_outcome.get("fills"),
            "final_equity": stressed_outcome.get("final_equity"),
            "breaker_reason": stressed_outcome.get("breaker_reason"),
            "error": stressed_outcome.get("error"),
        }
    # A crashed run has to carry a reason. The failure lives in
    # `outcome["error"]`, and leaving it there produced the least
    # actionable thing this platform can say -- "refused", with an empty
    # findings list and no message anywhere -- which is exactly what an
    # operator saw the first time a real 1,647-session run died.
    findings: tuple[Finding, ...] = ()
    if not passed:
        findings = (
            Finding(
                code="BACKTEST_RUN_FAILED",
                message=str(
                    outcome.get("error") or "the run did not complete and reported no reason"
                )[:1000],
                contract_section="§9",
            ),
        )
    return BacktestVerdict(
        passed=passed,
        report=ValidationReport(findings=findings),
        plan=plan,
        bars="1d",
        outcome=outcome,
        runtime=result.runtime,
        kernel_isolated=result.kernel_isolated,
        instrument_ids=tuple(instrument_ids),
        stress=stress,
        notes=() if charge_note is None else (charge_note,),
        max_daily_loss=max_daily_loss,
        max_drawdown_pct=max_drawdown_pct,
    )
