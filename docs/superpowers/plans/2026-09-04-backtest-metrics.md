# Metrics over the stored equity curve — Implementation Plan (Phase 3, 3d)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a stored equity curve into return, risk and drawdown metrics.

**Architecture:** One pure module, `trading.metrics.curve`. Curve in, metrics
out. No tables, no writes, no migration. The API detail route folds it in.

**Tech Stack:** Python 3.12, `decimal` (no numpy), FastAPI, pytest.

**Spec:** `docs/superpowers/specs/2026-09-04-backtest-metrics-design.md`

## Global Constraints

- **C1. No float, anywhere in the chain.** `Decimal` throughout, including
  `Decimal.sqrt()`. Returns are derived from money; a carve-out for
  "statistics" is a boundary maintained by attention rather than by rule.
- **C2. Ratios cross the wire as strings**, like every other number in this
  API. JSON has no decimal type.
- **C3. An undefined metric is `None`, never a zero and never an exception.**
  Sharpe with zero volatility, CAGR over a zero-day window, a drawdown that
  never happened — each is genuinely undefined, and reporting 0 would be a
  claim.
- **C4. Annualization derives from the run's `bars`**, never hardcoded, so
  the day an intraday interval is served the factor is not silently wrong.
- **C5. Verify by mutation:** changing 252, and changing the `n-1` divisor to
  `n`, must each redden a test.
- **C6. Routes are `def`. GETs never write.**

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/trading/metrics/__init__.py` | package | 1 |
| `src/trading/metrics/curve.py` | every pure function over a curve | 1, 2, 3 |
| `src/trading/agent_contract/api.py` | `metrics` on the detail route | 4 |
| `tests/metrics/test_curve.py` | hand-computed fixtures | 1, 2, 3 |
| `tests/agent_contract/test_api.py` | the route carries metrics | 4 |

A new top-level `trading.metrics` package rather than a module under
`agent_contract`: 3f will compute these over Monte Carlo reshuffles and the
plan wants them over live paper portfolios too, neither of which involves
the agent contract. It depends on nothing but `decimal` and `datetime`.

---

### Task 1: Returns and risk

**Files:**
- Create: `src/trading/metrics/__init__.py`, `src/trading/metrics/curve.py`
- Test: `tests/metrics/test_curve.py`

**Interfaces:**
- Produces: `CurvePoint = tuple[datetime, Decimal, Decimal]` (ts, equity, cash);
  `periods_per_year(bars: str | None) -> int`;
  `period_returns(points) -> list[Decimal]`;
  `total_return(points) -> Decimal | None`;
  `cagr(points) -> Decimal | None`;
  `volatility(returns, periods) -> Decimal | None`;
  `sharpe(returns, periods, risk_free) -> Decimal | None`;
  `sortino(returns, periods, risk_free) -> Decimal | None`;
  `value_at_risk(returns, quantile) -> Decimal | None`;
  `worst_period(points) -> tuple[datetime, Decimal] | None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/metrics/test_curve.py`. Hand-computed answers, independent of
the implementation:

```python
"""Metrics over an equity curve, against answers computed by hand.

Every fixture here has an answer that can be checked with a pencil, which
is the point: a metrics module tested only against its own output tests
nothing. The mutation checks in the plan's C5 exist for the same reason.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest


def _curve(values, *, start=datetime(2024, 1, 1, 10, 0, tzinfo=UTC), step_days=1):
    """A curve of equities on consecutive days, cash equal to equity."""
    return [
        (start + timedelta(days=i * step_days), Decimal(v), Decimal(v))
        for i, v in enumerate(values)
    ]


def test_period_returns_are_consecutive_ratios() -> None:
    from trading.metrics.curve import period_returns

    assert period_returns(_curve(["100", "110", "99"])) == [
        Decimal("0.1"),
        Decimal("-0.1"),
    ]


def test_a_flat_curve_has_zero_return_and_zero_volatility() -> None:
    from trading.metrics.curve import period_returns, total_return, volatility

    points = _curve(["100", "100", "100", "100"])
    assert total_return(points) == Decimal("0")
    assert volatility(period_returns(points), 252) == Decimal("0")


def test_sharpe_is_undefined_rather_than_a_division_error_on_a_flat_curve() -> None:
    """C3: zero volatility makes Sharpe genuinely undefined. Reporting 0
    would claim the strategy had a neutral risk-adjusted return when it in
    fact has no measurable risk at all."""
    from trading.metrics.curve import period_returns, sharpe

    returns = period_returns(_curve(["100", "100", "100"]))
    assert sharpe(returns, 252, Decimal("0.065")) is None


def test_volatility_uses_the_sample_divisor() -> None:
    """Hand-computed. Returns [0.1, -0.1]; mean 0; sample variance
    ((0.1)^2 + (0.1)^2) / (2-1) = 0.02; sd = sqrt(0.02) = 0.1414213562...;
    annualized by sqrt(252) = 15.8745078664... -> 2.2450...

    With the population divisor (n) it would be sqrt(0.01) = 0.1 and the
    annualized figure 1.5874..., so this test distinguishes them.
    """
    from trading.metrics.curve import volatility

    got = volatility([Decimal("0.1"), Decimal("-0.1")], 252)
    assert got is not None
    assert got.quantize(Decimal("0.0001")) == Decimal("2.2450")


def test_cagr_compounds_over_calendar_days_not_sessions() -> None:
    """A year is a year regardless of how many times the exchange opened.
    100 -> 200 across exactly 365 days is a 100% CAGR."""
    from trading.metrics.curve import cagr

    points = [
        (datetime(2024, 1, 1, 10, 0, tzinfo=UTC), Decimal("100"), Decimal("100")),
        (datetime(2025, 1, 1, 10, 0, tzinfo=UTC), Decimal("200"), Decimal("200")),
    ]
    got = cagr(points)
    assert got is not None
    assert got.quantize(Decimal("0.0001")) == Decimal("1.0000")


def test_value_at_risk_is_the_historical_fifth_percentile() -> None:
    """Nearest-rank on the sorted series, not a normal assumption: equity
    curves are not normal and saying so costs nothing."""
    from trading.metrics.curve import value_at_risk

    returns = [Decimal(str(x)) for x in ("-0.10", "-0.05", "0.00", "0.02", "0.03")]
    assert value_at_risk(returns, Decimal("0.05")) == Decimal("-0.10")


def test_periods_per_year_is_derived_from_the_served_interval() -> None:
    """C4. Hardcoding 252 makes the factor silently wrong the day an
    intraday interval is served."""
    from trading.metrics.curve import periods_per_year

    assert periods_per_year("1d") == 252
    assert periods_per_year(None) is None
```

- [ ] **Step 2: Run them and watch them fail**

Run: `uv run pytest tests/metrics/test_curve.py -v`
Expected: FAIL — `ModuleNotFoundError: trading.metrics`.

- [ ] **Step 3: Write the module**

`src/trading/metrics/__init__.py` is empty. `src/trading/metrics/curve.py`:

```python
"""Return, risk and drawdown metrics over an equity curve.

Pure functions: a curve in, numbers out. Nothing here reads a database,
writes anything, or knows what produced the curve -- 3f will fold these
over Monte Carlo reshuffles and the plan wants them over live paper
portfolios, neither of which involves the agent contract.

**No float, anywhere.** `Decimal.sqrt()` exists and the cost is irrelevant
at a few thousand points. The reason is not precision for its own sake --
a Sharpe wrong in the fifteenth decimal changes nothing -- it is that
returns are derived from money, and once a float enters the chain the
boundary between "money, which must be exact" and "statistics, where error
is harmless" is enforced by nothing but attention.

An undefined metric is `None`, never `0`. Sharpe with zero volatility,
CAGR over a zero-day window, a drawdown that never happened: reporting
zero for any of these would be a claim the data does not support.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

CurvePoint = tuple[datetime, Decimal, Decimal]

# NSE trades ~252 sessions a year. Keyed by the interval the run was served
# so the factor cannot be silently wrong when another one is.
_PERIODS_PER_YEAR: dict[str, int] = {"1d": 252}

_DAYS_PER_YEAR = Decimal("365")


def periods_per_year(bars: str | None) -> int | None:
    """Periods in a year for the served interval, or None if unknown.

    None rather than a default: annualizing by a guessed factor produces a
    number that looks like a Sharpe and is not one.
    """
    return _PERIODS_PER_YEAR.get(bars or "")


def period_returns(points: list[CurvePoint]) -> list[Decimal]:
    """`r_t = E_t / E_{t-1} - 1` over consecutive points.

    A zero previous equity yields no return rather than a division error:
    a portfolio at zero equity has no meaningful return to report.
    """
    out: list[Decimal] = []
    for (_, prev, _c1), (_, cur, _c2) in zip(points, points[1:], strict=False):
        if prev == 0:
            continue
        out.append(cur / prev - 1)
    return out
```

...continuing with `total_return`, `cagr`, `volatility`, `sharpe`,
`sortino`, `value_at_risk`, `worst_period` exactly as D3d-3 defines them.
`cagr` uses `Decimal` exponentiation via `exp`/`ln`:
`(ratio ** (365/days))` is written `(ratio.ln() * (_DAYS_PER_YEAR / days)).exp() - 1`
because `Decimal.__pow__` refuses a non-integer exponent.

- [ ] **Step 4: Run them and watch them pass**

- [ ] **Step 5: Mutation checks (C5)**

Change `252` to `250` in `_PERIODS_PER_YEAR` — `test_volatility_uses_the_sample_divisor`
must redden. Change the variance divisor from `n - 1` to `n` — the same test
must redden. Restore both.

- [ ] **Step 6: Commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
git add -A && git commit -m "feat(metrics): return and risk metrics over an equity curve"
```

---

### Task 2: Drawdown, depth and duration

**Files:**
- Modify: `src/trading/metrics/curve.py`
- Test: `tests/metrics/test_curve.py`

**Interfaces:**
- Produces: `drawdown(points) -> Drawdown | None`, a frozen dataclass with
  `depth: Decimal` (non-positive), `peak_ts`, `trough_ts`,
  `recovered_ts: datetime | None`, `sessions: int`, `days: int`,
  `recovered: bool`; and `calmar(points, bars) -> Decimal | None`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_monotonic_curve_has_no_drawdown() -> None:
    from trading.metrics.curve import drawdown

    assert drawdown(_curve(["100", "110", "120"])) is None


def test_a_v_shaped_curve_reports_an_exact_depth_and_duration() -> None:
    """100 -> 80 -> 100 on consecutive days. Depth -20%; the drawdown runs
    from the peak at day 0 to the recovery at day 2: 2 sessions, 2 calendar
    days."""
    from trading.metrics.curve import drawdown

    dd = drawdown(_curve(["100", "80", "100"]))
    assert dd is not None
    assert dd.depth == Decimal("-0.2")
    assert dd.recovered is True
    assert dd.sessions == 2
    assert dd.days == 2


def test_a_drawdown_that_never_recovers_says_so(...)-> None:
    """The case a naive implementation reports as recovered, which is the
    single most misleading thing this module could do: an open drawdown
    presented as a closed one understates the risk still being carried."""
    from trading.metrics.curve import drawdown

    dd = drawdown(_curve(["100", "90", "85", "88"]))
    assert dd is not None
    assert dd.recovered is False
    assert dd.recovered_ts is None
    assert dd.depth == Decimal("-0.15")
    # Runs to the last point, not to the trough.
    assert dd.sessions == 3
```

- [ ] **Step 2: Run, watch fail, implement, watch pass**

Track a running peak; the deepest trough defines the drawdown; the recovery
is the first later point that reaches the peak again. When none does,
`recovered=False`, `recovered_ts=None`, and the span runs to the final point.

- [ ] **Step 3: Commit**

---

### Task 3: The series — drawdown curve, monthly returns, rolling Sharpe

**Files:**
- Modify: `src/trading/metrics/curve.py`
- Test: `tests/metrics/test_curve.py`

**Interfaces:**
- Produces: `drawdown_curve(points) -> list[tuple[datetime, Decimal]]`;
  `monthly_returns(points) -> list[tuple[str, Decimal]]` keyed `"YYYY-MM"` in
  **IST**; `rolling_sharpe(points, bars, risk_free, window=126) -> list[tuple[datetime, Decimal]]`.

- [ ] **Step 1: Write the failing tests**

Including: monthly grouping uses IST, so a point at `2024-01-31T19:00Z`
(00:30 IST on 1 Feb) belongs to **February** — the same timezone convention
the DP scrip-day key and the breaker's day rollover already use. And:
rolling Sharpe emits nothing until the window is full, rather than padding,
because a Sharpe over eleven points is noise wearing the same name.

- [ ] **Step 2: Run, watch fail, implement, watch pass**

- [ ] **Step 3: Commit**

---

### Task 4: Metrics on the detail route

**Files:**
- Modify: `src/trading/agent_contract/api.py`
- Test: `tests/agent_contract/test_api.py`

**Interfaces:**
- Produces: `BacktestDetail.metrics: dict[str, Any] | None`, and an optional
  `risk_free` query parameter (default `0.065`) echoed back inside it.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_detail_route_carries_metrics_and_echoes_the_risk_free_rate(...):
    """rf is echoed because on an Indian platform assuming 0 is a
    systematically flattering lie: a 6% strategy reads as respectable and is
    worse than a G-Sec. A reader who never thought about it must be told
    what was assumed rather than left to infer zero."""
    detail = client.get(f"/backtests/{run_id}")
    m = detail.json()["metrics"]
    assert m["risk_free"] == "0.065"
    assert m["total_return"] is not None
    assert isinstance(m["total_return"], str)     # C2

    override = client.get(f"/backtests/{run_id}?risk_free=0")
    assert override.json()["metrics"]["risk_free"] == "0"
    assert override.json()["metrics"]["sharpe"] != m["sharpe"]


def test_the_list_route_still_carries_no_metrics(...):
    """D3d-5: computing metrics for 50 runs means loading 50 curves, which
    is the cost two routes exist to avoid."""
    assert "metrics" not in client.get(f"/strategies/{sid}/backtests").json()[0]
```

- [ ] **Step 2: Run, watch fail, implement, watch pass**

- [ ] **Step 3: Full verification and commit**

```bash
uv run pytest tests/ -q -m "not golden and not sandbox and not live"
uv run pytest tests/ -q -m sandbox
uv run ruff check src tests && uv run ruff format src tests && uv run mypy src
```

---

## Verification before calling 3d done

- [ ] Fast + sandbox suites green; ruff, format, mypy clean
- [ ] Both C5 mutations performed and restored
- [ ] **Metrics computed against the real stored run**, not only fixtures:
      `GET /backtests/2` (the 1,647-point RELIANCE run) returns a plausible
      CAGR for a 6.6-year buy-and-hold that ended +5.6%, a drawdown whose
      depth matches the curve's visible worst stretch, and a Sharpe that
      moves when `risk_free` changes
- [ ] `docs/STATUS.md` updated: 3d shipped, 3e next
