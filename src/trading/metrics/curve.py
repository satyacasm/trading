"""Return, risk and drawdown metrics over an equity curve.

Pure functions: a curve in, numbers out. Nothing here reads a database,
writes anything, or knows what produced the curve. That is deliberate --
3f folds these over Monte Carlo reshuffles and the plan wants them over
live paper portfolios too, neither of which involves the agent contract.

**No float, anywhere.** `Decimal.sqrt()` exists and the cost is irrelevant
at a few thousand points. The reason is not precision for its own sake --
a Sharpe wrong in the fifteenth decimal changes nothing -- it is that
returns are derived from money. Once a float enters the chain, the
boundary between "money, which must be exact" and "statistics, where error
is harmless" is enforced by nothing but attention, and this codebase's
history is six quantization defects found by review rather than by tests.
One rule with no carve-out is cheaper to hold than a boundary.

**An undefined metric is `None`, never `0`.** Sharpe with zero volatility,
CAGR over a zero-day window, a drawdown that never happened: reporting
zero for any of these would be a claim the data does not support.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

__all__ = [
    "CurvePoint",
    "cagr",
    "period_returns",
    "periods_per_year",
    "sharpe",
    "sortino",
    "total_return",
    "value_at_risk",
    "worst_period",
]

CurvePoint = tuple[datetime, Decimal, Decimal]

# NSE trades ~252 sessions a year. Keyed by the interval the run was served,
# so the factor cannot be silently wrong when a different one is in play.
_PERIODS_PER_YEAR: dict[str, int] = {"1d": 252}

_DAYS_PER_YEAR = Decimal("365")


def periods_per_year(bars: str | None) -> int | None:
    """Periods in a year for the served interval, or `None` if unknown.

    `None` rather than a default: annualizing by a guessed factor produces
    a number that looks like a Sharpe and is not one.
    """
    return _PERIODS_PER_YEAR.get(bars or "")


def period_returns(points: list[CurvePoint]) -> list[Decimal]:
    """`r_t = E_t / E_{t-1} - 1` over consecutive points.

    A zero previous equity contributes no return rather than raising: a
    portfolio at zero equity has no meaningful return to report, and a
    division error here would lose every other metric with it.
    """
    out: list[Decimal] = []
    for (_, prev, _pc), (_, cur, _cc) in zip(points, points[1:], strict=False):
        if prev == 0:
            continue
        out.append(cur / prev - 1)
    return out


def total_return(points: list[CurvePoint]) -> Decimal | None:
    """`E_last / E_first - 1`."""
    if len(points) < 2 or points[0][1] == 0:
        return None
    return points[-1][1] / points[0][1] - 1


def cagr(points: list[CurvePoint]) -> Decimal | None:
    """Compounded over **calendar days**, not sessions.

    A year is a year regardless of how many times the exchange opened, and
    annualizing by session count would make a strategy's CAGR depend on the
    holiday calendar of the period it happened to run over.
    """
    if len(points) < 2 or points[0][1] <= 0 or points[-1][1] <= 0:
        return None
    days = Decimal((points[-1][0] - points[0][0]).days)
    if days <= 0:
        return None
    ratio = points[-1][1] / points[0][1]
    # `Decimal.__pow__` refuses a non-integer exponent, so compound through
    # ln/exp rather than dropping to float for the one operation that would
    # put a float back in the chain.
    return (ratio.ln() * (_DAYS_PER_YEAR / days)).exp() - 1


def _mean(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def volatility(returns: list[Decimal], periods: int) -> Decimal | None:
    """Sample standard deviation of period returns, annualized.

    The `n - 1` divisor, not `n`: these returns are a sample of the
    strategy's behaviour, not the entire population of it.
    """
    if len(returns) < 2:
        return None
    mean = _mean(returns)
    variance = sum(((r - mean) ** 2 for r in returns), Decimal(0)) / Decimal(len(returns) - 1)
    return variance.sqrt() * Decimal(periods).sqrt()


def _annualized_mean(returns: list[Decimal], periods: int) -> Decimal:
    return _mean(returns) * Decimal(periods)


def sharpe(returns: list[Decimal], periods: int, risk_free: Decimal) -> Decimal | None:
    """`(annualized mean return - risk_free) / annualized volatility`.

    `None` when volatility is zero: a flat curve has no measurable risk, so
    its risk-adjusted return is undefined rather than neutral.
    """
    vol = volatility(returns, periods)
    if vol is None or vol == 0:
        return None
    return (_annualized_mean(returns, periods) - risk_free) / vol


def downside_deviation(returns: list[Decimal], periods: int) -> Decimal | None:
    """Root-mean-square of the returns below zero, annualized.

    Divided by the count of ALL returns, not just the negative ones. That
    is what keeps Sortino comparable between a strategy that rarely loses
    and one that often does -- dividing by the negative count alone would
    reward frequent small losses.
    """
    if not returns:
        return None
    downside = [r for r in returns if r < 0]
    if not downside:
        return Decimal(0)
    mean_square = sum((r**2 for r in downside), Decimal(0)) / Decimal(len(returns))
    return mean_square.sqrt() * Decimal(periods).sqrt()


def sortino(returns: list[Decimal], periods: int, risk_free: Decimal) -> Decimal | None:
    """As Sharpe, but penalising only downside deviation."""
    dd = downside_deviation(returns, periods)
    if dd is None or dd == 0:
        return None
    return (_annualized_mean(returns, periods) - risk_free) / dd


def value_at_risk(returns: list[Decimal], quantile: Decimal) -> Decimal | None:
    """Historical VaR by nearest-rank on the sorted series.

    Not a parametric normal assumption: equity curves are not normal, and
    a historical quantile says only what actually happened.
    """
    if not returns:
        return None
    ordered = sorted(returns)
    rank = int((quantile * Decimal(len(ordered))).to_integral_value(rounding="ROUND_CEILING"))
    index = max(0, min(rank - 1, len(ordered) - 1))
    return ordered[index]


def worst_period(points: list[CurvePoint]) -> tuple[datetime, Decimal] | None:
    """The single worst period return, with the timestamp it ended on."""
    returns = period_returns(points)
    if not returns:
        return None
    worst = min(returns)
    return points[returns.index(worst) + 1][0], worst
