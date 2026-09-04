"""Metrics over an equity curve, against answers computed by hand.

Every fixture here has an answer checkable with a pencil, which is the
point: a metrics module tested only against its own output tests nothing.
The mutation checks in the plan's C5 exist for the same reason.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal


def _curve(values, *, start=datetime(2024, 1, 1, 10, 0, tzinfo=UTC), step_days=1):  # noqa: ANN001, ANN202
    """A curve of equities on consecutive days, cash equal to equity."""
    return [
        (start + timedelta(days=i * step_days), Decimal(v), Decimal(v))
        for i, v in enumerate(values)
    ]


def test_period_returns_are_consecutive_ratios() -> None:
    from trading.metrics.curve import period_returns

    assert period_returns(_curve(["100", "110", "99"])) == [Decimal("0.1"), Decimal("-0.1")]


def test_a_flat_curve_has_zero_return_and_zero_volatility() -> None:
    from trading.metrics.curve import period_returns, total_return, volatility

    points = _curve(["100", "100", "100", "100"])
    assert total_return(points) == Decimal("0")
    assert volatility(period_returns(points), 252) == Decimal("0")


def test_sharpe_is_undefined_rather_than_a_division_error_on_a_flat_curve() -> None:
    """Zero volatility makes Sharpe genuinely undefined. Reporting 0 would
    claim a neutral risk-adjusted return where there is no measurable risk
    at all."""
    from trading.metrics.curve import period_returns, sharpe

    returns = period_returns(_curve(["100", "100", "100"]))
    assert sharpe(returns, 252, Decimal("0.065")) is None


def test_volatility_uses_the_sample_divisor() -> None:
    """Hand-computed. Returns [0.1, -0.1]; mean 0; sample variance
    (0.01 + 0.01) / (2 - 1) = 0.02; sd = 0.14142135...; annualized by
    sqrt(252) = 15.87450787... -> 2.2450...

    With the population divisor the sd would be 0.1 and the annualized
    figure 1.5875, so this test distinguishes the two.
    """
    from trading.metrics.curve import volatility

    got = volatility([Decimal("0.1"), Decimal("-0.1")], 252)
    assert got is not None
    assert got.quantize(Decimal("0.0001")) == Decimal("2.2450")


def test_cagr_compounds_over_calendar_days_not_sessions() -> None:
    """A year is a year regardless of how many times the exchange opened.
    100 -> 200 across exactly 365 days is a 100% CAGR.

    Note the span deliberately avoids 2024: it is a leap year, so
    2024-01-01 -> 2025-01-01 is 366 days and the correct answer would be
    2^(365/366) - 1 = 0.9962, not 1.0000. The first draft of this test used
    that span and the implementation was right to disagree with it.
    """
    from trading.metrics.curve import cagr

    points = [
        (datetime(2023, 1, 1, 10, 0, tzinfo=UTC), Decimal("100"), Decimal("100")),
        (datetime(2024, 1, 1, 10, 0, tzinfo=UTC), Decimal("200"), Decimal("200")),
    ]
    got = cagr(points)
    assert got is not None
    assert got.quantize(Decimal("0.0001")) == Decimal("1.0000")


def test_cagr_does_not_treat_a_leap_year_as_a_year() -> None:
    """The other half of the same point, pinned so nobody 'simplifies' the
    calendar-day arithmetic into a 365-day assumption."""
    from trading.metrics.curve import cagr

    got = cagr(
        [
            (datetime(2024, 1, 1, 10, 0, tzinfo=UTC), Decimal("100"), Decimal("100")),
            (datetime(2025, 1, 1, 10, 0, tzinfo=UTC), Decimal("200"), Decimal("200")),
        ]
    )
    assert got is not None
    assert got.quantize(Decimal("0.0001")) == Decimal("0.9962")


def test_value_at_risk_is_the_historical_fifth_percentile() -> None:
    """Nearest-rank on the sorted series, not a normal assumption: equity
    curves are not normal, and saying so costs nothing."""
    from trading.metrics.curve import value_at_risk

    returns = [Decimal(x) for x in ("-0.10", "-0.05", "0.00", "0.02", "0.03")]
    assert value_at_risk(returns, Decimal("0.05")) == Decimal("-0.10")


def test_periods_per_year_is_derived_from_the_served_interval() -> None:
    """Hardcoding 252 makes the factor silently wrong the day an intraday
    interval is served."""
    from trading.metrics.curve import periods_per_year

    assert periods_per_year("1d") == 252
    assert periods_per_year(None) is None
    assert periods_per_year("5m") is None


def test_sortino_penalises_only_downside_deviation() -> None:
    """Sharpe and Sortino must differ on a curve with asymmetric returns --
    if they agree, the downside filter is not being applied."""
    from trading.metrics.curve import period_returns, sharpe, sortino

    points = _curve(["100", "120", "115", "140", "138"])
    returns = period_returns(points)
    s = sharpe(returns, 252, Decimal("0"))
    so = sortino(returns, 252, Decimal("0"))
    assert s is not None and so is not None
    assert so > s
