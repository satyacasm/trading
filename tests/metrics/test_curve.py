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


def test_a_monotonic_curve_has_no_drawdown() -> None:
    """None, not a zero-depth drawdown: a drawdown that never happened has
    no peak, no trough and no duration to report."""
    from trading.metrics.curve import drawdown

    assert drawdown(_curve(["100", "110", "120"])) is None


def test_a_v_shaped_curve_reports_an_exact_depth_and_duration() -> None:
    """100 -> 80 -> 100 on consecutive days. Depth -20%, and the drawdown
    runs from the peak at day 0 to the recovery at day 2: 2 sessions,
    2 calendar days."""
    from trading.metrics.curve import drawdown

    dd = drawdown(_curve(["100", "80", "100"]))
    assert dd is not None
    assert dd.depth == Decimal("-0.2")
    assert dd.recovered is True
    assert dd.sessions == 2
    assert dd.days == 2
    assert dd.peak_ts == datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
    assert dd.trough_ts == datetime(2024, 1, 2, 10, 0, tzinfo=UTC)
    assert dd.recovered_ts == datetime(2024, 1, 3, 10, 0, tzinfo=UTC)


def test_a_drawdown_that_never_recovers_says_so() -> None:
    """The case a naive implementation reports as recovered, and the single
    most misleading thing this module could do: an open drawdown presented
    as closed understates the risk still being carried.

    100 -> 90 -> 85 -> 88. Deepest is 85, i.e. -15%, and 88 never regains
    the 100 peak, so the span runs to the last point.
    """
    from trading.metrics.curve import drawdown

    dd = drawdown(_curve(["100", "90", "85", "88"]))
    assert dd is not None
    assert dd.depth == Decimal("-0.15")
    assert dd.recovered is False
    assert dd.recovered_ts is None
    assert dd.sessions == 3
    assert dd.days == 3


def test_the_deepest_drawdown_wins_not_the_longest() -> None:
    """Two separate drawdowns: a long shallow one and a short deep one.
    `max drawdown` is defined by depth, so the deep one must be reported --
    an implementation tracking the longest span would return the other."""
    from trading.metrics.curve import drawdown

    dd = drawdown(_curve(["100", "98", "97", "96", "101", "70", "101"]))
    assert dd is not None
    assert dd.depth.quantize(Decimal("0.0001")) == Decimal("-0.3069")


def test_calmar_is_cagr_over_the_drawdown_depth() -> None:
    from trading.metrics.curve import cagr, calmar, drawdown

    points = [
        (datetime(2023, 1, 1, 10, 0, tzinfo=UTC), Decimal("100"), Decimal("100")),
        (datetime(2023, 7, 1, 10, 0, tzinfo=UTC), Decimal("80"), Decimal("80")),
        (datetime(2024, 1, 1, 10, 0, tzinfo=UTC), Decimal("200"), Decimal("200")),
    ]
    c, dd = cagr(points), drawdown(points)
    assert c is not None and dd is not None
    got = calmar(points)
    assert got is not None
    assert got == c / abs(dd.depth)


def test_the_drawdown_curve_is_the_decline_from_the_running_peak() -> None:
    from trading.metrics.curve import drawdown_curve

    got = drawdown_curve(_curve(["100", "80", "100", "120"]))
    assert [d for _, d in got] == [
        Decimal("0"),
        Decimal("-0.2"),
        Decimal("0"),
        Decimal("0"),
    ]


def test_monthly_returns_compound_within_each_ist_calendar_month() -> None:
    """IST, not UTC -- the same convention the DP scrip-day key and the
    breaker's day rollover already use.

    The third point sits at 19:00 UTC on 31 January, which is 00:30 IST on
    1 February, so it belongs to February. Grouping by UTC would put it in
    January and silently move a day's P&L between months.
    """
    from trading.metrics.curve import monthly_returns

    points = [
        (datetime(2024, 1, 15, 10, 0, tzinfo=UTC), Decimal("100"), Decimal("100")),
        (datetime(2024, 1, 30, 10, 0, tzinfo=UTC), Decimal("110"), Decimal("110")),
        (datetime(2024, 1, 31, 19, 0, tzinfo=UTC), Decimal("121"), Decimal("121")),
    ]
    got = dict(monthly_returns(points))
    assert set(got) == {"2024-01", "2024-02"}
    assert got["2024-01"] == Decimal("0.1")
    assert got["2024-02"].quantize(Decimal("0.0001")) == Decimal("0.1000")


def test_rolling_sharpe_emits_nothing_until_its_window_is_full() -> None:
    """A Sharpe over eleven points is noise wearing the same name, so the
    series starts where the window does rather than being padded."""
    from trading.metrics.curve import rolling_sharpe

    points = _curve([str(100 + i) for i in range(10)])
    assert rolling_sharpe(points, "1d", Decimal("0"), window=126) == []
    got = rolling_sharpe(points, "1d", Decimal("0"), window=5)
    # 9 returns, window 5 -> 5 windows.
    assert len(got) == 5


def test_rolling_sharpe_is_undefined_without_a_known_interval() -> None:
    """Annualizing by a guessed factor produces a number that looks like a
    Sharpe and is not one."""
    from trading.metrics.curve import rolling_sharpe

    assert rolling_sharpe(_curve(["100", "110", "120"]), None, Decimal("0"), window=2) == []
