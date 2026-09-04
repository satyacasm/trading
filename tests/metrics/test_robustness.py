"""The reshuffle distribution, against properties that can be reasoned about."""

from __future__ import annotations

from decimal import Decimal


def _pnls(values):  # noqa: ANN001, ANN202
    return [Decimal(str(v)) for v in values]


def test_the_reshuffle_is_deterministic() -> None:
    """Unseeded, the same stored backtest would report different robustness
    figures on every page load -- and an unseeded shuffle passes every other
    test in this file. Determinism is a rule this platform enforces on
    strategies; a metrics layer that broke it would be indefensible.
    """
    from trading.metrics.robustness import reshuffle

    # Thirty distinct values, not six. With six there are only 720
    # orderings and 1,000 samples saturates them, so the percentiles
    # converge to the same numbers whatever the seed -- the first version of
    # this test passed with the seed removed, which is no test at all.
    # 30! orderings sampled 1,000 times is nowhere near saturation, so an
    # unseeded shuffle genuinely diverges.
    fills = _pnls([(-1) ** i * (i * 37 % 211) for i in range(30)])
    first = reshuffle(fills, starting_equity=Decimal("100000"))
    second = reshuffle(fills, starting_equity=Decimal("100000"))
    assert first == second


def test_a_single_trade_has_no_dispersion() -> None:
    """The degenerate case a percentile implementation gets wrong: with one
    trade there is only one ordering, so every percentile coincides."""
    from trading.metrics.robustness import reshuffle

    result = reshuffle(_pnls([250]), starting_equity=Decimal("100000"))
    assert result["terminal_equity"]["p5"] == result["terminal_equity"]["p95"]
    assert result["terminal_equity"]["p50"] == "100250.0000"


def test_terminal_equity_is_identical_in_every_ordering() -> None:
    """Addition is commutative, so reshuffling cannot change where the
    equity ends -- only the path it took. A distribution of terminal
    equities that varied would mean the accumulation is wrong.
    """
    from trading.metrics.robustness import reshuffle

    result = reshuffle(_pnls([100, -50, 200, -30]), starting_equity=Decimal("1000"))
    assert result["terminal_equity"]["p5"] == result["terminal_equity"]["p95"] == "1220.0000"


def test_ordering_changes_the_drawdown_which_is_the_whole_point() -> None:
    """Losses that arrive together dig a deeper hole than the same losses
    spread out. The actual order here front-loads every loss, so its
    drawdown is worse than the reshuffled median -- which is exactly the
    question the check answers: how much of this drawdown was the order the
    trades happened to arrive in?
    """
    from trading.metrics.robustness import max_drawdown_of, reshuffle

    clustered = _pnls([-100, -100, -100, 50, 50, 50, 50, 50, 50])
    actual = max_drawdown_of(clustered, Decimal("1000"))
    result = reshuffle(clustered, starting_equity=Decimal("1000"))
    # The realised path is worse than the typical shuffled one.
    assert actual < Decimal(result["max_drawdown"]["p50"])


def test_no_trades_yields_no_distribution() -> None:
    from trading.metrics.robustness import reshuffle

    assert reshuffle([], starting_equity=Decimal("1000")) is None
