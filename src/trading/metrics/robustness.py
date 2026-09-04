"""How much of a backtest's result was the order the trades arrived in.

A backtest reports one path. The reshuffle asks a narrow, answerable
question about it: had the same trades occurred in a different order, how
bad would the worst stretch have been?

**What it destroys, said plainly.** Reshuffling removes serial correlation.
A strategy whose losses genuinely cluster -- a trend follower in a choppy
market -- looks better reshuffled than it was, because the clustering is
real information the shuffle throws away. The distribution is not a
neutral fact about the strategy; it is the answer to one question, and the
report says so rather than implying more.

**Seeded**, because determinism is a rule this platform enforces on
strategies and a metrics layer that reported different figures on every
page load would be indefensible.

Pure: fills in, percentiles out. No database, no float.
"""

from __future__ import annotations

import random
from decimal import Decimal
from typing import Any

__all__ = ["ITERATIONS", "max_drawdown_of", "reshuffle"]

ITERATIONS = 1000

# Fixed, so the same stored run always reports the same distribution. The
# value is arbitrary; that it never changes is not.
_SEED = 20260904

_MONEY = Decimal("0.0001")


def max_drawdown_of(pnls: list[Decimal], starting_equity: Decimal) -> Decimal:
    """The worst peak-to-trough decline of the equity path these P&Ls trace,
    as a non-positive ratio."""
    equity = starting_equity
    peak = starting_equity
    worst = Decimal(0)
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            worst = min(worst, equity / peak - 1)
    return worst


def _percentile(ordered: list[Decimal], fraction: Decimal) -> Decimal:
    """Nearest-rank, matching `curve.value_at_risk`. Not interpolated: an
    interpolated percentile reports a value no ordering actually produced."""
    rank = int((fraction * Decimal(len(ordered))).to_integral_value(rounding="ROUND_CEILING"))
    return ordered[max(0, min(rank - 1, len(ordered) - 1))]


def reshuffle(
    pnls: list[Decimal], *, starting_equity: Decimal, iterations: int = ITERATIONS
) -> dict[str, Any] | None:
    """The distribution of outcomes across `iterations` orderings.

    `None` when there are no closed trades: a distribution over nothing is
    not an empty distribution, it is the absence of one.

    Terminal equity is reported even though addition is commutative and it
    cannot vary. That is deliberate -- seeing p5 and p95 coincide is what
    tells a reader the *drawdown* spread is about path and not about
    outcome, which is the distinction the whole check rests on.
    """
    if not pnls:
        return None

    rng = random.Random(_SEED)  # noqa: S311 - not cryptographic; reproducibility is the point
    order = list(pnls)
    terminals: list[Decimal] = []
    drawdowns: list[Decimal] = []
    for _ in range(iterations):
        rng.shuffle(order)
        terminals.append(starting_equity + sum(order, Decimal(0)))
        drawdowns.append(max_drawdown_of(order, starting_equity))

    terminals.sort()
    drawdowns.sort()
    return {
        "iterations": iterations,
        "terminal_equity": {
            "p5": str(_percentile(terminals, Decimal("0.05")).quantize(_MONEY)),
            "p50": str(_percentile(terminals, Decimal("0.50")).quantize(_MONEY)),
            "p95": str(_percentile(terminals, Decimal("0.95")).quantize(_MONEY)),
        },
        "max_drawdown": {
            # Sorted ascending, so p5 is the DEEPEST drawdown -- the bad
            # tail, which §228 wants shown as prominently as the middle.
            "p5": str(_percentile(drawdowns, Decimal("0.05")).quantize(_MONEY)),
            "p50": str(_percentile(drawdowns, Decimal("0.50")).quantize(_MONEY)),
            "p95": str(_percentile(drawdowns, Decimal("0.95")).quantize(_MONEY)),
        },
    }
