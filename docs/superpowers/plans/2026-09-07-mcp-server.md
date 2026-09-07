# MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the existing trading platform to an external agent as an MCP server, so the agent can read market state, compute indicators, run backtests, and place orders itself.

**Architecture:** Two new packages. `src/trading/indicators/` holds pure `Decimal` technical-indicator functions shared by MCP and (later) strategy scripts. `src/trading/mcp/` holds an MCP server whose tools call the existing FastAPI gateway over async httpx — never the database — so every invariant keeps exactly one enforcement path. One shared `tools.py` is served over both stdio and streamable HTTP.

**Tech Stack:** Python 3.12, `mcp>=2.1`, httpx, pydantic v2, pytest, Hypothesis, psycopg 3 (existing routes only).

**Spec:** `docs/superpowers/specs/2026-09-07-mcp-server-design.md` — read it before starting. The plan argues from the spec; where they disagree, the spec wins and the plan is wrong.

## Global Constraints

- **Python 3.12 exactly** (`requires-python = "==3.12.*"`).
- **`from __future__ import annotations` at the top of every new module.** Every existing module does this.
- **mypy strict passes.** `[tool.mypy] strict = true` covers `src/`. No `Any` returns, every function annotated.
- **ruff, line-length 100**, rules `E, F, I, UP, B, SIM`.
- **The MCP layer never opens a database connection.** No `psycopg` import anywhere under `src/trading/mcp/`. Every read and write goes through an HTTP route.
- **Money crosses the MCP boundary as strings**, never floats. JSON has no decimal type.
- **`portfolio_id` is never an MCP tool parameter.** It comes from the session.
- **Indicators are `Decimal` in, `Decimal` out.** No `float` anywhere in `src/trading/indicators/`.
- **An indicator returns `None` rather than a guess** when it has too little data or an undefined result. This matches `metrics.curve.periods_per_year`: *"`None` rather than a default: annualizing by a guessed factor produces a number that looks like a Sharpe and is not one."*
- **Tests that need Postgres carry `pytestmark = pytest.mark.db`** and use the `db_conn` fixture from `tests/conftest.py`, which always rolls back.
- **Commit after every task.** Message body explains *why*, following the existing log's style.

---

## File Structure

**Created:**

| File | Responsibility |
|---|---|
| `src/trading/indicators/__init__.py` | Token parsing (`"rsi14"`), the catalogue, and `compute()` dispatch |
| `src/trading/indicators/warmup.py` | How many extra bars an indicator needs to converge |
| `src/trading/indicators/trend.py` | `sma`, `ema`, `ema_series`, `macd`, `adx` |
| `src/trading/indicators/momentum.py` | `rsi` (Wilder), `roc`, `stochastic_k` |
| `src/trading/indicators/volatility.py` | `true_ranges`, `atr`, `bollinger`, `realised_vol` |
| `src/trading/indicators/levels.py` | `pct_from_high`, `pct_from_low`, `range_position` |
| `src/trading/streaming/perp_reference_api.py` | `GET /perp-context/{instrument_id}` |
| `src/trading/mcp/__init__.py` | Package marker |
| `src/trading/mcp/session.py` | Token to `AgentSession(portfolio_id)`; the only source of portfolio scope |
| `src/trading/mcp/client.py` | `GatewayClient`, async httpx over the gateway |
| `src/trading/mcp/formatting.py` | `money`, freshness envelope, refusal shaping |
| `src/trading/mcp/tools.py` | Every tool, transport-agnostic |
| `src/trading/mcp/serve_stdio.py` | stdio entrypoint; scope from config |
| `src/trading/mcp/serve_http.py` | Streamable HTTP entrypoint; scope from bearer token |

**Modified:**

| File | Change |
|---|---|
| `pyproject.toml` | Add `mcp>=2.1` dependency |
| `src/trading/streaming/market_data_api.py:95-207` | Add `precision` query parameter to `/candles/{instrument_id}` |
| `src/trading/streaming/gateway.py:86-89` | Include the perp reference router |

---

## Task 1: Indicators package, `sma`, `ema`, warmup

**Files:**
- Create: `src/trading/indicators/__init__.py`
- Create: `src/trading/indicators/trend.py`
- Create: `src/trading/indicators/warmup.py`
- Test: `tests/indicators/__init__.py`, `tests/indicators/test_trend.py`, `tests/indicators/test_warmup.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `trend.sma(values: Sequence[Decimal], period: int) -> Decimal | None`
  - `trend.ema(values: Sequence[Decimal], period: int) -> Decimal | None`
  - `trend.ema_series(values: Sequence[Decimal], period: int) -> list[Decimal]`
  - `warmup.warmup_bars_for(period: int) -> int`
  - `warmup.warmup_bars_for_all(periods: Iterable[int]) -> int`

- [ ] **Step 1: Write the failing tests**

Create `tests/indicators/__init__.py` as an empty file, then `tests/indicators/test_trend.py`:

```python
from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.trend import ema, ema_series, sma

_prices = st.lists(
    st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=2),
    min_size=1,
    max_size=60,
)


def test_sma_returns_none_when_there_are_fewer_bars_than_the_period() -> None:
    assert sma([Decimal(1), Decimal(2)], 3) is None


def test_sma_averages_only_the_last_period_bars() -> None:
    # The leading 100 must not be counted: mean(2, 4, 6) == 4.
    values = [Decimal(100), Decimal(2), Decimal(4), Decimal(6)]
    assert sma(values, 3) == Decimal(4)


def test_sma_rejects_a_non_positive_period() -> None:
    with pytest.raises(ValueError):
        sma([Decimal(1)], 0)


@given(_prices, st.integers(min_value=1, max_value=20))
def test_sma_of_a_constant_series_is_that_constant(
    values: list[Decimal], period: int
) -> None:
    constant = values[0]
    series = [constant] * max(len(values), period)
    assert sma(series, period) == constant


def test_ema_seeds_with_the_sma_so_the_first_value_equals_it() -> None:
    # With exactly `period` bars there is nothing to smooth yet, so the
    # only EMA value is the seed.
    values = [Decimal(2), Decimal(4), Decimal(6)]
    assert ema(values, 3) == Decimal(4)


def test_ema_weights_the_newest_bar_by_alpha() -> None:
    # period 3 -> alpha = 2/4 = 0.5. Seed = mean(2, 4, 6) = 4.
    # Next bar 10 -> 0.5*10 + 0.5*4 = 7.
    values = [Decimal(2), Decimal(4), Decimal(6), Decimal(10)]
    assert ema(values, 3) == Decimal(7)


def test_ema_series_has_one_value_per_bar_after_the_seed() -> None:
    values = [Decimal(i) for i in range(1, 11)]
    assert len(ema_series(values, 4)) == len(values) - 4 + 1


def test_ema_returns_none_when_there_are_fewer_bars_than_the_period() -> None:
    assert ema([Decimal(1)], 5) is None


@given(_prices)
def test_ema_never_leaves_the_range_of_its_inputs(values: list[Decimal]) -> None:
    # A weighted average of the series cannot escape the series' bounds.
    result = ema(values, 3)
    if result is not None:
        assert min(values) <= result <= max(values)
```

And `tests/indicators/test_warmup.py`:

```python
from __future__ import annotations

from trading.indicators.warmup import warmup_bars_for, warmup_bars_for_all


def test_warmup_is_never_less_than_fifty_bars() -> None:
    # Wilder smoothing converges slowly; a short period still needs a
    # meaningful run-up before its output is stable.
    assert warmup_bars_for(2) == 50


def test_warmup_scales_with_the_period_once_it_exceeds_the_floor() -> None:
    assert warmup_bars_for(14) == 70


def test_warmup_for_several_indicators_takes_the_largest() -> None:
    assert warmup_bars_for_all([2, 14, 26]) == 130


def test_warmup_for_no_indicators_is_zero() -> None:
    assert warmup_bars_for_all([]) == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/indicators -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.indicators'`

- [ ] **Step 3: Write the implementation**

Create `src/trading/indicators/__init__.py` containing only a docstring for now:

```python
"""Technical indicators over bar series.

`Decimal` throughout, and shared rather than private to the MCP layer on
purpose: a strategy script and the live agent must compute RSI with the
same code. If they diverged, every lesson carried from a backtest into a
live decision would be measuring a subtly different thing, invisibly in
both places.
"""

from __future__ import annotations
```

Create `src/trading/indicators/warmup.py`:

```python
"""How much run-up an indicator needs before its output means anything.

Wilder smoothing has no closed form: each value depends on the one
before it, so a 14-period RSI computed from 15 bars is simply not the
number the same RSI computed from 100 bars produces. Fetching only
`history + period` bars would hand a caller plausible values that
disagree with what a backtest computed over the same window.
"""

from __future__ import annotations

from collections.abc import Iterable

# Five periods of run-up puts the residual weight of the seed below 1% for
# the smoothings used here; the floor covers very short periods, where five
# periods is still only a handful of bars.
_WARMUP_MULTIPLE = 5
_MIN_WARMUP_BARS = 50


def warmup_bars_for(period: int) -> int:
    """Bars to fetch BEYOND the caller's requested history."""
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")
    return max(_WARMUP_MULTIPLE * period, _MIN_WARMUP_BARS)


def warmup_bars_for_all(periods: Iterable[int]) -> int:
    """The largest requirement across several indicators.

    Zero for an empty request: a caller asking for no indicators wants
    exactly the history it asked for.
    """
    return max((warmup_bars_for(p) for p in periods), default=0)
```

Create `src/trading/indicators/trend.py`:

```python
"""Trend indicators: moving averages and what is built from them."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")


def sma(values: Sequence[Decimal], period: int) -> Decimal | None:
    """Simple moving average of the last `period` values.

    `None` when there are not enough of them -- averaging whatever is
    available would return a different indicator under the same name.
    """
    _require_positive_period(period)
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window, Decimal(0)) / Decimal(period)


def ema_series(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Every EMA value, oldest first, seeded with the SMA of the first
    `period` bars.

    Seeding with the SMA rather than the first close is what makes this
    reproducible: seeding with a single bar leaves the whole series
    dependent on how far back the caller happened to fetch.

    Returns `[]` when there is not enough data, so callers can test it
    without a separate length check.
    """
    _require_positive_period(period)
    if len(values) < period:
        return []
    alpha = Decimal(2) / Decimal(period + 1)
    seed = sum(values[:period], Decimal(0)) / Decimal(period)
    out = [seed]
    for value in values[period:]:
        out.append(alpha * value + (Decimal(1) - alpha) * out[-1])
    return out


def ema(values: Sequence[Decimal], period: int) -> Decimal | None:
    """The latest exponential moving average, or `None`."""
    series = ema_series(values, period)
    return series[-1] if series else None
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/indicators -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/indicators && uv run ruff check src/trading/indicators tests/indicators`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/indicators tests/indicators
git commit -m "$(cat <<'MSG'
feat(indicators): moving averages, and an honest warmup rule

Shared rather than private to the MCP layer because a strategy script
and the live agent must compute the same number from the same bars. If
they diverged, a lesson carried from a backtest into a live order would
be measuring a different thing in each place, and nothing would say so.

EMA seeds with the SMA of the first period rather than the first close:
seeded from one bar, the whole series depends on how far back the caller
happened to fetch, which makes it irreproducible across two callers
asking for different history.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 2: Momentum indicators

**Files:**
- Create: `src/trading/indicators/momentum.py`
- Test: `tests/indicators/test_momentum.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `momentum.rsi(closes: Sequence[Decimal], period: int = 14) -> Decimal | None`
  - `momentum.roc(closes: Sequence[Decimal], period: int = 12) -> Decimal | None`
  - `momentum.stochastic_k(highs: Sequence[Decimal], lows: Sequence[Decimal], closes: Sequence[Decimal], period: int = 14) -> Decimal | None`

- [ ] **Step 1: Write the failing tests**

Create `tests/indicators/test_momentum.py`:

```python
from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.momentum import roc, rsi, stochastic_k

_prices = st.lists(
    st.decimals(min_value=Decimal("1"), max_value=Decimal("10000"), places=2),
    min_size=16,
    max_size=60,
)


def test_rsi_needs_one_more_bar_than_its_period() -> None:
    # `period` deltas require `period + 1` closes.
    assert rsi([Decimal(10)] * 14, period=14) is None
    assert rsi([Decimal(10)] * 15, period=14) is not None


def test_rsi_of_a_series_that_only_rises_is_one_hundred() -> None:
    closes = [Decimal(i) for i in range(1, 20)]
    assert rsi(closes, period=14) == Decimal(100)


def test_rsi_of_a_series_that_only_falls_is_zero() -> None:
    closes = [Decimal(i) for i in range(20, 1, -1)]
    assert rsi(closes, period=14) == Decimal(0)


def test_rsi_matches_a_hand_computed_wilder_example() -> None:
    # closes 10, 11, 10, 11, 10 with period 2.
    #   deltas          +1  -1  +1  -1
    #   gains            1   0   1   0
    #   losses           0   1   0   1
    #   seed (first 2)  avg_gain = 0.5   avg_loss = 0.5
    #   bar 3 (g=1,l=0) avg_gain = (0.5*1 + 1)/2 = 0.75
    #                   avg_loss = (0.5*1 + 0)/2 = 0.25
    #   bar 4 (g=0,l=1) avg_gain = (0.75*1 + 0)/2 = 0.375
    #                   avg_loss = (0.25*1 + 1)/2 = 0.625
    #   RS = 0.6 -> RSI = 100 - 100/1.6 = 37.5
    closes = [Decimal(10), Decimal(11), Decimal(10), Decimal(11), Decimal(10)]
    assert rsi(closes, period=2) == Decimal("37.5")


def test_a_truncated_rsi_differs_from_a_fully_warmed_one() -> None:
    # The whole reason `warmup` exists. Wilder smoothing carries the seed
    # forward indefinitely, so the same 15 final bars give a different RSI
    # depending on how much history preceded them. If this ever stops
    # being true, the warmup machinery is measuring nothing.
    closes = [Decimal(100) + Decimal((i * 7) % 13) for i in range(200)]
    truncated = rsi(closes[-15:], period=14)
    warmed = rsi(closes, period=14)
    assert truncated is not None and warmed is not None
    assert truncated != warmed


@given(_prices)
def test_rsi_always_lies_between_zero_and_one_hundred(closes: list[Decimal]) -> None:
    result = rsi(closes, period=14)
    if result is not None:
        assert Decimal(0) <= result <= Decimal(100)


def test_roc_is_the_percentage_change_over_the_period() -> None:
    # 100 -> 110 across 2 bars is +10%.
    closes = [Decimal(100), Decimal(105), Decimal(110)]
    assert roc(closes, period=2) == Decimal(10)


def test_roc_returns_none_when_the_reference_bar_is_zero() -> None:
    closes = [Decimal(0), Decimal(5), Decimal(10)]
    assert roc(closes, period=2) is None


def test_stochastic_k_is_zero_at_the_low_and_one_hundred_at_the_high() -> None:
    highs = [Decimal(10), Decimal(12), Decimal(14)]
    lows = [Decimal(5), Decimal(6), Decimal(7)]
    at_high = stochastic_k(highs, lows, [Decimal(9), Decimal(9), Decimal(14)], period=3)
    at_low = stochastic_k(highs, lows, [Decimal(9), Decimal(9), Decimal(5)], period=3)
    assert at_high == Decimal(100)
    assert at_low == Decimal(0)


def test_stochastic_k_is_none_when_the_range_is_flat() -> None:
    # A flat window has no position within it. Reporting 50 would invent
    # a midpoint that the data does not contain.
    flat = [Decimal(10)] * 3
    assert stochastic_k(flat, flat, flat, period=3) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/indicators/test_momentum.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.indicators.momentum'`

- [ ] **Step 3: Write the implementation**

Create `src/trading/indicators/momentum.py`:

```python
"""Momentum indicators: rate and position of price change."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")


def rsi(closes: Sequence[Decimal], period: int = 14) -> Decimal | None:
    """Wilder's Relative Strength Index over `closes`.

    Wilder smoothing, not a simple average of gains: the two disagree,
    and Wilder's is what every charting package means by "RSI". Needs
    `period + 1` closes, since `period` deltas require that many bars.

    A window with no losses is 100 rather than a division by zero -- the
    index is defined at that boundary and the caller should see it.
    """
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None

    gains: list[Decimal] = []
    losses: list[Decimal] = []
    for previous, current in zip(closes, closes[1:], strict=False):
        delta = current - previous
        gains.append(delta if delta > 0 else Decimal(0))
        losses.append(-delta if delta < 0 else Decimal(0))

    divisor = Decimal(period)
    avg_gain = sum(gains[:period], Decimal(0)) / divisor
    avg_loss = sum(losses[:period], Decimal(0)) / divisor
    for gain, loss in zip(gains[period:], losses[period:], strict=True):
        avg_gain = (avg_gain * (divisor - 1) + gain) / divisor
        avg_loss = (avg_loss * (divisor - 1) + loss) / divisor

    if avg_loss == 0:
        return Decimal(100) if avg_gain > 0 else Decimal(50)
    relative_strength = avg_gain / avg_loss
    return Decimal(100) - (Decimal(100) / (Decimal(1) + relative_strength))


def roc(closes: Sequence[Decimal], period: int = 12) -> Decimal | None:
    """Percentage rate of change over `period` bars."""
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None
    reference = closes[-period - 1]
    if reference == 0:
        return None
    return (closes[-1] - reference) / reference * Decimal(100)


def stochastic_k(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Where the last close sits in the period's range, as a percentage.

    `None` for a flat range rather than 50: a window with no range has no
    position within it, and a midpoint invented here would read as a
    fact about the market.
    """
    _require_positive_period(period)
    if min(len(highs), len(lows), len(closes)) < period:
        return None
    highest = max(highs[-period:])
    lowest = min(lows[-period:])
    if highest == lowest:
        return None
    return (closes[-1] - lowest) / (highest - lowest) * Decimal(100)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/indicators/test_momentum.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/indicators && uv run ruff check src/trading/indicators tests/indicators`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/indicators/momentum.py tests/indicators/test_momentum.py
git commit -m "$(cat <<'MSG'
feat(indicators): Wilder RSI, rate of change, stochastic %K

Wilder smoothing rather than a simple average of gains, because the two
disagree and Wilder's is what every charting package means by "RSI". An
agent comparing our number against any chart it can see must find the
same value.

Stochastic %K over a flat window returns None rather than 50: a window
with no range has no position within it, and a midpoint invented here
would reach the caller looking like a fact about the market.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 3: Volatility indicators

**Files:**
- Create: `src/trading/indicators/volatility.py`
- Test: `tests/indicators/test_volatility.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `volatility.true_ranges(highs, lows, closes) -> list[Decimal]`
  - `volatility.atr(highs, lows, closes, period: int = 14) -> Decimal | None`
  - `volatility.Bands` — `NamedTuple(lower: Decimal, mid: Decimal, upper: Decimal)`
  - `volatility.bollinger(closes, period: int = 20, num_std: Decimal = Decimal(2)) -> Bands | None`
  - `volatility.realised_vol(closes, period: int = 20) -> Decimal | None`

**Note on `realised_vol`:** it returns **per-bar** standard deviation of
simple returns and is deliberately NOT annualised. `metrics.curve`
only knows `{"1d": 252}`, and periods-per-year cannot be derived from the
bar interval alone: an hourly bar is 8,760 a year for a 24/7 crypto
market and roughly 1,575 for a 6h15m NSE session. Annualising here would
require guessing the asset's calendar, and a guessed factor produces a
number that looks like a volatility and is not one.

- [ ] **Step 1: Write the failing tests**

Create `tests/indicators/test_volatility.py`:

```python
from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from trading.indicators.volatility import atr, bollinger, realised_vol, true_ranges

_prices = st.lists(
    st.decimals(min_value=Decimal("1"), max_value=Decimal("10000"), places=2),
    min_size=25,
    max_size=60,
)


def test_true_range_uses_the_previous_close_when_the_bar_gaps_up() -> None:
    # Bar 2 gaps: high 20, low 18, previous close 10. The high-to-previous
    # -close distance (10) is the true range, not the 2-point bar range.
    highs = [Decimal(11), Decimal(20)]
    lows = [Decimal(9), Decimal(18)]
    closes = [Decimal(10), Decimal(19)]
    assert true_ranges(highs, lows, closes) == [Decimal(10)]


def test_true_ranges_has_one_fewer_entry_than_the_bars() -> None:
    bars = [Decimal(i) for i in range(1, 11)]
    assert len(true_ranges(bars, bars, bars)) == len(bars) - 1


def test_atr_of_constant_ranges_is_that_range() -> None:
    # Every bar spans exactly 2 with no gaps, so every true range is 2 and
    # Wilder smoothing of a constant is that constant.
    closes = [Decimal(10)] * 20
    highs = [Decimal(11)] * 20
    lows = [Decimal(9)] * 20
    assert atr(highs, lows, closes, period=14) == Decimal(2)


def test_atr_returns_none_without_enough_true_ranges() -> None:
    closes = [Decimal(10)] * 14
    assert atr(closes, closes, closes, period=14) is None


@given(_prices)
def test_atr_is_never_negative(closes: list[Decimal]) -> None:
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    result = atr(highs, lows, closes, period=14)
    if result is not None:
        assert result >= 0


def test_bollinger_mid_band_is_the_simple_moving_average() -> None:
    closes = [Decimal(2), Decimal(4), Decimal(6)]
    bands = bollinger(closes, period=3)
    assert bands is not None
    assert bands.mid == Decimal(4)


def test_bollinger_collapses_to_the_mean_when_the_series_is_flat() -> None:
    closes = [Decimal(10)] * 20
    bands = bollinger(closes, period=20)
    assert bands is not None
    assert bands.lower == bands.mid == bands.upper == Decimal(10)


def test_bollinger_bands_are_symmetric_around_the_mid() -> None:
    closes = [Decimal(i) for i in range(1, 21)]
    bands = bollinger(closes, period=20)
    assert bands is not None
    assert bands.upper - bands.mid == bands.mid - bands.lower


def test_bollinger_returns_none_with_too_few_bars() -> None:
    assert bollinger([Decimal(1), Decimal(2)], period=20) is None


def test_realised_vol_of_a_flat_series_is_zero() -> None:
    assert realised_vol([Decimal(10)] * 25, period=20) == Decimal(0)


def test_realised_vol_returns_none_when_a_reference_close_is_zero() -> None:
    closes = [Decimal(0)] + [Decimal(10)] * 24
    assert realised_vol(closes, period=24) is None


@given(_prices)
def test_realised_vol_is_never_negative(closes: list[Decimal]) -> None:
    result = realised_vol(closes, period=20)
    if result is not None:
        assert result >= 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/indicators/test_volatility.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.indicators.volatility'`

- [ ] **Step 3: Write the implementation**

Create `src/trading/indicators/volatility.py`:

```python
"""Volatility indicators: how far price moves, not which way."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import NamedTuple


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")


class Bands(NamedTuple):
    """A Bollinger envelope. `mid` is the simple moving average."""

    lower: Decimal
    mid: Decimal
    upper: Decimal


def true_ranges(
    highs: Sequence[Decimal], lows: Sequence[Decimal], closes: Sequence[Decimal]
) -> list[Decimal]:
    """True range per bar, from the second bar onwards.

    The previous close is part of the definition: a bar that gaps away
    from the last close has travelled the whole gap, and measuring only
    its own high-to-low would report a quiet bar after a violent move.
    """
    count = min(len(highs), len(lows), len(closes))
    out: list[Decimal] = []
    for index in range(1, count):
        previous_close = closes[index - 1]
        out.append(
            max(
                highs[index] - lows[index],
                abs(highs[index] - previous_close),
                abs(lows[index] - previous_close),
            )
        )
    return out


def atr(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Wilder's Average True Range."""
    _require_positive_period(period)
    ranges = true_ranges(highs, lows, closes)
    if len(ranges) < period:
        return None
    divisor = Decimal(period)
    value = sum(ranges[:period], Decimal(0)) / divisor
    for current in ranges[period:]:
        value = (value * (divisor - 1) + current) / divisor
    return value


def bollinger(
    closes: Sequence[Decimal], period: int = 20, num_std: Decimal = Decimal(2)
) -> Bands | None:
    """Bollinger bands around the simple moving average.

    Population standard deviation, dividing by `period` rather than
    `period - 1`: the window is the whole population being described, and
    it is what charting packages plot.
    """
    _require_positive_period(period)
    if len(closes) < period:
        return None
    window = closes[-period:]
    divisor = Decimal(period)
    mid = sum(window, Decimal(0)) / divisor
    variance = sum(((value - mid) ** 2 for value in window), Decimal(0)) / divisor
    deviation = variance.sqrt()
    return Bands(lower=mid - num_std * deviation, mid=mid, upper=mid + num_std * deviation)


def realised_vol(closes: Sequence[Decimal], period: int = 20) -> Decimal | None:
    """Per-bar standard deviation of simple returns.

    Deliberately NOT annualised. Periods-per-year cannot be derived from
    the bar interval alone -- an hourly bar is 8,760 a year on a 24/7
    crypto venue and about 1,575 across a 6h15m NSE session -- so
    annualising here would mean guessing the asset's calendar. The result
    would look like a volatility and not be one. A caller who knows the
    calendar can scale this itself.
    """
    _require_positive_period(period)
    if len(closes) < period + 1:
        return None
    window = closes[-period - 1 :]
    returns: list[Decimal] = []
    for previous, current in zip(window, window[1:], strict=True):
        if previous == 0:
            return None
        returns.append(current / previous - Decimal(1))
    divisor = Decimal(len(returns))
    mean = sum(returns, Decimal(0)) / divisor
    variance = sum(((value - mean) ** 2 for value in returns), Decimal(0)) / divisor
    return variance.sqrt()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/indicators/test_volatility.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/indicators && uv run ruff check src/trading/indicators tests/indicators`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/indicators/volatility.py tests/indicators/test_volatility.py
git commit -m "$(cat <<'MSG'
feat(indicators): true range, ATR, Bollinger bands, return dispersion

realised_vol is per-bar and deliberately not annualised. Periods-per-year
cannot be derived from the bar interval: an hourly bar is 8,760 a year on
a 24/7 crypto venue and about 1,575 across a 6h15m NSE session. Scaling
by a guessed calendar would produce a number that looks like a volatility
and is not one -- the same rule metrics.curve.periods_per_year already
applies by returning None for an interval it does not know.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 4: MACD, ADX, levels, and the indicator registry

**Files:**
- Modify: `src/trading/indicators/trend.py` (append `macd`, `adx`)
- Create: `src/trading/indicators/levels.py`
- Modify: `src/trading/indicators/__init__.py` (registry, parsing, dispatch)
- Test: `tests/indicators/test_trend.py` (append), `tests/indicators/test_levels.py`, `tests/indicators/test_registry.py`

**Interfaces:**
- Consumes: `trend.ema_series`, `trend.sma` (Task 1); `volatility.true_ranges` (Task 3).
- Produces:
  - `trend.Macd` — `NamedTuple(line: Decimal, signal: Decimal, histogram: Decimal)`
  - `trend.macd(closes, fast: int = 12, slow: int = 26, signal: int = 9) -> Macd | None`
  - `trend.adx(highs, lows, closes, period: int = 14) -> Decimal | None`
  - `levels.pct_from_high(closes, highs, period) -> Decimal | None`
  - `levels.pct_from_low(closes, lows, period) -> Decimal | None`
  - `levels.range_position(closes, highs, lows, period) -> Decimal | None`
  - `indicators.IndicatorRequest` — frozen dataclass `(token: str, name: str, period: int)`
  - `indicators.CATALOGUE: dict[str, str]` — indicator name to one-line description
  - `indicators.parse(token: str) -> IndicatorRequest`
  - `indicators.compute(request, *, highs, lows, closes) -> Decimal | dict[str, Decimal] | None`
  - `indicators.warmup_for(requests: Sequence[IndicatorRequest]) -> int`

- [ ] **Step 1: Write the failing tests**

Append to `tests/indicators/test_trend.py`:

```python
from trading.indicators.trend import adx, macd


def test_macd_of_a_flat_series_is_zero_on_every_leg() -> None:
    closes = [Decimal(10)] * 60
    result = macd(closes)
    assert result is not None
    assert result.line == Decimal(0)
    assert result.signal == Decimal(0)
    assert result.histogram == Decimal(0)


def test_macd_line_is_positive_while_price_rises() -> None:
    # The fast EMA sits above the slow one in an uptrend.
    closes = [Decimal(i) for i in range(1, 81)]
    result = macd(closes)
    assert result is not None
    assert result.line > 0


def test_macd_histogram_is_the_line_minus_its_signal() -> None:
    closes = [Decimal(i) for i in range(1, 81)]
    result = macd(closes)
    assert result is not None
    assert result.histogram == result.line - result.signal


def test_macd_returns_none_without_enough_bars() -> None:
    assert macd([Decimal(10)] * 20) is None


def test_macd_rejects_a_fast_period_that_is_not_faster() -> None:
    with pytest.raises(ValueError):
        macd([Decimal(10)] * 60, fast=26, slow=26)


def test_adx_of_a_flat_series_is_none() -> None:
    # No directional movement and no true range: DX is undefined at every
    # bar, so there is nothing to average.
    flat = [Decimal(10)] * 60
    assert adx(flat, flat, flat, period=14) is None


def test_adx_of_a_persistent_uptrend_is_high() -> None:
    # Every bar makes a higher high and higher low, so -DI is zero, DX is
    # pinned at 100, and its average is 100.
    closes = [Decimal(i) for i in range(1, 61)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    result = adx(highs, lows, closes, period=14)
    assert result is not None
    assert result == Decimal(100)


def test_adx_returns_none_without_enough_bars() -> None:
    closes = [Decimal(i) for i in range(1, 10)]
    assert adx(closes, closes, closes, period=14) is None
```

Create `tests/indicators/test_levels.py`:

```python
from __future__ import annotations

from decimal import Decimal

from trading.indicators.levels import pct_from_high, pct_from_low, range_position


def test_pct_from_high_is_zero_at_the_period_high() -> None:
    highs = [Decimal(10), Decimal(20), Decimal(15)]
    closes = [Decimal(9), Decimal(19), Decimal(20)]
    assert pct_from_high(closes, highs, period=3) == Decimal(0)


def test_pct_from_high_is_negative_below_the_period_high() -> None:
    highs = [Decimal(10), Decimal(20), Decimal(15)]
    closes = [Decimal(9), Decimal(19), Decimal(10)]
    # 10 against a 20 high is 50% below it.
    assert pct_from_high(closes, highs, period=3) == Decimal(-50)


def test_pct_from_low_is_positive_above_the_period_low() -> None:
    lows = [Decimal(10), Decimal(5), Decimal(8)]
    closes = [Decimal(11), Decimal(6), Decimal(10)]
    # 10 against a 5 low is 100% above it.
    assert pct_from_low(closes, lows, period=3) == Decimal(100)


def test_range_position_spans_zero_to_one_hundred() -> None:
    highs = [Decimal(20)] * 3
    lows = [Decimal(10)] * 3
    assert range_position([Decimal(0), Decimal(0), Decimal(15)], highs, lows, 3) == Decimal(50)
    assert range_position([Decimal(0), Decimal(0), Decimal(20)], highs, lows, 3) == Decimal(100)
    assert range_position([Decimal(0), Decimal(0), Decimal(10)], highs, lows, 3) == Decimal(0)


def test_range_position_is_none_when_the_range_is_flat() -> None:
    flat = [Decimal(10)] * 3
    assert range_position(flat, flat, flat, 3) is None


def test_levels_return_none_with_too_few_bars() -> None:
    one = [Decimal(10)]
    assert pct_from_high(one, one, period=5) is None
    assert pct_from_low(one, one, period=5) is None
    assert range_position(one, one, one, period=5) is None
```

Create `tests/indicators/test_registry.py`:

```python
from __future__ import annotations

from decimal import Decimal

import pytest

from trading.indicators import CATALOGUE, compute, parse, warmup_for


def test_parse_splits_a_token_into_a_name_and_a_period() -> None:
    request = parse("rsi14")
    assert request.name == "rsi"
    assert request.period == 14
    assert request.token == "rsi14"


def test_parse_falls_back_to_the_documented_default_period() -> None:
    assert parse("rsi").period == 14
    assert parse("ema").period == 20


def test_parse_is_case_insensitive_and_ignores_surrounding_space() -> None:
    assert parse("  RSI14 ").name == "rsi"


def test_parse_rejects_an_unknown_indicator_and_names_the_known_ones() -> None:
    with pytest.raises(ValueError) as excinfo:
        parse("supertrend9")
    assert "supertrend" in str(excinfo.value)
    assert "rsi" in str(excinfo.value)


def test_parse_rejects_a_period_on_macd() -> None:
    # MACD is three periods, not one; "macd12" cannot mean anything
    # unambiguous, so it is refused rather than silently reinterpreted.
    with pytest.raises(ValueError):
        parse("macd12")


def test_parse_rejects_a_zero_period() -> None:
    with pytest.raises(ValueError):
        parse("rsi0")


def test_every_catalogued_indicator_parses_and_computes() -> None:
    closes = [Decimal(i) for i in range(1, 121)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    for name in CATALOGUE:
        request = parse(name)
        result = compute(request, highs=highs, lows=lows, closes=closes)
        assert result is not None, f"{name} returned None on 120 clean bars"


def test_compute_returns_a_mapping_for_multi_valued_indicators() -> None:
    closes = [Decimal(i) for i in range(1, 121)]
    highs = [c + Decimal(1) for c in closes]
    lows = [c - Decimal(1) for c in closes]
    bands = compute(parse("bb20"), highs=highs, lows=lows, closes=closes)
    assert isinstance(bands, dict)
    assert set(bands) == {"lower", "mid", "upper"}
    lines = compute(parse("macd"), highs=highs, lows=lows, closes=closes)
    assert isinstance(lines, dict)
    assert set(lines) == {"line", "signal", "histogram"}


def test_warmup_for_takes_the_largest_requirement() -> None:
    # rsi14 -> 70, macd -> 5*35 = 175.
    assert warmup_for([parse("rsi14"), parse("macd")]) == 175


def test_warmup_for_nothing_is_zero() -> None:
    assert warmup_for([]) == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/indicators -v`
Expected: FAIL — `ImportError: cannot import name 'macd' from 'trading.indicators.trend'`

- [ ] **Step 3: Append `macd` and `adx` to `src/trading/indicators/trend.py`**

```python
class Macd(NamedTuple):
    """Moving Average Convergence Divergence, all three legs."""

    line: Decimal
    signal: Decimal
    histogram: Decimal


def macd(
    closes: Sequence[Decimal], fast: int = 12, slow: int = 26, signal: int = 9
) -> Macd | None:
    """MACD line, its signal line, and the histogram between them."""
    _require_positive_period(fast)
    _require_positive_period(slow)
    _require_positive_period(signal)
    if fast >= slow:
        raise ValueError(f"fast period {fast} must be shorter than slow period {slow}")

    fast_series = ema_series(closes, fast)
    slow_series = ema_series(closes, slow)
    if not fast_series or not slow_series:
        return None

    # Both series end on the same bar but the slow one starts later, so
    # align on the shorter tail. Zipping from the front would subtract
    # EMAs computed at different bars and produce a plausible, wrong line.
    overlap = min(len(fast_series), len(slow_series))
    line = [
        quick - slow_value
        for quick, slow_value in zip(fast_series[-overlap:], slow_series[-overlap:], strict=True)
    ]
    signal_series = ema_series(line, signal)
    if not signal_series:
        return None
    return Macd(
        line=line[-1], signal=signal_series[-1], histogram=line[-1] - signal_series[-1]
    )


def adx(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> Decimal | None:
    """Wilder's Average Directional Index: trend strength, not direction.

    `None` when no bar has a defined DX -- a series with no range and no
    directional movement has no trend strength to report, and zero would
    claim it measured one.
    """
    _require_positive_period(period)
    count = min(len(highs), len(lows), len(closes))
    if count < 2:
        return None

    plus_dm: list[Decimal] = []
    minus_dm: list[Decimal] = []
    ranges: list[Decimal] = []
    for index in range(1, count):
        up_move = highs[index] - highs[index - 1]
        down_move = lows[index - 1] - lows[index]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else Decimal(0))
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else Decimal(0))
        previous_close = closes[index - 1]
        ranges.append(
            max(
                highs[index] - lows[index],
                abs(highs[index] - previous_close),
                abs(lows[index] - previous_close),
            )
        )

    if len(ranges) < period:
        return None

    def wilder_sum(values: list[Decimal]) -> list[Decimal]:
        running = sum(values[:period], Decimal(0))
        out = [running]
        for value in values[period:]:
            running = running - running / Decimal(period) + value
            out.append(running)
        return out

    smoothed_range = wilder_sum(ranges)
    smoothed_plus = wilder_sum(plus_dm)
    smoothed_minus = wilder_sum(minus_dm)

    directional: list[Decimal] = []
    for total_range, up, down in zip(
        smoothed_range, smoothed_plus, smoothed_minus, strict=True
    ):
        if total_range == 0:
            continue
        plus_di = Decimal(100) * up / total_range
        minus_di = Decimal(100) * down / total_range
        if plus_di + minus_di == 0:
            continue
        directional.append(Decimal(100) * abs(plus_di - minus_di) / (plus_di + minus_di))

    if len(directional) < period:
        return None
    divisor = Decimal(period)
    value = sum(directional[:period], Decimal(0)) / divisor
    for current in directional[period:]:
        value = (value * (divisor - 1) + current) / divisor
    return value
```

Add `NamedTuple` to the `typing` import at the top of `trend.py`:

```python
from typing import NamedTuple
```

- [ ] **Step 4: Create `src/trading/indicators/levels.py`**

```python
"""Where price sits relative to its own recent extremes."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal


def _require_positive_period(period: int) -> None:
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")


def pct_from_high(
    closes: Sequence[Decimal], highs: Sequence[Decimal], period: int
) -> Decimal | None:
    """How far the last close sits below the period's high, as a percentage.

    Zero at the high and negative below it, so the sign carries the
    meaning without the caller having to remember a convention.
    """
    _require_positive_period(period)
    if min(len(closes), len(highs)) < period:
        return None
    highest = max(highs[-period:])
    if highest == 0:
        return None
    return (closes[-1] - highest) / highest * Decimal(100)


def pct_from_low(
    closes: Sequence[Decimal], lows: Sequence[Decimal], period: int
) -> Decimal | None:
    """How far the last close sits above the period's low, as a percentage."""
    _require_positive_period(period)
    if min(len(closes), len(lows)) < period:
        return None
    lowest = min(lows[-period:])
    if lowest == 0:
        return None
    return (closes[-1] - lowest) / lowest * Decimal(100)


def range_position(
    closes: Sequence[Decimal],
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    period: int,
) -> Decimal | None:
    """Position of the last close within the period's range, 0 to 100.

    `None` for a flat range, for the reason `stochastic_k` gives: a
    window with no range has no position within it.
    """
    _require_positive_period(period)
    if min(len(closes), len(highs), len(lows)) < period:
        return None
    highest = max(highs[-period:])
    lowest = min(lows[-period:])
    if highest == lowest:
        return None
    return (closes[-1] - lowest) / (highest - lowest) * Decimal(100)
```

- [ ] **Step 5: Replace `src/trading/indicators/__init__.py` with the registry**

```python
"""Technical indicators over bar series.

`Decimal` throughout, and shared rather than private to the MCP layer on
purpose: a strategy script and the live agent must compute RSI with the
same code. If they diverged, every lesson carried from a backtest into a
live decision would be measuring a subtly different thing, invisibly in
both places.

Callers name an indicator with a token -- `"rsi14"`, `"ema20"`, `"macd"`
-- rather than calling the functions directly, so that one request can
carry a list of them and the warmup requirement can be derived before any
data is fetched.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from trading.indicators.levels import pct_from_high, pct_from_low, range_position
from trading.indicators.momentum import roc, rsi, stochastic_k
from trading.indicators.trend import adx, ema, macd, sma
from trading.indicators.volatility import atr, bollinger, realised_vol
from trading.indicators.warmup import warmup_bars_for_all

__all__ = [
    "CATALOGUE",
    "IndicatorRequest",
    "compute",
    "parse",
    "warmup_for",
]

CATALOGUE: dict[str, str] = {
    "sma": "Simple moving average of close.",
    "ema": "Exponential moving average of close, seeded with the SMA.",
    "macd": "MACD line, signal and histogram. Fixed at 12/26/9.",
    "adx": "Wilder ADX: trend strength, not direction. 0-100.",
    "rsi": "Wilder RSI. 0-100.",
    "roc": "Percentage rate of change over the period.",
    "stoch": "Stochastic %K: close within the period's range. 0-100.",
    "atr": "Wilder Average True Range, in price units.",
    "bb": "Bollinger bands: lower, mid, upper.",
    "vol": "Per-bar standard deviation of simple returns. NOT annualised.",
    "pcthigh": "Percent of the last close below the period high. 0 or negative.",
    "pctlow": "Percent of the last close above the period low. 0 or positive.",
    "rangepos": "Position of the last close within the period range. 0-100.",
}

# The period a bare token means. `macd` carries its warmup requirement
# here (slow 26 + signal 9) even though it takes no period of its own.
_DEFAULT_PERIODS: dict[str, int] = {
    "sma": 20,
    "ema": 20,
    "macd": 35,
    "adx": 14,
    "rsi": 14,
    "roc": 12,
    "stoch": 14,
    "atr": 14,
    "bb": 20,
    "vol": 20,
    "pcthigh": 52,
    "pctlow": 52,
    "rangepos": 20,
}

# Indicators whose parameters are fixed, so a period in the token would be
# ambiguous rather than merely unused.
_FIXED_PARAMETER_INDICATORS = frozenset({"macd"})

_TOKEN = re.compile(r"([a-z]+)(\d*)")


@dataclass(frozen=True)
class IndicatorRequest:
    """One parsed indicator token."""

    token: str
    name: str
    period: int


def parse(token: str) -> IndicatorRequest:
    """Turn `"rsi14"` into a request, or raise with the known names.

    Raising rather than skipping an unknown token: an agent that misspells
    an indicator should be told, not handed a snapshot that quietly lacks
    the thing it asked for and looks complete.
    """
    cleaned = token.strip().lower()
    match = _TOKEN.fullmatch(cleaned)
    if match is None:
        raise ValueError(
            f"unparseable indicator token {token!r}; expected a name "
            f"optionally followed by a period, like 'rsi14'"
        )
    name, digits = match.group(1), match.group(2)
    if name not in _DEFAULT_PERIODS:
        known = ", ".join(sorted(_DEFAULT_PERIODS))
        raise ValueError(f"unknown indicator {name!r}; known indicators are: {known}")
    if digits and name in _FIXED_PARAMETER_INDICATORS:
        raise ValueError(
            f"{name!r} takes no period -- its parameters are fixed; use {name!r} alone"
        )
    period = int(digits) if digits else _DEFAULT_PERIODS[name]
    if period <= 0:
        raise ValueError(f"period must be positive, got {period} in {token!r}")
    return IndicatorRequest(token=cleaned, name=name, period=period)


def warmup_for(requests: Sequence[IndicatorRequest]) -> int:
    """Extra bars to fetch so every requested indicator converges."""
    return warmup_bars_for_all(request.period for request in requests)


def compute(
    request: IndicatorRequest,
    *,
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
) -> Decimal | dict[str, Decimal] | None:
    """Evaluate one parsed request against a bar series.

    `None` means the series was too short or the value is undefined; the
    caller reports that rather than substituting a number.
    """
    name, period = request.name, request.period
    if name == "sma":
        return sma(closes, period)
    if name == "ema":
        return ema(closes, period)
    if name == "macd":
        lines = macd(closes)
        return None if lines is None else lines._asdict()
    if name == "adx":
        return adx(highs, lows, closes, period)
    if name == "rsi":
        return rsi(closes, period)
    if name == "roc":
        return roc(closes, period)
    if name == "stoch":
        return stochastic_k(highs, lows, closes, period)
    if name == "atr":
        return atr(highs, lows, closes, period)
    if name == "bb":
        bands = bollinger(closes, period)
        return None if bands is None else bands._asdict()
    if name == "vol":
        return realised_vol(closes, period)
    if name == "pcthigh":
        return pct_from_high(closes, highs, period)
    if name == "pctlow":
        return pct_from_low(closes, lows, period)
    if name == "rangepos":
        return range_position(closes, highs, lows, period)
    raise ValueError(f"no implementation for catalogued indicator {name!r}")
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/indicators -v`
Expected: PASS, all tests. `test_every_catalogued_indicator_parses_and_computes` is the one that matters — it proves `CATALOGUE`, `_DEFAULT_PERIODS` and `compute` agree on every name.

- [ ] **Step 7: Check types and lint**

Run: `uv run mypy src/trading/indicators && uv run ruff check src/trading/indicators tests/indicators`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add src/trading/indicators tests/indicators
git commit -m "$(cat <<'MSG'
feat(indicators): MACD, ADX, range levels, and a token registry

Callers name indicators with tokens rather than calling functions, so one
request can carry a list and the warmup requirement can be derived before
any bar is fetched -- the fetch has to know how far back to reach.

An unknown token raises instead of being skipped. An agent that misspells
an indicator should be told; silently returning a snapshot that lacks the
thing it asked for, and looks complete, is the worse failure.

MACD aligns its two EMA series on the shorter tail. Zipping from the
front subtracts EMAs computed at different bars, which yields a plausible
line that is wrong everywhere.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 5: `precision=string` on the candles route

**Files:**
- Modify: `src/trading/streaming/market_data_api.py:95-207`
- Test: `tests/streaming/test_market_data_api.py` (append)

**Why:** `Candle` currently serialises OHLCV as `float`. The MCP layer must
hand an agent prices as strings, and it cannot recover a `Decimal` it never
received. The default stays `float` so the existing web charts are
untouched — this is additive.

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `GET /candles/{instrument_id}?precision=string` returning
  `{"instrument_id": int, "interval": str, "candles": [{"ts": iso8601,
  "open": str, "high": str, "low": str, "close": str, "volume": str}]}`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/streaming/test_market_data_api.py`:

```python
@pytest.fixture
def instrument_with_daily_bars(db_conn, fixture_instrument_id: int) -> int:
    for day, close in enumerate((Decimal("100.25"), Decimal("101.50")), start=1):
        db_conn.execute(
            """
            INSERT INTO bars_daily
                (instrument_id, ts, open, high, low, close, volume, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 6)
            """,
            (
                fixture_instrument_id,
                datetime(2026, 1, day, tzinfo=UTC),
                close,
                close,
                close,
                close,
                Decimal("1000"),
            ),
        )
    return fixture_instrument_id


def test_candles_default_to_floats_so_existing_clients_are_untouched(
    client: TestClient, instrument_with_daily_bars: int
) -> None:
    response = client.get(f"/candles/{instrument_with_daily_bars}", params={"interval": "1d"})
    assert response.status_code == 200
    first = response.json()["candles"][0]
    assert isinstance(first["close"], float)


def test_candles_with_string_precision_return_exact_decimal_text(
    client: TestClient, instrument_with_daily_bars: int
) -> None:
    response = client.get(
        f"/candles/{instrument_with_daily_bars}",
        params={"interval": "1d", "precision": "string"},
    )
    assert response.status_code == 200
    closes = [candle["close"] for candle in response.json()["candles"]]
    assert closes == ["100.25", "101.50"]


def test_candles_reject_an_unknown_precision(
    client: TestClient, instrument_with_daily_bars: int
) -> None:
    response = client.get(
        f"/candles/{instrument_with_daily_bars}",
        params={"interval": "1d", "precision": "exact"},
    )
    assert response.status_code == 422
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v -k precision`
Expected: FAIL — the string test returns floats, the reject test returns 200.

- [ ] **Step 3: Refactor the fetchers to return `Decimal` rows**

Replace `_fetch_bucketed_candles` and `_fetch_daily_candles` in
`src/trading/streaming/market_data_api.py` with row fetchers plus two
builders, and add the string models. The SQL is unchanged.

```python
class StringCandle(BaseModel):
    """OHLCV as decimal text.

    A price is money, and JSON has no decimal type. A client that must not
    round -- anything sizing a position, and every indicator computed from
    these bars -- asks for this shape instead.
    """

    ts: datetime
    open: str
    high: str
    low: str
    close: str
    volume: str


class StringCandlesResponse(BaseModel):
    instrument_id: int
    interval: str
    candles: list[StringCandle]


_Row = tuple[datetime, Decimal, Decimal, Decimal, Decimal, Decimal]


def _decimal(value: object) -> Decimal:
    """Whatever the driver returned, as a Decimal. `None` volume is zero.

    `str()` first for a float: `Decimal(0.1)` is 0.1000000000000000055…,
    while `Decimal(str(0.1))` is the 0.1 the database meant.
    """
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(str(value))


def _fetch_bucketed_rows(
    conn: Connection, instrument_id: int, bucket: str, limit: int
) -> list[_Row]:
    rows = conn.execute(_BUCKETED_CANDLES_SQL, (bucket, instrument_id, limit)).fetchall()
    built = [
        (ts, _decimal(o), _decimal(h), _decimal(low), _decimal(c), _decimal(v))
        for ts, o, h, low, c, v in rows
    ]
    return list(reversed(built))


def _fetch_daily_rows(conn: Connection, instrument_id: int, limit: int) -> list[_Row]:
    rows = conn.execute(_DAILY_CANDLES_SQL, (instrument_id, limit)).fetchall()
    built = [
        (ts, _decimal(o), _decimal(h), _decimal(low), _decimal(c), _decimal(v))
        for ts, o, h, low, c, v in rows
    ]
    return list(reversed(built))


def _as_float_candles(rows: list[_Row]) -> list[Candle]:
    return [
        Candle(ts=ts, open=float(o), high=float(h), low=float(low), close=float(c), volume=float(v))
        for ts, o, h, low, c, v in rows
    ]


def _as_string_candles(rows: list[_Row]) -> list[StringCandle]:
    return [
        StringCandle(ts=ts, open=str(o), high=str(h), low=str(low), close=str(c), volume=str(v))
        for ts, o, h, low, c, v in rows
    ]
```

- [ ] **Step 4: Add the `precision` parameter to the route**

Replace the `get_candles` route body:

```python
@router.get("/candles/{instrument_id}", response_model=None)
def get_candles(
    instrument_id: int,
    interval: str = Query(...),
    limit: int = _DEFAULT_LIMIT,
    precision: Literal["float", "string"] = "float",
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> CandlesResponse | StringCandlesResponse:
    """`precision` defaults to `float`, which is what the web charts read.

    `string` is for clients that must not round: money has no float
    representation, and an indicator or a position size computed from a
    rounded close is wrong in a way nothing downstream can detect.
    """
    if interval not in _VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid interval {interval!r}; expected one of {sorted(_VALID_INTERVALS)}",
        )
    row = conn.execute(
        "SELECT asset_class FROM instruments WHERE instrument_id = %s", (instrument_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )
    asset_class = row[0]

    if interval == "1d" and asset_class != "CRYPTO":
        rows = _fetch_daily_rows(conn, instrument_id, limit)
    else:
        bucket = _INTERVAL_BUCKETS.get(interval, "1 day")
        rows = _fetch_bucketed_rows(conn, instrument_id, bucket, limit)

    if precision == "string":
        return StringCandlesResponse(
            instrument_id=instrument_id, interval=interval, candles=_as_string_candles(rows)
        )
    return CandlesResponse(
        instrument_id=instrument_id, interval=interval, candles=_as_float_candles(rows)
    )
```

Add `Literal` to the `typing` import at the top of the module.

- [ ] **Step 5: Run the whole streaming suite**

Run: `uv run pytest tests/streaming/test_market_data_api.py -v`
Expected: PASS, including every pre-existing test — the default path must be unchanged.

- [ ] **Step 6: Check types and lint**

Run: `uv run mypy src/trading/streaming/market_data_api.py && uv run ruff check src/trading/streaming/market_data_api.py`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add src/trading/streaming/market_data_api.py tests/streaming/test_market_data_api.py
git commit -m "$(cat <<'MSG'
feat(candles): serve exact decimal text for clients that must not round

The route serialised OHLCV as float, which is right for a chart and wrong
for anything sizing a position or computing an indicator: a price is
money, JSON has no decimal type, and a consumer cannot recover precision
it never received.

`precision` defaults to float, so the web charts read exactly what they
read before. Only a caller that asks gets the string shape.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 6: `GET /perp-context/{instrument_id}`

**Files:**
- Create: `src/trading/streaming/perp_reference_api.py`
- Modify: `src/trading/streaming/gateway.py:86-89`
- Test: `tests/streaming/test_perp_reference_api.py`

**Why:** the agent must size a perpetual on the contract's own step and
floors. DOGE steps by a whole coin, BTC by 0.001, and minimum notionals
run 5 / 20 / 50. Without this the agent guesses and eats a rejection from
`_require_perp_order_is_tradable` that it cannot diagnose. The data is
already seeded; nothing serves it.

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `GET /perp-context/{instrument_id}` returning `PerpContext`:
  `instrument_id: int, symbol: str, step_size: str, min_qty: str,
  min_notional: str, liquidation_fee: str | None, max_leverage: str | None,
  latest_funding_rate: str | None, latest_funding_time: datetime | None,
  latest_mark_price: str | None, margin_tiers: list[dict[str, str]]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/streaming/test_perp_reference_api.py`:

```python
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trading.streaming.db import get_db_connection
from trading.streaming.perp_reference_api import router

pytestmark = pytest.mark.db


@pytest.fixture
def client(db_conn) -> Iterator[TestClient]:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def perp_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('PERP', 'BINANCE_FUTURES', 'PERP', 'DOGEUSDT', 'ACTIVE',
                'BINANCE_FUTURES:PERP:DOGEUSDT')
        RETURNING instrument_id
        """
    ).fetchone()
    instrument_id = row[0]
    db_conn.execute(
        """
        INSERT INTO perp_contract_specs
            (instrument_id, step_size, min_qty, min_notional, liquidation_fee,
             effective_from, effective_to)
        VALUES (%s, 1, 1, 5, 0.015, '2020-01-01', NULL)
        """,
        (instrument_id,),
    )
    db_conn.execute(
        """
        INSERT INTO perp_margin_tiers
            (instrument_id, notional_floor, notional_cap, max_leverage,
             maintenance_rate, maintenance_amount)
        VALUES (%s, 0, 5000, 75, 0.0065, 0)
        """,
        (instrument_id,),
    )
    db_conn.execute(
        """
        INSERT INTO perp_funding (instrument_id, funding_time, rate, mark_price)
        VALUES (%s, %s, %s, %s)
        """,
        (instrument_id, datetime(2026, 9, 1, 8, tzinfo=UTC), Decimal("0.0001"), Decimal("0.21")),
    )
    return instrument_id


@pytest.fixture
def spot_instrument_id(db_conn) -> int:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        RETURNING instrument_id
        """
    ).fetchone()
    return row[0]


def test_perp_context_carries_the_contract_filters_as_exact_text(
    client: TestClient, perp_instrument_id: int
) -> None:
    response = client.get(f"/perp-context/{perp_instrument_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "DOGEUSDT"
    assert Decimal(body["step_size"]) == Decimal(1)
    assert Decimal(body["min_notional"]) == Decimal(5)
    assert isinstance(body["step_size"], str)


def test_perp_context_reports_the_latest_funding_observation(
    client: TestClient, perp_instrument_id: int
) -> None:
    body = client.get(f"/perp-context/{perp_instrument_id}").json()
    assert Decimal(body["latest_funding_rate"]) == Decimal("0.0001")
    assert Decimal(body["latest_mark_price"]) == Decimal("0.21")
    assert body["latest_funding_time"].startswith("2026-09-01T08:00:00")


def test_perp_context_lists_the_margin_tiers_in_notional_order(
    client: TestClient, perp_instrument_id: int
) -> None:
    tiers = client.get(f"/perp-context/{perp_instrument_id}").json()["margin_tiers"]
    assert len(tiers) == 1
    assert Decimal(tiers[0]["max_leverage"]) == Decimal(75)
    assert Decimal(tiers[0]["maintenance_rate"]) == Decimal("0.0065")


def test_perp_context_refuses_an_instrument_that_is_not_a_perpetual(
    client: TestClient, spot_instrument_id: int
) -> None:
    # Answering with empty filters would read as "no constraints", which
    # is the opposite of the truth for a spot instrument.
    response = client.get(f"/perp-context/{spot_instrument_id}")
    assert response.status_code == 404
    assert "not a perpetual" in response.json()["detail"]


def test_perp_context_404s_for_an_unknown_instrument(client: TestClient) -> None:
    assert client.get("/perp-context/999999999").status_code == 404
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/streaming/test_perp_reference_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.streaming.perp_reference_api'`

- [ ] **Step 3: Write the implementation**

Create `src/trading/streaming/perp_reference_api.py`:

```python
"""Reference data a caller needs before it can size a perpetual order.

`paper.api._require_perp_order_is_tradable` refuses an order that is off
the contract's step or under its floors, which is right -- Binance would
refuse it too. But nothing served those numbers, so a caller could only
discover them by being rejected. DOGE steps by a whole coin, BTC by
0.001, and minimum notionals run 5 / 20 / 50: they are not guessable.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from pydantic import BaseModel

from trading.contracts.enums import AssetClass
from trading.streaming.db import get_db_connection

router = APIRouter()


class PerpContext(BaseModel):
    """Everything fixed about one perpetual, plus its latest funding print.

    Money and rates are strings for the reason `BacktestResponse` gives:
    JSON has no decimal type, and a step size read as a float is a step
    size that silently fails to divide.
    """

    instrument_id: int
    symbol: str
    step_size: str
    min_qty: str
    min_notional: str
    liquidation_fee: str | None
    max_leverage: str | None
    latest_funding_rate: str | None
    latest_funding_time: datetime | None
    latest_mark_price: str | None
    margin_tiers: list[dict[str, str]]


_INSTRUMENT = """
    SELECT symbol, asset_class FROM instruments WHERE instrument_id = %s
"""

_SPEC = """
    SELECT step_size, min_qty, min_notional, liquidation_fee
    FROM perp_contract_specs
    WHERE instrument_id = %s AND effective_to IS NULL
"""

_TIERS = """
    SELECT notional_floor, notional_cap, max_leverage, maintenance_rate, maintenance_amount
    FROM perp_margin_tiers WHERE instrument_id = %s ORDER BY notional_floor
"""

_LATEST_FUNDING = """
    SELECT funding_time, rate, mark_price FROM perp_funding
    WHERE instrument_id = %s ORDER BY funding_time DESC LIMIT 1
"""


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


@router.get("/perp-context/{instrument_id}", response_model=PerpContext)
def get_perp_context(
    instrument_id: int,
    conn: Connection = Depends(get_db_connection),  # noqa: B008
) -> PerpContext:
    instrument = conn.execute(_INSTRUMENT, (instrument_id,)).fetchone()
    if instrument is None:
        raise HTTPException(
            status_code=404, detail=f"no instrument with instrument_id={instrument_id}"
        )
    symbol, asset_class = instrument
    if asset_class != AssetClass.PERP.value:
        # Not an empty answer: empty filters read as "no constraints",
        # which is the opposite of the truth for anything else.
        raise HTTPException(
            status_code=404,
            detail=f"instrument_id={instrument_id} is not a perpetual (asset_class={asset_class})",
        )

    spec = conn.execute(_SPEC, (instrument_id,)).fetchone()
    if spec is None:
        # The same refusal the order path gives. A perpetual with no
        # current spec cannot be sized, and inventing one would invent a
        # trade the venue would never have accepted.
        raise HTTPException(
            status_code=404,
            detail=f"no current contract spec for instrument_id={instrument_id}",
        )
    step_size, min_qty, min_notional, liquidation_fee = spec

    tier_rows = conn.execute(_TIERS, (instrument_id,)).fetchall()
    tiers = [
        {
            "notional_floor": str(floor),
            "notional_cap": str(cap),
            "max_leverage": str(leverage),
            "maintenance_rate": str(rate),
            "maintenance_amount": str(amount),
        }
        for floor, cap, leverage, rate, amount in tier_rows
    ]

    funding = conn.execute(_LATEST_FUNDING, (instrument_id,)).fetchone()
    funding_time, funding_rate, mark_price = funding if funding is not None else (None, None, None)

    return PerpContext(
        instrument_id=instrument_id,
        symbol=symbol,
        step_size=str(step_size),
        min_qty=str(min_qty),
        min_notional=str(min_notional),
        liquidation_fee=_text(liquidation_fee),
        # The first tier's ceiling: leverage above it is refused for any
        # size, so it is the only one a caller can use without knowing
        # its notional yet.
        max_leverage=tiers[0]["max_leverage"] if tiers else None,
        latest_funding_rate=_text(funding_rate),
        latest_funding_time=funding_time,
        latest_mark_price=_text(mark_price),
        margin_tiers=tiers,
    )
```

- [ ] **Step 4: Mount the router on the gateway**

In `src/trading/streaming/gateway.py`, add the import beside the others:

```python
from trading.streaming import market_data_api, perp_reference_api
```

and include it beside the existing routers:

```python
app.include_router(perp_reference_api.router)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/streaming/test_perp_reference_api.py -v`
Expected: PASS, all tests.

- [ ] **Step 6: Check types and lint**

Run: `uv run mypy src/trading/streaming && uv run ruff check src/trading/streaming tests/streaming`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add src/trading/streaming/perp_reference_api.py src/trading/streaming/gateway.py tests/streaming/test_perp_reference_api.py
git commit -m "$(cat <<'MSG'
feat(perp): serve the contract filters a caller needs to size an order

_require_perp_order_is_tradable refuses an order off the contract's step
or under its floors, which is right -- the venue would refuse it too. But
nothing served those numbers, so the only way to learn them was to be
rejected. DOGE steps by a whole coin, BTC by 0.001, minimum notionals run
5 / 20 / 50; none of that is guessable.

A non-perpetual instrument 404s rather than returning empty filters.
Empty filters read as "no constraints", which is the opposite of what is
true for spot.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 7: MCP package, settings, and session scope

**Files:**
- Modify: `pyproject.toml` (add `mcp>=2.1`)
- Modify: `src/trading/config.py` (three settings)
- Create: `src/trading/mcp/__init__.py`
- Create: `src/trading/mcp/session.py`
- Test: `tests/mcp/__init__.py`, `tests/mcp/test_session.py`

**Interfaces:**
- Consumes: `trading.config.get_settings`.
- Produces:
  - `session.AgentSession` — frozen dataclass `(token: str | None, portfolio_id: int)`
  - `session.SessionStore.from_settings(settings) -> SessionStore`
  - `session.SessionStore.resolve(token: str | None) -> AgentSession` — raises `SessionRefused`
  - `session.SessionRefused(Exception)`

- [ ] **Step 1: Add the dependency and settings**

In `pyproject.toml`, add to `dependencies`:

```toml
    "mcp>=2.1",
```

Run: `uv sync`

In `src/trading/config.py`, add to `Settings`:

```python
    # Where the MCP server finds the gateway. It speaks HTTP to the same
    # routes a browser uses rather than touching the database, so that
    # every invariant -- market hours, sufficient cash, contract filters,
    # the breaker -- keeps exactly one enforcement path.
    mcp_gateway_url: str = "http://localhost:8000"

    # Comma-separated `token:portfolio_id` pairs, in the style of
    # `cors_allow_origins`. A token binds an agent to exactly one
    # portfolio: the agent never names a book, so it cannot trade the
    # wrong one. Empty means the HTTP transport refuses every caller.
    mcp_tokens: str = ""

    # Which portfolio the stdio transport trades. There is no token over
    # stdio -- the subprocess is already inside the trust boundary -- so
    # the scope has to come from configuration. None means stdio refuses
    # to start rather than guessing a book.
    mcp_stdio_portfolio_id: int | None = None
```

- [ ] **Step 2: Write the failing tests**

Create `tests/mcp/__init__.py` empty, then `tests/mcp/test_session.py`:

```python
from __future__ import annotations

import pytest

from trading.mcp.session import AgentSession, SessionRefused, SessionStore


def test_a_known_token_resolves_to_its_portfolio() -> None:
    store = SessionStore({"secret-a": 1, "secret-b": 2})
    assert store.resolve("secret-a") == AgentSession(token="secret-a", portfolio_id=1)
    assert store.resolve("secret-b").portfolio_id == 2


def test_an_unknown_token_is_refused() -> None:
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused):
        store.resolve("secret-c")


def test_a_missing_token_is_refused_when_there_is_no_stdio_scope() -> None:
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused):
        store.resolve(None)


def test_a_missing_token_falls_back_to_the_stdio_portfolio() -> None:
    # get_access_token() returns None over stdio; the subprocess is
    # already inside the trust boundary, so scope comes from config.
    store = SessionStore({"secret-a": 1}, stdio_portfolio_id=7)
    assert store.resolve(None).portfolio_id == 7
    assert store.resolve(None).token is None


def test_the_refusal_never_repeats_the_token_back() -> None:
    # A rejected credential must not be echoed into logs or an agent's
    # transcript.
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused) as excinfo:
        store.resolve("hunter2")
    assert "hunter2" not in str(excinfo.value)


def test_settings_parse_comma_separated_token_pairs() -> None:
    store = SessionStore.from_pairs("alpha:1, beta:2", stdio_portfolio_id=None)
    assert store.resolve("alpha").portfolio_id == 1
    assert store.resolve("beta").portfolio_id == 2


def test_an_empty_token_setting_yields_a_store_that_refuses_everything() -> None:
    store = SessionStore.from_pairs("", stdio_portfolio_id=None)
    with pytest.raises(SessionRefused):
        store.resolve("alpha")


def test_a_malformed_token_pair_is_rejected_at_construction() -> None:
    # Failing at startup rather than on the first order: a typo here
    # otherwise surfaces as an authentication failure mid-session.
    with pytest.raises(ValueError):
        SessionStore.from_pairs("alpha-1", stdio_portfolio_id=None)
    with pytest.raises(ValueError):
        SessionStore.from_pairs("alpha:notanumber", stdio_portfolio_id=None)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.mcp'`

- [ ] **Step 4: Write the implementation**

Create `src/trading/mcp/__init__.py`:

```python
"""An MCP surface over the trading platform.

Every tool calls the gateway's HTTP routes. Nothing here opens a database
connection: the routes already enforce market hours, sufficient cash,
contract filters, the charge model and the breaker, and a second
enforcement path is a fork that an agent exploring the surface would
eventually find.

It also keeps blocking psycopg off an async event loop. The gateway's
routes are plain `def` because `async def` plus a blocking driver
deadlocked it once; MCP's handlers are async and get no threadpool.
"""

from __future__ import annotations
```

Create `src/trading/mcp/session.py`:

```python
"""Which portfolio an agent is allowed to trade.

The scope is never a tool parameter. An agent cannot name a book, so a
confused or misled one cannot trade the wrong book -- the only guardrail
between the agent and the order API, and the reason it has to be
airtight.
"""

from __future__ import annotations

from dataclasses import dataclass

from trading.config import Settings


class SessionRefused(Exception):
    """The caller has no valid scope. Never carries the token."""


@dataclass(frozen=True)
class AgentSession:
    """One authenticated agent, bound to one portfolio.

    `token` is `None` over stdio, where `get_access_token()` returns
    nothing because the subprocess is already inside the trust boundary.
    """

    token: str | None
    portfolio_id: int


class SessionStore:
    """Token to portfolio, with an optional stdio fallback."""

    def __init__(
        self, tokens: dict[str, int], stdio_portfolio_id: int | None = None
    ) -> None:
        self._tokens = dict(tokens)
        self._stdio_portfolio_id = stdio_portfolio_id

    @classmethod
    def from_pairs(cls, pairs: str, stdio_portfolio_id: int | None) -> SessionStore:
        """Parse `"token:portfolio_id, token:portfolio_id"`.

        Malformed entries raise here rather than at first use: a typo
        would otherwise surface as an authentication failure in the
        middle of a trading session, which is the worst time to debug
        configuration.
        """
        tokens: dict[str, int] = {}
        for entry in (part.strip() for part in pairs.split(",")):
            if not entry:
                continue
            token, separator, portfolio = entry.partition(":")
            if not separator or not token.strip() or not portfolio.strip():
                raise ValueError(
                    "mcp_tokens entries must look like 'token:portfolio_id'; "
                    f"one entry has no ':' separator (position {len(tokens) + 1})"
                )
            try:
                tokens[token.strip()] = int(portfolio.strip())
            except ValueError:
                raise ValueError(
                    "mcp_tokens portfolio ids must be integers; "
                    f"entry {len(tokens) + 1} is not"
                ) from None
        return cls(tokens, stdio_portfolio_id)

    @classmethod
    def from_settings(cls, settings: Settings) -> SessionStore:
        return cls.from_pairs(settings.mcp_tokens, settings.mcp_stdio_portfolio_id)

    def resolve(self, token: str | None) -> AgentSession:
        """The session for this caller, or `SessionRefused`.

        The refusal never repeats the token back: a rejected credential
        must not reach a log or an agent's transcript.
        """
        if token is None:
            if self._stdio_portfolio_id is None:
                raise SessionRefused(
                    "no bearer token, and no mcp_stdio_portfolio_id is configured"
                )
            return AgentSession(token=None, portfolio_id=self._stdio_portfolio_id)
        portfolio_id = self._tokens.get(token)
        if portfolio_id is None:
            raise SessionRefused("the supplied token is not recognised")
        return AgentSession(token=token, portfolio_id=portfolio_id)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp -v`
Expected: PASS, all tests.

- [ ] **Step 6: Check types and lint**

Run: `uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock src/trading/config.py src/trading/mcp tests/mcp
git commit -m "$(cat <<'MSG'
feat(mcp): bind an agent to exactly one portfolio, by token

The portfolio is never a tool parameter. An agent cannot name a book, so
a confused or a misled one cannot trade the wrong book -- this is the
only guardrail standing between an autonomous caller and the order API,
so it has to be the one thing it cannot talk its way around.

Malformed token configuration raises at construction rather than at first
use. A typo would otherwise surface as an authentication failure in the
middle of a session, which is the worst moment to debug a config file.

A refusal never repeats the token back: a rejected credential must not
reach a log or an agent's transcript.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 8: Gateway client and response formatting

**Files:**
- Create: `src/trading/mcp/client.py`
- Create: `src/trading/mcp/formatting.py`
- Test: `tests/mcp/test_client.py`, `tests/mcp/test_formatting.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `client.GatewayRefusal(Exception)` — `.status_code: int`, `.detail: str`
  - `client.GatewayUnavailable(Exception)`
  - `client.GatewayClient(base_url: str, http: httpx.AsyncClient)`
  - `client.GatewayClient.get(path: str, params: dict[str, object] | None = None) -> Any`
  - `client.GatewayClient.post(path: str, body: dict[str, object]) -> Any`
  - `client.GatewayClient.delete(path: str) -> Any`
  - `formatting.money(value: object) -> str | None`
  - `formatting.refused(detail: str, **extra: object) -> dict[str, object]`
  - `formatting.freshness(last_ts: datetime | None, interval: str, now: datetime) -> dict[str, object]`

**How the spec's error table is realised:** a **refusal is data, not an
exception** — tools return `{"status": "REFUSED", "reason": ...}` so the
agent reads the gateway's own wording and can adapt. Only infrastructure
and session failures raise, which the MCP runtime surfaces to the agent as
an error. This keeps every tool free of transport-specific error plumbing.

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_formatting.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.mcp.formatting import freshness, money, refused


def test_money_renders_a_decimal_as_exact_text() -> None:
    assert money(Decimal("100.25")) == "100.25"


def test_money_passes_a_string_through_untouched() -> None:
    # The gateway already sends money as text; re-parsing risks losing it.
    assert money("1234.5600") == "1234.5600"


def test_money_of_none_is_none() -> None:
    assert money(None) is None


def test_money_never_returns_a_float() -> None:
    assert money(0.1) == "0.1"
    assert isinstance(money(0.1), str)


def test_refused_carries_the_reason_verbatim() -> None:
    result = refused("insufficient cash: order needs 500, portfolio has 100")
    assert result["status"] == "REFUSED"
    assert result["reason"] == "insufficient cash: order needs 500, portfolio has 100"


def test_refused_merges_extra_context() -> None:
    result = refused("market closed", exchange="NSE")
    assert result["exchange"] == "NSE"
    assert result["status"] == "REFUSED"


def test_freshness_of_a_recent_daily_bar_is_not_stale() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(hours=20), "1d", now)
    assert result["stale"] is False
    assert result["warning"] is None
    assert result["age_seconds"] == 72000


def test_freshness_flags_a_daily_bar_older_than_three_days() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(days=17), "1d", now)
    assert result["stale"] is True
    assert "17 days" in str(result["warning"])


def test_freshness_tolerates_a_weekend_on_a_daily_series() -> None:
    # Friday's close read on Monday morning is about 2.7 days old and must
    # not be reported as stale, or every Monday would raise a false alarm.
    now = datetime(2026, 9, 7, 9, 15, tzinfo=UTC)
    result = freshness(now - timedelta(days=2, hours=18), "1d", now)
    assert result["stale"] is False


def test_freshness_flags_a_minute_bar_after_a_few_minutes() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(minutes=10), "1m", now)
    assert result["stale"] is True


def test_freshness_with_no_bars_is_stale_and_says_so() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(None, "1d", now)
    assert result["stale"] is True
    assert result["as_of"] is None
    assert "no bars" in str(result["warning"])
```

Create `tests/mcp/test_client.py`:

```python
from __future__ import annotations

import httpx
import pytest

from trading.mcp.client import GatewayClient, GatewayRefusal, GatewayUnavailable


def _client(handler: httpx.MockTransport) -> GatewayClient:
    return GatewayClient("http://gateway", httpx.AsyncClient(transport=handler))


@pytest.mark.anyio
async def test_get_returns_the_decoded_body() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
    assert await _client(transport).get("/instruments") == {"ok": True}


@pytest.mark.anyio
async def test_a_four_hundred_becomes_a_refusal_carrying_the_detail() -> None:
    detail = "insufficient cash: order needs 500, portfolio has 100"
    transport = httpx.MockTransport(
        lambda request: httpx.Response(400, json={"detail": detail})
    )
    with pytest.raises(GatewayRefusal) as excinfo:
        await _client(transport).post("/orders", {"quantity": "1"})
    assert excinfo.value.detail == detail
    assert excinfo.value.status_code == 400


@pytest.mark.anyio
async def test_a_five_hundred_is_unavailable_not_a_refusal() -> None:
    # A refusal is the platform saying no; a 500 is the platform broken.
    # An agent must not adapt its strategy to a crash.
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="boom"))
    with pytest.raises(GatewayUnavailable):
        await _client(transport).get("/instruments")


@pytest.mark.anyio
async def test_a_read_is_retried_once_on_a_transport_error() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json=[])

    assert await _client(httpx.MockTransport(handler)).get("/instruments") == []
    assert attempts == 2


@pytest.mark.anyio
async def test_a_write_is_never_retried() -> None:
    # A timed-out order may or may not exist. Re-POSTing is how one
    # decision becomes two positions.
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(GatewayUnavailable):
        await _client(httpx.MockTransport(handler)).post("/orders", {})
    assert attempts == 1
```

Add the anyio backend fixture to `tests/mcp/__init__.py`'s sibling — create `tests/mcp/conftest.py`:

```python
from __future__ import annotations

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.mcp.client'`

- [ ] **Step 3: Write `src/trading/mcp/formatting.py`**

```python
"""Shaping every value that crosses the boundary to an agent."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

# How old a bar may be before the series is called stale, as a multiple of
# its own interval. Three is chosen so a daily series survives a weekend:
# Friday's close read on Monday morning is about 2.7 days old, and an
# alarm every Monday is an alarm nobody reads.
_STALE_MULTIPLE = 3

_INTERVAL_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}


def money(value: Any) -> str | None:
    """Any monetary value as exact text, or `None`.

    A string passes through untouched -- the gateway already sends money
    as text, and re-parsing it can only lose precision. A float is
    stringified rather than fed to `Decimal` directly, since
    `Decimal(0.1)` is 0.1000000000000000055… and `Decimal(str(0.1))` is
    the 0.1 that was meant.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return str(value)
    return str(Decimal(str(value)))


def refused(detail: str, **extra: Any) -> dict[str, Any]:
    """A business refusal, shaped as data rather than raised.

    The gateway's wording passes through verbatim. Its refusals already
    say what would satisfy them -- "order needs 500, portfolio has 100" --
    and flattening that into "order failed" would remove the only thing
    an agent could act on.
    """
    return {"status": "REFUSED", "reason": detail, **extra}


def freshness(last_ts: datetime | None, interval: str, now: datetime) -> dict[str, Any]:
    """How current a series is, on every market-data response.

    Not an error, and never a refusal: an agent is entitled to trade a
    stale series if it decides to. It is not entitled to do so without
    being told, which is the failure this exists to prevent.
    """
    if last_ts is None:
        return {
            "as_of": None,
            "age_seconds": None,
            "stale": True,
            "warning": "no bars available for this instrument and interval",
        }
    age_seconds = int((now - last_ts).total_seconds())
    limit = _INTERVAL_SECONDS.get(interval, 86400) * _STALE_MULTIPLE
    stale = age_seconds > limit
    warning: str | None = None
    if stale:
        days, seconds = divmod(max(age_seconds, 0), 86400)
        span = f"{days} days" if days else f"{seconds // 3600} hours"
        warning = (
            f"last {interval} bar is {span} old; this series may not be current, "
            f"and any decision taken from it inherits that"
        )
    return {
        "as_of": last_ts.isoformat(),
        "age_seconds": age_seconds,
        "stale": stale,
        "warning": warning,
    }
```

- [ ] **Step 4: Write `src/trading/mcp/client.py`**

```python
"""Async HTTP to the gateway. The only way this package reaches data.

Never psycopg: the routes enforce market hours, sufficient cash, contract
filters, the charge model and the breaker, and a second path to the
database would fork all of it.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

_TIMEOUT_SECONDS = 60.0


class GatewayRefusal(Exception):
    """The platform said no, and said why. Not a failure of the platform."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class GatewayUnavailable(Exception):
    """The platform could not answer. Distinct from a refusal on purpose.

    An agent should adapt to a refusal and must not adapt to a crash: a
    strategy rewritten around a 500 is a strategy fitted to a bug.
    """


class GatewayClient:
    """One method per HTTP verb, with the retry policy the spec requires."""

    def __init__(self, base_url: str, http: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http

    @classmethod
    def open(cls, base_url: str) -> GatewayClient:
        return cls(base_url, httpx.AsyncClient(timeout=_TIMEOUT_SECONDS))

    async def aclose(self) -> None:
        await self._http.aclose()

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    @staticmethod
    def _decode(response: httpx.Response) -> Any:
        if response.status_code >= 500:
            raise GatewayUnavailable(
                f"gateway returned {response.status_code} for {response.request.url.path}"
            )
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail")
            except ValueError:
                detail = None
            raise GatewayRefusal(response.status_code, str(detail or response.text))
        if not response.content:
            return None
        return response.json()

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Retried once: a read is idempotent, so a dropped connection
        costs nothing to repeat."""
        for attempt in (1, 2):
            try:
                return self._decode(await self._http.get(self._url(path), params=params))
            except httpx.HTTPError as error:
                if attempt == 2:
                    raise GatewayUnavailable(f"GET {path} failed: {error}") from error
                log.warning("mcp.gateway.read_retry", path=path, error=str(error))
        raise AssertionError("unreachable")

    async def post(self, path: str, body: dict[str, Any]) -> Any:
        """Never retried. A timed-out write may already have happened, and
        re-sending it is how one decision becomes two positions. Callers
        that need certainty read the state back instead."""
        try:
            return self._decode(await self._http.post(self._url(path), json=body))
        except httpx.HTTPError as error:
            raise GatewayUnavailable(f"POST {path} failed: {error}") from error

    async def delete(self, path: str) -> Any:
        """Never retried, for the reason `post` gives."""
        try:
            return self._decode(await self._http.delete(self._url(path)))
        except httpx.HTTPError as error:
            raise GatewayUnavailable(f"DELETE {path} failed: {error}") from error
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp -v`
Expected: PASS, all tests.

- [ ] **Step 6: Check types and lint**

Run: `uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add src/trading/mcp/client.py src/trading/mcp/formatting.py tests/mcp
git commit -m "$(cat <<'MSG'
feat(mcp): gateway client, and the line between a refusal and a crash

A refusal is the platform saying no and saying why; a 500 is the platform
broken. They are separate exceptions because an agent should adapt to the
first and must never adapt to the second -- a strategy rewritten around a
crash is a strategy fitted to a bug.

Reads retry once, writes never. A timed-out order may already exist, and
re-POSTing it is exactly how one decision becomes two positions.

Freshness rides on every market-data response rather than being a
refusal. An agent is entitled to trade a stale series; it is not entitled
to do so without being told.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 9: Orientation tools

**Files:**
- Create: `src/trading/mcp/tools.py`
- Test: `tests/mcp/test_tools_orientation.py`

**Interfaces:**
- Consumes: `client.GatewayClient` (Task 8), `session.SessionStore` (Task 7), `indicators.CATALOGUE` (Task 4).
- Produces:
  - `tools.ToolDeps` — frozen dataclass `(client: GatewayClient, sessions: SessionStore, token_provider: Callable[[], str | None])`
  - `tools.register(server: Any, deps: ToolDeps) -> None`
  - `tools.get_capabilities(deps) -> dict[str, Any]`
  - `tools.list_instruments(deps, asset_class: str | None, query: str | None) -> dict[str, Any]`
    — rows carry `instrument_id, symbol, asset_class, exchange` plus the
    added `tradeable` flag. The route serves no segment, lot size or tick
    size; do not invent them. Lot and tick for a perpetual come from
    `get_perp_context`.
  - `tools.get_strategy_contract(deps) -> dict[str, Any]`

**Design note:** every tool is a plain module-level `async def` taking
`deps` first. `register()` wraps them for the MCP server. This keeps the
tests free of MCP machinery and is what lets one `tools.py` serve both
transports.

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_orientation.py`:

```python
from __future__ import annotations

import httpx
import pytest

from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, get_capabilities, get_strategy_contract, list_instruments

# `InstrumentSummary` (gateway.py:100-105) carries exactly these four
# fields -- no segment, no lot size, no tick size. The mock must not
# invent columns the real route does not serve.
_INSTRUMENTS = [
    {"instrument_id": 1, "symbol": "BTCUSDT", "asset_class": "PERP",
     "exchange": "BINANCE_FUTURES"},
    {"instrument_id": 2, "symbol": "RELIANCE", "asset_class": "EQUITY", "exchange": "NSE"},
    {"instrument_id": 3, "symbol": "NIFTY", "asset_class": "INDEX", "exchange": "NSE"},
]


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


def _routes(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/instruments":
        return httpx.Response(200, json=_INSTRUMENTS)
    if request.url.path == "/strategies/contract":
        return httpx.Response(200, json={"version": "1.0", "schema": {"type": "object"}})
    return httpx.Response(404, json={"detail": "no route"})


@pytest.mark.anyio
async def test_capabilities_name_only_the_asset_classes_that_can_be_traded() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert set(result["tradeable_asset_classes"]) == {"EQUITY", "CRYPTO", "PERP"}


@pytest.mark.anyio
async def test_capabilities_list_every_indicator_with_a_description() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert "rsi" in result["indicators"]
    assert result["indicators"]["rsi"]


@pytest.mark.anyio
async def test_capabilities_carry_the_order_vocabulary() -> None:
    result = await get_capabilities(_deps(httpx.MockTransport(_routes)))
    assert set(result["order_types"]) == {"MARKET", "LIMIT"}
    assert set(result["sides"]) == {"BUY", "SELL"}
    assert set(result["products"]) == {"DELIVERY", "INTRADAY"}
    assert set(result["time_in_force"]) == {"DAY", "GTC"}


@pytest.mark.anyio
async def test_list_instruments_returns_every_instrument_by_default() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, None)
    assert len(result["instruments"]) == 3


@pytest.mark.anyio
async def test_list_instruments_filters_by_asset_class() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), "PERP", None)
    assert [i["symbol"] for i in result["instruments"]] == ["BTCUSDT"]


@pytest.mark.anyio
async def test_list_instruments_matches_a_symbol_substring_case_insensitively() -> None:
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, "reli")
    assert [i["symbol"] for i in result["instruments"]] == ["RELIANCE"]


@pytest.mark.anyio
async def test_list_instruments_marks_which_ones_cannot_be_traded() -> None:
    # An INDEX has no charge schedule, so an order in it would be refused.
    # Saying so here saves the agent a rejection it cannot diagnose.
    result = await list_instruments(_deps(httpx.MockTransport(_routes)), None, None)
    by_symbol = {i["symbol"]: i for i in result["instruments"]}
    assert by_symbol["NIFTY"]["tradeable"] is False
    assert by_symbol["RELIANCE"]["tradeable"] is True


@pytest.mark.anyio
async def test_get_strategy_contract_passes_the_bundle_through() -> None:
    result = await get_strategy_contract(_deps(httpx.MockTransport(_routes)))
    assert result["version"] == "1.0"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_orientation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.mcp.tools'`

- [ ] **Step 3: Write `src/trading/mcp/tools.py`**

```python
"""Every MCP tool, defined once and served over both transports.

Each tool is a module-level `async def` taking `ToolDeps` first, so the
tests exercise them without any MCP machinery and `register()` stays a
thin adapter.

Business refusals are returned as data (`formatting.refused`), not
raised: the gateway's refusals already say what would satisfy them, and
an agent can only act on wording it receives.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from trading.indicators import CATALOGUE
from trading.mcp.client import GatewayClient
from trading.mcp.session import AgentSession, SessionStore
from trading.paper.charges import BROKER_BY_ASSET_CLASS


@dataclass(frozen=True)
class ToolDeps:
    """What every tool needs. `token_provider` is the only thing that
    differs between transports: over HTTP it reads the bearer token, over
    stdio it returns `None` and the session falls back to config."""

    client: GatewayClient
    sessions: SessionStore
    token_provider: Callable[[], str | None]


def _session(deps: ToolDeps) -> AgentSession:
    return deps.sessions.resolve(deps.token_provider())


async def get_capabilities(deps: ToolDeps) -> dict[str, Any]:
    """What this platform can actually do, so an agent need not be told.

    `tradeable_asset_classes` comes from `BROKER_BY_ASSET_CLASS` rather
    than from `AssetClass`: the enum lists eight, but an order in one
    without a charge schedule is refused by `MissingChargeSchedule`.
    Advertising the enum would promise five markets that do not exist.
    """
    return {
        "tradeable_asset_classes": sorted(BROKER_BY_ASSET_CLASS),
        "brokers": dict(BROKER_BY_ASSET_CLASS),
        "sides": ["BUY", "SELL"],
        "order_types": ["MARKET", "LIMIT"],
        "products": ["DELIVERY", "INTRADAY"],
        "time_in_force": ["DAY", "GTC"],
        "margin_modes": ["ISOLATED", "CROSS"],
        "bar_intervals": ["1m", "5m", "15m", "1h", "1d"],
        "indicators": dict(CATALOGUE),
        "notes": [
            "leverage is required for a PERP order and meaningless otherwise",
            "rationale is required on every order and is stored with it",
            "an order's portfolio comes from the session, never from a parameter",
        ],
    }


async def list_instruments(
    deps: ToolDeps, asset_class: str | None = None, query: str | None = None
) -> dict[str, Any]:
    """Instruments, optionally filtered, each flagged tradeable or not."""
    rows: list[dict[str, Any]] = await deps.client.get("/instruments")
    if asset_class is not None:
        wanted = asset_class.upper()
        rows = [row for row in rows if row.get("asset_class") == wanted]
    if query is not None:
        needle = query.strip().lower()
        rows = [row for row in rows if needle in str(row.get("symbol", "")).lower()]
    instruments = [
        {**row, "tradeable": row.get("asset_class") in BROKER_BY_ASSET_CLASS} for row in rows
    ]
    return {"count": len(instruments), "instruments": instruments}


async def get_strategy_contract(deps: ToolDeps) -> dict[str, Any]:
    """The contract a strategy script must satisfy, served verbatim.

    Passed through rather than summarised: the validator checks against
    this document, and a paraphrase here would send an agent to write
    against rules that are not the ones enforced.
    """
    bundle: dict[str, Any] = await deps.client.get("/strategies/contract")
    return bundle
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp/test_tools_orientation.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/mcp/tools.py tests/mcp/test_tools_orientation.py
git commit -m "$(cat <<'MSG'
feat(mcp): orientation tools, so an agent can learn the platform itself

get_capabilities reports the asset classes with a charge schedule, not
the eight in the AssetClass enum. Five of those have no schedule and an
order in one is refused by MissingChargeSchedule, so advertising the enum
would promise markets that do not exist and send an agent to trade them.

list_instruments flags each row tradeable or not for the same reason: a
rejection an agent cannot diagnose is worse than a field it can read.

The strategy contract passes through verbatim rather than summarised. The
validator checks against that document, and a paraphrase would send an
agent to write against rules nobody enforces.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 10: Thin market-data tools

**Files:**
- Modify: `src/trading/mcp/tools.py` (append)
- Test: `tests/mcp/test_tools_market_data.py`

**Interfaces:**
- Consumes: `ToolDeps` (Task 9), `formatting.freshness` (Task 8), `GET /candles?precision=string` (Task 5), `GET /perp-context` (Task 6).
- Produces:
  - `tools.get_candles(deps, instrument_id: int, interval: str = "1d", limit: int = 300) -> dict[str, Any]`
  - `tools.get_data_freshness(deps, instrument_ids: list[int], interval: str = "1d") -> dict[str, Any]`
  - `tools.get_perp_context(deps, instrument_id: int) -> dict[str, Any]`
  - `tools._utcnow() -> datetime` — indirection so tests can pin the clock

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_market_data.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools, "_utcnow", lambda: _NOW)


def _candles(count: int, *, last_ts: datetime, close: str = "100.25") -> dict[str, object]:
    return {
        "instrument_id": 1,
        "interval": "1d",
        "candles": [
            {
                "ts": (last_ts - timedelta(days=count - 1 - offset)).isoformat(),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": "1000",
            }
            for offset in range(count)
        ],
    }


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_get_candles_returns_prices_as_strings() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(200, json=_candles(3, last_ts=_NOW))
    )
    result = await tools.get_candles(_deps(handler), 1, "1d", 3)
    assert result["candles"][0]["close"] == "100.25"
    assert isinstance(result["candles"][0]["close"], str)


@pytest.mark.anyio
async def test_get_candles_asks_the_gateway_for_string_precision() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=_candles(3, last_ts=_NOW))

    await tools.get_candles(_deps(httpx.MockTransport(handler)), 1, "1d", 3)
    assert seen["precision"] == "string"


@pytest.mark.anyio
async def test_get_candles_attaches_freshness() -> None:
    stale_end = _NOW - timedelta(days=17)
    handler = httpx.MockTransport(
        lambda request: httpx.Response(200, json=_candles(3, last_ts=stale_end))
    )
    result = await tools.get_candles(_deps(handler), 1, "1d", 3)
    assert result["freshness"]["stale"] is True


@pytest.mark.anyio
async def test_get_candles_refuses_an_unknown_instrument_with_the_gateway_wording() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(404, json={"detail": "no instrument with instrument_id=99"})
    )
    result = await tools.get_candles(_deps(handler), 99, "1d", 3)
    assert result["status"] == "REFUSED"
    assert "no instrument with instrument_id=99" in result["reason"]


@pytest.mark.anyio
async def test_data_freshness_reports_each_instrument_separately() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        instrument_id = int(request.url.path.rsplit("/", 1)[-1])
        last = _NOW if instrument_id == 1 else _NOW - timedelta(days=17)
        return httpx.Response(200, json=_candles(1, last_ts=last))

    result = await tools.get_data_freshness(_deps(httpx.MockTransport(handler)), [1, 2], "1d")
    by_id = {row["instrument_id"]: row for row in result["instruments"]}
    assert by_id[1]["stale"] is False
    assert by_id[2]["stale"] is True


@pytest.mark.anyio
async def test_data_freshness_flags_an_instrument_with_no_bars_at_all() -> None:
    empty = {"instrument_id": 1, "interval": "1d", "candles": []}
    handler = httpx.MockTransport(lambda request: httpx.Response(200, json=empty))
    result = await tools.get_data_freshness(_deps(handler), [1], "1d")
    assert result["instruments"][0]["stale"] is True
    assert result["any_stale"] is True


@pytest.mark.anyio
async def test_get_perp_context_passes_the_contract_filters_through() -> None:
    context = {
        "instrument_id": 1, "symbol": "DOGEUSDT", "step_size": "1", "min_qty": "1",
        "min_notional": "5", "liquidation_fee": "0.015", "max_leverage": "75",
        "latest_funding_rate": "0.0001", "latest_funding_time": "2026-09-01T08:00:00+00:00",
        "latest_mark_price": "0.21", "margin_tiers": [],
    }
    handler = httpx.MockTransport(lambda request: httpx.Response(200, json=context))
    result = await tools.get_perp_context(_deps(handler), 1)
    assert result["step_size"] == "1"
    assert result["min_notional"] == "5"


@pytest.mark.anyio
async def test_get_perp_context_refuses_a_spot_instrument_with_the_reason() -> None:
    handler = httpx.MockTransport(
        lambda request: httpx.Response(
            404, json={"detail": "instrument_id=2 is not a perpetual (asset_class=CRYPTO)"}
        )
    )
    result = await tools.get_perp_context(_deps(handler), 2)
    assert result["status"] == "REFUSED"
    assert "not a perpetual" in result["reason"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_market_data.py -v`
Expected: FAIL — `AttributeError: module 'trading.mcp.tools' has no attribute '_utcnow'`

- [ ] **Step 3: Append to `src/trading/mcp/tools.py`**

Add these imports at the top of the module:

```python
from datetime import UTC, datetime

from trading.mcp.client import GatewayRefusal
from trading.mcp.formatting import freshness, refused
```

Then append:

```python
def _utcnow() -> datetime:
    """Indirection so tests can pin the clock without patching `datetime`."""
    return datetime.now(UTC)


def _last_ts(candles: list[dict[str, Any]]) -> datetime | None:
    if not candles:
        return None
    return datetime.fromisoformat(str(candles[-1]["ts"]))


async def _fetch_candles(
    deps: ToolDeps, instrument_id: int, interval: str, limit: int
) -> list[dict[str, Any]]:
    """Bars as decimal text, oldest first. Raises `GatewayRefusal`."""
    body = await deps.client.get(
        f"/candles/{instrument_id}",
        params={"interval": interval, "limit": limit, "precision": "string"},
    )
    return list(body.get("candles", []))


async def get_candles(
    deps: ToolDeps, instrument_id: int, interval: str = "1d", limit: int = 300
) -> dict[str, Any]:
    """Raw OHLCV for an agent's own analysis, with a freshness verdict."""
    try:
        candles = await _fetch_candles(deps, instrument_id, interval, limit)
    except GatewayRefusal as refusal:
        return refused(refusal.detail, instrument_id=instrument_id)
    return {
        "instrument_id": instrument_id,
        "interval": interval,
        "count": len(candles),
        "candles": candles,
        "freshness": freshness(_last_ts(candles), interval, _utcnow()),
    }


async def get_data_freshness(
    deps: ToolDeps, instrument_ids: list[int], interval: str = "1d"
) -> dict[str, Any]:
    """How current each series is.

    Its own tool rather than only a field on a snapshot, because the
    question "is this database current?" is one an agent should be able
    to ask before it reasons, not only after. The bhavcopy feed has gone
    weeks without a write before now, and a backtest run against it looks
    exactly like one run against fresh data.
    """
    rows: list[dict[str, Any]] = []
    for instrument_id in instrument_ids:
        try:
            candles = await _fetch_candles(deps, instrument_id, interval, 1)
        except GatewayRefusal as refusal:
            rows.append(
                {"instrument_id": instrument_id, "stale": True, "warning": refusal.detail}
            )
            continue
        rows.append(
            {
                "instrument_id": instrument_id,
                **freshness(_last_ts(candles), interval, _utcnow()),
            }
        )
    return {
        "interval": interval,
        "any_stale": any(row["stale"] for row in rows),
        "instruments": rows,
    }


async def get_perp_context(deps: ToolDeps, instrument_id: int) -> dict[str, Any]:
    """Contract filters and the latest funding print for one perpetual.

    Sizing a perpetual without these is guesswork: DOGE steps by a whole
    coin, BTC by 0.001, and the order path refuses anything off-step.
    """
    try:
        return dict(await deps.client.get(f"/perp-context/{instrument_id}"))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, instrument_id=instrument_id)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp/test_tools_market_data.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/mcp/tools.py tests/mcp/test_tools_market_data.py
git commit -m "$(cat <<'MSG'
feat(mcp): candles, freshness and perpetual context

Freshness is its own tool, not only a field on a snapshot. "Is this
database current?" is a question an agent should be able to ask before it
reasons rather than after it has traded: the bhavcopy feed has gone weeks
without a write, and a backtest run against a stale database looks exactly
like one run against a fresh one.

Candles are requested with precision=string. An indicator or a position
size computed from a rounded close is wrong in a way nothing downstream
can detect.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 11: `get_market_snapshot`

**Files:**
- Modify: `src/trading/mcp/tools.py` (append)
- Test: `tests/mcp/test_tools_snapshot.py`

**Interfaces:**
- Consumes: `_fetch_candles`, `_utcnow`, `freshness` (Task 10); `indicators.parse`, `compute`, `warmup_for` (Task 4).
- Produces: `tools.get_market_snapshot(deps, instrument_ids: list[int], interval: str = "1d", indicators: list[str] | None = None, history: int = 50) -> dict[str, Any]`

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_snapshot.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools, "_utcnow", lambda: _NOW)


def _rising_candles(count: int) -> dict[str, object]:
    return {
        "instrument_id": 1,
        "interval": "1d",
        "candles": [
            {
                "ts": (_NOW - timedelta(days=count - 1 - offset)).isoformat(),
                "open": str(offset + 1),
                "high": str(offset + 2),
                "low": str(offset),
                "close": str(offset + 1),
                "volume": "1000",
            }
            for offset in range(count)
        ],
    }


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


def _serving(count: int) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(200, json=_rising_candles(count)))


@pytest.mark.anyio
async def test_snapshot_returns_the_last_price_as_a_string() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(120)), [1], "1d", [], 5)
    instrument = result["instruments"][0]
    assert instrument["last_price"] == "120"
    assert isinstance(instrument["last_price"], str)


@pytest.mark.anyio
async def test_snapshot_trims_bars_to_the_requested_history() -> None:
    # The warmup is fetched but must not be dumped on the agent.
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    assert len(result["instruments"][0]["bars"]) == 5


@pytest.mark.anyio
async def test_snapshot_requests_history_plus_warmup_from_the_gateway() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=_rising_candles(200))

    # rsi14 needs max(5*14, 50) = 70 warmup bars on top of 5 of history.
    await tools.get_market_snapshot(_deps(httpx.MockTransport(handler)), [1], "1d", ["rsi14"], 5)
    assert int(seen["limit"]) == 75


@pytest.mark.anyio
async def test_snapshot_computes_the_requested_indicators() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    # A series that only rises has no losses, so Wilder RSI pins at 100.
    assert Decimal(result["instruments"][0]["indicators"]["rsi14"]) == Decimal(100)


@pytest.mark.anyio
async def test_snapshot_reports_multi_valued_indicators_as_a_mapping() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["bb20"], 5)
    bands = result["instruments"][0]["indicators"]["bb20"]
    assert set(bands) == {"lower", "mid", "upper"}
    assert isinstance(bands["mid"], str)


@pytest.mark.anyio
async def test_snapshot_says_when_the_warmup_was_not_available() -> None:
    # Only 30 bars exist, but rsi14 asked for 5 + 70. The number must not
    # be presented as though it converged.
    result = await tools.get_market_snapshot(_deps(_serving(30)), [1], "1d", ["rsi14"], 5)
    instrument = result["instruments"][0]
    assert instrument["warmup_sufficient"] is False
    assert instrument["warmup_bars_used"] == 70


@pytest.mark.anyio
async def test_snapshot_marks_warmup_sufficient_when_the_data_is_there() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["rsi14"], 5)
    assert result["instruments"][0]["warmup_sufficient"] is True


@pytest.mark.anyio
async def test_snapshot_reports_none_for_an_indicator_with_too_little_data() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(10)), [1], "1d", ["rsi14"], 5)
    assert result["instruments"][0]["indicators"]["rsi14"] is None


@pytest.mark.anyio
async def test_snapshot_refuses_an_unknown_indicator_and_names_the_known_ones() -> None:
    # Refused outright rather than skipped: a snapshot missing what the
    # agent asked for, but shaped as though complete, is the worse failure.
    result = await tools.get_market_snapshot(_deps(_serving(200)), [1], "1d", ["supertrend"], 5)
    assert result["status"] == "REFUSED"
    assert "supertrend" in result["reason"]


@pytest.mark.anyio
async def test_snapshot_carries_freshness_per_instrument() -> None:
    result = await tools.get_market_snapshot(_deps(_serving(120)), [1], "1d", [], 5)
    assert result["instruments"][0]["freshness"]["stale"] is False


@pytest.mark.anyio
async def test_snapshot_reports_one_instrument_refusal_without_losing_the_others() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/99"):
            return httpx.Response(404, json={"detail": "no instrument with instrument_id=99"})
        return httpx.Response(200, json=_rising_candles(120))

    result = await tools.get_market_snapshot(
        _deps(httpx.MockTransport(handler)), [1, 99], "1d", [], 5
    )
    by_id = {row["instrument_id"]: row for row in result["instruments"]}
    assert by_id[1]["last_price"] == "120"
    assert by_id[99]["status"] == "REFUSED"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_snapshot.py -v`
Expected: FAIL — `AttributeError: module 'trading.mcp.tools' has no attribute 'get_market_snapshot'`

- [ ] **Step 3: Append to `src/trading/mcp/tools.py`**

Add to the imports:

```python
from decimal import Decimal

from trading.indicators import IndicatorRequest, compute, parse, warmup_for
from trading.mcp.formatting import money
```

Then append:

```python
def _column(candles: list[dict[str, Any]], field: str) -> list[Decimal]:
    return [Decimal(str(candle[field])) for candle in candles]


def _rendered(value: Decimal | dict[str, Decimal] | None) -> Any:
    """Indicator output as text, preserving the shape.

    `None` survives as `None` rather than becoming a number: an indicator
    that could not be computed must not be indistinguishable from one
    that computed to zero.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: money(inner) for key, inner in value.items()}
    return money(value)


async def _snapshot_one(
    deps: ToolDeps,
    instrument_id: int,
    interval: str,
    requests: list[IndicatorRequest],
    history: int,
    warmup: int,
) -> dict[str, Any]:
    try:
        candles = await _fetch_candles(deps, instrument_id, interval, history + warmup)
    except GatewayRefusal as refusal:
        # One bad instrument must not lose the others: an agent watching
        # six symbols should still see five when the sixth is unknown.
        return refused(refusal.detail, instrument_id=instrument_id)

    highs = _column(candles, "high")
    lows = _column(candles, "low")
    closes = _column(candles, "close")
    computed = {
        request.token: _rendered(compute(request, highs=highs, lows=lows, closes=closes))
        for request in requests
    }
    return {
        "instrument_id": instrument_id,
        "interval": interval,
        "last_price": money(closes[-1]) if closes else None,
        "bars": candles[-history:] if history else [],
        "indicators": computed,
        "warmup_bars_used": warmup,
        # Whether the database could supply the run-up the indicators
        # needed. An RSI computed from 15 bars is not the RSI computed
        # from 100, and a caller told nothing would never know which it
        # holds.
        "warmup_sufficient": len(candles) >= history + warmup,
        "freshness": freshness(_last_ts(candles), interval, _utcnow()),
    }


async def get_market_snapshot(
    deps: ToolDeps,
    instrument_ids: list[int],
    interval: str = "1d",
    indicators: list[str] | None = None,
    history: int = 50,
) -> dict[str, Any]:
    """Current state of several instruments, with indicators computed here.

    Indicators are computed server-side from decimal bars rather than
    handed over as raw OHLCV for the caller to reduce: a language model
    doing Wilder smoothing over 200 rows in its head produces a number
    that looks right and is not, and nothing downstream would catch it.
    """
    try:
        requests = [parse(token) for token in (indicators or [])]
    except ValueError as error:
        # The whole call is refused, not the one token. A snapshot missing
        # what the agent asked for, but shaped as though complete, is the
        # worse failure.
        return refused(str(error))

    warmup = warmup_for(requests)
    rows = [
        await _snapshot_one(deps, instrument_id, interval, requests, history, warmup)
        for instrument_id in instrument_ids
    ]
    return {
        "interval": interval,
        "requested_indicators": [request.token for request in requests],
        "history": history,
        "instruments": rows,
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp/test_tools_snapshot.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types and lint**

Run: `uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/trading/mcp/tools.py tests/mcp/test_tools_snapshot.py
git commit -m "$(cat <<'MSG'
feat(mcp): market snapshot with indicators computed from decimal bars

Indicators are computed here rather than handed over as raw OHLCV for the
caller to reduce. A language model doing Wilder smoothing across 200 rows
in its head produces a number that looks right and is not, and nothing
downstream would catch it.

The snapshot fetches history plus warmup and reports both
warmup_bars_used and warmup_sufficient. A 14-period RSI computed from 15
bars is not the one computed from 100; a caller told nothing would never
know which it is holding, and would compare it against a backtest that
used the other.

One unknown instrument refuses only its own row. An agent watching six
symbols should still see five when the sixth is wrong.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 12: Portfolio read tools

**Files:**
- Modify: `src/trading/mcp/tools.py` (append)
- Test: `tests/mcp/test_tools_portfolio.py`

**Interfaces:**
- Consumes: `_session` (Task 9), `refused`, `money` (Task 8).
- Produces:
  - `tools.get_portfolio_state(deps) -> dict[str, Any]`
  - `tools.get_perp_positions(deps) -> dict[str, Any]`
  - `tools.list_orders(deps, status: str | None = None, limit: int = 50) -> dict[str, Any]`

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_portfolio.py`:

```python
from __future__ import annotations

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionRefused, SessionStore
from trading.mcp.tools import ToolDeps

_PORTFOLIOS = [
    {"portfolio_id": 1, "user_id": 1, "name": "agent", "base_currency": "INR",
     "initial_capital": "100000", "cash_balance": "95000.50", "status": "ACTIVE",
     "max_daily_loss": None, "max_drawdown_pct": None, "margin_mode": "ISOLATED"},
    {"portfolio_id": 2, "user_id": 1, "name": "mine", "base_currency": "INR",
     "initial_capital": "500000", "cash_balance": "500000", "status": "ACTIVE",
     "max_daily_loss": None, "max_drawdown_pct": None, "margin_mode": "CROSS"},
]
_POSITIONS = [{"instrument_id": 7, "quantity": "10", "average_price": "100.25"}]
_ORDERS = [
    {"order_id": 11, "portfolio_id": 1, "instrument_id": 7, "status": "OPEN",
     "side": "BUY", "quantity": "10"},
    {"order_id": 12, "portfolio_id": 1, "instrument_id": 7, "status": "FILLED",
     "side": "BUY", "quantity": "5"},
]


def _deps(handler: httpx.MockTransport, token: str | None = "tok") -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: token,
    )


def _routes(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/portfolios":
        return httpx.Response(200, json=_PORTFOLIOS)
    if path == "/portfolios/1/positions":
        return httpx.Response(200, json=_POSITIONS)
    if path == "/portfolios/1/perp-positions":
        return httpx.Response(200, json=[{"instrument_id": 9, "quantity": "100"}])
    if path == "/orders":
        return httpx.Response(200, json=_ORDERS)
    return httpx.Response(404, json={"detail": f"no route {path}"})


@pytest.mark.anyio
async def test_portfolio_state_returns_only_the_session_portfolio() -> None:
    # Portfolio 2 exists and belongs to the same user; the session must
    # not be able to see it.
    result = await tools.get_portfolio_state(_deps(httpx.MockTransport(_routes)))
    assert result["portfolio"]["portfolio_id"] == 1
    assert "2" not in str(result["portfolio"]["portfolio_id"])


@pytest.mark.anyio
async def test_portfolio_state_carries_cash_as_a_string() -> None:
    result = await tools.get_portfolio_state(_deps(httpx.MockTransport(_routes)))
    assert result["portfolio"]["cash_balance"] == "95000.50"


@pytest.mark.anyio
async def test_portfolio_state_includes_positions_and_open_orders() -> None:
    result = await tools.get_portfolio_state(_deps(httpx.MockTransport(_routes)))
    assert result["positions"] == _POSITIONS
    assert [o["order_id"] for o in result["open_orders"]] == [11]


@pytest.mark.anyio
async def test_an_unknown_token_refuses_before_any_request_is_made() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("the gateway must not be reached without a valid session")

    with pytest.raises(SessionRefused):
        await tools.get_portfolio_state(_deps(httpx.MockTransport(handler), token="wrong"))


@pytest.mark.anyio
async def test_perp_positions_are_scoped_to_the_session_portfolio() -> None:
    result = await tools.get_perp_positions(_deps(httpx.MockTransport(_routes)))
    assert result["positions"][0]["instrument_id"] == 9


@pytest.mark.anyio
async def test_list_orders_filters_by_status() -> None:
    result = await tools.list_orders(_deps(httpx.MockTransport(_routes)), status="FILLED")
    assert [o["order_id"] for o in result["orders"]] == [12]


@pytest.mark.anyio
async def test_orders_are_requested_with_the_required_portfolio_parameter() -> None:
    # GET /orders takes portfolio_id as a REQUIRED query parameter.
    # Omitting it is a 422, so the scoping must be sent, not only applied
    # after the fact.
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/orders":
            seen.update(dict(request.url.params))
            return httpx.Response(200, json=_ORDERS)
        return _routes(request)

    await tools.list_orders(_deps(httpx.MockTransport(handler)))
    assert seen["portfolio_id"] == "1"


@pytest.mark.anyio
async def test_list_orders_respects_the_limit() -> None:
    result = await tools.list_orders(_deps(httpx.MockTransport(_routes)), limit=1)
    assert len(result["orders"]) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_portfolio.py -v`
Expected: FAIL — `AttributeError: module 'trading.mcp.tools' has no attribute 'get_portfolio_state'`

- [ ] **Step 3: Append to `src/trading/mcp/tools.py`**

```python
_OPEN_ORDER_STATUSES = frozenset({"PENDING", "OPEN", "PARTIALLY_FILLED"})


async def get_portfolio_state(deps: ToolDeps) -> dict[str, Any]:
    """Cash, positions and working orders for the session's portfolio.

    The portfolio is filtered here from the session, never passed as a
    parameter. `GET /portfolios` returns every book the operator owns,
    and an agent that could name one could name the wrong one.
    """
    session = _session(deps)
    portfolios: list[dict[str, Any]] = await deps.client.get("/portfolios")
    mine = next(
        (row for row in portfolios if row.get("portfolio_id") == session.portfolio_id), None
    )
    if mine is None:
        return refused(
            f"the session's portfolio_id={session.portfolio_id} does not exist; "
            f"check the mcp_tokens configuration"
        )
    positions = await deps.client.get(f"/portfolios/{session.portfolio_id}/positions")
    # `portfolio_id` is a REQUIRED query parameter on this route, not an
    # optional filter: omitting it is a 422, and an unknown id is a 404
    # rather than an empty list. The scoping therefore happens server-side;
    # the comprehension below is belt-and-braces against a future change.
    orders: list[dict[str, Any]] = await deps.client.get(
        "/orders", params={"portfolio_id": session.portfolio_id, "limit": 500}
    )
    mine_orders = [row for row in orders if row.get("portfolio_id") == session.portfolio_id]
    return {
        "portfolio": mine,
        "positions": positions,
        "open_orders": [
            row for row in mine_orders if row.get("status") in _OPEN_ORDER_STATUSES
        ],
    }


async def get_perp_positions(deps: ToolDeps) -> dict[str, Any]:
    """Open perpetual positions with their margin and liquidation price."""
    session = _session(deps)
    positions = await deps.client.get(f"/portfolios/{session.portfolio_id}/perp-positions")
    return {"portfolio_id": session.portfolio_id, "positions": positions}


async def list_orders(
    deps: ToolDeps, status: str | None = None, limit: int = 50
) -> dict[str, Any]:
    """The session portfolio's order blotter, newest last."""
    session = _session(deps)
    # `portfolio_id` is required by the route; `limit` is capped at 500.
    orders: list[dict[str, Any]] = await deps.client.get(
        "/orders", params={"portfolio_id": session.portfolio_id, "limit": min(limit, 500)}
    )
    rows = [row for row in orders if row.get("portfolio_id") == session.portfolio_id]
    if status is not None:
        wanted = status.upper()
        rows = [row for row in rows if row.get("status") == wanted]
    return {"count": len(rows[:limit]), "orders": rows[:limit]}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mcp/test_tools_portfolio.py -v`
Expected: PASS, all tests.

- [ ] **Step 5: Check types, lint, and commit**

```bash
uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp
git add src/trading/mcp/tools.py tests/mcp/test_tools_portfolio.py
git commit -m "$(cat <<'MSG'
feat(mcp): portfolio state, scoped to the session and nothing else

GET /portfolios returns every book the operator owns. The filter happens
here, from the session, so an agent never receives a portfolio it was not
given -- and never learns that the others exist.

An unrecognised token refuses before any request is made. A rejected
credential should cost the gateway nothing.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 13: Execution tools

**Files:**
- Modify: `src/trading/mcp/tools.py` (append)
- Test: `tests/mcp/test_tools_execution.py`

> ### Spec deviation — read before implementing
>
> The spec says: *"The MCP layer never re-POSTs. It re-queries `GET
> /orders` by idempotency key."* Both halves turn out to be wrong against
> the real API, and this task deliberately does something better:
>
> 1. **The re-query is impossible.** `_ORDER_COLUMNS`
>    (`paper/api.py:108-113`) does not include `idempotency_key`, so the
>    order list cannot be searched by it.
> 2. **The re-query is unnecessary.** `POST /orders` is genuinely
>    idempotent: `idempotency_key` carries a unique constraint, and
>    `_insert_order` (`paper/api.py:524-576`) recovers from
>    `UniqueViolation` by returning the order that won the race.
>
> So a timed-out write **is** retried exactly once, with the *same*
> idempotency key. If the first attempt committed, the retry returns that
> order; if it did not, the retry creates it. Exactly one order exists
> either way — which is the property the spec wanted, reached by the
> mechanism the platform already provides.

**Interfaces:**
- Consumes: `_session`, `_utcnow`, `refused`, `GatewayRefusal`, `GatewayUnavailable`.
- Produces:
  - `tools.place_order(deps, instrument_id: int, side: str, order_type: str, quantity: str, product: str, rationale: str, limit_price: str | None = None, time_in_force: str = "DAY", leverage: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]`
  - `tools.cancel_order(deps, order_id: int) -> dict[str, Any]`
  - `tools._derive_idempotency_key(...) -> str`

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_execution.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_NOW = datetime(2026, 9, 7, 12, 30, 15, tzinfo=UTC)
_ORDER = {"order_id": 11, "portfolio_id": 1, "instrument_id": 7, "status": "PENDING",
          "side": "BUY", "quantity": "10", "order_type": "MARKET"}


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools, "_utcnow", lambda: _NOW)


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


async def _place(deps: ToolDeps, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "instrument_id": 7, "side": "BUY", "order_type": "MARKET", "quantity": "10",
        "product": "DELIVERY", "rationale": "momentum breakout",
    }
    kwargs.update(overrides)
    return await tools.place_order(deps, **kwargs)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_place_order_injects_the_session_portfolio() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(request.read() and __import__("json").loads(request.read()))
        return httpx.Response(201, json=_ORDER)

    await _place(_deps(httpx.MockTransport(handler)))
    assert sent["portfolio_id"] == 1


@pytest.mark.anyio
async def test_place_order_ignores_a_caller_supplied_portfolio_id() -> None:
    # `portfolio_id` is not a parameter at all, so this must be a
    # TypeError rather than a silently honoured override.
    with pytest.raises(TypeError):
        await _place(_deps(httpx.MockTransport(lambda r: httpx.Response(201, json=_ORDER))),
                     portfolio_id=2)


@pytest.mark.anyio
async def test_place_order_derives_a_stable_key_within_the_same_minute() -> None:
    first = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    second = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    assert first == second


@pytest.mark.anyio
async def test_a_different_quantity_derives_a_different_key() -> None:
    first = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "10", None, _NOW)
    second = tools._derive_idempotency_key(1, 7, "BUY", "MARKET", "11", None, _NOW)
    assert first != second


@pytest.mark.anyio
async def test_an_explicit_idempotency_key_is_used_unchanged() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(__import__("json").loads(request.read()))
        return httpx.Response(201, json=_ORDER)

    await _place(_deps(httpx.MockTransport(handler)), idempotency_key="mine-1")
    assert sent["idempotency_key"] == "mine-1"


@pytest.mark.anyio
async def test_a_refusal_reaches_the_agent_with_the_gateway_wording() -> None:
    detail = "insufficient cash: order needs 500, portfolio has 100"
    handler = httpx.MockTransport(lambda r: httpx.Response(400, json={"detail": detail}))
    result = await _place(_deps(handler))
    assert result["status"] == "REFUSED"
    assert result["reason"] == detail


@pytest.mark.anyio
async def test_a_market_closed_refusal_is_not_an_exception() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(400, json={"detail": "market closed for NSE CM on 2026-09-07"})
    )
    result = await _place(_deps(handler))
    assert result["status"] == "REFUSED"
    assert "market closed" in result["reason"]


@pytest.mark.anyio
async def test_a_timed_out_write_is_retried_once_with_the_same_key() -> None:
    # POST /orders is idempotent on idempotency_key, so retrying with the
    # same key yields exactly one order whether or not the first attempt
    # committed.
    attempts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = __import__("json").loads(request.read())
        attempts.append(str(body["idempotency_key"]))
        if len(attempts) == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(201, json=_ORDER)

    result = await _place(_deps(httpx.MockTransport(handler)))
    assert result["order_id"] == 11
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]


@pytest.mark.anyio
async def test_a_write_that_keeps_timing_out_reports_the_key_to_reconcile_with() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    result = await _place(_deps(httpx.MockTransport(handler)))
    assert result["status"] == "UNKNOWN"
    assert result["idempotency_key"]
    assert "may or may not" in str(result["reason"])


@pytest.mark.anyio
async def test_cancel_order_returns_the_cancelled_order() -> None:
    cancelled = {**_ORDER, "status": "CANCELLED"}
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=cancelled))
    result = await tools.cancel_order(_deps(handler), 11)
    assert result["status"] == "CANCELLED"


@pytest.mark.anyio
async def test_cancel_order_surfaces_a_refusal_verbatim() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(400, json={"detail": "order 11 is already FILLED"})
    )
    result = await tools.cancel_order(_deps(handler), 11)
    assert result["status"] == "REFUSED"
    assert "already FILLED" in result["reason"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_execution.py -v`
Expected: FAIL — `AttributeError: module 'trading.mcp.tools' has no attribute 'place_order'`

- [ ] **Step 3: Append to `src/trading/mcp/tools.py`**

Add to the imports:

```python
import hashlib

from trading.mcp.client import GatewayUnavailable
```

Then append:

```python
def _derive_idempotency_key(
    portfolio_id: int,
    instrument_id: int,
    side: str,
    order_type: str,
    quantity: str,
    limit_price: str | None,
    now: datetime,
) -> str:
    """A key that is stable across retries of the same decision.

    Bucketed to the minute: an agent that retries a decision seconds
    later means the same order, and two orders would be the wrong answer.
    An agent that genuinely wants to buy twice in one minute passes its
    own key -- which is why the parameter stays.
    """
    material = "|".join(
        [
            str(portfolio_id),
            str(instrument_id),
            side,
            order_type,
            quantity,
            limit_price or "",
            now.strftime("%Y-%m-%dT%H:%M"),
        ]
    )
    return hashlib.sha256(material.encode()).hexdigest()


async def place_order(
    deps: ToolDeps,
    instrument_id: int,
    side: str,
    order_type: str,
    quantity: str,
    product: str,
    rationale: str,
    limit_price: str | None = None,
    time_in_force: str = "DAY",
    leverage: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Place one order in the session's portfolio.

    There is no `portfolio_id` parameter. The book comes from the session,
    so a confused or misled agent cannot trade the wrong one.

    `rationale` is required by the API and stored with the order. It is
    the only record of why an autonomous decision was taken, so it is
    passed through rather than defaulted.
    """
    session = _session(deps)
    key = idempotency_key or _derive_idempotency_key(
        session.portfolio_id, instrument_id, side, order_type, quantity, limit_price, _utcnow()
    )
    body: dict[str, Any] = {
        "portfolio_id": session.portfolio_id,
        "instrument_id": instrument_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "product": product,
        "time_in_force": time_in_force,
        "rationale": rationale,
        "idempotency_key": key,
    }
    if limit_price is not None:
        body["limit_price"] = limit_price
    if leverage is not None:
        body["leverage"] = leverage

    for attempt in (1, 2):
        try:
            return dict(await deps.client.post("/orders", body))
        except GatewayRefusal as refusal:
            return refused(refusal.detail, idempotency_key=key)
        except GatewayUnavailable as unavailable:
            # Safe to repeat: idempotency_key carries a unique constraint
            # and `_insert_order` returns the winner of a race rather than
            # creating a second order. Exactly one order exists whether or
            # not the first attempt reached the database.
            if attempt == 2:
                return {
                    "status": "UNKNOWN",
                    "reason": (
                        f"the gateway did not answer, so this order may or may not have been "
                        f"placed: {unavailable}. Call list_orders before retrying; re-sending "
                        f"with the same idempotency_key will not create a second order."
                    ),
                    "idempotency_key": key,
                }
    raise AssertionError("unreachable")


async def cancel_order(deps: ToolDeps, order_id: int) -> dict[str, Any]:
    """Cancel a working order. Refusals carry the gateway's wording."""
    _session(deps)
    try:
        return dict(await deps.client.delete(f"/orders/{order_id}"))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, order_id=order_id)
```

- [ ] **Step 4: Run the tests, check types, and commit**

Run: `uv run pytest tests/mcp/test_tools_execution.py -v`
Expected: PASS, all tests.

```bash
uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp
git add src/trading/mcp/tools.py tests/mcp/test_tools_execution.py
git commit -m "$(cat <<'MSG'
feat(mcp): place and cancel orders, with retries that cannot double-fill

place_order has no portfolio_id parameter. The book comes from the
session, so a confused or a misled agent cannot trade the wrong one.

idempotency_key becomes optional and is derived from the decision itself,
bucketed to the minute. An agent that retries seconds later means the
same order; two orders would be the wrong answer. One that genuinely
wants to buy twice in a minute still passes its own key.

A timed-out write is retried once with the same key rather than being
abandoned. POST /orders holds a unique constraint on idempotency_key and
_insert_order returns the winner of a race, so exactly one order exists
whether or not the first attempt reached the database. If it times out
twice the tool reports UNKNOWN and hands back the key, because claiming
either outcome would be a guess about real money.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 14: Backtesting tools

**Files:**
- Modify: `src/trading/mcp/tools.py` (append)
- Test: `tests/mcp/test_tools_backtest.py`

**Interfaces:**
- Consumes: `_session`, `refused`, `GatewayRefusal`.
- Produces:
  - `tools.submit_strategy(deps, name: str, python_code: str) -> dict[str, Any]`
  - `tools.run_backtest(deps, strategy_id: int, start: str, end: str, starting_cash: str | None = None, max_daily_loss: str | None = None, max_drawdown_pct: str | None = None) -> dict[str, Any]`
  - `tools.get_backtest(deps, backtest_run_id: int) -> dict[str, Any]`

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_tools_backtest.py`:

```python
from __future__ import annotations

import json

import httpx
import pytest

from trading.mcp import tools
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps

_ACCEPTED = {"strategy_id": 3, "status": "ACCEPTED", "findings": []}
_REJECTED = {
    "strategy_id": None,
    "status": "REJECTED",
    "findings": [{"code": "E_IMPORT", "message": "import of 'socket' is not permitted"}],
}
_PASSED = {
    "strategy_id": 3, "status": "PASSED", "backtest_run_id": 9, "bars": "1d",
    "fills": 12, "final_equity": "104200.75",
    "equity_curve": [{"ts": "2026-01-01T00:00:00+00:00", "equity": "100000", "cash": "100000"}],
    "fills_ledger": [], "findings": [], "notes": [],
}


def _deps(handler: httpx.MockTransport) -> ToolDeps:
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=handler)),
        sessions=SessionStore({"tok": 1}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_submit_strategy_sends_the_code_and_returns_the_verdict() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(200, json=_ACCEPTED)

    result = await tools.submit_strategy(_deps(httpx.MockTransport(handler)), "ema-cross", "code")
    assert sent["name"] == "ema-cross"
    assert result["strategy_id"] == 3


@pytest.mark.anyio
async def test_a_rejected_strategy_returns_its_findings_rather_than_an_error() -> None:
    # The findings are the whole point: they tell the agent what to fix.
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_REJECTED))
    result = await tools.submit_strategy(_deps(handler), "bad", "import socket")
    assert result["status"] == "REJECTED"
    assert "socket" in result["findings"][0]["message"]


@pytest.mark.anyio
async def test_run_backtest_passes_the_window_through() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")
    assert sent["start"] == "2024-01-01"
    assert sent["end"] == "2026-09-01"


@pytest.mark.anyio
async def test_run_backtest_omits_overrides_that_were_not_given() -> None:
    # Sending starting_cash=None would override the manifest with nothing.
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(_deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01")
    assert "starting_cash" not in sent


@pytest.mark.anyio
async def test_run_backtest_forwards_an_explicit_starting_cash() -> None:
    sent: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(200, json=_PASSED)

    await tools.run_backtest(
        _deps(httpx.MockTransport(handler)), 3, "2024-01-01", "2026-09-01",
        starting_cash="50000",
    )
    assert sent["starting_cash"] == "50000"


@pytest.mark.anyio
async def test_run_backtest_returns_the_curve_and_the_metrics() -> None:
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_PASSED))
    result = await tools.run_backtest(_deps(handler), 3, "2024-01-01", "2026-09-01")
    assert result["final_equity"] == "104200.75"
    assert result["equity_curve"][0]["equity"] == "100000"


@pytest.mark.anyio
async def test_run_backtest_on_an_unknown_strategy_is_a_refusal() -> None:
    handler = httpx.MockTransport(
        lambda r: httpx.Response(404, json={"detail": "no strategy with strategy_id=99"})
    )
    result = await tools.run_backtest(_deps(handler), 99, "2024-01-01", "2026-09-01")
    assert result["status"] == "REFUSED"
    assert "no strategy" in result["reason"]


@pytest.mark.anyio
async def test_get_backtest_reads_a_stored_run() -> None:
    handler = httpx.MockTransport(lambda r: httpx.Response(200, json=_PASSED))
    assert (await tools.get_backtest(_deps(handler), 9))["backtest_run_id"] == 9
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_tools_backtest.py -v`
Expected: FAIL — `AttributeError: module 'trading.mcp.tools' has no attribute 'submit_strategy'`

- [ ] **Step 3: Append to `src/trading/mcp/tools.py`**

```python
async def submit_strategy(deps: ToolDeps, name: str, python_code: str) -> dict[str, Any]:
    """Register a strategy: validated statically, then smoke-run sandboxed.

    A rejection comes back as findings rather than an error, because the
    findings are the useful part -- they say what to change. An agent
    iterating on a strategy reads them and resubmits.
    """
    _session(deps)
    try:
        return dict(await deps.client.post("/strategies", {"name": name, "code": python_code}))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, name=name)


async def run_backtest(
    deps: ToolDeps,
    strategy_id: int,
    start: str,
    end: str,
    starting_cash: str | None = None,
    max_daily_loss: str | None = None,
    max_drawdown_pct: str | None = None,
) -> dict[str, Any]:
    """Replay a registered strategy over a window and return its metrics.

    Blocks for the run, matching the route. Overrides that were not given
    are omitted from the body rather than sent as null: the route reads a
    missing field as "use the strategy's own", and an explicit null would
    override the manifest with nothing.
    """
    _session(deps)
    body: dict[str, Any] = {"start": start, "end": end}
    if starting_cash is not None:
        body["starting_cash"] = starting_cash
    if max_daily_loss is not None:
        body["max_daily_loss"] = max_daily_loss
    if max_drawdown_pct is not None:
        body["max_drawdown_pct"] = max_drawdown_pct
    try:
        return dict(await deps.client.post(f"/strategies/{strategy_id}/backtests", body))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, strategy_id=strategy_id)


async def get_backtest(deps: ToolDeps, backtest_run_id: int) -> dict[str, Any]:
    """Re-read a stored run, with its curve and itemised fills."""
    _session(deps)
    try:
        return dict(await deps.client.get(f"/backtests/{backtest_run_id}"))
    except GatewayRefusal as refusal:
        return refused(refusal.detail, backtest_run_id=backtest_run_id)
```

- [ ] **Step 4: Run the tests, check types, and commit**

Run: `uv run pytest tests/mcp/test_tools_backtest.py -v`
Expected: PASS, all tests.

```bash
uv run mypy src/trading/mcp && uv run ruff check src/trading/mcp tests/mcp
git add src/trading/mcp/tools.py tests/mcp/test_tools_backtest.py
git commit -m "$(cat <<'MSG'
feat(mcp): submit a strategy, backtest it, read the run back

A rejected strategy returns its findings rather than an error. The
findings are the useful part -- they say what to change -- and an agent
iterating on a strategy reads them and resubmits.

Backtest overrides that were not given are omitted rather than sent as
null. The route reads a missing field as "use the strategy's own", so an
explicit null would override the manifest with nothing.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Task 15: Both transports, and an end-to-end test

**Files:**
- Modify: `src/trading/mcp/tools.py` (append `register`)
- Create: `src/trading/mcp/serve_stdio.py`
- Create: `src/trading/mcp/serve_http.py`
- Test: `tests/mcp/test_serve_http.py`, `tests/mcp/test_end_to_end.py`

**Interfaces:**
- Consumes: every tool from Tasks 9-14.
- Produces:
  - `tools.register(server: Any, deps: ToolDeps) -> None`
  - `serve_http.bearer_token` — `ContextVar[str | None]`
  - `serve_http.BearerTokenMiddleware`
  - `serve_http.build_app() -> Any`
  - `serve_stdio.main() -> None`

**Verify against the installed SDK before writing.** `mcp` 2.x renamed
`FastMCP` to `MCPServer` and moved transport configuration from the
constructor to `run()`. Confirm with:

```bash
uv run python -c "from mcp.server import MCPServer; print(MCPServer)"
```

The reference pages are `https://py.sdk.modelcontextprotocol.io/v2/run`
and `.../v2/run/asgi`. If the import differs, follow the installed
version and note the difference in the commit message.

**Why the token is read by our own middleware** rather than the SDK's
`TokenVerifier`: the SDK's bearer support is built for OAuth resource
servers and brings protected-resource metadata with it. The scope here is
a static token-to-portfolio map, so a five-line ASGI middleware that
stashes the header in a `ContextVar` is the whole requirement and leaves
nothing to misconfigure.

- [ ] **Step 1: Write the failing tests**

Create `tests/mcp/test_serve_http.py`:

```python
from __future__ import annotations

import pytest

from trading.mcp.serve_http import BearerTokenMiddleware, bearer_token


@pytest.mark.anyio
async def test_middleware_extracts_a_bearer_token_into_the_context() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"authorization", b"Bearer secret-a")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == ["secret-a"]


@pytest.mark.anyio
async def test_middleware_is_case_insensitive_about_the_scheme() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"Authorization", b"bearer secret-a")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == ["secret-a"]


@pytest.mark.anyio
async def test_a_request_with_no_authorization_header_carries_no_token() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    await BearerTokenMiddleware(app)({"type": "http", "headers": []}, None, None)
    assert seen == [None]


@pytest.mark.anyio
async def test_a_non_bearer_authorization_header_carries_no_token() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"authorization", b"Basic abc")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == [None]


@pytest.mark.anyio
async def test_one_requests_token_does_not_leak_into_the_next() -> None:
    # Each request must resolve its own scope. A leaked token is a
    # request trading another agent's book.
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    middleware = BearerTokenMiddleware(app)
    await middleware({"type": "http", "headers": [(b"authorization", b"Bearer a")]}, None, None)
    await middleware({"type": "http", "headers": []}, None, None)
    assert seen == ["a", None]
```

Create `tests/mcp/test_end_to_end.py` — this is the test that proves the
tools run against the **real** invariant chain rather than a mock of it:

```python
from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import httpx
import pytest
from fastapi import FastAPI

from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, get_portfolio_state, list_instruments, place_order
from trading.paper import api as paper_api
from trading.streaming import market_data_api
from trading.streaming.db import get_db_connection

pytestmark = pytest.mark.db


@pytest.fixture
def gateway_app(db_conn) -> Iterator[FastAPI]:
    app = FastAPI()
    app.include_router(market_data_api.router)
    app.include_router(paper_api.router)
    app.dependency_overrides[get_db_connection] = lambda: db_conn
    try:
        yield app
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def portfolio_id(db_conn) -> int:
    user = db_conn.execute(
        "INSERT INTO users (email) VALUES ('agent@example.com') RETURNING user_id"
    ).fetchone()
    row = db_conn.execute(
        """
        INSERT INTO portfolios
            (user_id, name, base_currency, initial_capital, cash_balance, status, margin_mode)
        VALUES (%s, 'agent', 'INR', 100000, 100000, 'ACTIVE', 'ISOLATED')
        RETURNING portfolio_id
        """,
        (user[0],),
    ).fetchone()
    return row[0]


@pytest.fixture
def deps(gateway_app: FastAPI, portfolio_id: int) -> ToolDeps:
    transport = httpx.ASGITransport(app=gateway_app)
    return ToolDeps(
        client=GatewayClient("http://gateway", httpx.AsyncClient(transport=transport)),
        sessions=SessionStore({"tok": portfolio_id}),
        token_provider=lambda: "tok",
    )


@pytest.mark.anyio
async def test_portfolio_state_reads_the_real_portfolio(
    deps: ToolDeps, portfolio_id: int
) -> None:
    result = await get_portfolio_state(deps)
    assert result["portfolio"]["portfolio_id"] == portfolio_id
    assert Decimal(result["portfolio"]["cash_balance"]) == Decimal(100000)


@pytest.mark.anyio
async def test_every_money_field_reaching_the_agent_is_a_string(deps: ToolDeps) -> None:
    portfolio = (await get_portfolio_state(deps))["portfolio"]
    for field in ("cash_balance", "initial_capital"):
        assert isinstance(portfolio[field], str), f"{field} reached the agent as a non-string"


@pytest.mark.anyio
async def test_an_order_for_an_untradeable_asset_class_is_refused_by_the_real_chain(
    deps: ToolDeps, db_conn
) -> None:
    # An INDEX has no charge schedule. The refusal must come from the
    # platform's own invariant chain, not from anything the MCP layer
    # re-implements -- that is the whole reason tools speak HTTP.
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('INDEX', 'NSE', 'CM', 'NIFTY', 'ACTIVE', 'NSE:CM:NIFTY')
        RETURNING instrument_id
        """
    ).fetchone()
    result = await place_order(
        deps,
        instrument_id=row[0],
        side="BUY",
        order_type="MARKET",
        quantity="1",
        product="DELIVERY",
        rationale="should never fill",
    )
    assert result["status"] == "REFUSED"


@pytest.mark.anyio
async def test_a_second_identical_order_in_the_same_minute_does_not_double(
    deps: ToolDeps, db_conn
) -> None:
    row = db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('CRYPTO', 'BINANCE', 'SPOT', 'BTC-USDT', 'ACTIVE', 'BINANCE:SPOT:BTC-USDT')
        RETURNING instrument_id
        """
    ).fetchone()
    kwargs = {
        "instrument_id": row[0], "side": "BUY", "order_type": "MARKET",
        "quantity": "1", "product": "DELIVERY", "rationale": "same decision, twice",
    }
    first = await place_order(deps, **kwargs)  # type: ignore[arg-type]
    second = await place_order(deps, **kwargs)  # type: ignore[arg-type]
    if first.get("status") == "REFUSED":
        pytest.skip(f"order refused before idempotency could be observed: {first['reason']}")
    assert first["order_id"] == second["order_id"]


@pytest.mark.anyio
async def test_list_instruments_flags_tradeability_against_the_real_table(
    deps: ToolDeps, db_conn
) -> None:
    db_conn.execute(
        """
        INSERT INTO instruments (asset_class, exchange, segment, symbol, status, canonical_key)
        VALUES ('INDEX', 'NSE', 'CM', 'BANKNIFTY', 'ACTIVE', 'NSE:CM:BANKNIFTY')
        """
    )
    result = await list_instruments(deps, "INDEX", None)
    assert result["instruments"]
    assert all(row["tradeable"] is False for row in result["instruments"])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mcp/test_serve_http.py tests/mcp/test_end_to_end.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.mcp.serve_http'`

- [ ] **Step 3: Append `register` to `src/trading/mcp/tools.py`**

```python
def register(server: Any, deps: ToolDeps) -> None:
    """Expose every tool on an MCP server.

    A thin adapter on purpose: the tools above are plain functions, so
    both transports register the same objects and the tests never touch
    MCP machinery. The docstrings become the tool descriptions an agent
    reads, which is why they say what a tool refuses and why.
    """

    @server.tool()
    async def get_capabilities_() -> dict[str, Any]:
        """What this platform can trade, and the vocabulary it accepts."""
        return await get_capabilities(deps)

    @server.tool()
    async def list_instruments_(
        asset_class: str | None = None, query: str | None = None
    ) -> dict[str, Any]:
        """Instruments, optionally filtered, each flagged tradeable or not."""
        return await list_instruments(deps, asset_class, query)

    @server.tool()
    async def get_strategy_contract_() -> dict[str, Any]:
        """The contract a strategy script must satisfy to be accepted."""
        return await get_strategy_contract(deps)

    @server.tool()
    async def get_market_snapshot_(
        instrument_ids: list[int],
        interval: str = "1d",
        indicators: list[str] | None = None,
        history: int = 50,
    ) -> dict[str, Any]:
        """Prices, bars and computed indicators, with a freshness verdict."""
        return await get_market_snapshot(deps, instrument_ids, interval, indicators, history)

    @server.tool()
    async def get_candles_(
        instrument_id: int, interval: str = "1d", limit: int = 300
    ) -> dict[str, Any]:
        """Raw OHLCV as decimal strings, oldest first."""
        return await get_candles(deps, instrument_id, interval, limit)

    @server.tool()
    async def get_data_freshness_(
        instrument_ids: list[int], interval: str = "1d"
    ) -> dict[str, Any]:
        """How current each series is. Ask before trusting a backtest."""
        return await get_data_freshness(deps, instrument_ids, interval)

    @server.tool()
    async def get_perp_context_(instrument_id: int) -> dict[str, Any]:
        """Step size, minimum notional, leverage tiers and latest funding."""
        return await get_perp_context(deps, instrument_id)

    @server.tool()
    async def get_portfolio_state_() -> dict[str, Any]:
        """Cash, positions and working orders for this session's portfolio."""
        return await get_portfolio_state(deps)

    @server.tool()
    async def get_perp_positions_() -> dict[str, Any]:
        """Open perpetual positions with margin and liquidation price."""
        return await get_perp_positions(deps)

    @server.tool()
    async def list_orders_(status: str | None = None, limit: int = 50) -> dict[str, Any]:
        """This session portfolio's order blotter."""
        return await list_orders(deps, status, limit)

    @server.tool()
    async def place_order_(
        instrument_id: int,
        side: str,
        order_type: str,
        quantity: str,
        product: str,
        rationale: str,
        limit_price: str | None = None,
        time_in_force: str = "DAY",
        leverage: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Place an order in this session's portfolio.

        Quantities and prices are decimal STRINGS, never numbers.
        `rationale` is required and stored with the order. `leverage` is
        required for a perpetual and meaningless otherwise. A refusal
        comes back as status REFUSED with the platform's own reason;
        retrying it unchanged will be refused again.
        """
        return await place_order(
            deps, instrument_id, side, order_type, quantity, product, rationale,
            limit_price, time_in_force, leverage, idempotency_key,
        )

    @server.tool()
    async def cancel_order_(order_id: int) -> dict[str, Any]:
        """Cancel a working order."""
        return await cancel_order(deps, order_id)

    @server.tool()
    async def submit_strategy_(name: str, python_code: str) -> dict[str, Any]:
        """Register a strategy: statically validated, then smoke-run."""
        return await submit_strategy(deps, name, python_code)

    @server.tool()
    async def run_backtest_(
        strategy_id: int,
        start: str,
        end: str,
        starting_cash: str | None = None,
        max_daily_loss: str | None = None,
        max_drawdown_pct: str | None = None,
    ) -> dict[str, Any]:
        """Replay a strategy over a window. Dates are YYYY-MM-DD."""
        return await run_backtest(
            deps, strategy_id, start, end, starting_cash, max_daily_loss, max_drawdown_pct
        )

    @server.tool()
    async def get_backtest_(backtest_run_id: int) -> dict[str, Any]:
        """Re-read a stored backtest run with its curve and fills."""
        return await get_backtest(deps, backtest_run_id)
```

**Naming:** the trailing underscore avoids shadowing the module-level
functions. If the installed SDK takes an explicit name — `@server.tool(name="place_order")` —
use it so the agent sees clean names; otherwise rename the inner
functions to the exact tool names and reference the module functions via
`globals()`. Confirm which by reading `MCPServer.tool`'s signature.

- [ ] **Step 4: Create `src/trading/mcp/serve_http.py`**

```python
"""Streamable HTTP entrypoint. Scope comes from the bearer token."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from mcp.server import MCPServer

from trading.config import get_settings
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, register

bearer_token: ContextVar[str | None] = ContextVar("mcp_bearer_token", default=None)

_SCHEME = "bearer "


class BearerTokenMiddleware:
    """Stash the request's bearer token where the tools can find it.

    Our own middleware rather than the SDK's TokenVerifier: that machinery
    is built for OAuth resource servers and brings protected-resource
    metadata with it, while the requirement here is a static
    token-to-portfolio map. Five lines with nothing to misconfigure beats
    a framework whose defaults would have to be audited.

    The token is set per request, so one caller's scope can never be read
    by the next -- a leak there is a request trading another agent's book.
    """

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            header = ""
            for key, value in scope.get("headers", []):
                if key.decode().lower() == "authorization":
                    header = value.decode()
                    break
            token = header[len(_SCHEME) :].strip() if header.lower().startswith(_SCHEME) else None
            bearer_token.set(token or None)
        await self._app(scope, receive, send)


def build_app() -> Any:
    """The ASGI app to serve, with tools registered and auth wrapped."""
    settings = get_settings()
    deps = ToolDeps(
        client=GatewayClient.open(settings.mcp_gateway_url),
        sessions=SessionStore.from_settings(settings),
        token_provider=bearer_token.get,
    )
    server = MCPServer("trading")
    register(server, deps)
    # Stateless so each tool call resolves its own request scope; a
    # long-lived session would outlive the ContextVar the token lives in.
    return BearerTokenMiddleware(server.streamable_http_app(stateless_http=True))


app = build_app()
```

- [ ] **Step 5: Create `src/trading/mcp/serve_stdio.py`**

```python
"""stdio entrypoint. Scope comes from configuration.

There is no token over stdio: the subprocess is spawned by the agent's
own harness and is already inside the trust boundary. `SessionStore`
refuses to resolve at all unless `mcp_stdio_portfolio_id` is set, so a
misconfigured server fails on the first tool call rather than guessing a
book.
"""

from __future__ import annotations

from mcp.server import MCPServer

from trading.config import get_settings
from trading.mcp.client import GatewayClient
from trading.mcp.session import SessionStore
from trading.mcp.tools import ToolDeps, register


def build() -> MCPServer:
    settings = get_settings()
    deps = ToolDeps(
        client=GatewayClient.open(settings.mcp_gateway_url),
        sessions=SessionStore.from_settings(settings),
        token_provider=lambda: None,
    )
    server = MCPServer("trading")
    register(server, deps)
    return server


def main() -> None:
    build().run()


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the whole suite**

Run: `uv run pytest tests/ -v`
Expected: PASS — every new test, and every pre-existing test unchanged.

- [ ] **Step 7: Smoke-test both transports by hand**

```bash
# stdio: should print a tools/list response containing place_order
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | uv run python -m trading.mcp.serve_stdio

# HTTP: should start and answer on /mcp
uv run uvicorn trading.mcp.serve_http:app --port 8081
```

- [ ] **Step 8: Check types, lint, and commit**

```bash
uv run mypy src/trading && uv run ruff check src tests
git add src/trading/mcp tests/mcp
git commit -m "$(cat <<'MSG'
feat(mcp): serve one tool layer over stdio and streamable HTTP

register() is a thin adapter over plain async functions, so both
transports expose the same objects and the tests never construct an MCP
server to exercise a tool.

The bearer token is read by a five-line ASGI middleware into a
ContextVar rather than through the SDK's TokenVerifier. That machinery is
built for OAuth resource servers and brings protected-resource metadata
with it; the requirement here is a static token-to-portfolio map, and
there is nothing in five lines to misconfigure.

The end-to-end tests drive the tools through httpx.ASGITransport against
the real gateway app on a rolled-back transaction. An order for an INDEX
is refused by the platform's own chain, not by anything this package
re-implements -- which is the property that keeps the invariants
unforked, and the test that fails if anyone ever reaches for psycopg here.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## After the plan

**Configuration to add to `.env.local` before first use:**

```
MCP_GATEWAY_URL=http://localhost:8000
MCP_TOKENS=<a-long-random-token>:<portfolio_id>
MCP_STDIO_PORTFOLIO_ID=<portfolio_id>
```

**Wiring the agent (stdio):**

```json
{
  "mcpServers": {
    "trading": {
      "command": "uv",
      "args": ["run", "python", "-m", "trading.mcp.serve_stdio"],
      "cwd": "/Users/satyam/claude/trading"
    }
  }
}
```

**Known gaps carried out of this plan**, all deferred by decision in the
spec: order attribution (`agent_session_id`), notional and rate caps, a
kill switch and dry-run mode, `start_live_run` over MCP, news ingestion,
and NSE F&O or options trading. Separately, EOD bhavcopy ingestion is
still unscheduled: `get_data_freshness` makes that visible but does not
fix it.
