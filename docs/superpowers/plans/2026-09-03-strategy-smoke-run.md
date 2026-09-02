# Strategy Smoke Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build §9 stage 2 of the Agent Contract — a five-session smoke run that executes a submitted strategy inside the existing sandbox against real bars, real fills, and the real cost model, and returns an agent-readable verdict.

**Architecture:** A new pure `src/trading/runtime/` package (bar provider, run state, live Context, event loop) is copied into the sandbox image alongside the already-pure `trading.paper.{fills,charges,breaker}`. The host resolves the manifest in a first `configure` container pass, fetches bars and charge schedules, gzips them into a stdin envelope, and runs two identical `smoke` passes whose order sequences are compared for determinism. `trading.agent_contract.smoke` is the only module here that touches Postgres or Docker.

**Tech Stack:** Python 3.12, psycopg 3, pydantic v2 (frozen models), alembic, Docker, pytest.

**Spec:** `docs/superpowers/specs/2026-09-03-strategy-smoke-run-design.md` — read it before Task 1. Every decision below argues from a D-S number in that document.

## Global Constraints

- **All money and quantities are `decimal.Decimal`, never `float`.** Contract §5. A `float` anywhere in a price, quantity, cash, or charge path is a defect.
- **Every `trading.paper` model is `ConfigDict(frozen=True)`.** `Order`, `Position`, `ChargeSchedule`, `ChargeBreakdown`, `FillDecision`, `Portfolio`. Mutation is `obj.model_copy(update={...})`, which returns a new object. Never assign to a field.
- **Nothing under `src/trading/runtime/` may import psycopg, docker, or `trading.config`.** That package is copied into a container that has neither a database nor a network. `tests/agent_contract/test_image_contents.py` (Task 4) enforces this.
- **`platform_sdk.py` is not modified by this plan.** D-S2: `LiveContext` subclasses `platform_sdk.Context`. If a change to the stub seems necessary, stop and raise it — it means the subclass relationship has broken and the design needs revisiting.
- **Line length 100** (`[tool.ruff] line-length = 100`). `uv run ruff check src tests` and `uv run mypy src` must pass before every commit.
- Container tests carry `@pytest.mark.sandbox`; DB tests carry `@pytest.mark.db`. Default run is `-m 'not live and not golden'`, so sandbox and db tests DO run by default and need Docker plus a migrated `trading_test`.
- **Contract version is `"0.1"`**, from `trading.agent_contract.registry.CONTRACT_VERSION`. Import it; do not retype the literal.
- Run tests with `uv run pytest`. Migrations with `uv run alembic upgrade head`.

---

### Task 1: Bar records, the in-memory provider, and the payload envelope

The data carrier. Pure, no Docker, no database. Everything later in the plan moves `BarRecord`s around.

**Files:**
- Create: `src/trading/runtime/__init__.py`
- Create: `src/trading/runtime/provider.py`
- Create: `src/trading/runtime/payload.py`
- Test: `tests/runtime/__init__.py`
- Test: `tests/runtime/test_provider.py`
- Test: `tests/runtime/test_payload.py`

**Interfaces:**
- Consumes: `trading.paper.models.ChargeSchedule`, `trading.agent_contract.registry.CONTRACT_VERSION`.
- Produces:
  - `BarRecord` — frozen dataclass with `instrument_id: int`, `ts: datetime`, `interval_sec: int`, `open/high/low/close: Decimal`, `volume: Decimal | None`, `trades: int | None`, `open_interest: int | None`, `oi_change: int | None`, and a `close_ts` property.
  - `InMemoryBars(bars: Mapping[int, Sequence[BarRecord]])` with `.instruments() -> tuple[int, ...]`, `.groups() -> Iterator[tuple[datetime, tuple[BarRecord, ...]]]`, `.history(instrument_id, upto_index) -> tuple[BarRecord, ...]`.
  - `SmokePayload` frozen dataclass and `encode_payload(payload) -> bytes` / `decode_payload(raw: bytes) -> SmokePayload`.

- [ ] **Step 1: Write the failing provider test**

Create `tests/runtime/__init__.py` (empty) and `tests/runtime/test_provider.py`:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.runtime.provider import BarRecord, InMemoryBars


def _bar(instrument_id: int, minute: int, close: str) -> BarRecord:
    return BarRecord(
        instrument_id=instrument_id,
        ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
        interval_sec=60,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("100"),
        trades=None,
        open_interest=None,
        oi_change=None,
    )


def test_close_ts_is_the_end_of_the_interval() -> None:
    bar = _bar(1, 15, "100")
    assert bar.close_ts == bar.ts + timedelta(seconds=60)


def test_groups_are_ordered_and_merge_instruments_printing_together() -> None:
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11")], 2: [_bar(2, 1, "20")]})
    groups = list(bars.groups())
    assert [ts for ts, _ in groups] == [
        datetime(2026, 9, 1, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 9, 2, tzinfo=UTC),
    ]
    assert [b.instrument_id for b in groups[0][1]] == [1]
    assert [b.instrument_id for b in groups[1][1]] == [1, 2]


def test_ties_break_by_instrument_id_so_ordering_is_total() -> None:
    # Determinism (D-S6) is only checkable if the merge order is total.
    bars = InMemoryBars({9: [_bar(9, 0, "1")], 2: [_bar(2, 0, "2")], 5: [_bar(5, 0, "3")]})
    _, group = next(iter(bars.groups()))
    assert [b.instrument_id for b in group] == [2, 5, 9]


def test_history_never_includes_the_bar_being_processed() -> None:
    # The anti-lookahead guarantee of contract §4, at the data layer.
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11"), _bar(1, 2, "12")]})
    assert [b.close for b in bars.history(1, 0)] == []
    assert [b.close for b in bars.history(1, 2)] == [Decimal("10"), Decimal("11")]
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/runtime/test_provider.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trading.runtime'`

- [ ] **Step 3: Implement the provider**

Create `src/trading/runtime/__init__.py` (empty) and `src/trading/runtime/provider.py`:

```python
"""Bars, and the only shape the runtime reads them through.

This package is copied into the strategy sandbox image, so nothing here
may import psycopg, docker, or `trading.config` -- the container has no
database and no network. `tests/agent_contract/test_image_contents.py`
enforces that.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import groupby

__all__ = ["BarRecord", "InMemoryBars"]


@dataclass(frozen=True)
class BarRecord:
    """One completed interval. Structurally a `platform_sdk.Bar`.

    Frozen because the same record is handed to the strategy and kept in
    the history the strategy reads back; a mutable bar would let a
    strategy rewrite its own past.
    """

    instrument_id: int
    ts: datetime
    interval_sec: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    trades: int | None = None
    open_interest: int | None = None
    oi_change: int | None = None

    @property
    def close_ts(self) -> datetime:
        """When this bar's values became knowable.

        `ts` marks the START of the interval (contract §5), so a clock set
        to `ts` while reading `close` would be reading the future. The loop
        advances to `close_ts` instead.
        """
        return self.ts + timedelta(seconds=self.interval_sec)


class InMemoryBars:
    """Every bar the smoke run will feed, held in memory.

    The sandbox implementation of what Phase 3 will serve from Timescale.
    The interface is deliberately narrow -- group iteration and a history
    prefix -- because those are the only two things the event loop and
    `ctx.data` need, and a wider one would invite a strategy-visible query
    that could reach past `ctx.now`.
    """

    def __init__(self, bars: Mapping[int, Sequence[BarRecord]]) -> None:
        self._bars = {
            instrument_id: tuple(sorted(series, key=lambda b: b.ts))
            for instrument_id, series in bars.items()
        }
        # Index of each bar within its own instrument's series, so
        # `history` can be answered without a scan during the loop.
        flat = [
            (bar, index)
            for series in self._bars.values()
            for index, bar in enumerate(series)
        ]
        # Total ordering: by close time, then instrument id. Ties must
        # break deterministically or the double-run check (D-S6) would
        # report false divergences.
        flat.sort(key=lambda pair: (pair[0].close_ts, pair[0].instrument_id))
        self._flat = flat

    def instruments(self) -> tuple[int, ...]:
        return tuple(sorted(self._bars))

    def total_bars(self) -> int:
        return len(self._flat)

    def groups(self) -> Iterator[tuple[datetime, tuple[BarRecord, ...]]]:
        """Bars grouped by the instant they all became knowable.

        An instrument that did not print in an interval is simply absent
        from its group -- never carried forward. Contract §4: the platform
        will not invent a trade that did not happen.
        """
        for close_ts, pairs in groupby(self._flat, key=lambda pair: pair[0].close_ts):
            yield close_ts, tuple(pair[0] for pair in pairs)

    def indexed_groups(self) -> Iterator[tuple[datetime, tuple[tuple[BarRecord, int], ...]]]:
        """`groups()`, but each bar paired with its index in its own series."""
        for close_ts, pairs in groupby(self._flat, key=lambda pair: pair[0].close_ts):
            yield close_ts, tuple(pairs)

    def history(self, instrument_id: int, upto_index: int) -> tuple[BarRecord, ...]:
        """Bars STRICTLY BEFORE `upto_index`.

        Strict, not inclusive: the bar currently being processed has not
        closed from the strategy's point of view until the loop has
        dispatched it, and returning it here is the lookahead the contract
        promises is impossible.
        """
        return self._bars.get(instrument_id, ())[:upto_index]
```

- [ ] **Step 4: Run the provider tests and confirm they pass**

Run: `uv run pytest tests/runtime/test_provider.py -v`
Expected: 4 passed

- [ ] **Step 5: Write the failing payload test**

Create `tests/runtime/test_payload.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
from trading.paper.models import ChargeSchedule
from trading.runtime.payload import SmokePayload, decode_payload, encode_payload
from trading.runtime.provider import BarRecord


def _schedule() -> ChargeSchedule:
    return ChargeSchedule(
        broker="ZERODHA",
        exchange="NSE",
        asset_class="EQUITY",
        product=Product.DELIVERY,
        charge_type=ChargeType.BROKERAGE,
        basis=ChargeBasis.FLAT_PER_ORDER,
        applies_to_side="BOTH",
        rate=Decimal("0"),
        cap=None,
        rounding=Rounding.TWO_DECIMALS,
        gst_base_types=(),
        effective_from=datetime(2020, 1, 1).date(),
        effective_to=None,
        source_note="test",
    )


def _payload() -> SmokePayload:
    return SmokePayload(
        mode="smoke",
        source="class S: pass\n",
        window={"start": "2026-08-27T00:00:00+00:00", "end": "2026-09-02T00:00:00+00:00",
                "sessions": 5},
        bars={
            1401: (
                BarRecord(
                    instrument_id=1401,
                    ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
                    interval_sec=60,
                    open=Decimal("1300.05"),
                    high=Decimal("1301.00"),
                    low=Decimal("1299.50"),
                    close=Decimal("1300.75"),
                    volume=Decimal("1234"),
                ),
            )
        },
        charge_schedules=(_schedule(),),
        starting_cash=Decimal("1000000.00"),
        slippage_bps=Decimal("5"),
    )


def test_round_trip_preserves_every_field_exactly() -> None:
    restored = decode_payload(encode_payload(_payload()))
    original = _payload()
    assert restored.mode == original.mode
    assert restored.source == original.source
    assert restored.window == original.window
    assert restored.starting_cash == original.starting_cash
    assert restored.slippage_bps == original.slippage_bps
    assert restored.charge_schedules == original.charge_schedules
    assert restored.bars == original.bars


def test_money_survives_as_decimal_not_float() -> None:
    # The whole cost model depends on this. A payload that quietly
    # round-trips prices through binary floating point would corrupt
    # every charge computed in the container.
    restored = decode_payload(encode_payload(_payload()))
    bar = restored.bars[1401][0]
    assert isinstance(bar.close, Decimal)
    assert bar.close == Decimal("1300.75")
    assert str(bar.close) == "1300.75"


def test_encoding_is_compressed() -> None:
    # D-S7 feeds an uncapped universe, so the payload must compress.
    raw = encode_payload(_payload())
    assert raw[:2] == b"\x1f\x8b"  # gzip magic


def test_columnar_encoding_beats_per_object_for_a_realistic_series() -> None:
    bars = tuple(
        BarRecord(
            instrument_id=1401,
            ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("1300.05"),
            high=Decimal("1301.00"),
            low=Decimal("1299.50"),
            close=Decimal("1300.75"),
            volume=Decimal("1234"),
        )
        for _ in range(2000)
    )
    payload = SmokePayload(mode="smoke", source="x", bars={1401: bars})
    # 2000 bars must fit comfortably; a per-object JSON encoding would not.
    assert len(encode_payload(payload)) < 200_000


def test_decode_rejects_a_payload_without_a_mode() -> None:
    import gzip
    import json

    raw = gzip.compress(json.dumps({"source": "x"}).encode())
    with pytest.raises(ValueError, match="mode"):
        decode_payload(raw)
```

- [ ] **Step 6: Run it and confirm it fails**

Run: `uv run pytest tests/runtime/test_payload.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trading.runtime.payload'`

- [ ] **Step 7: Implement the payload envelope**

Create `src/trading/runtime/payload.py`:

```python
"""The stdin envelope carrying a smoke run into the sandbox.

Three properties are load-bearing.

**Money is text, never a JSON number.** Every price, quantity, and rate
crosses as a string and is rebuilt with `Decimal(...)`. JSON numbers are
IEEE 754 doubles, and a payload that quietly round-tripped prices through
binary floating point would corrupt every charge computed in the
container -- in a system whose headline feature is an honest cost model.

**Bars are columnar, and the whole envelope is gzipped.** The universe is
not capped (D-S7), so a wide manifest can mean ~88,000 bars. Per-bar
objects repeat eight keys each; parallel arrays do not, and numeric text
in columns compresses about ten to one.

**`mode` is explicit and required.** The runner never guesses whether it
received raw source or an envelope. Format detection by sniffing is the
compatibility shim that breaks silently a year later.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from trading.paper.models import ChargeSchedule
from trading.runtime.provider import BarRecord

__all__ = ["MODE_CONFIGURE", "MODE_SMOKE", "SmokePayload", "decode_payload", "encode_payload"]

MODE_CONFIGURE = "configure"
MODE_SMOKE = "smoke"
_MODES = frozenset({MODE_CONFIGURE, MODE_SMOKE})


@dataclass(frozen=True)
class SmokePayload:
    mode: str
    source: str
    contract_version: str = "0.1"
    window: dict[str, Any] | None = None
    bars: Mapping[int, tuple[BarRecord, ...]] = field(default_factory=dict)
    charge_schedules: tuple[ChargeSchedule, ...] = ()
    starting_cash: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")


def _money(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _encode_series(series: Sequence[BarRecord]) -> dict[str, Any]:
    return {
        "interval_sec": series[0].interval_sec if series else 60,
        "ts": [bar.ts.isoformat() for bar in series],
        "o": [str(bar.open) for bar in series],
        "h": [str(bar.high) for bar in series],
        "l": [str(bar.low) for bar in series],
        "c": [str(bar.close) for bar in series],
        "v": [_money(bar.volume) for bar in series],
        "t": [bar.trades for bar in series],
        "oi": [bar.open_interest for bar in series],
        "oic": [bar.oi_change for bar in series],
    }


def _decode_series(instrument_id: int, column: Mapping[str, Any]) -> tuple[BarRecord, ...]:
    interval_sec = int(column["interval_sec"])
    return tuple(
        BarRecord(
            instrument_id=instrument_id,
            ts=datetime.fromisoformat(ts),
            interval_sec=interval_sec,
            open=Decimal(o),
            high=Decimal(h),
            low=Decimal(low),
            close=Decimal(c),
            volume=None if v is None else Decimal(v),
            trades=t,
            open_interest=oi,
            oi_change=oic,
        )
        for ts, o, h, low, c, v, t, oi, oic in zip(
            column["ts"],
            column["o"],
            column["h"],
            column["l"],
            column["c"],
            column["v"],
            column["t"],
            column["oi"],
            column["oic"],
            strict=True,
        )
    )


def encode_payload(payload: SmokePayload) -> bytes:
    document = {
        "mode": payload.mode,
        "contract_version": payload.contract_version,
        "source": payload.source,
        "window": payload.window,
        "bars": {
            str(instrument_id): _encode_series(series)
            for instrument_id, series in payload.bars.items()
        },
        # ChargeSchedule serialises Decimals as floats via its own
        # field_serializer, which is right for the API and wrong here --
        # so the schedules cross as JSON produced by pydantic's own
        # round-trip-safe mode instead.
        "charge_schedules": [
            json.loads(schedule.model_dump_json()) for schedule in payload.charge_schedules
        ],
        "starting_cash": str(payload.starting_cash),
        "slippage_bps": str(payload.slippage_bps),
    }
    return gzip.compress(json.dumps(document, separators=(",", ":")).encode("utf-8"))


def decode_payload(raw: bytes) -> SmokePayload:
    document = json.loads(gzip.decompress(raw).decode("utf-8"))
    mode = document.get("mode")
    if mode not in _MODES:
        raise ValueError(
            f"payload mode must be one of {sorted(_MODES)}, got {mode!r}; "
            "the runner requires an explicit envelope and never guesses"
        )
    return SmokePayload(
        mode=mode,
        source=document["source"],
        contract_version=document.get("contract_version", "0.1"),
        window=document.get("window"),
        bars={
            int(instrument_id): _decode_series(int(instrument_id), column)
            for instrument_id, column in document.get("bars", {}).items()
        },
        charge_schedules=tuple(
            ChargeSchedule.model_validate(row) for row in document.get("charge_schedules", [])
        ),
        starting_cash=Decimal(document.get("starting_cash", "0")),
        slippage_bps=Decimal(document.get("slippage_bps", "0")),
    )
```

- [ ] **Step 8: Run the payload tests**

Run: `uv run pytest tests/runtime/test_payload.py -v`
Expected: 5 passed.

If `test_round_trip_preserves_every_field_exactly` fails on `charge_schedules`, inspect what `model_dump_json()` produced — `ChargeSchedule` may carry a `field_serializer` that lowers `Decimal` to `float`. If so, encode the schedules field-by-field as strings the same way bars are, rather than changing the model (the model's serialisation is what the API depends on).

- [ ] **Step 9: Lint, type-check, and commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
git add src/trading/runtime tests/runtime
git commit -m "feat(runtime): bar records, the in-memory provider, and the payload envelope

The data carrier for the smoke run (design D-S1, D-S7). Money crosses the
envelope as text and is rebuilt as Decimal -- a JSON number is an IEEE 754
double, and a payload that quietly round-tripped prices through binary
floating point would corrupt every charge computed in the container.

Bars are columnar and the envelope is gzipped because the universe is
deliberately uncapped, so a wide manifest can mean ~88,000 bars.

InMemoryBars.history() is exclusive of the bar being processed, which is
where contract §4's anti-lookahead guarantee actually lives: there is no
argument reaching unconsumed data because unconsumed data is not in the
structure being read."
```

---

### Task 2: Run state and the live Context

The surface a strategy touches. Still pure — no fills yet; orders are submitted and rest.

**Files:**
- Create: `src/trading/runtime/state.py`
- Create: `src/trading/runtime/context.py`
- Test: `tests/runtime/test_context.py`

**Interfaces:**
- Consumes: `BarRecord`, `InMemoryBars` (Task 1); `platform_sdk.{Context, DataAccess, PortfolioView}`; `trading.paper.models.{Order, Position}`; `trading.paper.enums.{OrderStatus, OrderType, Product, Side, TimeInForce}`.
- Produces:
  - `RunState` — mutable dataclass: `now: datetime`, `cash: Decimal`, `starting_cash: Decimal`, `positions: dict[int, Position]`, `orders: dict[int, Order]`, `submissions: list[Order]`, `logs: list[dict[str, Any]]`, `bar_calls: int`, `next_order_id: int`, `cursor: dict[int, int]`, `marks: dict[int, Decimal]`, `rejections: list[str]`.
  - `LiveContext(state: RunState, bars: InMemoryBars, portfolio_id: int = 1)` subclassing `platform_sdk.Context`.
  - `SMOKE_PORTFOLIO_ID = 1`.

- [ ] **Step 1: Write the failing Context test**

Create `tests/runtime/test_context.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.paper.enums import OrderStatus, Side
from trading.runtime.context import LiveContext
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState


def _bar(instrument_id: int, minute: int, close: str) -> BarRecord:
    return BarRecord(
        instrument_id=instrument_id,
        ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
        interval_sec=60,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("100"),
    )


def _ctx(cursor: dict[int, int] | None = None) -> LiveContext:
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11"), _bar(1, 2, "12")]})
    state = RunState(
        now=datetime(2026, 9, 1, 9, 3, tzinfo=UTC),
        cash=Decimal("100000"),
        starting_cash=Decimal("100000"),
    )
    state.cursor = cursor if cursor is not None else {1: 2}
    return LiveContext(state=state, bars=bars)


def test_it_is_a_platform_sdk_context() -> None:
    # D-S2: the offline stub and the live runtime cannot drift on shape,
    # because the live one IS the stub, subclassed.
    from trading.agent_contract import platform_sdk

    assert isinstance(_ctx(), platform_sdk.Context)


def test_now_is_the_simulation_clock() -> None:
    assert _ctx().now == datetime(2026, 9, 1, 9, 3, tzinfo=UTC)


def test_bars_cannot_reach_past_the_cursor() -> None:
    ctx = _ctx(cursor={1: 2})
    assert [b.close for b in ctx.data.bars(1, count=10)] == [Decimal("10"), Decimal("11")]


def test_bars_returns_fewer_than_count_without_complaint() -> None:
    ctx = _ctx(cursor={1: 1})
    assert len(ctx.data.bars(1, count=50)) == 1


def test_last_is_the_most_recent_closed_bar() -> None:
    last = _ctx(cursor={1: 2}).data.last(1)
    assert last is not None
    assert last.close == Decimal("11")


def test_last_is_none_before_any_bar_has_closed() -> None:
    assert _ctx(cursor={1: 0}).data.last(1) is None


def test_order_returns_an_id_and_rests_the_order() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="fast crossed slow")
    order = ctx._state.orders[order_id]
    assert order.status is OrderStatus.OPEN
    assert order.side is Side.BUY
    assert order.quantity == Decimal("10")
    assert order.submitted_at == ctx.now


def test_order_ids_are_sequential_so_two_runs_can_be_compared() -> None:
    ctx = _ctx()
    first = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="a")
    second = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="b")
    assert (first, second) == (1, 2)


def test_a_float_quantity_is_refused() -> None:
    ctx = _ctx()
    with pytest.raises(TypeError, match="Decimal"):
        ctx.order(1, side="BUY", quantity=10.0, rationale="oops")  # type: ignore[arg-type]


def test_an_empty_rationale_is_refused() -> None:
    ctx = _ctx()
    with pytest.raises(ValueError, match="rationale"):
        ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="   ")


def test_an_order_for_an_instrument_outside_the_universe_is_rejected_not_raised() -> None:
    # Contract §6: a rejection is a normal outcome the strategy learns
    # about through on_order_update, never an exception.
    ctx = _ctx()
    order_id = ctx.order(9999, side="BUY", quantity=Decimal("1"), rationale="not mine")
    order = ctx._state.orders[order_id]
    assert order.status is OrderStatus.REJECTED
    assert order.rejection_reason is not None
    assert "universe" in order.rejection_reason


def test_a_non_positive_quantity_is_rejected_not_raised() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("0"), rationale="zero")
    assert ctx._state.orders[order_id].status is OrderStatus.REJECTED


def test_cancel_marks_an_open_order_cancelled() -> None:
    ctx = _ctx()
    order_id = ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="a")
    ctx.cancel(order_id)
    assert ctx._state.orders[order_id].status is OrderStatus.CANCELLED


def test_cancelling_an_unknown_order_is_silent() -> None:
    # A strategy cancelling an order that already filled is ordinary, not
    # a crash -- and a crash here would fail an otherwise sound strategy.
    _ctx().cancel(4242)


def test_log_records_structured_events() -> None:
    ctx = _ctx()
    ctx.log("crossover", fast="10.5", slow="10.1")
    assert ctx._state.logs == [
        {"ts": ctx.now.isoformat(), "event": "crossover", "fast": "10.5", "slow": "10.1"}
    ]


def test_portfolio_cash_and_equity_are_visible() -> None:
    ctx = _ctx()
    assert ctx.portfolio.cash == Decimal("100000")
    assert ctx.portfolio.equity == Decimal("100000")
    assert ctx.portfolio.positions == {}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/runtime/test_context.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trading.runtime.context'`

Note: the runtime imports the SDK as `from trading.agent_contract import platform_sdk`, its real location. The bare name `platform_sdk` works only inside the container, where `PYTHONPATH=/opt` puts it there, and Task 4 makes the runner alias the canonical module under that bare name so a strategy's `from platform_sdk import Strategy` and the runtime's import resolve to the **same module object**. Do NOT add a `pythonpath` entry to `pyproject.toml` to make the bare import work on the host — that would create a second module object for the same file, with a second `Context` class, and the subclass relationship this decision rests on would silently stop being one.

- [ ] **Step 3: Implement RunState**

Create `src/trading/runtime/state.py`:

```python
"""Everything one run of a strategy accumulates.

Held in one mutable object rather than threaded through call signatures
because two collaborators need the same view of it: `LiveContext` writes
orders and logs into it, and `EventLoop` reads those out, fills them, and
writes cash and positions back. Passing it explicitly to both keeps the
sharing visible instead of hiding it in globals.

Every value inside is either a Decimal or a frozen pydantic model, so a
strategy handed a `Position` cannot rewrite the portfolio by mutating it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from trading.paper.models import Order, Position

__all__ = ["RunState"]


@dataclass
class RunState:
    now: datetime
    cash: Decimal
    starting_cash: Decimal
    positions: dict[int, Position] = field(default_factory=dict)
    orders: dict[int, Order] = field(default_factory=dict)
    # Submission order, kept separately from `orders` because dict
    # ordering is an implementation detail and D-S6 compares sequences.
    submissions: list[int] = field(default_factory=list)
    logs: list[dict[str, Any]] = field(default_factory=list)
    # How many of each instrument's bars have closed. `ctx.data` reads
    # strictly below this, which is the whole anti-lookahead mechanism.
    cursor: dict[int, int] = field(default_factory=dict)
    marks: dict[int, Decimal] = field(default_factory=dict)
    bar_calls: int = 0
    next_order_id: int = 1
    day_open_equity: Decimal | None = None
    peak_equity: Decimal | None = None
    breaker_reason: str | None = None
```

- [ ] **Step 4: Implement LiveContext**

Create `src/trading/runtime/context.py`:

```python
"""The live `Context` -- what a strategy actually holds.

**It subclasses the offline stub rather than reimplementing it** (design
D-S2). The classes in `platform_sdk` that raise `NotOnThisPlatform` are
exactly the ones a strategy only ever *receives*: `Context`, `DataAccess`,
`PortfolioView`. The ones a strategy constructs -- `StrategyManifest`,
`Param`, `Query`, `DataRequest`, and the `Strategy` base -- are real
working code with real validation already in them.

So there is no second SDK, no `sys.modules` substitution, and no
de-stubbed copy to keep in step. A strategy imports the same
`platform_sdk` it type-checked against offline, and shape drift between
the stub and the runtime is impossible because the runtime is a subclass
of the stub. A method added to the stub and not implemented here raises
`NotOnThisPlatform` at exactly its call site -- loud and correct, rather
than silently diverging.

The order path deliberately splits its failures in two, following
contract §6. A **contract violation** -- a float quantity, an empty
rationale, a LIMIT without a price -- raises, because it is a bug in the
strategy's code that the author must see immediately and that the offline
stub would already have caught. A **business rejection** -- an unknown
instrument, a non-positive size -- does not raise: it produces a REJECTED
order delivered through `on_order_update`, because §6 makes a rejection a
normal outcome the strategy must survive, and a smoke run that crashed on
one would never test that it does.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from trading.agent_contract import platform_sdk
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.models import Order, Position
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState

__all__ = ["SMOKE_PORTFOLIO_ID", "LiveContext"]

# The smoke run is one strategy against one portfolio (contract D6), and
# nothing is persisted, so the id is a constant rather than a lookup.
SMOKE_PORTFOLIO_ID = 1


class LiveDataAccess(platform_sdk.DataAccess):
    def __init__(self, state: RunState, bars: InMemoryBars) -> None:
        self._state = state
        self._bars = bars

    def bars(
        self, instrument_id: int, *, interval: str = "1m", count: int = 100
    ) -> list[BarRecord]:
        closed = self._state.cursor.get(instrument_id, 0)
        history = self._bars.history(instrument_id, closed)
        return list(history[-count:]) if count > 0 else []

    def last(self, instrument_id: int) -> BarRecord | None:
        recent = self.bars(instrument_id, count=1)
        return recent[-1] if recent else None

    def lot_size(self, instrument_id: int) -> int | None:
        # Point-in-time lot history is not shipped into the sandbox, and
        # inventing a lot size would size every F&O order wrong in a way
        # the strategy could not detect. None is the honest answer, and
        # it is the documented value for instruments with no lot concept.
        return None


class LivePortfolioView(platform_sdk.PortfolioView):
    def __init__(self, state: RunState) -> None:
        self._state = state

    @property
    def cash(self) -> Decimal:
        return self._state.cash

    @property
    def positions(self) -> dict[int, Position]:
        # A copy: the dict is the runtime's, and a strategy that cleared
        # it would silently erase its own holdings.
        return dict(self._state.positions)

    @property
    def equity(self) -> Decimal:
        total = self._state.cash
        for position in self._state.positions.values():
            if position.quantity == 0:
                continue
            mark = self._state.marks.get(position.instrument_id)
            if mark is not None:
                total += position.quantity * mark
        return total


class LiveContext(platform_sdk.Context):
    def __init__(
        self, state: RunState, bars: InMemoryBars, portfolio_id: int = SMOKE_PORTFOLIO_ID
    ) -> None:
        self._state = state
        self._bars = bars
        self._portfolio_id = portfolio_id
        self._universe = frozenset(bars.instruments())
        self.data = LiveDataAccess(state, bars)
        self.portfolio = LivePortfolioView(state)
        self.state: dict[str, Any] = {}

    @property
    def now(self):  # type: ignore[no-untyped-def]
        return self._state.now

    def _reject(self, order: Order, reason: str) -> int:
        rejected = order.model_copy(
            update={"status": OrderStatus.REJECTED, "rejection_reason": reason}
        )
        self._state.orders[rejected.order_id] = rejected
        return rejected.order_id

    def order(
        self,
        instrument_id: int,
        *,
        side: str,
        quantity: Decimal,
        rationale: str,
        order_type: str = "MARKET",
        limit_price: Decimal | None = None,
        product: str = "DELIVERY",
        time_in_force: str = "DAY",
    ) -> int:
        # Contract violations raise, via the stub's own checks: float
        # money, empty rationale, LIMIT without a price, MARKET with one.
        # Deliberately called for its validation, and its `NotOnThisPlatform`
        # is what tells us validation passed.
        try:
            super().order(
                instrument_id,
                side=side,  # type: ignore[arg-type]
                quantity=quantity,
                rationale=rationale,
                order_type=order_type,  # type: ignore[arg-type]
                limit_price=limit_price,
                product=product,  # type: ignore[arg-type]
                time_in_force=time_in_force,  # type: ignore[arg-type]
            )
        except platform_sdk.NotOnThisPlatform:
            pass

        order_id = self._state.next_order_id
        self._state.next_order_id += 1
        self._state.submissions.append(order_id)

        order = Order(
            order_id=order_id,
            portfolio_id=self._portfolio_id,
            instrument_id=instrument_id,
            side=Side(side),
            order_type=OrderType(order_type),
            quantity=quantity,
            filled_quantity=Decimal("0"),
            limit_price=limit_price,
            product=Product(product),
            time_in_force=TimeInForce(time_in_force),
            status=OrderStatus.OPEN,
            rationale=rationale,
            submitted_at=self._state.now,
        )

        if instrument_id not in self._universe:
            return self._reject(
                order,
                f"instrument_id={instrument_id} is not in this run's universe; "
                "a strategy may only trade what its manifest declared",
            )
        if quantity <= 0:
            return self._reject(order, f"quantity must be positive, got {quantity}")

        self._state.orders[order_id] = order
        return order_id

    def cancel(self, order_id: int) -> None:
        order = self._state.orders.get(order_id)
        if order is None:
            return
        if order.status not in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED):
            return
        self._state.orders[order_id] = order.model_copy(
            update={"status": OrderStatus.CANCELLED}
        )

    def log(self, event: str, **fields: Any) -> None:
        self._state.logs.append(
            {"ts": self._state.now.isoformat(), "event": event, **fields}
        )
```

- [ ] **Step 5: Run the Context tests**

Run: `uv run pytest tests/runtime/test_context.py -v`
Expected: 15 passed.

- [ ] **Step 6: Verify the subclass check is not vacuous**

Temporarily change `class LiveContext(platform_sdk.Context)` to `class LiveContext:`. Run `uv run pytest tests/runtime/test_context.py::test_it_is_a_platform_sdk_context -v` and confirm it FAILS. Revert.

- [ ] **Step 7: Lint, type-check, and commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
git add src/trading/runtime/state.py src/trading/runtime/context.py tests/runtime/test_context.py pyproject.toml
git commit -m "feat(runtime): run state and the live Context

LiveContext SUBCLASSES platform_sdk.Context rather than reimplementing it
(design D-S2). The stub's raising classes -- Context, DataAccess,
PortfolioView -- are exactly the ones a strategy only ever receives, so
the runtime can inherit the shape it must match. Drift between the
offline stub an agent type-checks against and the runtime that executes
its code is therefore not a thing that has to be tested for; it is a
thing that cannot happen.

Order failures split as contract §6 requires: a contract violation (float
quantity, empty rationale) raises because it is a bug the author must
see, while a business rejection produces a REJECTED order delivered
through on_order_update, because a strategy is required to survive one
and a run that crashed would never prove it does."
```

---

### Task 3: The event loop

The engine. Pure, no Docker, no database — and the module Phase 3's backtester will reuse unchanged.

**Files:**
- Create: `src/trading/runtime/outcome.py`
- Create: `src/trading/runtime/loop.py`
- Test: `tests/runtime/test_loop.py`

**Interfaces:**
- Consumes: `RunState`, `LiveContext` (Task 2); `InMemoryBars`, `BarRecord` (Task 1); `trading.paper.fills.decide_fill`; `trading.paper.charges.compute_charges`; `trading.paper.breaker.evaluate_breach`.
- Produces:
  - `OrderSnapshot` — frozen dataclass: `order_id`, `instrument_id`, `side`, `order_type`, `quantity: str`, `limit_price: str | None`, `status`, `submitted_at: str`. The unit D-S6 compares.
  - `RunOutcome` — frozen dataclass: `ok: bool`, `crashed_at: dict | None`, `error: str | None`, `bar_calls: int`, `orders: tuple[OrderSnapshot, ...]`, `fills: int`, `rejections: tuple[str, ...]`, `final_cash: str`, `final_equity: str`, `breaker_reason: str | None`, `logs: tuple[dict, ...]`.
  - `run_loop(strategy, bars, schedules, starting_cash, slippage_bps) -> RunOutcome`.

- [ ] **Step 1: Write the failing loop test**

Create `tests/runtime/test_loop.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal

from trading.paper.enums import ChargeBasis, ChargeType, OrderStatus, Product, Rounding
from trading.paper.models import ChargeSchedule
from trading.runtime.loop import run_loop
from trading.runtime.provider import BarRecord, InMemoryBars


def _schedules() -> tuple[ChargeSchedule, ...]:
    return (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("20.00"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )


def _series(instrument_id: int, closes: list[str]) -> list[BarRecord]:
    return [
        BarRecord(
            instrument_id=instrument_id,
            ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal(close),
            high=Decimal(close),
            low=Decimal(close),
            close=Decimal(close),
            volume=Decimal("100"),
        )
        for minute, close in enumerate(closes)
    ]


class _Recorder:
    """A strategy that records what it was handed."""

    def __init__(self) -> None:
        self.bar_batches: list[list[int]] = []
        self.updates: list[tuple[int, str, str]] = []
        self.initialized = False

    def initialize(self, ctx) -> None:  # noqa: ANN001
        self.initialized = True

    def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
        self.bar_batches.append(sorted(bars))

    def on_order_update(self, ctx, update) -> None:  # noqa: ANN001
        self.updates.append(
            (update.order.order_id, str(update.previous_status), str(update.order.status))
        )


def _run(strategy, bars, cash="100000") -> object:  # noqa: ANN001
    return run_loop(
        strategy=strategy,
        bars=bars,
        schedules=_schedules(),
        starting_cash=Decimal(cash),
        slippage_bps=Decimal("0"),
    )


def test_initialize_runs_once_before_any_bar() -> None:
    recorder = _Recorder()
    _run(recorder, InMemoryBars({1: _series(1, ["10", "11"])}))
    assert recorder.initialized is True
    assert len(recorder.bar_batches) == 2


def test_an_instrument_that_did_not_print_is_absent_not_carried_forward() -> None:
    # Contract §4. The platform will not invent a trade that did not happen.
    recorder = _Recorder()
    bars = InMemoryBars({1: _series(1, ["10", "11"]), 2: _series(2, ["20"])})
    _run(recorder, bars)
    assert recorder.bar_batches == [[1, 2], [1]]


def test_a_market_order_fills_on_the_next_bar_never_the_current_one() -> None:
    # The lookahead that would matter most: a strategy that saw this
    # bar's close must not trade at this bar's prices.
    class BuyOnce:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="entry")

    outcome = _run(BuyOnce(), InMemoryBars({1: _series(1, ["10", "20", "30"])}))
    assert outcome.fills == 1
    # Submitted while bar 0 (close 10) was dispatched; filled against bar
    # 1's open of 20, not bar 0's 10.
    assert outcome.orders[0].status == str(OrderStatus.FILLED)
    assert Decimal(outcome.final_cash) == Decimal("100000") - Decimal("200") - Decimal("20")


def test_on_order_update_fires_with_the_previous_status() -> None:
    class BuyOnce:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="entry")

    recorder = _Recorder()
    strategy = BuyOnce()
    strategy.on_order_update = recorder.on_order_update  # type: ignore[attr-defined]
    _run(strategy, InMemoryBars({1: _series(1, ["10", "20"])}))
    assert recorder.updates == [(1, "OPEN", "FILLED")]


def test_a_rejection_is_delivered_through_on_order_update_not_raised() -> None:
    class BadOrder:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(9999, side="BUY", quantity=Decimal("1"), rationale="not mine")

    recorder = _Recorder()
    strategy = BadOrder()
    strategy.on_order_update = recorder.on_order_update  # type: ignore[attr-defined]
    outcome = _run(strategy, InMemoryBars({1: _series(1, ["10", "20"])}))
    assert outcome.ok is True
    assert len(outcome.rejections) == 1
    assert recorder.updates == [(1, "OPEN", "REJECTED")]


def test_a_limit_order_fills_at_the_limit_not_at_the_better_price() -> None:
    class LimitBuy:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(
                    1,
                    side="BUY",
                    quantity=Decimal("10"),
                    order_type="LIMIT",
                    limit_price=Decimal("15"),
                    rationale="limit entry",
                )

    outcome = _run(LimitBuy(), InMemoryBars({1: _series(1, ["20", "10"])}))
    # Bar 1 trades at 10, well below the 15 limit. The fill is at 15.
    assert Decimal(outcome.final_cash) == Decimal("100000") - Decimal("150") - Decimal("20")


def test_a_crash_in_on_bar_is_captured_with_where_it_happened() -> None:
    class Exploding:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            raise ValueError("boom")

    outcome = _run(Exploding(), InMemoryBars({1: _series(1, ["10", "11"])}))
    assert outcome.ok is False
    assert "boom" in (outcome.error or "")
    assert outcome.crashed_at is not None
    assert outcome.crashed_at["handler"] == "on_bar"
    assert outcome.crashed_at["ts"] == "2026-09-01T09:01:00+00:00"


def test_a_strategy_that_never_orders_completes_cleanly() -> None:
    outcome = _run(_Recorder(), InMemoryBars({1: _series(1, ["10", "11"])}))
    assert outcome.ok is True
    assert outcome.orders == ()
    assert outcome.bar_calls == 2


def test_the_breaker_trips_on_a_declared_daily_loss() -> None:
    class BuyAndHold:
        def __init__(self) -> None:
            self.done = False

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if not self.done:
                self.done = True
                ctx.order(1, side="BUY", quantity=Decimal("100"), rationale="entry")

    bars = InMemoryBars({1: _series(1, ["100", "100", "10"])})
    outcome = run_loop(
        strategy=BuyAndHold(),
        bars=bars,
        schedules=_schedules(),
        starting_cash=Decimal("100000"),
        slippage_bps=Decimal("0"),
        max_daily_loss=Decimal("1000"),
    )
    assert outcome.breaker_reason is not None
    assert "MAX_DAILY_LOSS" in outcome.breaker_reason


def test_two_identical_runs_produce_identical_order_snapshots() -> None:
    # The property D-S6's double-run check relies on.
    class BuyEveryBar:
        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="always")

    bars = InMemoryBars({1: _series(1, ["10", "11", "12"])})
    first = _run(BuyEveryBar(), bars)
    second = _run(BuyEveryBar(), bars)
    assert first.orders == second.orders
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/runtime/test_loop.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trading.runtime.loop'`

- [ ] **Step 3: Implement the outcome types**

Create `src/trading/runtime/outcome.py`:

```python
"""What one execution of a strategy produced.

Every money value is a **string**, not a Decimal and never a float. This
crosses a process boundary as JSON on its way out of the container, and
the same reasoning that makes the inbound payload text applies to the
result: a number here would be an IEEE 754 double, and a smoke run that
reported subtly wrong cash would be worse than one that reported none.

`OrderSnapshot` is the unit the determinism check compares (D-S6), which
is why it is frozen, fully ordered, and carries no object references --
two runs of the same payload must produce tuples that are equal or
unequal for reasons visible in the tuple itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["OrderSnapshot", "RunOutcome"]


@dataclass(frozen=True)
class OrderSnapshot:
    order_id: int
    instrument_id: int
    side: str
    order_type: str
    quantity: str
    limit_price: str | None
    status: str
    submitted_at: str


@dataclass(frozen=True)
class RunOutcome:
    ok: bool
    bar_calls: int
    orders: tuple[OrderSnapshot, ...]
    fills: int
    rejections: tuple[str, ...]
    final_cash: str
    final_equity: str
    breaker_reason: str | None = None
    logs: tuple[dict[str, Any], ...] = ()
    error: str | None = None
    crashed_at: dict[str, Any] | None = None
```

- [ ] **Step 4: Implement the loop**

Create `src/trading/runtime/loop.py`:

```python
"""The event loop -- the runtime the whole Agent Contract rests on.

Pure: bars in, orders and fills out. No database, no Docker, no clock.
That is not tidiness. This module is what Phase 3's backtester will reuse
unchanged, fed from Timescale instead of from a payload, and it is what
runs inside the sandbox where neither a database nor a network exists.

**Step order is load-bearing, and it is: clock, fills, updates, on_bar.**

`decide_fill`'s anti-lookahead guard is `tick_ts < order.submitted_at`.
An order submitted during bar N's `on_bar` carries
`submitted_at == N.close_ts`, which is not *less than* bar N's own tick
timestamps -- so if `on_bar` ran before fills, an order could fill against
the very bar whose close the strategy had just read. Filling first means
such an order simply does not exist yet when bar N is priced, and the
guard is never asked a question it would answer wrongly.

**Bars are expanded into four price events, open then high then low then
close.** `decide_fill` prices one event at a time by design (it is shared
with the live tick engine), so a bar has to become ticks. Open-high-low-
close is the conventional backtest approximation and it is an
approximation: it assumes a limit order resting inside the bar's range
was reachable, which flatters limit fills, and it cannot know the true
intra-bar path. Stated here rather than discovered later.

**`decide_fill` always fills `order.remaining` in full**, so
`PARTIALLY_FILLED` never occurs in a smoke run. That is a real gap in
what stage 2 exercises, and `trading.agent_contract.smoke` reports it
rather than letting anyone infer coverage that does not exist.
"""

from __future__ import annotations

import traceback
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Protocol

from trading.paper.breaker import evaluate_breach
from trading.paper.charges import compute_charges
from trading.paper.enums import OrderStatus, Product, Side
from trading.paper.fills import decide_fill
from trading.paper.models import ChargeSchedule, Order, Position
from trading.runtime.context import SMOKE_PORTFOLIO_ID, LiveContext
from trading.runtime.outcome import OrderSnapshot, RunOutcome
from trading.runtime.provider import BarRecord, InMemoryBars
from trading.runtime.state import RunState

__all__ = ["run_loop"]

_TERMINAL = (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED)


class _StrategyLike(Protocol):
    def initialize(self, ctx: Any) -> None: ...
    def on_bar(self, ctx: Any, bars: dict[int, Any]) -> None: ...


class _Update:
    """The `OrderUpdate` shape the contract promises `on_order_update`."""

    def __init__(self, order: Order, previous_status: OrderStatus) -> None:
        self.order = order
        self.previous_status = previous_status


class _Crash(Exception):
    """A strategy handler raised. Carries where, so the report can say."""

    def __init__(self, handler: str, ts: str, detail: str) -> None:
        super().__init__(detail)
        self.handler = handler
        self.ts = ts
        self.detail = detail


def _call(strategy: object, handler: str, ts: str, *args: Any) -> None:
    method = getattr(strategy, handler, None)
    if method is None:
        return
    try:
        method(*args)
    except Exception as exc:  # noqa: BLE001 - every strategy failure is an outcome
        raise _Crash(handler, ts, traceback.format_exc(limit=20)) from exc


def _snapshot(order: Order) -> OrderSnapshot:
    return OrderSnapshot(
        order_id=order.order_id,
        instrument_id=order.instrument_id,
        side=str(order.side),
        order_type=str(order.order_type),
        quantity=str(order.quantity),
        limit_price=None if order.limit_price is None else str(order.limit_price),
        status=str(order.status),
        submitted_at=order.submitted_at.isoformat(),
    )


def _apply_position(state: RunState, order: Order, quantity: Decimal, price: Decimal) -> None:
    existing = state.positions.get(order.instrument_id)
    signed = quantity if order.side is Side.BUY else -quantity
    if existing is None:
        state.positions[order.instrument_id] = Position(
            portfolio_id=SMOKE_PORTFOLIO_ID,
            instrument_id=order.instrument_id,
            quantity=signed,
            avg_cost=price,
            realised_pnl=Decimal("0"),
        )
        return
    new_quantity = existing.quantity + signed
    if existing.quantity != 0 and (existing.quantity > 0) != (signed > 0):
        # Reducing or reversing: realise against the average cost.
        closed = min(abs(signed), abs(existing.quantity))
        direction = Decimal("1") if existing.quantity > 0 else Decimal("-1")
        realised = (price - existing.avg_cost) * closed * direction
        avg_cost = existing.avg_cost if new_quantity != 0 else Decimal("0")
        state.positions[order.instrument_id] = existing.model_copy(
            update={
                "quantity": new_quantity,
                "avg_cost": avg_cost,
                "realised_pnl": existing.realised_pnl + realised,
            }
        )
        return
    total_cost = existing.avg_cost * abs(existing.quantity) + price * quantity
    avg_cost = total_cost / abs(new_quantity) if new_quantity != 0 else Decimal("0")
    state.positions[order.instrument_id] = existing.model_copy(
        update={"quantity": new_quantity, "avg_cost": avg_cost}
    )


def _price_events(bar: BarRecord) -> tuple[Decimal, ...]:
    return (bar.open, bar.high, bar.low, bar.close)


def run_loop(
    strategy: _StrategyLike,
    bars: InMemoryBars,
    schedules: Sequence[ChargeSchedule],
    starting_cash: Decimal,
    slippage_bps: Decimal,
    max_daily_loss: Decimal | None = None,
    max_drawdown_pct: Decimal | None = None,
) -> RunOutcome:
    state = RunState(
        now=None,  # type: ignore[arg-type]  # set before any handler runs
        cash=starting_cash,
        starting_cash=starting_cash,
    )
    state.cursor = dict.fromkeys(bars.instruments(), 0)
    state.day_open_equity = starting_cash
    state.peak_equity = starting_cash
    ctx = LiveContext(state=state, bars=bars)

    fills = 0
    rejections: list[str] = []
    reported: dict[int, OrderStatus] = {}
    # A FLAT_PER_SCRIP_PER_DAY charge (DP) is once per scrip per day, not
    # per fill. `compute_charges` stays pure and is told, not asked.
    scrip_days: set[tuple[int, Any]] = set()

    def _deliver_updates(ts_iso: str) -> None:
        nonlocal rejections
        for order_id in list(state.submissions):
            order = state.orders.get(order_id)
            if order is None:
                continue
            previous = reported.get(order_id)
            if previous == order.status:
                continue
            if previous is not None or order.status is not OrderStatus.OPEN:
                _call(strategy, "on_order_update", ts_iso, ctx, _Update(order, previous or OrderStatus.OPEN))
                if order.status is OrderStatus.REJECTED and order.rejection_reason:
                    rejections.append(order.rejection_reason)
            reported[order_id] = order.status

    try:
        first_ts = next(iter(bars.groups()), None)
        if first_ts is None:
            raise _Crash("initialize", "", "no bars were provided to the run")
        state.now = first_ts[0]
        _call(strategy, "initialize", state.now.isoformat(), ctx)

        for close_ts, indexed in bars.indexed_groups():
            state.now = close_ts
            ts_iso = close_ts.isoformat()
            printed = {bar.instrument_id: bar for bar, _ in indexed}
            for instrument_id, bar in printed.items():
                state.marks[instrument_id] = bar.close

            # 1. Price resting orders against this bar, before the
            #    strategy has seen it. See the module docstring.
            for order_id in list(state.submissions):
                order = state.orders.get(order_id)
                if order is None or order.status in _TERMINAL:
                    continue
                bar = printed.get(order.instrument_id)
                if bar is None:
                    continue
                for price in _price_events(bar):
                    order = state.orders[order_id]
                    if order.status in _TERMINAL:
                        break
                    decision = decide_fill(order, price, close_ts, slippage_bps)
                    if decision is None:
                        continue
                    key = (order.instrument_id, close_ts.date())
                    already = key in scrip_days
                    breakdown = compute_charges(
                        schedules,
                        order.side,
                        decision.quantity,
                        decision.price,
                        scrip_day_charge_already_applied=already,
                    )
                    if order.product is Product.DELIVERY and order.side is Side.SELL:
                        scrip_days.add(key)
                    notional = decision.quantity * decision.price
                    if order.side is Side.BUY:
                        state.cash -= notional + breakdown.total
                    else:
                        state.cash += notional - breakdown.total
                    _apply_position(state, order, decision.quantity, decision.price)
                    filled = order.filled_quantity + decision.quantity
                    state.orders[order_id] = order.model_copy(
                        update={
                            "filled_quantity": filled,
                            "status": (
                                OrderStatus.FILLED
                                if filled >= order.quantity
                                else OrderStatus.PARTIALLY_FILLED
                            ),
                        }
                    )
                    fills += 1

            # 2. Tell the strategy what changed.
            _deliver_updates(ts_iso)

            # 3. Dispatch the bar. Only instruments that actually printed.
            state.bar_calls += 1
            _call(strategy, "on_bar", ts_iso, ctx, dict(printed))

            # 4. Any order submitted in on_bar is OPEN and unreported;
            #    a rejection must reach the strategy in the same session.
            _deliver_updates(ts_iso)

            # 5. Advance the cursor. Only now has this bar "closed" for
            #    ctx.data -- during on_bar it was the present, not history.
            for bar, index in indexed:
                state.cursor[bar.instrument_id] = index + 1

            # 6. The breaker.
            equity = ctx.portfolio.equity
            state.peak_equity = max(state.peak_equity or equity, equity)
            if state.breaker_reason is None:
                state.breaker_reason = evaluate_breach(
                    equity,
                    state.day_open_equity or starting_cash,
                    state.peak_equity,
                    max_daily_loss,
                    max_drawdown_pct,
                )
    except _Crash as crash:
        return RunOutcome(
            ok=False,
            bar_calls=state.bar_calls,
            orders=tuple(_snapshot(state.orders[i]) for i in state.submissions if i in state.orders),
            fills=fills,
            rejections=tuple(rejections),
            final_cash=str(state.cash),
            final_equity=str(state.cash),
            breaker_reason=state.breaker_reason,
            logs=tuple(state.logs),
            error=crash.detail,
            crashed_at={"handler": crash.handler, "ts": crash.ts, "bar_calls": state.bar_calls},
        )

    return RunOutcome(
        ok=True,
        bar_calls=state.bar_calls,
        orders=tuple(_snapshot(state.orders[i]) for i in state.submissions if i in state.orders),
        fills=fills,
        rejections=tuple(rejections),
        final_cash=str(state.cash),
        final_equity=str(ctx.portfolio.equity),
        breaker_reason=state.breaker_reason,
        logs=tuple(state.logs),
    )
```

- [ ] **Step 5: Run the loop tests**

Run: `uv run pytest tests/runtime/test_loop.py -v`
Expected: 10 passed. (11 after Step 6 adds the golden test.)

Debugging notes if a test fails:
- `test_a_market_order_fills_on_the_next_bar` failing with a fill at 10 means fills ran after `on_bar`. Re-read the module docstring's step-order argument.
- A `MissingChargeSchedule` means `compute_charges` was handed an empty sequence; the fixture must supply at least one schedule.
- Cash off by the brokerage means the `.total` was not applied on the correct side.

- [ ] **Step 6: Add the golden crossover test**

Behaviour tests prove each rule in isolation; this pins them *together*, so a
change that keeps every unit test green while altering what a strategy
actually trades still fails. Append to `tests/runtime/test_loop.py`:

```python
class _SmaCrossover:
    """Buy when the 2-bar mean crosses above the 4-bar mean, sell when back below."""

    def __init__(self) -> None:
        self.held = False

    def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
        history = ctx.data.bars(1, count=4)
        if len(history) < 4:
            return
        fast = sum(b.close for b in history[-2:]) / Decimal("2")
        slow = sum(b.close for b in history) / Decimal("4")
        if fast > slow and not self.held:
            self.held = True
            ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="fast crossed above slow")
        elif fast < slow and self.held:
            self.held = False
            ctx.order(1, side="SELL", quantity=Decimal("10"), rationale="fast crossed below slow")


def test_golden_sma_crossover_trades_exactly_where_expected() -> None:
    # Closes: a rise into bar 5, then a fall. Hand-computed:
    #   bar 4 (close 14): fast=(13+14)/2=13.5  slow=(11+12+13+14)/4=12.5  -> cross up, BUY
    #   bar 7 (close 8):  fast=(10+8)/2=9.0    slow=(14+12+10+8)/4=11.0   -> cross down, SELL
    bars = InMemoryBars({1: _series(1, ["11", "12", "13", "14", "15", "12", "10", "8", "8"])})
    outcome = _run(_SmaCrossover(), bars)

    assert outcome.ok is True
    assert [(o.side, o.quantity, o.status) for o in outcome.orders] == [
        ("BUY", "10", "FILLED"),
        ("SELL", "10", "FILLED"),
    ]
    assert outcome.fills == 2
```

Run it and read the failure before adjusting anything. If the orders land on
different bars than the comment predicts, **recompute the means by hand from
the series before touching the loop** — a golden test whose expectations were
edited to match the code it grades proves nothing. Update the comment with the
real arithmetic and keep the assertion honest.

- [ ] **Step 7: Verify the anti-lookahead test is not vacuous**

Move the fills block (step 1) to after the `on_bar` dispatch (step 3). Run `uv run pytest tests/runtime/test_loop.py::test_a_market_order_fills_on_the_next_bar_never_the_current_one -v` and confirm it FAILS. Revert.

- [ ] **Step 8: Lint, type-check, and commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
git add src/trading/runtime/loop.py src/trading/runtime/outcome.py tests/runtime/test_loop.py
git commit -m "feat(runtime): the event loop

Pure -- bars in, orders and fills out -- because this is the module Phase
3's backtester reuses unchanged and the module that runs inside a sandbox
with no database and no network.

Step order is the load-bearing decision: clock, fills, updates, on_bar.
decide_fill's anti-lookahead guard is 'tick_ts < submitted_at', and an
order submitted during bar N's on_bar carries submitted_at == N.close_ts,
which is not less than bar N's own ticks. Filling before dispatch means
such an order does not exist yet when the bar is priced, so the guard is
never asked a question it would answer wrongly. A regression test pins
this by moving money if the order is reversed.

Two approximations are documented rather than left to be discovered:
bars are expanded to open/high/low/close price events, which flatters
limit fills; and decide_fill always fills the full remaining quantity, so
PARTIALLY_FILLED is never exercised by a smoke run."
```

---

### Task 4: The runner, the image, and the drift test

Puts the runtime inside the container. Everything before this ran in-process.

**Files:**
- Modify: `sandbox/runner.py` (full rewrite of `main()`; keep `_emit` and `_describe_manifest`)
- Modify: `sandbox/Dockerfile:26-31` (the COPY block)
- Modify: `src/trading/agent_contract/sandbox.py:190-277` (`run_strategy_in_sandbox` builds an envelope; add `run_smoke_in_sandbox`)
- Create: `tests/agent_contract/test_image_contents.py`
- Modify: `tests/agent_contract/test_sandbox.py` (add smoke-mode container tests)

**Interfaces:**
- Consumes: `encode_payload`, `decode_payload`, `SmokePayload`, `MODE_CONFIGURE`, `MODE_SMOKE` (Task 1); `run_loop` (Task 3).
- Produces:
  - `SandboxLimits` gains `timeout_seconds: float = 30.0` unchanged plus a new module constant `SMOKE_TIMEOUT_SECONDS = 120.0`.
  - `SandboxResult` gains `outcome: dict[str, Any] | None = None`.
  - `run_smoke_in_sandbox(payload: SmokePayload, limits: SandboxLimits | None = None) -> SandboxResult`.

- [ ] **Step 1: Write the failing drift test**

Create `tests/agent_contract/test_image_contents.py`:

```python
"""The image must contain what the runner imports, and the runtime must
not import what the container cannot provide.

Both directions matter. A module the runner imports but the Dockerfile
does not COPY fails at run time, inside a container, as an opaque import
error -- exactly the failure the structured result exists to avoid. And a
`trading.runtime` module that reaches for psycopg would work in every
in-process test and die only in the sandbox.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNTIME = REPO / "src" / "trading" / "runtime"
DOCKERFILE = REPO / "sandbox" / "Dockerfile"

FORBIDDEN_IN_CONTAINER = {"psycopg", "docker", "trading.config", "requests", "redis"}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def test_no_runtime_module_imports_something_the_container_lacks() -> None:
    offenders: list[str] = []
    for path in sorted(RUNTIME.glob("*.py")):
        for module in _imported_modules(path):
            root = module.split(".")[0]
            if module in FORBIDDEN_IN_CONTAINER or root in FORBIDDEN_IN_CONTAINER:
                offenders.append(f"{path.name} imports {module}")
    assert offenders == [], f"these would fail inside the sandbox: {offenders}"


def test_every_trading_module_the_runtime_needs_is_copied_into_the_image() -> None:
    needed: set[str] = set()
    for path in sorted(RUNTIME.glob("*.py")):
        needed.update(m for m in _imported_modules(path) if m.startswith("trading."))
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    missing = []
    for module in sorted(needed):
        relative = module.replace(".", "/") + ".py"
        package = "/".join(relative.split("/")[:2])
        if relative not in dockerfile and package not in dockerfile:
            missing.append(module)
    assert missing == [], f"imported by trading.runtime but never COPYed: {missing}"
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_image_contents.py -v`
Expected: `test_every_trading_module_the_runtime_needs_is_copied_into_the_image` FAILS listing `trading.paper.breaker`, `trading.paper.charges`, `trading.paper.enums`, `trading.paper.fills`, `trading.paper.models`, `trading.runtime.*`.

- [ ] **Step 3: Update the Dockerfile**

In `sandbox/Dockerfile`, replace the `COPY runner.py ...` block with:

```dockerfile
# pydantic joins numpy and pandas because trading.paper.models is a
# pydantic model set and the real cost model runs in here (design D-S3).
RUN pip install --no-cache-dir --disable-pip-version-check \
      numpy==2.5.2 \
      pandas==2.3.3 \
      pydantic==2.12.3

# The runner is baked in rather than mounted, so a caller cannot swap it
# for something that skips the guards below. The same is true of the
# runtime: a strategy must not be able to replace the event loop that
# decides what its orders did.
COPY runner.py /opt/runner.py
COPY trading /opt/trading
RUN chmod -R a-w /opt
```

The SDK is no longer copied to `/opt/platform_sdk.py`. It ships inside
`/opt/trading/agent_contract/platform_sdk.py` — its real package path — and
the runner aliases it under the bare name `platform_sdk` that strategies
import. Two copies at two paths would be two module objects with two
`Context` classes, and the subclass relationship D-S2 rests on would
silently stop being one.

Then create the build context by copying the real modules — a build step, not a checked-in duplicate:

```bash
cat >> sandbox/build.sh <<'SH'
#!/usr/bin/env bash
# Assemble the image's build context from the real source tree, so the
# container runs the same bytes the host tests do rather than a copy that
# can drift. Run from the repo root.
set -euo pipefail
rm -rf sandbox/trading
mkdir -p sandbox/trading/paper sandbox/trading/runtime sandbox/trading/agent_contract
touch sandbox/trading/__init__.py sandbox/trading/agent_contract/__init__.py
cp src/trading/paper/{__init__,enums,models,fills,charges,breaker}.py sandbox/trading/paper/
cp src/trading/runtime/*.py sandbox/trading/runtime/
# The SDK ships at its real package path, not as a second top-level copy.
cp src/trading/agent_contract/platform_sdk.py sandbox/trading/agent_contract/
docker build -t trading-strategy-sandbox:0.1 sandbox/
SH
chmod +x sandbox/build.sh
echo "sandbox/trading/" >> .gitignore
```

- [ ] **Step 4: Verify the drift test now passes and the image builds**

```bash
uv run pytest tests/agent_contract/test_image_contents.py -v
./sandbox/build.sh
```
Expected: 2 passed; image builds. If `trading/paper/charges.py` fails to import inside the image because it imports `psycopg` at module level for `load_schedules`, move that import inside the function — `compute_charges` is the pure half and must not drag psycopg in. Note this in the commit if it happens.

- [ ] **Step 5: Rewrite the runner to dispatch on mode**

Replace `main()` in `sandbox/runner.py` (keep `_emit` and `_describe_manifest` as they are) with:

```python
def _install_sdk_alias() -> None:
    """Make one module answer to both names.

    A strategy writes `from platform_sdk import Strategy`; the runtime
    writes `from trading.agent_contract import platform_sdk`. Two import
    paths to one file produce two distinct module objects in Python, with
    two distinct `Strategy` and `Context` classes -- and the subclass
    relationship the whole SDK decision rests on would silently stop being
    one. Aliasing before any strategy source is executed means there is
    exactly one module, under two names.
    """
    from trading.agent_contract import platform_sdk

    sys.modules.setdefault("platform_sdk", platform_sdk)


def _load_strategy_class(source: str) -> tuple[type | None, dict[str, Any] | None]:
    namespace: dict[str, Any] = {"__name__": "strategy"}
    try:
        exec(compile(source, SOURCE_NAME, "exec"), namespace)  # noqa: S102
    except BaseException:  # noqa: BLE001 - every failure is a reportable outcome
        return None, {"ok": False, "stage": "import", "error": traceback.format_exc(limit=20)}
    candidates = [
        obj
        for name, obj in namespace.items()
        if isinstance(obj, type)
        and name != "Strategy"
        and any(base.__name__ == "Strategy" for base in obj.__mro__[1:])
    ]
    if not candidates:
        return None, {
            "ok": False,
            "stage": "discover",
            "error": "no class inheriting Strategy was defined at module level",
        }
    return candidates[0], None


def main() -> int:
    from trading.runtime.payload import MODE_SMOKE, decode_payload

    _install_sdk_alias()

    try:
        payload = decode_payload(sys.stdin.buffer.read())
    except Exception:  # noqa: BLE001
        _emit({"ok": False, "stage": "payload", "error": traceback.format_exc(limit=20)})
        return 0

    strategy_cls, failure = _load_strategy_class(payload.source)
    if failure is not None:
        _emit(failure)
        return 0
    assert strategy_cls is not None

    try:
        instance = strategy_cls()
        manifest = instance.configure()
    except BaseException:  # noqa: BLE001
        _emit(
            {
                "ok": False,
                "stage": "configure",
                "strategy_class": strategy_cls.__name__,
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    if payload.mode != MODE_SMOKE:
        _emit(
            {
                "ok": True,
                "stage": "configure",
                "strategy_class": strategy_cls.__name__,
                "manifest": _describe_manifest(manifest),
            }
        )
        return 0

    from dataclasses import asdict

    from trading.runtime.loop import run_loop
    from trading.runtime.provider import InMemoryBars

    try:
        outcome = run_loop(
            strategy=instance,
            bars=InMemoryBars(payload.bars),
            schedules=payload.charge_schedules,
            starting_cash=payload.starting_cash,
            slippage_bps=payload.slippage_bps,
            max_daily_loss=getattr(manifest, "max_daily_loss", None),
            max_drawdown_pct=getattr(manifest, "max_drawdown_pct", None),
        )
    except BaseException:  # noqa: BLE001 - the loop itself failing is still an outcome
        _emit(
            {
                "ok": False,
                "stage": "smoke",
                "strategy_class": strategy_cls.__name__,
                "error": traceback.format_exc(limit=20),
            }
        )
        return 0

    _emit(
        {
            "ok": outcome.ok,
            "stage": "smoke",
            "strategy_class": strategy_cls.__name__,
            "manifest": _describe_manifest(manifest),
            "outcome": asdict(outcome),
            "error": outcome.error,
        }
    )
    return 0
```

- [ ] **Step 6: Update the host sandbox module**

In `src/trading/agent_contract/sandbox.py`:

1. Add near `DEFAULT_IMAGE`: `SMOKE_TIMEOUT_SECONDS = 120.0`.
2. Add `outcome: dict[str, Any] | None = None` to `SandboxResult`.
3. Extract the body of `run_strategy_in_sandbox` into `_run_payload(raw: bytes, limits: SandboxLimits) -> SandboxResult`, changing `subprocess.run(..., input=source, capture_output=True, text=True, ...)` to `input=raw` with `text=False`, and decoding `stdout`/`stderr` with `.decode("utf-8", "replace")` before parsing.
4. Rewrite the two public entry points:

```python
def run_strategy_in_sandbox(source: str, limits: SandboxLimits | None = None) -> SandboxResult:
    """Run one strategy's `configure()` inside the sandbox.

    The envelope is built here rather than by callers, so every existing
    call site keeps passing a source string. What must never happen is the
    *runner* guessing whether it received source or an envelope: format
    detection by sniffing is the compatibility shim that breaks silently a
    year later.
    """
    return _run_payload(
        encode_payload(SmokePayload(mode=MODE_CONFIGURE, source=source)),
        limits or SandboxLimits(),
    )


def run_smoke_in_sandbox(
    payload: SmokePayload, limits: SandboxLimits | None = None
) -> SandboxResult:
    """Run five simulated sessions of a strategy (contract §9 stage 2).

    A longer wall clock than `configure` because it is doing far more
    work; a timeout here is still a hard failure, and a useful one -- the
    same per-bar cost runs against years of bars in Phase 3.
    """
    return _run_payload(
        encode_payload(payload),
        limits or SandboxLimits(timeout_seconds=SMOKE_TIMEOUT_SECONDS),
    )
```

5. Pass `outcome=payload.get("outcome")` when constructing the final `SandboxResult`.

- [ ] **Step 7: Add the container smoke test**

Append to `tests/agent_contract/test_sandbox.py`:

```python
@pytest.mark.sandbox
def test_a_smoke_run_returns_fills_from_inside_the_container() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.agent_contract.sandbox import run_smoke_in_sandbox
    from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
    from trading.paper.models import ChargeSchedule
    from trading.runtime.payload import MODE_SMOKE, SmokePayload
    from trading.runtime.provider import BarRecord

    source = '''
from decimal import Decimal
from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


class Buyer(Strategy):
    def configure(self):
        return StrategyManifest(
            name="buyer",
            version="1.0.0",
            universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="TEST")],
            data=DataRequest(bars="1m", history_bars=10),
            capital=Decimal("100000"),
            base_currency="INR",
        )

    def on_bar(self, ctx, bars):
        if not ctx.state.get("done"):
            ctx.state["done"] = True
            ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="smoke entry")
'''

    bars = tuple(
        BarRecord(
            instrument_id=1,
            ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume=Decimal("10"),
        )
        for minute in range(5)
    )
    schedules = (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("20.00"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )

    result = run_smoke_in_sandbox(
        SmokePayload(
            mode=MODE_SMOKE,
            source=source,
            bars={1: bars},
            charge_schedules=schedules,
            starting_cash=Decimal("100000"),
            slippage_bps=Decimal("0"),
        )
    )

    assert result.ok is True, result.error
    assert result.outcome is not None
    assert result.outcome["bar_calls"] == 5
    assert result.outcome["fills"] == 1
    assert Decimal(result.outcome["final_cash"]) == Decimal("99980")
```

- [ ] **Step 8: Rebuild the image and run the full agent_contract suite**

```bash
./sandbox/build.sh
uv run pytest tests/agent_contract tests/runtime -v
```
Expected: all pass, including the fifteen pre-existing isolation tests, which must not have needed any change (Step 6's envelope is built inside `run_strategy_in_sandbox`).

- [ ] **Step 9: Lint, type-check, and commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
git add sandbox src/trading/agent_contract/sandbox.py tests/agent_contract .gitignore
git commit -m "feat(sandbox): run the event loop inside the container

The runner now dispatches on an explicit envelope mode: 'configure'
returns the manifest as before, 'smoke' runs five sessions through the
real event loop and returns the outcome. The envelope is built by the
host inside run_strategy_in_sandbox, so the fifteen existing isolation
tests are unchanged -- what the runner must never do is guess whether it
received source or an envelope.

The image now carries trading.runtime and the pure half of
trading.paper, assembled from the real source tree by sandbox/build.sh
rather than checked in as a copy that could drift. Two drift tests hold
it: one asserts every trading module the runtime imports is COPYed, the
other that no runtime module imports psycopg, docker, or trading.config,
which would work in every in-process test and die only in the sandbox."
```

---

### Task 5: Host orchestration and the verdict

The two-pass sequence, window selection, the determinism check, and the agent-facing report.

**Files:**
- Create: `src/trading/agent_contract/smoke.py`
- Test: `tests/agent_contract/test_smoke.py`

**Interfaces:**
- Consumes: everything above; `trading.agent_contract.validation.{Finding, ValidationReport}`; `trading.agent_contract.schemas.definition`.
- Produces:
  - Finding codes: `SMOKE_CRASH`, `SMOKE_TIMEOUT`, `SMOKE_OOM`, `NO_DATA`, `MANIFEST_UNRESOLVABLE`, `NONDETERMINISTIC`, `NO_ORDERS`, `ALL_ORDERS_REJECTED`, `BREAKER_TRIPPED`. (What a smoke run did *not* exercise — partial fills, ticks, expiry — is reported as a note on the verdict, not as a finding: it is true of every run and is not the strategy's defect.)
  - `SmokeVerdict` frozen dataclass: `passed: bool`, `warnings_only: bool`, `report: ValidationReport`, `window: dict`, `outcome: dict | None`, `runtime: str`, `kernel_isolated: bool`, and `as_agent_feedback() -> str`.
  - `select_window(conn, instrument_ids, sessions=5) -> dict`
  - `fetch_bars(conn, instrument_ids, window) -> dict[int, tuple[BarRecord, ...]]`
  - `resolve_universe(conn, manifest, as_of) -> list[int]`
  - `smoke_test(conn, source, *, limits=None) -> SmokeVerdict`

- [ ] **Step 1: Write the failing verdict tests**

Create `tests/agent_contract/test_smoke.py`:

```python
from trading.agent_contract.smoke import SmokeVerdict, build_verdict


def _outcome(**overrides) -> dict:  # noqa: ANN003
    base = {
        "ok": True,
        "bar_calls": 1875,
        "orders": [],
        "fills": 0,
        "rejections": [],
        "final_cash": "100000",
        "final_equity": "100000",
        "breaker_reason": None,
        "logs": [],
        "error": None,
        "crashed_at": None,
    }
    base.update(overrides)
    return base


_WINDOW = {"start": "2026-08-27T00:00:00+00:00", "end": "2026-09-02T00:00:00+00:00", "sessions": 5}


def test_a_quiet_strategy_passes_with_a_warning() -> None:
    verdict = build_verdict(_outcome(), _outcome(), _WINDOW, "runc", False)
    assert verdict.passed is True
    assert verdict.warnings_only is True
    assert [f.code for f in verdict.report.findings] == ["NO_ORDERS"]
    assert "1,875" in verdict.as_agent_feedback()
    assert "PASSED WITH WARNINGS" in verdict.as_agent_feedback()


def test_a_crash_fails() -> None:
    crashed = _outcome(ok=False, error="ValueError: boom", crashed_at={"handler": "on_bar", "ts": "x"})
    verdict = build_verdict(crashed, crashed, _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "SMOKE_CRASH" in verdict.as_agent_feedback()


def test_differing_order_sequences_fail_as_nondeterministic() -> None:
    first = _outcome(orders=[{"order_id": 1, "submitted_at": "09:31"}])
    second = _outcome(orders=[{"order_id": 1, "submitted_at": "14:22"}])
    verdict = build_verdict(first, second, _WINDOW, "runc", False)
    assert verdict.passed is False
    codes = [f.code for f in verdict.report.findings]
    assert "NONDETERMINISTIC" in codes


def test_identical_order_sequences_do_not_trip_the_determinism_check() -> None:
    orders = [{"order_id": 1, "submitted_at": "09:31"}]
    verdict = build_verdict(_outcome(orders=orders, fills=1), _outcome(orders=orders, fills=1),
                            _WINDOW, "runc", False)
    assert [f.code for f in verdict.report.findings] == []
    assert verdict.passed is True


def test_all_orders_rejected_warns_and_names_the_reason() -> None:
    orders = [{"order_id": 1, "status": "REJECTED"}]
    outcome = _outcome(orders=orders, rejections=["insufficient funds"])
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert verdict.passed is True
    assert "insufficient funds" in verdict.as_agent_feedback()


def test_a_tripped_breaker_warns() -> None:
    outcome = _outcome(orders=[{"order_id": 1}], fills=1,
                       breaker_reason="REASON_MAX_DAILY_LOSS: loss of 5000 exceeds 1000")
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert verdict.passed is True
    assert "BREAKER_TRIPPED" in verdict.as_agent_feedback()


def test_the_report_records_how_well_isolated_the_run_was() -> None:
    # Carried forward from SandboxResult: a stored pass must never be
    # readable as better isolated than it was.
    verdict = build_verdict(_outcome(), _outcome(), _WINDOW, "runc", False)
    assert "host kernel is shared" in verdict.as_agent_feedback()
    gvisor = build_verdict(_outcome(), _outcome(), _WINDOW, "runsc", True)
    assert "runsc" in gvisor.as_agent_feedback()


def test_the_report_states_what_was_not_exercised() -> None:
    # decide_fill fills the full remaining quantity, so PARTIALLY_FILLED
    # never occurs. Saying so beats letting a reader infer coverage.
    verdict = build_verdict(_outcome(fills=1, orders=[{"order_id": 1}]), 
                            _outcome(fills=1, orders=[{"order_id": 1}]), _WINDOW, "runc", False)
    assert "partial fill" in verdict.as_agent_feedback().lower()
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trading.agent_contract.smoke'`

- [ ] **Step 3: Implement the verdict half**

Create `src/trading/agent_contract/smoke.py` with the report logic first (the DB half comes in Step 5):

```python
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

from dataclasses import dataclass
from typing import Any

from trading.agent_contract.validation import Finding, ValidationReport

__all__ = [
    "SmokeVerdict",
    "build_verdict",
]

_FAIL_CODES = frozenset(
    {"SMOKE_CRASH", "SMOKE_TIMEOUT", "SMOKE_OOM", "NO_DATA", "MANIFEST_UNRESOLVABLE",
     "NONDETERMINISTIC"}
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


def _order_key(order: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(order.get(field) for field in sorted(order))


def build_verdict(
    first: dict[str, Any],
    second: dict[str, Any],
    window: dict[str, Any],
    runtime: str,
    kernel_isolated: bool,
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
                    f"      {(first.get('error') or '').strip().splitlines()[-1:] or ['']}"[:600]
                ),
                contract_section="§2",
            )
        )
    else:
        first_orders = [_order_key(o) for o in first.get("orders", [])]
        second_orders = [_order_key(o) for o in second.get("orders", [])]
        if first_orders != second_orders:
            index = next(
                (i for i, (a, b) in enumerate(zip(first_orders, second_orders, strict=False))
                 if a != b),
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
                        f"all {len(orders)} orders were rejected. Most common reason: "
                        f"{reasons[0]}"
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
    )
```

- [ ] **Step 4: Run the verdict tests**

Run: `uv run pytest tests/agent_contract/test_smoke.py -v`
Expected: 8 passed.

- [ ] **Step 5: Prove the determinism check catches a really non-deterministic strategy**

The dict-level tests above prove `build_verdict` compares sequences. This
proves the pair of them catches the thing they exist for, and it is written
so that removing the second run makes it fail. Append to
`tests/agent_contract/test_smoke.py`:

```python
def test_an_actually_nondeterministic_strategy_is_caught_end_to_end() -> None:
    import random
    from dataclasses import asdict
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
    from trading.paper.models import ChargeSchedule
    from trading.runtime.loop import run_loop
    from trading.runtime.provider import BarRecord, InMemoryBars

    class CoinFlipper:
        """Unseeded randomness -- the most common cause of a strategy that
        cannot be backtested, and invisible to static validation."""

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if random.random() < 0.5:  # noqa: S311 - the defect under test
                ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="coin flip")

    bars = InMemoryBars(
        {
            1: [
                BarRecord(
                    instrument_id=1,
                    ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
                    interval_sec=60,
                    open=Decimal("100"),
                    high=Decimal("100"),
                    low=Decimal("100"),
                    close=Decimal("100"),
                    volume=Decimal("10"),
                )
                for minute in range(40)
            ]
        }
    )
    schedules = (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("0"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )

    def _once() -> dict:
        return asdict(
            run_loop(
                strategy=CoinFlipper(),
                bars=bars,
                schedules=schedules,
                starting_cash=Decimal("1000000"),
                slippage_bps=Decimal("0"),
            )
        )

    random.seed()  # explicitly unseeded-equivalent: fresh entropy per run
    verdict = build_verdict(_once(), _once(), _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "NONDETERMINISTIC" in [f.code for f in verdict.report.findings]


def test_a_single_run_compared_with_itself_never_flags() -> None:
    # The vacuity guard: if the pipeline were changed to run once and
    # compare the outcome with itself, the check above would silently
    # stop catching anything. This pins that a self-comparison is clean,
    # so the value of the check lives entirely in there being two runs.
    outcome = _outcome(orders=[{"order_id": 1, "submitted_at": "09:31"}], fills=1)
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert "NONDETERMINISTIC" not in [f.code for f in verdict.report.findings]
```

40 bars of coin flips make a collision between two runs about 1 in 10^12 —
low enough that a flake is far less likely than the CI machine failing, and
the alternative (asserting on a seeded sequence) would test the seed rather
than the check.

- [ ] **Step 6: Write the failing DB-half test**

Append to `tests/agent_contract/test_smoke.py`:

```python
import pytest


@pytest.mark.db
def test_select_window_finds_the_latest_sessions_every_instrument_shares(db_conn) -> None:  # noqa: ANN001
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.agent_contract.smoke import fetch_bars, select_window

    ids = []
    for symbol in ("SMOKEA", "SMOKEB"):
        row = db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
            "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
            "RETURNING instrument_id",
            (symbol, f"NSE:CM:{symbol}"),
        ).fetchone()
        ids.append(row[0])

    # A shares days 1-3, B shares days 2-4. The overlap is days 2-3.
    for instrument_id, days in ((ids[0], (1, 2, 3)), (ids[1], (2, 3, 4))):
        for day in days:
            db_conn.execute(
                "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
                "close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
                (instrument_id, datetime(2026, 7, day, 9, 15, tzinfo=UTC)),
            )

    window = select_window(db_conn, ids, sessions=5)
    assert window["sessions"] == 2
    bars = fetch_bars(db_conn, ids, window)
    assert set(bars) == set(ids)
    assert all(isinstance(bar.close, Decimal) for series in bars.values() for bar in series)
```

- [ ] **Step 7: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke.py -k select_window -v`
Expected: FAIL with `ImportError: cannot import name 'select_window'`

- [ ] **Step 8: Implement the DB half and the orchestrator**

Append to `src/trading/agent_contract/smoke.py`:

```python
_WINDOW_SQL = """
    SELECT ts::date AS session
    FROM bars_intraday
    WHERE instrument_id = ANY(%s) AND interval_sec = 60
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


def select_window(conn: Connection, instrument_ids: Sequence[int], sessions: int = 5) -> dict:
    """The most recent sessions EVERY instrument printed in.

    Intersection rather than union, deliberately: a window where half the
    universe has no bars would hand a strategy a market in which half its
    instruments silently do not exist, and the absent-not-carried-forward
    rule would make that indistinguishable from a quiet day.
    """
    rows = conn.execute(
        _WINDOW_SQL, (list(instrument_ids), len(set(instrument_ids)), sessions)
    ).fetchall()
    days = sorted(row[0] for row in rows)
    if not days:
        return {"start": None, "end": None, "sessions": 0, "instruments": {}}
    start = datetime.combine(days[0], time.min, tzinfo=UTC)
    end = datetime.combine(days[-1], time.max, tzinfo=UTC)
    return {"start": start.isoformat(), "end": end.isoformat(), "sessions": len(days),
            "instruments": {}}


def fetch_bars(
    conn: Connection, instrument_ids: Sequence[int], window: dict
) -> dict[int, tuple[BarRecord, ...]]:
    if window["start"] is None:
        return {}
    rows = conn.execute(
        _BARS_SQL,
        (list(instrument_ids), datetime.fromisoformat(window["start"]),
         datetime.fromisoformat(window["end"])),
    ).fetchall()
    series: dict[int, list[BarRecord]] = {}
    for (instrument_id, ts, interval_sec, open_, high, low, close, volume, trades,
         open_interest, oi_change) in rows:
        series.setdefault(instrument_id, []).append(
            BarRecord(
                instrument_id=instrument_id,
                ts=ts,
                interval_sec=interval_sec,
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
```

The module's complete import block, once every piece below is added — write it in one go rather than accreting it:

```python
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
from trading.paper.charges import load_schedules
from trading.paper.enums import Product
from trading.runtime.payload import MODE_SMOKE, SmokePayload
from trading.runtime.provider import BarRecord
```

Then add `resolve_universe` and `smoke_test`:

```python
def resolve_universe(conn: Connection, manifest: dict, as_of: date) -> list[int]:
    """Instrument ids for a manifest's universe, point-in-time.

    Resolution is against `listed_on`/`delisted_on` at `as_of`, not against
    what exists today -- the same survivorship-bias defence the data layer
    keeps, applied here so a smoke run cannot quietly include an instrument
    that had not listed yet.
    """
    universe = manifest.get("universe")
    if isinstance(universe, list):
        ids = []
        for ref in universe:
            row = conn.execute(
                "SELECT instrument_id FROM instruments WHERE exchange=%s AND segment=%s "
                "AND symbol=%s AND (listed_on IS NULL OR listed_on <= %s) "
                "AND (delisted_on IS NULL OR delisted_on > %s)",
                (ref["exchange"], ref["segment"], ref["symbol"], as_of, as_of),
            ).fetchone()
            if row is not None:
                ids.append(row[0])
        return ids
    clauses, params = ["(listed_on IS NULL OR listed_on <= %s)",
                       "(delisted_on IS NULL OR delisted_on > %s)"], [as_of, as_of]
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


def smoke_test(conn: Connection, source: str, limits: SandboxLimits | None = None) -> SmokeVerdict:
    """The whole of stage 2: configure, fetch, run twice, judge."""
    configured = run_strategy_in_sandbox(source)
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
    instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
    window = select_window(conn, instrument_ids) if instrument_ids else {
        "start": None, "end": None, "sessions": 0, "instruments": {}
    }
    bars = fetch_bars(conn, instrument_ids, window) if instrument_ids else {}
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
                            "1-minute bars. A smoke run needs recorded data for every "
                            "instrument it will feed."
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
                    Finding(code="MANIFEST_UNRESOLVABLE", message=str(mixed),
                            contract_section="§3"),
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
```

`load_schedules(conn, broker, exchange, asset_class, product, on)` is positional — that is its real signature, verified against `src/trading/paper/charges.py:79`. There is no `DEFAULT_BROKER` constant; the seeded `charge_schedules` rows carry exactly three combinations, verified against the live table:

| broker | exchange | asset_class | product |
|---|---|---|---|
| `UPSTOX` | `NSE` | `EQUITY` | `DELIVERY`, `INTRADAY` |
| `BINANCE` | `BINANCE` | `CRYPTO` | `DELIVERY` |

So the key is derived from the universe rather than hardcoded. Add this helper above `smoke_test`:

```python
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
```

- [ ] **Step 9: Run the whole smoke test file**

Run: `uv run pytest tests/agent_contract/test_smoke.py -v`
Expected: 11 passed.

- [ ] **Step 10: Lint, type-check, and commit**

```bash
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
git add src/trading/agent_contract/smoke.py tests/agent_contract/test_smoke.py
git commit -m "feat(agent-contract): the smoke run, stage 2 of the upload pipeline

The two-pass sequence is forced, not chosen: the host cannot build a
payload without the manifest, and the manifest is whatever configure()
returns, which only runs inside the container. So configure mode is step
one, and one upload costs three container runs -- configure, then the
smoke payload twice.

Running it twice is what finally enforces contract §2. Determinism was a
documented rule that nothing checked: static validation catches a literal
datetime.now() and misses set iteration, unseeded random, and dict-hash
dependence. Comparing two order sequences catches all three, and a
divergence is a hard fail because a strategy whose orders are not
reproducible makes every number a backtest would report meaningless.

Warnings pass, and pass loudly. A strategy that never orders is not
obviously broken -- five arbitrary days may not trigger a selective
signal -- but the report leads with the fact that nothing about its order
path was tested. The report also states what the run did NOT prove:
partial fills never occur, ticks and expiry are not routed, and the
window is not permanent."
```

---

### Task 6: Persisting smoke runs

So a pass is a record rather than a transient console message.

**Files:**
- Create: `migrations/versions/0011_strategy_smoke_runs.py`
- Modify: `src/trading/agent_contract/smoke.py` (add `record_smoke_run`)
- Test: `tests/agent_contract/test_smoke_persistence.py`

**Interfaces:**
- Consumes: `SmokeVerdict` (Task 5).
- Produces: `record_smoke_run(conn, strategy_id, verdict) -> int` returning `smoke_run_id`.

- [ ] **Step 1: Write the migration**

Create `migrations/versions/0011_strategy_smoke_runs.py`:

```python
"""Add `strategy_smoke_runs`: the record of §9 stage 2.

One row per smoke run, many rows per strategy version -- deliberately not
one column set on `strategies`. A version is immutable, but the window it
was smoked against is not: D-S4 selects the most recent sessions every
instrument shares, so re-running the same version next week meets
different bars. Collapsing that to a single "smoked: true" flag would
lose the only thing that makes an old pass interpretable.

`runtime` and `kernel_isolated` are carried forward from `SandboxResult`
for the same reason they exist there: a stored pass must never be
readable as better isolated than it was. Dropping them here would
reintroduce exactly the ambiguity the sandbox refuses to leave open.

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_smoke_runs",
        sa.Column("smoke_run_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "strategy_id",
            sa.BigInteger,
            sa.ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("verdict", sa.Text, nullable=False),
        sa.Column("window_start", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("window_end", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("sessions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("instruments", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("bar_calls", sa.Integer, nullable=False, server_default="0"),
        sa.Column("orders_placed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("fills", sa.Integer, nullable=False, server_default="0"),
        sa.Column("rejections", sa.Integer, nullable=False, server_default="0"),
        sa.Column("final_cash", sa.Numeric(18, 4), nullable=True),
        sa.Column("final_equity", sa.Numeric(18, 4), nullable=True),
        sa.Column("breaker_reason", sa.Text, nullable=True),
        sa.Column("findings", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("runtime", sa.Text, nullable=False),
        sa.Column("kernel_isolated", sa.Boolean, nullable=False),
        sa.Column("contract_version", sa.Text, nullable=False),
        sa.Column(
            "ran_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint(
            "verdict IN ('PASSED','PASSED_WITH_WARNINGS','REJECTED')",
            name="ck_smoke_run_verdict",
        ),
    )
    op.create_index(
        "ix_strategy_smoke_runs_strategy",
        "strategy_smoke_runs",
        ["strategy_id", "ran_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_strategy_smoke_runs_strategy", table_name="strategy_smoke_runs")
    op.drop_table("strategy_smoke_runs")
```

- [ ] **Step 2: Apply the migration to both databases**

```bash
uv run alembic upgrade head
```
That migrates the development database. **The test database migrates itself** — `tests/conftest.py:169-197`'s session-scoped `db_url` fixture runs `alembic upgrade head` against `trading_test` with `DATABASE_URL` overridden, so no second command is needed. Verify both:

```bash
uv run python -c "
from trading.config import get_settings
import psycopg
for db in ('trading', 'trading_test'):
    url = get_settings().database_url.rsplit('/', 1)[0] + '/' + db
    with psycopg.connect(url) as c:
        print(db, c.execute(\"SELECT to_regclass('strategy_smoke_runs')\").fetchone()[0])
"
```
Expected: `trading strategy_smoke_runs` and `trading_test strategy_smoke_runs`. If `trading_test` prints `None`, run the test suite once — the fixture migrates on first use.

- [ ] **Step 3: Write the failing persistence test**

Create `tests/agent_contract/test_smoke_persistence.py`:

```python
import pytest

from trading.agent_contract.smoke import SmokeVerdict, record_smoke_run
from trading.agent_contract.validation import Finding, ValidationReport


def _verdict(**overrides) -> SmokeVerdict:  # noqa: ANN003
    base = dict(
        passed=True,
        warnings_only=True,
        report=ValidationReport(findings=(Finding(code="NO_ORDERS", message="0 orders"),)),
        window={"start": "2026-08-27T00:00:00+00:00", "end": "2026-09-02T00:00:00+00:00",
                "sessions": 5, "instruments": {"1401": {"bars": 1875}}},
        outcome={"ok": True, "bar_calls": 1875, "orders": [], "fills": 0, "rejections": [],
                 "final_cash": "100000", "final_equity": "100000", "breaker_reason": None,
                 "logs": [], "error": None, "crashed_at": None},
        runtime="runc",
        kernel_isolated=False,
    )
    base.update(overrides)
    return SmokeVerdict(**base)  # type: ignore[arg-type]


@pytest.mark.db
def test_a_warned_pass_is_stored_with_its_counts(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, _verdict())
    row = db_conn.execute(
        "SELECT verdict, sessions, bar_calls, orders_placed, fills, runtime, kernel_isolated "
        "FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row == ("PASSED_WITH_WARNINGS", 5, 1875, 0, 0, "runc", False)


@pytest.mark.db
def test_a_rejection_is_stored_with_its_findings(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    verdict = _verdict(
        passed=False,
        warnings_only=False,
        report=ValidationReport(findings=(Finding(code="SMOKE_CRASH", message="boom"),)),
        outcome=None,
    )
    smoke_run_id = record_smoke_run(db_conn, registered_strategy_id, verdict)
    row = db_conn.execute(
        "SELECT verdict, findings FROM strategy_smoke_runs WHERE smoke_run_id = %s",
        (smoke_run_id,),
    ).fetchone()
    assert row[0] == "REJECTED"
    assert row[1][0]["code"] == "SMOKE_CRASH"


@pytest.mark.db
def test_a_version_can_be_smoked_more_than_once(db_conn, registered_strategy_id) -> None:  # noqa: ANN001
    # D-S4: the window moves, so a second run against the same immutable
    # version is a new fact, not an overwrite of the old one.
    record_smoke_run(db_conn, registered_strategy_id, _verdict())
    record_smoke_run(db_conn, registered_strategy_id, _verdict())
    count = db_conn.execute(
        "SELECT COUNT(*) FROM strategy_smoke_runs WHERE strategy_id = %s",
        (registered_strategy_id,),
    ).fetchone()[0]
    assert count == 2
```

Add the `registered_strategy_id` fixture to `tests/agent_contract/conftest.py` (create the file if absent), registering a trivial valid strategy via `trading.agent_contract.registry.register_strategy` and returning its `strategy_id`. Copy the strategy source and user setup from `tests/agent_contract/test_registry.py` so the two files agree.

- [ ] **Step 4: Run it and confirm it fails**

Run: `uv run pytest tests/agent_contract/test_smoke_persistence.py -v`
Expected: FAIL with `ImportError: cannot import name 'record_smoke_run'`

- [ ] **Step 5: Implement `record_smoke_run`**

Append to `src/trading/agent_contract/smoke.py`:

```python
_INSERT_SMOKE_RUN = """
    INSERT INTO strategy_smoke_runs (
        strategy_id, verdict, window_start, window_end, sessions, instruments,
        bar_calls, orders_placed, fills, rejections, final_cash, final_equity,
        breaker_reason, findings, runtime, kernel_isolated, contract_version
    ) VALUES (
        %(strategy_id)s, %(verdict)s, %(window_start)s, %(window_end)s, %(sessions)s,
        %(instruments)s, %(bar_calls)s, %(orders_placed)s, %(fills)s, %(rejections)s,
        %(final_cash)s, %(final_equity)s, %(breaker_reason)s, %(findings)s, %(runtime)s,
        %(kernel_isolated)s, %(contract_version)s
    ) RETURNING smoke_run_id
"""


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
    row = conn.execute(
        _INSERT_SMOKE_RUN,
        {
            "strategy_id": strategy_id,
            "verdict": label,
            "window_start": verdict.window.get("start"),
            "window_end": verdict.window.get("end"),
            "sessions": verdict.window.get("sessions", 0),
            "instruments": json.dumps(verdict.window.get("instruments", {})),
            "bar_calls": outcome.get("bar_calls", 0),
            "orders_placed": len(outcome.get("orders", [])),
            "fills": outcome.get("fills", 0),
            "rejections": len(outcome.get("rejections", [])),
            "final_cash": outcome.get("final_cash"),
            "final_equity": outcome.get("final_equity"),
            "breaker_reason": outcome.get("breaker_reason"),
            "findings": json.dumps(
                [
                    {"code": f.code, "message": f.message, "line": f.line,
                     "contract_section": f.contract_section}
                    for f in verdict.report.findings
                ]
            ),
            "runtime": verdict.runtime,
            "kernel_isolated": verdict.kernel_isolated,
            "contract_version": CONTRACT_VERSION,
        },
    ).fetchone()
    return int(row[0])
```

Add `import json` and `from trading.agent_contract.registry import CONTRACT_VERSION` to the module's imports.

- [ ] **Step 6: Run the persistence tests**

Run: `uv run pytest tests/agent_contract/test_smoke_persistence.py -v`
Expected: 3 passed.

- [ ] **Step 7: Run everything**

```bash
./sandbox/build.sh
uv run pytest
uv run ruff check src tests && uv run ruff format --check src tests && uv run mypy src
```
Expected: the full suite green, with the new tests added to the 862 baseline. Record the new count for `docs/STATUS.md`.

- [ ] **Step 8: Update STATUS.md and the contract**

In `docs/agent-contract/STRATEGY_CONTRACT.md`, change §9 item 2 from "*Waiting on the sandbox.*" to "**Implemented** (`trading.agent_contract.smoke`)", list the new finding codes alongside the stage 1 codes, and state in §9 what a smoke run does not exercise (partial fills, ticks, expiry) and that a pass is against a stated window.

In `docs/STATUS.md`, replace the "Next in Phase 2" paragraph with what now remains: **D4, the worked examples**, which stage 2 has unblocked because they can finally be executed before publication.

- [ ] **Step 9: Commit**

```bash
git add migrations/versions/0011_strategy_smoke_runs.py src/trading/agent_contract/smoke.py \
        tests/agent_contract docs/STATUS.md docs/agent-contract/STRATEGY_CONTRACT.md
git commit -m "feat(agent-contract): persist smoke runs (migration 0011)

One row per run, many per version -- not a flag on strategies. A version
is immutable but the window it was smoked against is not: D-S4 picks the
most recent sessions every instrument shares, so the same version
re-smoked next week meets different bars. A single 'smoked: true' column
would lose the only thing that makes an old pass interpretable.

runtime and kernel_isolated are carried forward from SandboxResult for
the reason they exist there: a stored pass must never be readable as
better isolated than it was.

§9 stage 2 is now implemented, which unblocks D4 -- the worked examples
can finally be executed before they are published, which is the whole
reason they were withheld."
```

---

## What this plan does not build

Named so they read as scope rather than oversight, matching the spec's §7:

- **`ctx.intel`** — Phase 2.5. The stub keeps raising.
- **Tick routing** — `data.ticks=True` is accepted and ignored; the report says so.
- **`on_expiry`** — needs the expiry calendar wired to the loop.
- **Multi-currency** — D6 fixes one strategy to one portfolio to one currency for V1.
- **Partial fills** — `decide_fill` fills the full remaining quantity, so `PARTIALLY_FILLED` cannot occur. Reported, not hidden.
- **The D4 worked examples** — unblocked by this plan, written after it.
