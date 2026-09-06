# Strategy Contract

**Version 0.1 — the runtime described here is live.**

> Read this before generating anything. A strategy written against this
> document can be uploaded today: it is statically validated, run twice in a
> gVisor-isolated container against real bars, registered on a pass,
> backtested over multi-year windows, and run forward against live prices
> into a simulated portfolio. All of that is built and in use.
>
> Two interfaces in here are **not** implemented, and are marked where they
> appear: `ctx.intel` (§4, Phase 2.5) and `on_tick` (§2). Everything else
> described below runs.
>
> What the platform has data for is a different question from what this
> document permits, and it is the most common cause of a rejection. §3 has
> the table; read it before writing a universe.
>
> Remaining interface questions are listed under
> [Open decisions](#open-decisions) with their reasoning rather than papered
> over. The version stays 0.1 because none of the interface has changed —
> what changed is that it is now real.

---

## 1. What this is

This platform is a **simulation-only** trading sandbox for Indian retail
markets. It does not route real orders to any broker or exchange. Nothing here
is investment advice, and simulated results do not represent actual trading.

You are reading the specification an external AI agent needs in order to write
a strategy that runs here. Hand this file to any capable model — Claude,
ChatGPT, Gemini, a local model — with no other context, and it should produce
conforming code. If it cannot, that is a defect in this document.

The bundle is three files:

| File | Purpose |
|---|---|
| `STRATEGY_CONTRACT.md` | This file. The full specification. |
| `schema.json` | JSON Schemas for the manifest, instruments, bars, ticks, and orders. Its enumerations are **generated from the platform's own enums and pinned by a test**, so a value it accepts is a value the order API accepts. Source: `src/trading/agent_contract/schema.json`. |
| `platform_sdk.py` | Typed no-op stubs of every interface below, so generated code can be lint-checked and dry-run locally before upload. Every stub **raises** rather than returning a plausible value — see §11. Source: `src/trading/agent_contract/platform_sdk.py`. |

---

## 2. The strategy interface

A strategy is a single Python class that **subclasses `Strategy`**, imported
from `platform_sdk`. Name it whatever you like *except* `Strategy` -- that name
belongs to the base class, and the runner skips it when looking for yours.
Every method except `configure` and `initialize` is optional; implement only
the events you need.

```python
from platform_sdk import (
    Bar,
    Context,
    ExpiryEvent,
    InstrumentId,
    OrderUpdate,
    Strategy,
    StrategyManifest,
    Tick,
)


class MyStrategy(Strategy):          # your own name, subclassing Strategy
    def configure(self) -> StrategyManifest:
        """Declare universe, data needs, capital, schedule, and parameters.
        Called once, before anything else, outside the simulation clock.
        Must be a pure function of nothing: no I/O, no randomness, no clock."""

    def initialize(self, ctx: Context) -> None:
        """Called once at the start of the run, after the manifest is
        accepted. Set up indicators and state here."""

    def on_bar(self, ctx: Context, bars: dict[InstrumentId, Bar]) -> None:
        """Called once per completed bar interval, with every subscribed
        instrument that produced a bar in that interval.

        An instrument that did not trade is ABSENT from the dict -- never
        present with the previous close carried forward. Carrying forward
        would be friendlier and would invent a trade that did not happen,
        letting a strategy act on liquidity that was not there. Use
        `ctx.data.last()` when you want the last known price regardless."""

    def on_tick(self, ctx: Context, tick: Tick) -> None:
        """NOT ROUTED YET. Declaring `ticks=True` does not make this fire;
        the runtime dispatches bars only. Defining it is harmless and it
        will never be called, so a strategy that depends on it does
        nothing. Use on_bar."""

    def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
        """Called on EVERY state change -- including each partial fill, not
        only on reaching a terminal state. A GTC limit can rest partially
        filled indefinitely, and a strategy sizing its next order from
        `update.order.filled_quantity` needs to see that as it happens.

        A rejection arrives here too, and is a normal outcome rather than
        an exception. Your strategy must survive one."""

    def on_expiry(self, ctx: Context, event: ExpiryEvent) -> None:  # NOT ROUTED YET
        """F&O only. Called at settlement for a position in an expiring
        contract."""
```

`InstrumentId` is an `int`.

### Determinism

The same strategy, over the same data, must produce the same orders. This is
what makes a backtest comparable to a forward paper run, and it is enforced,
not merely requested:

- **Never read the wall clock.** `datetime.now()`, `time.time()`, and
  `date.today()` are unavailable. The only time is `ctx.now`.
- **Never use unseeded randomness.** `random` and `numpy.random` are seeded
  per-run from the run id and reset before `initialize`.
- **Never depend on iteration order of sets.** Dict order is insertion-ordered
  and safe; `set` iteration order is not.
- **Never reach the network or filesystem.** Neither is available (§8). All
  data comes through `ctx`.

---

## 3. The manifest

`configure()` returns a `StrategyManifest`:

```python
StrategyManifest(
    name="sma-crossover",
    version="1.0.0",
    universe=[...],              # see below
    data=DataRequest(
        bars="1m",               # "1m" | "5m" | "15m" | "1h" | "1d"
        ticks=False,             # True routes on_tick
        history_bars=200,        # bars of warm-up before the first on_bar
    ),
    capital=Decimal("1000000"),
    base_currency="INR",         # one strategy -> one portfolio -> ONE currency
    params={
        "fast": Param(int, default=10, bounds=(2, 100)),
        "slow": Param(int, default=30, bounds=(5, 400)),
    },
    max_daily_loss=Decimal("20000"),      # optional; arms the circuit breaker
    max_drawdown_pct=Decimal("10"),       # optional; arms the circuit breaker
)
```

### Universe

Either an explicit list or a query. Both are accepted, because they answer
different needs:

```python
# explicit -- what you write for a handful of named instruments
universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")]

# query -- resolved point-in-time
universe=Query(asset_class="EQUITY", exchange="NSE", index="NIFTY50")
```

**A universe is always resolved point-in-time**, whichever spelling you use:
against `listed_on`/`delisted_on` at `ctx.now`. A backtest over 2023 sees the
instruments that existed in 2023, including ones since delisted.

That is the survivorship-bias guarantee, and it is why the query form exists at
all. Hardcoding today's NIFTY 50 constituents into a 2022 backtest silently
tests a portfolio of companies selected *for having survived to 2026* — one of
the classic ways a retail backtest lies to its author. A query cannot make that
mistake.

### What the platform actually has bars for

A manifest naming an instrument with no bars is rejected after three
containers have already run, so check this table before you write the
universe. It is the state of the database, not an aspiration.

| Asset class | Daily bars | 1-minute bars | Can be traded |
|---|---|---|---|
| NSE equity | ✅ ~8.4M rows, 2016→ | ✅ 5 symbols only (RELIANCE, TCS, INFY, HDFCBANK, ICICIBANK) | ✅ |
| Crypto spot (Binance) | ❌ **none** | ✅ 26 USDT pairs | ✅ |
| Crypto perpetual | ✅ 2019→ | ❌ | ❌ no cost model yet |
| NSE options, futures | ✅ EOD | ❌ | ❌ no cost model yet |
| Mutual funds | ✅ NAVs | ❌ | ❌ |

Three consequences worth stating plainly, because each one has cost
somebody a rejection:

- **A crypto strategy cannot be backtested.** Backtests run on daily bars
  (see §9), and crypto spot has none. Crypto runs forward, live, and smoke-
  tests on 1-minute bars — it just has no history in the shape a backtest
  reads.
- **A `bars="1m"` equity strategy is limited to those five symbols.** The
  daily universe is the whole bhavcopy — thousands of instruments — but the
  1-minute universe is only what this platform has recorded.
- **Options, futures and perpetuals have data but cannot be ordered.** They
  have no charge schedule, and an order in an asset class with no cost
  model is refused at submission rather than filled at a cost of zero.

### Perpetual futures (leverage and shorting)

The one place this platform lets a strategy hold a **negative** position.
Everything else here is long-only by construction: a spot sell can only
reduce something already held, and the database refuses a negative
quantity. A perpetual inverts that -- the sell *is* the position.

```python
StrategyManifest(
    name="funding-carry",
    version="1.0.0",
    universe=[InstrumentRef(exchange="BINANCE_FUTURES", segment="PERP", symbol="BTC-USDT")],
    data=DataRequest(bars="1m", history_bars=20),
    capital=Decimal("100000"),
    base_currency="USDT",
    leverage=Decimal("5"),        # REQUIRED for a perpetual, forbidden otherwise
)
```

`leverage` is declared once for the strategy, not per order and not per
instrument: one strategy holds one portfolio in one currency and one asset
class (D6). Omit it and every perpetual order is refused -- the platform
will not assume one, because leverage decides how much margin the position
locks up.

Four things a perpetual does that nothing else here does:

- **A sell with no position opens a short.** `ctx.order(..., side="SELL")`
  is how you go short; there is no separate call and no "short" flag.
- **Opening costs no cash.** Margin is *reserved*, not spent. Cash moves
  on realised P&L, fees, and funding. Your equity reflects
  `quantity x (mark - entry)`, signed -- a short gains as the mark falls.
- **Funding settles every eight hours**, at 00:00, 08:00 and 16:00 UTC.
  When the rate is positive longs pay shorts; when it is negative the flow
  reverses. It is charged on notional, not on margin, so leverage
  multiplies it. BTC-USDT's mean rate across 2019-2026 is 0.0001059 per
  settlement -- roughly 11.6% a year that a long pays. A strategy that
  holds a levered long through a bull market pays this three times a day.
- **A position can be liquidated.** If what is left of the margin falls
  below the exchange's maintenance requirement, the position is closed at
  the mark, a 1.25% fee is charged, and the run continues. At 20x,
  bankruptcy is 5% from entry and liquidation about 0.4% inside that.
  Liquidation is **not** a circuit-breaker halt: the breaker pauses your
  portfolio, a liquidation closes one position.

**What is not yet wired.** `ctx.portfolio` does not report perpetual
positions -- the runtime's own position model is still spot-only, so a
strategy must track its own exposure in `ctx.state`. Perpetuals also
cannot be backtested yet, for the same reason and because crypto has no
daily bars (§3). Both land together. Until then a perpetual strategy is a
forward-running one.

### Circuit breaker

If `max_daily_loss` or `max_drawdown_pct` is declared and breached, the
platform **pauses the strategy's portfolio and cancels its resting orders**,
then notifies. This is not advisory. It is also runaway-loop protection: a
strategy that malfunctions cannot dig indefinitely.

Both are evaluated against equity (cash plus positions marked to last traded
price), quantized to 4 decimal places, and the comparison is strict — a loss
landing *exactly* on `max_daily_loss` does not breach.

**A backtest may override both**, per run, without touching your code. What
the manifest declares is the strategy's own stated risk appetite; what a run
passes is the operator asking "what would this have done under a different
limit". A run that halted early says so in its report, and names the limit
that stopped it — a strategy whose equity curve simply stops in 2021 has
usually tripped a breaker, not run out of data.

---

## 4. The Context API

`ctx` is the only way a strategy touches the outside world.

### `ctx.now -> datetime`

The simulation clock, timezone-aware UTC. In a backtest this is the timestamp
of the event being processed; in a forward paper run it tracks real time. It is
the only clock available.

### `ctx.data` — point-in-time history

```python
ctx.data.bars(instrument_id, interval="1m", count=200) -> list[Bar]
ctx.data.last(instrument_id) -> Bar | None
```

**This API physically cannot return data later than `ctx.now`.** Lookahead bias
is prevented by construction rather than by discipline: there is no argument
you can pass that reaches into the future. Bars are returned oldest-first, and
a bar's timestamp marks the **start** of its interval while its values are only
knowable at the interval's **end** — so the most recent bar `ctx.data.bars`
returns is always one that has closed.

Fewer than `count` bars may be returned (a newly listed instrument, a gap in
recording). Never assume the list is full.

### `ctx.portfolio` — cash and positions

```python
ctx.portfolio.cash          -> Decimal
ctx.portfolio.positions     -> dict[InstrumentId, Position]
ctx.portfolio.equity        -> Decimal   # cash + positions at last mark
```

A position whose `quantity` is `0` may still be present (it has realised P&L
history). Check `quantity != 0` for "held".

### `ctx.order(...)` and `ctx.cancel(...)`

```python
ctx.order(
    instrument_id,
    side="BUY" | "SELL",
    quantity=Decimal("10"),
    order_type="MARKET" | "LIMIT",
    limit_price=Decimal("1300.00"),      # required for LIMIT, forbidden for MARKET
    product="DELIVERY" | "INTRADAY",
    time_in_force="DAY" | "GTC",
    rationale="fast crossed above slow",  # REQUIRED, non-empty
) -> OrderId

ctx.cancel(order_id) -> None
```

`rationale` is mandatory and must be non-empty. Every order on this platform
carries a reason, so that a trade log can be read back and confronted with what
actually happened. Write something a human would find useful six months later,
not `"buy"`.

Orders are validated at submission and can be **rejected outright** — see §6.
A rejection is a normal outcome, not an exception: your strategy learns about
it through `on_order_update`, and must survive it.

### `ctx.log(...)`

```python
ctx.log("crossover", fast=fast_ma, slow=slow_ma, position=qty)
```

Structured logging. Retained with the run and shown in its report. Not stdout —
`print()` is captured but discouraged.

### `ctx.state`

A persisted key-value store surviving restarts within a run. Values must be
JSON-serializable.

```python
ctx.state["entry_price"] = str(price)   # Decimals as strings; see §5
```

### `ctx.intel` — market intelligence

**Not available.** Ships in Phase 2.5. It will expose news, sentiment, and flow
features as point-in-time per-instrument numbers (`ctx.intel.features(...)`).
Do not write strategies against it yet.

---

## 5. The data model

### Money and precision — read this before writing arithmetic

**All money is `decimal.Decimal`. Never `float`.** This is not stylistic. A
platform whose headline feature is an honest cost model cannot afford binary
floating point in its money path, and the runtime rejects a strategy that
submits a float quantity or price.

| Value | Type | Scale |
|---|---|---|
| Prices (`open`/`high`/`low`/`close`, `limit_price`, `avg_cost`) | `Decimal` | 4 dp |
| Quantities | `Decimal` | 8 dp (crypto needs it; equities are whole numbers) |
| Charges, cash, equity | `Decimal` | 4 dp internally, 2 dp for charges |

Timestamps are **timezone-aware UTC** `datetime`. A naive datetime is an error,
not a convenience. Indian market sessions are Asia/Kolkata; convert for
display, never for storage or comparison.

### Instrument

The canonical identity of every tradable thing. `instrument_id` is the key
every other structure references.

| Field | Type | Notes |
|---|---|---|
| `instrument_id` | int | Primary key |
| `asset_class` | str | `EQUITY`, `FUTURE`, `OPTION`, `CRYPTO`, `MF` |
| `exchange` | str | `NSE`, `BSE`, `BINANCE`, `AMFI` |
| `segment` | str | `CM` (cash), `FO` (F&O), `SPOT`, `MF` |
| `symbol` | str | `RELIANCE`, `BTC-USDT` |
| `currency` | str | `INR`, `USDT`. **Load-bearing** — see §6 |
| `underlying_id` | int \| None | F&O: the instrument this derives from |
| `expiry` | date \| None | F&O |
| `strike` | Decimal \| None | Options |
| `option_type` | str \| None | `CE`, `PE` |
| `tick_size` | Decimal \| None | Minimum price increment |
| `isin`, `name`, `series` | str \| None | Reference data |
| `listed_on`, `delisted_on` | date \| None | Point-in-time universe resolution |
| `status` | str | `ACTIVE`, and others |

**Lot size is not a field on the instrument.** Lot sizes are revised over time,
so they are dated. Read it through `ctx.data.lot_size(instrument_id)`, which
resolves the size in force at `ctx.now` and returns `None` where the instrument
has no lot concept (equity cash, crypto).

A method on `ctx.data` rather than a field on the instrument record,
deliberately: a static field invites caching a 2026 lot size into a 2022
backtest and sizing every F&O order wrong.

### Bar

| Field | Type | Notes |
|---|---|---|
| `instrument_id` | int | |
| `ts` | datetime | UTC. Marks the **start** of the interval — except for `1d`, see below |
| `interval_sec` | int | 60, 300, 900, 3600, 86400 |
| `open`/`high`/`low`/`close` | Decimal | |
| `volume` | Decimal \| None | |
| `trades` | int \| None | Trade count in the interval |
| `open_interest`, `oi_change` | int \| None | F&O |
| `delivery_qty`, `delivery_pct` | — | Equities, EOD only |

**Exception — `1d` bars carry the session close, not the interval start.**

For every intraday interval (`1m`, `5m`, `15m`, `1h`) `ts` is the start of the
interval, and the bar becomes knowable one interval later. A daily bar does not
work that way. It records a whole trading session, and an NSE session is 6h15m
of market time sitting inside a 24-hour calendar day — so there is no
interval-start timestamp that adding one day would turn into the close. The
platform therefore stamps a `1d` bar's `ts` with the **session close**, because
that is the instant the bar became knowable.

What this means when you write a daily strategy:

- During `on_bar`, `ctx.now` **equals** that bar's `ts` — 15:30 IST on the
  session the bar describes. It is not the next day, and not the session open.
- `ts` is still the right thing to key on, compare, and log. The change is what
  it means, not how you use it.
- Day-of-week, month-end and holiday logic all read correctly from `ts`
  directly. You do not need to subtract anything.

### Tick

| Field | Type | Notes |
|---|---|---|
| `instrument_id` | int | |
| `ts` | datetime | UTC, always timezone-aware |
| `price` | Decimal | Strictly positive |
| `quantity` | Decimal | May be **zero** — an index tick has no traded size |
| `side` | str \| None | Aggressor side where the feed provides it |

### Order

| Field | Type | Notes |
|---|---|---|
| `order_id` | int | |
| `instrument_id` | int | |
| `side` | str | `BUY`, `SELL` |
| `order_type` | str | `MARKET`, `LIMIT` |
| `quantity`, `filled_quantity` | Decimal | |
| `limit_price` | Decimal \| None | |
| `product` | str | `DELIVERY`, `INTRADAY` |
| `time_in_force` | str | `DAY`, `GTC` |
| `status` | str | See below |
| `rationale` | str | Non-empty, yours |
| `rejection_reason` | str \| None | Populated when `status == REJECTED` |

**Statuses:** `PENDING` (accepted, not yet working), `OPEN` (resting),
`PARTIALLY_FILLED`, `FILLED`, `CANCELLED`, `REJECTED`, `EXPIRED`.
Terminal: `FILLED`, `CANCELLED`, `REJECTED`, `EXPIRED`.

A `DAY` order that has not filled by session close becomes `EXPIRED`.

### OrderUpdate

What `on_order_update` receives. **It is a wrapper, not an order** — the two
fields below are all it has, and everything about the order itself is reached
through `.order`.

| Field | Type | Notes |
|---|---|---|
| `order` | Order | The order in its **new** state — read `status`, `filled_quantity`, `rejection_reason` from here |
| `previous_status` | str | The status it held before this update, so you can tell what changed |

```python
def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
    if update.order.status == "REJECTED":
        ctx.log("rejected", reason=update.order.rejection_reason)
    filled = update.order.filled_quantity      # NOT update.filled_quantity
```

Reading `update.status` or `update.filled_quantity` directly is the most common
way to crash a strategy in this handler: those fields exist, one level down, on
`update.order`.

### Position

`instrument_id`, `quantity`, `avg_cost`, `realised_pnl` — all `Decimal`.

---

## 6. Order rules and rejections

Orders are checked at submission. Each of these produces a rejection whose
message names exactly what is wrong.

**Currency must match.** A portfolio holds **one** currency and performs no FX
conversion. An INR portfolio cannot buy a USDT-denominated instrument. This
rejects rather than converting, because a silent conversion at an invented rate
would misstate P&L by roughly the exchange rate.

**The market must be open**, per the exchange trading calendar for that
instrument's exchange and segment. `CRYPTO` is exempt — Binance is 24/7. An
unknown calendar state is treated as *closed*, never optimistically as open.

**Cash or position must suffice.** A buy needs cash for notional plus charges;
a sell needs the position.

**A charge schedule must cover the instrument, product, and date.** If no rule
covers this fill, the order is rejected rather than priced at zero. A silently
zero charge produces a P&L that looks correct and is systematically optimistic,
which is the most dangerous failure mode in a simulator.

**Fills happen on subsequent price events, never on the submitting bar.** A
market order fills at the next available price plus slippage, moved against you
(5 bps default). A limit order rests until the price crosses it and then fills
**at the limit price**, never at the price that crossed it — so a bar-driven
backtest can never obtain a better price than a tick-driven forward run would
have. Your strategy must tolerate an order that never fills.

---

## 7. The cost model

Every fill is charged the real Indian statutory costs, itemised. **Rates are
dated data, not constants** — NSE cash transaction charges changed on
2026-03-01, and a backtest spanning that date uses the correct rate on each
side of it.

Components: `brokerage`, `stt`, `exchange_txn`, `sebi_fee`, `stamp_duty`,
`ipft`, `gst`, `dp_charges`, `tds`.

Worked example, RELIANCE 100 @ ₹1,313.10 (turnover ₹131,310):

| | Delivery BUY | Delivery SELL | Intraday BUY | Intraday SELL |
|---|---|---|---|---|
| Brokerage | 20.00 | 20.00 | 20.00 | 20.00 |
| STT | 131.00 | 131.00 | — | 33.00 |
| Exchange txn | 4.03 | 4.03 | 4.03 | 4.03 |
| SEBI fee | 0.13 | 0.13 | 0.13 | 0.13 |
| Stamp duty | 19.70 | — | 3.94 | — |
| GST | 4.33 | 7.93 | 4.33 | 4.33 |
| DP charges | — | 20.00 | — | — |
| **Total** | **179.19** | **183.09** | **32.43** | **61.49** |

The asymmetries are real and worth internalising: delivery STT applies to
**both** sides, stamp duty is **buy-side only**, DP charges are **sell-side
only** and only on delivery — and only **once per scrip per day**, so a second
same-day delivery sell of the same scrip pays no DP (and correspondingly less
GST, since GST's base includes it).

Crypto on Binance is a flat 0.1% taker fee. The 1% VDA TDS is plumbed through
but not currently switched on.

**Write cost-aware strategies.** A strategy trading 100 shares of a ₹1,300
stock intraday pays ~₹94 round-trip against ₹131,310 of turnover — about 7 bps.
An edge thinner than that is not an edge.

---

## 8. Sandbox limits

Strategy code runs isolated: **no network, read-only filesystem**, hard CPU,
memory, and wall-clock limits, non-root.

**Settled and enforced.** Strategy code runs in a container with:

| Limit | Default |
|---|---|
| Memory | 256 MB, no swap |
| CPU | 1.0 core |
| Processes | 64 (a fork bomb stays the container's problem) |
| Wall clock | 30 s, enforced by the host |
| Filesystem | read-only, with one 16 MB `tmpfs` at `/tmp` (`noexec`, `nosuid`) |
| Network | **none** — no interface exists, not merely a blocked port |
| Capabilities | all dropped, plus `no-new-privileges` |
| User | non-root, uid 10001 |

Your source is piped in over stdin; no host path is mounted, and there is no
file on disk for a strategy to rewrite between validation and execution. `/tmp`
is the only writable surface and it dies with the container.

The allowlist is in §8's companion — `numpy` and `pandas` are installed and
importable; the full permitted set is enforced by static validation before a
strategy ever reaches the sandbox.

**About the runtime.** The plan specifies gVisor (`runsc`), which interposes a
user-space kernel so an escape must first get through *it*. Where a host
provides `runsc` the sandbox uses it. Where it does not, the confinement above
still holds, but **the host kernel is shared**, so a kernel-level exploit that
gVisor would absorb is not contained.

An earlier revision of this section claimed gVisor was unavailable on the
development machine because Docker Desktop ships `runc` only. That was wrong:
the machine runs Colima, whose VM is ordinary Ubuntu, and `runsc` installs
there normally. gVisor is verified working on arm64 in a dedicated Colima
profile, at a measured cost of roughly 0.2s per run.

Every run records which runtime confined it, rather than leaving it to be
assumed. That distinction is load-bearing before Phase 4, when code from
strangers runs here; for single-user V1, where the code is your own agent's,
hardened `runc` is proportionate.

Do not write code that reads files, opens sockets, spawns processes, or imports
anything not on the allowlist — it will fail, and the failure will be reported
against your strategy.

---

## 9. Upload, validation, and the feedback loop

1. **Static validation** — manifest schema check, import allowlist, AST scan.
   **Implemented** (`trading.agent_contract.validation`).
2. **Smoke run** — five simulated days in a throwaway sandbox. Must not crash
   and must parse orders correctly. **Implemented**
   (`trading.agent_contract.smoke`).

   **Your strategy is run twice, and the two order sequences are compared.**
   §2 makes determinism a rule; this is what enforces it. Static validation
   catches a literal `datetime.now()` and misses iterating a set, `random`
   without a seed, and depending on dict insertion order — all of which two
   runs catch immediately. Differing sequences are a rejection, not a warning:
   a strategy whose orders are not reproducible makes every number a backtest
   would report meaningless.

   **What a smoke run does not exercise**, stated so a pass is not read as more
   than it is:

   - **Partial fills.** The fill model fills an order's full remaining
     quantity, so `PARTIALLY_FILLED` never occurs and your handling of it is
     untested.
   - **Ticks, `on_expiry`, and `ctx.intel`.** Not routed by a smoke run.
   - **The window is not permanent.** A run is against the most recent sessions
     every instrument in your universe shares. Re-submitting next week meets
     different bars, so a pass is a statement about a stated window rather than
     about the strategy for all time. The window is stored with the run.

   A run that places no orders **passes with a warning**, loudly. Five
   arbitrary days may not trigger a selective signal, so failing it would
   refuse legitimate strategies — but the report leads with the fact that
   nothing about your order path was tested.
3. **Registration** — versioned and stored, ready to backtest or forward-run.
   **Implemented** (`trading.agent_contract.registry`).

   **A registered version is immutable.** Re-uploading `demo 1.0.0` with
   different source is refused, not applied. Every backtest report and forward
   run refers to a `(name, version)`, and if that could be overwritten those
   results would silently describe code that no longer exists. Re-uploading
   *identical* source is idempotent — that is a retry, not a change. Publish a
   change as a new version.

Every rejection returns a report **written to be pasted straight back into the
agent that generated the code** — every finding at once, never one per round
trip:

```
REJECTED: static validation found 3 problems.

  [IMPORT_NOT_ALLOWED] line 2: imports 'requests', which is not on the allowlist.
      A strategy reaches data only through `ctx`; there is no network and no filesystem.
      See STRATEGY_CONTRACT.md §8.
  [WALL_CLOCK] line 7: calls datetime.now(), which reads the real clock. Use ctx.now
      -- a strategy that reads wall-clock time cannot be replayed, so its backtest
      would prove nothing.
      See STRATEGY_CONTRACT.md §2.
  [MISSING_CONFIGURE] file: the Strategy class does not implement `configure()`.
      See STRATEGY_CONTRACT.md §3.

Fix these and resubmit. All findings are listed above, not only the first.
```

Finding codes are stable, so an agent can branch on them.

Stage 1, static validation: `SYNTAX_ERROR`, `IMPORT_NOT_ALLOWED`,
`FORBIDDEN_CALL`, `FORBIDDEN_ATTRIBUTE`, `WALL_CLOCK`, `NO_STRATEGY_CLASS`,
`MISSING_CONFIGURE`, `MANIFEST_INVALID`.

Stage 2, the smoke run: `SMOKE_CRASH`, `SMOKE_TIMEOUT`, `SMOKE_OOM`,
`NO_DATA`, `MANIFEST_UNRESOLVABLE`, `NONDETERMINISTIC`, `NO_ORDERS`,
`ALL_ORDERS_REJECTED`, `BREAKER_TRIPPED`. The first six are rejections; the
last three pass with warnings.

Fix, resubmit, repeat. Closing that loop is the point.

### 9.1 Backtesting a registered strategy

Registration is not the end of the pipeline. A registered `(name, version)`
can be backtested over a window you choose, and that is where the numbers
worth reading come from.

**Backtests run on daily bars.** A strategy declaring `data.bars="1m"` is
refused with `BACKTEST_INTERVAL_UNSUPPORTED` rather than quietly served
daily bars it did not ask for: a multi-year intraday run is millions of
bars, and the sandbox receives a run's data as one payload. This is the
most common surprise in the pipeline, so it is worth internalising early —
**if you want the strategy backtested, declare `bars="1d"`.** A `1m`
strategy is still smoke-tested and can still run forward against live
prices; it simply has no backtest path today.

Backtest findings, all stable and branchable:

| Code | Means |
|---|---|
| `BACKTEST_INTERVAL_UNSUPPORTED` | the manifest declares an interval backtests cannot serve |
| `BACKTEST_WINDOW_UNCOVERED` | the requested window runs past the data, or before it |
| `BACKTEST_TOO_LARGE` | the window would exceed the payload the sandbox accepts |
| `BACKTEST_RUN_FAILED` | the container crashed; the traceback is in the report |

A completed run stores, and its report shows: the equity curve, per-period
returns, CAGR, volatility, Sharpe, Sortino, Calmar, drawdown depth **and
duration**, VaR, monthly returns, rolling Sharpe, win rate, profit factor,
expectancy, a cost-drag report (gross versus net of every Indian charge),
a 2× cost-and-slippage stress rerun, a Monte Carlo reshuffle of trade
order, and **a per-fill ledger with every charge itemised and the rationale
your strategy gave**. That last one is why §4 makes `rationale` mandatory:
it is read back months later, next to what the trade actually did.

### 9.2 Running forward against live prices

A registered strategy can also be run **forward**, in the same sandbox
image, against live closed bars, placing simulated orders into a portfolio
you pick. Backtest and forward run share one dispatcher, which is what
makes the two comparable.

Four things differ from a backtest, and a strategy that ignores them
behaves worse live than its backtest suggested:

- **Dispatch is 1-minute closed bars, whatever `data.bars` declares.** A
  `1d` strategy runs forward, but it will see a bar a minute rather than a
  bar a day. If your logic counts bars to mean days, it is wrong live.
- **One live run per portfolio.** A second start against a busy portfolio
  is refused by the database, not by a check that could race.
- **The portfolio is single-currency.** An order for a USDT instrument from
  an INR portfolio is refused — there is no FX conversion anywhere.
- **Refusals are normal and are counted.** The gateway checks a strategy's
  order exactly as it checks a human's: currency, market hours, sufficient
  cash or position, and a charge schedule for the asset class. The run
  continues; the refusal and its reason are recorded on the run. A strategy
  refused on every bar sits at zero fills, which looks identical to one
  that decided to sit still unless you read the reason.

**A forward run does not survive a supervisor restart with its memory
intact.** The row keeps running and a fresh container is launched, so
whatever your strategy held in `self` is gone and its next bar looks like
its first. Keep durable state in `ctx.state`, which is persisted, not in
instance attributes.

### Static validation is not the sandbox

Worth stating plainly, because a checker that greps for `eval` and `socket`
invites being mistaken for a security control. It is not one. An AST scan is
bypassable by anyone actually trying — a name assembled at run time, a payload
decoded from a string. **Containment is the sandbox's job** (§8), and none of
it depends on this stage.

What this stage does is catch the mistakes generated code actually makes, fast
and locally, and explain them well enough to fix. A passing report means no
honest mistakes were found — not that the code has been proven safe.

---

## 10. Worked examples

**UNDECIDED (D4).** §5 of the implementation plan calls for four: SMA crossover
on equity, an iron condor on NIFTY weeklies, BTC momentum, and a multi-asset
rebalancer.

They are deliberately **not** written yet. An example in a contract is a
promise that the code runs, and writing four plausible-looking examples would
mean shipping four untested claims in the most load-bearing part of the
document — an agent copying a broken example produces broken strategies with
full confidence.

**The blocker has cleared.** The runtime exists, strategies have been
registered, smoke-tested, backtested over multi-year windows and run forward
against live prices. Two of the four are now writable and executable today: an
SMA crossover on a daily-bar NSE equity, and a multi-asset rebalancer within
one asset class. The other two are still blocked, and on data rather than on
runtime: an iron condor needs an options cost model, and BTC momentum needs
daily crypto bars, which this platform does not have (see §3).

Writing and running the two that are unblocked is the next task on this
document.

---

## 11. Using the SDK stub offline

Before uploading, import your strategy against `platform_sdk.py` and run your
type checker and linter over it. That catches a misspelled method, a call that
does not exist, or the wrong argument type without a round trip.

**Every runtime call in the stub raises `NotOnThisPlatform`.** Seeing that
exception means your code reached a real call with the right shape — it is the
expected outcome of a local dry run, not a defect.

Nothing returns a plausible value on purpose. A stub that handed back an empty
bar list and a zero cash balance would let a broken strategy "run" locally,
produce no orders, and look fine. An import error is a far better outcome than
a green run that proves nothing.

Three rules are checked eagerly, because they are the ones generated code
breaks most often and each is cheaper to find here than as a rejection after
upload:

- a blank `rationale` raises `ValueError`
- a `float` quantity or price raises `TypeError` (money is `Decimal` end to end)
- reading `ctx.now` raises rather than falling back to the wall clock

---

## Open decisions

These need resolving before v1.0. Each changes what a generated strategy looks
like, so each is worth settling deliberately.

| # | Decision | State |
|---|---|---|
| **D1** | Universe: explicit list, query, or both? | **Settled — both.** An explicit `InstrumentRef` list for a handful of named instruments; a `Query` when the universe is dynamic. Resolution is point-in-time either way, which is where the survivorship guarantee lives. The query form exists because hardcoding today's index constituents into a 2022 backtest silently selects for survival. |
| **D2** | How dated lot size reaches `ctx`. | **Settled.** `ctx.data.lot_size(instrument_id)`, resolved at `ctx.now`, `None` where there is no lot concept. A method on `ctx.data`, not a field on the instrument, so a 2026 lot size cannot be cached into a 2022 backtest. |
| **D3** | Sandbox limits and the import allowlist. | **Settled** — see §8 for the enforced table. The runtime is configurable and every result records which one confined it. A first attempt concluded gVisor was unavailable here; that was an error about the local Docker backend (Colima, not Docker Desktop), corrected 2026-09-03 — `runsc` is verified working on arm64 in a dedicated Colima VM. gVisor becomes a requirement before Phase 4, not before V1, and is now available rather than hypothetical. |
| **D4** | Worked examples. | **Open; the runtime blocker has cleared.** Strategies are now registered, backtested and run forward, so an example can be executed before publication as this decision requires. Two of the four are writable today; the iron condor is blocked on an options cost model and BTC momentum on daily crypto bars (§3). |
| **D5** | Does `on_bar` fire for an instrument that did not trade? | **Settled — absent from the dict.** Carrying the previous close forward invents a trade that did not happen and lets a strategy act on liquidity that was not there. `ctx.data.last()` covers the "last known price" need. |
| **D6** | Can one strategy hold more than one portfolio? | **Settled — no.** One strategy, one portfolio, one currency. This matches the platform: the currency gate is enforced at order submission and there is no FX mark model. **Consequence, stated plainly:** a single strategy cannot trade NSE equities and crypto together in V1. Revisit when multi-currency portfolios exist. |
| **D7** | How partial fills surface. | **Settled.** `on_order_update` fires on every state change, each partial included. A GTC limit can rest partially filled indefinitely; only firing at terminal state would make that invisible to a strategy sizing from `filled_quantity`. |

## Verification standard

Per §10 of the implementation plan, this contract is **not done** until three
different frontier agents, each given only this file, each produce a working
strategy on the first try. Until then it is a draft, whatever its version
number says.
