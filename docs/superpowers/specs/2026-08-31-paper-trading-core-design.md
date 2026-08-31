# Design: paper trading core — orders, Indian cost model, portfolio ledger

**Date:** 2026-08-31 · **Status:** approved, ready for implementation planning
**Parent:** implementation-plan.md §10 Phase 1 ("Streaming + manual paper trading"), §4.3, §6, §8

## What this is

The second sub-project of Phase 1, following the crypto-streaming and
charts/watchlist-UI sub-projects. Phase 1's data half is now shipped:
Binance and Upstox WebSocket ingestion, the stream gateway, the bar
aggregator, the intraday backfill, and the watchlist/chart UI. This
sub-project starts the trading half.

It turns the platform from a market-data viewer into something you can
actually paper-trade: submit an order against a live instrument, have it
fill realistically, and see the position, cash, and net-of-charges P&L
that result — with the Indian cost model applied at the fidelity §4.3
demands, because Phase 3's backtest engine will call the exact same
calculator.

## Success criterion

With the stack running during an NSE session, you can create a portfolio
with virtual capital, submit a market buy on RELIANCE with a written
rationale, watch it fill within a tick or two at a price plus adverse
slippage, and see cash decrease by exactly the amount an Upstox contract
note would show for that trade — brokerage, STT, transaction charges,
SEBI fee, stamp duty, IPFT, and GST itemised, not lumped. A subsequent
sell produces the DP charge that a delivery sell attracts and an intraday
sell does not.

Independently: a portfolio breaching its declared max daily loss pauses
itself, cancels its resting orders, and notifies you on Telegram without
that notification ever sitting in the fill path.

## Explicitly out of scope (deferred, not forgotten)

- **F&O.** Lot sizes, SPAN-approximate margin, expiry/assignment, STT on
  premium. Needs derivatives ingestion, which does not exist yet.
- **US equities**, LRS notes, forex spread simulation.
- **Short selling, leverage, margin calls.** This slice is long-only and
  cash-settled. §6's margin realism arrives with F&O, where it belongs.
- **Multi-currency portfolios and RBI reference-rate FX marks.** See the
  currency ruling below.
- **Post-trade journal analytics, weekly digests, revenge-trade
  detection** (§8). The rationale *field* ships here; the analytics that
  read it are their own sub-project.
- **Post-tax P&L lens** (§8) — Phase 3, hooks the backtest reporter.
- **Replay service** (§4.4) — next sub-project; it needs this ledger to
  replay into.
- **Strategy/agent-submitted orders** — Phase 2. The order API is
  designed so a strategy runtime can call it later, but nothing here
  assumes one.

## Decisions taken during design

1. **Scope:** NSE equity (delivery + intraday) and crypto spot. Both are
   already streaming live.
2. **Fill clock:** tick-driven live, bar-driven backtest, over one engine
   consuming an abstract price-event stream — §6's "one engine, two clock
   speeds", §12 Q4's event-driven choice.
3. **§8 behavioural layer:** the full Phase 1 set — mandatory rationale,
   portfolio circuit breaker, Telegram alerts.
4. **`user_id`:** on `portfolios` only, per §12 Q1's intent that
   multi-tenancy be *representable* without a later migration. `orders`
   and `fills` reach it through `portfolio_id`; a portfolio cannot change
   owner, so denormalising the column onto them would add an update
   anomaly for no gain. `watchlists` remains the deliberate exception its
   own sub-project carved out. The `users` table exists but is empty, so
   migration `0007` seeds the single local user portfolios reference.
5. **Broker profile:** Upstox, matching the live feed — ₹20/order
   delivery brokerage, ₹20-or-0.1%-whichever-lower intraday, DP ₹20 per
   scrip per day on delivery sells, GST base = brokerage + transaction +
   demat + IPFT.
6. **Currency:** a portfolio is single-currency — INR for NSE equity, or
   USDT for crypto, never mixed. No FX conversion anywhere in this slice.
   §4.3's multi-currency marks arrive with their own sub-project.

## Architecture

```
   ┌──────────┐   POST /orders    ┌─────────────┐
   │  web UI  │──────────────────►│   gateway   │
   └──────────┘                   │ paper_api.py│
        ▲                         └──────┬──────┘
        │ fills:{portfolio_id}           │ writes order (PENDING)
        │                                │ publishes orders:control
        │                                ▼
   ┌────┴──────────────────────────────────────────┐
   │                   Redis                        │
   │   ticks:*        orders:control    fills:*     │
   └────┬───────────────────┬───────────────────────┘
        │ ticks             │ control
        ▼                   ▼
   ┌────────────────────────────────┐    ┌──────────────────┐
   │        paper_engine            │    │  alert_worker    │
   │  open orders in memory         │    │  drains outbox   │
   │  fill eval · breaker (5s)      │    │  → Telegram      │
   └───────────────┬────────────────┘    └────────▲─────────┘
                   │ one transaction:              │ polls
                   │ fill+ledger+position+cash     │
                   ▼                               │
            ┌──────────────────────────────────────┴───┐
            │            TimescaleDB / Postgres         │
            └───────────────────────────────────────────┘
```

`paper_engine` is a standalone process alongside `crypto_ingestor`,
`bar_aggregator`, and `upstox_ingestor` — the pattern this project already
uses for every long-running consumer. It is deliberately not inside the
gateway: trading correctness must not share a process with the web server,
and commit `5d03a2e` is a fresh reminder of what blocking work in that
process costs.

If `paper_engine` is down, orders remain `PENDING` and do not fill. Not
filling is always safer than filling wrongly.

## Components

### Data model (migration `0007`)

Following existing conventions: `numeric(18,4)` for money, `timestamptz`
with `now()` defaults, `text` for enums, bigserial identifiers.

- **`portfolios`** — `portfolio_id`, `user_id` FK, `name`,
  `base_currency`, `initial_capital`, `cash_balance`, `status`
  (`ACTIVE`/`PAUSED`), `max_daily_loss`, `max_drawdown_pct`.
  Unique `(user_id, name)`. §4.3's multiple concurrent portfolios.
- **`orders`** — `portfolio_id`, `instrument_id`, `side`, `order_type`
  (`MARKET`/`LIMIT`), `quantity` **numeric** (crypto is fractional),
  `limit_price`, `product` (`DELIVERY`/`INTRADAY`), `time_in_force`,
  `status`, **`rationale` NOT NULL**, `rejection_reason`,
  `idempotency_key` unique.
- **`fills`** — `order_id`, `quantity`, `price`, `filled_at`, and
  **`tick_ts`**: the timestamp of the price event that caused the fill.
  Charges stored **per component** — `brokerage`, `stt`, `exchange_txn`,
  `sebi_fee`, `stamp_duty`, `ipft`, `gst`, `dp_charges` — never only a
  total. §8's cost-drag report needs the breakdown and it cannot be
  reconstructed from a lump sum later.
- **`ledger_entries`** — every cash movement, signed, with `entry_type`,
  originating fill reference, and `balance_after`.
- **`positions`** — `(portfolio_id, instrument_id)`, `quantity`,
  `avg_cost`, `realised_pnl`.
- **`portfolio_equity_snapshots`** — `ts`, `equity`, `peak_equity`,
  `drawdown_pct`. Read by the circuit breaker; reused by Phase 3 metrics.
- **`circuit_breaker_events`**, **`alert_deliveries`**.
- **`charge_schedules`** — see the cost model below.

**Source of truth vs cache.** `fills` and `ledger_entries` are
authoritative. `cash_balance` and `positions` are caches maintained in the
same transaction as the fill. Deriving them on every read would be purer
but puts a full fill replay on the circuit breaker's hot path. The cache
is only acceptable because it cannot drift silently: replaying all fills
must reproduce it exactly, enforced as a property test and available as a
reconciliation command.

### Order lifecycle

```
                    ┌──────────► REJECTED      (validation failed)
                    │
  submit ──► PENDING ──► OPEN ──┬──► PARTIALLY_FILLED ──► FILLED
                    │           │
                    │           ├──► CANCELLED         (user)
                    │           └──► EXPIRED           (DAY, session close)
```

`PENDING` means *accepted and durable, not yet acknowledged by the
engine*. It exists because the API and engine are separate processes;
without it, a crash between HTTP 200 and engine pickup leaves an order the
user believes in and nothing owns.

Validation at submit, before the row is written: market open per
`trading_calendar` for the instrument's exchange (crypto is 24/7),
instrument tradable, quantity positive, sufficient cash for a buy,
sufficient position for a sell (long-only), rationale non-empty, and a
`charge_schedules` row covering this instrument/product/date.

### The Indian cost model

**Rates are data, not constants.** `charge_schedules` carries
`exchange, segment, asset_class, product, charge_type, basis,
applies_to_side, rate, cap, rounding, effective_from, effective_to,
source_note`.

This is required, not defensive: NSE cash transaction charges were
0.00297% from 2024-10-01 and were revised to 0.00307% effective
**2026-03-01**. The backfill spans 2022-01 → 2026-08 and crosses that
boundary, so a constant computes the wrong charge for most of the
historical period — quietly, by paise per trade.

`source_note` carries a citation per row. When a number is questioned in
a year's time, the provenance must be in the row.

Verified NSE cash rates to seed, checked 2026-08-31 against
[Upstox brokerage charges](https://upstox.com/brokerage-charges/),
[Zerodha charges](https://zerodha.com/charges/),
[NSE SEBI turnover fees](https://www.nseindia.com/regulations/member-compliance-sebi-turnover-fees),
and the [NSE transaction-charge circular](https://nsearchives.nseindia.com/content/circulars/FA64232.pdf).
Each seeded row carries its own `source_note`:

| Charge | Delivery | Intraday | Side |
|---|---|---|---|
| STT | 0.1% | 0.025% | delivery both; intraday sell only |
| NSE txn | 0.00307% (from 2026-03-01; 0.00297% before) | same | both |
| SEBI fee | ₹10/crore | same | both |
| Stamp duty | 0.015% | 0.003% | buy only |
| IPFT | ₹0.01/crore | same | both |
| DP charges | ₹20/scrip/day (Upstox) | none | sell only |
| Brokerage | ₹20/order (Upstox) | ₹20 or 0.1%, lower | both |
| GST | 18% on brokerage + txn + demat + IPFT | 18% on brokerage + txn + IPFT | — |

Two subtleties that are the classic silent errors:

- **The GST base is a named set of charge types, not a multiplier on a
  total.** It excludes STT and stamp duty, and it differs between brokers
  — Zerodha quotes DP inclusive of GST (₹13 × 1.18 = ₹15.34) while Upstox
  lists demat charges inside its GST base. Each broker profile is its own
  set of rows.
- **Rounding is per charge type**: STT to the nearest rupee, most others
  to two decimals, brokerage caps applied after the percentage. Rounding
  at the wrong step drifts by paise per trade — invisible per trade,
  material over a backtest.

Crypto: taker/maker fee as a percentage of turnover, plus §4.3's optional
1% TDS drag as a separate labelled charge type. The 30% crypto tax with no
loss offset is a *tax lens* concern (§8, Phase 3), not a per-fill charge —
including it here would double-count it later.

The calculator is a pure function, `compute_charges(fill, schedules) ->
ChargeBreakdown`, with no DB access inside. Schedules are loaded and
passed in, so the backtest engine can call it a million times without
touching Postgres.

### Fill engine

Subscribes to `ticks:*` and `orders:control`. Open orders held in memory
keyed by `instrument_id` — at 107 ticks/sec a dict lookup rather than a
query is the difference between working and not. On startup, loads
`OPEN`/`PARTIALLY_FILLED` orders and promotes `PENDING` ones, so a restart
resumes rather than resets.

- **Market:** fills on the next tick at that price, slippage applied
  adversely (buys higher, sells lower). Never favourable.
- **Limit buy:** triggers when a tick trades at or below the limit,
  **fills at the limit price, not the tick price.**
- **Limit sell:** mirror image.

Filling at the limit rather than the better tick price is deliberate
conservatism. An engine that assumes price improvement manufactures free
money on every limit order, and Phase 3 would inherit the flattery.

Slippage model: fixed basis points by default, volume-participation
available per §4.3. Partial fills arise only from the participation model.

**Crash safety.** The commit point is a single transaction writing fill,
ledger entry, position, cash balance, and order status together;
publication to Redis follows. Dying before the transaction means no fill
happened; dying after it means the UI misses a live event a refresh
recovers. A double fill is impossible because status advances inside the
same transaction.

**Sessions.** Equity follows `trading_calendar`; crypto is 24/7. `DAY`
orders sweep to `EXPIRED` at session close — which is also where §6's
"orders can go unfilled" becomes real.

### Circuit breaker

Equity is cash plus mark-to-market positions at last tick, so it moves
continuously; evaluating per tick would mean ~107 evaluations a second for
a threshold that moves in minutes. It evaluates **on a 5-second timer and
immediately after every fill**. Worst-case detection lag is 5 seconds —
stated rather than pretended away.

On breach of `max_daily_loss` or `max_drawdown_pct`: portfolio →
`PAUSED`, open orders cancelled, `circuit_breaker_events` row written,
alert enqueued.

### Telegram alerts — transactional outbox

The engine never calls the Telegram API. It writes an `alert_deliveries`
row inside the same transaction as the event; a separate `alert_worker`
drains that table with retry and backoff.

A direct call would place a third-party HTTP request inside the fill path,
so a Telegram outage or slow response becomes latency or failure in the
thing recording trades. With the outbox, the worst case is a late
notification, never a stalled or corrupted ledger — and there is a durable
record of what was sent and what failed.

Events: fills, circuit-breaker trips, order rejections.

### API

New `paper_api.py` router mounted on the existing gateway: portfolio CRUD,
order submit and cancel, positions, P&L reads.

**Every route is a plain `def`, never `async def`** — psycopg is
synchronous, and `5d03a2e` documents exactly what the alternative costs.
Money serialises as JSON **numbers**, not strings (the Task 7b lesson),
and is `Decimal` end to end internally — never float, anywhere.

## Testing

1. **Golden tests against real Upstox contract notes.** Given quantity,
   price, and product, the calculator reproduces every line to the paisa.
   Nothing else validates an Indian charge stack honestly. If no executed
   trades are available, Upstox's published charges calculator is the
   fallback oracle — weaker, and labelled as such.
2. **Date-boundary test.** A delivery fill on 2026-02-28 computes NSE
   transaction charges at 0.00297%; the identical fill on 2026-03-01 uses
   0.00307%. A real revision on a real date inside the backfill window.
3. **Ledger replay invariant** (property-based, generated fill
   sequences): replaying all fills reproduces `cash_balance` and every
   `positions` row exactly.
4. **Anti-lookahead invariants.** An order submitted at T never fills on a
   tick with `tick_ts < T`, asserted against the provenance column. And
   over the same window the bar-driven path never produces a fill better
   than the tick-driven path — if coarser data yields a better price,
   lookahead is leaking in.
5. **No negative cash**, as a property over arbitrary valid order
   sequences.
6. **Circuit breaker:** a synthetic equity curve breaching threshold
   pauses the portfolio, cancels open orders, writes the event, enqueues
   the alert.
7. **Outbox:** a Telegram failure does not roll back a fill; retry drains
   the backlog.
8. **Engine resilience:** a single malformed tick is logged and dropped,
   never kills the loop — the containment pattern from `5373207`.

Two error-handling rules carry spec-level weight:

- **A missing charge schedule is a hard rejection, never a silent zero.**
  Computing what is available and treating the missing charge as ₹0 yields
  a P&L that looks fine and is systematically optimistic. This is the most
  dangerous failure mode in the subsystem and it fails loudly.
- **No silent fallbacks anywhere in the fill or charge path.** If
  something cannot be computed correctly, the order is rejected or the
  tick is dropped with a log — never approximated.

Tests use the dedicated Redis instance on port 6380 and the existing guard
fixture; no test may touch the live stack.

## Open questions for the implementation plan (not this design)

- Whether the seeded local user comes from an env var, a constant, or a
  small CLI — a plan-level detail.
- Exact slippage default in basis points, per asset class. Needs a number;
  the mechanism is settled.
- Whether `alert_worker` is its own process or a thread inside
  `paper_engine`. Outbox semantics hold either way.
- Telegram bot token and chat id configuration, and whether alerts are
  per-portfolio or global.
- Whether the UI work (order ticket, positions view, portfolio switcher)
  belongs in this sub-project's plan or a following one.
