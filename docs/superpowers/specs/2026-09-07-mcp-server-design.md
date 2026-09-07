# MCP server — agent-controlled trading over the existing platform

**Status:** design approved 2026-09-07. Implementation plan to follow.

## Why

Today the platform is deterministic by construction. An external agent
writes a strategy as a Python file; the platform validates it, sandboxes
it, and replays it. Every decision the running system makes was fixed
when that file was written.

The goal is to move the decision-making out of the file and into an
agent that runs continuously: one that reads market state, weighs its own
news and research, recalls what backtesting taught it, and places orders
itself across equities, crypto spot and perpetuals.

Nothing about the trading core needs to change for that. The invariants,
the charge model, the fill rules and the breaker are all already enforced
at the HTTP boundary. What is missing is a way for an agent to reach
them. This spec adds that surface and nothing else.

## Decisions taken

| Question | Decision |
|---|---|
| How does the agent backtest? | It authors a strategy script and calls the existing engine; it can also pull raw history for its own analysis. Backtesting is an assessment tool, not the live path. |
| What wakes the agent live? | Nothing on our side. The agent's own harness drives; MCP is a pure pull surface. No new runtime machinery. |
| Guardrails | Token auth and portfolio scoping only. Everything else explicitly deferred (see below). |
| Transport | Both stdio and streamable HTTP over one shared tool layer. |
| Indicators | A new shared `src/trading/indicators/`, usable by both MCP and strategy scripts. |
| Intelligence layer | The agent's own. No news ingestion is in scope. |

## Scope

**Tradeable, and therefore in scope:** `EQUITY` (NSE via Upstox),
`CRYPTO` (Binance spot), `PERP` (Binance USDT-margined perpetuals).

**Not tradeable, and therefore not in scope:** `FUTURE`, `OPTION`,
`INDEX`, `MF`, `COMMODITY`. They exist in `AssetClass` but have no entry
in `BROKER_BY_ASSET_CLASS` (`paper/charges.py:35-38`), so an order in one
would be refused by `MissingChargeSchedule`. The options-chain recorder
records chains; nothing can trade them. The instrument tools are shaped
so that adding an asset class later is a data change, not a tool change.

**Deferred by explicit decision.** Recorded here so they stay visible
rather than being forgotten:

- Order attribution (`agent_session_id` on `orders`). Without it, agent
  orders arrive with `live_run_id = NULL` and are indistinguishable in
  the blotter from manually placed ones. Cheap to add now; impossible to
  reconstruct after the fact. Worth revisiting before real money.
- Per-order notional caps, per-day order caps, cooldowns.
- Kill switch and dry-run mode.
- `start_live_run` through MCP — deploying a deterministic strategy to
  the supervisor is a different product from an agent trading directly.
- News and announcements ingestion.

## Architecture

```
   external agent
        |
        +-- stdio ----------> serve_stdio.py --+
        +-- HTTPS + Bearer -> serve_http.py ---+   token -> portfolio scope
                                               |
                                               v
                                           tools.py        one definition of every tool
                                               |  async httpx
                                               v
                                  gateway (FastAPI :8000)   ALL invariants live here
                                               |
                                    TimescaleDB  +  Redis
```

Two new packages. No behavioural change to anything that exists.

| Package | Module | Responsibility |
|---|---|---|
| `src/trading/mcp/` | `client.py` | Async httpx wrapper over the gateway; one method per upstream route |
| | `session.py` | Token to `AgentSession(portfolio_id)`; the only source of portfolio scope. Tokens are read from configuration (env-backed, one operator, no migration), each mapping one opaque token to one `portfolio_id`. No token table is added; issuing a token means adding a config entry and restarting the server. |
| | `tools.py` | Every tool definition, transport-agnostic |
| | `formatting.py` | Decimal to string, freshness envelope, refusal shaping |
| | `serve_stdio.py` | stdio entrypoint; scope from config |
| | `serve_http.py` | Streamable HTTP entrypoint; scope from bearer token |
| `src/trading/indicators/` | `trend.py` | `sma`, `ema`, `macd`, `adx` |
| | `momentum.py` | `rsi` (Wilder), `roc`, `stoch` |
| | `volatility.py` | `atr`, `bollinger`, `realised_vol` |
| | `levels.py` | `pct_from_high`, `pct_from_low`, `range_position` |

### Three rules the implementation must not break

1. **The MCP layer never opens a database connection.** Every read and
   every write goes through the HTTP routes. This keeps
   `_require_market_open`, `_require_sufficient_cash`,
   `_require_perp_order_is_tradable`, the charge model and the breaker
   enforced in exactly one place. A second enforcement path is a fork,
   and an agent exploring the surface will eventually find the fork.

   It also keeps blocking psycopg off the MCP event loop. The gateway's
   routes are plain `def` precisely because `async def` plus blocking
   psycopg deadlocked it once already; MCP's streamable-HTTP handlers are
   async and receive no threadpool treatment, so an in-process mount that
   touched the driver would reproduce that defect with an autonomous
   caller driving it.

2. **Money crosses the wire as strings.** The same reason
   `BacktestResponse` already does it: JSON has no decimal type, and an
   agent sizing a position from a float-mangled balance is a defect that
   stays invisible until it costs money.

3. **`portfolio_id` is never a tool parameter.** It is derived from the
   session token. A confused or misled agent cannot name a book, so it
   cannot trade the wrong one.

### Why indicators live outside `mcp/`

So that a strategy script and the live agent compute RSI with the same
code. If the two diverged, every lesson the agent carried from a
backtest into a live decision would be measuring a subtly different
thing, and the discrepancy would be invisible in both places.

## Tool surface

Fifteen tools, ordered as a session actually unfolds.

### Orientation

| Tool | Backed by |
|---|---|
| `get_capabilities()` — tradeable asset classes, order types, products, TIF, margin modes, indicator catalogue, charge brokers | static + `paper/charges.py` |
| `list_instruments(asset_class?, query?)` — id, symbol, exchange, segment, lot and tick size | `GET /instruments` |
| `get_strategy_contract()` — the contract bundle, so the agent can write a valid strategy | `GET /strategies/contract` |

### Market data

| Tool | Backed by |
|---|---|
| `get_market_snapshot(instrument_ids[], interval, indicators[], history)` — last price, recent bars, computed indicators, freshness, per instrument | `GET /candles` + `indicators/` |
| `get_candles(instrument_id, interval, limit, start?, end?)` — raw OHLCV for the agent's own analysis. `interval` accepts exactly the vocabulary the existing `/candles` route resolves; the MCP layer adds none of its own and rejects anything else with the route's own message | `GET /candles/{id}` |
| `get_data_freshness(instrument_ids?)` — last bar timestamp, market-open state, explicit `stale` flag | `GET /candles` metadata |
| `get_perp_context(instrument_id)` — funding rate, next funding time, mark, step size, min notional | **new gateway route required** |

### Portfolio and execution

| Tool | Backed by |
|---|---|
| `get_portfolio_state()` — cash, equity, positions, open orders, breaker status | `GET /portfolios`, `/positions`, `/orders` |
| `get_perp_positions()` — signed size, entry, leverage, margin, liquidation price | `GET /portfolios/{id}/perp-positions` |
| `place_order(instrument_id, side, order_type, quantity, product, limit_price?, tif?, leverage?, rationale, idempotency_key?)` | `POST /orders` |
| `cancel_order(order_id)` | `DELETE /orders/{id}` |
| `list_orders(status?, limit?)` | `GET /orders` |

### Backtesting

| Tool | Backed by |
|---|---|
| `submit_strategy(name, python_code)` — validate and sandbox smoke-run, returning findings | `POST /strategies` |
| `run_backtest(strategy_id, start, end, starting_cash?, max_daily_loss?, max_drawdown_pct?)` — metrics, equity curve, itemised fills | `POST /strategies/{id}/backtests` |
| `get_backtest(backtest_run_id)` — re-read a stored run | `GET /backtests/{id}` |

### The one piece of new backend work

`get_perp_context` has no route behind it. The data exists — 56,734
funding rows and per-contract filters are seeded — but nothing serves it
over HTTP. Without it an agent will size a DOGE perpetual in fractions
and eat a rejection it cannot diagnose: DOGE steps by a whole coin, BTC
by 0.001, and minimum notionals run 5 / 20 / 50.

## Data flow

### Live decision cycle

The agent's harness drives. A representative pass:

1. `get_data_freshness()` and `get_portfolio_state()`.
2. `get_market_snapshot([...], interval, indicators, history)`. The MCP
   layer fetches history plus warmup bars per instrument, computes
   indicators in `Decimal`, and attaches the freshness envelope.
3. The agent reasons, using its own news and its own recollection of
   what backtesting taught it.
4. `place_order(..., rationale=...)`. The MCP layer injects
   `portfolio_id` from the token and POSTs; the gateway runs the full
   invariant chain and returns an order or a refusal.

### Backtest assessment cycle

1. `get_strategy_contract()`.
2. `submit_strategy(name, code)`. Validation findings and the smoke
   verdict come back; a rejection tells the agent what to fix.
3. `run_backtest(strategy_id, start, end)`. Blocks for seconds and
   returns metrics, equity curve and itemised fills.
4. The agent iterates, or carries the lesson into live decisions.

### Indicator warmup

A 14-period Wilder RSI computed from 15 bars is not the number a
14-period Wilder RSI computed with 100 bars of warmup produces; the
smoothing converges slowly. Fetching only `history + period` bars would
hand the agent plausible values that disagree with what its own
backtested strategy computed over the same window.

`get_market_snapshot` therefore fetches `max(5 * period, 50)` bars of
warmup beyond the requested history, taking the largest requirement
across the indicators asked for. It reports `warmup_bars_used` and
`warmup_sufficient` on every response; when the database cannot supply
the full warmup, `warmup_sufficient` is `false` and the indicator is
returned with that flag rather than silently computed from too little
history.

### Order idempotency under retry

`idempotency_key` becomes optional. When omitted, the MCP layer derives
`sha256(session | instrument | side | quantity | type | limit |
minute-bucket)`. An agent that retries the same decision within a minute
— which agents do — dedupes into one order rather than two.

This is a correctness default on a field the API already requires, not a
rate cap by another name.

## Error handling

Three classes, kept distinct because an agent responds to each
differently.

| Class | Examples | Surfaced as | Retried |
|---|---|---|---|
| Refusal | market closed, insufficient cash, `MissingChargeSchedule`, perp step-size violation | tool content, `status: "REFUSED"`, gateway `detail` verbatim, plus have-versus-need figures where cheap | never |
| Infrastructure | gateway down, 5xx, timeout | `isError: true` | reads once; writes never |
| Session and auth | bad token, unknown portfolio | `isError: true`, immediately | never |

A refusal is information, not an exception. The gateway's messages are
already worded well enough to act on, so they pass through untouched
rather than being flattened into "order failed".

**A timeout on `POST /orders` is ambiguous** — the order may or may not
exist. The MCP layer never re-POSTs. It re-queries `GET /orders` by
idempotency key to establish what actually happened and reports the true
state. This follows the same instinct as the `recorder-auth` change: a
refused credential stops the run rather than being retried.

Staleness is not an error. It rides as a warning field on every
market-data response so the agent can see it and decide.

## Testing

Test-driven throughout.

- **`indicators/`** — fixture tests against hand-verified series, plus
  Hypothesis properties (already a dependency): the SMA of a constant
  series is that constant; EMA stays within `[min, max]`; RSI lies in
  `[0, 100]`; ATR is non-negative; the Bollinger mid-band equals the SMA.
- **Warmup** — a test proving truncated and fully-warmed RSI differ, and
  that the tool reports the difference rather than hiding it.
- **MCP tools** — exercised over an httpx ASGI transport against the real
  gateway app, with `dependency_overrides` on a rolled-back transaction,
  following the established pattern. Tools are tested against the actual
  invariant chain rather than a mock of it; this is the test that catches
  any future attempt to bypass the API.
- **Scoping** — a caller-supplied `portfolio_id` is ignored, and two
  tokens cannot see each other's book.
- **Money** — every money field in every tool response is a string.
- **Refusal surfacing** — a market-closed refusal reaches the agent as
  readable content, not a stack trace.
- **Idempotency reconciliation** — a timed-out placement resolves by
  read-back and never double-places.

## Dependency

One addition: the `mcp` Python SDK.

## Known risks carried into implementation

**The database is stale.** `docs/STATUS.md` records that EOD bhavcopy
last wrote on 2026-08-21 and nothing schedules it. An agent that
backtests on stale data and then trades the lesson live is exactly the
case where silent staleness does damage. `get_data_freshness` and the
per-response freshness envelope exist to make this impossible to
overlook, but they report the problem rather than fixing it. Scheduling
ingestion remains separate work.

**Agent orders are unattributed.** Deferred by decision; noted above.
