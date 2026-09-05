# Phase 3.5 — Crypto perpetuals: the derivative core, first instance

**Status:** design, approved in principle 2026-09-05 · **Author:** Satyam + Claude

`implementation-plan.md` has no perpetuals in it. §3's asset-class table
covers spot crypto, and §4.3's simulation engine is written for instruments
you buy and later sell. This is therefore an addition to the plan, not an
unbuilt part of it, and it is worth being explicit about why it earns a
phase.

---

## 1. What the platform does today, precisely

Long-only spot. Three independent layers enforce it, which is why this
cannot be a configuration flag:

- `positions` carries `CHECK (quantity >= 0)`, named
  `ck_no_negative_position`. A short position cannot be *represented*.
- `_require_sufficient_position` (`paper/api.py:278`) rejects any SELL
  beyond what is held.
- All 26 crypto instruments are `segment = 'SPOT'`, fed from
  `wss://stream.binance.com:9443/stream` — Binance spot, not `fstream`.

`_apply_position`'s own docstring says it: *"Long-only in this slice."* An
acknowledged simplification, not an accident.

There is no margin anywhere in `src/`. `Product` is `DELIVERY | INTRADAY`,
which are Indian cash-segment concepts. The crypto cost model is one row:
`BROKERAGE, PERCENT_OF_TURNOVER, 0.001`.

## 2. Why perps, and why now

**The data is free, real-time, deep, and backfillable.** Verified
2026-09-05, all unauthenticated:

| Endpoint | Serves |
|---|---|
| `fapi/v1/premiumIndex` | mark price, index price, `lastFundingRate`, `nextFundingTime` |
| `fapi/v1/fundingRate` | historical funding, per settlement |
| `fapi/v1/klines` | perp OHLC to 2019-09-08 (~7 years) |
| `fapi/v1/exchangeInfo` | `contractType: PERPETUAL`, tick `0.10`, step `0.001`, minNotional `50`, `liquidationFee 0.0125` |
| `fapi/v1/leverageBracket` | maintenance-margin tiers — **requires a key**, see §9 |

This matters because it is the opposite of the platform's other two gaps.
NSE intraday options history is a commercial product, which is why the
chain recorder now runs daily and accrues one irreplaceable day at a time.
Tagged Indian news is the same. Perpetuals have neither problem: the
history already exists and can be pulled whenever.

**It is the smallest honest derivative.** No expiry, no strike, no
exercise, linear payoff. Every hard mechanism a derivative needs — signed
positions, margin, mark-to-market, liquidation, carry — is present in its
simplest form. F&O is the same machinery plus expiry, strike, exercise and
SPAN. Building perps first is how F&O gets a tested core to extend rather
than a second margin system to duplicate.

**It is what the user actually trades.** Longing and shorting BTC-USDT and
ETH-USDT with leverage is the stated ask.

## 3. The decision that shapes everything: the money model

Spot and perps move cash differently, and this is the whole design.

**Spot.** Buying moves cash by notional. `apply_fill` computes
`delta = -(notional + charges)` on BUY and `+(notional - charges)` on SELL.
Equity is `cash + Σ(quantity × mark)` (`breaker.compute_equity`).

**Perp.** Opening moves *no* notional. Margin is **reserved**, not spent.
Cash changes only on realised P&L, fees, and funding. Equity is

```
equity = cash
       + Σ spot_qty × mark                    (unchanged)
       + Σ perp_qty × (mark − entry_price)    (unrealised, signed)
```

Reusing `cash + qty × mark` with a negative quantity would be wrong twice
over: it assumes a short sale credited cash by notional (a *spot* short,
not a perp) and it double-counts entry value. So `compute_equity` gains a
perp term rather than having its existing term reinterpreted.

**Approved approach: a separate derivative core.** Spot keeps its CHECK
constraint, its notional cash model, and its charge model, all untouched
and all still passing. Perps get their own tables. The alternatives were
considered and rejected:

- *Signed spot positions* — drop the constraint, let quantity go negative.
  Cheapest, and it buys short selling, not perpetuals: no leverage, no
  funding, no liquidation.
- *Unify everything now* — one signed margin-aware model for spot, perps
  and F&O. Rewrites `apply_fill`, `paper.engine`, `compute_equity`,
  `runtime/state.py` and the backtester in one pass. The right end state,
  the wrong first step.

## 4. Schema

New instrument identity: `asset_class = 'PERP'`, `exchange =
'BINANCE_FUTURES'`, `segment = 'PERP'`. A distinct `asset_class` rather
than reusing `CRYPTO`, because `load_schedules` and
`_BROKER_BY_ASSET_CLASS` both key on it — sharing `CRYPTO` would silently
apply spot's 10 bps brokerage to a perp fill.

```sql
-- one row per open position; closed positions keep a zero row like spot
CREATE TABLE perp_positions (
    portfolio_id    bigint  NOT NULL REFERENCES portfolios,
    instrument_id   bigint  NOT NULL REFERENCES instruments,
    quantity        numeric(28,8) NOT NULL,     -- SIGNED. no CHECK.
    entry_price     numeric(18,8) NOT NULL,     -- weighted average
    leverage        numeric(6,2)  NOT NULL,
    reserved_margin numeric(18,8) NOT NULL CHECK (reserved_margin >= 0),
    realised_pnl    numeric(18,8) NOT NULL DEFAULT 0,
    funding_paid    numeric(18,8) NOT NULL DEFAULT 0,
    opened_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (portfolio_id, instrument_id)
);

-- the funding series, backfilled and then kept current
CREATE TABLE perp_funding (
    instrument_id bigint NOT NULL REFERENCES instruments,
    funding_time  timestamptz NOT NULL,
    rate          numeric(12,10) NOT NULL,
    mark_price    numeric(18,8)  NOT NULL,
    PRIMARY KEY (instrument_id, funding_time)
);

-- maintenance margin tiers, seeded like charge_schedules
CREATE TABLE perp_margin_tiers (
    instrument_id      bigint NOT NULL REFERENCES instruments,
    notional_floor     numeric(18,2) NOT NULL,
    notional_cap       numeric(18,2) NOT NULL,
    max_leverage       numeric(6,2)  NOT NULL,
    maintenance_rate   numeric(8,6)  NOT NULL,
    maintenance_amount numeric(18,8) NOT NULL,
    effective_from     date NOT NULL,
    PRIMARY KEY (instrument_id, notional_floor, effective_from)
);
```

`ledger_entries.entry_type` gains `FUNDING`, `LIQUIDATION`, and
`MARGIN_RESERVED` / `MARGIN_RELEASED`. Funding and liquidation must be
ledger entries, not derived numbers: a P&L a user cannot trace to a row is
a P&L they are right not to trust.

## 5. Mechanics the engine must get right

**Mark price, not last price.** Liquidation triggers on Binance's mark
price (index-derived), never on last traded. A simulator that liquidates on
last price liquidates on wicks that never touched the mark, and reports
blow-ups that did not happen. `premiumIndex` gives the mark live;
`perp_funding.mark_price` carries it historically.

**Funding, every 8 hours** at 00:00 / 08:00 / 16:00 UTC:

```
payment = position_notional × funding_rate      # longs pay shorts when > 0
```

Applied in the live engine on the settlement boundary, and in the
backtester at the same boundaries. Omitting it makes every carry strategy
backtest as free money — the single most seductive lie available in this
asset class.

**Liquidation** when the margin ratio falls below maintenance:

```
maintenance_margin = |qty| × mark × maintenance_rate − maintenance_amount
liquidate when  equity_of_position < maintenance_margin
```

On liquidation: force-close at the mark, charge `liquidationFee` (1.25%),
write a `LIQUIDATION` ledger entry, and — critically — keep this **distinct
from a circuit-breaker halt**. The breaker is the platform protecting a
portfolio from a strategy; a liquidation is the market closing a position.
Conflating them would let a liquidated strategy read as a paused one.

**Bankruptcy.** If the mark gaps through the bankruptcy price, the loss can
exceed posted margin. `portfolios` carries `ck_no_negative_cash`. Ruling:
close at the bankruptcy price, floor cash at zero, and record the shortfall
explicitly in the ledger entry rather than silently absorbing it — the real
exchange has an insurance fund and we do not, so the honest thing is to
show what the fund would have covered.

**Order validation** gains the contract filters `exchangeInfo` publishes:
tick size `0.10`, step `0.001`, minNotional `50`. Today `tick_size` is NULL
for every crypto row.

## 6. Contract and runtime changes

Strategies need to express things the contract cannot currently say:

- **A short.** `ctx.order(instrument_id, side="SELL", ...)` on a perp with
  no position must open a short rather than being refused.
- **Leverage.** Declared per-instrument in `StrategyManifest`, since a
  strategy's risk profile is a property of the strategy, not of an order.
- **What it is carrying.** `ctx.portfolio` exposes signed quantity, entry
  price, unrealised P&L, reserved margin, free margin, and liquidation
  price.
- **The funding rate**, as a series through `ctx.data`, so a carry strategy
  can condition on it. This is the perp analogue of the intel features in
  plan §7.4: pre-computed, point-in-time, queryable.

`STRATEGY_CONTRACT.md` gains a perpetuals section and a worked example
(funding-carry or trend-with-stop), per the plan's rule that the contract
is not done until three frontier agents each write a working strategy from
it first try.

## 7. Cost model

`charge_schedules` extends to `asset_class = 'PERP'` with Binance USDⓈ-M's
published rates: taker 0.05%, maker 0.02%, liquidation 1.25%. Funding is
**not** a charge — it is a transfer between position holders, and modelling
it as a fee would make it always-negative when it is signed.

The Indian tax lens (plan §8, post-tax P&L) is explicitly **out of scope**
here. The 30% VDA rate and 1% TDS treatment of perpetual derivatives is
genuinely unsettled, and guessing it in a headline number would be worse
than not showing it. Noted for the post-tax phase.

## 8. Task sequence

Each ends in something independently demonstrable, per the plan's phasing
rule.

1. **Instruments and specs.** Seed perp instruments from `exchangeInfo`
   with tick/step/minNotional; seed `perp_margin_tiers`. *Demo: the
   contracts exist with correct filters.*
2. **Market data.** `fstream` ingestor for `@markPrice` and `@aggTrade`;
   `fundingRate` backfill into `perp_funding`; klines backfill. *Demo: 7
   years of BTCUSDT perp bars and every funding settlement in the DB.*
3. **Signed positions and margin.** `perp_positions`, margin reserve and
   release, `compute_equity`'s perp term. *Demo: open a short, watch equity
   move the right way.*
4. **Funding accrual.** 8-hourly settlement in the live engine and at the
   same boundaries in the backtester. *Demo: a position held across a
   settlement shows a FUNDING ledger entry.*
5. **Liquidation.** Maintenance-margin check on the mark, forced close,
   fee, ledger entry, distinct from a breaker halt. *Demo: an over-levered
   position liquidates at the price Binance's own calculator says.*
6. **Contract and runtime.** Manifest leverage, short orders, portfolio
   exposure, funding series in `ctx.data`, contract docs and worked
   example. *Demo: an agent-written short strategy runs forward.*
7. **Backtest and report.** Perp-aware backtesting; the report gains
   funding P&L and liquidation events alongside cost drag. *Demo: a carry
   strategy backtested over 2019–2026 with funding included.*

## 9. Open questions

- **Maintenance-margin tiers need a Binance API key** (`leverageBracket` is
  signed). Seeding them as static reference data is the zero-budget path
  and matches how `charge_schedules` works, at the cost of going stale when
  Binance revises tiers. A free read-only key would let them refresh. Task
  1 decides.
- **Validate the liquidation price against Binance's own calculator**
  before trusting it, exactly as Task 12 validated the Indian charge model
  against Upstox's brokerage calculator. Same discipline, same reason: a
  cost or liquidation model that is subtly wrong is worse than one that is
  obviously missing.
- **Cross vs isolated margin.** This design assumes **isolated** — margin
  is reserved per position. Cross margin shares the whole balance and makes
  liquidation a portfolio-level event. Isolated is simpler, is what a
  retail trader learning risk should start with, and does not preclude
  cross later.
- **One-way vs hedge mode.** Assumes **one-way**: one signed position per
  instrument, which the `PRIMARY KEY (portfolio_id, instrument_id)` above
  encodes. Hedge mode (simultaneous long and short) would need a position
  side in the key.

## 10. What this deliberately is not

No live order routing — the §2 bright lines are unchanged, and a simulated
perp is as far outside SEBI's algo perimeter as a simulated equity. No
options on crypto. No cross-margin. No hedge mode. No perpetuals on any
venue but Binance, though the `data_sources` abstraction means a second
venue is a config change rather than a rewrite.
