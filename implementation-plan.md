# Implementation Plan: Multi-Asset Paper Trading & Backtesting Platform for Indian Retail Traders

**Working name:** (TBD) · **Author:** Satyam + Claude · **Date:** August 2026 · **Status:** v1.4 — consistency review applied (data-table modes, relative-strike policy, historical-data cliff) + Retail Trader Experience layer (§8). All decisions resolved.

---

## 1. What we are building, precisely

A simulation-only platform where Indian retail traders can (a) watch streaming prices across every asset class an Indian resident can legally invest in, (b) paper-trade manually against those prices, (c) write or import algorithmic strategies and run them forward in a sandbox against live-simulated markets, and (d) backtest those same strategies against historical data with institution-grade correctness and a full metrics suite, and (e) consume a market-intelligence layer (§7) that converts large-order flows, news, and social sentiment into structured per-instrument features available to both the trader and their strategies.

The differentiating idea is the **Agent Contract**: the platform publishes a machine-readable specification (a `STRATEGY_CONTRACT.md` plus JSON schemas and an SDK stub) that a user feeds to any external AI agent — Claude, ChatGPT, Gemini, a local model — and the agent produces strategy code guaranteed to run on the platform. The platform never needs to build its own LLM; it becomes the *execution substrate* that every agent can target. This is analogous to how MCP standardized tool access: you are standardizing strategy authorship.

V1 is a single-user personal platform (multi-user retail launch is a later phase). Two things this platform deliberately is **not** in V1: it does not route real orders to any broker or exchange, and it does not recommend, rank, or sell strategies. Both exclusions are load-bearing for the regulatory analysis below.

### Who already plays here

Streak (Zerodha) and Tradetron do no-code algo creation with live deployment; AlgoTest does options backtesting + paper trading; QuantConnect/Backtrader serve the global coder crowd; TradingView and Moneybhai offer manual paper trading. Nobody combines (i) all-asset Indian coverage including US stocks and crypto under one simulated portfolio, (ii) code-first strategies authored by *external* AI agents against a published contract, and (iii) an honest Indian cost model (STT, stamp duty, GST, SEBI charges, brokerage) baked into fills. That third point sounds boring but is where most retail backtests silently lie, and it is cheap for us to get right.

---

## 2. Regulatory ground truth (researched Aug 2026)

This section is the foundation. Everything in the architecture is shaped by three findings.

**Finding 1 — Market data licensing is the single biggest constraint, not technology.** NSE, BSE, and MCX own their market data. Displaying or redistributing *real-time* exchange data to third-party users without a vendor/redistribution license is illegal, and licenses cost lakhs per year plus onboarding time. Authorized vendors (TrueData, Global Datafeeds, etc.) license data for *internal/individual* use and explicitly prohibit redistribution to your users without separate exchange approval. Broker APIs (Kite Connect, Dhan, Fyers, Upstox) are licensed for the account holder's personal use only — building a multi-user product on top of a personal broker API key violates their terms and exchange policy. The practical consequence: **the MVP must run on data that is free to redistribute — end-of-day official files, delayed quotes, and asset classes with genuinely open data (crypto, and to a large extent US markets)** — and real-time licensed Indian data becomes a funded-stage line item, not a day-one requirement. Note that for a *simulator*, 15-minute-delayed data is almost cosmetically indistinguishable from real-time for strategy evaluation purposes, and we can be transparent about it.

**Finding 2 — SEBI's retail algo framework (fully mandatory since April 1, 2026) applies to live order routing, not simulation.** The February 2025 circular and its glide path (broker registration of algos from Oct 2025, no new non-compliant API clients from Jan 2026, full enforcement April 2026) governs algorithms that place real orders through brokers: exchange-registered Algo IDs, whitelisted static IPs, broker accountability. A platform that only *simulates* execution sits outside this perimeter. The bright lines we must never cross in V1: no live order routing, no "guaranteed returns" or performance-based marketing claims (SEBI acted against 120+ brokers tied to unregulated algo platforms making such claims), no strategy marketplace where we distribute third parties' strategies, and no investment advice (which would trigger RIA/RA registration). Prominent, persistent disclaimers ("educational simulation; not investment advice; simulated results do not represent actual trading") are mandatory hygiene. If we ever add live deployment later, the compliant path is a partnership where the *broker* registers the algo and routes orders — an entire Phase 5 of its own.

**Finding 3 — Crypto is legal to trade in India (VDA regime), so simulating it is unambiguously fine.** Holding/trading is legal, not legal tender, 30% tax + 1% TDS on real trades — none of which applies to simulation. Crypto data (Binance, CoinGecko, etc.) is also the one asset class with genuinely free real-time streaming data, which makes it ideal for building and demonstrating the streaming pipeline first. US stocks are legally investable by Indians under LRS, and US market data has free/cheap tiers (Alpaca, Finnhub, yfinance) with 15-min delayed or IEX-only real-time feeds that permit personal/app use far more liberally than Indian exchanges.

---

## 3. Data strategy per asset class (phased)

The table below shows what data each *mode* can legally use. "Personal mode" = V1, you as sole user (broker-API personal data is additionally available — see §3.1, which supersedes this column for V1). "Public mode" = the day strangers sign up.

| Asset class | Public mode (redistributable, free) | Latency (public mode) | Public mode, funded (licensed) |
|---|---|---|---|
| Indian equities (NSE/BSE) | Official EOD bhavcopy files (free, redistributable as derived DB), delayed quotes for watchlists | EOD + ~15-min delayed | Authorized vendor real-time L1 feed (TrueData/GFDL, ₹ tens of thousands/yr) with redistribution addendum |
| Indian F&O (index + stock options, futures) | EOD F&O bhavcopy (all strikes, OI, settlement), option-chain snapshots at coarse intervals | EOD + periodic snapshots | Vendor real-time option chains; historical tick/1-min options data (expensive — see §12 Q3) |
| US stocks/ETFs | yfinance for historical; Alpaca/Finnhub free tier for delayed or IEX real-time | Near-real-time (IEX) | Polygon.io paid tier |
| Crypto | Binance/CoinGecko WebSocket + REST — free, real-time, redistributable | True real-time | Same (already solved) |
| Commodities (MCX) | EOD bhavcopy; international proxies (COMEX gold, Brent) free | EOD | MCX vendor feed |
| Bonds / G-Secs | NSE/BSE debt segment EOD; RBI/CCIL published yields | EOD (bonds barely tick intraday for retail anyway) | Same, mostly |
| IPO / SME | NSE/BSE announcements + listing-day data (scrape/EOD); simulate allotment lottery | Event-driven | Same |
| Mutual funds (worth adding) | AMFI daily NAVs — free, official | Daily | Same |

Design consequence: the ingestion layer must treat **update cadence as a per-instrument property**, not a platform-wide one. A crypto pair ticks every 100 ms; a bond reprices daily; an option chain snapshot arrives every 3 minutes. The simulation clock and the UI both need to handle heterogeneous cadence gracefully rather than pretending everything streams uniformly.

Historical depth targets for credible backtesting: 10+ years EOD for equities/indices (bhavcopy archives go back decades), 3–5 years daily for F&O (free), 1-minute equity/index data purchased later from a vendor when users demand intraday backtests (this is a discrete, deferrable cost).

Corporate actions (splits, bonuses, dividends, symbol changes) must be ingested as first-class events from day one — retrofitting adjusted-price logic into a populated database is one of the classic regrets in this domain.

### 3.1 Personal-mode data plan (DECIDED: platform is for personal use in V1)

Since this is your own account serving only you, broker-API personal data is fully legitimate, and the licensing constraints of §2 Finding 1 are deferred until the day strangers sign up. The concrete free stack:

**Upstox Developer API (free, no API charges)** becomes the primary Indian data source: real-time WebSocket market feed for equities and F&O, historical candles down to 1-minute for active contracts, and — critically — the **Expired Historical Candle Data API**, which serves OHLC at 1/3/5/15/30-minute and daily intervals for expired F&O contracts (you first fetch the expired-contract instrument keys per underlying and expiry, then pull candles). One community thread associates expired-contract data with "Upstox Plus," so empirically verifying free-tier access is a week-1 Phase 0 task.

**Dhan API v2 (Expired Options Data endpoint)** is the strongest known dataset for your use case: pre-processed **1-minute expired options data going back 5 years**, strike-wise relative to spot (ATM and ±10 strikes), for both index and stock options, including OHLC, **implied volatility, volume, open interest, and spot** — fetched on a rolling basis, 30 days per call. IV and OI at minute granularity is what makes serious options backtesting (straddle deltas, OI-based filters, IV-rank entries) possible at all. Caveat: Dhan's trading APIs are free but its market *data* APIs have historically carried a ~₹499/month charge, and it is unclear from documentation alone which side of that line the expired-options endpoint falls on. Verification task: open a free Dhan account, hit the endpoint, and see.

**Self-recording as the guaranteed-free floor:** from the first week the platform exists, an ingestion worker subscribes to the live option-chain WebSocket (Upstox free feed) for NIFTY/BANKNIFTY/SENSEX and your chosen stock underlyings, and archives 1-minute bars + OI into TimescaleDB every trading day. This costs nothing, is unambiguously permitted (personal use), and compounds: six months from now you own six months of clean intraday chains regardless of what any broker's historical endpoint does. Backfill strategy is therefore three-layered: Dhan/Upstox expired-contract endpoints for the past, self-recording for the future, EOD F&O bhavcopy (free, 10+ years) as the always-available base layer.

**The relative-strike trap (engine requirement, not a footnote).** Dhan's data is keyed *relative to spot* (ATM, ATM+1, …), but backtests trade *absolute* contracts. Two consequences the backtest engine must handle explicitly. First, reconstruction: relative strikes are mapped to absolute strikes through the spot series at every timestamp to rebuild each contract's own price history. Second, drift gaps: a *held* position's strike migrates relative to ATM as the market moves — sell a strangle at ATM±8, watch NIFTY move 3%, and the short leg exits the ±10 window mid-position, exactly when P&L matters most. Gap policy: mark such positions via last-known IV through Black-Scholes, tag every affected fill/mark as `reconstructed`, and have the backtest report disclose the percentage of P&L derived from reconstructed prices. Silent interpolation here is how a backtesting platform loses trust; disclosure is how it earns it. (Self-recorded chains from §3.1 use absolute strikes and don't have this problem — one more reason the recorder starts week one.)

---

## 4. System architecture

Seven services, deliberately boring technology, monolith-first with clean internal boundaries so services can be split when load justifies it.

### 4.1 Component map

```
                      ┌────────────────────────────────────────────┐
                      │                Web App (Next.js)           │
                      │  Watchlists · Charts · Portfolio · Strategy│
                      │  Editor · Backtest Reports · Leaderboard   │
                      └───────▲──────────────────────▲─────────────┘
                              │ REST                  │ WebSocket
                      ┌───────┴───────┐      ┌───────┴────────┐
                      │  API Gateway  │      │ Stream Gateway │
                      │ (FastAPI)     │      │ (Redis pub/sub │
                      └───┬───────┬───┘      │  fan-out)      │
                          │       │          └───────▲────────┘
            ┌─────────────┘       └──────────┐       │
   ┌────────▼─────────┐            ┌─────────▼───────┴──┐
   │ Portfolio &      │            │  Market Data Core  │
   │ Simulation Engine│◄──prices───│  Ingestors (crypto │
   │ (orders, fills,  │            │  WS, US API, EOD   │
   │ Indian cost model│            │  jobs, snapshots)  │
   │ margins, expiry) │            └─────────┬──────────┘
   └────────▲─────────┘                      │
            │                       ┌────────▼──────────┐
   ┌────────┴─────────┐             │ TimescaleDB       │
   │ Strategy Runtime │◄──history───│ (candles, ticks,  │
   │ (sandboxed pods, │             │ chains, corp acts,│
   │ agent-contract   │             │ instruments)      │
   │ SDK)             │             └───────────────────┘
   └────────▲─────────┘
            │ runs
   ┌────────┴─────────┐
   │ Backtest Engine  │  + Metrics Service (shared library, not a service)
   └──────────────────┘
```

### 4.2 Storage

**TimescaleDB (Postgres extension)** as the single source of truth: hypertables for OHLCV at multiple granularities, option-chain snapshots, and (later) ticks; ordinary tables for the instrument master, corporate actions, users, portfolios, orders, fills, strategy definitions, and backtest results. One database engine covering both relational and time-series workloads is a massive operational simplification for a solo founder, and Timescale's continuous aggregates give you 1-min → 1-day rollups for free. ClickHouse or QuestDB become relevant only if tick volume explodes (Phase 3+ problem). Redis for pub/sub price fan-out, session cache, and rate limiting.

The **instrument master** is the schema's heart: a canonical `instrument_id` mapping every tradable thing (RELIANCE equity, NIFTY 24AUG26 24500 CE, BTC-USDT, AAPL, SGB tranche, an IPO application) to its asset class, exchange, tick size, lot size, expiry/strike where applicable, currency, and data-source binding. Every other table references it. Getting this right early is the highest-leverage schema decision — and it is also exactly what the Agent Contract exposes to external AI agents.

### 4.3 Simulation engine (the paper-trading heart)

Order lifecycle: user/strategy submits order → validation (market hours per exchange calendar, circuit limits, margin check) → fill simulation → position/ledger update → event published to stream gateway.

Fill realism matters more than data latency for trustworthy results. The fill model should support, per asset class: market orders filled at next available price plus a configurable slippage model (fixed bps, or volume-participation based); limit orders resting until the simulated price crosses; for EOD-only instruments, fills at next open or at close with explicit labeling. For F&O: lot-size enforcement, SPAN-approximate margining (a simplified but honest margin model — flag it as approximate), physical-settlement warnings for stock options, and automatic expiry handling (exercise/assign ITM at settlement price, expire OTM worthless).

**The Indian cost model is a headline feature.** Every fill computes: brokerage (configurable — flat ₹20 discount-broker default), STT/CTT (segment-specific rates, notably STT on options premium on sell and on exercise at intrinsic), exchange transaction charges, SEBI turnover fee, stamp duty (buy-side, state-uniform rates), GST on charges, and DP charges on equity delivery sells. For US stocks: LRS-context notes and forex conversion spread simulation. For crypto: exchange taker/maker fees and optionally the 1% TDS drag so users see how brutal it really is. Currency: portfolios hold multi-currency positions with INR as reporting currency and daily FX marks sourced from the RBI reference rate (free, official).

Multiple concurrent portfolios per user (e.g., "swing ideas", "options income", "crypto momentum") each with independent virtual capital, so one blown-up experiment doesn't pollute another's track record.

### 4.4 Streaming layer

Ingestors normalize every source into one internal tick/candle message shape and publish to Redis channels keyed by `instrument_id`. The stream gateway holds client WebSocket connections and fans out only subscribed instruments (a user typically watches 20–50, never all 5,000). Delayed Indian data is streamed with an honest `as_of` timestamp and a UI badge — never masquerade delayed data as live. For instruments that only update EOD, the "stream" degrades to a daily event, and the UI shows last close + change rather than a fake ticking price.

A crucial extra: the **replay service**. Because the simulator owns its clock, we can replay any historical day at 1×/10×/60× speed through the same streaming pipeline. This gives users "live-like" intraday practice on historical days using only EOD-derived or stored snapshot data — a genuinely loved feature in this niche (it's what made TradingView's replay sticky) and it converts our data-latency weakness into a product strength.

---

## 5. The Agent Contract (your MD-file idea, formalized)

This is the platform's moat, so it deserves rigor. The contract is a versioned bundle the user downloads (or copy-pastes) and hands to any AI agent:

**`STRATEGY_CONTRACT.md`** — human-and-agent-readable spec containing: the strategy lifecycle API, the instrument schema, the data access API with exact field names and units, order types and constraints per asset class, the cost model summary (so agents write cost-aware strategies), sandbox limits (CPU/memory/time, no network, allowed libraries), determinism rules, and 3–4 complete worked examples (SMA crossover on equity, iron condor on NIFTY weeklies, BTC momentum, multi-asset rebalancer). Written the way a great MCP server documents its tools: assume the reader is a capable model with zero platform context.

**`schema.json`** — machine-validatable JSON Schemas for the strategy manifest, instrument records, bar/tick payloads, and order objects, so an agent (or our validator) can check conformance mechanically.

**SDK stub (`platform_sdk.py`)** — typed no-op implementation of the runtime interface so generated code can be lint-checked and even dry-run locally by the user before upload.

### Strategy interface (proposed)

```python
class Strategy:
    def configure(self) -> StrategyManifest: ...
        # declares: universe (instrument queries), data granularity,
        # capital, schedule (on_bar/on_tick/cron), params with types/bounds
    def initialize(self, ctx: Context): ...
    def on_bar(self, ctx: Context, bars: dict[InstrumentId, Bar]): ...
    def on_tick(self, ctx: Context, tick: Tick): ...          # optional
    def on_order_update(self, ctx: Context, update: OrderUpdate): ...
    def on_expiry(self, ctx: Context, event: ExpiryEvent): ...  # F&O
```

`Context` exposes: `ctx.data` (point-in-time historical window queries only — the API physically cannot return data past the simulation clock, which kills lookahead bias by construction), `ctx.portfolio` (positions, cash, margin), `ctx.order()` / `ctx.cancel()`, `ctx.log()`, `ctx.state` (persisted KV for the strategy), and `ctx.now` (simulation clock — strategies never read wall-clock time).

### Upload → run pipeline

1. **Static validation**: manifest schema check, import allowlist (numpy, pandas, ta-lib, stdlib subset), AST scan rejecting `open`/`socket`/`exec`/`subprocess`/dunder tricks.
2. **Smoke run**: 5 simulated days in a throwaway sandbox; must produce no crashes and at least parse orders correctly.
3. **Registration**: strategy stored, versioned, ready for backtest or forward paper-run.
4. **Failure feedback loop**: every rejection returns a structured error report *formatted to be pasted back into the user's AI agent* ("Your strategy failed validation: line 42 imports `requests`, which is not permitted. See STRATEGY_CONTRACT.md §6.") — closing the agent iteration loop is the UX detail that makes the whole concept sing.

### Sandbox security (non-negotiable, since we execute strangers' code)

Each run executes in a container with gVisor (`runsc`) runtime — syscall-level isolation on top of Docker — with: no network namespace, read-only filesystem plus a small tmpfs, hard CPU/memory/PID limits, wall-clock timeout, seccomp default-deny beyond gVisor, and non-root UID. Strategy I/O happens exclusively over a Unix socket RPC to the runtime supervisor (which enforces the point-in-time data rule and order-rate limits). Backtests run the same image with the clock accelerated, so backtest and forward-paper behavior are bit-identical by construction. Firecracker microVMs are the upgrade path if we ever host truly adversarial workloads at scale.

---

## 6. Backtesting engine and correctness doctrine

**Event-driven, not vectorized, as the core.** Vectorized engines (vectorbt-style) are 100× faster but can't share code with the forward paper-trading path and invite subtle lookahead. Our event-driven engine feeds the *same* strategy container a compressed stream of historical events through the *same* Context API. One engine, two clock speeds. (A vectorized "quick scan" mode for parameter sweeps can come later as a clearly-labeled approximation.) Build on the shoulders of open source where sensible — Nautilus Trader and zipline-reloaded are reference-quality event-driven designs — but the Indian cost model, F&O expiry mechanics, and the contract API are ours; see §12 Q4.

Correctness checklist the engine must enforce, because each item is a classic silent lie in retail backtests:

survivorship bias (universe queries resolve against point-in-time listings — delisted stocks stay in history); lookahead bias (structurally impossible via the data API, plus bar timestamps mark *close* time and orders fill next bar at earliest); corporate-action adjustment (backtests run on adjusted series, fills recorded at unadjusted actual prices); realistic fills and costs (§4.3 model applied identically in backtest); F&O mechanics (settlement prices at expiry, weekly-expiry calendar changes over the years, lot-size revisions as dated events); circuit limits and trading halts (orders can go unfilled — a strategy must survive that); capital realism (no negative cash, margin calls simulated, position sizing respects lot sizes); and parameter-overfitting guardrails (walk-forward splits and an in-sample/out-of-sample report shown by default, with a gentle warning when a user re-runs the same strategy many times on identical data).

**Metrics suite** (computed per backtest and per live paper portfolio, benchmark-relative to NIFTY 50 TRI or user-chosen benchmark): total & annualized return, CAGR; volatility; Sharpe, Sortino, Calmar; max drawdown depth *and duration*; win rate, profit factor, average win/loss, expectancy; exposure %, turnover, number of trades; alpha/beta vs benchmark; VaR (95) and worst-day; equity curve, drawdown curve, monthly returns heatmap, rolling 6-month Sharpe; cost drag report (gross vs net-of-all-Indian-charges — often the most sobering chart we can show a retail options trader); and for F&O, Greeks exposure over time where computable.

---

## 7. Market Intelligence Layer (large flows, news, sentiment)

### 7.0 The architectural insight that shapes this feature

Two constraints collide productively here. Your strategy sandbox has **no network access** (a security invariant from §5), so a running strategy cannot fetch news or call an LLM. And the external AI agent *authoring* a strategy may have no live internet either. Therefore the intelligence layer's job is precisely defined: continuously convert the unstructured world (news, filings, social chatter, order flow) into **structured, per-instrument, point-in-time features** stored in the database — which strategies then query through the contract API exactly like price data. The heavy NLP happens once, at ingestion time, in our pipeline; strategies consume cheap pre-computed numbers. The Agent Contract ships the full feature dictionary, so an offline agent can still write "if sentiment_1d < -0.5 and buzz_ratio > 3, exit longs" without ever seeing a news site.

### 7.1 Signal inventory (all free-tier, per the zero-budget rule)

**Large orders and flows.** True whale-watching needs order-book depth that Indian retail feeds don't expose beyond 5 levels, but India compensates with unusually good *official* disclosures, all free: NSE/BSE **bulk deals** (any single-client trade >0.5% of listed shares) and **block deals** published daily with counterparty names; daily **FII/DII net flows**; **delivery percentage** per stock (conviction proxy); F&O **open-interest changes, put-call ratio, futures basis** from the EOD bhavcopy we already ingest; and from the live Upstox depth feed, computed **large-trade imprints** (per-minute volume z-scores vs trailing baseline, VWAP pressure). Crypto is the luxury segment: Binance gives full depth, large-trade prints, funding rates, and long/short ratios free and real-time. MCX flows come from EOD OI.

**News.** The precision anchor is **NSE/BSE corporate announcements** — official, free, and *already tagged to the exact symbol* (earnings, order wins, pledges, resignations), which makes them ground truth for training and sanity-checking the classifier. Around that: RSS feeds of the major Indian financial press (Moneycontrol, ET Markets, Business Standard, Livemint, Reuters India), **GDELT** (free, global, historical — the only source with meaningful backfill) for macro/world events, and a hand-maintained **event calendar** (RBI MPC, CPI/WPI prints, Fed meetings, budget day, expiry days) ingested as scheduled known-unknowns.

**Social.** Reddit (r/IndianStreetBets, r/IndiaInvestments) and StockTwits have workable free tiers. Twitter/X's API is paid (~$100+/month) and is **excluded from V1** — see decision Q6.

### 7.2 Pipeline: ingest → link → classify → score → features

Five stages, running as scheduled workers next to the price ingestors:

**Entity linking** (news → instrument) is the make-or-break stage, and it is mostly *not* deep learning: an **alias table** derived from the instrument master (legal names, tickers, common abbreviations like "RIL", brands, key subsidiaries, index membership) plus fuzzy matching resolves the large majority of Indian financial headlines, because financial text names companies explicitly. Ambiguous leftovers go to an embedding-similarity match, and only the residue to an LLM fallback. Macro news links to index/commodity instruments via a keyword→instrument map ("crude", "monsoon", "repo rate" → their affected instruments).

**Classification** assigns each item an event type from a fixed taxonomy (earnings, guidance, order-win, regulatory action, management change, M&A, macro-policy, macro-data, rumor/unverified) — this taxonomy is part of the Agent Contract, so agents can condition on event types.

**Sentiment** uses **pretrained FinBERT** (free, open-source, runs on CPU — no GPU bill, and comfortably on your machine), producing a signed score per item. An optional local-LLM pass (Ollama, which you already run) can be layered on for Indian-English nuance and headline sarcasm, batch-processed off-peak. **No custom deep-learning training in V1** — see decision Q7 for why.

**Feature computation** rolls item-level outputs into the per-instrument, per-timestamp features strategies actually consume: `sentiment_1h/1d/5d` (decay-weighted), `buzz_ratio` (coverage volume vs 30-day baseline), `novelty` (first-report vs follow-up), event-type flags with recency, `fii_dii_net_5d`, `oi_change_pct`, `pcr`, `delivery_pct_zscore`, `bulk_deal_flag`, `large_trade_imprint`. Stored in an `intel_features` hypertable alongside a raw archive of every source item (for audit and future re-scoring when models improve).

### 7.3 Point-in-time discipline (the part everyone gets wrong)

Every feature is timestamped by **when our recorder knew it**, not when the event nominally happened — because that is what a live strategy would have experienced. The backtest data API serves intel features under the same past-only rule as prices, killing the most seductive lookahead bias in news trading ("the model knew the earnings headline at 09:14 that was actually published at 09:31"). The honest consequence: **news-driven backtests are only trustworthy from the day your recorder starts running** — historical *tagged* Indian news archives are commercial products, so like the options recorder, the news recorder starts in Phase 0 week one and compounds. GDELT backfill gives partial macro history with an explicit "reconstructed, lower-trust" label.

### 7.4 Contract extension

`ctx.intel.features(instrument_id, names, window)` returns the feature time-series; `ctx.intel.events(instrument_id, types, window)` returns structured event records; `ctx.intel.headlines(instrument_id, window)` returns archived headline text + scores (structured records, never live web). The strategy manifest declares intel subscriptions so the runtime pre-loads only what is needed. The contract's feature dictionary documents every feature's definition, unit, update cadence, and available history depth — the last one matters so agents don't write 2-year sentiment lookbacks against a 3-month archive.

### 7.5 UI surface

Per-instrument intelligence tab: sentiment timeline overlaid on price, event markers pinned to the chart, flow dashboard (FII/DII, OI change, delivery %, bulk/block deals), and a buzz meter. Plus a market-wide pulse page for NIFTY-level macro sentiment and the event calendar.

---

## 8. Retail trader experience layer (v1.4 — behavior, not just strategy)

The plan so far optimizes for strategy correctness, but SEBI's own research says retail losses (90%+ of F&O traders losing; ₹1.05 lakh crore net individual losses in FY25) are driven as much by *behavior* as by bad strategies. Four features close that gap, and all are cheap relative to their value:

**Trading journal with mandatory rationale.** Every manual order requires a one-line "why" before it submits (strategy orders auto-log their triggering condition). Post-trade analytics then confront the record with reality: "you exited at 10:42; holding to your stated target was worth +₹3,400 / your stop would have saved −₹1,900." Weekly digest of discipline stats — plan-adherence rate, average premature-exit cost, revenge-trade detector (re-entry within N minutes of a loss). This is the feature category retail traders pay $30/month for elsewhere, built here on data the ledger already has.

**Robustness suite on every backtest, by default.** Alongside the base result: an automatic 2× slippage-and-cost stress rerun (if the edge dies at 2×, it was never an edge), and a Monte Carlo trade-order reshuffle producing a *distribution* of max drawdowns and terminal equities rather than the single lucky path — with the 5th-percentile outcome displayed as prominently as the mean. Complements the walk-forward analysis of §6.

**Blind replay.** The §4.4 replay service with symbol and date hidden: pure pattern-and-process practice with hindsight bias surgically removed. Trivial to build on existing replay, disproportionately loved by traders.

**Post-tax P&L.** Per-portfolio tax lens applying Indian reality: STCG/LTCG classification with holding periods for equity, business-income framing for F&O turnover, the 30% + 1% TDS regime for crypto (no loss offset — shown honestly), and LRS/forex context for US holdings. Headline number becomes post-tax, post-cost CAGR next to the gross figure — the truth almost no retail platform shows.

Also folded in: **Telegram alerts** via the free Bot API (price levels, intel events from §7 like sentiment spikes or bulk deals on watchlist names, strategy health: fills, drawdown-pause trips, sandbox errors) — the platform must reach you during MathWorks hours, not wait for you; and a **portfolio-level circuit breaker** for forward paper runs (auto-pause any strategy breaching its declared max daily loss or max drawdown, notify via Telegram), which doubles as runaway-loop protection.

Build placement: journal + circuit breaker + Telegram land in Phase 1–2 (they hook the order/ledger path); robustness suite and post-tax lens land in Phase 3 (they hook the backtest reporter); blind replay whenever, it's a weekend task.

---

## 9. Tech stack recommendation

Python everywhere the money logic lives (FastAPI gateway, ingestion workers, simulation + backtest engine, strategy runtime — and Python is also what AI agents generate best, which matters unusually much here). Next.js + TypeScript frontend with TradingView Lightweight Charts (free, canonical for finance UIs). TimescaleDB + Redis. Celery or arq for scheduled jobs (EOD ingestion, expiry processing, corporate actions). Docker Compose on a single VPS (Hetzner/DO, ~₹3–5k/month) for Phase 1; Kubernetes only when sandbox-pod scheduling demands it. Auth via a managed provider (Clerk/Auth0/Supabase Auth). Everything IaC'd and observable (Grafana + Prometheus) from early, because a data platform's failures are silent data gaps, not crashes.

---

## 10. Phased roadmap with AI-model delegation

Each phase notes which Claude tier to use in Claude Code — reserve the frontier model for design-heavy work, use cheaper models for well-specified implementation.

**Phase 0 — Foundations (2–3 weeks).** Instrument master schema, TimescaleDB setup, EOD ingestion jobs for NSE/BSE equity + F&O bhavcopy with 10 years of backfill, corporate actions ingestion, AMFI NAVs. **Week-1 empirical checks:** open free Upstox + Dhan accounts, verify free-tier access to expired-contract candles and the expired-options (IV/OI) endpoint, and record findings in the data-source registry. **Start both self-recorders immediately** — the options chain recorder (§3.1) and the news/announcements/flows recorder (§7.3) — since each accrues irreplaceable point-in-time history every day it runs. *Mostly Sonnet-tier: schemas and ETL against a written spec. Use the frontier tier once, to review the instrument-master design.*

**Phase 1 — Streaming + manual paper trading (4–6 weeks).** Upstox real-time WebSocket ingestion for Indian equities + F&O (personal mode), crypto WebSocket (Binance), US delayed feed, stream gateway, charts/watchlists UI, order placement with full Indian cost model, portfolio ledger, exchange calendars, the replay service, trading journal + Telegram alert bot (§8). *Simulation engine + cost model: frontier tier (correctness-critical). UI and ingestors: Sonnet.*

**Phase 2 — Agent Contract + strategy runtime (6–8 weeks).** Contract docs + schemas + SDK, sandbox infrastructure (gVisor), validation pipeline, forward paper-running of strategies with monitoring dashboard. Dogfood by having Claude/ChatGPT write strategies from the contract with zero extra context — the contract isn't done until three different frontier agents each produce a working strategy first-try. *Contract authoring and sandbox security: frontier tier. SDK stub, validators, dashboards: Sonnet.*

**Phase 2.5 — Intelligence layer (4–6 weeks, partly parallel with Phase 2).** Bulk/block/FII-DII/delivery ingestion, RSS + announcements + GDELT pipelines, alias table + entity linking, FinBERT sentiment scoring, feature computation jobs, `ctx.intel` contract extension, intelligence UI tab. The recorders have been running since Phase 0, so real archived data exists to build against. *Entity-linking design and feature definitions: frontier tier. Ingestion workers, FinBERT integration, UI: Sonnet.*

**Phase 3 — Backtesting + metrics (6–8 weeks).** Event-driven backtest engine, correctness checklist (including the relative-strike reconstruction policy of §3.1), full metrics + report UI, walk-forward analysis, robustness suite + post-tax P&L lens (§8). By the end of this phase the platform is complete for its V1 purpose: your personal research lab. *Engine core: frontier tier. Metrics library and report UI: Sonnet, with Haiku fine for chart components.*

**Phase 4 — Hardening + growth (ongoing).** Licensed real-time Indian data (now justified by usage), 1-min historical intraday purchase — **and note the historical-data cliff**: the archive accumulated via personal broker APIs (Upstox candles, Dhan options, self-recorded chains) is derived exchange data that cannot be served to other users' backtests without licensing; going public means re-licensing *history* as well as real-time, or restricting public users to genuinely redistributable layers (bhavcopy-derived, crypto, transformed intel features) until licensed. Also: leaderboards/community (carefully — no performance-claim marketing), monetization (freemium: limited backtests/day free, unlimited + intraday data paid). **Phase 5 (someday):** live deployment via broker partnership under the SEBI algo framework — a separate compliance project.

Total to a complete personal V1 (Phases 0–3): roughly 5–7 months of consistent part-time work alongside MathWorks and Astro Acharya, front-loaded on schema and contract design where mistakes are expensive. A public launch is Phase 4+ and gated on the data-licensing decisions above.

---

## 11. Risks worth naming

Data licensing enforcement if we accidentally redistribute licensed feeds (mitigation: MVP uses only free/redistributable sources, every source's ToS reviewed and logged); sandbox escape (mitigation: gVisor + no-network + allowlist, and we don't store anything valuable on sandbox hosts anyway); backtest-trust erosion if results are subtly wrong (mitigation: the correctness doctrine, plus publishing our fill/cost assumptions openly — transparency as brand); SEBI perimeter drift if we add advice-like or marketplace features casually (mitigation: the two bright lines in §2 reviewed before every feature ship); intelligence-layer specific risks — sentiment models misreading Indian-English financial text, rumor/manipulation content (pump groups) polluting social signals (mitigation: source-level trust weights, `rumor` event class, official announcements as ground truth), and entity-linking errors silently corrupting features (mitigation: precision-over-recall linking, weekly spot-check sample); and solo-founder bandwidth against your MathWorks job and Astro Acharya (mitigation: the phasing above is designed so every phase ends in something independently demo-able).

---

## 12. Decisions — ALL RESOLVED (Aug 2026)

**Q1 — Personal use in V1.** The platform serves you alone until you decide otherwise. Consequences: broker-API personal data is the primary source (real-time, free, legitimate); the §2 licensing constraints are deferred, but the ingestion layer keeps its source-abstraction boundary so that swapping to redistributable sources when going public is a config change, not a rewrite. Auth can be a single-user login for now; multi-tenancy stays in the schema (a `user_id` on everything costs nothing today and saves a migration later).

**Q2 — Python-only strategies in V1.** The Agent Contract is the no-code layer; a visual builder is a V2 concern. This removes an entire frontend subsystem from scope.

**Q3 — Intraday options via the free three-layer stack (§3.1).** Dhan expired-options endpoint (5 yr, 1-min, IV/OI, ATM±10) + Upstox expired-contract candles for backfill, self-recording from week one for the future, EOD bhavcopy as base. Zero-budget constraint honored; the only open verification is whether Dhan's endpoint sits behind its ₹499/mo data plan — checked empirically in Phase 0, and if it does, Upstox expired candles + self-recording still deliver the feature (minus historical IV, which we can approximate by computing IV from prices ourselves via Black-Scholes given the spot series — a nice Sonnet-tier task).

**Q4 — Custom event-driven engine,** borrowing proven event-loop and data-portal designs from Nautilus Trader and zipline-reloaded rather than embedding them.

**Q5 — Private portfolios only in V1.** Leaderboards/social deferred to Phase 4 behind opt-in and standardized-capital rules.

**Q6 — Social sources: Reddit + StockTwits in V1; Twitter/X excluded** (paid API conflicts with zero-budget rule).

**Q7 — No custom DL training in V1: pretrained FinBERT + alias-table entity linking + optional local Ollama fallback.** Rationale: entity linking in financial text is dominantly solvable with deterministic alias matching; FinBERT is free, CPU-friendly, and purpose-built for financial sentiment; training a custom model requires a labeled dataset we don't have and won't have until the recorders have run for months — at which point fine-tuning becomes an evidence-driven Phase 4 decision rather than a speculative V1 one.

**Q8 — Accept that news-driven backtests begin at recorder-start.** Historical tagged Indian news archives are commercial; GDELT backfill covers macro only, labeled lower-trust. The alternative (buying archives) violates the zero-budget rule.

