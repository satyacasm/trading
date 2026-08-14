# Phase 0 — Data Foundations: Design Spec

**Date:** 2026-08-14 · **Status:** Approved for implementation planning
**Parent plan:** [`implementation-plan.md`](../../../implementation-plan.md) §3, §4.2, §6, §10 (Phase 0)
**Scope:** Phase 0 only. Phases 1–4 get their own spec cycles.

---

## 1. Scope

### In scope

Build the data foundation: a canonical instrument master, a TimescaleDB store, and an idempotent ingestion pipeline that backfills ten years of official end-of-day data from NSE, BSE, and AMFI, plus corporate actions.

Concretely, at the end of Phase 0:

- TimescaleDB and Redis run locally under Docker Compose.
- The instrument master holds every NSE/BSE equity, index, futures, and options contract seen in ten years of archives, including delisted and expired ones.
- `bars_daily` holds ~10 years of official EOD OHLCV for NSE equities, NSE F&O, and BSE equities, plus daily AMFI mutual-fund NAVs.
- Corporate actions are ingested as dated events, and price adjustment is computed at read time.
- Re-running any ingestion day is provably a no-op.
- Four reconciliation checks pass against the populated database.
- **The raw market recorder is running daily** (§5.6), archiving option-chain WebSocket frames to disk from the day credentials land.

### Out of scope (deferred, with the phase that owns them)

| Deferred | Owner phase |
|---|---|
| Real-time streaming *for the UI*, stream gateway, Redis fan-out | Phase 1 |
| Order placement, fills, cost model, portfolio ledger | Phase 1 |
| **Parsing** recorded frames into `bars_intraday` / option-chain tables | Phase 1 — Phase 0 only captures (§5.6) |
| News / announcements recorder | Phase 1 |
| Agent Contract, sandbox, strategy runtime | Phase 2 |
| Intelligence layer, FinBERT, entity linking | Phase 2.5 |
| Backtest engine, metrics | Phase 3 |
| Any UI | Phase 1+ |

### Explicit non-goals

- No live order routing (parent plan §2, Finding 2 — load-bearing for the regulatory perimeter).
- No intraday bars in Phase 0. The `bars_intraday` hypertable is **created but left empty**, so Phase 1 needs no migration.
- No US or crypto ingestion in Phase 0. The source abstraction is designed to accept them without change.

---

## 2. Research findings that amend the parent plan

Two items were verified against official documentation on 2026-08-14 and **contradict the parent plan**. Both materially change Phase 0's assumptions and are recorded here as the authoritative version.

### 2.1 Upstox Expired Instruments APIs require a paid tier

Parent plan §3.1 hedged: *"One community thread associates expired-contract data with 'Upstox Plus,' so empirically verifying free-tier access is a week-1 Phase 0 task."*

**Verified:** Upstox's official announcement page states the four Expired Instruments APIs (Get Expiries, Get Expired Option Contracts, Get Expired Future Contracts, Get Expired Historical Candle Data) are available **within the Upstox Plus plan**, a premium tier.

**Consequence:** §12 Q3's "free three-layer stack" loses one layer. The remaining free layers are self-recording (future) and EOD F&O bhavcopy (past, 10y). Phase 0 is unaffected — it depends only on bhavcopy — but Phase 1's backfill strategy must be re-planned.

### 2.2 Dhan's strike window is narrower than documented in the plan

Parent plan §3.1 states the Dhan expired-options dataset covers *"ATM and ±10 strikes, for both index and stock options."*

**Verified:** Dhan's official API documentation states **ATM±10 only for index options near expiry**, and **ATM±3 for all other contracts**.

**Consequence:** The "relative-strike trap" described in §3.1 is significantly worse than planned for stock options. A ±3 window means a held position's strike exits the available data after roughly a 2% underlying move — i.e. routinely, and precisely when P&L matters. The `reconstructed` tagging policy and the P&L-disclosure requirement in §3.1 become **mandatory rather than prudent** for any stock-options backtest.

**Also confirmed:** Dhan's Data API subscription is ₹499 + tax/month. The expired-options documentation does not state whether it falls inside that subscription. This remains an open empirical question (§9.1) but does not block Phase 0.

### 2.3 Net effect on strategy

Self-recording is promoted from "guaranteed-free floor" to **the primary source of intraday options history**. This does not change Phase 0's contents, but it raises the priority of the recorder work at the start of Phase 1.

---

## 3. Decisions

Decisions taken during brainstorming on 2026-08-14, with rationale.

### 3.1 Confirmed from the parent plan

| # | Decision | Rationale |
|---|---|---|
| D1 | Phase 0 delivered literally as planned — schema-first, correctness over demo speed | Parent §10: mistakes in schema are expensive; front-load them |
| D2 | TimescaleDB as single source of truth; Redis alongside | Parent §4.2 |
| D3 | `user_id` multi-tenancy stays in the schema even at one user | Parent §12 Q1: costs nothing now, saves a migration later |

### 3.2 New decisions

| # | Decision | Rationale | Rejected alternative |
|---|---|---|---|
| D4 | **Colima + Docker Compose** for local Postgres/Redis | Local environment stays identical to the Phase 1 VPS deploy target | Homebrew-native Postgres — faster, but diverges from prod and the formula lags |
| D5 | **Surrogate `BIGSERIAL instrument_id`** + `UNIQUE` natural key | Narrow 8-byte FK across ~250M bar rows; rename-safe; one physical contract keeps one ID forever | Human-readable string PK — ~35 bytes × 250M rows, index bloat, convention changes become mass migrations |
| D6 | **`canonical_key` as a plain column** maintained by the resolver, `UNIQUE`-indexed — not a Postgres generated column | The natural expression needs `to_char`/date→text casts, which are `STABLE` not `IMMUTABLE`, and Postgres rejects those in generated columns. Application-side construction is deterministic and avoids the minefield | Generated column — cleaner in principle, fragile in practice |
| D7 | **Full breadth backfill:** NSE equity + NSE F&O + BSE equity + AMFI, ~10 years | User decision. Four parser variants is exactly the pressure that proves the abstraction | 3 years / NSE-only — faster, but too thin for credible equity backtests |
| D8 | **Approach A** — six typed pipeline stages, Polars inside bulk stages | Typed boundaries where bugs hide and subagents hand off; columnar speed where volume is | One-class-per-source (untestable without mocks); pure-dataframe (no typed boundaries) |
| D9 | **Separate `bars_daily` and `bars_intraday` hypertables** | Daily bars are *authoritative* bhavcopy settlement data, not a rollup. One table would let a continuous aggregate silently overwrite official settlement prices; and 1-day vs 1-min data need different chunk sizing | Single `bars` table with an `interval` column |
| D10 | **No `adjusted_close` column** — adjustment computed at read time | Parent §6: fills recorded at unadjusted actual prices. Storing adjusted prices means rewriting history on every corporate action | Materialised adjusted series |
| D11 | **`instrument_lot_history` as a temporal table** | Parent §6 requires "lot-size revisions as dated events". A scalar `lot_size` silently mis-sizes every backtest before the last revision | `lot_size` column on `instruments` |
| D12 | **NSE and BSE listings are separate `instruments` rows** sharing an ISIN | Fills, costs, and circuit limits are exchange-specific | One row per ISIN with an exchange array |
| D13 | **Python 3.12 via `uv`**, not system 3.14 | Ecosystem wheel maturity; discovering a missing wheel in month three is far worse than pinning now | System Python 3.14 |
| D14 | **Polars only, no pandas, in Phase 0** | Polars covers every Phase 0 need | Adding pandas pre-emptively |
| D15 | **Real trimmed exchange files committed as golden fixtures** | Hand-written fake CSVs test your own misunderstanding of the format. Public free data, private repo | Synthetic fixtures; network-dependent tests |
| D16 | **Raw market recorder included in Phase 0**, capturing to disk only | User decision, overruling initial scoping. Lost days of point-in-time history are unrecoverable, and finding §2.3 makes self-recording the primary intraday options source rather than a backup. Coupling is near-zero, so it does not slow the rest of Phase 0 | Deferring to Phase 1 — cleaner, but the clock runs the whole time |
| D17 | **Recorder runs on Oracle Cloud Always Free** (Ampere A1, aarch64) | Resolves O4. True 24/7 uptime at zero cost, and large enough (4 cores / 24 GB / 200 GB) to also host TimescaleDB in Phase 1. Critically, **the dev Mac is Apple M5 / arm64 and Ampere A1 is aarch64 — the same architecture**, so Docker images run identically in both places with no cross-building or emulation | MacBook + launchd (loses any day the lid closes); home Pi (hardware-dependent); Mac-now-migrate-later (early gaps are permanent) |

---

## 4. Schema

Migrations are managed by Alembic. All DDL below is the target state.

### 4.1 Instrument master

```sql
CREATE TABLE instruments (
    instrument_id   BIGSERIAL PRIMARY KEY,
    asset_class     TEXT        NOT NULL,   -- EQUITY|INDEX|FUTURE|OPTION|MF|CRYPTO|COMMODITY
    exchange        TEXT        NOT NULL,   -- NSE|BSE|MCX|BINANCE|NASDAQ
    segment         TEXT        NOT NULL,   -- CM|FO|MF|CD|COM
    symbol          TEXT        NOT NULL,
    underlying_id   BIGINT      REFERENCES instruments(instrument_id),
    expiry          DATE,
    strike          NUMERIC(18,4),
    option_type     TEXT,                   -- CE|PE|NULL
    tick_size       NUMERIC(12,6),
    currency        TEXT        NOT NULL DEFAULT 'INR',
    isin            TEXT,
    name            TEXT,
    listed_on       DATE,                   -- survivorship-bias defence
    delisted_on     DATE,                   -- delisted rows are never deleted
    status          TEXT        NOT NULL,   -- ACTIVE|EXPIRED|DELISTED|SUSPENDED
    source_bindings JSONB       NOT NULL DEFAULT '{}'::jsonb,
    canonical_key   TEXT        NOT NULL,   -- D6: app-maintained
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_instrument_natural
        UNIQUE NULLS NOT DISTINCT (exchange, segment, symbol, expiry, strike, option_type),
    CONSTRAINT uq_instrument_canonical UNIQUE (canonical_key),
    CONSTRAINT ck_option_fields CHECK (
        (asset_class = 'OPTION') = (option_type IS NOT NULL AND strike IS NOT NULL)
    ),
    CONSTRAINT ck_derivative_expiry CHECK (
        (asset_class IN ('OPTION','FUTURE')) = (expiry IS NOT NULL)
    )
);

CREATE INDEX ix_instruments_symbol   ON instruments (exchange, segment, symbol);
CREATE INDEX ix_instruments_underlying ON instruments (underlying_id) WHERE underlying_id IS NOT NULL;
CREATE INDEX ix_instruments_expiry   ON instruments (expiry) WHERE expiry IS NOT NULL;
CREATE INDEX ix_instruments_isin     ON instruments (isin) WHERE isin IS NOT NULL;
```

`UNIQUE NULLS NOT DISTINCT` (PG15+) is essential: without it, every equity row (which has `NULL` expiry/strike/option_type) would be considered distinct from every other, and the natural key would enforce nothing.

**`canonical_key` format:** `EXCHANGE:SEGMENT:SYMBOL[:YYYY-MM-DD][:STRIKE][:CE|PE]`, e.g.
`NSE:CM:RELIANCE` · `NSE:FO:NIFTY:2026-08-27:24500:CE`. Strike is rendered with trailing zeros stripped to a fixed canonical form so the same contract never produces two keys.

### 4.2 Temporal attributes

```sql
CREATE TABLE instrument_lot_history (
    instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id),
    effective_from DATE   NOT NULL,
    effective_to   DATE,               -- NULL = current
    lot_size       INTEGER NOT NULL CHECK (lot_size > 0),
    source         TEXT   NOT NULL,
    PRIMARY KEY (instrument_id, effective_from),
    CONSTRAINT ck_lot_range CHECK (effective_to IS NULL OR effective_to > effective_from)
);
```

Point-in-time lookup: the lot size for `instrument_id` on date `d` is the row where
`effective_from <= d AND (effective_to IS NULL OR d < effective_to)`.

### 4.3 Corporate actions

```sql
CREATE TABLE corporate_actions (
    action_id     BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    action_type   TEXT   NOT NULL,   -- SPLIT|BONUS|DIVIDEND|RIGHTS|MERGER|DEMERGER|SYMBOL_CHANGE|FACE_VALUE_CHANGE
    ex_date       DATE   NOT NULL,
    record_date   DATE,
    ratio_from    NUMERIC(18,6),     -- SPLIT 1:5 => from=1, to=5
    ratio_to      NUMERIC(18,6),
    amount        NUMERIC(18,4),     -- DIVIDEND per share
    new_symbol    TEXT,              -- SYMBOL_CHANGE
    announced_at  TIMESTAMPTZ,       -- point-in-time: when WE learned it
    source        TEXT   NOT NULL,
    raw           JSONB,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- NOTE: this must be a unique INDEX, not a table-level UNIQUE constraint.
-- Postgres does not permit expressions in UNIQUE constraints, only in indexes.
CREATE UNIQUE INDEX uq_corp_action ON corporate_actions
    (instrument_id, action_type, ex_date, (COALESCE(ratio_to, amount, 0)));

CREATE INDEX ix_corp_actions_lookup ON corporate_actions (instrument_id, ex_date);
```

`announced_at` applies the same point-in-time discipline parent §7.3 demands for news: a backtest must not know about a split before it was announced.

**Adjustment factor (read time, D10):** for a query as of date `d`, the cumulative factor applied to a bar at date `t < d` is the product of all `ratio_to/ratio_from` for actions with `ex_date` in `(t, d]`. Dividends adjust additively when total-return series are requested; the default is price-return (no dividend adjustment), stated explicitly in the API.

### 4.4 Trading calendar

```sql
CREATE TABLE trading_calendar (
    exchange       TEXT NOT NULL,
    segment        TEXT NOT NULL,
    session_date   DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    session_open   TIME,
    session_close  TIME,
    note           TEXT,             -- 'Diwali Muhurat', 'Unscheduled closure'
    PRIMARY KEY (exchange, segment, session_date)
);
```

This is what lets the ledger distinguish *"we missed a day"* from *"it was a holiday."* Without it, gap detection is guesswork. It is a hard dependency of the `BackfillRunner`.

### 4.5 Price hypertables

```sql
CREATE TABLE bars_daily (
    instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    ts            TIMESTAMPTZ NOT NULL,   -- bar CLOSE time (parent §6)
    open          NUMERIC(18,4) NOT NULL,
    high          NUMERIC(18,4) NOT NULL,
    low           NUMERIC(18,4) NOT NULL,
    close         NUMERIC(18,4) NOT NULL,
    prev_close    NUMERIC(18,4),
    volume        BIGINT,
    turnover      NUMERIC(22,4),
    trades        INTEGER,
    -- F&O only
    settle_price  NUMERIC(18,4),
    open_interest BIGINT,
    oi_change     BIGINT,
    -- equity only (arrives from a separate NSE file, lands via upsert)
    delivery_qty  BIGINT,
    delivery_pct  NUMERIC(7,4),
    extra         JSONB,
    source        SMALLINT    NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, ts),
    CONSTRAINT ck_ohlc_order CHECK (high >= low AND high >= open AND high >= close
                                    AND low <= open AND low <= close)
);

SELECT create_hypertable('bars_daily', 'ts', chunk_time_interval => INTERVAL '1 month');

ALTER TABLE bars_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_id',
    timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('bars_daily', INTERVAL '3 months');
```

`bars_intraday` has the same shape with `chunk_time_interval => INTERVAL '1 day'` and an added `interval_sec SMALLINT` in its primary key. **Created empty in Phase 0** so Phase 1 requires no migration.

The `ck_ohlc_order` CHECK is deliberately in the database, not only in the validator. Any future write path — a Phase 1 recorder, a manual backfill — inherits the invariant for free.

**`source` provenance codes.** `source SMALLINT` references a small `data_sources` lookup table (`source_id`, `source_key`, `description`), seeded by migration and mirrored by a Python `IntEnum` kept in sync by a test. A smallint rather than a text column because it is repeated across ~250M rows; a lookup table rather than a bare enum because Phase 1 adds broker sources without a schema change.

### 4.6 Operational tables

```sql
CREATE TABLE ingest_jobs (
    job_id          BIGSERIAL PRIMARY KEY,
    source_key      TEXT NOT NULL,      -- 'nse_eq_bhavcopy'
    business_date   DATE NOT NULL,
    status          TEXT NOT NULL,      -- PENDING|RUNNING|SUCCESS|FAILED|SKIPPED_HOLIDAY|SKIPPED_NO_DATA
    attempt         INTEGER NOT NULL DEFAULT 0,
    rows_written    BIGINT,
    quarantine_count INTEGER NOT NULL DEFAULT 0,
    content_hash    TEXT,               -- detects source restatements
    archive_path    TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           TEXT,
    CONSTRAINT uq_ingest_job UNIQUE (source_key, business_date)
);

CREATE TABLE quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    job_id        BIGINT NOT NULL REFERENCES ingest_jobs(job_id),
    reason        TEXT   NOT NULL,
    row_payload   JSONB  NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (            -- empty in Phase 0; D3
    user_id    BIGSERIAL PRIMARY KEY,
    email      TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**`raw_archive`** is the filesystem, not a table: fetched files are written to
`data/raw/{source_key}/{YYYY}/{MM}/{business_date}.{ext}` and referenced from `ingest_jobs.archive_path` + `content_hash`. When a parser bug surfaces in month four, we re-parse from the archive instead of re-downloading 2,500 files. Estimated a few GB — cheap on disk, painful as `bytea`.

---

## 5. Pipeline architecture

### 5.1 The six stage contracts

```python
class Source(Protocol):
    source_key: str
    def fetch(self, d: date) -> RawPayload | None:   # None => legitimately no data
        ...

class Parser(Protocol):
    def can_parse(self, p: RawPayload) -> bool:      # format-variant dispatch
        ...
    def parse(self, p: RawPayload) -> pl.DataFrame:  # source-shaped
        ...

class Normalizer(Protocol):
    def normalize(self, df: pl.DataFrame, p: RawPayload) -> NormalizedBatch:
        ...

class InstrumentResolver(Protocol):
    def resolve(self, refs: set[InstrumentRef]) -> dict[InstrumentRef, int]:
        ...

class Validator(Protocol):
    def validate(self, b: NormalizedBatch) -> ValidationOutcome:   # .valid / .quarantined
        ...

class Loader(Protocol):
    def load(self, o: ValidationOutcome, conn) -> LoadResult:
        ...
```

Every stage boundary carries a typed model (Pydantic v2 for scalars/metadata, Polars schema assertion for bulk frames). Inside `parse` and `normalize` it is Polars throughout.

Stages are pure with respect to each other: no stage knows its neighbours' types beyond the declared contract, and only `InstrumentResolver` and `Loader` touch the database.

**Why `can_parse` matters:** it is the mechanism that makes format variants additive. NSE-legacy, NSE-UDiFF, BSE, and AMFI each become one class plus one golden-file test, registered in an ordered list. Adding a fifth format touches no existing code.

### 5.2 Dataflow

```
                    BackfillRunner
                          │  missing days = trading_calendar ⊖ ingest_jobs
                          ▼
   ┌───────────── Pipeline.run(source, date) ─────────────┐
   │                 [ ONE transaction ]                  │
   │                                                      │
   │  Source.fetch ──► RawPayload ──► raw_archive         │
   │       │ None → SKIPPED_NO_DATA        (disk + hash)  │
   │       ▼                                              │
   │  ParserRegistry.select()  ◄── can_parse() dispatch   │
   │       ▼  pl.DataFrame                                │
   │  Normalizer ──► canonical cols + InstrumentRef       │
   │       ▼                                              │
   │  InstrumentResolver ──► bulk-upsert new instruments  │
   │       ▼                 attach instrument_id         │
   │  Validator ──┬─► valid ──────────────┐               │
   │              └─► bad ──► quarantine  │               │
   │                                      ▼               │
   │  Loader ─► COPY to staging ─► ON CONFLICT upsert     │
   │                                                      │
   │  ingest_jobs ← SUCCESS, rows, hash    [ COMMIT ]     │
   └──────────────────────────────────────────────────────┘
```

### 5.3 Idempotency model

Four mechanisms together guarantee *re-running any day produces identical state*:

1. **Job claim.** `INSERT … ON CONFLICT (source_key, business_date) DO UPDATE SET status='RUNNING', attempt=attempt+1 WHERE ingest_jobs.status IN ('PENDING','FAILED')`. Two runners cannot process the same day concurrently.
2. **Upsert loads.** `ON CONFLICT (instrument_id, ts) DO UPDATE`. Never a blind `INSERT`, so duplicates are structurally impossible.
3. **One transaction per (source, date).** Data and ledger commit together. A crash mid-day rolls back to a clean prior state.
4. **Content hash.** If the fetched file is byte-identical to a prior `SUCCESS` run, skip in milliseconds. If it differs, NSE has restated the file — re-parse, overwrite, and log at WARN.

**Loader performance:** `COPY` into an `UNLOGGED` staging table, then a single `INSERT … SELECT … ON CONFLICT DO UPDATE`. Row-by-row upsert of a 100k-row F&O day takes minutes; this takes roughly two seconds.

### 5.4 Error taxonomy

| Failure | Handling | Terminal job status |
|---|---|---|
| Network timeout, 5xx | Exponential backoff, 3 attempts | `FAILED` |
| HTTP 404 on a calendar trading day | **Loud** — likely a URL convention change | `FAILED` |
| HTTP 404 on a calendar holiday | Expected | `SKIPPED_HOLIDAY` |
| Empty file on a trading day | Suspicious, do not treat as success | `FAILED` |
| No registered parser returns `can_parse` | Keep raw, never guess | `FAILED` |
| Row violates an invariant | Row → `quarantine` with reason | `SUCCESS` + `quarantine_count > 0` |

Row-level failures must not fail the job: three bad rows out of 100k on one 2019 date must not block a 2,500-day backfill. They must, however, be visible — hence the counter on the job and a reconciliation check that surfaces any date with an unusual quarantine rate.

### 5.5 Instrument resolution

The resolver is the only stage that both reads and writes instrument state, and it auto-creates instruments it has not seen — necessary, because every trading day mints new option strikes.

Algorithm, per batch:
1. Collect the distinct set of `InstrumentRef` in the batch.
2. One query fetches existing `(canonical_key → instrument_id)` mappings.
3. Missing refs are bulk-inserted with `ON CONFLICT (canonical_key) DO NOTHING RETURNING`, then re-read to capture rows another transaction created.
4. An in-process bounded LRU cache carries mappings across days within a backfill run.

**Abort guard.** A parser typo would silently mint garbage instruments at scale. The resolver aborts the job if a single business date would create more than **5,000** new instruments. Real F&O days mint a few hundred; the first day of a backfill is exempted via an explicit `bootstrap=True` flag.

### 5.6 Raw market recorder

Included in Phase 0 by explicit decision (D16), overruling the initial scoping. Rationale: every day the recorder does not run is a day of point-in-time intraday option history that can never be recovered — and finding §2.3 promotes self-recording from backup plan to primary source.

**Governing principle: record raw, parse later.**

The recorder's only responsibility is durable, lossless capture of exactly the bytes the broker sent. It does not parse, normalise, resolve instruments, or touch the database. If capture-time interpretation is wrong, the data is lost forever; if raw frames are archived, they can be re-parsed indefinitely as understanding improves.

This gives the recorder near-zero coupling: it depends on no other Phase 0 component and can run standalone while the rest of Phase 0 is still under construction.

**What it captures.** Upstox market-feed WebSocket frames for the configured option-chain universe — NIFTY, BANKNIFTY, SENSEX, plus a configurable list of stock underlyings — for the full trading session.

**Storage layout:**

```
data/recordings/{source_key}/{YYYY-MM-DD}/{HH}.frames.gz     # raw frames, append-only
data/recordings/{source_key}/{YYYY-MM-DD}/session.json       # manifest
```

Files are append-only and gzip-compressed, rotated hourly so a crash costs at most the current hour's buffer.

**The session manifest is as important as the frames.** It records, as first-class events:

- the exact subscription list requested, and what the broker acknowledged
- every connect, disconnect, and reconnect with timestamps
- **explicit gap records** for each disconnected interval
- frame counts per hour and a terminal end-of-session summary

Without explicit gap records, a WebSocket drop from 11:32 to 11:35 is indistinguishable from three minutes in which no trades occurred. A future backtest would read the silence as market data. This is the single most important correctness property of the recorder, and it is why the manifest is not optional metadata.

**Failure posture.** The recorder favours capturing something over capturing perfectly: reconnect with backoff and keep going, never crash the session over a malformed frame, and record every anomaly to the manifest rather than to a log that nobody reads. A heartbeat file is touched every minute so an external check can detect a dead recorder the same day rather than a month later.

**Explicitly deferred to Phase 1:** a `RecordedFrameSource` implementing the §5.1 `Source` protocol, which reads the archive off disk and flows recordings through the same six pipeline stages as every other source. Phase 0 captures; Phase 1 ingests.

---

## 6. Testing strategy

### 6.1 Test layers

| Layer | What it covers | Speed | Network | DB |
|---|---|---|---|---|
| Contract suites | Every `Parser`/`Loader` implementation passes one shared parametrized suite | ms | No | No |
| Unit | Normalizers, validators, calendar math, adjustment factors, canonical-key construction | ms | No | No |
| Golden-file | Real trimmed exchange file → expected canonical output | ms | No | No |
| Integration | Pipeline end-to-end against real TimescaleDB | s | No | Yes |
| Idempotency | Same day run 3× → row-for-row identical state | s | No | Yes |
| Live smoke (`@pytest.mark.live`) | Source URLs still resolve | s | Yes | No |

Only the live smoke layer touches the network. It is excluded from the default `pytest` run and executed on a schedule, so that when NSE changes a URL convention we learn from one failing nightly job rather than from a mysteriously failing backfill.

Database tests run against the Docker Compose Timescale instance, with migrations applied once per session and **transaction-per-test rollback** so tests cannot pollute one another.

### 6.2 The parser contract suite

The highest-leverage test artifact in Phase 0. One parametrized suite that every registered parser must satisfy:

- `can_parse` returns `True` for its own fixture and `False` for every other parser's fixture (mutual exclusivity — this is what prevents silent misdispatch).
- `parse` returns a frame matching the declared source schema exactly.
- Parsing is pure: the same payload twice yields identical frames.
- An empty or truncated payload raises `ParseError`, never returns a partial frame.
- Numeric columns contain no nulls where the format guarantees values.

Four parsers written by four different subagents will otherwise drift in four directions. With a contract suite, consistency is enforced mechanically rather than by review diligence.

### 6.3 Fixtures

Real files are downloaded once, trimmed to ~50 representative rows (preserving header structure and at least one edge case per file: a suspended scrip, a deep-OTM option, a zero-volume row), and committed under `tests/fixtures/{source_key}/`.

Hand-written synthetic CSVs are prohibited for parser tests. They test the author's *understanding* of the format rather than the format, which is precisely how parser bugs survive to production.

### 6.4 TDD workflow with subagents

```
Opus writes:                        Sonnet subagent writes:
  • the stage Protocol                • the implementation
  • the contract test suite           • its own additional unit tests
  • the failing golden-file test      • until everything is green
  • the trimmed real fixture
```

Each subagent receives a self-contained brief: one interface, one failing test file, one fixture, and an explicit instruction not to modify the test file. It never needs to understand the resolver, the ledger, or the schema.

If a subagent cannot make the test pass without editing the test, that is treated as a signal that **the interface is wrong**, and it escalates rather than adapting the test.

---

## 7. Build order and delegation

| # | Work | Assignee | Depends on |
|---|---|---|---|
| 0 | Repo scaffold: `uv` + Python 3.12, `pyproject`, ruff/mypy/pytest config, `docker-compose.yml`, `.gitignore` (`.env.local` first) | Haiku | — |
| 1 | Alembic migrations for all §4 tables | Sonnet | 0 |
| 2 | **Domain models + six stage Protocols** | **Opus** | 0 |
| 3 | **Contract test suites** | **Opus** | 2 |
| 4 | Trading calendar + NSE/BSE holiday ingestion | Sonnet | 1, 2 |
| 5 | Pipeline runner, `ingest_jobs` ledger, `BackfillRunner` | Sonnet | 2, 3 |
| 6 | `InstrumentResolver` incl. abort guard | Sonnet | 1, 2 |
| 7a | Parser: NSE equity, legacy format | Sonnet (parallel) | 3, 4 |
| 7b | Parser: NSE equity, UDiFF format | Sonnet (parallel) | 3, 4 |
| 7c | Parser: NSE F&O | Sonnet (parallel) | 3, 4 |
| 7d | Parser: BSE equity | Sonnet (parallel) | 3, 4 |
| 7e | Parser: AMFI NAV | Sonnet (parallel) | 3, 4 |
| 8 | Corporate actions ingestion + read-time adjustment | Sonnet | 6 |
| 9 | Execute 10-year backfill + reconciliation | Opus drives | all |
| **R** | **Raw market recorder (§5.6) + scheduling** | Sonnet | 0, credentials |

**Step R runs on its own track.** It depends only on the repo scaffold and on broker credentials — not on the schema, the contracts, or any other step. It is scheduled **first among all delegated work** the moment credentials exist, because its value is a function of wall-clock days elapsed, not of engineering effort.

Steps **2 and 3 are the critical path** for everything else and are single-authored, because contract consistency is where correctness comes from. Nothing else parallelises until they exist. Step 7 is where delegation pays: five parsers, five briefs, one shared contract suite.

**Stack:** `uv` · Python 3.12 · Polars · Pydantic v2 · psycopg3 · httpx · Alembic · structlog · pytest · ruff · mypy (strict on the contracts module only).

---

## 8. Verification criteria

Phase 0 is complete when all of the following pass. "It ran without errors" is not a criterion.

1. **Calendar completeness.** Every `is_trading_day` row in `trading_calendar` has a terminal `SUCCESS` or explicitly-justified `SKIPPED_*` job for every active source. Zero unexplained gaps.
2. **Known-value spot checks.** A committed table of hand-verified values — e.g. RELIANCE close on 2020-03-23, NIFTY close on Budget day, a known NIFTY option settlement — asserted against the database.
3. **Cross-source agreement.** NIFTY spot from the index file and the underlying value carried in the F&O file agree within one tick on every shared date.
4. **Continuity.** No unexplained single-day price move beyond ±20% that is not matched by a corporate action. This is the check that actually catches split-adjustment bugs.
5. **Idempotency proof.** Re-running a randomly selected 30-day window changes zero rows (verified by comparing a checksum of the affected chunks before and after).
6. **Quarantine review.** Total quarantine rate below 0.01% of rows, and every distinct quarantine `reason` reviewed and either fixed or documented as expected.
7. **Recorder liveness.** For every trading day since credentials landed, a session manifest exists, its declared subscription list matches the configured universe, and total gap duration is under 1% of session length. Days that fail this are visible as an explicit report, never as silent absence.

---

## 9. Open questions and risks

### 9.1 Open — do not block Phase 0

| # | Question | Resolution path |
|---|---|---|
| O1 | Does Dhan's expired-options endpoint sit behind the ₹499/mo Data API subscription? | Empirical: hit `/v2/charts/rollingoption` once credentials exist |
| O2 | Given §2.1, what replaces Upstox expired-instruments in the Phase 1 backfill plan? | Re-plan at the start of Phase 1, informed by O1 |
| O3 | How far back do NSE/BSE archives remain reliably downloadable? | Discovered during backfill; the ledger records exactly where coverage ends |
| ~~O4~~ | ~~Where does the recorder run?~~ | **Resolved 2026-08-14 → D17: Oracle Cloud Always Free (Ampere A1).** Remaining sub-task: confirm ARM capacity is available in the chosen region at signup |

### 9.2 Risks

| Risk | Mitigation |
|---|---|
| NSE/BSE archive URLs change or rate-limit during a 2,500-day backfill | Raw archive means re-download is never needed twice; ledger makes the backfill resumable; polite fixed-delay throttling; live smoke test catches convention changes |
| Ten years spans more format variants than the four anticipated | `can_parse` dispatch makes a fifth variant additive; an unmatched payload fails loudly rather than being guessed at |
| Corporate action data is incomplete for older years, corrupting adjusted series | Verification check 4 surfaces this directly; unexplained jumps are quarantined for manual review rather than silently accepted |
| Parser typo mints garbage instruments at scale | Resolver abort guard (>5,000 new instruments/day) |
| Schema churn once Phase 1 begins | `bars_intraday` and `users` created empty now; `user_id` multi-tenancy retained; source abstraction isolates broker bindings in `source_bindings` JSONB |
| Solo bandwidth against MathWorks and Astro Acharya | Parallel parser delegation; each build-order step is independently mergeable and testable |
| **Recorder silently dies and nobody notices for weeks** — the highest-cost failure in Phase 0, since the loss is unrecoverable | Per-minute heartbeat file; session manifest written even on failure; verification check 7 reports gaps explicitly; daily summary surfaces a dead recorder same-day |
| **Recorder host sleeps or loses network**, losing whole trading days | Resolved by D17 — always-on cloud host rather than a laptop |
| Oracle reclaims an idle Always Free instance, or ARM capacity is unavailable at signup | The recorder keeps the instance genuinely non-idle; confirm capacity during signup; the recorder's zero coupling makes re-hosting cheap if it ever happens |

---

## 10. Amendments to the parent plan

The following should be reflected in `implementation-plan.md` when next revised:

- **§3.1 / §12 Q3** — Upstox Expired Instruments APIs are confirmed **Upstox Plus (paid)**, not free. The three-layer free stack is now two layers.
- **§3.1** — Dhan strike coverage is **ATM±10 for index options near expiry only; ATM±3 otherwise**, not ATM±10 for both index and stock options. The relative-strike reconstruction policy becomes mandatory for stock options.
- **§10 Phase 0** — the option-chain recorder stays in Phase 0 as the parent plan requires (D16), but in a deliberately reduced form: **capture to disk only, no parsing and no database writes**. Parsing recorded frames into the database moves to Phase 1. The news/announcements recorder moves to Phase 1 in full.
- **§3.1 / §7.3** — the parent plan's argument that recorders must start immediately is *strengthened* by finding §2.3, not weakened. With Upstox expired data paywalled and Dhan's window narrowed, self-recording is the primary intraday options source rather than a floor.
