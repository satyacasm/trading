# Design: crypto tick persistence + intraday bar aggregation

**Date:** 2026-08-24 · **Status:** approved, ready for implementation planning
**Parent:** implementation-plan.md §4.4 ("the streaming layer... a crucial
extra: the replay service") and §10 Phase 1 ("Streaming + manual paper
trading")

## What this is

The second sub-project of Phase 1, following directly from
[`2026-08-24-crypto-streaming-design.md`](2026-08-24-crypto-streaming-design.md),
which explicitly deferred both persistence and bar aggregation as
out-of-scope follow-ups. This is that follow-up: a third process,
`bar_aggregator`, that subscribes to the ticks `crypto_ingestor` already
publishes to Redis, buckets them into 1-minute OHLCV bars, and writes
closed bars into `bars_intraday` — a table Phase 0 created empty
specifically so Phase 1 could start using it with no migration for the
table itself.

## Why this, not the replay service directly

The master plan's replay service (§4.4) replays a historical day "at
1x/10x/60x speed through the same streaming pipeline." Phase 0 only
backfilled *daily* bars (one OHLCV row per instrument per trading day) —
there is no intraday history for any instrument yet, crypto included.
Building the replay service before any intraday data exists would mean
either faking sub-bar movement (dishonest, and this project's correctness
doctrine explicitly rejects presenting synthetic data as real) or waiting
on Upstox to unblock (defeats the point of working around the Upstox
blocker at all). Aggregating the *already-live* Binance feed into real
1-minute bars gives the replay service honest intraday history to replay
against, with no new external dependency — and proves the tick→bar
aggregation path any future ingestor (Upstox, US feed) will also need.

## Success criterion

With `crypto_ingestor` and `bar_aggregator` both running against the live
Binance feed, `bars_intraday` accumulates real 1-minute bars for the
seeded pairs within a few minutes of continuous operation — verified by a
direct SQL query showing monotonically increasing bar counts with sane
OHLC values, the same verification discipline Phase 0's Task 17 backfill
and this sub-project's predecessor's Task 6 both held themselves to. No
UI is built or required to declare this done.

## Explicitly out of scope (deferred, not forgotten)

- **The replay service itself.** This sub-project only produces the data
  it needs. Replaying `bars_intraday` back through the streaming pipeline
  at controllable speed is the next sub-project, once this one has
  produced enough real bars to replay.
- **Raw tick archival.** No table for individual ticks exists in the
  schema (only `bars_daily`/`bars_intraday`), and this sub-project doesn't
  add one. Only aggregated bars are persisted; the underlying ticks remain
  transient (Redis pub/sub only), exactly as the crypto-streaming design
  already established.
- **Charts/watchlist UI.** A separate, later sub-project per the master
  plan. This sub-project's success criterion is a SQL query, not a chart.
- **Upstox / US feed aggregation.** The aggregator subscribes to the
  existing `ticks:*` channel convention generically — any future ingestor
  publishing `Tick`-shaped JSON on `ticks:{instrument_id}` gets aggregated
  for free, no code change here. Wiring up those ingestors themselves is
  separate, later work.
- **Configurable/multiple bar granularities.** `bars_intraday`'s primary
  key already includes `interval_sec`, so nothing here blocks adding a
  second granularity later — but this sub-project writes only 1-minute
  bars.

## Architecture

A third process, coupled to the existing two only by Redis — extending,
not modifying, the crypto-streaming design's "processes coupled only by
Redis pub/sub" principle.

```
crypto_ingestor (unchanged)
   │  PUBLISH ticks:{instrument_id}
   ▼
Redis pub/sub
   │  PSUBSCRIBE ticks:*  (same pattern-subscribe shape stream_gateway uses)
   ▼
bar_aggregator (new worker process)
   │  bucket ticks into 1-minute (instrument_id, minute) OHLCV in memory
   │  on minute rollover (or periodic safety-net flush):
   │  INSERT ... ON CONFLICT (instrument_id, ts, interval_sec) DO UPDATE
   ▼
bars_intraday (TimescaleDB, existing table)
```

`bar_aggregator` does not touch `stream_gateway` or the browser path at
all — it is a pure Redis consumer running alongside the existing two
processes, not in front of or behind them.

## Components

### Schema change: `bars_intraday.volume` widens to `NUMERIC(28,8)`

`bars_intraday.volume` is currently `BIGINT`. Crypto trade quantities are
fractional `Decimal`s (e.g., `0.01000000` BTC) and cannot be represented
in a `BIGINT` column. A new Alembic migration widens `bars_intraday.volume`
only to `NUMERIC(28,8)` (8 decimal places, matching Binance's own quantity
precision) — a backward-compatible, additive change since the table is
still empty. `bars_daily.volume` is deliberately left untouched: this
sub-project never writes to `bars_daily`, real NSE/BSE equity and F&O
volumes are always whole-unit integers, and that table is live,
already populated by Phase 0's backfill, and already covered by Phase 0's
closed-out reconcile/validation checks — widening it would be an unrelated
risk for no benefit this sub-project needs. `open`/`high`/`low`/`close`
stay `NUMERIC(18,4)`; every seeded pair's price today is comfortably
within 4-decimal precision. If a sub-cent-priced pair is ever added, that
precision limit would need revisiting — not a problem this sub-project
needs to solve.

### `DataSource.BINANCE_WS = 6`

Appended to the existing `DataSource` enum (`src/trading/contracts/enums.py`),
append-only per its own docstring. Used as `bars_intraday.source` for
every row this aggregator writes.

### `bar_aggregator` (new worker)

`src/trading/streaming/bar_aggregator.py`, `python -m
trading.streaming.bar_aggregator`:

- Subscribes via `pubsub.psubscribe("ticks:*")` on a real `redis.asyncio`
  connection — the same pattern-subscribe shape `stream_gateway` already
  uses, for the same reason (a single upfront subscription, no further
  Redis round trips, no fire-and-forget-ack race to reason about).
- Maintains an in-memory dict keyed by `(instrument_id, minute_bucket)`,
  where `minute_bucket` is the tick's `ts` truncated to the minute. Each
  incoming tick updates that bucket's `open` (first tick only),
  `high`/`low` (running max/min), `close` (last tick), `volume` (running
  sum of `quantity`), and `trades` (running count).
- Every other `bars_intraday` column is either not applicable to crypto
  spot pairs (`settle_price`, `open_interest`, `oi_change`, `delivery_qty`,
  `delivery_pct` — F&O/equity-only, already nullable) or intentionally left
  `NULL` for this first version (`prev_close`, `turnover`, `extra`) — none
  are load-bearing for this sub-project's success criterion, and adding
  them is a trivial follow-up once there's a consumer that needs them.
- **A bar is only ever written once its minute window has fully closed —
  never a partial, in-progress bar.** Detected two ways: (a) a tick
  arrives whose bucket is later than an instrument's currently-open
  bucket — the old bucket is closed and flushed; (b) a periodic safety-net
  timer (checks every few seconds for any open bucket whose window has
  elapsed with no new tick) — needed so a bar still gets written even if
  an instrument goes quiet right at a minute boundary, instead of waiting
  indefinitely for the next trade to trigger detection.
- On process shutdown (`SIGINT`/`SIGTERM`), any still-open bucket is
  **discarded, not force-flushed**. A bar written from an incomplete
  window would have a `close` that's just "whatever the last tick
  happened to be so far," not a true minute close — writing that would be
  a dishonest bar. A restart leaves a small last-partial-minute gap in
  `bars_intraday`, acceptable at this sub-project's proof-of-shape tier.
- Writes are per-closed-bar `INSERT ... ON CONFLICT (instrument_id, ts,
  interval_sec) DO UPDATE` — the same idempotent-upsert convention
  `seed_instruments.py` and the corpactions ingest already use, not the
  heavier staging-COPY loader (`loaders/bars.py`) built for large EOD
  batch backfills. One row at a time, in real time, is the right shape
  here.
- A malformed or unparseable message on `ticks:*` is logged and skipped,
  never fatal — same governing principle as every other consumer in this
  sub-project.

## Testing

Follows this project's existing streaming-test conventions exactly:

- Real Redis and real Postgres (`db_conn`), no mocks for infrastructure.
- Tests stay synchronous — `asyncio.run(...)` around the async aggregation
  loop under test, no `pytest-asyncio`.
- The minute-bucketing logic needs an **injectable clock**, the same seam
  pattern `crypto_ingestor`'s `sleep` parameter already establishes — tests
  drive bucket rollover by injecting timestamps/a fake clock rather than
  waiting on real wall-clock minutes.
- Coverage: a tick sequence within one minute produces one correct bar on
  rollover; a tick sequence spanning multiple minutes produces multiple
  correct bars; an instrument that goes quiet mid-minute still gets its
  bar flushed via the safety-net timer; a malformed message on `ticks:*`
  is skipped without crashing the loop; re-processing the same closed bar
  (e.g., after a restart) upserts rather than duplicating.

## Open questions for the implementation plan (not this design)

- Exact safety-net timer interval (a few seconds, per the design above,
  but the precise value is an implementation tuning choice, not a design
  one).
- Whether `bar_aggregator` needs its own `docker-compose.yml`/deploy entry
  or is run the same ad-hoc way `crypto_ingestor`/`stream_gateway` are
  today (both currently started manually, not containerized) — an
  operational detail, not a design one.
