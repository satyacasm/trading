# Design: real-time crypto ingestion + stream gateway

**Date:** 2026-08-24 · **Status:** approved, ready for implementation planning
**Parent:** implementation-plan.md §10 Phase 1 ("Streaming + manual paper trading")

## What this is

The first sub-project of Phase 1. Phase 1 as scoped in the plan bundles
several fairly independent subsystems (real-time ingestion for three asset
classes, a stream gateway, a charts/watchlist UI, the paper-trading
simulation engine, a replay service, a trading journal + Telegram bot).
Rather than one spec for all of it, each gets its own design → plan → build
cycle. This is the first: prove the live-data pipeline shape end to end
against one source before adding the others.

## Why crypto first, not Upstox

Upstox reactivation (UDAPI100058) is still blocked as of this session.
Binance's WebSocket feed is free, genuinely real-time, and requires no
credentials — it lets the pipeline shape (ingest → normalize → fan out →
render) get built and proven now rather than waiting on an external
dependency. Once the shape is proven, wiring in Upstox for NSE/BSE
equities+F&O is mechanically the same pipeline with a different feed
adapter — not a redesign.

## Success criterion

A throwaway static HTML/JS page (no framework, no build step) opens a
WebSocket connection to the stream gateway, subscribes to BTC-USDT and
ETH-USDT, and shows their prices updating live as trades happen on Binance.
That's the finish line for this sub-project. The real charts/watchlist UI
is a separate, later sub-project — this page is disposable proof, not a
component to keep polishing.

## Explicitly out of scope (deferred, not forgotten)

- **Persistence.** No live ticks or bars get written to TimescaleDB in this
  sub-project. It is pure fan-out: ingest → Redis → WebSocket. Extending
  the Phase 0 self-recording pattern to crypto is a real follow-up, just
  not this one — keeping this sub-project to "does the pipe work" keeps it
  fast to build and verify.
- **Bar aggregation.** Binance's raw trade stream is used directly (one
  message per trade). Turning that into 1-minute (or any) candles for
  charting is downstream work belonging to the charts/watchlist sub-project.
- **Upstox / US delayed feed.** Same pipeline, different feed adapters,
  built once this shape is proven and (for Upstox) once reactivation lands.
- **Any browser polish.** The proof page is deliberately disposable.

## Architecture

Two processes, coupled only by Redis — matching implementation-plan.md
§4.1's component diagram (separate Ingestors and Stream Gateway boxes) and
using the Redis instance `docker-compose.yml` already provisions but has
never been used yet.

```
Binance WS (wss://stream.binance.com:9443)
   │  raw trade messages
   ▼
crypto_ingestor (worker process)
   │  parse -> resolve instrument_id -> build a Tick
   │  PUBLISH ticks:{instrument_id}  (Redis pub/sub)
   ▼
Redis pub/sub
   │  SUBSCRIBE ticks:{instrument_id}  (only while >=1 client wants it)
   ▼
stream_gateway (FastAPI, WebSocket endpoint /ws)
   │  forward the tick verbatim to every subscribed client
   ▼
browser client (static HTML/JS proof page)
```

## Components

### `Tick` contract

A new, minimal model — a trade tick, not a candle:

| field | type | notes |
|---|---|---|
| `instrument_id` | `int` | resolved against the instrument master, same identity every other table uses |
| `ts` | `datetime` (UTC) | trade time from the exchange |
| `price` | `Decimal` | |
| `quantity` | `Decimal` | trade size |
| `side` | `str \| None` | `"buy"`/`"sell"` if the feed provides it |

Lives alongside the existing contract models (`src/trading/contracts/`),
following the same typed-model convention as `CANONICAL_BAR_SCHEMA` and the
rest of the pipeline's contracts.

### Crypto instrument seed

Zero crypto instruments exist in the instrument master today. A small,
explicit seed (mirroring the existing `python -m trading.calendar.seed`
CLI shape) creates `CRYPTO`/`BINANCE` instrument rows for a short, named
list of pairs we actually care about (BTC-USDT, ETH-USDT, and a small
handful more) — not an attempt to mirror Binance's entire tradable
universe. Every `Tick` then carries a real `instrument_id`, consistent with
how `bars_daily`, `corporate_actions`, and everything else in this codebase
already identifies instruments — no parallel `(exchange, symbol)` identity
scheme.

### `crypto_ingestor` (worker)

`python -m trading.streaming.crypto_ingestor`, shaped like
`src/trading/recorder/upstox_ws.py`'s proven pattern (not its code — a
different wire protocol):

- A `BinanceFeed` Protocol supplies the connection, so tests drive the
  ingestor against a fake feed yielding canned messages — no real network
  in tests, same convention `UpstoxFeed` already established.
- Exponential-backoff reconnect on a dropped connection; never fatal.
- A single malformed/unexpected message is logged and skipped, never
  crashes the loop (same "don't die on one bad frame" principle as the
  recorder, even though this ingestor parses live rather than recording
  raw bytes).
- An in-memory `symbol -> instrument_id` map, loaded from the seed at
  startup, used to resolve each parsed trade into a `Tick` before
  publishing.
- Redis publish failures are logged and retried; they never take down the
  WebSocket connection to Binance.

### `stream_gateway` (FastAPI service)

`src/trading/streaming/gateway.py`, one WebSocket endpoint, `/ws`:

- A connected client sends `{"action": "subscribe", "instrument_id": N}`
  (and an analogous `"unsubscribe"`).
- The gateway maintains a Redis `SUBSCRIBE` for `ticks:{instrument_id}`
  only while at least one connected client wants it, and drops the Redis
  subscription the moment the last interested client disconnects or
  unsubscribes — no leaked subscriptions accumulating over a long-running
  gateway process.
- Every tick received from a subscribed Redis channel is forwarded
  verbatim (as JSON) to every browser client currently subscribed to that
  `instrument_id`.

## Testing

Follows this repo's existing conventions rather than introducing new ones:

- `crypto_ingestor`: tested against a fake `BinanceFeed` yielding canned
  messages, the same no-mocked-network approach `tests/recorder/` already
  uses for `UpstoxFeed`.
- `Tick`: a small contract/schema test, matching how `CANONICAL_BAR_SCHEMA`
  has its own.
- `stream_gateway`: tested against a **real Redis** instance (already in
  `docker-compose.yml`), not a mock — consistent with how `db_conn` tests
  run against a real, rolled-back Postgres transaction rather than a fake
  database throughout this codebase.

## Open questions for the implementation plan (not this design)

- Exact Binance stream endpoint(s)/subscription format for the chosen
  pairs (a live-verification task, same spirit as Phase 0's week-1
  empirical checks — Binance's public WS API is well-documented and
  unauthenticated, so this should be low-risk, but still worth confirming
  against the real feed before committing to a parser shape).
- Where the disposable proof page lives (a static file served by the
  gateway itself vs. opened directly from disk) — an implementation
  decision, not a design one.
