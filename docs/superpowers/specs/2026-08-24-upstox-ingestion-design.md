# Design: Upstox real-time market-data ingestion

**Date:** 2026-08-24 · **Status:** approved, ready for implementation planning
**Parent:** implementation-plan.md §10 Phase 1 ("Streaming + manual paper
trading" — "Upstox real-time WebSocket ingestion for Indian equities + F&O
(personal mode)")

## What this is

The third sub-project of Phase 1's streaming layer, following
[`2026-08-24-crypto-streaming-design.md`](2026-08-24-crypto-streaming-design.md)
(the live pipeline shape: ingest → Redis → gateway) and
[`2026-08-24-bar-aggregator-design.md`](2026-08-24-bar-aggregator-design.md)
(persistence into `bars_intraday`). This sub-project adds a new feed
adapter — a `upstox_ingestor` process that authenticates with Upstox's V3
market-data feed, subscribes to a small curated NSE equity watchlist,
parses the binary protobuf frames into `Tick`s, and publishes them to the
same `ticks:{instrument_id}` Redis channels `crypto_ingestor` already
publishes to. `bar_aggregator` and `stream_gateway` require **zero code
changes** to also serve real NSE data — this sub-project is the direct
validation of the crypto design doc's original claim: "wiring in Upstox...
is mechanically the same pipeline with a different feed adapter, not a
redesign."

## Why now

Upstox's API reactivation (UDAPI100058), blocked since Phase 0, is
resolved as of this session: the user's Upstox account is verified and a
1-year-valid **Analytics Access Token** has been generated. This token
type is read-only, covers Market Data and Real-time & Streaming APIs
specifically, and — notably — **cannot place, modify, or cancel orders**.
That restriction is a non-issue for this platform: it is explicitly a
paper-trading platform (implementation-plan.md §1), where fills are
simulated against real market data rather than routed to a real broker.
The Analytics Token is therefore sufficient for everything this
sub-project (and, most likely, every future Upstox sub-project short of a
live-trading mode this project has never scoped) needs. It is a distinct
credential from `UPSTOX_ACCESS_TOKEN`, the daily-refreshing OAuth token
`trading.auth.upstox` already mints for order-placement-capable access —
that flow is untouched and reserved for if/when this project ever adds
real order routing.

## Reuse, not rebuild

Phase 0's `trading.recorder.upstox_ws` already contains a correctly-built,
never-live-tested `UpstoxFeed` Protocol and `LiveUpstoxFeed` implementation
(no valid token existed until this session): `authorize()` makes a REST
call to Upstox's `/v3/feed/market-data-feed/authorize` endpoint with a
Bearer token and receives a one-time WebSocket redirect URI;
`subscribe(instrument_keys)` sends a JSON text control message
(`{"guid", "method": "sub", "data": {"mode": "full", "instrumentKeys": [...]}}`)
over that connection; frame iteration and `aclose()` follow. `upstox_ingestor`
**imports and reuses this class directly** rather than duplicating it — the
only thing Phase 0's recorder never needed is *decoding* the frames (its
governing principle is "record raw, parse later"). This sub-project is
where the "parse later" finally happens, live.

## Success criterion

With `upstox_ingestor` and `bar_aggregator` both running during NSE market
hours (9:15–15:30 IST) against the real Upstox feed, real ticks for the
watchlist flow into Redis and `bar_aggregator` produces real
`bars_intraday` rows for those instruments — verified by SQL, the same
verification discipline `bar_aggregator`'s Task 4 and Phase 0's Task 17
both held themselves to. No new browser demo is required: `stream_gateway`'s
existing `psubscribe("ticks:*")` is already instrument-agnostic, so the
crypto proof page (or a WS client pointed at the same instrument_ids) may
show real NSE ticks live for free — a bonus to confirm if convenient, not
the success bar.

## Explicitly out of scope (deferred, not forgotten)

- **F&O (futures & options).** Strikes, expiries, lot sizes, and options'
  instrument-key format all add real complexity. This sub-project proves
  the pipe shape against equities first, the same way crypto proved itself
  against 3 pairs before widening to 25 — F&O is a later widening, not a
  day-one requirement.
- **Market-hours scheduling.** NSE trading hours are not 24/7 like crypto.
  This sub-project does not add automatic start/stop around session
  windows — `upstox_ingestor` is started and stopped manually, exactly how
  `crypto_ingestor` is operated today. Outside market hours, a connection
  drop or extended silence is expected, not a failure to alarm on; teaching
  the reconnect/backoff logic to distinguish "market closed" from "genuine
  outage" is a real fast-follow, not v1.
- **Order placement.** The Analytics Token cannot place orders, and this
  platform's simulation engine (a separate, later sub-project) fills orders
  against ticks/bars this pipeline produces — it never routes real orders
  to Upstox. Nothing here touches `trading.auth.upstox`'s existing
  OAuth flow.
- **Any UI/charting work.** Per the master plan, charts/watchlists are a
  separate, later sub-project.
- **The full NSE/BSE instrument universe.** Only the chosen watchlist gets
  `source_bindings` populated in this sub-project; the rest of Phase 0's
  ~thousands of instrument rows stay as they are.

## Architecture

A fourth process, coupled to the existing three only by Redis — extending,
not modifying, the crypto-streaming design's "processes coupled only by
Redis pub/sub" principle.

```
Upstox V3 market-data feed
   │  REST authorize() -> WS redirect URI -> WS connect
   │  subscribe(instrument_keys) -> binary protobuf trade/LTP frames
   ▼
upstox_ingestor (new worker process)
   │  reuses trading.recorder.upstox_ws.LiveUpstoxFeed for auth/subscribe/frames
   │  parse_upstox_frame(raw bytes, instrument_ids) -> Tick | None
   │  PUBLISH ticks:{instrument_id}  (same Redis convention as crypto_ingestor)
   ▼
Redis pub/sub  (unchanged: bar_aggregator, stream_gateway already consume this)
```

## Components

### Instrument-key seeding

Upstox's documented instrument-key convention for NSE equities is
`NSE_EQ|<ISIN>`. Phase 0 already stores real ISINs for every NSE equity
row in `instruments`. A new seed step populates `instruments.source_bindings`
(currently `'{}'::jsonb` for every row — never yet used) for the chosen
watchlist, deriving each instrument's Upstox key from its stored ISIN
rather than requiring a separate manual mapping table. **This derivation
needs live verification against the real feed before the parser's shape is
finalized** — the same "verify against a live source before committing"
discipline Phase 0 applied to every EOD source format. If Upstox's actual
key format for a watchlist instrument doesn't match the `NSE_EQ|<ISIN>`
assumption, that surfaces immediately as a subscribe-time or parse-time
failure, not a silent misattribution.

The watchlist itself: a small, explicit list of well-known, liquid NSE
equities (e.g. RELIANCE, TCS, INFY, HDFCBANK — final list decided at
implementation-plan time), mirroring `CRYPTO_PAIRS`'s shape and the same
"a short, named list we actually care about, not the whole universe"
philosophy from the crypto design doc.

### Protobuf schema and parser

Upstox publishes a `.proto` schema for its V3 market-data feed
(`MarketDataFeed.proto`). This sub-project vendors that schema and compiles
it to Python bindings **once, as a one-time generation step** (via
`protoc`/`grpcio-tools`), then commits the generated `_pb2.py` module to
the repo like any other source file — not a step every build or every
developer machine re-runs. This keeps `protoc` (a system binary, not a
Python package) out of the normal dev/CI dependency chain entirely; only
whoever regenerates the bindings after a schema change needs it installed,
the same way a handful of other ecosystems vendor generated code rather
than a build-time compile step. The hand-rolled parse function sits on top
of the generated bindings — matching the "no vendor WS/parsing SDK,
hand-roll the transport layer" pattern `crypto_ingestor`/`binance_feed.py`
and Phase 0's recorder already established (the official Upstox Python SDK
was explicitly considered and rejected for this reason).

`parse_upstox_frame(raw: bytes, instrument_ids: dict[str, int]) -> Tick | None`
mirrors `binance_feed.parse_trade_message`'s contract exactly: never
raises, returns `None` and logs a warning for anything unparseable or for
a tracked-but-not-subscribed instrument, decodes price/quantity as
`Decimal` (never float), and resolves the exchange-native identifier
(Upstox's `instrument_key`) to this codebase's own `instrument_id` via the
`instrument_ids` map built from `source_bindings` at ingestor startup —
same shape as `crypto_ingestor`'s lowercase-Binance-symbol map, different
key format.

### `upstox_ingestor` (new worker)

`src/trading/streaming/upstox_ingestor.py`, `python -m
trading.streaming.upstox_ingestor`, shaped like `crypto_ingestor.py`:

- Reads `settings.upstox_analytics_token` (new `Settings` field, from
  `UPSTOX_ANALYTICS_TOKEN`) — no OAuth dance needed, the Analytics Token
  is static for its full year of validity.
- Builds `LiveUpstoxFeed(access_token, instrument_keys)` from
  `trading.recorder.upstox_ws`, reusing Phase 0's proven auth/subscribe/
  frame-iteration code unchanged.
- Calls `feed.authorize()` then `feed.subscribe(instrument_keys)` before
  entering the frame loop — the one structural difference from
  `crypto_ingestor`'s loop, which needs neither (Binance's public stream
  has no auth step and bakes subscription into the connection URL).
- Exponential-backoff reconnect on any failure; never fatal, matching every
  other worker in this pipeline.
- A single malformed/unparseable frame is logged and skipped, never
  crashes the loop.
- Publishes exactly the same `Tick.model_dump_json()` shape to exactly the
  same `ticks:{instrument_id}` channel convention `crypto_ingestor` uses —
  this identity is what lets `bar_aggregator`/`stream_gateway` stay
  untouched.

## Testing

Follows this pipeline's existing conventions:

- `parse_upstox_frame`: tested against real (or realistically-shaped,
  vendored-fixture) protobuf-encoded sample frames, no live network in
  tests — the same no-live-network-except-`@pytest.mark.live` convention
  `binance_feed.py`'s tests already use. A `@pytest.mark.live` test
  (excluded from the default run) checks one real message's shape against
  what the parser assumes, matching `binance_feed.py`'s live shape-check
  test.
- `upstox_ingestor`'s loop: tested against a fake `UpstoxFeed` yielding
  scripted frames — same no-mocked-network approach the crypto ingestor
  and Phase 0's recorder both already use for their respective feeds.
- Real Redis for any Redis-touching test, never a mock — unchanged
  convention.

## Open questions for the implementation plan (not this design)

- The exact watchlist (final instrument list) — a small implementation
  detail, not an architectural one.
- Whether `source_bindings`' JSON shape needs a specific key name (e.g.
  `{"upstox": "NSE_EQ|INE..."}`) versus a flatter convention — an
  implementation-plan-level schema decision.
- Whether Upstox's protobuf message carries a single "trade" concept
  analogous to Binance's `@trade` stream, or separate LTP/quote/full-depth
  message types requiring the parser to filter for the right one — this is
  exactly the kind of thing that needs empirical verification against the
  real feed (per Upstox's own docs and a live probe) before the parser's
  shape is finalized, the same spirit as Phase 0's week-1 format checks.
