# Design: charts + watchlist web UI

**Date:** 2026-08-24 · **Status:** approved, ready for implementation planning
**Parent:** implementation-plan.md §4.1 (Web App component), §10 Phase 1
("Streaming + manual paper trading — ...charts/watchlists UI...")

## What this is

The first user-facing sub-project of Phase 1. Every prior sub-project
(crypto-streaming, bar-aggregator, upstox-ingestion, upstox-intraday-backfill)
built backend data paths with no UI beyond a bare tick-printing smoke-test
page (`stream_gateway`'s `proof.html`). This sub-project adds a real Next.js
web app — a live-updating watchlist and a candlestick chart — the first
place a human actually looks at this platform's data instead of querying it
with SQL.

## Why now, and why this shape

Two independent data paths are fully live end to end and unused by anything
but SQL verification queries: crypto ticks (`crypto_ingestor` →
`bar_aggregator` → `bars_intraday`) and NSE equities (`upstox_ingestor` for
live ticks, `upstox_intraday_backfill` for ~4.6 years of 1-minute history,
also landing in `bars_intraday`). Both publish `Tick`-shaped JSON to the
same `ticks:{instrument_id}` Redis convention, and `stream_gateway`'s `/ws`
already fans those out generically — this sub-project is the first consumer
to actually make use of that generality across two asset classes at once,
rather than adding a new streaming mechanism.

No order-placement or portfolio UI is in scope — the simulation engine
those depend on hasn't been designed yet. This is a read-only market-data
surface: see live prices, see history, nothing else.

## Success criterion

Running `uvicorn trading.streaming.gateway:app` and `npm run dev` in `web/`
side by side, with `crypto_ingestor`/`upstox_ingestor`/`bar_aggregator`
running: the watchlist dashboard shows live-updating prices (color-flashing
on tick) for a mix of crypto and NSE equity symbols added via the UI, and
clicking into an instrument shows a candlestick chart with real history
across all five supported timeframes, its rightmost candle updating live
from ticks. Verified by manual browser testing against the real dev stack,
documented with a screenshot — the same evidentiary bar this project has
held every other live-verification task to (`demo-proof.png`,
`bar_aggregator`'s live-verification task, etc.).

## Explicitly out of scope (deferred, not forgotten)

- **Order placement, portfolio, any trading UI.** Depends on the
  simulation engine (§4.3 of the master plan), which is a separate,
  not-yet-designed sub-project.
- **Auth / multi-user.** V1 is single-user personal use per plan §12 Q1.
  No login screen, no per-user watchlists — one global watchlist.
- **Real NSE session-hours logic.** The "live vs. closed" indicator uses a
  generic recent-tick heuristic (see Components below), not a real
  exchange-calendar-aware open/closed check. `trading.calendar` currently
  tracks trading *days*, not intraday session times, and building that out
  is unrelated scope creep for a UI sub-project.
- **`docker-compose.yml` integration for `web/`.** Runs as a second
  ad-hoc dev process (`npm run dev`), the same informal way
  `crypto_ingestor`/`stream_gateway`/`upstox_ingestor` are run today.
  Containerizing the whole stack together is a later operational task, not
  a design concern here.
- **Frontend automated test framework.** See Testing below.
- **Replay-service integration.** The chart consumes only real live/
  historical data. Wiring the (not-yet-built) replay service into this UI
  is future work once that service exists.
- **US-feed / other asset classes.** This sub-project covers exactly the
  two asset classes with a fully live backend path today: crypto and NSE
  equities. Extending `/instruments` to a third source is additive, later
  work.

## Architecture

Two additions on top of existing infra — no new services, no new
processes beyond the two already run informally today (`gateway`,
ingestors):

```
                     ┌─────────────────────────────┐
                     │   web/ (Next.js, port 3000)  │
                     │  watchlist dashboard         │
                     │  /instrument/[id] chart page │
                     └──────▲───────────────▲────────┘
                REST (candles,│      WS (/ws, unchanged)
                watchlist,    │              │
                instruments)  │              │
                     ┌────────┴──────────────┴───────┐
                     │   gateway (FastAPI, port 8000) │
                     │   + market_data_api router      │
                     └────────┬────────────────────────┘
                              │
                     ┌────────▼────────┐
                     │  TimescaleDB     │
                     │  (bars_intraday, │
                     │   bars_daily,    │
                     │   watchlists new)│
                     └──────────────────┘
```

`gateway`'s existing `/ws` endpoint is unchanged — it already fans out
`ticks:*` generically regardless of asset class. This sub-project only adds
REST surface (candles, watchlist, a reshaped `/instruments`) and the
frontend that consumes all of it.

## Components

### Schema change: `watchlists` table

New migration `0005_watchlist.py`:

```sql
CREATE TABLE watchlists (
    instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    added_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

No `user_id` — V1 has exactly one implicit user and no auth (see Out of
scope), so a single global list is the honest shape, not a `user_id`
column that would carry a fake/sentinel value for the only row that will
ever exist. Adding a real `user_id` column later, when multi-user actually
exists, is a straightforward additive migration.

### `GET /instruments` — reshaped

Currently returns crypto-only `{symbol: instrument_id}` (`stream_gateway.py`
today). Reshapes to a combined list across both live asset classes:

```json
[{"instrument_id": 501, "symbol": "BTC-USDT", "asset_class": "CRYPTO", "exchange": "BINANCE"},
 {"instrument_id": 88,  "symbol": "RELIANCE", "asset_class": "EQUITY", "exchange": "NSE"}]
```

Implementation keeps the existing per-request-upsert simplification (the
current code's own comment already accepts this as fine for a
low-frequency endpoint): calls both `seed_crypto_instruments(conn)` and
`seed_upstox_instrument_keys(conn)` to get their respective id sets, then
issues one `SELECT instrument_id, symbol, asset_class, exchange FROM
instruments WHERE instrument_id = ANY(%s)` to decorate the combined ids
uniformly — necessary because the two seed functions return
differently-shaped keys (plain ticker symbol vs. Upstox's
`NSE_EQ|<isin>` instrument key) and can't simply be merged as dicts.

`proof.html` (the existing crypto-only smoke-test page) gets a one-line
patch to read the new response shape; it stays a crypto-only demo, not
folded into this sub-project's UI.

CORS middleware is added to `gateway.py`'s `app`, allowing
`http://localhost:3000` for local dev.

### `GET /candles/{instrument_id}?interval={1m|5m|15m|1h|1d}&limit=`

New router in `src/trading/streaming/market_data_api.py` (kept out of
`gateway.py` to keep that file from accumulating unrelated responsibilities
— the same boundary discipline this project has applied elsewhere).

- `interval` in `{1m, 5m, 15m, 1h}`: `time_bucket(interval, ts)` over
  `bars_intraday`, aggregating `first(open, ts)`, `max(high)`, `min(low)`,
  `last(close, ts)`, `sum(volume)`, filtered to the given
  `instrument_id`, most recent `limit` buckets (default e.g. 300),
  returned in chronological order.
- `interval = 1d`: the endpoint first looks up the instrument's
  `asset_class`. **Equities** read `bars_daily` directly — Phase 0's
  10-year bhavcopy backfill is strictly better history than anything
  derivable by bucketing 1-minute data, which for most equities only
  starts at the 2022-01 backfill floor or later. **Crypto** has no
  `bars_daily` rows at all (this sub-project's predecessor never wrote
  there — see `bar_aggregator`'s design), so `1d` for crypto instead
  buckets `bars_intraday` into daily candles, same mechanism as the other
  intervals. This branch is invisible to the frontend — same response
  shape either way.
- 404 if `instrument_id` doesn't exist; 400 for an `interval` outside the
  five supported values.

### `GET /watchlist`, `POST /watchlist`, `DELETE /watchlist/{instrument_id}`

Also in `market_data_api.py`.

- `GET`: one query joining `watchlists` → `instruments` →
  a `LEFT JOIN LATERAL` pulling each instrument's single most-recent
  `bars_intraday` row (`ORDER BY ts DESC LIMIT 1`), so the response
  already carries `last_price`/`last_ts` — avoiding an N-request
  fan-out from the frontend just to paint initial prices. Ordered by
  `added_at`.
- `POST {"instrument_id": ...}`: `INSERT ... ON CONFLICT (instrument_id)
  DO NOTHING`, always 200 — idempotent, not a 409, matching this being a
  personal tool where "add a symbol that's already there" is a no-op, not
  an error. 404 if the `instrument_id` doesn't exist in `instruments`.
- `DELETE /watchlist/{instrument_id}`: removes the row if present; 200
  either way (deleting something already gone is not an error here).

### Frontend (`web/`)

Next.js (App Router) + TypeScript + Tailwind + `lightweight-charts`
(TradingView's free charting library, per master plan §9).

- **`/` — watchlist dashboard.** Rows from `GET /watchlist` (price +
  `last_ts` present immediately, no blank-then-populate flash). Add a
  symbol via a search box against `GET /instruments` (client-side filter
  over the full list — small enough not to need server-side search).
  Remove via the `DELETE` endpoint.
- **`/instrument/[id]` — chart page.** `GET /candles` for history on
  mount and on every timeframe change; a timeframe selector for the five
  supported intervals; the rightmost candle updates live from ticks (see
  live-merge below).
- **Shared WS hook.** One `WebSocket` connection reused across the app,
  subscribing/unsubscribing as the visible instrument set changes
  (all watchlist ids on `/`, one id on the chart page). Wraps
  reconnect-with-backoff: a dashboard left open for hours across a
  laptop sleep or network blip must recover its subscription on its own,
  not go silently stale.
- **Live tick → candle merge (chart page only).** Ticks are not
  pre-bucketed by the backend for the live path — the frontend buckets
  each incoming `{instrument_id, ts, price, ...}` into the currently
  selected interval client-side: if `ts` falls within the already-open
  (rightmost) candle's bucket, that candle's `high`/`low`/`close`/volume
  update in place; if `ts` starts a new bucket, a new `open=high=low=
  close=price` candle is appended. This matches `lightweight-charts`'
  own `series.update()` semantics (same time = replace last bar, later
  time = append). Switching timeframe discards this in-progress state
  and re-fetches REST history fresh for the new interval, then
  resubscribes — simpler and more honest than trying to re-bucket
  client-side history that was fetched at a different granularity.
- **"Live" vs. "closed" badge (dashboard rows).** A row shows "live" if a
  WS tick for that instrument arrived within the last 90 seconds, else
  "last close as of `<last_ts>`". One generic heuristic covering both
  asset classes (~100ms tick cadence for crypto; dead outside NSE hours
  for equities) with no new exchange-hours logic — see Out of scope.
- **Error handling.** A failed `GET /candles` or `GET /watchlist` shows an
  inline error on that chart/row, not a full-page crash. WS disconnects
  are invisible to the user beyond a brief "reconnecting" state — no
  fake ticks are ever synthesized while disconnected.

## Testing

- **Backend:** pytest, the same `db_conn` + `TestClient` pattern
  `stream_gateway`'s existing tests already use. New coverage:
  - `/instruments` returns the combined, correctly-decorated list.
  - `/candles` bucketing correctness: seed known `bars_intraday` rows
    spanning multiple buckets, assert the returned OHLCV matches
    hand-computed expected values, for at least one sub-daily interval
    and both `1d` branches (equity-reads-`bars_daily`,
    crypto-buckets-`bars_intraday`).
  - `/candles` 404/400 error paths.
  - `/watchlist` CRUD: add, idempotent re-add, remove, 404 on an unknown
    `instrument_id`, and that `GET` returns correct `last_price`/`last_ts`
    for a seeded `bars_intraday` row.
- **Frontend:** no unit-test framework added in this sub-project. This
  project's established convention for every prior live-data
  sub-project (crypto-streaming's `proof.html` verification,
  bar-aggregator's live-accumulation check, the intraday backfill's
  verification queries) is manual, real-stack verification documented
  with evidence — a screenshot here, matching `demo-proof.png`'s
  precedent — not a mocked component-test suite standing in for actually
  looking at live data render correctly. The backend is where a real bug
  would hide (bucketing math, the `1d` branch, watchlist idempotency),
  and that's what gets automated pytest coverage; the frontend's
  correctness bar is "does it actually render right against the real
  running stack," which only a human eye on a real browser can confirm.

## Open questions for the implementation plan (not this design)

- Exact default `limit` for `/candles` (a few hundred bars is enough for
  a readable chart; the precise number is a tuning choice).
- Whether the frontend needs a loading skeleton vs. a blank flash on
  first paint — a UI-polish detail, not a design one.
- Task decomposition and ordering between backend (migration + API) and
  frontend work — the frontend can be scaffolded against the API's
  documented shape before the backend is fully wired, if that parallelizes
  better; that's an implementation-plan call, not a design one.
