# Design Spec: Upstox Intraday Candle Backfill

**Date:** 2026-08-24 · **Status:** Approved, ready for implementation plan

## 1. Problem

`bars_intraday` today only has 1-minute crypto bars, accumulating live since
`bar_aggregator` shipped a few hours ago (docs/superpowers/specs/
2026-08-24-bar-aggregator-design.md). For NSE equities, `upstox_ingestor`
(docs/superpowers/plans/2026-08-24-upstox-ingestion.md) will start
accumulating 1-minute bars live once it runs during market hours — but that
only builds history forward from whenever it's first run. Everything before
that point is a gap.

Upstox's REST historical-candle API can fill that gap for the same
5-symbol NSE watchlist (`RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, `ICICIBANK`
— `trading.streaming.seed_upstox_instruments.UPSTOX_WATCHLIST`), back to
whatever depth Upstox's API actually supports at 1-minute granularity. This
spec covers a one-shot backfill worker that closes that gap once; the live
ingestor is what keeps extending coverage forward after that.

## 2. Upstox V3 Historical Candle API (verified against current docs)

```
GET https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/1/{to_date}/{from_date}
Authorization: Bearer {access_token}
Accept: application/json
```

- `instrument_key`: e.g. `NSE_EQ|INE002A01018` — already populated in
  `instruments.source_bindings ->> 'upstox_instrument_key'` by Task 2 of the
  upstox-ingestion plan (`seed_upstox_instrument_keys()`).
- `to_date`/`from_date`: `YYYY-MM-DD`, inclusive.
- **1-minute candle data is available only from January 2022 onward** —
  this is a hard floor regardless of an instrument's actual listing date.
- **Each request returns at most ~1 month of 1-minute candles.** A
  multi-year backfill must walk the range in month-sized windows, one
  request per window per symbol.
- Response shape:
  ```json
  {"status": "success", "data": {"candles": [
    ["2024-01-02T09:15:00+05:30", 2456.5, 2460.0, 2455.0, 2458.25, 12345, 0],
    ...
  ]}}
  ```
  Candle array order: `[timestamp, open, high, low, close, volume, open_interest]`.
  Timestamp is an ISO-8601 string with the `+05:30` offset already applied
  (not epoch millis, unlike the WS feed's `ltt`) — convert via
  `datetime.fromisoformat(...).astimezone(UTC)`, not the epoch-ms pattern
  `upstox_feed.py` uses for the WS feed.

This is fetched-and-documented, not hand-verified end-to-end the way Task 1
vendored the protobuf schema — the implementation plan's first task
includes one `@pytest.mark.live` request against the real endpoint to
confirm this shape holds before the bulk backfill logic depends on it.

**Auth is a different token than today's WS work.** The WS market-data
feed's `authorize()` call takes `UPSTOX_ANALYTICS_TOKEN` (a long-lived
Analytics Access Token). This REST endpoint wants the OAuth
authorization-code-flow token that `trading.auth.upstox`
(`uv run python -m trading.auth.upstox`) already mints and writes to
`.env.local` as `UPSTOX_ACCESS_TOKEN` — built in Phase 0, currently read
directly from the environment by `trading.recorder.__main__`, not yet
exposed on `Settings`. This backfill adds
`Settings.upstox_access_token: str | None` for that env var, following the
same pattern Task 2 used for `upstox_analytics_token`. This token expires
daily; a full backfill run (~280 requests, see §4) comfortably finishes
within one day's token lifetime, so no token-refresh logic is in scope —
but see §5 for how an expired/invalid token is *detected* and handled
distinctly from a transient per-request failure.

## 3. Storage

Same `bars_intraday` table, same `(instrument_id, ts, interval_sec)`
primary key, same `ON CONFLICT ... DO UPDATE` upsert shape `bar_aggregator`
already uses — this keeps backfilled and live-streamed bars in one place,
queryable uniformly. `interval_sec` is `bar_aggregator.INTERVAL_SECONDS`
(60), imported rather than duplicated as a magic number.

New provenance value, appended per `contracts/enums.py`'s "append only,
never renumber" rule:

```python
class DataSource(IntEnum):
    ...
    BINANCE_WS = 6
    UPSTOX_HISTORICAL_CANDLE = 7
```

A migration seeds `data_sources` with `(7, 'UPSTOX_HISTORICAL_CANDLE')`,
mirroring migration `0003`'s seeding of `(6, 'BINANCE_WS')`.

**Deliberately not reusing `bar_aggregator.write_closed_bar`/`ClosedBar`/
`OpenBar`.** Those model *tick accumulation* — `OpenBar.trades` counts
individual ticks as they arrive, and `OpenBar.start()`/`update()` only make
sense fed one `Tick` at a time. A backfilled candle from the REST API
arrives pre-aggregated with no trade count at all. Force-fitting it through
`OpenBar` would mean either lying about `trades` (e.g. defaulting to 0,
which reads as "zero trades occurred" rather than "unknown") or widening
`OpenBar.trades` to `int | None` for the sake of a caller that never
accumulates anything — stretching a shipped, reviewed abstraction to fit a
shape it wasn't designed for. Instead, this module gets its own small
upsert function with the same SQL shape but `trades = NULL` (genuinely
unknown, not a claimed zero) and no `OpenBar` involved. Two independent,
narrow write paths into the same table — each honest about what it knows —
rather than one shared abstraction bent to cover both.

## 4. Module: `src/trading/streaming/upstox_intraday_backfill.py`

Synchronous throughout (`httpx.Client`, `psycopg.Connection`), matching
this repo's convention for one-shot batch/backfill scripts
(`seed_upstox_instruments.py`, `src/trading/loaders/bars.py`) rather than
the streaming subsystem's async convention — there's no concurrent I/O to
overlap here, just a sequential walk over month windows.

- **`month_windows(start: date, end: date) -> list[tuple[date, date]]`**
  — pure, no I/O. Splits `[start, end]` into calendar-month-aligned
  `(from_date, to_date)` pairs, each ≤ 1 month, matching the API's per-request
  cap. Easy to unit test exhaustively (partial first/last month, single-month
  range, `start > end`).

- **`BackfillCandle`** — a small frozen dataclass: `instrument_id: int`,
  `ts: datetime` (UTC), `open/high/low/close: Decimal`,
  `volume: Decimal`, `open_interest: int | None`. Decimal conversion
  follows the project-wide rule: `Decimal(str(value))`, never
  `Decimal(float_value)`.

- **`parse_candle_response(payload: dict, instrument_id: int) -> list[BackfillCandle]`**
  — pure. Parses the `data.candles` array per §2's row shape. Unlike
  `upstox_feed.py`'s per-frame "log and skip" philosophy for the WS
  firehose, a REST response is one deliberate, retryable request — a
  response that doesn't match the documented shape (missing `candles` key,
  wrong array arity) is a loud `ValueError`, not a silent skip, since it
  signals the API contract changed and every subsequent window would fail
  the same way.

- **`fetch_candles(client: httpx.Client, instrument_key: str, from_date: date, to_date: date, token: str) -> dict`**
  — thin wrapper issuing the GET, raising on non-2xx. Takes an injected
  `httpx.Client` so tests supply one built on `httpx.MockTransport` with
  canned JSON — no real network in the default test run, matching every
  other ingestion task's "no network in tests except `@pytest.mark.live`"
  rule.

- **`write_backfill_candle(conn: Connection, candle: BackfillCandle) -> None`**
  — the dedicated upsert from §3. Never commits, matching
  `write_closed_bar`'s convention (test-safe against `db_conn` rollback;
  production commits via an `autocommit=True` connection).

- **`backfill_symbol(conn, client, instrument_key, instrument_id, token, *, start=date(2022,1,1), end=None) -> BackfillReport`**
  — orchestrates one symbol: builds `month_windows(start, end or yesterday)`,
  fetches+parses+writes each window in order (oldest first), sleeping
  `REQUEST_DELAY_SECONDS` (0.3s — Upstox documents no rate limit for this
  endpoint, so this is a conservative, unverified guess flagged as such,
  matching this project's practice of naming an assumption rather than
  silently picking a number) between requests. Retries a failing window
  once after a 2-second backoff on a transient failure (timeout, 5xx);
  on a second failure, logs and records the window in the report's
  `skipped_windows`, then continues to the next window. **A 401/403 response
  aborts the whole backfill immediately** (raises, doesn't retry-then-skip)
  — an auth failure means every remaining request will fail the same way,
  so treating it like a transient per-window error would burn through
  dozens of retries and requests for no benefit; this is a categorically
  different failure the caller needs to see immediately, not a skip line
  buried in a summary.

- **`main()`** — CLI entry point (`uv run python -m
  trading.streaming.upstox_intraday_backfill`). Reads
  `Settings.upstox_access_token` (hard error with a clear message if unset,
  same style as `upstox_ingestor.main()`'s missing-token check), calls
  `seed_upstox_instrument_keys()` (idempotent, already built) to get the
  watchlist's instrument keys, runs `backfill_symbol()` for each, and prints
  a final summary: bars written per symbol, any skipped windows across all
  symbols. Exits nonzero if any window was skipped, so a wrapping script or
  a human can tell success from partial coverage at a glance. Re-running is
  always safe (idempotent upsert) and will only need to redo the windows
  that were skipped, since Upstox has no way to ask "just the gaps" —
  the full walk re-runs, but already-written rows are cheap no-op upserts.

## 5. Error handling summary

| Failure | Behavior |
|---|---|
| One month-window request times out / 5xx | Retry once after 2s, then skip + record, continue |
| 401/403 (bad/expired token) | Abort entire backfill immediately with a clear error |
| Response doesn't match documented shape | Hard `ValueError` from `parse_candle_response` — not caught per-window, since a shape change breaks every window identically |
| A single candle row has unparseable numeric/timestamp data | Not expected from a REST API the way malformed WS frames are; treated as part of "response doesn't match documented shape" above, not silently dropped |

## 6. Testing

- `month_windows`: pure unit tests, no fixtures needed.
- `parse_candle_response`: pure unit tests against canned JSON (valid
  multi-row response, empty `candles`, malformed shape raising).
- `fetch_candles`: tests inject an `httpx.Client(transport=httpx.MockTransport(...))`
  returning canned responses (success, 401, 500) — no real network.
- `write_backfill_candle`: real Postgres (`db_conn` fixture), asserts the
  row lands with `source = DataSource.UPSTOX_HISTORICAL_CANDLE` and
  `trades IS NULL`; a second call with the same `(instrument_id, ts)`
  confirms the upsert overwrites rather than duplicating.
- `backfill_symbol`: real Postgres + injected fake `httpx.Client`, covers
  the retry-then-skip path and the immediate-abort-on-401 path using
  scripted responses, same style as `upstox_ingestor`'s
  `ScriptedUpstoxFeed`.
- One `@pytest.mark.live` test: a single real request for one symbol, one
  day, confirming the response shape in §2 still holds. Excluded from the
  default run.

## 7. Out of scope

- Any symbol beyond the existing 5-symbol Upstox watchlist.
- Any granularity other than 1-minute.
- Scheduled/incremental re-runs to keep catching up (this is a one-shot
  gap-filler; the live `upstox_ingestor` is what extends coverage forward
  from whenever it's run).
- Token refresh mid-run.
- F&O options intraday history (that's Dhan's expired-options endpoint per
  plan §3.1 — a separate, not-yet-built sub-project).
