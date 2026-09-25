# Live stack resilience — surviving Wi-Fi outages and sleep on a local Mac

**Status:** design, approved 2026-09-25 · **Author:** Satyam + Claude

The live crypto paper-trading stack runs on a MacBook, lid open, on the
charger. A power cut takes the router down for 5-10 minutes while the Mac
stays up on battery. Today that ends a live run: bars stop, nothing recovers
the missing minutes, and several components go quietly dead. This spec makes
a live run survive that outage, a process restart, and a supervisor restart,
without anyone touching it.

Hosting it elsewhere was considered and rejected for now: Oracle's free tier
reclaims idle instances and was halved without notice in 2026, and
single-container hosts (Hugging Face Spaces and similar) cannot run the
supervisor, which launches a gVisor container per strategy.

---

## 1. What happens today in a 10-minute outage

Verified against the source 2026-09-25.

| # | Component | Behaviour | Evidence |
|---|---|---|---|
| 1 | `crypto_ingestor` | Reconnects with 1s→30s backoff, never exits. A dead socket is detected by `websockets`' default keepalive (ping 20s, timeout 20s). **Fine.** | `crypto_ingestor.py:79-83`, `binance_feed.py:97` |
| 2 | `bar_aggregator` | Builds bars from ticks only. No REST backfill for spot: the outage minutes are gone for good. | `bar_aggregator.py:126-137` |
| 3 | `bar_aggregator`, paper engine | `pubsub.listen()` returns normally on a Redis connection loss; the consumer task ends silently and bar writing stops with no error. | `bar_aggregator.py:484-518`, `paper/engine.py:1010-1022` |
| 4 | `bar_aggregator` | `_closed_through` is in memory only. After a restart a late tick reopens an already-announced minute and republishes it; with different values the strategy crashes `"arrived out of order"`. Likely cause of live run 1's crash. | `bar_aggregator.py:86-102`, `sandbox/trading/runtime/provider.py:122-133` |
| 5 | live supervisor | A Redis `ConnectionError` from `get_message` is unhandled and kills the process. | `live/supervisor.py:512-525` |
| 6 | live supervisor | `read_frames` uses a blocking `readline()`; its 30s deadline is only checked between reads, so a hung container freezes every run. | `live/supervisor.py:218-241` |
| 7 | paper API | `_require_sufficient_cash` and `_load_marks` price from the latest `bars_intraday` close with no freshness check. | `paper/api.py:271-296`, `paper/engine.py:259` |
| 8 | runtime | `ctx.state` is a plain dict, never saved, although `STRATEGY_CONTRACT.md:760` says it "is persisted". | `sandbox/trading/runtime/context.py:124` |
| 9 | deployment | Services start under `nohup`; no restart policy in `docker-compose.yml`; system sleep is 1 minute. The sandbox VM's `runsc` registration exists only inside the VM. | `pmset -g`, `docker-compose.yml` |

---

## 2. The approach: Redis is a notification, Postgres is the record

Bars are always read from Postgres. A `closed_bars:*` message only tells the
supervisor that new bars may exist. Each run keeps, per instrument, the
timestamp of the last bar it was sent, and on every notification (or timer)
the supervisor sends everything after that timestamp, oldest first.

This makes one mechanism cover every "deliver it late" case: a network
outage, a missed Redis message, an aggregator restart, and a supervisor
restart. Duplicates cannot occur because the timestamp only moves forward.

Rejected: republishing backfilled bars over pub/sub (a bar published while
the supervisor is down is lost) and moving to Redis Streams (touches gateway,
paper engine and supervisor, and duplicates what Postgres already holds).

---

## 3. Gap backfill

New module `src/trading/streaming/spot_backfill.py`, modelled on
`perp_backfill.py`.

- Fetches closed 1m klines from Binance `GET /api/v3/klines` for a
  `[since, until)` window per instrument. Klines include zero-trade minutes,
  so a filled window has no holes.
- Never returns the forming minute: `until` is clamped to the start of the
  current minute.
- Writes to `bars_intraday` with `ON CONFLICT (instrument_id, ts,
  interval_sec) DO NOTHING`. A tick-built bar is never overwritten, because
  a strategy may already have been sent it.
- After writing, publishes the usual `closed_bars:*` notification.

`bar_aggregator` runs backfill:

1. **On startup**, from each instrument's `max(ts)` to the startup cutoff.
2. **When ticks resume after more than 90 seconds of silence** for an
   instrument, over the silent window.
3. **On a 5-minute sweep** of the last 30 minutes, as a safety net.

On startup, `_closed_through` is seeded from `max(ts)` per instrument in
`bars_intraday`, so a late tick for an already-written minute is dropped
rather than republished (fixes §1 row 4).

A Binance REST failure is logged and retried on the next trigger; backfill
never blocks tick aggregation.

---

## 4. Per-run delivery position

New table `live_run_cursors`:

```sql
CREATE TABLE live_run_cursors (
    live_run_id   bigint      NOT NULL REFERENCES live_runs(live_run_id),
    instrument_id bigint      NOT NULL REFERENCES instruments(instrument_id),
    last_ts       timestamptz NOT NULL,
    PRIMARY KEY (live_run_id, instrument_id)
);
```

- **Per instrument, not per run.** A single run-wide position would skip
  ETH's 10:05 bar if BTC's 10:05 bar had already moved it to 10:05.
- **Initial value** is the run's `started_at` truncated to the minute, so a
  new run does not replay history.
- **Delivery.** On any `closed_bars:*` notification, and on a 30-second
  timer when none arrives, the supervisor selects bars for the run's
  universe with `ts > last_ts`, ordered by `ts`, and sends them one at a
  time.
- **Atomicity.** For each bar, `last_ts`, `live_runs.strategy_state` (§5),
  `bars_seen`, `orders_placed` and `orders_refused` are written in one
  transaction after the strategy's reply. A crash replays at most that one
  bar, identical in value, which the runtime already treats as a harmless
  redelivery (`provider.py:122-127`).
- **Catch-up flag.** A bar whose close (`ts + 60s`) is more than 2 minutes
  before the time it is sent is delivered with `catchup = true`.
- **Replay cap.** At most 24 hours is replayed. Older missing bars are
  skipped; the skip is logged and recorded on the run
  (`live_runs.last_gap_note`) so the run's own history shows the
  discontinuity.

---

## 5. Strategy contract changes

### 5.1 Catch-up bars

- The bar frame gains `catchup: bool`, exposed to strategies as
  `ctx.is_catchup`.
- Strategies receive catch-up bars as normal; indicators and `ctx.state`
  update on them.
- **Orders placed on a catch-up bar are refused by the supervisor**, not by
  the runtime: the container is untrusted, so the enforcing check sits
  outside it. Refusals count in `orders_refused` with `last_refusal =
  "catch-up bar: price no longer tradeable"`.
- Trading resumes on the first bar that is not catch-up.

### 5.2 `ctx.state` persisted

- After each bar the runner includes `ctx.state` in the `orders` frame.
- The supervisor stores it in new column `live_runs.strategy_state jsonb`,
  in the same transaction as the delivery position (§4).
- On relaunch, the supervisor passes the stored state in the container's
  init frame and the runtime restores it before the first bar.
- State that is not JSON-serialisable, or exceeds **64 KB** serialised,
  crashes the run on that bar with a message naming which limit was broken.
- **Lookback on relaunch** must be seeded exactly as a fresh start seeds it.
  First plan task: establish how a fresh start seeds lookback today and make
  relaunch use the same path.

### 5.3 Documentation

`docs/agent-contract/STRATEGY_CONTRACT.md` §4 (`ctx.state`) and the
supervisor-restart note near line 760 are updated: catch-up semantics,
`ctx.is_catchup`, the 64 KB limit, and that persistence is per bar.

---

## 6. Surviving Redis, Postgres and stuck containers

- **`src/trading/streaming/resilient_pubsub.py`**, async and sync variants.
  Subscribes (pattern or channel), yields messages, and treats both an
  exception and a `listen()` that returns as a disconnect: reconnect with
  1s→30s backoff, resubscribe, log each attempt. Used by `bar_aggregator`,
  the paper engine and the supervisor. Lost messages are harmless under §2.
- **Postgres reconnect.** A shared connection helper that detects a broken
  connection (`conn.closed` / `OperationalError`) and reconnects with the
  same backoff, replacing connections opened once at process start.
- **Reply timeout.** `read_frames` waits with `select()` on the container's
  stdout pipe against the deadline instead of a blocking `readline()`. After
  **30 seconds** without an `orders`/`error` frame the container is killed
  and the run marked `CRASHED` with `"no reply in 30s"`.
- **Stale-price guard.** `_require_sufficient_cash` and `_load_marks` treat a
  latest bar older than **3 minutes** as stale: a MARKET order is refused
  with `"reference price stale"`, and a stale mark is logged. Resting orders
  are unaffected; they only fill on live ticks.
- **Heartbeats.** Each long-running process sets `health:<name>` in Redis
  with a 30-second TTL, refreshed every 10 seconds. The gateway serves
  `GET /health` listing each component and whether its key is present. This
  route only reads (per the rule that GETs never write).

---

## 7. Keeping processes alive on the Mac

- **launchd agents** with `KeepAlive` and `RunAtLoad` for: gateway,
  crypto_ingestor, bar_aggregator, paper engine, paper alerts, live
  supervisor, perp_ingestor. Installed and removed by
  `deploy/install-live-stack.sh`, following
  `deploy/install-chain-recorder.sh`. Logs to `logs/<name>.log`. Start order
  is irrelevant because every service reconnects (§6).
- **Sleep.** The supervisor's agent runs under `caffeinate -i`, which blocks
  idle sleep on battery too. `-s` would release on battery, which is exactly
  the power-cut case.
- **Docker.** `restart: unless-stopped` on `timescaledb` and `redis` in
  `docker-compose.yml`. A login agent runs `colima start` for the `default`
  and `sandbox` profiles.
- **`deploy/provision-sandbox-vm.sh`**, idempotent: ensures the `sandbox`
  profile's `/etc/docker/daemon.json` registers `runsc` at
  `/usr/local/bin/runsc`, restarts dockerd if it changed, and verifies with
  `docker --context colima-sandbox info`.

---

## 8. Thresholds

| Name | Value | Where |
|---|---|---|
| Silence that triggers backfill | 90 s | §3 |
| Backfill sweep | every 5 min, last 30 min | §3 |
| Delivery timer | 30 s | §4 |
| Catch-up threshold | bar close > 2 min before delivery | §4 |
| Replay cap | 24 h | §4 |
| `ctx.state` size limit | 64 KB | §5.2 |
| Strategy reply timeout | 30 s | §6 |
| Stale reference price | 3 min | §6 |
| Heartbeat TTL / refresh | 30 s / 10 s | §6 |

All live in `trading.config` with these defaults.

---

## 9. Testing

TDD throughout. Tests use `redis_test` (port 6380), never the live instance.

**Unit**
- Backfill against a fake klines client: window clamped to the last closed
  minute, zero-trade minutes written, existing rows untouched.
- `_closed_through` seeded from the database; a late tick after restart is
  dropped, not republished.
- Cursors: late ETH bar still delivered; repeated notifications deliver
  once; a missed notification is recovered by the timer; crash between reply
  and commit replays exactly one identical bar.
- Catch-up flag boundaries; orders on catch-up bars refused and counted.
- `ctx.state` round trip; 64 KB and non-JSON failures crash with the named
  reason.
- `resilient_pubsub` against a fake that raises, and one whose `listen()`
  returns.
- `read_frames` against a fake process that writes nothing: returns at the
  deadline.
- Stale-price guard at 2m59s and 3m01s.

**Integration**
- Restart the `redis_test` container mid-stream: aggregator, paper engine and
  supervisor resume without a process restart.

**Live drill** (runbook `docs/live-resilience-drill.md`)
1. Start a paper run on BTC-USDT 1m.
2. Turn Wi-Fi off for 10 minutes, then on.
3. Expect: `bars_intraday` has no gap; the run received ~10 bars with
   `catchup = true` in order; no crash; orders resume on the first live bar.
4. `launchctl kickstart -k` the supervisor mid-run: the run relaunches with
   `ctx.state` intact and no duplicate or missing bar.
5. `docker restart trading_redis`: bars and fills resume within a minute.

---

## 10. Out of scope

- Telegram alerting (still needs `TELEGRAM_BOT_TOKEN` / `CHAT_ID`).
- Perpetuals live runs and the perp charge-schedule retry.
- Daily spot crypto bars for backtesting.
- Tick-level dispatch.
- Hosting off the Mac.
