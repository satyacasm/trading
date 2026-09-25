# Live resilience drill

Run this after every change to the live stack, and periodically
thereafter. It exercises design §9's "Live drill" end to end: a real
outage, a real supervisor restart, a real Redis restart -- nothing
simulated.

## 1. Start the stack

```bash
docker compose up -d timescaledb redis redis_test
bash deploy/install-live-stack.sh install
```

Verify every component is up:

```bash
curl -s http://127.0.0.1:8010/health | python3 -m json.tool
```

`"ok": true` and every component `true`. If any is `false`, check
`logs/<name>.log` before continuing.

A heartbeat only proves the *process* is alive, not that its loop is
making progress -- `GET /health`'s six `health:<name>` entries (30s TTL,
refreshed every 10s) can all read `true` while a loop is stuck. For bar
flow specifically, cross-check that the latest row in `bars_intraday` is
recent (step 4's query, with the time window narrowed to a minute or two).

## 2. Start a paper run

```bash
curl -s -X POST http://127.0.0.1:8010/strategies -H 'content-type: application/json' \
  -d '{"source": "<a strategy trading BTC-USDT 1m>"}'
# note the returned strategy_id, then:
curl -s -X POST "http://127.0.0.1:8010/strategies/<strategy_id>/live" \
  -H 'content-type: application/json' -d '{"portfolio_id": <a portfolio id>}'
# note the returned live_run_id
```

## 3. Verify it's actually running

```bash
curl -s "http://127.0.0.1:8010/live/<live_run_id>" | python3 -m json.tool
psql "$DATABASE_URL" -c \
  "SELECT instrument_id, last_ts FROM live_run_cursors WHERE live_run_id=<live_run_id>"
```

`bars_seen` should be climbing roughly once a minute.

## 4. Check for gaps in bars_intraday

```bash
psql "$DATABASE_URL" -c "
  SELECT instrument_id, ts, ts - lag(ts) OVER (PARTITION BY instrument_id ORDER BY ts) AS gap
  FROM bars_intraday
  WHERE interval_sec = 60 AND ts > now() - interval '2 hours'
  ORDER BY instrument_id, ts"
```

Every `gap` should read `00:01:00`. Anything wider is a hole.

## 5. The outage

Turn Wi-Fi off for 10 minutes, then on.

Expected, once Wi-Fi is back:
- `bars_intraday` has no gap across the outage (re-run step 4's query).
  The aggregator backfills 1m bars from Binance REST on four triggers --
  at startup, for the exact minute still open at that startup once its
  tick-built bar is discarded as incomplete, after 90s of silence, and
  via a 5-minute sweep over the last 30 minutes -- so the gap should
  close within a few minutes of reconnecting even without a process
  restart.
- The run received roughly 10 bars with `catchup = true`, in order.
  Check `logs/live_supervisor.log` for one `"live.catchup_refused"`
  entry per catch-up bar that tried to trade (`live_run_id`,
  `instrument_id`, `bar_ts`), and cross-check
  `SELECT orders_refused, last_refusal FROM live_runs WHERE
  live_run_id=<live_run_id>` -- `orders_refused` should be roughly 10
  higher than before the outage and `last_refusal` should read
  `"catch-up bar: price no longer tradeable"`. The supervisor's own
  `catchup` flag (recomputed at send time, not just once per pass) is
  the only thing enforcing this: the paper API's staleness guard checks
  the instrument's *latest* known bar, not each order's own reference
  bar, so once the feed is live again it does not by itself catch a
  catch-up order trading on an old bar. Replay is capped at 24h -- for a
  10-minute outage this never engages, but if it ever does, the skip is
  logged as `live.replay_gap` and stored in `live_runs.last_gap_note`.
- No crash: `SELECT status, stopped_reason FROM live_runs WHERE
  live_run_id=<live_run_id>` still shows `RUNNING`.
- Orders resume being placed (not just refused) on the first live bar
  after the catch-up run ends.

Known limitation: the bar during which Wi-Fi drops, and the first bar
after it returns, are built from whatever ticks this process actually
saw -- partial in both cases -- and are never corrected afterward: a
backfilled bar for the same minute is skipped (`ON CONFLICT DO
NOTHING`) if a tick-built row already exists. The first bar after
reconnection is traded on live, not just recorded.

## 6. Kill the supervisor mid-run

```bash
launchctl kickstart -k gui/$(id -u)/com.satyam.trading.live_supervisor
```

Expected: the run relaunches (a new container, same `live_run_id`),
`ctx.state` picks up where it left off (compare
`SELECT strategy_state FROM live_runs WHERE live_run_id=<live_run_id>`
before and after), and no bar was duplicated or skipped across the
relaunch. `live_run_cursors` only holds each instrument's *latest*
delivered `ts`, not a history, so check this instead: note
`bars_seen` just before the kill, then after the run has caught back up
compare its growth against the *sum, across every instrument the run
trades*, of the 1-minute bars actually in the window -- `bars_seen`
increments once per bar delivered, and a run with more than one
instrument would otherwise undercount against a single-instrument
query --

```bash
psql "$DATABASE_URL" -c \
  "SELECT count(*) FROM bars_intraday
   WHERE interval_sec = 60 AND ts > '<kill_time>'
     AND instrument_id IN (
       SELECT DISTINCT instrument_id FROM live_run_cursors
       WHERE live_run_id = <live_run_id>)"
```

should equal the increase in `SELECT bars_seen FROM live_runs WHERE
live_run_id=<live_run_id>`. `ctx.state` (up to 64 KB) is what makes the
relaunch clean -- it survives the container relaunch; anything a
strategy needs remembered across a restart has to fit in that budget.

Not drilled here, but worth knowing: this step kills the *supervisor*
process, which `reconcile()` (run by whatever relaunches the
supervisor) then converges back to what `live_runs` says should be
running. If the *strategy container* itself exits instead (a sandbox
VM or Docker hiccup, e.g. after the Mac sleeps) `reconcile()` marks
that run `CRASHED` -- a terminal status it never auto-relaunches from.
Recovering that run means starting it again via the API, by hand.

## 7. Restart Redis

```bash
docker restart trading_redis
```

Expected: bars and fills resume within about a minute, with no process
restart needed -- `GET /health` should show every component recovering
back to `true` without any launchd `KeepAlive` relaunch being triggered
(check `logs/*.log` for `resilient_pubsub.reconnecting` entries instead
of a fresh process-start banner).

Known limitation: the live supervisor's pubsub reconnect
(`SyncResilientPubSub`) is synchronous and blocks its single-threaded
loop for as long as Redis stays down -- `reconcile()`, the delivery
timer, and honouring `stop` all wait behind it too, not just the
`closed_bars:*` wake-up. Recovery is still bounded by the same 1s-30s
backoff (check `logs/live_supervisor.log` for
`resilient_pubsub.sync_disconnected`/`sync_resubscribe_failed`
entries), it just isn't concurrent with the loop's other work while it
runs. Since Redis is local (`trading_redis`, not reached over Wi-Fi), a
Wi-Fi outage never triggers this -- a Redis container restart (this
step) or a colima restart does.

## 8. Clean up

```bash
curl -s -X POST "http://127.0.0.1:8010/strategies/<strategy_id>/live/stop"
bash deploy/install-live-stack.sh uninstall   # only if this was a one-off drill
```
