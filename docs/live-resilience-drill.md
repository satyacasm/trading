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
curl -s http://localhost:8000/health | python3 -m json.tool
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
curl -s -X POST http://localhost:8000/strategies -H 'content-type: application/json' \
  -d '{"source": "<a strategy trading BTC-USDT 1m>"}'
# note the returned strategy_id, then:
curl -s -X POST "http://localhost:8000/strategies/<strategy_id>/live" \
  -H 'content-type: application/json' -d '{"portfolio_id": <a portfolio id>}'
# note the returned live_run_id
```

## 3. Verify it's actually running

```bash
curl -s "http://localhost:8000/live/<live_run_id>" | python3 -m json.tool
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
  The aggregator backfills 1m bars from Binance REST on three triggers --
  at startup, after 90s of silence, and via a 5-minute sweep over the
  last 30 minutes -- so the gap should close within a few minutes of
  reconnecting even without a process restart.
- The run received roughly 10 bars with `catchup = true`, in order
  (check `logs/live_supervisor.log` for `"live.order_refused"` entries
  with reason `"catch-up bar: price no longer tradeable"`, one per
  catch-up bar that tried to trade). The supervisor replays missed bars
  per run and flags each one as catch-up; the paper API separately
  refuses any MARKET order whose reference bar closed more than 180s
  ago, which is what actually blocks a catch-up bar from trading even if
  the supervisor's own flag were ignored. Replay is capped at 24h -- for
  a 10-minute outage this never engages, but if it ever does, the skip
  is logged as `live.replay_gap` and stored in `live_runs.last_gap_note`.
- No crash: `SELECT status, stopped_reason FROM live_runs WHERE
  live_run_id=<live_run_id>` still shows `RUNNING`.
- Orders resume being placed (not just refused) on the first live bar
  after the catch-up run ends.

Known limitation: ticks that arrive during the startup backfill are not
captured, so the first live minute after a restart may be built from
partial ticks rather than the full set.

## 6. Kill the supervisor mid-run

```bash
launchctl kickstart -k gui/$(id -u)/com.satyam.trading.live_supervisor
```

Expected: the run relaunches (a new container, same `live_run_id`),
`ctx.state` picks up where it left off (compare
`SELECT strategy_state FROM live_runs WHERE live_run_id=<live_run_id>`
before and after), and no bar is duplicated or missing in
`live_run_cursors`. `ctx.state` (up to 64 KB) is what makes this
possible -- it survives the container relaunch; anything a strategy
needs remembered across a restart has to fit in that budget.

## 7. Restart Redis

```bash
docker restart trading_redis
```

Expected: bars and fills resume within about a minute, with no process
restart needed -- `GET /health` should show every component recovering
back to `true` without any launchd `KeepAlive` relaunch being triggered
(check `logs/*.log` for `resilient_pubsub.reconnecting` entries instead
of a fresh process-start banner).

## 8. Clean up

```bash
curl -s -X POST "http://localhost:8000/strategies/<strategy_id>/live/stop"
bash deploy/install-live-stack.sh uninstall   # only if this was a one-off drill
```
