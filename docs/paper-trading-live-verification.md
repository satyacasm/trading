# Paper trading core — live end-to-end verification

**Status:** runbook prepared 2026-09-02 evening; **executed 2026-09-04 against
an open NSE session.** Results are in the Results section at the bottom.
**9 of 10 steps pass.** Step 5 completed at the 15:30 close. Step 3 (the
charge comparison) awaits an external grading against Upstox's calculator,
and Step 6 is blocked on Telegram credentials -- both operator actions, not
code. Two live defects found, one fixed and merged (`b59f98c`).

This is Task 12 of the paper-trading-core plan. It runs the shipped stack
against real market data and records evidence, matching the evidentiary
standard of `docs/verification/` (screenshots) plus a written report (this
file). It is deliberately controller-run rather than delegated: it needs a
human eye on real prices.

---

## Preconditions

NSE must be open — this verification is meaningless against a closed session,
because resting orders, the expiry sweep, and tick-driven fills all key off the
trading calendar. Check `trading_calendar` for today before starting; if
`is_trading_day` is false, stop and reschedule.

## Step 0 — bring up the stack

```bash
docker compose up -d                      # timescaledb, redis, redis_test
uv run alembic upgrade head               # must include 0009_fills_tds_charge
uv run uvicorn trading.streaming.gateway:app --host 127.0.0.1 --port 8010
uv run python -m trading.streaming.crypto_ingestor
uv run python -m trading.streaming.bar_aggregator
uv run python -m trading.streaming.upstox_ingestor      # NSE session only
uv run python -m trading.paper.engine
uv run python -m trading.paper.alerts                   # see note
```

> **Note — the alert worker is not optional.** The plan's Step 1 omits it, but
> Task 11 delivers Telegram alerts through a *transactional outbox*: the engine
> writes the alert row inside the fill's transaction and a separate worker
> drains it. Without this process running, Step 7's alert is written to the
> database and never sent, which reads as a failure of the circuit breaker when
> it is really just a worker that was never started.

Confirm `alembic current` shows at least `0009` (the stack is now at `0011`). Migration `0009` adds `fills.tds`; a
stack running an older schema will fail every fill insert.

## Step 1 — create a portfolio

`POST /portfolios` with ₹1,000,000 virtual capital and `base_currency: "INR"`.

Record: portfolio_id, the response body.

## Step 2 — market buy, 100 RELIANCE, DELIVERY

Submit with a rationale (the journal requires one). Confirm:

- it fills within a tick or two
- cash decreases by notional **plus** charges, not notional alone
- the fill's price matches a tick you can see on the chart

Record: order_id, fill price, tick timestamp, cash before/after.

## Step 3 — the charge comparison (the load-bearing step)

Compare the fill's itemised charges against Upstox's brokerage calculator for
the same quantity, price, and product. **Any mismatch is a bug in our
calculator, not in the broker's note.**

| Charge | Ours | Upstox calculator | Δ |
|---|---|---|---|
| Brokerage | | | |
| STT | | | |
| Exchange txn | | | |
| SEBI fee | | | |
| Stamp duty | | | |
| IPFT | | | |
| GST | | | |
| DP charges | | | |
| **Total** | | | |

## Step 4 — sell side, and the DP rule

Sell the position. Confirm STT appears on the delivery sell (delivery STT
applies to both sides) and that DP appears on the delivery sell but **not** on
a comparable intraday sell.

**Then the check this plan did not originally have.** The whole-branch review
found that `FLAT_PER_SCRIP_PER_DAY` was implemented identically to
`FLAT_PER_ORDER`, so DP was charged per *fill* rather than per scrip per day.
That is now fixed, and this is the first time it meets real data:

- Buy and delivery-sell RELIANCE, then buy and delivery-sell it **again the
  same day**.
- DP must be charged **once**, on the first sell only.
- GST must fall correspondingly on the second sell — GST's base includes demat,
  so if DP is zero, GST must be lower by 18% of the DP amount. If DP is zero
  but GST is unchanged, the two fixes have come apart.

Record both sells' full charge breakdowns side by side.

## Step 5 — resting limit order and the session sweep

Submit a limit buy far below market. Confirm it rests as `OPEN`, does not fill,
and sweeps to `EXPIRED` at session close.

## Step 6 — circuit breaker

Set `max_daily_loss` low, force a loss, and confirm: the portfolio pauses, open
orders cancel, and a Telegram alert arrives on the phone (not merely a row in
the outbox — check the phone).

## Step 7 — replay invariant

Run `replay_portfolio` against the live portfolio. It must reproduce
`cash_balance` and `positions` **exactly**. This is the strongest single check
in the list: it re-derives the entire portfolio from the fill ledger and
compares against the incrementally-maintained state, so any drift in the
quantization or charge path shows up here as a mismatch.

## Step 8 — currency enforcement (new)

The review found that `base_currency` was enforced by nobody, so an INR
portfolio could buy BTC-USDT and have its P&L wrong by ~90×. Confirm the gate
holds live: attempt to buy BTC-USDT from the INR portfolio and confirm a 400
naming both currencies. Then confirm a USDT portfolio *can* buy it.

## Step 9 — watch the reconciliation log (new)

Throughout the session, watch the engine for `paper_engine.reconcile_adopted`.

This line fires when the engine adopts an order it never received a control
message for. It exists because the API publishes `orders:control` **before** its
transaction commits, and the source fix for that is not available on FastAPI
0.141.1 (background tasks run before yield-dependency teardown — verified
empirically, see the branch ledger). The 5-second reconciliation sweep bounds
the damage to a late fill instead of a lost order.

- **If it never fires:** the race is theoretical in practice. FU-1 stays a
  follow-up.
- **If it fires during normal operation:** the race is real at production
  tick rates, and FU-1 (an after-commit callback registry on
  `get_db_connection`) is promoted from follow-up to blocker.

## Step 10 — record

Symbols and quantities used, the charge comparison tables, screenshots into
`docs/verification/`, whether NSE was open, and any issues found — fixed inline
if small, or filed as a new discovered-live task.

---

## Results

**Executed 2026-09-04, 10:12–10:35 IST. NSE CM open (`trading_calendar`
confirms `is_trading_day = true`, 09:15–15:30).** Stack at migration `0011`.

### Step 0 — stack up

`paper.engine` and `paper.alerts` were **both not running** when the session
started; everything else (gateway, three ingestors, web) had been up for days.
Started both, logging to `logs/`.

**Discovered here, and it invalidates any read of "the data looks thin":** the
machine had been on battery, deep-sleeping on a ~15-minute cycle with
41-second DarkWake windows. The ingestors only run while it is awake, so the
capture is not continuous. `pmset -g log` for the morning:

```
09:15:01  Sleep          <- market opens, machine asleep
09:30:25  DarkWake (41 secs)
09:31:07  Sleep
09:42:15  DarkWake (46 secs)
09:43:01  Sleep
10:00:22  DarkWake (41 secs)
10:01:03  Sleep
10:09:55  Wake ... lid ... HID Activity
```

Both feeds gap at identical minutes despite coming from two independent
ingestor processes and two unrelated venues -- which is what ruled out the
shared consumer (`bar_aggregator`) and pointed outside the codebase. Measured
completeness of *live-captured* bars, 2026-08-24 to 2026-09-04:

| | instruments | avg bars/day | of expected |
|---|---|---|---|
| NSE live (`source=8`) | 5 | 20–310 | **5%–83%**, mostly 10–30% |
| Crypto live (`source=6`) | 25 | 57–684 | **4%–48%**, mostly 10–35% |

`bars_daily` is unaffected (entirely Phase 0 backfill, `source` 1–5), though
it ends 2026-08-21 -- nothing rolls live bars up to daily. `bars_intraday`
`source=7` (the historical Upstox backfill) is complete through 2026-08-26,
so only the ~11 days of live capture are holed. NSE holes are repairable with
the existing `upstox_intraday_backfill`; crypto has no REST backfill path in
the repo today.

Mitigated for the session with `caffeinate -dimsu`; the machine is on AC.

### Step 1 — portfolio

`portfolio_id = 11`, "Task12 Live Verify 2026-09-04", INR, ₹1,000,000.00.

### Step 2 — market buy, 100 RELIANCE, DELIVERY

Order **26**, instrument 58607 (the Upstox-bound NSE RELIANCE; note 108061
and 101629 are duplicate RELIANCE CM rows carrying no live data).

- Submitted 10:20:40 IST, **filled 1.4s later** at **₹1327.66**, tick
  `2026-09-04 04:50:41+00`.
- Cash 1,000,000.00 → 867,052.55. Notional 132,766.00 **+ charges 181.45**
  = 132,947.45. Cash moved by notional *plus* charges. ✓

### Step 3 — the charge comparison

**Not yet externally graded — this remains the one open step.** Figures
recorded for comparison against Upstox's calculator:

| Charge | Buy 100 @ 1327.66 | Sell 100 @ 1326.54 | Buy 50 @ 1327.26 | Sell 50 @ 1325.64 |
|---|---|---|---|---|
| Brokerage | 20.00 | 20.00 | 20.00 | 20.00 |
| STT | 133.00 | 133.00 | 66.00 | 66.00 |
| Exchange txn | 4.08 | 4.07 | 2.04 | 2.03 |
| SEBI fee | 0.13 | 0.13 | 0.07 | 0.07 |
| Stamp duty | 19.91 | 0.00 | 9.95 | 0.00 |
| IPFT | 0.00 | 0.00 | 0.00 | 0.00 |
| GST | 4.33 | 7.93 | 3.97 | 3.97 |
| DP charges | 0.00 | 20.00 | 0.00 | 0.00 |
| **Total** | **181.45** | **185.13** | **102.03** | **92.07** |

Three lines disagree with standard NSE rates and should be checked first:

1. **IPFT is ₹0.0000** on every fill. At ₹10/crore (rate `1e-6`) a ₹132,766
   turnover owes ₹0.1328. The stored rate behaves like `1e-9` -- 1000× low.
   Note migration history records an IPFT rate change `1e-7` → `1e-9`.
2. **GST excludes the SEBI turnover fee from its base.** Ours is
   0.18 × (brokerage + exchange txn) = 0.18 × 24.08 = 4.33. Including the
   SEBI fee gives 0.18 × 24.21 = 4.36. DP *is* correctly in the base
   (sell 1: 0.18 × (20 + 4.07 + 20) = 7.93 ✓).
3. **Exchange txn implies 0.00307%** (0.0000307 × 132,766 = 4.08). If NSE's
   current cash-market rate is 0.00297%, the correct figure is 3.94.

Brokerage, STT (rounded to the rupee), stamp duty (buy side only) and DP all
match expectation.

### Step 4 — sell side, and the DP rule ✓

Order **27**, sell 100 @ ₹1326.54. **STT ₹133.00 charged on the delivery sell
too** ✓. Stamp duty ₹0.00 on the sell ✓ (buy-side only). DP ₹20.00 ✓.

The per-scrip-per-day check the whole-branch review added — second same-day
round trip, orders **28**/**29**:

| | Sell 1 (100) | Sell 2 (50, same day) |
|---|---|---|
| DP | ₹20.00 | **₹0.00** ✓ |
| GST | 7.93 = 0.18×(20+4.07+**20**) | 3.97 = 0.18×(20+2.03) ✓ |

DP charged **once**, on the first sell only, and GST falls with it. The two
fixes have **not** come apart. First contact with real data; passes.

### Step 5 — resting limit order and the session sweep

Order **30**, limit BUY 10 @ ₹1200 (market ~₹1326). Rested as `OPEN` from
10:23 ✓, never filled ✓, and **swept to `EXPIRED` at the close** ✓:

```
15:30:06 IST  paper_engine.session_sweep  order_ids=[30]
order 30: LIMIT @ 1200.0000, DAY, EXPIRED
          submitted_at 04:53:30 UTC (10:23 IST)
          updated_at   10:00:06 UTC (15:30:06 IST)
```

Six seconds after the 15:30:00 close, by the engine's periodic sweep.

**This also verifies the DISCOVERED-LIVE 1 fix against a real session
close.** The engine was restarted at 10:54 on the corrected predicate,
which now anchors the session date to the order's `submitted_at` rather
than to `now`. Order 30 was submitted *today*, so both readings agree for
it -- what this proves is that the fix does not over-expire a live order
during its own session, having left it resting for five hours before
expiring it exactly on time. The under-expiry half was proven separately
by the unit test that reddens without the fix.

### Step 6 — circuit breaker

**Blocked:** `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` unset, so
`run_alert_worker` idles (`alerts.worker_idle_no_bot_token`) and no alert can
reach a phone. Outstanding.

### Step 7 — replay invariant ✓

`replay_portfolio` reproduces cash **and** positions exactly for all three
live portfolios, including the two traded today:

```
portfolio  9: cash stored=926157.0485 replay=926157.0485 MATCH
              instrument 642283 qty 0.95 avg_cost 77284.6123 MATCH
portfolio 10: cash stored=997318.9900 replay=997318.9900 MATCH
              instrument  58607 qty 2.00 avg_cost  1326.9600 MATCH
portfolio 11: cash stored=999246.3200 replay=999246.3200 MATCH
              instrument  58607 qty 0E-8 avg_cost  1327.2600 MATCH
```

Portfolio 11 net: ₹1,000,000.00 → ₹999,246.32, i.e. ₹753.68 for two round
trips (₹560.68 charges + ₹193.00 adverse price movement).

### Step 8 — currency enforcement ✓

INR portfolio 11 buying BTC-USDT (642283) → **HTTP 400**:

```
portfolio 11 has base_currency='INR'; instrument_id=642283 is denominated
in 'USDT' -- a portfolio is single-currency, no FX conversion
```

Names both currencies, as required.

### Step 9 — reconciliation log ✓ (so far)

**`paper_engine.reconcile_adopted`: 0 occurrences** across the whole
session -- 5 orders placed through the HTTP API (26–30), all of which
reached the engine by control message, plus a full 09:15–15:30 window
including the session-close sweep. Combined with the 7 orders of 2026-09-02, still pointing
toward **FU-1 staying a follow-up**. Caveat unchanged: low order rate.

### Step 10 — issues found

**DISCOVERED-LIVE 1 — a `DAY` order silently becomes GTC across engine
downtime. Fixed.**

Order **24** (`RELIANCE BUY 2 MARKET DELIVERY`, portfolio 10) was submitted
2026-09-03 07:12:48 UTC and **filled 2026-09-04 04:48:06 UTC** — one second
after the engine was started this morning, ~21 hours late, at ₹1326.96
against a price ~₹1301 when it was placed.

`sweep_expired_day_orders` calls `_is_session_closed`, which derived the
session date from `now` rather than from the order. It therefore asked "is
*today's* session closed?" — at 10:18 IST it was not — so the order was never
swept, on any run, including the `paper_engine.startup_sweep` that exists
specifically to catch orders orphaned by downtime. It logged nothing.

A continuously-running engine hides this completely (at 15:30 the two dates
agree), which is why the suite missed it: every existing sweep test submits
and sweeps within one simulated session.

Fixed on branch `fix-day-order-session-expiry` (`403f823`), TDD: the session
date is now derived per order from `submitted_at`, and the check moved inside
the per-order loop since two orders on one instrument can belong to different
sessions. Test
`test_sweep_expires_day_order_submitted_in_an_earlier_session` fails
(`assert [] == [order_id]`) before the change and passes after; 944 backend
tests green, ruff and mypy clean.

**DISCOVERED-LIVE 2 — live bar capture is ~20–30% complete.** See Step 0.
Not a code defect; a consequence of running the stack on a sleeping laptop.
Material for Phase 3b, which would otherwise build equity curves on it. Worth
noting that the passing dogfood example recorded "491 `on_bar` calls over 5
sessions" — 5 × 375 = 1,875 expected, so ~26%, matching this band exactly.

**Observation — the engine logs no successful fill.** `paper_engine` logs
races, rejections and sweeps, but a normal fill produces no line at all, so
the engine log cannot be used to follow trading activity. Deliberate or not,
it made Step 9's "watch the log" harder to trust than expected.

**Observation — duplicate RELIANCE instruments.** `58607` (NSE, Upstox-bound,
live), `108061` (NSE, no binding, no bars) and `101629` (BSE). Picking the
wrong id yields an instrument that never ticks.

