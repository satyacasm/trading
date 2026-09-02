# Paper trading core — live end-to-end verification

**Status:** runbook prepared 2026-09-02 evening; execution pending an open NSE
session (09:15–15:30 IST). Results are recorded in place, below each step.

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
uv run uvicorn trading.streaming.gateway:app --port 8000
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

Confirm `alembic current` shows `0009`. Migration `0009` adds `fills.tds`; a
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

_To be filled in during the session._
