# Where this project stands

**Updated:** 2026-09-03, ~01:15 IST. Keep this file current — it is the
first thing to read when picking the work back up.

---

## The one thing to do next

**Task 12: live end-to-end verification of paper trading.** It needs an open
NSE session — **09:15–15:30 IST**. Runbook: `docs/paper-trading-live-verification.md`
(untracked; decide whether to commit it with results filled in).

It is the last item on the 12-task paper-trading plan, and two of its steps
matter more than the rest:

- **Step 3 — the charge comparison.** Compare a real fill's itemised charges
  against Upstox's brokerage calculator, line by line. This is the *only*
  external grading the cost model gets. `replay_portfolio` re-derives cash from
  the **stored** `fills.total_charges`, so it proves ledger/cache consistency,
  not charge correctness — a fill written with a wrong charge replays to the
  same wrong balance and the invariant still passes. Write the recorded figures
  into `tests/paper/test_charges_golden.py`, which is currently failing on its
  `REPLACE ME` guard (8 tests, excluded by default via `-m 'not golden'`) and is
  exactly what those figures unblock.
- **Step 9 — watch for `paper_engine.reconcile_adopted`.** If it fires during
  normal operation, the `orders:control` publish/commit race is real at
  production tick rates and **FU-1 is promoted from follow-up to blocker**.
  Evidence so far: it did **not** fire across the seven orders placed on the
  night of 2026-09-02 (`grep -c reconcile_adopted` on the engine log: 0). Weak (low tick rate, crypto only), but pointing toward FU-1 staying a
  follow-up.

Note the UI now covers most of the runbook — create the INR portfolio from
`/portfolio`, place the RELIANCE order from the chart page, read the fill in
the blotter. Equity orders are refused before 09:15 by the market-hours check;
that is correct behaviour and the ticket will say so.

---

## Phase status (implementation-plan.md §10)

| Phase | State |
|---|---|
| **Phase 0** — foundations | Complete 2026-08-24. 51M bars, 44,341 corporate actions, `docs/phase-0-closeout.md`. |
| **Phase 1** — streaming + manual paper trading | Shipped, bar Task 12. Crypto streaming, bar aggregation, Upstox WS, charts/watchlist UI, paper-trading core, and the trading UI are all merged to `main`. |
| **Phase 2** — Agent Contract + strategy runtime | **Started.** Draft at `docs/agent-contract/STRATEGY_CONTRACT.md`. |
| **Phase 2.5** — intelligence layer | Not started. Recorders were meant to start in Phase 0 and compound; check whether the news/announcements recorder is actually running. |
| **Phase 3** — backtesting + metrics | Not started. Reuses `decide_fill` and the contract unchanged. |

---

## Recent merges on `main`

```
f49f069  Merge 'frontend-paper-trading': trade from the UI
ec6fa0c  Merge 'paper-trading-core': paper trading core (Phase 1)
```

Both feature branches (`paper-trading-core`, `frontend-paper-trading`) still
exist as local refs. Fully merged; safe to delete with `git branch -d`.

Test counts: **847 backend** (8 golden deselected), **36 frontend**. ruff,
mypy, eslint, tsc all clean.

---

## Phase 2 — where the contract draft stands

`docs/agent-contract/STRATEGY_CONTRACT.md` is at **v0.1 draft**. It is truthful
about the built platform (real field names, real enums, real charge figures
computed from the seeded schedules) and explicit that the runtime does not
exist.

**Five of the seven open decisions are settled** (D1, D2, D5, D6, D7 — each
with its reasoning in the contract's decisions table). Two remain:

- **D3 (sandbox limits)** — the *environment* is decided: gVisor runs inside
  **Docker Desktop's Linux VM** on this machine (gVisor is Linux-only and
  cannot run on macOS directly); a VPS is a scale question, not a correctness
  one. The specific limits, timeout, and import allowlist stay open until the
  sandbox is built.
- **D4 (worked examples)** — deliberately unwritten. An example in a contract
  is a promise the code runs; none can be executed until the runtime exists,
  and an agent copying a broken example produces broken strategies
  confidently.

Consequence of D6 worth remembering: **one strategy → one portfolio → one
currency**, so a single strategy cannot trade NSE equities and crypto together
in V1.

`schema.json` and `platform_sdk.py` are **written**, at
`src/trading/agent_contract/`. They live in the package rather than under
`docs/` so they fall under `mypy src` and the drift test — the schema's
enumerations are generated from `trading.paper.enums` and pinned by
`tests/agent_contract/test_contract_bundle.py`, so a value the schema accepts
is a value the order API accepts. Verified by mutation: appending a bogus
status to the schema fails the suite.

**Static validation is built** (`trading.agent_contract.validation`) — stage 1
of the §9 pipeline, plus stage 4's paste-back report. Manifest check against
the schema, import allowlist, AST scan for forbidden calls and escape-shaped
attribute access, wall-clock reads (a determinism rule, not a security one),
and structural checks. Stable finding codes so an agent can branch on them.

**Read the module docstring before extending it.** Static validation is *not*
the security boundary — an AST scan is bypassable by anyone trying, and
containment is the sandbox's job. It is a fast local filter for honest mistakes
in generated code. Both the module and the contract say so, and the "ACCEPTED"
report says so too, so nobody reads a pass as a proof of safety.

**Registration is built** (`trading.agent_contract.registry`, migration
`0010`) — §9 stage 3. Its load-bearing rule is that **a registered version is
immutable**: re-registering `(name, version)` with different source raises
`VersionConflict` rather than updating, because results already attributed to
that version must keep describing the code that produced them. Identical
source re-registers idempotently (a retry, not a change). Nothing that fails
static validation is stored, and the rejection carries the agent-facing report
so a caller can hand it straight back.

Next in Phase 2: **the sandbox** (gVisor in Docker Desktop's Linux VM). It is
now the single gate on the rest — §9 stage 2 (smoke run) and D4 (the worked
examples, which must be *executed* before publication) both wait on it. The
upload path is otherwise complete: validate → register.

**Acceptance bar** (plan §10): the contract is not done until *three different
frontier agents*, each given only that file, each produce a working strategy
first-try.

Useful sequencing thought: the contract-and-schema half is separable from the
sandbox half. Drafting and dogfooding the contract against three agents tests
the risky part (is the spec good?) before provisioning anything. The sandbox is
well-understood engineering; the contract is the bet.

---

## Open follow-ups

| # | What | Status |
|---|---|---|
| **FU-1** | After-commit callback registry on `get_db_connection`, so the `orders:control` `"new"` publish happens after the commit. The documented FastAPI fix does **not** exist in 0.141.1 — verified empirically, background tasks run before yield-dependency teardown. A 5s reconciliation sweep is the shipped backstop. | Task 12 Step 9 decides promotion. |
| **FU-2** | The paper engine is single-process **by design**, and DP-charge dedup correctness now depends on it. A second engine process on the same instrument would race the dedup SELECT and double-charge. | Gate any horizontal scaling on making that race-safe. |
| **FU-3** | No CHECK constraint ties `charge_schedules.basis` to `.charge_type`. `InvalidChargeSchedule` catches a malformed row at fill time; a constraint would refuse it at write time. | Open. |
| — | Migrations `0007`, `0008`, `0009` reference `.superpowers/sdd/` paths in their docstrings — dangling once that scratch directory is deleted. Same class as the M-a fix, in three committed files. | Cosmetic. |

---

## Running the stack

Infra is `docker compose up -d` (timescaledb, redis on 6379, redis_test on
6380 — a separate *instance*, because Redis pub/sub ignores the db number).

**The database must be at migration `0010`.** `0009` adds `fills.tds` (an older
schema fails every fill insert); `0010` adds the strategy registry. Both
`trading` and `trading_test` are at `0010`.

```bash
uv run alembic upgrade head
uv run uvicorn trading.streaming.gateway:app --reload --port 8000
uv run python -m trading.streaming.crypto_ingestor      # Binance, 24/7
uv run python -m trading.streaming.bar_aggregator
uv run python -m trading.streaming.upstox_ingestor      # NSE session only
uv run python -m trading.paper.engine                   # the fill loop
uv run python -m trading.paper.alerts                   # outbox drain
cd web && npm run dev                                   # localhost:3000
```

Check for already-running processes before starting any of these — several
have been up for days, and **two crypto ingestors would double-publish every
tick**.

**Telegram is unconfigured**, so `run_alert_worker` idles and alerts queue in
`alert_deliveries` as `PENDING`. That is a deliberate configuration state, not
a fault. To switch it on, set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in
`.env`. Task 12 Step 6 wants an alert to actually reach the phone.

Demo portfolios from the 2026-09-02 session: **9** ("Crypto Demo", USDT) and
**10** ("INR Demo", INR).

---

## Two things about this codebase worth not relearning

**Review, not tests, has found nearly every real defect here** — six
quantization asymmetries, a fill-vs-cancel race, DP billed per fill instead of
per scrip per day, a missing currency gate (an INR portfolio could buy BTC-USDT
and be ~90× wrong). Each fix was then validated by *mutation* — flip the
operator, drop the exception from the catch tuple, select the wrong limit — and
several of those mutants survived a green suite. Use that on money paths.

**FastAPI routes must be `def`, never `async def`.** psycopg is synchronous;
an async route running a blocking DB call on the event loop deadlocked the
gateway permanently under concurrency. `test_no_route_is_a_coroutine_function`
guards it. GETs must never write.
