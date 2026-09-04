# Persisting backtest runs and their equity curves — Phase 3, sub-project 3c

**Status:** design, approved 2026-09-04.
**Phase:** 3 (Backtesting + metrics), third sub-project.
**Follows:** 3b (backtest runs at scale), which produces exactly what this stores.
**Precedes:** 3d (metrics), 3e (report UI), 3f (walk-forward + robustness) —
all three read from here, which is why the shape settles now rather than
alongside the first consumer.

---

## 1. What this is

3b runs a strategy over an operator-chosen window and hands back a result
that nothing keeps. `POST /strategies/{id}/backtests` returns 1,647 curve
points and forgets all of them the moment the response is written.

That is a deliberate boundary in 3b's design ("this sub-project returns
the result and does not store it. Persistence is 3c"), and it is now the
thing in the way: every remaining sub-project in Phase 3 consumes a stored
curve. 3d computes metrics from it. 3e draws it. 3f compares many of them.
None of that is reachable while a run is a transient HTTP response.

3c stores what ran and what it produced, and lets it be read back. It adds
no analysis of its own — deliberately, because a storage layer that also
computes is a storage layer whose format is decided by today's metric.

### What a stored run has to be good for

Three consumers, and they want different things:

- **3d** wants the whole series, in order, as exact decimals.
- **3e** wants a strategy's run history cheaply (a list that does not drag
  a megabyte of curve behind it), then one run in full.
- **3f** wants many runs' curves compared, which is the case that decides
  whether the curve is a document or a relation.

---

## 2. Settled decisions

### D3c-0. The schema — migration `0012`, two tables

```sql
CREATE TABLE backtest_runs (
    backtest_run_id         bigserial PRIMARY KEY,
    strategy_id             bigint NOT NULL
                            REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    status                  text NOT NULL
                            CHECK (status IN ('PASSED', 'FAILED')),
    -- what the caller asked for
    requested_start         date NOT NULL,
    requested_end           date NOT NULL,
    -- what the plan resolved: these differ from the request by warm-up
    fetch_start             timestamptz NOT NULL,
    dispatch_from           timestamptz NOT NULL,
    sessions                integer NOT NULL DEFAULT 0,
    instruments             jsonb NOT NULL DEFAULT '[]'::jsonb,
    history_bars_requested  integer NOT NULL DEFAULT 0,
    history_bars_available  integer NOT NULL DEFAULT 0,
    -- what the run did
    bars                    text,          -- the served interval, "1d" today
    bar_calls               integer NOT NULL DEFAULT 0,
    orders_placed           integer NOT NULL DEFAULT 0,
    fills                   integer NOT NULL DEFAULT 0,
    final_cash              numeric(18,4),
    final_equity            numeric(18,4),
    breaker_reason          text,
    error                   text,          -- populated when status = 'FAILED'
    findings                jsonb NOT NULL DEFAULT '[]'::jsonb,
    -- how it was confined, and against which contract
    runtime                 text NOT NULL,
    kernel_isolated         boolean NOT NULL,
    contract_version        text NOT NULL,
    ran_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ix_backtest_runs_strategy ON backtest_runs (strategy_id, ran_at DESC);

CREATE TABLE backtest_equity_points (
    backtest_run_id  bigint NOT NULL
                     REFERENCES backtest_runs(backtest_run_id) ON DELETE CASCADE,
    ts               timestamptz NOT NULL,
    equity           numeric(18,4) NOT NULL,
    cash             numeric(18,4) NOT NULL,
    PRIMARY KEY (backtest_run_id, ts)
);
```

`status` carries only `PASSED` and `FAILED` because refusals are never
stored (D3c-2); a `REFUSED` value would be unreachable, and an unreachable
enum member invites someone to make it reachable. `bars` is nullable and
records the interval actually served rather than the one declared, the
same distinction 3a introduced for smoke runs.

No `updated_at`. A run is an immutable record of an event; nothing edits
one, and a column implying otherwise would be a lie the schema tells.

### D3c-1. The curve is a table, not a JSONB column

**Decision: `backtest_equity_points`, one row per point,
`numeric(18,4)` money.**

The alternative — a `jsonb` column on the run row — matches the existing
precedent more closely: `strategy_smoke_runs` already stores
`instruments`, `findings` and `rejection_reasons` that way, and a curve is
written once, read whole, and never updated, which is document-shaped.
It was rejected for three reasons, in order of weight:

1. **Money.** This platform's headline feature is an honest cost model,
   and every money column it owns is `numeric(18,4)` — `fills`,
   `portfolios`, `positions`. A curve in JSONB is a curve of strings (the
   only safe JSON representation, per `outcome.py`'s reasoning that JSON
   numbers are IEEE 754 doubles). Storing money as text in a database that
   has a decimal type, purely because the wire format needs strings, gets
   the layering backwards: the wire constraint should not reach into
   storage. Stored as `numeric`, `min()`/`max()` for drawdown remain
   available to SQL, and no float can ever touch the value.
2. **It is the format 3f cannot migrate away from cheaply.** 3b deferred
   downsampling explicitly ("needed the day intraday runs land"), and an
   intraday curve is 100k+ points where a daily one is ~2,600. A table
   makes downsampling a `WHERE`/stride, and paging a `LIMIT`. JSONB makes
   both an application-side read of the whole blob. Choosing storage once
   is cheaper than migrating a format three sub-projects depend on — a
   cost 3b already worried about out loud when it insisted the clock fix
   land before the curve existed.
3. **Volume is a non-argument here.** 1,000 runs at 1,650 points is 1.65M
   rows, against a database already holding 51,081,227 daily bars and
   2,237,083 intraday ones.

### D3c-1a. `(backtest_run_id, ts)` is the primary key, and that is a constraint not an id

`run_loop` appends exactly one curve point per dispatched bar, and bars
are grouped by `close_ts` (`InMemoryBars.indexed_groups`), so timestamps
within a run are unique **by construction**.

Making that pair the primary key turns a property of the loop into
something the database refuses to let break. If a future change ever
dispatches the same timestamp twice — a plausible mistake once intraday
intervals or multi-venue runs arrive — the insert fails loudly instead of
storing a curve with a doubled point, which 3d would silently average into
a wrong Sharpe and 3e would draw as a real feature of the equity path.

It also gives the read path its index for free: `WHERE backtest_run_id = ?
ORDER BY ts` is a straight index scan, no second index needed.

Rejected alternative: a surrogate `point_id` with a separate unique index.
Same guarantees, one more column, one more index, and nothing gained — a
point has no identity apart from its run and its instant.

### D3c-2. Only runs that executed are stored

**Stored:** anything that reached the container, `PASSED` or `FAILED`.
A crash — `SMOKE_OOM`, `SMOKE_TIMEOUT`, `SMOKE_CRASH` — is stored *with
its partial curve*, because `run_loop` returns the curve on the crash path
too and a partial curve says where the run died. That is the single most
useful artifact when the platform, rather than the strategy, is at fault;
3b's own 64 KiB gVisor truncation was diagnosed from exactly that kind of
evidence.

**Not stored:** pre-flight refusals — `BACKTEST_TOO_LARGE`,
`BACKTEST_WINDOW_UNCOVERED`, `BACKTEST_INTERVAL_UNSUPPORTED`,
`MANIFEST_UNRESOLVABLE`, `NO_DATA`. They are returned to the caller and
nothing else. Each is a deterministic function of the request and the data
available, so re-deriving one costs a `COUNT`; storing them would fill the
table with rows that carry no result and imply, to anyone reading it
later, that a run happened.

A run is an event, not a resource: submitting the same window twice stores
two rows. There is no idempotency key, unlike `orders`, because there is
nothing to make idempotent — a second identical backtest is a second
observation, and a caller that wanted the first one can read it back.

### D3c-3. The write path does not commit

`record_backtest_run(conn, strategy_id, verdict) -> int` mirrors
`record_smoke_run` exactly, including its transaction discipline: it
writes and returns, and the caller owns the boundary. The run row and its
points therefore land as one unit or not at all — a half-written curve is
not a state 3d should ever have to defend against.

Points are inserted with `executemany`, the convention in this codebase
for batches of this size (`pipeline/runner.py`, `calendar/trading_days.py`,
`resolver/instruments.py`, `corpactions/ingest.py`). `COPY` is used here
only by the bulk bar loader, at row counts three orders of magnitude
larger; reaching for it at 1,650 rows would be optimising the wrong thing.
The day intraday curves arrive is the day to revisit that, and D3c-1's
table shape is what leaves the option open.

### D3c-4. Attribution: what a stored run has to say about itself

A result is only interpretable if it records what produced it. Three
groups of columns exist for that, and each has a specific failure it
prevents:

- **The request as asked** (`requested_start`, `requested_end`) versus
  **the window as resolved** (`fetch_start`, `dispatch_from`,
  `sessions`). These differ by warm-up, and 3b's whole D3b-3 is that the
  difference is deliberate. Storing only one of them makes a run's
  `bar_calls` unexplainable.
- **`history_bars_requested` / `history_bars_available`.** A strategy
  warmed on 40 of the 200 bars it asked for is a different experiment from
  the one requested — 3b reports that at run time, and dropping it here
  would make the distinction unrecoverable a week later.
- **`runtime` / `kernel_isolated`**, carried forward for the reason
  `SandboxResult` and `strategy_smoke_runs` both carry them: a stored run
  must never be readable as better isolated than it was.

`instruments` is stored as the resolved universe, not a count. The
universe is resolved point-in-time at the window's end
(`resolve_universe(..., as_of=end)`), so the same manifest can resolve
differently as listings change; a count would record that something was
traded without recording what.

The source itself is **not** copied. `strategy_id` references a registry
row whose source is immutable by construction — re-registering
`(name, version)` with different source raises `VersionConflict` — so the
foreign key already is the attribution. Copying the source would create a
second thing that could disagree with the first.

### D3c-5. Two read routes, and a list that stays cheap

- `GET /strategies/{strategy_id}/backtests` — run summaries, newest
  first, **without** curves.
- `GET /backtests/{backtest_run_id}` — one run, **with** its curve, in
  `ts` order. 404 when the id is unknown.

Splitting them is the whole point of the list route: 3e's history view
would otherwise transfer every point of every run to render a table of
dates and final equities.

Both are plain `def`. psycopg is synchronous, an `async def` route running
a blocking DB call on the event loop deadlocked this gateway permanently
once already, and `test_no_route_is_a_coroutine_function` is the guard.
Both are GETs and neither writes.

Neither is scoped by `user_id`, matching `GET /strategies` — harmless with
one seeded user and no auth, and the same single line to change when auth
lands. Recorded here so it is a known deferral rather than an oversight.

Money leaves as strings on both routes, for the reason `RunSummary`
already gives.

---

## 3. Explicitly out of scope

Named so they read as sequencing rather than oversight:

- **Metrics of any kind** — returns, Sharpe, drawdown, exposure, turnover.
  That is 3d, and it consumes this without needing the store to compute.
- **The report UI.** 3e.
- **Walk-forward and robustness.** 3f.
- **Curve downsampling and paging.** Unnecessary at ~2,600 points; D3c-1's
  table is what makes it available without a format change when it is not.
- **Pruning, retention, or deleting runs.** `ON DELETE CASCADE` from
  `strategies` is the only deletion path, and it exists so a removed
  strategy does not leave orphans — not as a retention policy.
- **Re-running a stored backtest.** A run records what happened; asking
  for it again is a new `POST`.

---

## 4. Testing

The pattern 3a and 3b established holds: prove the property, then verify
by mutation rather than by a green suite.

- **The curve round-trips exactly, in value and in order.** Assert stored
  points equal the outcome's points as `Decimal`, not as floats compared
  approximately. Mutation: route a value through `float()` on the way in
  and confirm the test reddens — this is the money path, and 3b's
  experience is that a wrong number survives a green suite comfortably.
- **A crashed run stores its partial curve.** The most useful artifact in
  the whole table, and the one an "only store successes" implementation
  would quietly drop.
- **A refused run stores nothing.** Assert zero rows, not a status field:
  a gate that refuses and writes anyway would pass an assertion on the
  response alone.
- **Run and points are atomic.** Roll the transaction back and assert
  neither exists. This is the property D3c-3 claims and the one a caller
  that commits too early would break.
- **The composite key rejects a duplicate timestamp.** Insert the same
  `(run_id, ts)` twice and assert the database refuses. Pins D3c-1a as a
  constraint rather than a comment.
- **The list route excludes curves; the detail route includes them.**
  Asserting the shape of both, because "cheap list" is the reason there
  are two routes and nothing else enforces it.
- **`test_no_route_is_a_coroutine_function` still passes.**

---

## 5. What this unblocks

3d can compute metrics over a stored series without re-running a
container; 3e can render a strategy's history and one run's equity path;
3f can compare curves across windows. All three become reads against two
tables rather than orchestration against Docker.
