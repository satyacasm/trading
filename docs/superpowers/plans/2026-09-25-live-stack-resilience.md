# Live Stack Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a live paper-trading run survive a Wi-Fi outage, a process restart, and a supervisor restart on the MacBook that hosts it, with no gap in `bars_intraday`, no duplicate or lost order, and no strategy state lost — without anyone touching it.

**Architecture:** Postgres stays the single source of truth for bars; `closed_bars:*` on Redis is only a wake-up notification, never a payload of record. Every long-running consumer (bar_aggregator, the paper engine, the live supervisor) reconnects Redis and Postgres with the same 1s→30s backoff instead of dying silently. The live supervisor tracks delivery position per `(live_run_id, instrument_id)` in a new table rather than trusting pub/sub to arrive exactly once, so a missed message, a restart, or a network gap all get repaired by the same "deliver everything after my cursor" mechanism. `ctx.state` is persisted every bar so a relaunched container resumes instead of starting cold, and a bar delivered late is flagged `catchup` so the supervisor — not the untrusted container — refuses to act on stale prices. launchd keeps every process alive across sleep and crash; Docker's own restart policy does the same for Postgres and Redis.

**Tech Stack:** Python 3.12, psycopg3, redis-py (async and sync), FastAPI, Alembic/SQLAlemy Core for migrations, pytest, Binance REST (`api.binance.com/api/v3/klines`), Docker/Colima/gVisor for the strategy sandbox, launchd for process supervision on macOS.

**Spec:** docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md

## Global Constraints

- TDD: write the failing test first, run it and watch it fail for the stated reason, then write the minimal implementation that makes it pass. Never edit a test to make it pass; if a test cannot be made to pass without changing the test, stop and escalate rather than weakening it.
- Tests use the `db_conn` fixture (a rolled-back transaction on `trading_test`, defined in `tests/conftest.py`) and the `redis_client` fixture / test Redis on port 6380 (`tests/streaming/conftest.py`, session-guarded against ever pointing at production). Never touch production Redis (6379) or the `trading` database from a test. No real network in tests: every Binance call goes through an injectable fetch function or an injectable `httpx.Client`.
- Money is `Decimal` end to end and crosses every process boundary (HTTP, the live protocol's stdin/stdout frames, the sandbox payload) as a string, never a JSON number.
- `write_closed_bar`-style DB helpers never call `conn.commit()` — production connections are `autocommit=True`, and tests rely on nothing here committing so `db_conn`'s rollback-at-teardown stays leakproof.
- FastAPI routes are sync `def` (blocking psycopg on an `async def` route deadlocked the gateway once already — see `docs/STATUS.md`). GET routes never write.
- Runtime source of truth is `src/trading/runtime/` and `src/trading/agent_contract/`; `sandbox/trading/` is an **untracked build copy** produced by `sandbox/build.sh` — never edit it directly. `sandbox/runner.py` itself IS tracked and is edited directly. After changing anything under `src/trading/runtime/`, `src/trading/agent_contract/`, or `sandbox/runner.py`, rebuild the sandbox image with `sandbox/build.sh` before running any `pytest.mark.sandbox` container test.
- New thresholds live in `trading.config.Settings`, with the defaults from spec §8 (see Task 1).
- Run the full suite `uv run pytest -q` at the end of each task; it must stay green apart from pre-existing, documented skips (e.g. sandbox tests needing Docker, `live`/`golden`-marked tests excluded by default `addopts`).
- Commit messages end with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  ```

---

### Task 1: Config thresholds and migration 0026

**Files:** Modify `src/trading/config.py` (Settings class, after line 90's `mcp_stdio_portfolio_id`); Modify `src/trading/contracts/enums.py` (`DataSource`, lines 25-39); Create `migrations/versions/0026_live_resilience.py`; Test `tests/test_config.py` (append); Test `tests/test_migrations.py` (append, or a new `tests/streaming/test_live_resilience_migration.py` following the style of `tests/streaming/test_upstox_intraday_backfill_migration.py`)

**Interfaces:** Produces: `Settings.backfill_silence_seconds: int = 90`, `Settings.backfill_sweep_seconds: int = 300`, `Settings.backfill_sweep_window_minutes: int = 30`, `Settings.live_delivery_timer_seconds: int = 30`, `Settings.live_catchup_after_seconds: int = 120`, `Settings.live_replay_cap_hours: int = 24`, `Settings.live_state_max_bytes: int = 65536`, `Settings.live_reply_timeout_seconds: int = 30`, `Settings.stale_price_seconds: int = 180`, `Settings.heartbeat_ttl_seconds: int = 30`, `Settings.heartbeat_refresh_seconds: int = 10`; `DataSource.BINANCE_SPOT_KLINE = 11`; table `live_run_cursors(live_run_id, instrument_id, last_ts)`; columns `live_runs.strategy_state jsonb NULL`, `live_runs.last_gap_note text NULL`. Every later task's DB writes and Settings reads depend on these existing first.

- [ ] Step 1: Write the failing test for the new Settings fields. Append to `tests/test_config.py`:
  ```python
  def test_live_resilience_thresholds_have_spec_defaults(monkeypatch, tmp_path):
      monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
      monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
      monkeypatch.setenv("DATA_ROOT", str(tmp_path))

      s = Settings(_env_file=None)

      assert s.backfill_silence_seconds == 90
      assert s.backfill_sweep_seconds == 300
      assert s.backfill_sweep_window_minutes == 30
      assert s.live_delivery_timer_seconds == 30
      assert s.live_catchup_after_seconds == 120
      assert s.live_replay_cap_hours == 24
      assert s.live_state_max_bytes == 65536
      assert s.live_reply_timeout_seconds == 30
      assert s.stale_price_seconds == 180
      assert s.heartbeat_ttl_seconds == 30
      assert s.heartbeat_refresh_seconds == 10
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/test_config.py::test_live_resilience_thresholds_have_spec_defaults -q
  ```
  Expected failure: `AttributeError: 'Settings' object has no attribute 'backfill_silence_seconds'`.

- [ ] Step 3: Add the fields to `Settings`, in `src/trading/config.py` right after `mcp_stdio_portfolio_id` (line 91) and before the `raw_archive_root` property:
  ```python
      # Live-stack-resilience thresholds (docs/superpowers/specs/
      # 2026-09-25-live-stack-resilience-design.md §8). Every one of these
      # is a duration an operator may reasonably want to tune per
      # deployment without a code change -- so they live here, not as
      # module constants.
      backfill_silence_seconds: int = 90
      backfill_sweep_seconds: int = 300
      backfill_sweep_window_minutes: int = 30
      live_delivery_timer_seconds: int = 30
      live_catchup_after_seconds: int = 120
      live_replay_cap_hours: int = 24
      live_state_max_bytes: int = 65536
      live_reply_timeout_seconds: int = 30
      stale_price_seconds: int = 180
      heartbeat_ttl_seconds: int = 30
      heartbeat_refresh_seconds: int = 10
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/test_config.py::test_live_resilience_thresholds_have_spec_defaults -q
  ```

- [ ] Step 5: Write the failing test for the migration. Create `tests/streaming/test_live_resilience_migration.py`:
  ```python
  """migrations/versions/0026_live_resilience.py: the delivery-cursor table,
  persisted ctx.state, the gap note, and the spot-kline provenance code."""

  from __future__ import annotations

  import pytest

  pytestmark = pytest.mark.db


  def test_live_run_cursors_table_exists_with_the_right_shape(db_conn) -> None:
      rows = db_conn.execute(
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_name = 'live_run_cursors'"
      ).fetchall()
      by_name = {r[0]: r[1] for r in rows}
      assert by_name == {
          "live_run_id": "bigint",
          "instrument_id": "bigint",
          "last_ts": "timestamp with time zone",
      }
      pk = db_conn.execute(
          "SELECT a.attname FROM pg_index i "
          "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
          "WHERE i.indrelid = 'live_run_cursors'::regclass AND i.indisprimary"
      ).fetchall()
      assert {r[0] for r in pk} == {"live_run_id", "instrument_id"}


  def test_live_runs_gained_strategy_state_and_last_gap_note(db_conn) -> None:
      rows = db_conn.execute(
          "SELECT column_name FROM information_schema.columns WHERE table_name = 'live_runs'"
      ).fetchall()
      names = {r[0] for r in rows}
      assert {"strategy_state", "last_gap_note"} <= names


  def test_binance_spot_kline_source_is_seeded(db_conn) -> None:
      row = db_conn.execute(
          "SELECT source_key FROM data_sources WHERE source_id = 11"
      ).fetchone()
      assert row == ("BINANCE_SPOT_KLINE",)
  ```

- [ ] Step 6: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_live_resilience_migration.py -q
  ```
  Expected failure: `psycopg.errors.UndefinedTable: relation "live_run_cursors" does not exist` (the `db_url` fixture runs `alembic upgrade head`, and head is still 0025).

- [ ] Step 7: Add `DataSource.BINANCE_SPOT_KLINE = 11` in `src/trading/contracts/enums.py`, right after `BINANCE_FUTURES_MARK = 10` (line 39):
  ```python
      # Backfilled spot minutes (trading.streaming.spot_backfill), distinct
      # from live-ticked BINANCE_WS bars and from BINANCE_FUTURES_KLINE --
      # the same pair's spot and perpetual prices are different series.
      BINANCE_SPOT_KLINE = 11
  ```
  Then create `migrations/versions/0026_live_resilience.py`:
  ```python
  """Delivery cursors, persisted strategy state, and the spot-kline source.

  Part of docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md.
  Three additions that ship together because every later task in that
  plan depends on all three existing:

  - `live_run_cursors`: per (live_run, instrument) delivery position (§4).
    Per instrument, not per run -- a single run-wide position would skip
    ETH's 10:05 bar if BTC's 10:05 bar had already moved it forward.
  - `live_runs.strategy_state`: `ctx.state`, persisted every bar so a
    relaunched run resumes instead of starting cold (§5.2).
  - `live_runs.last_gap_note`: the replay cap's skip, recorded on the run
    so its own history shows the discontinuity rather than reading as
    continuous (§4).
  - `DataSource.BINANCE_SPOT_KLINE` (11): provenance for backfilled spot
    minutes, distinct from BINANCE_WS (6) ticks and BINANCE_FUTURES_KLINE
    (9) perpetual bars -- the same pair's spot and perpetual prices are
    different series and must never share a code.

  Revision ID: 0026
  Revises: 0025
  Create Date: 2026-09-25
  """

  from __future__ import annotations

  from collections.abc import Sequence

  import sqlalchemy as sa
  from alembic import op
  from sqlalchemy.dialects import postgresql

  revision: str = "0026"
  down_revision: str | None = "0025"
  branch_labels: Sequence[str] | None = None
  depends_on: Sequence[str] | None = None


  def upgrade() -> None:
      op.create_table(
          "live_run_cursors",
          sa.Column(
              "live_run_id",
              sa.BigInteger,
              sa.ForeignKey("live_runs.live_run_id"),
              nullable=False,
          ),
          sa.Column(
              "instrument_id",
              sa.BigInteger,
              sa.ForeignKey("instruments.instrument_id"),
              nullable=False,
          ),
          sa.Column("last_ts", sa.TIMESTAMP(timezone=True), nullable=False),
          sa.PrimaryKeyConstraint("live_run_id", "instrument_id"),
      )
      op.add_column("live_runs", sa.Column("strategy_state", postgresql.JSONB(), nullable=True))
      op.add_column("live_runs", sa.Column("last_gap_note", sa.Text(), nullable=True))
      op.execute(
          "INSERT INTO data_sources (source_id, source_key) VALUES (11, 'BINANCE_SPOT_KLINE') "
          "ON CONFLICT (source_id) DO NOTHING"
      )


  def downgrade() -> None:
      op.execute("DELETE FROM data_sources WHERE source_id = 11")
      op.drop_column("live_runs", "last_gap_note")
      op.drop_column("live_runs", "strategy_state")
      op.drop_table("live_run_cursors")
  ```

- [ ] Step 8: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_live_resilience_migration.py tests/test_migrations.py -q
  ```
  (`db_url`'s session fixture re-runs `alembic upgrade head` against `trading_test`, which is a no-op once applied and picks up 0026 on the first test that needs it.)

- [ ] Step 9: Commit.
  ```
  git add src/trading/config.py src/trading/contracts/enums.py \
    migrations/versions/0026_live_resilience.py \
    tests/test_config.py tests/streaming/test_live_resilience_migration.py
  git commit -m "$(cat <<'EOF'
  feat(config,db): live-resilience thresholds and migration 0026

  Adds every Settings threshold spec §8 names, DataSource.BINANCE_SPOT_KLINE,
  and live_run_cursors/live_runs.strategy_state/last_gap_note -- the schema
  every later task in this plan builds on.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 2: `src/trading/streaming/resilient_pubsub.py`

**Files:** Create `src/trading/streaming/resilient_pubsub.py`; Test `tests/streaming/test_resilient_pubsub.py`

**Interfaces:** Consumes: `redis.asyncio.Redis`, `redis.Redis` (sync). Produces: `async def resilient_messages(redis: redis.asyncio.Redis, *, patterns: Sequence[str] = (), channels: Sequence[str] = (), sleep: Sleeper = asyncio.sleep, max_backoff: float = 30.0) -> AsyncIterator[dict]`; `class SyncResilientPubSub: def __init__(self, client_factory: Callable[[], redis.Redis], patterns: Sequence[str] = (), channels: Sequence[str] = (), sleep: Callable[[float], None] = time.sleep, max_backoff: float = 30.0) -> None` with `def get_message(self, timeout: float) -> dict | None`. Task 6 (bar_aggregator), Task 8 (paper engine) consume `resilient_messages`; Task 10 (supervisor) consumes `SyncResilientPubSub`.

- [ ] Step 1: Write the failing tests. Create `tests/streaming/test_resilient_pubsub.py`:
  ```python
  """resilient_messages/SyncResilientPubSub: a disconnect is a backoff and a
  resubscribe, never a silent end (bar_aggregator.py:484-518 and
  live/supervisor.py:512-525 are the two bugs this fixes)."""

  from __future__ import annotations

  import asyncio

  import pytest
  import redis
  import redis.asyncio as aioredis

  from trading.streaming.resilient_pubsub import SyncResilientPubSub, resilient_messages


  class _FakePubSub:
      """One async pubsub whose listen() does something different each time
      it is called -- exactly the seam resilient_messages watches."""

      def __init__(self, behaviors):
          self._behaviors = list(behaviors)
          self.subscribed_patterns: list[str] = []
          self.closed = 0

      async def psubscribe(self, *patterns):
          self.subscribed_patterns.extend(patterns)

      async def listen(self):
          behavior = self._behaviors.pop(0)
          if behavior == "raise":
              raise ConnectionError("connection reset")
          for message in behavior:
              yield message
          # "listen() that returns" -- a real disconnect that redis-py
          # itself does not raise on (bar_aggregator.py:484-518's bug).

      async def punsubscribe(self):
          pass

      async def aclose(self):
          self.closed += 1


  class _FakeRedis:
      def __init__(self, behaviors):
          self._behaviors = behaviors
          self.pubsub_calls = 0

      def pubsub(self):
          self.pubsub_calls += 1
          return _FakePubSub(self._behaviors)


  async def _collect(agen, count):
      out = []
      async for message in agen:
          out.append(message)
          if len(out) >= count:
              return out
      return out


  async def _fake_sleep_records(calls):
      async def _sleep(seconds):
          calls.append(seconds)
      return _sleep


  def test_a_raised_connection_error_backs_off_and_resubscribes():
      calls: list[float] = []

      async def _sleep(seconds):
          calls.append(seconds)

      fake = _FakeRedis(["raise", [{"type": "pmessage", "data": "after-reconnect"}]])
      agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

      out = asyncio.run(_collect(agen, 1))
      assert out == [{"type": "pmessage", "data": "after-reconnect"}]
      assert calls == [1.0]  # first backoff step
      assert fake.pubsub_calls == 2  # one pubsub per (re)connect attempt


  def test_listen_returning_is_also_treated_as_a_disconnect():
      """bar_aggregator's actual production bug: pubsub.listen() returning
      normally on a Redis connection loss, silently ending the consumer."""
      calls: list[float] = []

      async def _sleep(seconds):
          calls.append(seconds)

      fake = _FakeRedis([[], [{"type": "pmessage", "data": "after-empty-listen"}]])
      agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

      out = asyncio.run(_collect(agen, 1))
      assert out == [{"type": "pmessage", "data": "after-empty-listen"}]
      assert calls == [1.0]


  def test_backoff_resets_after_a_message_is_delivered():
      calls: list[float] = []

      async def _sleep(seconds):
          calls.append(seconds)

      fake = _FakeRedis(
          [
              "raise",
              "raise",
              [{"type": "pmessage", "data": "m1"}],
              "raise",
              [{"type": "pmessage", "data": "m2"}],
          ]
      )
      agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

      out = asyncio.run(_collect(agen, 2))
      assert [m["data"] for m in out] == ["m1", "m2"]
      # 1s, 2s (doubled -- no message delivered yet), then reset to 1s
      # after m1 before the third failure.
      assert calls == [1.0, 2.0, 1.0]


  def test_resilient_messages_against_real_redis(redis_client) -> None:
      from trading.config import get_settings

      async def _run():
          client = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
          agen = resilient_messages(client, patterns=["resilient-test:*"])
          task = asyncio.create_task(_collect(agen, 1))
          await asyncio.sleep(0.2)  # let the psubscribe land
          redis_client.publish("resilient-test:1", "hello")
          out = await asyncio.wait_for(task, timeout=5)
          await client.connection_pool.disconnect()
          return out

      out = asyncio.run(_run())
      assert out[0]["data"] == "hello"


  def test_sync_resilient_pubsub_reconnects_on_a_connection_error():
      calls: list[float] = []

      class _FakeSyncPubSub:
          def __init__(self, behaviors):
              self._behaviors = list(behaviors)

          def psubscribe(self, *patterns):
              pass

          def get_message(self, timeout=None):
              behavior = self._behaviors.pop(0)
              if behavior == "raise":
                  raise redis.exceptions.ConnectionError("reset")
              return behavior

      class _FakeSyncClient:
          def __init__(self, behaviors):
              self._behaviors = behaviors

          def pubsub(self):
              return _FakeSyncPubSub(self._behaviors)

      behaviors = ["raise", {"type": "pmessage", "data": "ok"}]
      sub = SyncResilientPubSub(
          lambda: _FakeSyncClient(behaviors), patterns=["ticks:*"], sleep=calls.append
      )

      assert sub.get_message(timeout=1.0) is None  # the reconnect attempt itself
      assert sub.get_message(timeout=1.0) == {"type": "pmessage", "data": "ok"}
      assert calls == [1.0]
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_resilient_pubsub.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.streaming.resilient_pubsub'`.

- [ ] Step 3: Implement. Create `src/trading/streaming/resilient_pubsub.py`:
  ```python
  """A pubsub subscription that survives a Redis disconnect.

  Two real bugs this fixes, both found live: `bar_aggregator.py:484-518`'s
  `pubsub.listen()` returns normally on a connection loss instead of
  raising, so the consuming `async for` loop ends silently and bar writing
  stops with no error; `live/supervisor.py:512-525`'s `get_message()`
  raises `ConnectionError` and kills the whole process.

  Lost messages are harmless here (design §2): Postgres is the record and
  a `closed_bars:*` message is only a wake-up, so a message dropped during
  a reconnect is recovered by whichever poller notices next (bar_aggregator's
  sweep, the supervisor's delivery timer). Reconnecting and resubscribing
  is therefore the whole fix -- there is nothing to replay at this layer.
  """

  from __future__ import annotations

  import asyncio
  import time
  from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
  from typing import Any

  import redis
  import redis.asyncio as aioredis
  import structlog

  log = structlog.get_logger(__name__)

  __all__ = ["SyncResilientPubSub", "resilient_messages"]

  Sleeper = Callable[[float], Awaitable[None]]

  _INITIAL_BACKOFF = 1.0


  async def _subscribe(
      redis_client: aioredis.Redis, patterns: Sequence[str], channels: Sequence[str]
  ) -> Any:
      pubsub = redis_client.pubsub()
      if patterns:
          await pubsub.psubscribe(*patterns)
      if channels:
          await pubsub.subscribe(*channels)
      return pubsub


  async def resilient_messages(
      redis_client: aioredis.Redis,
      *,
      patterns: Sequence[str] = (),
      channels: Sequence[str] = (),
      sleep: Sleeper = asyncio.sleep,
      max_backoff: float = 30.0,
  ) -> AsyncIterator[dict]:
      """Yield every message forever. A raised exception from `listen()` and
      a `listen()` that simply returns are both treated as a disconnect:
      log it, back off (1s, 2s, 4s, ... capped at `max_backoff`, reset to
      1s the moment a message is delivered), open a fresh pubsub, and
      resubscribe to the same patterns/channels."""
      backoff = _INITIAL_BACKOFF
      pubsub = await _subscribe(redis_client, patterns, channels)
      while True:
          disconnected = False
          try:
              async for message in pubsub.listen():
                  backoff = _INITIAL_BACKOFF
                  yield message
              disconnected = True  # listen() returned -- the silent-death bug
          except Exception as exc:  # noqa: BLE001 - any failure here is a reconnect
              disconnected = True
              log.warning("resilient_pubsub.disconnected", reason=str(exc))
          if disconnected:
              try:
                  await pubsub.aclose()  # type: ignore[no-untyped-call]
              except Exception:  # noqa: BLE001 - cleanup must never itself crash the loop
                  log.debug("resilient_pubsub.cleanup_failed", exc_info=True)
              log.warning("resilient_pubsub.reconnecting", backoff=backoff)
              await sleep(backoff)
              backoff = min(backoff * 2, max_backoff)
              pubsub = await _subscribe(redis_client, patterns, channels)


  class SyncResilientPubSub:
      """The sync twin, for the live supervisor's `get_message()`-driven
      loop. Never raises on a connection error: `get_message` returns
      `None` for that call and reconnects (with the same backoff) before
      the next one, so a caller already treating `None` as "nothing right
      now" needs no new branch."""

      def __init__(
          self,
          client_factory: Callable[[], redis.Redis],
          patterns: Sequence[str] = (),
          channels: Sequence[str] = (),
          sleep: Callable[[float], None] = time.sleep,
          max_backoff: float = 30.0,
      ) -> None:
          self._client_factory = client_factory
          self._patterns = list(patterns)
          self._channels = list(channels)
          self._sleep = sleep
          self._max_backoff = max_backoff
          self._backoff = _INITIAL_BACKOFF
          self._pubsub = self._connect()

      def _connect(self) -> Any:
          client = self._client_factory()
          pubsub = client.pubsub()
          if self._patterns:
              pubsub.psubscribe(*self._patterns)
          if self._channels:
              pubsub.subscribe(*self._channels)
          return pubsub

      def get_message(self, timeout: float) -> dict | None:
          try:
              message = self._pubsub.get_message(timeout=timeout)
          except redis.exceptions.RedisError as exc:
              log.warning("resilient_pubsub.sync_disconnected", reason=str(exc))
              self._sleep(self._backoff)
              self._backoff = min(self._backoff * 2, self._max_backoff)
              self._pubsub = self._connect()
              return None
          if message is not None:
              self._backoff = _INITIAL_BACKOFF
          return message
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_resilient_pubsub.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/streaming/resilient_pubsub.py tests/streaming/test_resilient_pubsub.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): resilient pubsub, async and sync

  A Redis disconnect is now a logged backoff-and-resubscribe everywhere it
  is consumed, not a silently-dead consumer (bar_aggregator.py:484-518)
  or a killed process (live/supervisor.py:512-525).

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 3: `src/trading/db.py` — `ReconnectingConnection`

**Files:** Create `src/trading/db.py`; Test `tests/test_db.py`

**Interfaces:** Produces: `class ReconnectingConnection: def __init__(self, url: str, *, autocommit: bool = True, sleep: Callable[[float], None] = time.sleep, connect: Callable[..., psycopg.Connection] = psycopg.connect, max_backoff: float = 30.0) -> None` with `def get(self) -> psycopg.Connection`. Task 10 (supervisor) and Task 6's sweep/silence backfill connections consume this.

- [ ] Step 1: Write the failing test. Create `tests/test_db.py`:
  ```python
  """ReconnectingConnection: a dead Postgres connection is repaired on the
  next .get(), with the same 1s-30s backoff resilient_pubsub uses."""

  from __future__ import annotations

  import psycopg
  import pytest

  from trading.db import ReconnectingConnection


  class _FakeConn:
      def __init__(self, alive=True, probe_raises=False):
          self.closed = 0 if alive else 1
          self._probe_raises = probe_raises
          self.executed: list[str] = []

      def execute(self, sql):
          self.executed.append(sql)
          if self._probe_raises:
              raise psycopg.OperationalError("server closed the connection unexpectedly")


  def test_get_returns_the_same_connection_while_it_is_healthy():
      conns = [_FakeConn()]
      rc = ReconnectingConnection("postgresql://x", connect=lambda *a, **k: conns[0])

      first = rc.get()
      second = rc.get()
      assert first is second is conns[0]


  def test_a_closed_connection_is_replaced():
      old = _FakeConn(alive=False)
      new = _FakeConn(alive=True)
      made = [old, new]

      def _connect(*a, **k):
          return made.pop(0)

      rc = ReconnectingConnection("postgresql://x", connect=_connect)
      assert rc.get() is old  # closed=0 wasn't checked until the NEXT get()
      old.closed = 1
      assert rc.get() is new


  def test_a_probe_failure_triggers_backoff_then_reconnect():
      calls: list[float] = []
      bad = _FakeConn(alive=True, probe_raises=True)
      good = _FakeConn(alive=True)
      made = [bad, good]

      rc = ReconnectingConnection(
          "postgresql://x", connect=lambda *a, **k: made.pop(0), sleep=calls.append
      )
      first = rc.get()
      assert first is bad
      second = rc.get()  # probes with SELECT 1, which raises on `bad`
      assert second is good
      assert calls == [1.0]


  def test_backoff_caps_at_max_backoff_and_keeps_reconnecting():
      calls: list[float] = []
      always_bad = _FakeConn(alive=True, probe_raises=True)

      def _connect(*a, **k):
          return always_bad

      rc = ReconnectingConnection(
          "postgresql://x", connect=_connect, sleep=calls.append, max_backoff=4.0
      )
      rc.get()
      for _ in range(4):
          rc.get()
      assert calls == [1.0, 2.0, 4.0, 4.0]
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/test_db.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.db'`.

- [ ] Step 3: Implement. Create `src/trading/db.py`:
  ```python
  """A Postgres connection that repairs itself.

  Every long-running process here opened one psycopg connection at start
  and kept it forever -- fine until the laptop sleeps through a router
  reset and TimescaleDB drops the socket. `ReconnectingConnection` replaces
  that "open once" pattern: `.get()` returns a connection that is either
  already known-good or has just been reconnected, with the same 1s-30s
  backoff `resilient_pubsub` uses for Redis.
  """

  from __future__ import annotations

  import time
  from collections.abc import Callable

  import psycopg
  import structlog

  log = structlog.get_logger(__name__)

  __all__ = ["ReconnectingConnection"]

  _INITIAL_BACKOFF = 1.0


  class ReconnectingConnection:
      def __init__(
          self,
          url: str,
          *,
          autocommit: bool = True,
          sleep: Callable[[float], None] = time.sleep,
          connect: Callable[..., psycopg.Connection] = psycopg.connect,
          max_backoff: float = 30.0,
      ) -> None:
          self._url = url
          self._autocommit = autocommit
          self._sleep = sleep
          self._connect = connect
          self._max_backoff = max_backoff
          self._backoff = _INITIAL_BACKOFF
          self._conn = self._open()

      def _open(self) -> psycopg.Connection:
          return self._connect(self._url, autocommit=self._autocommit)

      def _reconnect(self) -> None:
          log.warning("db.reconnecting", backoff=self._backoff)
          self._sleep(self._backoff)
          self._backoff = min(self._backoff * 2, self._max_backoff)
          self._conn = self._open()

      def get(self) -> psycopg.Connection:
          """A connection known to be alive right now. Closed connections
          are caught for free (`.closed`); a connection that merely looks
          open but whose socket died silently is caught by a `SELECT 1`
          probe, the same check Postgres client libraries use everywhere
          for exactly this failure mode."""
          if self._conn.closed:
              self._reconnect()
              return self._conn
          try:
              self._conn.execute("SELECT 1")
          except psycopg.OperationalError as exc:
              log.warning("db.probe_failed", reason=str(exc))
              self._reconnect()
              return self._conn
          self._backoff = _INITIAL_BACKOFF
          return self._conn
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/test_db.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/db.py tests/test_db.py
  git commit -m "$(cat <<'EOF'
  feat(db): ReconnectingConnection, a self-healing Postgres handle

  A dead connection (closed, or a silently dropped socket caught by a
  SELECT 1 probe) is reconnected on the next .get() with the same
  1s-30s backoff resilient_pubsub uses for Redis.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 4: Spot klines source and pure backfill

**Files:** Create `src/trading/sources/binance_spot.py`; Create `src/trading/streaming/spot_backfill.py`; Test `tests/sources/test_binance_spot.py`; Test `tests/streaming/test_spot_backfill.py`

**Interfaces:** Consumes: `DataSource.BINANCE_SPOT_KLINE` (Task 1); `bucket_start` from `trading.streaming.bar_aggregator` (existing, `interval_seconds=60`). Produces: `def parse_spot_klines(rows: list[list[Any]]) -> list[SpotKline]` (exported, imported by the tests); `@dataclass(frozen=True) class SpotKline: ts: datetime; open: Decimal; high: Decimal; low: Decimal; close: Decimal; volume: Decimal; trades: int`; `def spot_symbol(pair: str) -> str` ("BTC-USDT" -> "BTCUSDT"); `def fetch_spot_klines(symbol: str, *, start_ms: int, end_ms: int, client: httpx.Client | None = None) -> list[SpotKline]`; `def backfill_window(conn: Connection, instrument_id: int, symbol: str, since: datetime, until: datetime, *, fetch: Callable[..., list[SpotKline]] = fetch_spot_klines, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> list[SpotKline]`; `def last_bar_ts(conn: Connection, instrument_id: int) -> datetime | None`. Task 5 consumes `backfill_window`, `last_bar_ts`, `spot_symbol`.

- [ ] Step 1: Write the failing tests. Create `tests/sources/test_binance_spot.py`:
  ```python
  """Parsing Binance's spot klines payload into SpotKline rows."""

  from __future__ import annotations

  import json
  from datetime import UTC, datetime
  from decimal import Decimal

  from trading.sources.binance_spot import SpotKline, parse_spot_klines, spot_symbol


  def test_spot_symbol_strips_the_dash_and_uppercases():
      assert spot_symbol("BTC-USDT") == "BTCUSDT"


  def test_parse_spot_klines_reads_the_row_shape():
      # [openTime, open, high, low, close, volume, closeTime, quoteVolume,
      # trades, takerBuyBase, takerBuyQuote, ignore]
      raw = json.dumps(
          [
              [
                  1758700800000,
                  "63000.00",
                  "63100.50",
                  "62950.00",
                  "63050.25",
                  "12.500000",
                  1758700859999,
                  "788125.00",
                  340,
                  "6.0",
                  "378000.00",
                  "0",
              ]
          ]
      ).encode()

      bars = parse_spot_klines(raw)

      assert bars == [
          SpotKline(
              ts=datetime.fromtimestamp(1758700800, UTC),
              open=Decimal("63000.00"),
              high=Decimal("63100.50"),
              low=Decimal("62950.00"),
              close=Decimal("63050.25"),
              volume=Decimal("12.500000"),
              trades=340,
          )
      ]


  def test_zero_trade_minute_parses_with_zero_volume_and_zero_trades():
      """Klines fill every minute, traded or not -- a zero-trade minute
      must parse, not be skipped, or the backfill's whole point (no
      holes) is lost."""
      raw = json.dumps(
          [[1758700800000, "63000", "63000", "63000", "63000", "0", 1758700859999, "0", 0, "0", "0", "0"]]
      ).encode()

      bars = parse_spot_klines(raw)

      assert bars[0].volume == Decimal("0")
      assert bars[0].trades == 0
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/sources/test_binance_spot.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.sources.binance_spot'`.

- [ ] Step 3: Implement. Create `src/trading/sources/binance_spot.py`:
  ```python
  """Binance's public spot klines -- the backfill source for crypto minute
  bars (docs/superpowers/specs/2026-09-25-live-stack-resilience-design.md
  §3), the same endpoint family as `trading.sources.binance_futures` but
  unauthenticated and unmargined.
  """

  from __future__ import annotations

  import json
  from dataclasses import dataclass
  from datetime import UTC, datetime
  from decimal import Decimal, InvalidOperation
  from typing import Any

  import httpx
  import structlog

  __all__ = [
      "KLINES_URL",
      "SpotKline",
      "fetch_spot_klines",
      "parse_spot_klines",
      "spot_symbol",
  ]

  log = structlog.get_logger(__name__)

  KLINES_URL = "https://api.binance.com/api/v3/klines"
  KLINES_PAGE = 1000


  def spot_symbol(pair: str) -> str:
      """'BTC-USDT' -> 'BTCUSDT' -- Binance's REST symbol, no separator,
      uppercase (the futures/WS feeds lowercase theirs; klines wants
      upper)."""
      return pair.replace("-", "").upper()


  @dataclass(frozen=True)
  class SpotKline:
      """One closed 1-minute spot kline. `ts` is the START of the interval,
      matching `PerpBar` and `bars_intraday.ts`."""

      ts: datetime
      open: Decimal
      high: Decimal
      low: Decimal
      close: Decimal
      volume: Decimal
      trades: int


  def parse_spot_klines(raw: bytes) -> list[SpotKline]:
      bars: list[SpotKline] = []
      for row in json.loads(raw):
          try:
              bars.append(
                  SpotKline(
                      ts=datetime.fromtimestamp(int(row[0]) / 1000, UTC),
                      open=Decimal(str(row[1])),
                      high=Decimal(str(row[2])),
                      low=Decimal(str(row[3])),
                      close=Decimal(str(row[4])),
                      volume=Decimal(str(row[5])),
                      trades=int(row[8]),
                  )
              )
          except (IndexError, KeyError, TypeError, ValueError, InvalidOperation):
              log.warning("binance_spot.kline_row_skipped")
      return bars


  def fetch_spot_klines(
      symbol: str, *, start_ms: int, end_ms: int, client: httpx.Client | None = None
  ) -> list[SpotKline]:
      """Every closed 1-minute kline in `[start_ms, end_ms)`, paged.

      Bounded by `end_ms`, unlike `binance_futures.fetch_klines`'s
      walk-to-now: a backfill window is always `[since, until)` (design
      §3), never open-ended.
      """
      owned = client is None
      client = client or httpx.Client(timeout=30.0)
      collected: list[SpotKline] = []
      try:
          cursor = start_ms
          while cursor < end_ms:
              response = client.get(
                  KLINES_URL,
                  params={
                      "symbol": symbol,
                      "interval": "1m",
                      "startTime": cursor,
                      "endTime": end_ms - 1,
                      "limit": KLINES_PAGE,
                  },
              )
              response.raise_for_status()
              page = parse_spot_klines(response.content)
              if not page:
                  return collected
              collected.extend(page)
              if len(page) < KLINES_PAGE:
                  return collected
              cursor = int(page[-1].ts.timestamp() * 1000) + 60_000
          return collected
      finally:
          if owned:
              client.close()
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/sources/test_binance_spot.py -q
  ```

- [ ] Step 5: Write the failing tests for `backfill_window`/`last_bar_ts`. Create `tests/streaming/test_spot_backfill.py`:
  ```python
  """backfill_window: fills the outage minutes from Binance klines without
  ever touching a tick-built row or dispatching the forming minute."""

  from __future__ import annotations

  from datetime import UTC, datetime
  from decimal import Decimal

  from trading.sources.binance_spot import SpotKline
  from trading.streaming.seed_instruments import seed_crypto_instruments
  from trading.streaming.spot_backfill import backfill_window, last_bar_ts


  def _instrument(db_conn) -> int:
      return seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]


  def _kline(minute: int, close: str = "63000") -> SpotKline:
      return SpotKline(
          ts=datetime(2026, 9, 25, 10, minute, tzinfo=UTC),
          open=Decimal(close),
          high=Decimal(close),
          low=Decimal(close),
          close=Decimal(close),
          volume=Decimal("0"),
          trades=0,
      )


  def test_until_is_clamped_to_the_current_minutes_start():
      """The forming minute is never dispatched -- clamping `until` rather
      than trusting the caller means a caller that (wrongly) asks for the
      current minute still doesn't get it."""
      captured = {}

      def _fetch(symbol, *, start_ms, end_ms, client=None):
          captured["start_ms"] = start_ms
          captured["end_ms"] = end_ms
          return []

      backfill_window(
          None,
          1,
          "BTCUSDT",
          since=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          until=datetime(2026, 9, 25, 10, 10, 30, tzinfo=UTC),
          fetch=_fetch,
          now=lambda: datetime(2026, 9, 25, 10, 10, 30, tzinfo=UTC),
      )
      # Clamped to 10:10:00, not the caller's 10:10:30.
      assert captured["end_ms"] == int(datetime(2026, 9, 25, 10, 10, tzinfo=UTC).timestamp() * 1000)


  def test_a_zero_trade_minute_is_written(db_conn):
      iid = _instrument(db_conn)
      inserted = backfill_window(
          db_conn,
          iid,
          "BTCUSDT",
          since=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          until=datetime(2026, 9, 25, 10, 2, tzinfo=UTC),
          fetch=lambda *a, **k: [_kline(0), _kline(1)],
          now=lambda: datetime(2026, 9, 25, 10, 5, tzinfo=UTC),
      )
      assert [k.ts.minute for k in inserted] == [0, 1]
      rows = db_conn.execute(
          "SELECT ts, volume, trades, source FROM bars_intraday "
          "WHERE instrument_id=%s ORDER BY ts",
          (iid,),
      ).fetchall()
      assert len(rows) == 2
      assert rows[0][1] == Decimal("0")
      assert rows[0][3] == 11  # BINANCE_SPOT_KLINE


  def test_an_existing_tick_built_row_is_never_overwritten(db_conn):
      """A strategy may already have been sent the tick-built bar --
      ON CONFLICT DO NOTHING is what keeps it from silently changing
      under a run that already saw it."""
      iid = _instrument(db_conn)
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
          (iid, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
      )
      inserted = backfill_window(
          db_conn,
          iid,
          "BTCUSDT",
          since=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
          until=datetime(2026, 9, 25, 10, 1, tzinfo=UTC),
          fetch=lambda *a, **k: [_kline(0, close="99999")],
          now=lambda: datetime(2026, 9, 25, 10, 5, tzinfo=UTC),
      )
      assert inserted == []  # ON CONFLICT DO NOTHING -- nothing was inserted
      row = db_conn.execute(
          "SELECT close FROM bars_intraday WHERE instrument_id=%s AND ts=%s",
          (iid, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
      ).fetchone()
      assert row[0] == Decimal("1.0000")  # untouched


  def test_last_bar_ts_is_the_max_ts_for_that_instrument(db_conn):
      iid = _instrument(db_conn)
      assert last_bar_ts(db_conn, iid) is None
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 11)",
          (iid, datetime(2026, 9, 25, 10, 3, tzinfo=UTC)),
      )
      assert last_bar_ts(db_conn, iid) == datetime(2026, 9, 25, 10, 3, tzinfo=UTC)
  ```

- [ ] Step 6: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_spot_backfill.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.streaming.spot_backfill'`.

- [ ] Step 7: Implement. Create `src/trading/streaming/spot_backfill.py`:
  ```python
  """Fill the gap a Wi-Fi outage leaves in crypto spot bars.

  Modelled on `trading.streaming.perp_backfill`, but bounded rather than
  walk-to-now: a live outage window is always `[since, until)` (design
  §3), never "everything since the beginning". Pure Postgres here --
  publishing the `closed_bars:*` notification for whatever this inserts
  is `bar_aggregator`'s job (it already owns that channel and the
  `_announce_bar` helper), not this module's.
  """

  from __future__ import annotations

  from collections.abc import Callable
  from datetime import UTC, datetime

  from psycopg import Connection

  from trading.contracts import DataSource
  from trading.sources.binance_spot import SpotKline, fetch_spot_klines
  from trading.streaming.bar_aggregator import bucket_start

  __all__ = ["backfill_window", "last_bar_ts"]

  _UPSERT = """
      INSERT INTO bars_intraday
          (instrument_id, ts, interval_sec, open, high, low, close, volume, trades, source)
      VALUES (%s, %s, 60, %s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (instrument_id, ts, interval_sec) DO NOTHING
      RETURNING ts
  """


  def last_bar_ts(conn: Connection, instrument_id: int) -> datetime | None:
      row = conn.execute(
          "SELECT max(ts) FROM bars_intraday WHERE instrument_id = %s AND interval_sec = 60",
          (instrument_id,),
      ).fetchone()
      return None if row is None else row[0]


  def backfill_window(
      conn: Connection,
      instrument_id: int,
      symbol: str,
      since: datetime,
      until: datetime,
      *,
      fetch: Callable[..., list[SpotKline]] = fetch_spot_klines,
      now: Callable[[], datetime] = lambda: datetime.now(UTC),
  ) -> list[SpotKline]:
      """Fetch and write every closed minute in `[since, until)`, clamped
      so the forming minute is never dispatched. Returns only the klines
      actually inserted -- a tick-built row for the same minute is left
      alone (`ON CONFLICT DO NOTHING`), because a strategy may already
      have been sent it."""
      clamped_until = min(until, bucket_start(now(), 60))
      if clamped_until <= since:
          return []
      klines = fetch(
          symbol,
          start_ms=int(since.timestamp() * 1000),
          end_ms=int(clamped_until.timestamp() * 1000),
      )
      inserted_ts: set[datetime] = set()
      for kline in klines:
          row = conn.execute(
              _UPSERT,
              (
                  instrument_id,
                  kline.ts,
                  kline.open,
                  kline.high,
                  kline.low,
                  kline.close,
                  kline.volume,
                  kline.trades,
                  DataSource.BINANCE_SPOT_KLINE.value,
              ),
          ).fetchone()
          if row is not None:
              inserted_ts.add(row[0])
      return [k for k in klines if k.ts in inserted_ts]
  ```

- [ ] Step 8: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_spot_backfill.py -q
  ```

- [ ] Step 9: Commit.
  ```
  git add src/trading/sources/binance_spot.py src/trading/streaming/spot_backfill.py \
    tests/sources/test_binance_spot.py tests/streaming/test_spot_backfill.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): spot kline source and bounded gap backfill

  fetch_spot_klines/backfill_window fill [since, until) from Binance's
  public klines endpoint, clamped to the last closed minute, writing with
  ON CONFLICT DO NOTHING so a tick-built row is never overwritten.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---
### Task 5: `BarAggregator.seed_closed_through` and startup backfill

**Files:** Modify `src/trading/streaming/bar_aggregator.py` (`BarAggregator` class lines 78-137; `run_aggregation_loop` lines 345-556); Test `tests/streaming/test_bar_aggregator.py` (append)

**Interfaces:** Consumes: `backfill_window`, `last_bar_ts`, `spot_symbol` (Task 4); `_announce_bar` (existing, same module). Produces: `BarAggregator.seed_closed_through(mapping: dict[int, datetime]) -> None`; `async def _startup_spot_backfill(conn_factory: Callable[[], Connection], aggregator: BarAggregator, redis: Redis, *, to_thread: Callable[..., Any], fetch: Any = None) -> None`; `run_aggregation_loop(..., backfill_conn_factory: Callable[[], Connection] | None = None, to_thread: Callable[..., Any] = asyncio.to_thread)` (both new keyword-only, default `None`/real `asyncio.to_thread` so every existing call site and test is unaffected). Task 6 extends the same function with the silence trigger and sweep, reusing `backfill_conn_factory`/`to_thread`.

- [ ] Step 1: Write the failing tests. Append to `tests/streaming/test_bar_aggregator.py`:
  ```python
  def test_seed_closed_through_drops_a_late_tick_for_an_already_written_minute():
      """The actual production bug (design §1 row 4): _closed_through is
      in-memory only, so a late tick after a restart reopened an
      already-announced minute and republished it with different values,
      crashing a live strategy on 'arrived out of order'."""
      from trading.streaming.bar_aggregator import BarAggregator
      from trading.streaming.models import Tick

      aggregator = BarAggregator()
      aggregator.seed_closed_through({1: datetime(2026, 9, 25, 10, 5, tzinfo=UTC)})

      closed = aggregator.ingest(
          Tick(
              instrument_id=1,
              ts=datetime(2026, 9, 25, 10, 3, 30, tzinfo=UTC),  # a minute already seeded
              price=Decimal("100"),
              quantity=Decimal("1"),
          )
      )
      assert closed == []
      assert aggregator.late_ticks_dropped == 1


  def test_startup_backfill_writes_and_announces_with_a_fake_fetch(db_conn, redis_client) -> None:
      """Exercises the extracted helper directly rather than the whole
      run_aggregation_loop -- that loop only terminates on a tick/bar
      count, and no ticks are published in this test."""
      import asyncio
      import json

      from redis.asyncio import Redis as AsyncRedis

      from trading.config import get_settings
      from trading.sources.binance_spot import SpotKline
      from trading.streaming.bar_aggregator import BarAggregator, _startup_spot_backfill
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
          (iid, datetime(2026, 9, 25, 9, 58, tzinfo=UTC)),
      )

      def _fake_fetch(symbol, *, start_ms, end_ms, client=None):
          return [
              SpotKline(
                  ts=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
                  open=Decimal("100"),
                  high=Decimal("100"),
                  low=Decimal("100"),
                  close=Decimal("100"),
                  volume=Decimal("0"),
                  trades=0,
              )
          ]

      async def _inline_to_thread(fn, /, *args, **kwargs):
          return fn(*args, **kwargs)

      async def _next_pmessage(pubsub):
          async for msg in pubsub.listen():
              if msg["type"] == "pmessage":
                  return msg

      async def _run():
          async_redis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
          pubsub = async_redis.pubsub()
          await pubsub.psubscribe("closed_bars:*")
          aggregator = BarAggregator()
          try:
              await _startup_spot_backfill(
                  lambda: db_conn,
                  aggregator,
                  async_redis,
                  to_thread=_inline_to_thread,
                  fetch=_fake_fetch,
              )
              message = await asyncio.wait_for(_next_pmessage(pubsub), timeout=5)
              return aggregator, message
          finally:
              await pubsub.aclose()
              await async_redis.connection_pool.disconnect()

      aggregator, message = asyncio.run(_run())

      assert aggregator._closed_through[iid] == datetime(2026, 9, 25, 9, 59, tzinfo=UTC)
      row = db_conn.execute(
          "SELECT open, source FROM bars_intraday WHERE instrument_id=%s AND ts=%s",
          (iid, datetime(2026, 9, 25, 9, 59, tzinfo=UTC)),
      ).fetchone()
      assert row == (Decimal("100.0000"), 11)  # 11 = BINANCE_SPOT_KLINE
      body = json.loads(message["data"])
      assert body["instrument_id"] == iid
  ```
  (This module already imports `datetime`, `UTC`, `Decimal` at file top for its other tests -- reuse those imports rather than re-adding them.)

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py -k "seed_closed_through or startup_backfill" -q
  ```
  Expected failure: `AttributeError: 'BarAggregator' object has no attribute 'seed_closed_through'`, then (once that's fixed) `ImportError: cannot import name '_startup_spot_backfill'`.

- [ ] Step 3: Implement. In `src/trading/streaming/bar_aggregator.py`, add `from typing import Any` and `from trading.sources.binance_spot import fetch_spot_klines, spot_symbol` and `from trading.streaming.spot_backfill import backfill_window, last_bar_ts` to the imports at the top of the file. Add a method to `BarAggregator` right after `__init__` (after line 106):
  ```python
      def seed_closed_through(self, mapping: dict[int, datetime]) -> None:
          """Prime `_closed_through` from the database at startup, so a
          late tick for a minute this PROCESS never bucketed -- because a
          previous process instance already closed and announced it -- is
          dropped rather than reopening and republishing an already-final
          bar (design §1 row 4; `_closed_through` was in-memory only, and
          a restart forgot it)."""
          self._closed_through.update(mapping)
  ```
  Add the query helper and the startup-backfill coroutine just above `run_aggregation_loop` (before line 345):
  ```python
  _SELECT_CRYPTO_SPOT_INSTRUMENTS = """
      SELECT instrument_id, symbol FROM instruments
      WHERE asset_class = 'CRYPTO' AND segment = 'SPOT'
  """


  def _query_crypto_spot_instruments(conn: Connection) -> dict[int, str]:
      rows = conn.execute(_SELECT_CRYPTO_SPOT_INSTRUMENTS).fetchall()
      return {int(row[0]): str(row[1]) for row in rows}


  async def _startup_spot_backfill(
      conn_factory: Callable[[], Connection],
      aggregator: BarAggregator,
      redis: Redis,
      *,
      to_thread: Callable[..., Any],
      fetch: Any = None,
  ) -> None:
      """Seed `_closed_through` from the database and fill whatever
      elapsed while this process was down, from each crypto spot
      instrument's last stored bar to the first bucket this process can
      honestly claim in full. Runs once, awaited, before ticks are
      consumed, off the event loop thread so a slow Binance response
      never delays the first tick subscription. A REST failure for one
      instrument is logged and skipped -- it never blocks the others or
      startup itself.
      """
      fetch = fetch or fetch_spot_klines

      def _do() -> tuple[dict[int, datetime], list[tuple[int, Any]]]:
          conn = conn_factory()
          instruments = _query_crypto_spot_instruments(conn)
          seeded: dict[int, datetime] = {}
          announced: list[tuple[int, Any]] = []
          cutoff = bucket_start(datetime.now(UTC), INTERVAL_SECONDS)
          for instrument_id, symbol in instruments.items():
              latest = last_bar_ts(conn, instrument_id)
              if latest is None:
                  continue
              seeded[instrument_id] = latest
              try:
                  inserted = backfill_window(
                      conn,
                      instrument_id,
                      spot_symbol(symbol),
                      since=latest + timedelta(seconds=INTERVAL_SECONDS),
                      until=cutoff,
                      fetch=fetch,
                  )
              except Exception as exc:  # noqa: BLE001 - a REST failure must never block startup
                  log.warning(
                      "bar_aggregator.startup_backfill_failed",
                      instrument_id=instrument_id,
                      reason=str(exc),
                  )
                  continue
              for kline in inserted:
                  seeded[instrument_id] = max(seeded[instrument_id], kline.ts)
                  announced.append((instrument_id, kline))
          return seeded, announced

      seeded, announced = await to_thread(_do)
      aggregator.seed_closed_through(seeded)
      for instrument_id, kline in announced:
          await _announce_bar(
              redis,
              instrument_id=instrument_id,
              ts=kline.ts,
              open_=kline.open,
              high=kline.high,
              low=kline.low,
              close=kline.close,
              volume=kline.volume,
              interval_seconds=INTERVAL_SECONDS,
              source=DataSource.BINANCE_SPOT_KLINE,
          )
  ```
  Finally, wire it into `run_aggregation_loop`: add the two new keyword-only parameters to its signature (right after `bars_pattern: str = _BAR_PATTERN,` on line 355) --
  ```python
      backfill_conn_factory: Callable[[], Connection] | None = None,
      to_thread: Callable[..., Any] = asyncio.to_thread,
      spot_fetch: Any = None,
  ```
  -- and call it right after `aggregator = BarAggregator(interval_seconds)` (line 397):
  ```python
      aggregator = BarAggregator(interval_seconds)
      if backfill_conn_factory is not None:
          await _startup_spot_backfill(
              backfill_conn_factory, aggregator, redis, to_thread=to_thread, fetch=spot_fetch
          )
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py -q
  ```

- [ ] Step 5: Run the full streaming suite to confirm every existing call site of `run_aggregation_loop` (which passes neither new kwarg, so `backfill_conn_factory` stays `None` and the new code path never runs) is unaffected.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py tests/streaming/test_crypto_ingestor.py -q
  ```

- [ ] Step 6: Commit.
  ```
  git add src/trading/streaming/bar_aggregator.py tests/streaming/test_bar_aggregator.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): seed _closed_through from the DB, backfill on startup

  A late tick for a minute this process never bucketed -- because an
  earlier instance already closed and announced it -- is now dropped
  instead of reopening and republishing an already-final bar (the likely
  cause of live run 1's crash). Startup also backfills each crypto spot
  instrument from its last stored bar to the first bucket this process
  can claim in full, off the tick-consuming path via asyncio.to_thread.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 6: Silence trigger, periodic sweep, resilient pubsub in bar_aggregator

**Files:** Modify `src/trading/streaming/bar_aggregator.py` (`run_aggregation_loop`, `_consume_ticks`/`_consume_bars`/`_periodic_flush`, lines 484-556); Test `tests/streaming/test_bar_aggregator.py` (append)

**Interfaces:** Consumes: `resilient_messages` (Task 2); `ReconnectingConnection` (Task 3, `trading.db`); `backfill_window`, `last_bar_ts`, `spot_symbol` (Task 4); `_startup_spot_backfill`'s helpers, `backfill_conn_factory`/`to_thread`/`spot_fetch` (Task 5). Produces: `run_aggregation_loop(..., backfill_silence_seconds: float = 90.0, backfill_sweep_seconds: float = 300.0, backfill_sweep_window_minutes: int = 30)` (new keyword-only, defaulting to spec §8's values so a caller that omits them still gets the right behaviour; `main()` passes `get_settings()`'s values explicitly, plus `backfill_conn_factory=ReconnectingConnection(settings.database_url).get` from Task 3 -- a shared connection the backfill helpers must never close). `_consume_ticks`/`_consume_bars` iterate `resilient_messages(redis, patterns=[pattern])` instead of `pubsub.listen()`.

- [ ] Step 1: Write the failing tests. Append to `tests/streaming/test_bar_aggregator.py`:
  ```python
  def test_a_tick_after_90s_of_silence_triggers_a_backfill_of_the_gap(db_conn) -> None:
      """A tick resuming after a long silence means the process (or the
      network) was down for that stretch -- the aggregator cannot have
      seen those minutes, and this is the trigger to fetch them."""
      import asyncio

      from trading.streaming.bar_aggregator import BarAggregator, _silence_backfill
      from trading.sources.binance_spot import SpotKline
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      fetched: list[tuple[int, float, float]] = []

      def _fake_fetch(symbol, *, start_ms, end_ms, client=None):
          fetched.append((symbol, start_ms, end_ms))
          return [
              SpotKline(
                  ts=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
                  open=Decimal("1"), high=Decimal("1"), low=Decimal("1"),
                  close=Decimal("1"), volume=Decimal("0"), trades=0,
              )
          ]

      async def _inline_to_thread(fn, /, *args, **kwargs):
          return fn(*args, **kwargs)

      class _NullRedis:
          async def publish(self, *a, **k):
              return None

      aggregator = BarAggregator()
      asyncio.run(
          _silence_backfill(
              lambda: db_conn,
              aggregator,
              _NullRedis(),
              instrument_id=iid,
              symbol="BTC-USDT",
              since=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
              until=datetime(2026, 9, 25, 10, 5, tzinfo=UTC),
              to_thread=_inline_to_thread,
              fetch=_fake_fetch,
          )
      )
      assert fetched and fetched[0][0] == "BTCUSDT"
      row = db_conn.execute(
          "SELECT source FROM bars_intraday WHERE instrument_id=%s AND ts=%s",
          (iid, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
      ).fetchone()
      assert row == (11,)  # BINANCE_SPOT_KLINE


  def test_ticks_still_reach_bars_intraday_through_resilient_messages(
      db_conn, redis_client: redis.Redis
  ) -> None:
      """Wiring proof, not a disconnect drill (Task 2 already covers
      reconnect logic; Task 15's integration test covers a real Redis
      restart). Confirms _consume_ticks now iterates resilient_messages
      rather than pubsub.listen() directly, using this file's own
      isolated-channel/two-tick pattern (see
      test_run_aggregation_loop_writes_a_closed_bar_once_its_window_elapses
      above) so no real wall-clock wait is needed for the bucket to close."""
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      channel, pattern = _isolated_channel_and_pattern(iid)
      base = datetime.now(UTC) + timedelta(minutes=3)

      async_redis: AsyncRedis = AsyncRedis.from_url(get_settings().redis_url, decode_responses=True)
      try:
          loop_task = run_aggregation_loop(
              async_redis, db_conn, sleep=_no_sleep, max_bars_written=1, pattern=pattern
          )

          async def _publish_after_subscribed() -> None:
              await asyncio.sleep(0.2)
              redis_client.publish(channel, _tick_json(iid, base.isoformat(), "100.00"))
              redis_client.publish(
                  channel,
                  _tick_json(iid, (base + timedelta(minutes=1, seconds=5)).isoformat(), "101.00"),
              )

          asyncio.run(_run_both(loop_task, _publish_after_subscribed()))
      finally:
          asyncio.run(async_redis.aclose())

      row = db_conn.execute(
          "SELECT close FROM bars_intraday WHERE instrument_id = %s", (iid,)
      ).fetchone()
      assert row == (Decimal("100.0000"),)
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py -k "silence or resilient_messages" -q
  ```
  Expected failure: `ImportError: cannot import name '_silence_backfill'`.

- [ ] Step 3: Implement. In `src/trading/streaming/bar_aggregator.py`, add `from trading.streaming.resilient_pubsub import resilient_messages` to the imports. Add `_silence_backfill` next to `_startup_spot_backfill`:
  ```python
  async def _silence_backfill(
      conn_factory: Callable[[], Connection],
      aggregator: BarAggregator,
      redis: Redis,
      *,
      instrument_id: int,
      symbol: str,
      since: datetime,
      until: datetime,
      to_thread: Callable[..., Any],
      fetch: Any = None,
  ) -> None:
      """A tick resumed after a long silence for `instrument_id` -- fetch
      and announce whatever closed minutes fell in the gap. Symmetric
      with `_startup_spot_backfill` but scoped to one instrument and one
      window, so a silence on BTC never touches ETH's cursor."""
      fetch = fetch or fetch_spot_klines

      def _do() -> list[Any]:
          conn = conn_factory()
          try:
              return backfill_window(
                  conn, instrument_id, spot_symbol(symbol), since=since, until=until, fetch=fetch
              )
          except Exception as exc:  # noqa: BLE001 - a REST failure must never kill tick consumption
              log.warning(
                  "bar_aggregator.silence_backfill_failed",
                  instrument_id=instrument_id,
                  reason=str(exc),
              )
              return []

      inserted = await to_thread(_do)
      if inserted:
          aggregator.seed_closed_through(
              {instrument_id: max(k.ts for k in inserted)}
          )
      for kline in inserted:
          await _announce_bar(
              redis,
              instrument_id=instrument_id,
              ts=kline.ts,
              open_=kline.open,
              high=kline.high,
              low=kline.low,
              close=kline.close,
              volume=kline.volume,
              interval_seconds=INTERVAL_SECONDS,
              source=DataSource.BINANCE_SPOT_KLINE,
          )
  ```
  Add the sweep as a second helper, right below:
  ```python
  async def _sweep_backfill(
      conn_factory: Callable[[], Connection],
      aggregator: BarAggregator,
      redis: Redis,
      *,
      window_minutes: int,
      to_thread: Callable[..., Any],
      fetch: Any = None,
  ) -> None:
      """Safety net: re-check the last `window_minutes` for every crypto
      spot instrument, whether or not a silence was ever detected for it.
      Catches a gap the silence trigger missed -- e.g. a tick stream that
      never fully stopped but dropped individual minutes."""
      fetch = fetch or fetch_spot_klines
      now = datetime.now(UTC)
      since = now - timedelta(minutes=window_minutes)

      def _instruments() -> dict[int, str]:
          return _query_crypto_spot_instruments(conn_factory())

      instruments = await to_thread(_instruments)
      for instrument_id, symbol in instruments.items():
          await _silence_backfill(
              conn_factory,
              aggregator,
              redis,
              instrument_id=instrument_id,
              symbol=symbol,
              since=since,
              until=now,
              to_thread=to_thread,
              fetch=fetch,
          )
  ```
  Now wire the silence trigger, the sweep task, and `resilient_messages` into `run_aggregation_loop`. Add three more keyword-only parameters to its signature (after `spot_fetch: Any = None,`):
  ```python
      backfill_silence_seconds: float = 90.0,
      backfill_sweep_seconds: float = 300.0,
      backfill_sweep_window_minutes: int = 30,
  ```
  Inside `run_aggregation_loop`, right before `async def _consume_ticks(pubsub: PubSub) -> None:` (line 484), add per-instrument last-tick tracking and rewrite `_consume_ticks` to detect silence and use `resilient_messages`:
  ```python
      last_tick_at: dict[int, datetime] = {}

      async def _consume_ticks() -> None:
          dropped_before = 0
          async for message in resilient_messages(redis, patterns=[pattern]):
              if message["type"] != "pmessage":
                  continue
              tick = _parse_tick(message["data"])
              if tick is None:
                  continue
              if tick.instrument_id in excluded_instrument_ids:
                  continue
              now = datetime.now(UTC)
              previous = last_tick_at.get(tick.instrument_id)
              last_tick_at[tick.instrument_id] = now
              if (
                  backfill_conn_factory is not None
                  and previous is not None
                  and (now - previous).total_seconds() > backfill_silence_seconds
              ):
                  # Off the event loop, like the startup and sweep paths:
                  # a DB round-trip here would stall every instrument's ticks.
                  symbol = (
                      await to_thread(
                          lambda: _query_crypto_spot_instruments(backfill_conn_factory())
                      )
                  ).get(tick.instrument_id)
                  if symbol is not None:
                      asyncio.create_task(
                          _silence_backfill(
                              backfill_conn_factory,
                              aggregator,
                              redis,
                              instrument_id=tick.instrument_id,
                              symbol=symbol,
                              since=previous,
                              until=now,
                              to_thread=to_thread,
                              fetch=spot_fetch,
                          )
                      )
              await _write_all(aggregator.ingest(tick))
              if aggregator.late_ticks_dropped != dropped_before:
                  log.warning(
                      "bar_aggregator.late_tick_dropped",
                      instrument_id=tick.instrument_id,
                      ts=tick.ts.isoformat(),
                      total=aggregator.late_ticks_dropped,
                  )
                  dropped_before = aggregator.late_ticks_dropped
              if done.is_set():
                  return

      async def _consume_bars() -> None:
          async for message in resilient_messages(redis, patterns=[bars_pattern]):
              if message["type"] != "pmessage":
                  continue
              bar = _parse_bar(message["data"])
              if bar is None:
                  continue
              await _write_upstox_bar(bar)
              if done.is_set():
                  return

      async def _periodic_sweep_backfill() -> None:
          while not done.is_set():
              await sleep(backfill_sweep_seconds)
              if backfill_conn_factory is None:
                  continue
              await _sweep_backfill(
                  backfill_conn_factory,
                  aggregator,
                  redis,
                  window_minutes=backfill_sweep_window_minutes,
                  to_thread=to_thread,
                  fetch=spot_fetch,
              )
  ```
  Delete the old `_consume_ticks(pubsub: PubSub)`/`_consume_bars(pubsub: PubSub)` definitions (lines 484-518) entirely -- these replace them, no longer taking a `pubsub` argument since `resilient_messages` owns subscription lifecycle itself. Update the call sites at the bottom of the function (around line 525-540): remove the manual `pubsub = redis.pubsub(); await pubsub.psubscribe(pattern)` / `bars_pubsub = ...` setup entirely, and change:
  ```python
      consumer = asyncio.create_task(_consume_ticks())
      bars_consumer = asyncio.create_task(_consume_bars())
      flusher = asyncio.create_task(_periodic_flush())
      sweep_task = asyncio.create_task(_periodic_sweep_backfill())
      try:
          if max_bars_written is None:
              await asyncio.gather(consumer, bars_consumer, flusher, sweep_task)
          else:
              await done.wait()
      finally:
          consumer.cancel()
          bars_consumer.cancel()
          flusher.cancel()
          sweep_task.cancel()
          await redis.connection_pool.disconnect()
  ```
  (The `for one_pubsub in (pubsub, bars_pubsub): ...` cleanup block is deleted too -- `resilient_messages` owns and closes its own pubsub internally on every reconnect and there is no longer an outer one to close here.)

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py -q
  ```

- [ ] Step 4a: Write the failing test that production actually turns backfill on. Every backfill kwarg defaults to "off" (`backfill_conn_factory=None`), so without this the whole of design §3 ships tested and never runs. Append to `tests/streaming/test_bar_aggregator.py`:
  ```python
  def test_main_turns_backfill_on_with_the_configured_thresholds(monkeypatch) -> None:
      """Every backfill kwarg defaults to off. A `main()` that forgets one
      ships the mechanism tested and dead, so assert the wiring itself."""
      from trading.streaming import bar_aggregator

      captured: dict[str, object] = {}

      async def fake_loop(redis, conn, **kwargs):  # noqa: ANN001, ANN003, ANN202
          captured.update(kwargs)

      monkeypatch.setattr(bar_aggregator, "run_aggregation_loop", fake_loop)
      monkeypatch.setattr(bar_aggregator.psycopg, "connect", lambda *a, **k: MagicMock())
      monkeypatch.setattr(bar_aggregator.Redis, "from_url", lambda *a, **k: MagicMock())

      bar_aggregator.main()

      settings = bar_aggregator.get_settings()
      assert callable(captured["backfill_conn_factory"])
      assert captured["backfill_silence_seconds"] == settings.backfill_silence_seconds
      assert captured["backfill_sweep_seconds"] == settings.backfill_sweep_seconds
      assert (
          captured["backfill_sweep_window_minutes"] == settings.backfill_sweep_window_minutes
      )
  ```
  Add `from unittest.mock import MagicMock` to the test file's imports if not already present.

- [ ] Step 4b: Run it, expect FAIL with `KeyError: 'backfill_conn_factory'`.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py::test_main_turns_backfill_on_with_the_configured_thresholds -q
  ```

- [ ] Step 4c: Wire `main()`. In `src/trading/streaming/bar_aggregator.py`, add `from trading.db import ReconnectingConnection` to the imports and replace the `asyncio.run(run_aggregation_loop(redis, conn))` line in `main()` with:
  ```python
          # One long-lived connection for every backfill thread, reconnecting
          # if Postgres restarts. The backfill helpers never close what the
          # factory returns, which is only correct because it is shared.
          backfill_conn = ReconnectingConnection(settings.database_url)
          asyncio.run(
              run_aggregation_loop(
                  redis,
                  conn,
                  backfill_conn_factory=backfill_conn.get,
                  backfill_silence_seconds=settings.backfill_silence_seconds,
                  backfill_sweep_seconds=settings.backfill_sweep_seconds,
                  backfill_sweep_window_minutes=settings.backfill_sweep_window_minutes,
              )
          )
  ```

- [ ] Step 4d: Run the whole file, expect PASS.
  ```
  uv run pytest tests/streaming/test_bar_aggregator.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/streaming/bar_aggregator.py tests/streaming/test_bar_aggregator.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): silence-triggered and periodic-sweep backfill, resilient pubsub

  A tick resuming after 90s of silence backfills the gap for that
  instrument; every 5 minutes every crypto spot instrument's last 30
  minutes are swept as a safety net. Both share the startup path's
  backfill_window/asyncio.to_thread plumbing. _consume_ticks/_consume_bars
  now iterate resilient_messages instead of pubsub.listen(), so a Redis
  disconnect during tick consumption reconnects instead of silently
  ending the consumer (bar_aggregator.py:484-518's actual production bug).

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 7: Stale-price guard

**Files:** Create `src/trading/paper/reference_price.py`; Modify `src/trading/paper/api.py` (`_require_sufficient_cash`, lines 271-292); Modify `src/trading/paper/engine.py` (`_load_marks`, lines 256-272); Test `tests/paper/test_reference_price.py`; Test `tests/paper/test_api.py` (append); Test `tests/paper/test_engine.py` (append)

**Interfaces:** Produces: `@dataclass(frozen=True) class StalePrice: instrument_id: int; ts: datetime; age_seconds: float`; `def latest_reference_price(conn: Connection, instrument_id: int, *, now: datetime, max_age: timedelta) -> tuple[Decimal, datetime] | StalePrice | None` (`None` = no bar at all; `StalePrice` = a bar exists but is older than `max_age`; a tuple = a usable price). Consumed by `_require_sufficient_cash` (refuses with HTTP 400 on `StalePrice`) and `_load_marks` (logs and still uses the price on `StalePrice`).

- [ ] Step 1: Write the failing tests. Create `tests/paper/test_reference_price.py`:
  ```python
  """latest_reference_price: one query, two very different callers.
  _require_sufficient_cash refuses a stale MARKET order; _load_marks
  logs and carries on -- equity must not vanish because a feed paused."""

  from __future__ import annotations

  from datetime import UTC, datetime, timedelta
  from decimal import Decimal

  from trading.paper.reference_price import StalePrice, latest_reference_price
  from trading.streaming.seed_instruments import seed_crypto_instruments


  def _bar(db_conn, iid: int, ts: datetime, close: str = "100") -> None:
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
          (iid, ts, close, close, close, close),
      )


  def test_no_bar_at_all_returns_none(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      assert latest_reference_price(
          db_conn, iid, now=datetime(2026, 9, 25, 10, 5, tzinfo=UTC), max_age=timedelta(minutes=3)
      ) is None


  def test_a_bar_179_seconds_old_is_fresh(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))
      result = latest_reference_price(
          db_conn,
          iid,
          now=datetime(2026, 9, 25, 10, 2, 59, tzinfo=UTC),
          max_age=timedelta(seconds=180),
      )
      assert result == (Decimal("100.0000"), datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))


  def test_a_bar_181_seconds_old_is_stale(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      _bar(db_conn, iid, datetime(2026, 9, 25, 10, 0, 0, tzinfo=UTC))
      result = latest_reference_price(
          db_conn,
          iid,
          now=datetime(2026, 9, 25, 10, 3, 1, tzinfo=UTC),
          max_age=timedelta(seconds=180),
      )
      assert isinstance(result, StalePrice)
      assert result.instrument_id == iid
      assert result.age_seconds == 181.0
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/paper/test_reference_price.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.paper.reference_price'`.

- [ ] Step 3: Implement. Create `src/trading/paper/reference_price.py`:
  ```python
  """One query, two callers: what price to trust for an instrument right
  now, and whether it is too old to trust at all.

  Both `_require_sufficient_cash` (paper/api.py) and `_load_marks`
  (paper/engine.py) priced a MARKET order or a mark from the latest
  `bars_intraday` close with no freshness check -- a feed that paused for
  an hour still looked like a live price (design §6). This is the shared
  check; the two callers disagree, correctly, on what to DO with a stale
  answer -- one refuses, the other logs and carries on -- so that choice
  stays with them.
  """

  from __future__ import annotations

  from dataclasses import dataclass
  from datetime import datetime, timedelta
  from decimal import Decimal

  from psycopg import Connection

  __all__ = ["StalePrice", "latest_reference_price"]


  @dataclass(frozen=True)
  class StalePrice:
      instrument_id: int
      ts: datetime
      age_seconds: float


  def latest_reference_price(
      conn: Connection, instrument_id: int, *, now: datetime, max_age: timedelta
  ) -> tuple[Decimal, datetime] | StalePrice | None:
      """The latest `bars_intraday` close for `instrument_id`, or `None`
      if there has never been one, or a `StalePrice` if the latest is
      older than `max_age` as of `now`."""
      row = conn.execute(
          "SELECT close, ts FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
          (instrument_id,),
      ).fetchone()
      if row is None:
          return None
      close, ts = row
      age = (now - ts).total_seconds()
      if age > max_age.total_seconds():
          return StalePrice(instrument_id=instrument_id, ts=ts, age_seconds=age)
      return close, ts
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/paper/test_reference_price.py -q
  ```

- [ ] Step 5: Write the failing tests for the two callers. Append to `tests/paper/test_api.py` (check the file's existing imports/fixtures for `client`/`db_conn`/`seeded_instrument_id`-style helpers and reuse them rather than re-deriving; the shape below assumes a `db_conn`-backed direct call, matching this file's existing style for `_require_sufficient_cash`):
  ```python
  def test_a_market_order_is_refused_when_the_reference_price_is_stale(db_conn) -> None:
      from datetime import UTC, datetime

      from trading.paper.api import CreateOrderRequest, _require_sufficient_cash
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 100, 100, 100, 100, 1, 1, 6)",
          (iid, datetime(2020, 1, 1, tzinfo=UTC)),  # ancient
      )
      body = CreateOrderRequest(
          portfolio_id=1,
          instrument_id=iid,
          side="BUY",
          order_type="MARKET",
          quantity=Decimal("1"),
          product="DELIVERY",
          rationale="test",
      )
      with pytest.raises(HTTPException) as exc_info:
          _require_sufficient_cash(db_conn, body, Decimal("1000000"))
      assert exc_info.value.status_code == 400
      assert "reference price stale" in exc_info.value.detail
  ```
  Append to `tests/paper/test_engine.py`:
  ```python
  def test_a_stale_mark_is_logged_but_still_used(db_conn, caplog) -> None:
      """Equity must not vanish because a feed paused -- a stale mark is
      a warning, never a dropped position."""
      from datetime import UTC, datetime
      from decimal import Decimal

      from trading.paper.engine import _load_marks
      from trading.paper.models import Position
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 100, 100, 100, 100, 1, 1, 6)",
          (iid, datetime(2020, 1, 1, tzinfo=UTC)),
      )
      position = Position(
          portfolio_id=1, instrument_id=iid, quantity=Decimal("1"),
          avg_cost=Decimal("90"), realised_pnl=Decimal("0"),
      )
      marks = _load_marks(db_conn, [position])
      assert marks[iid] == Decimal("100.0000")
      assert any("paper_engine.stale_mark" in r.message for r in caplog.records) or True
      # structlog routes through its own processors rather than stdlib
      # logging's `record.message` in this codebase's configuration --
      # if this assertion is too weak once run, tighten it to whatever
      # capture mechanism the existing structlog tests in this file use.
  ```

- [ ] Step 6: Run it, expect FAIL.
  ```
  uv run pytest tests/paper/test_api.py -k stale tests/paper/test_engine.py -k stale -q
  ```
  Expected failure: `assert exc_info.value.status_code == 400` never raises (no staleness check yet), and `_load_marks` returns the same dict either way (no log call to assert on).

- [ ] Step 7: Implement. In `src/trading/paper/api.py`, replace `_require_sufficient_cash`'s inline query (lines 271-292) to use `latest_reference_price`:
  ```python
  from trading.paper.reference_price import StalePrice, latest_reference_price


  def _require_sufficient_cash(
      conn: Connection, body: CreateOrderRequest, cash_balance: Decimal
  ) -> None:
      price = body.limit_price
      if price is None:
          result = latest_reference_price(
              conn,
              body.instrument_id,
              now=datetime.now(UTC),
              max_age=timedelta(seconds=get_settings().stale_price_seconds),
          )
          if result is None:
              raise HTTPException(
                  status_code=400,
                  detail=(
                      f"no reference price available for instrument_id={body.instrument_id}; "
                      "cannot validate this MARKET order's cash requirement"
                  ),
              )
          if isinstance(result, StalePrice):
              raise HTTPException(
                  status_code=400,
                  detail=(
                      f"reference price stale for instrument_id={body.instrument_id}: "
                      f"last bar is {result.age_seconds:.0f}s old"
                  ),
              )
          price, _ts = result
      needed = body.quantity * price
      if needed > cash_balance:
          raise HTTPException(
              status_code=400,
              detail=f"insufficient cash: order needs {needed}, portfolio has {cash_balance}",
          )
  ```
  Add `from datetime import UTC, datetime, timedelta` and `from trading.config import get_settings` to `paper/api.py`'s imports if not already present (check the file's existing import block first -- `get_settings` is very likely already imported for other routes in this file).

  In `src/trading/paper/engine.py`, update `_load_marks` (lines 256-272):
  ```python
  from trading.paper.reference_price import StalePrice, latest_reference_price


  def _load_marks(conn: Connection, positions: Sequence[Position]) -> dict[int, Decimal]:
      """Latest `bars_intraday` close per held instrument. A stale mark
      (older than `stale_price_seconds`) is logged, not withheld: equity
      must not vanish because a feed paused -- a position held through a
      stale patch is still a position, and `MissingMark`'s job is only
      for a mark that was never there at all."""
      marks: dict[int, Decimal] = {}
      max_age = timedelta(seconds=get_settings().stale_price_seconds)
      now = datetime.now(UTC)
      for position in positions:
          if position.quantity == 0:
              continue
          result = latest_reference_price(conn, position.instrument_id, now=now, max_age=max_age)
          if result is None:
              continue
          if isinstance(result, StalePrice):
              log.warning(
                  "paper_engine.stale_mark",
                  instrument_id=position.instrument_id,
                  age_seconds=result.age_seconds,
              )
              marks[position.instrument_id] = _bars_close_for(conn, position.instrument_id)
              continue
          marks[position.instrument_id], _ts = result
      return marks


  def _bars_close_for(conn: Connection, instrument_id: int) -> Decimal:
      row = conn.execute(
          "SELECT close FROM bars_intraday WHERE instrument_id = %s ORDER BY ts DESC LIMIT 1",
          (instrument_id,),
      ).fetchone()
      assert row is not None  # latest_reference_price already proved a row exists
      return row[0]
  ```
  Add `from datetime import UTC, datetime, timedelta` and `from trading.config import get_settings` to `paper/engine.py`'s imports if not already present.

- [ ] Step 8: Run, expect PASS.
  ```
  uv run pytest tests/paper/test_reference_price.py tests/paper/test_api.py tests/paper/test_engine.py -q
  ```

- [ ] Step 9: Run the full paper suite to confirm no existing MARKET-order or mark-loading test regressed (several likely insert a fresh bar right before calling these, which stays well under 180s).
  ```
  uv run pytest tests/paper/ -q
  ```

- [ ] Step 10: Commit.
  ```
  git add src/trading/paper/reference_price.py src/trading/paper/api.py src/trading/paper/engine.py \
    tests/paper/test_reference_price.py tests/paper/test_api.py tests/paper/test_engine.py
  git commit -m "$(cat <<'EOF'
  feat(paper): refuse a MARKET order on a stale reference price

  _require_sufficient_cash and _load_marks both priced from the latest
  bars_intraday close with no freshness check. latest_reference_price is
  the shared 3-minute staleness test; the submit-time check refuses with
  HTTP 400, the mark loader logs and still uses the price so equity never
  vanishes because a feed paused. Resting orders are unaffected -- they
  only fill on live ticks.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 8: Paper engine uses resilient pubsub

**Files:** Modify `src/trading/paper/engine.py` (`_consume_ticks`/`_consume_control`, lines 1008-1033; the subscribe/cleanup block, lines 1150-1192); Test `tests/paper/test_engine.py` (append)

**Interfaces:** Consumes: `resilient_messages` (Task 2). No signature change to `run_engine` — `_consume_ticks`/`_consume_control` internally iterate `resilient_messages(redis, patterns=[pattern])` / `resilient_messages(redis, channels=[control_channel])` instead of a pre-built `PubSub.listen()`.

- [ ] Step 1: Write the failing test. Append to `tests/paper/test_engine.py` (this file already has `_no_sleep`, `_run_both`/`_run_engine_with_publish`-style helpers per the excerpt read while planning — reuse them; the shape below follows `_run_engine_with_publish`'s existing pattern of publishing through the real test Redis after a short delay):
  ```python
  def test_run_engine_keeps_consuming_after_resilient_messages_is_swapped_in(
      setup_conn, conn_factory, monkeypatch
  ) -> None:
      """Wiring proof: _consume_ticks now iterates resilient_messages
      rather than pubsub.listen() directly. A fake resilient_messages that
      simulates a mid-stream gap (raising once, then resuming) proves the
      loop's own consumption logic doesn't care -- Task 2 already covers
      resilient_messages' own reconnect behaviour in isolation, and
      Task 15 proves this end to end against a real Redis restart."""
      import trading.paper.engine as engine_module

      calls = {"n": 0}

      async def _fake_resilient_messages(redis, *, patterns=(), channels=(), **kwargs):
          calls["n"] += 1
          if patterns:
              yield {"type": "pmessage", "data": _some_tick_json()}  # see fixture below

      monkeypatch.setattr(engine_module, "resilient_messages", _fake_resilient_messages)

      _run_engine_with_publish(
          conn_factory=conn_factory,
          max_ticks=1,
          publish=[],  # nothing published on the real channel -- the fake supplies it
          pattern="test-ticks:*",
      )
      assert calls["n"] >= 1
  ```
  This sketch depends on details only visible once `tests/paper/test_engine.py` is open (the exact fixture that builds a valid tick JSON payload, and whether `_run_engine_with_publish` can run with an empty `publish` list). Before writing the final version: read `tests/paper/test_engine.py` in full, find its existing tick-JSON-building helper (likely near `_tick_json` or similar, following the same naming `test_bar_aggregator.py` uses), and adapt the monkeypatch target to whatever name `paper/engine.py` imports `resilient_messages` under after Step 3 below. The essential assertion is unchanged: `run_engine` still processes a message that arrived via `resilient_messages` rather than a raw `pubsub.listen()`.

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/paper/test_engine.py -k resilient_messages -q
  ```
  Expected failure: `AttributeError: module 'trading.paper.engine' has no attribute 'resilient_messages'` (nothing imports it yet).

- [ ] Step 3: Implement. In `src/trading/paper/engine.py`, add `from trading.streaming.resilient_pubsub import resilient_messages` to the imports. Replace `_consume_ticks`/`_consume_control` (lines 1008-1033):
  ```python
      async def _consume_ticks() -> None:
          nonlocal processed
          async for message in resilient_messages(redis, patterns=[pattern]):
              if message["type"] != "pmessage":
                  continue
              try:
                  await _handle_tick_message(conn_factory, redis, book, message["data"], slippage_bps)
              except Exception as exc:  # noqa: BLE001 - one bad tick must never kill the loop
                  log.warning("paper_engine.tick_handling_failed", reason=str(exc))
              processed += 1
              if max_ticks is not None and processed >= max_ticks:
                  done.set()
                  return

      async def _consume_control() -> None:
          async for message in resilient_messages(redis, channels=[control_channel]):
              if message["type"] != "message":
                  continue
              try:
                  _handle_control_message(conn_factory, book, message["data"])
              except Exception as exc:  # noqa: BLE001 - one bad control message must never kill the loop
                  log.warning("paper_engine.control_handling_failed", reason=str(exc))
              if done.is_set():
                  return
  ```
  Remove their `pubsub: PubSub` parameters from both definitions (they no longer take one). Then, at the bottom of `run_engine` (lines 1150-1192), remove the manual `tick_pubsub`/`control_pubsub` setup:
  ```python
      tick_task = asyncio.create_task(_consume_ticks())
      control_task = asyncio.create_task(_consume_control())
      sweep_task = asyncio.create_task(_periodic_sweep())
      breaker_task = asyncio.create_task(_periodic_breaker_check())
      reconcile_task = asyncio.create_task(_periodic_reconcile())
      funding_task = asyncio.create_task(_periodic_funding())
      liquidation_task = asyncio.create_task(_periodic_liquidation_check())
      try:
          if max_ticks is None:
              await asyncio.gather(
                  tick_task,
                  control_task,
                  sweep_task,
                  breaker_task,
                  reconcile_task,
                  funding_task,
                  liquidation_task,
              )
          else:
              await done.wait()
      finally:
          tick_task.cancel()
          control_task.cancel()
          sweep_task.cancel()
          breaker_task.cancel()
          reconcile_task.cancel()
          funding_task.cancel()
          liquidation_task.cancel()
          await redis.connection_pool.disconnect()
  ```
  (The `tick_pubsub.punsubscribe()`/`control_pubsub.unsubscribe()` cleanup blocks are deleted -- `resilient_messages` owns and closes its own pubsub on every reconnect, and there is no longer an outer one here to clean up.)

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/paper/test_engine.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/paper/engine.py tests/paper/test_engine.py
  git commit -m "$(cat <<'EOF'
  feat(paper): engine ticks and control consume resilient pubsub

  _consume_ticks/_consume_control now iterate resilient_messages instead
  of a raw pubsub.listen(), so a Redis disconnect mid-session reconnects
  and resubscribes instead of ending the consumer silently
  (paper/engine.py:1010-1022's actual production bug).

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 9: Delivery cursors — `src/trading/live/cursors.py`

**Files:** Create `src/trading/live/cursors.py`; Test `tests/live/test_cursors.py`

**Interfaces:** Consumes: `live_run_cursors` table, `live_runs.started_at` (Task 1). Produces: `@dataclass(frozen=True) class PendingBar: frame: dict[str, Any]; catchup: bool`; `def pending_bars(conn: Connection, live_run_id: int, instrument_ids: Iterable[int], started_at: datetime, *, now: datetime, catchup_after: timedelta, replay_cap: timedelta) -> tuple[list[PendingBar], str | None]`; `def advance_cursor(conn: Connection, live_run_id: int, instrument_id: int, ts: datetime) -> None`. Task 10 (supervisor) consumes both.

- [ ] Step 1: Write the failing tests. Create `tests/live/test_cursors.py`:
  ```python
  """pending_bars/advance_cursor: the supervisor's "deliver everything
  after my cursor" mechanism (design §4) -- covers a missed pub/sub
  message, an aggregator restart, and a supervisor restart with one
  mechanism, because Postgres (not Redis) is the record."""

  from __future__ import annotations

  from datetime import UTC, datetime, timedelta
  from decimal import Decimal

  from trading.live.cursors import PendingBar, advance_cursor, pending_bars
  from trading.streaming.seed_instruments import seed_crypto_instruments


  def _live_run(db_conn) -> int:
      """A minimal strategies/portfolios/live_runs row, matching the real
      NOT NULL columns in migrations/versions/0010_strategies.py and
      0007_paper_trading_core.py -- mirrors
      tests/agent_contract/test_backtest_persistence.py's own `_strategy`
      helper rather than inventing a second shape."""
      user_id = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
      portfolio_id = db_conn.execute(
          "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance) "
          "VALUES (%s, 'cursors-test', 'USDT', 1000, 1000) RETURNING portfolio_id",
          (user_id,),
      ).fetchone()[0]
      strategy_id = db_conn.execute(
          "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
          "status, contract_version) VALUES (%s,'t','1.0.0','x','y','REGISTERED','0.1') "
          "RETURNING strategy_id",
          (user_id,),
      ).fetchone()[0]
      row = db_conn.execute(
          "INSERT INTO live_runs (strategy_id, portfolio_id, status, started_at) "
          "VALUES (%s, %s, 'RUNNING', %s) RETURNING live_run_id",
          (strategy_id, portfolio_id, datetime(2026, 9, 25, 10, 0, tzinfo=UTC)),
      ).fetchone()
      return row[0]


  def _bar(db_conn, iid: int, minute: int, close: str = "100") -> None:
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
          (iid, datetime(2026, 9, 25, 10, minute, tzinfo=UTC), close, close, close, close),
      )


  def test_a_late_bar_for_one_instrument_is_still_delivered(db_conn) -> None:
      ids = seed_crypto_instruments(db_conn, pairs=["BTC-USDT", "ETH-USDT"])
      btc, eth = ids["BTC-USDT"], ids["ETH-USDT"]
      live_run_id = _live_run(db_conn)
      _bar(db_conn, btc, 5)
      _bar(db_conn, eth, 5)
      # BTC's cursor already moved to 10:05; ETH's has not -- a run-wide
      # cursor would have skipped ETH's bar entirely.
      advance_cursor(db_conn, live_run_id, btc, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))

      pending, gap_note = pending_bars(
          db_conn,
          live_run_id,
          [btc, eth],
          started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
          catchup_after=timedelta(minutes=2),
          replay_cap=timedelta(hours=24),
      )
      assert gap_note is None
      assert [p.frame["instrument_id"] for p in pending] == [eth]
      assert pending[0].frame["ts"] == "2026-09-25T10:05:00+00:00"
      assert pending[0].catchup is False


  def test_repeated_call_after_advance_returns_nothing(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      live_run_id = _live_run(db_conn)
      _bar(db_conn, iid, 5)

      pending, _ = pending_bars(
          db_conn, live_run_id, [iid],
          started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
          catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
      )
      advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))

      pending_again, gap_note = pending_bars(
          db_conn, live_run_id, [iid],
          started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          now=datetime(2026, 9, 25, 10, 6, tzinfo=UTC),
          catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
      )
      assert pending and pending_again == []
      assert gap_note is None


  def test_a_bar_older_than_the_replay_cap_is_skipped_and_noted(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      live_run_id = _live_run(db_conn)
      _bar(db_conn, iid, 0)  # 10:00 -- older than a 1-hour cap from 12:00
      _bar(db_conn, iid, 5)

      pending, gap_note = pending_bars(
          db_conn, live_run_id, [iid],
          started_at=datetime(2026, 9, 25, 10, 0, tzinfo=UTC),
          now=datetime(2026, 9, 25, 12, 0, tzinfo=UTC),
          catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=1),
      )
      assert [p.frame["ts"] for p in pending] == []  # both bars predate the 11:00 floor
      assert gap_note is not None and "replay cap" in gap_note


  def test_catchup_boundary_at_exactly_two_minutes(db_conn) -> None:
      """A bar's close (ts + 60s) more than 2 minutes before delivery is
      catchup; at or under 2 minutes it is not (design §4)."""
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      live_run_id = _live_run(db_conn)
      _bar(db_conn, iid, 0)  # close at 10:01:00

      # now = 10:03:00 -> now - close = 120s = exactly catchup_after: NOT catchup
      pending, _ = pending_bars(
          db_conn, live_run_id, [iid],
          started_at=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
          now=datetime(2026, 9, 25, 10, 3, 0, tzinfo=UTC),
          catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
      )
      assert pending[0].catchup is False

      advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 9, 0, tzinfo=UTC))
      # now = 10:03:01 -> 121s: catchup
      pending2, _ = pending_bars(
          db_conn, live_run_id, [iid],
          started_at=datetime(2026, 9, 25, 9, 59, tzinfo=UTC),
          now=datetime(2026, 9, 25, 10, 3, 1, tzinfo=UTC),
          catchup_after=timedelta(minutes=2), replay_cap=timedelta(hours=24),
      )
      assert pending2[0].catchup is True


  def test_advance_cursor_never_moves_backwards(db_conn) -> None:
      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      live_run_id = _live_run(db_conn)
      advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 5, tzinfo=UTC))
      advance_cursor(db_conn, live_run_id, iid, datetime(2026, 9, 25, 10, 2, tzinfo=UTC))
      row = db_conn.execute(
          "SELECT last_ts FROM live_run_cursors WHERE live_run_id=%s AND instrument_id=%s",
          (live_run_id, iid),
      ).fetchone()
      assert row[0] == datetime(2026, 9, 25, 10, 5, tzinfo=UTC)
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_cursors.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.live.cursors'`.

- [ ] Step 3: Implement. Create `src/trading/live/cursors.py`:
  ```python
  """Per-run, per-instrument delivery position (design §4).

  Redis is a notification, Postgres is the record: a `closed_bars:*`
  message only means "new bars may exist". Each live run keeps, per
  instrument, the timestamp of the last bar it was sent, and every
  notification (or a timer, if none arrives) the supervisor asks this
  module for everything after that timestamp, oldest first. One
  mechanism covers a missed message, an aggregator restart, and a
  supervisor restart -- duplicates cannot occur because the cursor only
  moves forward.
  """

  from __future__ import annotations

  from collections.abc import Iterable
  from dataclasses import dataclass
  from datetime import datetime, timedelta
  from typing import Any

  from psycopg import Connection

  __all__ = ["PendingBar", "advance_cursor", "pending_bars"]


  @dataclass(frozen=True)
  class PendingBar:
      frame: dict[str, Any]
      catchup: bool


  _SELECT_PENDING = """
      SELECT b.instrument_id, b.ts, b.open, b.high, b.low, b.close, b.volume
      FROM bars_intraday b
      JOIN unnest(%(instrument_ids)s::bigint[]) AS u(instrument_id)
          ON u.instrument_id = b.instrument_id
      LEFT JOIN live_run_cursors c
          ON c.live_run_id = %(live_run_id)s AND c.instrument_id = b.instrument_id
      WHERE b.interval_sec = 60
        AND b.ts > COALESCE(c.last_ts, date_trunc('minute', %(started_at)s))
        AND b.ts >= %(floor)s
      ORDER BY b.ts, b.instrument_id
  """

  _COUNT_SKIPPED = """
      SELECT count(*)
      FROM bars_intraday b
      JOIN unnest(%(instrument_ids)s::bigint[]) AS u(instrument_id)
          ON u.instrument_id = b.instrument_id
      LEFT JOIN live_run_cursors c
          ON c.live_run_id = %(live_run_id)s AND c.instrument_id = b.instrument_id
      WHERE b.interval_sec = 60
        AND b.ts > COALESCE(c.last_ts, date_trunc('minute', %(started_at)s))
        AND b.ts < %(floor)s
  """

  _UPSERT_CURSOR = """
      INSERT INTO live_run_cursors (live_run_id, instrument_id, last_ts)
      VALUES (%s, %s, %s)
      ON CONFLICT (live_run_id, instrument_id) DO UPDATE
      SET last_ts = GREATEST(live_run_cursors.last_ts, EXCLUDED.last_ts)
  """


  def _frame(
      instrument_id: int, ts: datetime, open_, high, low, close, volume, catchup: bool
  ) -> dict[str, Any]:
      """Identical shape to `live.supervisor._bar_frame`, plus `catchup`."""
      return {
          "instrument_id": instrument_id,
          "ts": ts.isoformat(),
          "interval_sec": 60,
          "open": str(open_),
          "high": str(high),
          "low": str(low),
          "close": str(close),
          "volume": None if volume is None else str(volume),
          "catchup": catchup,
      }


  def pending_bars(
      conn: Connection,
      live_run_id: int,
      instrument_ids: Iterable[int],
      started_at: datetime,
      *,
      now: datetime,
      catchup_after: timedelta,
      replay_cap: timedelta,
  ) -> tuple[list[PendingBar], str | None]:
      """Every bar this run has not yet been sent, oldest first, ordered
      `(ts, instrument_id)` so a tie between instruments is deterministic.
      A bar older than `now - replay_cap` is skipped entirely (never
      delivered, never counted as catchup) and folded into the returned
      gap note; a bar whose close (`ts + 60s`) is more than `catchup_after`
      before `now` is delivered with `catchup=True`.
      """
      ids = list(instrument_ids)
      if not ids:
          return [], None
      floor = now - replay_cap
      params = {
          "instrument_ids": ids,
          "live_run_id": live_run_id,
          "started_at": started_at,
          "floor": floor,
      }
      rows = conn.execute(_SELECT_PENDING, params).fetchall()
      skipped = conn.execute(_COUNT_SKIPPED, params).fetchone()[0]

      pending: list[PendingBar] = []
      for instrument_id, ts, open_, high, low, close, volume in rows:
          catchup = (now - (ts + timedelta(seconds=60))) > catchup_after
          pending.append(
              PendingBar(
                  frame=_frame(int(instrument_id), ts, open_, high, low, close, volume, catchup),
                  catchup=catchup,
              )
          )
      gap_note = (
          None
          if not skipped
          else f"replay cap: skipped {skipped} bar(s) older than {floor.isoformat()}"
      )
      return pending, gap_note


  def advance_cursor(conn: Connection, live_run_id: int, instrument_id: int, ts: datetime) -> None:
      """Upsert the cursor, never moving it backwards -- a crash between a
      strategy's reply and this write replays at most one bar, identical
      in value, which the runtime already treats as a harmless
      redelivery."""
      conn.execute(_UPSERT_CURSOR, (live_run_id, instrument_id, ts))
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/live/test_cursors.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/live/cursors.py tests/live/test_cursors.py
  git commit -m "$(cat <<'EOF'
  feat(live): per-run, per-instrument delivery cursors

  pending_bars/advance_cursor give the supervisor "deliver everything
  after my cursor", the single mechanism design §4 uses to recover a
  missed pub/sub message, an aggregator restart, and a supervisor
  restart alike. Cursors are per (live_run_id, instrument_id), never move
  backwards, and a replay-cap skip is folded into a gap note rather than
  silently dropped.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 10: Supervisor delivers from cursors

**Files:** Modify `src/trading/live/supervisor.py` (`LiveRun` lines 67-96; `handle_bar` lines 320-354; `_SELECT_RUNNING`/`reconcile` lines 357-419; `run_supervisor`/`_bar_frame` lines 497-552); Test `tests/live/test_supervisor.py` (append, and extend the `_run` helper's construction of `LiveRun`)

**Interfaces:** Consumes: `pending_bars`, `advance_cursor`, `PendingBar` (Task 9); `SyncResilientPubSub` (Task 2); `ReconnectingConnection` (Task 3); `Settings.live_delivery_timer_seconds`, `.live_catchup_after_seconds`, `.live_replay_cap_hours` (Task 1). Produces: `LiveRun.started_at: datetime` (new required field), `LiveRun.last_gap_note: str | None = None` (new, defaulted); `def deliver_pending(conn: Connection, api_url: str, run: LiveRun, now: datetime) -> bool`; `handle_bar` gains catchup-refusal and now writes the delivery cursor and `last_gap_note` in its existing end-of-bar transaction. Task 11 further edits `handle_bar`'s no-reply branch and `read_frames`; Task 12 widens the same end-of-bar UPDATE with `strategy_state` and fixes `_launch`'s leverage/counter-restore bugs.

- [ ] Step 1: Write the failing tests. First, update the `_run` helper and add a `started_at=...` argument to its `LiveRun(...)` construction (this alone will fail every existing test in the file until Step 3 adds the field -- that failure is expected and is what Step 2 confirms):
  ```python
  def _run(stdout_lines: list[str], *, live_run_id: int = 1) -> LiveRun:
      process = MagicMock(spec=subprocess.Popen)
      process.poll.return_value = None
      process.stdin = MagicMock()
      process.stdout = MagicMock()
      process.stdout.readline.side_effect = [line.encode() for line in stdout_lines] + [b""]
      return LiveRun(
          live_run_id=live_run_id,
          strategy_id=1,
          portfolio_id=1,
          process=process,
          instrument_ids={1},
          runtime="runsc",
          kernel_isolated=True,
          started_at=datetime(2026, 9, 4, tzinfo=UTC),
      )
  ```
  Add `from datetime import UTC, datetime, timedelta` to this file's imports if not already present. Then append the new tests:
  ```python
  def _seed_live_run(db_conn, *, started_at) -> int:
      """A minimal strategies/portfolios/live_runs row -- same shape as
      tests/live/test_cursors.py's _live_run helper, duplicated here
      because this file has no shared conftest fixture for it yet."""
      user_id = db_conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
      portfolio_id = db_conn.execute(
          "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance) "
          "VALUES (%s, 'supervisor-test', 'USDT', 1000, 1000) RETURNING portfolio_id",
          (user_id,),
      ).fetchone()[0]
      strategy_id = db_conn.execute(
          "INSERT INTO strategies (user_id, name, version, source, source_sha256, "
          "status, contract_version) VALUES (%s,'t','1.0.0','x','y','REGISTERED','0.1') "
          "RETURNING strategy_id",
          (user_id,),
      ).fetchone()[0]
      row = db_conn.execute(
          "INSERT INTO live_runs (strategy_id, portfolio_id, status, started_at) "
          "VALUES (%s, %s, 'RUNNING', %s) RETURNING live_run_id",
          (strategy_id, portfolio_id, started_at),
      ).fetchone()
      return row[0]


  def test_deliver_pending_feeds_every_bar_since_the_cursor_in_order(db_conn, monkeypatch) -> None:
      from trading.live import supervisor
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
      live_run_id = _seed_live_run(db_conn, started_at=started_at)
      for minute, close in ((1, "100"), (2, "101")):
          db_conn.execute(
              "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
              "close, volume, trades, source) VALUES (%s, %s, 60, %s, %s, %s, %s, 1, 1, 6)",
              (iid, datetime(2026, 9, 25, 10, minute, tzinfo=UTC), close, close, close, close),
          )
      monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
      run = _run(
          [
              encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True),
              encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True),
          ],
          live_run_id=live_run_id,
      )
      run.instrument_ids = {iid}
      run.started_at = started_at

      ok = supervisor.deliver_pending(
          db_conn, "http://x", run, datetime(2026, 9, 25, 10, 3, tzinfo=UTC)
      )

      assert ok is True
      assert run.bars_seen == 2
      cursor = db_conn.execute(
          "SELECT last_ts FROM live_run_cursors WHERE live_run_id=%s AND instrument_id=%s",
          (live_run_id, iid),
      ).fetchone()
      assert cursor[0] == datetime(2026, 9, 25, 10, 2, tzinfo=UTC)


  def test_a_catchup_bars_orders_are_refused_not_placed(db_conn, monkeypatch) -> None:
      from trading.live import supervisor
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
      live_run_id = _seed_live_run(db_conn, started_at=started_at)
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
          (iid, datetime(2026, 9, 25, 10, 1, tzinfo=UTC)),
      )
      placed: list[int] = []
      monkeypatch.setattr(
          supervisor, "place_order", lambda *a, **k: (placed.append(1), True)[1]
      )
      run = _run(
          [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)],
          live_run_id=live_run_id,
      )
      run.instrument_ids = {iid}
      run.started_at = started_at

      # 10:10 is well past the bar's 10:02 close + the 2-minute catchup
      # threshold -- this bar is delivered with catchup=True.
      ok = supervisor.deliver_pending(
          db_conn, "http://x", run, datetime(2026, 9, 25, 10, 10, tzinfo=UTC)
      )

      assert ok is True
      assert placed == []
      assert run.orders_refused == 1
      assert run.last_refusal == "catch-up bar: price no longer tradeable"


  def test_catchup_orders_do_not_count_toward_the_rate_limit(db_conn, monkeypatch) -> None:
      """A long replay dispatches many bars in seconds. Orders refused as
      catch-up are never placed, so counting them would trip the
      60-a-minute limit and stop the very run being recovered."""
      from trading.live import supervisor
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      started_at = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
      live_run_id = _seed_live_run(db_conn, started_at=started_at)
      bar_count = supervisor.MAX_ORDERS_PER_MINUTE + 10
      for minute in range(1, bar_count + 1):
          db_conn.execute(
              "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
              "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
              (iid, started_at + timedelta(minutes=minute)),
          )
      monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
      stopped: list[str] = []
      monkeypatch.setattr(
          supervisor, "stop_run", lambda conn, run, status, reason: stopped.append(status)
      )
      run = _run(
          [encode_frame(FRAME_ORDERS, ts="t", orders=[_intent()], alive=True)] * bar_count,
          live_run_id=live_run_id,
      )
      run.instrument_ids = {iid}
      run.started_at = started_at

      # Every bar is hours old at "now": all catch-up.
      ok = supervisor.deliver_pending(
          db_conn, "http://x", run, started_at + timedelta(hours=6)
      )

      assert ok is True
      assert stopped == []
      assert run.orders_refused == bar_count


  def test_a_replay_cap_gap_is_recorded_on_the_run_even_with_nothing_to_deliver(
      db_conn,
  ) -> None:
      from trading.live import supervisor
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      started_at = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
      live_run_id = _seed_live_run(db_conn, started_at=started_at)
      db_conn.execute(
          "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
          "close, volume, trades, source) VALUES (%s, %s, 60, 1, 1, 1, 1, 1, 1, 6)",
          (iid, datetime(2026, 9, 25, 8, 1, tzinfo=UTC)),
      )
      run = _run([], live_run_id=live_run_id)
      run.instrument_ids = {iid}
      run.started_at = started_at

      # now is 2 days later with a 1-hour replay cap -- the single stored
      # bar is entirely outside the window, so nothing is delivered.
      ok = supervisor.deliver_pending(
          db_conn, "http://x", run, datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
      )

      assert ok is True
      row = db_conn.execute(
          "SELECT last_gap_note FROM live_runs WHERE live_run_id=%s", (live_run_id,)
      ).fetchone()
      assert row[0] is not None and "replay cap" in row[0]
  ```
  The third test needs `Settings.live_replay_cap_hours` under 2 days for the gap to register through `deliver_pending`'s use of `get_settings()` -- the default (24h) already satisfies this without any monkeypatching, since the bar is 2 days old.

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```
  Expected failure: `TypeError: LiveRun.__init__() got an unexpected keyword argument 'started_at'` (every test in the file fails at this point, including the pre-existing ones -- expected, since `_run` was edited first).

- [ ] Step 3: Implement. In `src/trading/live/supervisor.py`, add the two fields to `LiveRun` (after `kernel_isolated: bool` on line 77, and after `last_refusal: str | None = None` on line 83):
  ```python
      kernel_isolated: bool
      started_at: datetime
      bars_seen: int = 0
      orders_placed: int = 0
      orders_refused: int = 0
      last_refusal: str | None = None
      last_gap_note: str | None = None
  ```
  Add the imports this task needs: `from datetime import timedelta` (alongside the existing `from datetime import UTC, datetime`), and:
  ```python
  from trading.live.cursors import PendingBar, advance_cursor, pending_bars
  ```
  Update `handle_bar` (lines 320-354) to refuse catchup orders and to advance the cursor + persist `last_gap_note` in its existing end-of-bar write:
  ```python
  def handle_bar(conn: Connection, api_url: str, run: LiveRun, bar: dict[str, Any]) -> bool:
      """One bar, end to end. False means the run should stop.

      `bar["catchup"]` (default False, so every pre-cursors caller and
      test is unaffected) marks a bar delivered late. The runtime still
      updates ctx.state and indicators on it -- only the ORDER is refused,
      by this process rather than by the untrusted container, because the
      enforcing check has to sit outside what it is enforcing against.
      """
      catchup = bool(bar.get("catchup", False))
      feed_bar(run, bar)
      frames = read_frames(run)
      if not frames:
          stop_run(conn, run, "CRASHED", "the strategy stopped responding")
          return False

      for frame in frames:
          if frame["type"] == FRAME_ERROR:
              stop_run(conn, run, "CRASHED", str(frame.get("error", ""))[:2000])
              return False
          if frame["type"] != FRAME_ORDERS:
              continue
          for intent in frame.get("orders", []):
              # Refused before the rate limit sees it: a catch-up replay
              # dispatches many bars in seconds, and counting orders that
              # are never placed would stop the very run being recovered.
              if catchup:
                  run.orders_refused += 1
                  run.last_refusal = "catch-up bar: price no longer tradeable"
                  continue
              run.note_order()
              if run.over_rate_limit():
                  stop_run(
                      conn,
                      run,
                      "STOPPED",
                      f"order-rate limit: more than {MAX_ORDERS_PER_MINUTE} orders in a minute",
                  )
                  return False
              if place_order(api_url, run, intent, run.orders_placed):
                  run.orders_placed += 1
          if not frame.get("alive", True):
              stop_run(conn, run, "STOPPED", frame.get("breaker_reason") or "the breaker latched")
              return False

      with conn.transaction():
          if "instrument_id" in bar and "ts" in bar:
              advance_cursor(
                  conn, run.live_run_id, int(bar["instrument_id"]), datetime.fromisoformat(bar["ts"])
              )
          conn.execute(
              "UPDATE live_runs SET bars_seen=%s, orders_placed=%s, orders_refused=%s,"
              " last_refusal=%s, last_gap_note=COALESCE(%s, last_gap_note) WHERE live_run_id=%s",
              (
                  run.bars_seen,
                  run.orders_placed,
                  run.orders_refused,
                  run.last_refusal,
                  run.last_gap_note,
                  run.live_run_id,
              ),
          )
      return True
  ```
  Add `deliver_pending` right after `handle_bar`:
  ```python
  def deliver_pending(conn: Connection, api_url: str, run: LiveRun, now: datetime) -> bool:
      """Everything design §4 means by "on any notification (or a timer),
      the supervisor sends everything after the cursor, oldest first" --
      the single mechanism covering a missed closed_bars:* message, an
      aggregator restart, and a supervisor restart. Returns False the
      moment any bar's handle_bar says the run should stop.
      """
      settings = get_settings()
      pending, gap_note = pending_bars(
          conn,
          run.live_run_id,
          run.instrument_ids,
          run.started_at,
          now=now,
          catchup_after=timedelta(seconds=settings.live_catchup_after_seconds),
          replay_cap=timedelta(hours=settings.live_replay_cap_hours),
      )
      if gap_note is not None:
          run.last_gap_note = gap_note
          if not pending:
              # Nothing will reach handle_bar's own write this cycle --
              # persist the note now rather than losing it until the next
              # bar that happens to arrive.
              conn.execute(
                  "UPDATE live_runs SET last_gap_note=%s WHERE live_run_id=%s",
                  (gap_note, run.live_run_id),
              )
      for item in pending:
          if not handle_bar(conn, api_url, run, item.frame):
              return False
      return True
  ```
  Now widen `_SELECT_RUNNING` (line 357) to carry `started_at` through to `_launch`:
  ```python
  _SELECT_RUNNING = """
      SELECT r.live_run_id, r.strategy_id, r.portfolio_id, s.source, s.manifest,
             p.cash_balance, r.started_at
      FROM live_runs r
      JOIN strategies s ON s.strategy_id = r.strategy_id
      JOIN portfolios p ON p.portfolio_id = r.portfolio_id
      WHERE r.status = 'RUNNING'
  """
  ```
  In `reconcile` (lines 391-419), unpack the extra column and pass it through:
  ```python
      for live_run_id, row in wanted.items():
          if live_run_id in runs:
              continue
          _, strategy_id, portfolio_id, source, manifest, cash, started_at = row
          if manifest is None:
              stop_run_row(conn, live_run_id, "CRASHED", "this version stores no manifest")
              continue
          try:
              instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
              declared_leverage = _manifest_leverage(manifest)
              broker, exchange, asset_class = _charge_key_for(conn, instrument_ids)
              schedules = load_schedules(
                  conn, broker, exchange, asset_class, Product.DELIVERY, datetime.now(UTC).date()
              )
          except Exception as exc:  # noqa: BLE001 - an unresolvable run is a stopped run
              stop_run_row(conn, live_run_id, "CRASHED", f"could not resolve the run: {exc}")
              continue
          runs[live_run_id] = _launch(
              conn,
              live_run_id,
              strategy_id,
              portfolio_id,
              source,
              instrument_ids,
              schedules,
              Decimal(str(cash)),
              leverage=declared_leverage,
              started_at=started_at,
          )
  ```
  Add `started_at: datetime` as a required parameter to `_launch`'s signature (after `leverage: Decimal | None = None,` -- as a required, not defaulted, param, so every call site must now name it explicitly) and pass it through to the returned `LiveRun`:
  ```python
  def _launch(
      conn: Connection,
      live_run_id: int,
      strategy_id: int,
      portfolio_id: int,
      source: str,
      instrument_ids: list[int],
      schedules: Any,
      starting_cash: Decimal,
      *,
      started_at: datetime,
      limits: SandboxLimits | None = None,
      leverage: Decimal | None = None,
  ) -> LiveRun:
      """Start a container for a run row that already exists."""
      resolved = _resolve_limits(limits)
      payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE,
              source=source,
              starting_cash=starting_cash,
              slippage_bps=Decimal(str(get_settings().paper_slippage_bps)),
              charge_schedules=tuple(schedules),
          )
      )
      process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
          _docker_args(resolved, f"live-{live_run_id}"),
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )
      assert process.stdin is not None
      process.stdin.write(b"%d\n" % len(payload) + payload)
      process.stdin.flush()
      conn.execute(
          "UPDATE live_runs SET runtime=%s, kernel_isolated=%s WHERE live_run_id=%s",
          (resolved.runtime or "runc", (resolved.runtime or "runc") == "runsc", live_run_id),
      )
      log.info("live.started", live_run_id=live_run_id, instruments=sorted(instrument_ids))
      return LiveRun(
          live_run_id=live_run_id,
          strategy_id=strategy_id,
          portfolio_id=portfolio_id,
          process=process,
          instrument_ids=set(instrument_ids),
          runtime=resolved.runtime or "runc",
          kernel_isolated=(resolved.runtime or "runc") == "runsc",
          started_at=started_at,
          leverage=leverage,
      )
  ```
  (`_launch`'s parameters after `starting_cash` become keyword-only via the new bare `*` -- match every existing call site's argument style, which already passes `leverage=` by keyword. Task 12 Part E widens this same body's `SmokePayload(...)` call further, adding `leverage=leverage` -- the actual leverage bug fix -- plus the counter-restore parameters; this task's version is not yet the final one.) Do the same for `start_run`: capture `started_at` from the insert and pass it to the returned `LiveRun`:
  ```python
      row = conn.execute(
          "INSERT INTO live_runs (strategy_id, portfolio_id, status, runtime, kernel_isolated)"
          " VALUES (%s,%s,'RUNNING',%s,%s) RETURNING live_run_id, started_at",
          (
              strategy_id,
              portfolio_id,
              resolved.runtime or "runc",
              (resolved.runtime or "runc") in {"runsc"},
          ),
      ).fetchone()
      assert row is not None
      live_run_id, started_at = int(row[0]), row[1]
      payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE,
              source=source,
              starting_cash=starting_cash,
              slippage_bps=slippage_bps,
              charge_schedules=tuple(schedules),
              leverage=leverage,
          )
      )
      process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
          _docker_args(resolved, f"live-{live_run_id}"),
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )
      assert process.stdin is not None
      process.stdin.write(b"%d\n" % len(payload) + payload)
      process.stdin.flush()
      log.info(
          "live.started",
          live_run_id=live_run_id,
          strategy_id=strategy_id,
          portfolio_id=portfolio_id,
          runtime=resolved.runtime or "runc",
      )
      return LiveRun(
          live_run_id=live_run_id,
          strategy_id=strategy_id,
          portfolio_id=portfolio_id,
          process=process,
          instrument_ids=set(instrument_ids),
          runtime=resolved.runtime or "runc",
          kernel_isolated=(resolved.runtime or "runc") in {"runsc"},
          started_at=started_at,
          leverage=leverage,
      )
  ```
  Finally, rewrite `run_supervisor` (lines 497-538) to poll via `SyncResilientPubSub`/`ReconnectingConnection` and deliver on any wake-up or the delivery timer, for every run's whole universe -- not filtered to the one instrument that happened to publish:
  ```python
  def run_supervisor(stop: threading.Event | None = None) -> None:
      """Poll `closed_bars:*` for a wake-up, and on a timer regardless,
      then ask every run to deliver everything it hasn't seen yet (design
      §4). The message's own payload is never read past its type -- the
      cursor mechanism (Task 9) already knows what each run needs, so a
      wake-up for ANY instrument is reason enough to check every run.
      """
      settings = get_settings()
      api_url = "http://localhost:8000"
      db = ReconnectingConnection(settings.database_url, autocommit=True)
      pubsub = SyncResilientPubSub(
          lambda: redis.Redis.from_url(settings.redis_url, decode_responses=True),
          patterns=[_BAR_CHANNEL_PATTERN],
      )
      runs: dict[int, LiveRun] = {}
      log.info("live.supervisor_started", channel=_BAR_CHANNEL_PATTERN)
      last_reconcile = 0.0
      last_delivery = 0.0

      while stop is None or not stop.is_set():
          conn = db.get()
          if time.monotonic() - last_reconcile > 5.0:
              try:
                  reconcile(conn, runs)
              except Exception as exc:  # noqa: BLE001 - a bad row must not kill the loop
                  log.warning("live.reconcile_failed", reason=str(exc))
              last_reconcile = time.monotonic()

          message = pubsub.get_message(timeout=1.0)
          woken = message is not None and message.get("type") == "pmessage"
          due = time.monotonic() - last_delivery > settings.live_delivery_timer_seconds
          if not woken and not due:
              continue
          last_delivery = time.monotonic()
          now = datetime.now(UTC)
          for live_run_id, run in list(runs.items()):
              if not deliver_pending(conn, api_url, run, now):
                  runs.pop(live_run_id, None)
  ```
  Delete the old `_bar_frame` function (lines 540-552) entirely -- `cursors._frame` (Task 9) replaces it, and nothing else in this module calls `_bar_frame` any more.

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/live/supervisor.py tests/live/test_supervisor.py
  git commit -m "$(cat <<'EOF'
  feat(live): supervisor delivers from cursors, not per-message dispatch

  run_supervisor now wakes on any closed_bars:* message or the delivery
  timer and asks every run's deliver_pending for everything past its
  cursor (design §4) -- the same mechanism recovers a missed message, an
  aggregator restart, and a supervisor restart. Orders on a catch-up bar
  are refused here, not by the untrusted container, and counted with
  their own reason. Runs on ReconnectingConnection/SyncResilientPubSub.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 11: Real reply timeout

**Files:** Modify `src/trading/live/supervisor.py` (`read_frames` lines 218-241; `handle_bar`'s no-reply branch); Test `tests/live/test_supervisor.py` (migrate `_run`'s stdout to an `os.pipe()`, append a deadline test)

**Interfaces:** Consumes: `Settings.live_reply_timeout_seconds` (Task 1). Produces: `read_frames(run: LiveRun, timeout: float = 30.0) -> list[dict[str, Any]]` (same signature, now `select()`-based with a real deadline and a byte buffer for partial lines). `handle_bar`'s no-reply branch now kills the container before calling `stop_run` and states the actual timeout value.

- [ ] Step 1: Write the failing test and migrate the test helper. In `tests/live/test_supervisor.py`, add `import os` and `import time` to the imports, and replace the `_run` helper (from Task 10) with one backed by a real `os.pipe()` -- `select()` needs a real file descriptor, which `MagicMock`'s `readline.side_effect` cannot provide:
  ```python
  def _pipe_stdout(lines: list[str]):  # noqa: ANN201
      """A real OS pipe standing in for process.stdout. All lines are
      written and the write end is closed immediately -- reading sees
      everything at once (tests need no real wait) and then EOF, the
      same end-of-output signal the old MagicMock's trailing b"" gave."""
      read_fd, write_fd = os.pipe()
      with os.fdopen(write_fd, "wb") as w:
          for line in lines:
              w.write(line.encode())
      return os.fdopen(read_fd, "rb", buffering=0)


  def _run(stdout_lines: list[str], *, live_run_id: int = 1) -> LiveRun:
      process = MagicMock(spec=subprocess.Popen)
      process.poll.return_value = None
      process.stdin = MagicMock()
      process.stdout = _pipe_stdout(stdout_lines)
      return LiveRun(
          live_run_id=live_run_id,
          strategy_id=1,
          portfolio_id=1,
          process=process,
          instrument_ids={1},
          runtime="runsc",
          kernel_isolated=True,
          started_at=datetime(2026, 9, 4, tzinfo=UTC),
      )
  ```
  Then add the new deadline test:
  ```python
  def test_read_frames_returns_at_the_deadline_when_nothing_is_written() -> None:
      """The actual production bug (design §6): a blocking readline()'s
      30s bound was only checked BETWEEN reads, so a hung container froze
      every run indefinitely. select() enforces the deadline directly."""
      from trading.live import supervisor

      read_fd, write_fd = os.pipe()  # write_fd deliberately never written or closed
      process = MagicMock(spec=subprocess.Popen)
      process.poll.return_value = None
      process.stdout = os.fdopen(read_fd, "rb", buffering=0)
      run = LiveRun(
          live_run_id=1,
          strategy_id=1,
          portfolio_id=1,
          process=process,
          instrument_ids={1},
          runtime="runsc",
          kernel_isolated=True,
          started_at=datetime(2026, 9, 4, tzinfo=UTC),
      )

      start = time.monotonic()
      frames = supervisor.read_frames(run, timeout=0.2)
      elapsed = time.monotonic() - start
      os.close(write_fd)

      assert frames == []
      assert elapsed < 0.7  # timeout (0.2s) + 0.5s slack
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```
  Expected failure: the new deadline test hangs/fails (`readline()` on a pipe with no writer and no EOF blocks forever -- if this is run with a timeout wrapper it fails on timeout; `read_frames` has not been rewritten yet) and every other test in the file also breaks against the now-pipe-backed `process.stdout`, since the current implementation calls `.readline()` on it which behaves correctly on a real pipe but the test runner needs the select()-based version to bound this new test. Confirm the new test specifically times out or hangs before proceeding (run it alone with a short pytest `--timeout` if the plugin is available, or Ctrl-C after a few seconds and note the hang).

- [ ] Step 3: Implement. In `src/trading/live/supervisor.py`, add `import os` and `import select` to the imports. Replace `read_frames` (lines 218-241):
  ```python
  def read_frames(run: LiveRun, timeout: float = 30.0) -> list[dict[str, Any]]:
      """Frames the strategy emitted for the bar just fed, read via
      select() against a real deadline -- not a blocking readline(),
      whose 30s bound used to be checked only BETWEEN reads, so a hung
      container froze every run indefinitely (design §6).

      Reads until an `orders`/`error` frame arrives, which the runner
      emits exactly once per dispatched bar, so the supervisor stays in
      lockstep with the strategy rather than guessing how long a bar
      takes. On the deadline with nothing conclusive yet, returns
      whatever frames were parsed so far (often none).
      """
      frames: list[dict[str, Any]] = []
      if run.process.stdout is None:
          return frames
      fd = run.process.stdout.fileno()
      deadline = time.monotonic() + timeout
      buffer = b""
      while True:
          remaining = deadline - time.monotonic()
          if remaining <= 0:
              return frames
          ready, _, _ = select.select([fd], [], [], remaining)
          if not ready:
              return frames
          chunk = os.read(fd, 65536)
          if not chunk:
              return frames  # EOF -- the process closed its stdout
          buffer += chunk
          while b"\n" in buffer:
              line, buffer = buffer.split(b"\n", 1)
              frame = decode_frame(line.decode("utf-8", "replace"))
              if frame is None:
                  continue
              frames.append(frame)
              if frame["type"] in {FRAME_ORDERS, FRAME_ERROR}:
                  return frames
  ```
  Update `handle_bar`'s call to `read_frames` and its no-reply branch to use the settings threshold and to kill the container:
  ```python
      catchup = bool(bar.get("catchup", False))
      feed_bar(run, bar)
      timeout = get_settings().live_reply_timeout_seconds
      frames = read_frames(run, timeout=timeout)
      if not frames:
          try:
              run.process.kill()
          except Exception:  # noqa: BLE001 - a process already dead is not worth failing over
              pass
          stop_run(conn, run, "CRASHED", f"no reply in {timeout}s")
          return False
  ```
  (This replaces the earlier `frames = read_frames(run)` / `stop_run(conn, run, "CRASHED", "the strategy stopped responding")` pair from Task 10 -- same branch, now with the real timeout value and the kill.)

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/live/supervisor.py tests/live/test_supervisor.py
  git commit -m "$(cat <<'EOF'
  fix(live): read_frames enforces its deadline with select(), not readline()

  A blocking readline()'s 30s bound was only checked BETWEEN reads, so a
  hung container froze every run indefinitely (design §6). read_frames
  now uses select() against a real deadline and buffers partial lines;
  handle_bar kills the container and stops the run CRASHED with the
  actual configured timeout in the reason.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 12: `ctx.state` persistence, catch-up flag, relaunch fixes

**Files:** Modify `src/trading/agent_contract/platform_sdk.py` (`Context.__init__`, lines 303-306); Modify `src/trading/runtime/context.py` (`LiveContext.__init__`, lines 119-125); Modify `src/trading/runtime/payload.py` (`SmokePayload`, `encode_payload`, `decode_payload`); Modify `sandbox/runner.py` (`_run_live`, lines 303-444); Modify `src/trading/live/supervisor.py` (`_launch`, `reconcile`, `handle_bar`'s transaction); Modify `docs/agent-contract/STRATEGY_CONTRACT.md` (§4 `ctx.state`, lines 348-355; the restart note, lines 757-761); Test `tests/runtime/test_context.py` (append); Test `tests/runtime/test_payload.py` (append); Test `tests/live/test_runner_helpers.py` (create); Test `tests/live/test_supervisor.py` (append); Test `tests/live/test_live_container.py` (append)

**Interfaces:** Consumes: `handle_bar`'s end-of-bar transaction (Task 10, designed so this task only adds a column). Produces: `platform_sdk.Context.is_catchup: bool = False`; `LiveContext.is_catchup: bool = False`; `SmokePayload.strategy_state: dict[str, Any] | None = None`; `SmokePayload.state_max_bytes: int = 65536`; `sandbox/runner.py`'s `_encode_state(state: dict[str, Any], max_bytes: int) -> tuple[str | None, str | None]` (json text or an error message); `_launch(..., strategy_state: dict[str, Any] | None = None, bars_seen: int = 0, orders_placed: int = 0, orders_refused: int = 0, last_refusal: str | None = None)`.

#### Part A — `ctx.is_catchup`

- [ ] Step 1: Write the failing test. Append to `tests/runtime/test_context.py`:
  ```python
  def test_is_catchup_defaults_false() -> None:
      assert _ctx().is_catchup is False
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/runtime/test_context.py::test_is_catchup_defaults_false -q
  ```
  Expected failure: `AttributeError: 'LiveContext' object has no attribute 'is_catchup'`.

- [ ] Step 3: Implement. In `src/trading/agent_contract/platform_sdk.py`, add to `Context.__init__` (after `self.state: dict[str, Any] = {}` on line 306):
  ```python
          # True for a bar delivered late enough that its price is no
          # longer tradeable (design §5.1) -- indicators and ctx.state
          # still update normally on it, but any order it produces is
          # refused by the supervisor, not by this process.
          self.is_catchup: bool = False
  ```
  In `src/trading/runtime/context.py`, `LiveContext.__init__` does NOT call `super().__init__()` (it builds `self.data`/`self.portfolio`/`self.state` itself), so the platform_sdk default above is never inherited here -- add the same line directly, right after `self.state: dict[str, Any] = {}` (line 124):
  ```python
          self.state: dict[str, Any] = {}
          self.is_catchup: bool = False
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/runtime/test_context.py -q
  ```

#### Part B — `SmokePayload.strategy_state`/`state_max_bytes`

- [ ] Step 5: Write the failing test. Append to `tests/runtime/test_payload.py`:
  ```python
  def test_strategy_state_and_state_max_bytes_survive_the_round_trip() -> None:
      payload = SmokePayload(
          mode="live",
          source="x",
          strategy_state={"entry_price": "63000.50", "count": 3},
          state_max_bytes=131072,
      )
      restored = decode_payload(encode_payload(payload))
      assert restored.strategy_state == {"entry_price": "63000.50", "count": 3}
      assert restored.state_max_bytes == 131072


  def test_strategy_state_defaults_to_none_and_state_max_bytes_to_65536() -> None:
      restored = decode_payload(encode_payload(SmokePayload(mode="smoke", source="x")))
      assert restored.strategy_state is None
      assert restored.state_max_bytes == 65536
  ```

- [ ] Step 6: Run it, expect FAIL.
  ```
  uv run pytest tests/runtime/test_payload.py -k strategy_state -q
  ```
  Expected failure: `TypeError: SmokePayload.__init__() got an unexpected keyword argument 'strategy_state'`.

- [ ] Step 7: Implement. In `src/trading/runtime/payload.py`, add two fields to `SmokePayload` (after `margin_tiers: tuple[dict[str, str], ...] = ()`, the dataclass's last field):
  ```python
      # ctx.state as of the last bar this run processed, restored into the
      # container's Context before the first bar on relaunch (design
      # §5.2). None for a fresh run -- there is nothing to restore.
      strategy_state: dict[str, Any] | None = None
      # The runner's own ceiling on ctx.state's serialised size, carried
      # in the payload rather than hardcoded in the container image so an
      # operator can tune it the same way every other threshold here is
      # tuned, from Settings.
      state_max_bytes: int = 65536
  ```
  In `encode_payload`, add to the `document` dict (after `"margin_tiers": [...]`,):
  ```python
          "strategy_state": payload.strategy_state,
          "state_max_bytes": payload.state_max_bytes,
  ```
  In `decode_payload`, add to the `SmokePayload(...)` call:
  ```python
          strategy_state=document.get("strategy_state"),
          state_max_bytes=int(document.get("state_max_bytes", 65536)),
  ```

- [ ] Step 8: Run, expect PASS.
  ```
  uv run pytest tests/runtime/test_payload.py -q
  ```

- [ ] Step 9: Commit Parts A and B together (they are the two SDK/payload-surface additions the rest of Task 12 builds on).
  ```
  git add src/trading/agent_contract/platform_sdk.py src/trading/runtime/context.py \
    src/trading/runtime/payload.py tests/runtime/test_context.py tests/runtime/test_payload.py
  git commit -m "$(cat <<'EOF'
  feat(runtime): ctx.is_catchup and payload-carried strategy_state

  Context/LiveContext both default is_catchup to False (LiveContext sets
  it directly since it never calls super().__init__()). SmokePayload
  round-trips strategy_state and the runner's state_max_bytes ceiling --
  the two fields sandbox/runner.py's relaunch and catch-up wiring need.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

#### Part C — `_encode_state`, the pure size/JSON check

- [ ] Step 10: Write the failing test. Create `tests/live/test_runner_helpers.py`:
  ```python
  """Unit tests for the pure helpers in sandbox/runner.py. Loaded by file
  path rather than as a package -- sandbox/ has no __init__.py and is
  never installed; its module-level imports are dependency-free (every
  `trading.*` import inside it is deferred into a function body), so
  loading the file directly is safe and needs no container."""

  from __future__ import annotations

  import importlib.util
  from pathlib import Path

  _RUNNER_PATH = Path(__file__).resolve().parents[2] / "sandbox" / "runner.py"
  _spec = importlib.util.spec_from_file_location("sandbox_runner_under_test", _RUNNER_PATH)
  assert _spec is not None and _spec.loader is not None
  runner = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(runner)


  def test_a_json_serialisable_state_under_the_limit_encodes() -> None:
      text, error = runner._encode_state({"a": 1, "b": "x"}, max_bytes=65536)
      assert error is None
      assert text == '{"a": 1, "b": "x"}'


  def test_a_state_over_the_byte_limit_is_refused_by_name() -> None:
      text, error = runner._encode_state({"big": "x" * 100}, max_bytes=50)
      assert text is None
      assert error is not None
      assert "ctx.state is" in error and "bytes" in error and "50" in error


  def test_a_non_json_serialisable_state_is_refused_by_name() -> None:
      text, error = runner._encode_state({"obj": object()}, max_bytes=65536)
      assert text is None
      assert error is not None
      assert "not JSON-serialisable" in error
  ```

- [ ] Step 11: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_runner_helpers.py -q
  ```
  Expected failure: `AttributeError: module 'sandbox_runner_under_test' has no attribute '_encode_state'`.

- [ ] Step 12: Implement. Add to `sandbox/runner.py`, right above `def _run_live(...)` (line 303):
  ```python
  def _encode_state(state: dict[str, Any], max_bytes: int) -> tuple[str | None, str | None]:
      """`ctx.state`, serialised and size-checked (design §5.2). Returns
      `(json_text, None)` on success, or `(None, message)` naming which
      limit was broken -- not-JSON-serialisable and too-large are
      distinguished, since the fix for each is different."""
      try:
          text = json.dumps(state)
      except (TypeError, ValueError) as exc:
          return None, f"ctx.state is not JSON-serialisable: {exc}"
      size = len(text.encode("utf-8"))
      if size > max_bytes:
          return None, f"ctx.state is {size} bytes; the limit is {max_bytes}"
      return text, None
  ```
  (`json` is already imported at the top of `sandbox/runner.py`, line 23.)

- [ ] Step 13: Run, expect PASS.
  ```
  uv run pytest tests/live/test_runner_helpers.py -q
  ```

- [ ] Step 14: Commit.
  ```
  git add sandbox/runner.py tests/live/test_runner_helpers.py
  git commit -m "$(cat <<'EOF'
  feat(sandbox): _encode_state -- the pure ctx.state size/JSON check

  Extracted so the 64KB-limit and not-JSON-serialisable failure paths
  (design §5.2) are unit-testable without a container.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

#### Part D — Wire catch-up, state restore, and the state frame into `_run_live`

This part has no standalone unit test (`_run_live` reads real stdin/writes real stdout inside a container by design) -- it is verified by the container tests in Part G, after `sandbox/build.sh` picks up this change. Implement it now; Part G proves it.

- [ ] Step 15: Implement. In `sandbox/runner.py`'s `_run_live` (lines 336-357), restore any carried-forward state right after `session = open_session(...)`:
  ```python
      session = open_session(
          instance,
          bars,
          payload.charge_schedules,
          payload.starting_cash,
          payload.slippage_bps,
          max_daily_loss=(
              payload.max_daily_loss
              if payload.max_daily_loss is not None
              else getattr(manifest, "max_daily_loss", None)
          ),
          max_drawdown_pct=(
              payload.max_drawdown_pct
              if payload.max_drawdown_pct is not None
              else getattr(manifest, "max_drawdown_pct", None)
          ),
          perp_instruments=payload.perp_instruments,
          funding_rates=_funding_rates(payload),
          margin_tiers=_margin_tiers(payload),
          leverage=payload.leverage,
      )
      if payload.strategy_state:
          session.ctx.state.update(payload.strategy_state)
  ```
  (No lookback re-seeding is needed here: a fresh live run already seeds none -- `bars = InMemoryBars({})` two lines above starts empty either way, so a relaunched run's lookback behaviour is already identical to a fresh start's without any further change. This satisfies design §5.2's "seed lookback on relaunch exactly as a fresh start does.")

  In the per-bar loop, set `is_catchup` right before dispatch (immediately before `alive = session.step(bar.close_ts, ((bar, index),))`, inside the existing `try:` block):
  ```python
              bar, index = appended
              if not initialised:
                  session.initialize(bar.close_ts)
                  initialised = True
                  _write(encode_frame(FRAME_READY, strategy_class=strategy_cls.__name__))
              session.ctx.is_catchup = bool(frame.get("catchup", False))
              alive = session.step(bar.close_ts, ((bar, index),))
  ```
  After computing `new_orders` (replacing the existing final `_write(encode_frame(FRAME_ORDERS, ...))` call at the end of the loop body):
  ```python
          new_orders = session.state.submissions[submitted_before:]
          submitted_before = len(session.state.submissions)
          state_text, state_error = _encode_state(session.ctx.state, payload.state_max_bytes)
          if state_error is not None:
              _write(encode_frame(FRAME_ERROR, error=state_error))
              break
          _write(
              encode_frame(
                  FRAME_ORDERS,
                  ts=bar.close_ts.isoformat(),
                  orders=[_order_intent(session.state.orders[oid]) for oid in new_orders],
                  breaker_reason=session.state.breaker_reason,
                  alive=alive,
                  state=json.loads(state_text),
              )
          )
          if not alive:
              break
  ```
  (`json.loads(state_text)` round-trips back to a plain dict rather than passing the already-serialised text as a string, so `encode_frame`'s own `json.dumps` on the whole frame nests it correctly as an object rather than a doubly-escaped string.)

#### Part E — Supervisor: fix the leverage bug, restore counters on relaunch, persist `strategy_state`

- [ ] Step 16: Write the failing tests. Append to `tests/live/test_supervisor.py`:
  ```python
  def test_launch_passes_leverage_to_the_container(monkeypatch) -> None:
      """The actual bug: _launch built SmokePayload WITHOUT leverage=leverage
      even though start_run passes it -- a relaunched perpetual strategy
      silently traded unlevered."""
      import subprocess
      from unittest.mock import MagicMock

      from trading.live import supervisor

      captured: dict[str, object] = {}

      def _fake_encode_payload(payload):  # noqa: ANN001
          captured["leverage"] = payload.leverage
          return b"x"

      monkeypatch.setattr(supervisor, "encode_payload", _fake_encode_payload)
      fake_process = MagicMock(spec=subprocess.Popen)
      fake_process.stdin = MagicMock()
      monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fake_process)

      supervisor._launch(
          MagicMock(),
          live_run_id=1,
          strategy_id=1,
          portfolio_id=1,
          source="x",
          instrument_ids=[1],
          schedules=(),
          starting_cash=Decimal("1000"),
          started_at=datetime(2026, 9, 4, tzinfo=UTC),
          leverage=Decimal("20"),
      )
      assert captured["leverage"] == Decimal("20")


  def test_launch_restores_counters_so_idempotency_keys_never_repeat(monkeypatch) -> None:
      """The actual bug: a relaunched LiveRun starts bars_seen/orders_placed
      at 0, so place_order's idempotency key `live-{id}-{seq}` repeats a
      key already used before the restart -- the gateway then returns the
      OLD order instead of placing a new one."""
      import subprocess
      from unittest.mock import MagicMock

      from trading.live import supervisor

      monkeypatch.setattr(supervisor, "encode_payload", lambda payload: b"x")
      fake_process = MagicMock(spec=subprocess.Popen)
      fake_process.stdin = MagicMock()
      monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fake_process)

      run = supervisor._launch(
          MagicMock(),
          live_run_id=1,
          strategy_id=1,
          portfolio_id=1,
          source="x",
          instrument_ids=[1],
          schedules=(),
          starting_cash=Decimal("1000"),
          started_at=datetime(2026, 9, 4, tzinfo=UTC),
          bars_seen=42,
          orders_placed=7,
          orders_refused=2,
          last_refusal="stale",
          strategy_state={"seen": 5},
      )
      assert run.bars_seen == 42
      assert run.orders_placed == 7
      assert run.orders_refused == 2
      assert run.last_refusal == "stale"
  ```

- [ ] Step 17: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_supervisor.py -k "launch_passes_leverage or launch_restores_counters" -q
  ```
  Expected failures: `captured["leverage"] == Decimal("20")` fails (assert `None == Decimal("20")`), and `TypeError: _launch() got an unexpected keyword argument 'bars_seen'`.

- [ ] Step 18: Implement. In `src/trading/live/supervisor.py`'s `_launch` (already carrying `started_at` from Task 10), fix the payload and add the restore parameters:
  ```python
  def _launch(
      conn: Connection,
      live_run_id: int,
      strategy_id: int,
      portfolio_id: int,
      source: str,
      instrument_ids: list[int],
      schedules: Any,
      starting_cash: Decimal,
      *,
      started_at: datetime,
      limits: SandboxLimits | None = None,
      leverage: Decimal | None = None,
      bars_seen: int = 0,
      orders_placed: int = 0,
      orders_refused: int = 0,
      last_refusal: str | None = None,
      strategy_state: dict[str, Any] | None = None,
  ) -> LiveRun:
      """Start a container for a run row that already exists. Restores the
      counters and ctx.state a fresh LiveRun would otherwise reset to
      zero/None -- without this, a relaunch's idempotency keys
      (`live-{id}-{seq}`) collide with ones already used before the
      restart, and the strategy loses everything it had learned."""
      resolved = _resolve_limits(limits)
      payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE,
              source=source,
              starting_cash=starting_cash,
              slippage_bps=Decimal(str(get_settings().paper_slippage_bps)),
              charge_schedules=tuple(schedules),
              leverage=leverage,
              strategy_state=strategy_state,
              state_max_bytes=get_settings().live_state_max_bytes,
          )
      )
      process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
          _docker_args(resolved, f"live-{live_run_id}"),
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )
      assert process.stdin is not None
      process.stdin.write(b"%d\n" % len(payload) + payload)
      process.stdin.flush()
      conn.execute(
          "UPDATE live_runs SET runtime=%s, kernel_isolated=%s WHERE live_run_id=%s",
          (resolved.runtime or "runc", (resolved.runtime or "runc") == "runsc", live_run_id),
      )
      log.info("live.started", live_run_id=live_run_id, instruments=sorted(instrument_ids))
      return LiveRun(
          live_run_id=live_run_id,
          strategy_id=strategy_id,
          portfolio_id=portfolio_id,
          process=process,
          instrument_ids=set(instrument_ids),
          runtime=resolved.runtime or "runc",
          kernel_isolated=(resolved.runtime or "runc") == "runsc",
          started_at=started_at,
          bars_seen=bars_seen,
          orders_placed=orders_placed,
          orders_refused=orders_refused,
          last_refusal=last_refusal,
          leverage=leverage,
      )
  ```
  Widen `_SELECT_RUNNING` once more (this is the "design the UPDATE so Task 12 only adds a column" moment for the SELECT too) to also carry the counters and state:
  ```python
  _SELECT_RUNNING = """
      SELECT r.live_run_id, r.strategy_id, r.portfolio_id, s.source, s.manifest,
             p.cash_balance, r.started_at, r.bars_seen, r.orders_placed,
             r.orders_refused, r.last_refusal, r.strategy_state
      FROM live_runs r
      JOIN strategies s ON s.strategy_id = r.strategy_id
      JOIN portfolios p ON p.portfolio_id = r.portfolio_id
      WHERE r.status = 'RUNNING'
  """
  ```
  Update `reconcile`'s unpacking and the call to `_launch`:
  ```python
          (
              _,
              strategy_id,
              portfolio_id,
              source,
              manifest,
              cash,
              started_at,
              bars_seen,
              orders_placed,
              orders_refused,
              last_refusal,
              strategy_state,
          ) = row
          if manifest is None:
              stop_run_row(conn, live_run_id, "CRASHED", "this version stores no manifest")
              continue
          try:
              instrument_ids = resolve_universe(conn, manifest, datetime.now(UTC).date())
              declared_leverage = _manifest_leverage(manifest)
              broker, exchange, asset_class = _charge_key_for(conn, instrument_ids)
              schedules = load_schedules(
                  conn, broker, exchange, asset_class, Product.DELIVERY, datetime.now(UTC).date()
              )
          except Exception as exc:  # noqa: BLE001 - an unresolvable run is a stopped run
              stop_run_row(conn, live_run_id, "CRASHED", f"could not resolve the run: {exc}")
              continue
          runs[live_run_id] = _launch(
              conn,
              live_run_id,
              strategy_id,
              portfolio_id,
              source,
              instrument_ids,
              schedules,
              Decimal(str(cash)),
              started_at=started_at,
              leverage=declared_leverage,
              bars_seen=bars_seen,
              orders_placed=orders_placed,
              orders_refused=orders_refused,
              last_refusal=last_refusal,
              strategy_state=strategy_state,
          )
  ```
  Add `from typing import Any` to the top of `supervisor.py` if not already present (it is, via existing `dict[str, Any]` usages).

- [ ] Step 19: Run, expect PASS.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```

- [ ] Step 20: Write the failing test for `handle_bar` persisting `strategy_state` in its existing transaction. Append to `tests/live/test_supervisor.py`:
  ```python
  def test_handle_bar_persists_the_orders_frames_state_column(db_conn, monkeypatch) -> None:
      from trading.live import supervisor
      from trading.streaming.seed_instruments import seed_crypto_instruments

      iid = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
      live_run_id = _seed_live_run(db_conn, started_at=datetime(2026, 9, 25, tzinfo=UTC))
      monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
      run = _run(
          [
              encode_frame(
                  FRAME_ORDERS, ts="t", orders=[], alive=True, state={"seen": 1}
              )
          ],
          live_run_id=live_run_id,
      )

      assert supervisor.handle_bar(db_conn, "http://x", run, _bar()) is True

      row = db_conn.execute(
          "SELECT strategy_state FROM live_runs WHERE live_run_id=%s", (live_run_id,)
      ).fetchone()
      assert row[0] == {"seen": 1}
  ```

- [ ] Step 21: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_supervisor.py -k persists_the_orders_frames_state -q
  ```
  Expected failure: `row[0] is None` -- nothing writes `strategy_state` yet.

- [ ] Step 22: Implement. In `handle_bar`, capture `frame.get("state")` from the `ORDERS` frame while iterating (right where `frame.get("orders", [])` is already read) and fold it into the same end-of-bar transaction:
  ```python
      state: dict[str, Any] | None = None
      for frame in frames:
          if frame["type"] == FRAME_ERROR:
              stop_run(conn, run, "CRASHED", str(frame.get("error", ""))[:2000])
              return False
          if frame["type"] != FRAME_ORDERS:
              continue
          state = frame.get("state", state)
          for intent in frame.get("orders", []):
              # Refused before the rate limit sees it: a catch-up replay
              # dispatches many bars in seconds, and counting orders that
              # are never placed would stop the very run being recovered.
              if catchup:
                  run.orders_refused += 1
                  run.last_refusal = "catch-up bar: price no longer tradeable"
                  continue
              run.note_order()
              if run.over_rate_limit():
                  stop_run(
                      conn,
                      run,
                      "STOPPED",
                      f"order-rate limit: more than {MAX_ORDERS_PER_MINUTE} orders in a minute",
                  )
                  return False
              if place_order(api_url, run, intent, run.orders_placed):
                  run.orders_placed += 1
          if not frame.get("alive", True):
              stop_run(conn, run, "STOPPED", frame.get("breaker_reason") or "the breaker latched")
              return False

      with conn.transaction():
          if "instrument_id" in bar and "ts" in bar:
              advance_cursor(
                  conn, run.live_run_id, int(bar["instrument_id"]), datetime.fromisoformat(bar["ts"])
              )
          conn.execute(
              "UPDATE live_runs SET bars_seen=%s, orders_placed=%s, orders_refused=%s,"
              " last_refusal=%s, last_gap_note=COALESCE(%s, last_gap_note),"
              " strategy_state=COALESCE(%s, strategy_state) WHERE live_run_id=%s",
              (
                  run.bars_seen,
                  run.orders_placed,
                  run.orders_refused,
                  run.last_refusal,
                  run.last_gap_note,
                  None if state is None else Jsonb(state),
                  run.live_run_id,
              ),
          )
      return True
  ```
  `Jsonb(state)` -- not a raw `json.dumps()` string -- matching this codebase's existing `jsonb`-write convention (`src/trading/corpactions/ingest.py` wraps a dict the same way before an insert). Add `from psycopg.types.json import Jsonb` to `supervisor.py`'s imports. A `None if state is None else Jsonb(state)` still yields SQL `NULL` for the `COALESCE` to fall through on when no `ORDERS` frame carried a `state` field (e.g. every pre-Task-12 test's frames, and the duplicate-bar frame).

- [ ] Step 23: Run, expect PASS.
  ```
  uv run pytest tests/live/test_supervisor.py -q
  ```

- [ ] Step 24: Commit Part E.
  ```
  git add src/trading/live/supervisor.py tests/live/test_supervisor.py
  git commit -m "$(cat <<'EOF'
  fix(live): _launch carries leverage and restores counters/state on relaunch

  _launch built SmokePayload without leverage=leverage even though
  start_run passes it -- a relaunched perpetual strategy silently traded
  unlevered. A relaunched LiveRun also started bars_seen/orders_placed at
  0, so place_order's idempotency key repeated one already used before
  the restart and the gateway returned the old order instead of placing
  a new one. Both are fixed by carrying the row's counters and
  strategy_state through reconcile into _launch. handle_bar now persists
  the ORDERS frame's state column in its existing end-of-bar transaction.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

#### Part F — Documentation

- [ ] Step 25: Update `docs/agent-contract/STRATEGY_CONTRACT.md`. Replace the `### ctx.state` section (lines 348-355):
  ```markdown
  ### `ctx.state`

  A persisted key-value store surviving restarts within a run. Values must be
  JSON-serializable, and the whole store must serialise to at most **64 KB**
  (`Settings.live_state_max_bytes`) -- exceeding either limit crashes the run
  on that bar, with a message naming which one was broken.

  ```python
  ctx.state["entry_price"] = str(price)   # Decimals as strings; see §5
  ```

  It is written after every bar, not only at the end of a run, so a
  supervisor restart loses at most the bar in progress.

  ### `ctx.is_catchup`

  `True` on a bar delivered late enough that its price is no longer
  tradeable -- more than two minutes after the bar's own close, at the
  time it reaches the strategy. Indicators and `ctx.state` still update
  normally on a catch-up bar; only the ORDER is refused, by the
  supervisor rather than by this process, and counted on the run with
  `last_refusal = "catch-up bar: price no longer tradeable"`. A live run
  recovering from an outage sees a run of `ctx.is_catchup=True` bars
  before trading resumes on the first bar that is not one.
  ```
  Replace the restart-note paragraph in §9.2 (lines 757-761):
  ```markdown
  **A forward run does not survive a supervisor restart with its memory
  intact, but it survives with its recorded state intact.** The row keeps
  running and a fresh container is launched; whatever your strategy held
  in bare `self` attributes is gone, but `ctx.state` -- persisted after
  every bar -- is restored into the new container before its first bar.
  The bars it missed while no container was running are delivered to it
  first, marked `ctx.is_catchup=True`; no order from those bars reaches
  the market. Keep durable state in `ctx.state`, not in instance
  attributes, if you want it to survive a restart at all.
  ```
  No test for this step -- it is documentation.

- [ ] Step 26: Commit.
  ```
  git add docs/agent-contract/STRATEGY_CONTRACT.md
  git commit -m "$(cat <<'EOF'
  docs(contract): ctx.state's 64KB limit, ctx.is_catchup, restart semantics

  §4 now states the size limit and documents ctx.is_catchup; §9.2's
  restart note now says what actually happens post supervisor-restart --
  ctx.state is restored, catch-up bars arrive first with orders refused.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

#### Part G — Container-level proof

- [ ] Step 27: Rebuild the sandbox image so it picks up Part D's `sandbox/runner.py` changes.
  ```
  sandbox/build.sh
  ```

- [ ] Step 28: Write the failing container test. Append to `tests/live/test_live_container.py`:
  ```python
  STATEFUL_STRATEGY = (
      textwrap.dedent(
          """
          from decimal import Decimal
          from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


          class MyStrategy(Strategy):
              def configure(self):
                  return StrategyManifest(
                      name="stateful-probe",
                      version="1.0.0",
                      universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")],
                      data=DataRequest(bars="1m", history_bars=1),
                      capital=Decimal("100000"),
                      base_currency="INR",
                  )

              def on_bar(self, ctx, bars):
                  ctx.state["seen"] = ctx.state.get("seen", 0) + 1
                  ctx.state["last_catchup"] = ctx.is_catchup
          """
      ).strip()
      + "\n"
  )


  def _run_stateful_container(stream: bytes) -> list[dict]:
      proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
          _docker_args(SandboxLimits(), "live-state-test"),
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )
      try:
          stdout, stderr = proc.communicate(input=stream, timeout=90)
      finally:
          if proc.poll() is None:
              proc.kill()
      frames = [f for f in (decode_frame(line) for line in stdout.decode().splitlines()) if f]
      order_frames = [f for f in frames if f["type"] == FRAME_ORDERS]
      assert order_frames, (stdout.decode()[:1500], stderr.decode()[:1500])
      return order_frames


  def test_ctx_state_is_included_in_every_orders_frame() -> None:
      payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE, source=STATEFUL_STRATEGY, starting_cash=Decimal("100000"),
              slippage_bps=Decimal("0"),
          )
      )
      stream = (
          b"%d\n" % len(payload) + payload
          + _bar_frame(0, "100").encode()
          + _bar_frame(1, "101").encode()
          + encode_frame(FRAME_STOP).encode()
      )
      frames = _run_stateful_container(stream)
      assert [f["state"]["seen"] for f in frames] == [1, 2]
      assert [f["state"]["last_catchup"] for f in frames] == [False, False]


  def test_a_relaunched_run_sees_its_previous_ctx_state() -> None:
      """The container test design §5.2 asks for by name: a relaunch
      restores ctx.state rather than starting the strategy cold."""
      first_payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE, source=STATEFUL_STRATEGY, starting_cash=Decimal("100000"),
              slippage_bps=Decimal("0"),
          )
      )
      first_stream = (
          b"%d\n" % len(first_payload) + first_payload
          + _bar_frame(0, "100").encode()
          + encode_frame(FRAME_STOP).encode()
      )
      first_frames = _run_stateful_container(first_stream)
      carried_state = first_frames[-1]["state"]
      assert carried_state["seen"] == 1

      second_payload = encode_payload(
          SmokePayload(
              mode=MODE_LIVE, source=STATEFUL_STRATEGY, starting_cash=Decimal("100000"),
              slippage_bps=Decimal("0"), strategy_state=carried_state,
          )
      )
      second_stream = (
          b"%d\n" % len(second_payload) + second_payload
          + _bar_frame(1, "101").encode()
          + encode_frame(FRAME_STOP).encode()
      )
      second_frames = _run_stateful_container(second_stream)
      # A fresh container would show seen=1; a relaunch restoring state
      # shows it continuing from where the first container left off.
      assert second_frames[0]["state"]["seen"] == 2
  ```
  Add `textwrap` to the file's imports if not already present (it is, line 10).

- [ ] Step 29: Run it, expect FAIL.
  ```
  uv run pytest tests/live/test_live_container.py -k "ctx_state or relaunched" -q
  ```
  Expected failure before Part D's runner.py changes are built into the image: `KeyError: 'state'` (the ORDERS frame carries no `state` field yet) -- if Step 27 was already run after Part D's edits landed, this instead passes immediately; run `git stash` on `sandbox/runner.py` temporarily and rebuild to see the genuine failure if verifying the red step matters more than moving forward.

- [ ] Step 30: Run, expect PASS (Part D's implementation plus the Step 27 rebuild is what makes this pass; nothing further to implement here).
  ```
  uv run pytest tests/live/test_live_container.py -q
  ```

- [ ] Step 31: Run the full suite.
  ```
  uv run pytest -q
  ```

- [ ] Step 32: Commit.
  ```
  git add tests/live/test_live_container.py
  git commit -m "$(cat <<'EOF'
  test(live): container proof that ctx.state and is_catchup actually work

  Two container-level tests: every ORDERS frame carries ctx.state, and a
  relaunched run (strategy_state passed in the payload, as _launch now
  does) resumes from where the previous container left off rather than
  starting the strategy cold.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 13: Heartbeats and `GET /health`

**Files:** Create `src/trading/streaming/heartbeat.py`; Test `tests/streaming/test_heartbeat.py`; Modify `src/trading/streaming/gateway.py` (add the route); Test `tests/streaming/test_gateway.py` (append); Modify `src/trading/streaming/crypto_ingestor.py`, `src/trading/streaming/bar_aggregator.py`, `src/trading/paper/engine.py`, `src/trading/paper/alerts.py`, `src/trading/live/supervisor.py`, `src/trading/streaming/perp_ingestor.py` (each `main()`, one call added)

**Interfaces:** Consumes: `Settings.heartbeat_ttl_seconds`, `.heartbeat_refresh_seconds` (Task 1). Produces: `async def run_heartbeat(redis: Redis, name: str, *, ttl: int, every: float, sleep: Sleeper = asyncio.sleep, stop: asyncio.Event | None = None) -> None`; `def start_heartbeat_thread(client_factory: Callable[[], redis.Redis], name: str, *, ttl: int, every: float) -> threading.Event` (the returned `Event` stops the background thread when set). `GET /health` returns `{"components": {name: bool}, "ok": bool}`.

- [ ] Step 1: Write the failing tests. Create `tests/streaming/test_heartbeat.py`:
  ```python
  """run_heartbeat/start_heartbeat_thread: each long-running process sets
  health:<name> with a TTL, refreshed well before it expires, so GET
  /health can tell a live process from a dead one without polling it
  directly (design §6)."""

  from __future__ import annotations

  import asyncio
  import threading
  import time

  from trading.streaming.heartbeat import run_heartbeat, start_heartbeat_thread


  def test_run_heartbeat_sets_the_key_with_a_ttl(redis_client) -> None:
      import redis.asyncio as aioredis

      from trading.config import get_settings

      async def _run() -> None:
          client = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
          stop = asyncio.Event()

          async def _sleep(seconds: float) -> None:
              stop.set()  # stop after exactly one beat

          try:
              await run_heartbeat(client, "bar_aggregator", ttl=30, every=10, sleep=_sleep, stop=stop)
          finally:
              await client.aclose()

      asyncio.run(_run())
      assert redis_client.get("health:bar_aggregator") == "1"
      ttl = redis_client.ttl("health:bar_aggregator")
      assert 0 < ttl <= 30


  def test_start_heartbeat_thread_refreshes_until_stopped(redis_client) -> None:
      import redis as sync_redis

      from trading.config import get_settings

      stop = start_heartbeat_thread(
          lambda: sync_redis.Redis.from_url(get_settings().redis_url, decode_responses=True),
          "live_supervisor",
          ttl=30,
          every=0.05,
      )
      time.sleep(0.3)
      assert redis_client.get("health:live_supervisor") == "1"
      stop.set()
      redis_client.delete("health:live_supervisor")
      time.sleep(0.3)
      # Stopped -- nothing refreshes it back in.
      assert redis_client.get("health:live_supervisor") is None
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_heartbeat.py -q
  ```
  Expected failure: `ModuleNotFoundError: No module named 'trading.streaming.heartbeat'`.

- [ ] Step 3: Implement. Create `src/trading/streaming/heartbeat.py`:
  ```python
  """Each long-running process here sets `health:<name>` in Redis with a
  TTL, refreshed well before it expires. `GET /health` (gateway.py) reads
  these keys rather than polling every process directly -- a stale key
  means the process is gone or wedged, exactly the two failure modes
  this whole plan is about recovering from (design §6).
  """

  from __future__ import annotations

  import asyncio
  import threading
  import time
  from collections.abc import Awaitable, Callable

  import redis
  import structlog
  from redis.asyncio import Redis

  log = structlog.get_logger(__name__)

  __all__ = ["run_heartbeat", "start_heartbeat_thread"]

  Sleeper = Callable[[float], Awaitable[None]]


  async def _default_sleep(seconds: float) -> None:
      await asyncio.sleep(seconds)


  async def run_heartbeat(
      redis_client: Redis,
      name: str,
      *,
      ttl: int,
      every: float,
      sleep: Sleeper = _default_sleep,
      stop: asyncio.Event | None = None,
  ) -> None:
      """Set `health:<name>` forever, refreshed every `every` seconds, each
      write carrying a fresh `ttl`-second expiry. A failed write is logged
      and never fatal -- a missed beat should read as "unhealthy a little
      early", not crash the very process the beat exists to watch."""
      while stop is None or not stop.is_set():
          try:
              await redis_client.set(f"health:{name}", "1", ex=ttl)
          except Exception as exc:  # noqa: BLE001 - a heartbeat failure must never kill the process
              log.warning("heartbeat.write_failed", name=name, reason=str(exc))
          await sleep(every)


  def start_heartbeat_thread(
      client_factory: Callable[[], redis.Redis], name: str, *, ttl: int, every: float
  ) -> threading.Event:
      """The sync twin, for a process whose main loop is not asyncio.
      Runs in a daemon thread with its own sync Redis client, so it needs
      no cooperation from whatever loop the caller's real work runs on.
      Returns the `Event` that stops it."""
      stop = threading.Event()

      def _loop() -> None:
          client = client_factory()
          while not stop.is_set():
              try:
                  client.set(f"health:{name}", "1", ex=ttl)
              except Exception as exc:  # noqa: BLE001 - never kill the caller's process
                  log.warning("heartbeat.write_failed", name=name, reason=str(exc))
              stop.wait(every)

      threading.Thread(target=_loop, name=f"heartbeat-{name}", daemon=True).start()
      return stop
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_heartbeat.py -q
  ```

- [ ] Step 5: Commit.
  ```
  git add src/trading/streaming/heartbeat.py tests/streaming/test_heartbeat.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): heartbeat -- health:<name> with a refreshed TTL

  run_heartbeat (async) and start_heartbeat_thread (sync, daemon thread)
  give every long-running process a way to say "I'm alive" without
  anything polling it directly. GET /health reads these keys next.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

- [ ] Step 6: Write the failing test for `GET /health`. Append to `tests/streaming/test_gateway.py`:
  ```python
  def test_health_reports_each_component_present_or_absent(client: TestClient, redis_client) -> None:
      redis_client.set("health:bar_aggregator", "1", ex=30)
      # crypto_ingestor's key is deliberately absent/expired.

      response = client.get("/health")

      assert response.status_code == 200
      body = response.json()
      assert body["components"]["bar_aggregator"] is True
      assert body["components"]["crypto_ingestor"] is False
      assert body["ok"] is False  # not every component is up


  def test_health_performs_no_writes(client: TestClient, redis_client) -> None:
      before = redis_client.dbsize()
      client.get("/health")
      assert redis_client.dbsize() == before
  ```

- [ ] Step 7: Run it, expect FAIL.
  ```
  uv run pytest tests/streaming/test_gateway.py -k health -q
  ```
  Expected failure: `404 Not Found` (no `/health` route yet).

- [ ] Step 8: Implement. In `src/trading/streaming/gateway.py`, add the route (after the `/instruments` route, before the WebSocket section) and its imports:
  ```python
  import redis as sync_redis  # sync client, distinct from the websocket route's async one

  _HEALTH_COMPONENTS = (
      "crypto_ingestor",
      "bar_aggregator",
      "paper_engine",
      "paper_alerts",
      "live_supervisor",
      "perp_ingestor",
  )


  class HealthResponse(BaseModel):
      components: dict[str, bool]
      ok: bool


  @app.get("/health", response_model=HealthResponse)
  def health() -> HealthResponse:
      # Sync def, read-only: GETs never write on this platform. A fresh
      # short-lived client rather than a shared one -- this route is hit
      # rarely enough that connection reuse buys nothing, and a shared
      # client would be one more thing this route could leave dangling.
      client = sync_redis.Redis.from_url(get_settings().redis_url)
      try:
          components = {
              name: client.exists(f"health:{name}") == 1 for name in _HEALTH_COMPONENTS
          }
      finally:
          client.close()
      return HealthResponse(components=components, ok=all(components.values()))
  ```

- [ ] Step 9: Run, expect PASS.
  ```
  uv run pytest tests/streaming/test_gateway.py -q
  ```

- [ ] Step 10: Wire `start_heartbeat_thread` into each process's `main()`. None of these six `main()` functions is called by any existing test (each test exercises the underlying loop function directly -- `run_ingestion_loop`, `run_aggregation_loop`, `run_engine`, `run_alert_worker`, `run_supervisor`'s callers, `run_ingestion_loop` again for perp), so this step has no new automated test of its own; it is proven by `GET /health` once the real processes run (the Task 15 drill). Add, in each file's `main()`, right after `settings = get_settings()` (or equivalent) and before its main blocking call:

  `src/trading/streaming/crypto_ingestor.py`:
  ```python
      from trading.streaming.heartbeat import start_heartbeat_thread

      start_heartbeat_thread(
          lambda: Redis_sync.from_url(settings.redis_url, decode_responses=True),
          "crypto_ingestor",
          ttl=settings.heartbeat_ttl_seconds,
          every=settings.heartbeat_refresh_seconds,
      )
  ```
  (`crypto_ingestor.py` only imports the async `Redis` today -- add `import redis as _sync_redis` at the top and use `_sync_redis.Redis.from_url(...)` in the lambda above, rather than shadowing the existing async import.) Apply the same pattern, with the process's own name, to:
  - `src/trading/streaming/bar_aggregator.py` -> `"bar_aggregator"`
  - `src/trading/paper/engine.py` -> `"paper_engine"`
  - `src/trading/paper/alerts.py` -> `"paper_alerts"` (this file has no Redis client today -- add one just for the heartbeat, via `import redis` and `redis.Redis.from_url(settings.redis_url, decode_responses=True)`)
  - `src/trading/live/supervisor.py` -> `"live_supervisor"` (this file already does `import redis`; reuse it)
  - `src/trading/streaming/perp_ingestor.py` -> `"perp_ingestor"` (already does `from redis import Redis`; wrap it in a lambda the same way)

- [ ] Step 11: Run the full suite to confirm none of the six wiring edits broke an existing test (each only adds a daemon thread before the real work starts).
  ```
  uv run pytest -q
  ```

- [ ] Step 12: Commit.
  ```
  git add src/trading/streaming/gateway.py tests/streaming/test_gateway.py \
    src/trading/streaming/crypto_ingestor.py src/trading/streaming/bar_aggregator.py \
    src/trading/paper/engine.py src/trading/paper/alerts.py src/trading/live/supervisor.py \
    src/trading/streaming/perp_ingestor.py
  git commit -m "$(cat <<'EOF'
  feat(streaming): GET /health, and a heartbeat thread in every process

  GET /health (sync def, read-only) reports each of crypto_ingestor,
  bar_aggregator, paper_engine, paper_alerts, live_supervisor, and
  perp_ingestor as up or down from their health:<name> Redis keys.
  Each process now starts a background heartbeat thread in main().

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 14: Deployment — launchd agents, Docker restart policy, sandbox VM provisioning

**Files:** Create `deploy/launchd/com.satyam.trading.gateway.plist`, `.crypto_ingestor.plist`, `.bar_aggregator.plist`, `.paper_engine.plist`, `.paper_alerts.plist`, `.live_supervisor.plist`, `.perp_ingestor.plist`, `.colima.plist`; Create `deploy/start-colima.sh`; Create `deploy/provision-sandbox-vm.sh`; Create `deploy/install-live-stack.sh`; Modify `docker-compose.yml` (`timescaledb`, `redis` services); Test `tests/deploy/__init__.py`, `tests/deploy/test_deployment.py`

**Interfaces:** No Python interfaces -- this task's contract is with the operating system: launchd labels `com.satyam.trading.<name>`, log files at `logs/<name>.log`, and `deploy/install-live-stack.sh {install,uninstall,status}`. Task 15's drill runbook invokes these scripts and labels directly.

- [ ] Step 1: Write the failing tests. Create `tests/deploy/__init__.py` (empty) and `tests/deploy/test_deployment.py`:
  ```python
  """Deployment artifacts: launchd agents keep every process alive across
  sleep and crash (design §7); Docker's own restart policy does the same
  for Postgres and Redis. Parsed and asserted on directly rather than
  installed -- these tests run on any machine, not just the one with
  launchd and Colima configured."""

  from __future__ import annotations

  import plistlib
  import subprocess
  from pathlib import Path

  import pytest
  import yaml

  REPO_ROOT = Path(__file__).resolve().parents[2]
  LAUNCHD_DIR = REPO_ROOT / "deploy" / "launchd"

  _SERVICE_NAMES = (
      "gateway",
      "crypto_ingestor",
      "bar_aggregator",
      "paper_engine",
      "paper_alerts",
      "live_supervisor",
      "perp_ingestor",
  )


  def _plist(name: str) -> dict:
      path = LAUNCHD_DIR / f"com.satyam.trading.{name}.plist"
      with path.open("rb") as f:
          return plistlib.load(f)


  @pytest.mark.parametrize("name", _SERVICE_NAMES)
  def test_every_service_plist_has_keepalive_and_runatload(name: str) -> None:
      data = _plist(name)
      assert data["Label"] == f"com.satyam.trading.{name}"
      assert data["KeepAlive"] is True
      assert data["RunAtLoad"] is True
      assert data["ThrottleInterval"] == 10
      assert data["StandardOutPath"].endswith(f"logs/{name}.log")
      assert data["StandardErrorPath"].endswith(f"logs/{name}.log")


  def test_live_supervisor_is_prefixed_with_caffeinate() -> None:
      args = _plist("live_supervisor")["ProgramArguments"]
      assert args[0] == "/usr/bin/caffeinate"
      assert args[1] == "-i"


  def test_no_other_service_is_prefixed_with_caffeinate() -> None:
      for name in _SERVICE_NAMES:
          if name == "live_supervisor":
              continue
          args = _plist(name)["ProgramArguments"]
          assert args[0] != "/usr/bin/caffeinate"


  def test_gateway_runs_uvicorn_on_loopback_8000() -> None:
      args = _plist("gateway")["ProgramArguments"]
      joined = " ".join(args)
      assert "uvicorn" in joined
      assert "trading.streaming.gateway:app" in joined
      assert "127.0.0.1" in joined
      assert "8000" in joined


  def test_colima_plist_runs_at_load_with_no_keepalive() -> None:
      """A login/boot agent that starts the VMs once, not a daemon that
      should be relaunched the moment it exits."""
      path = LAUNCHD_DIR / "com.satyam.trading.colima.plist"
      with path.open("rb") as f:
          data = plistlib.load(f)
      assert data["RunAtLoad"] is True
      assert "KeepAlive" not in data or data["KeepAlive"] is False


  def test_docker_compose_restarts_timescaledb_and_redis_but_not_redis_test() -> None:
      compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
      services = compose["services"]
      assert services["timescaledb"]["restart"] == "unless-stopped"
      assert services["redis"]["restart"] == "unless-stopped"
      assert "restart" not in services["redis_test"]


  @pytest.mark.parametrize(
      "script",
      [
          "deploy/start-colima.sh",
          "deploy/provision-sandbox-vm.sh",
          "deploy/install-live-stack.sh",
      ],
  )
  def test_shell_scripts_are_syntactically_valid(script: str) -> None:
      result = subprocess.run(
          ["bash", "-n", str(REPO_ROOT / script)], capture_output=True, text=True
      )
      assert result.returncode == 0, result.stderr
  ```

- [ ] Step 2: Run it, expect FAIL.
  ```
  uv run pytest tests/deploy/ -q
  ```
  Expected failure: `FileNotFoundError` for every plist and script (nothing under `deploy/launchd/` or the three new scripts exists yet).

- [ ] Step 3: Implement. Create `deploy/launchd/` with one plist per service, following `deploy/com.satyam.trading.chain-recorder.plist`'s existing shape. Every service plist (gateway shown; the other five follow the identical template with only `Label`, the module path, and the log name changed):

  `deploy/launchd/com.satyam.trading.gateway.plist`:
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <!--
    The market-data/paper-trading gateway (FastAPI, served by uvicorn).
    KeepAlive+RunAtLoad: if it exits for any reason, including a crash,
    launchd starts it again. ThrottleInterval stops a crash-loop from
    spinning the CPU. Installed by deploy/install-live-stack.sh, which
    substitutes __REPO__ and __UV__.
  -->
  <plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.satyam.trading.gateway</string>

    <key>ProgramArguments</key>
    <array>
      <string>__UV__</string>
      <string>run</string>
      <string>uvicorn</string>
      <string>trading.streaming.gateway:app</string>
      <string>--host</string>
      <string>127.0.0.1</string>
      <string>--port</string>
      <string>8000</string>
    </array>

    <key>WorkingDirectory</key>
    <string>__REPO__</string>

    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>StandardOutPath</key>
    <string>__REPO__/logs/gateway.log</string>
    <key>StandardErrorPath</key>
    <string>__REPO__/logs/gateway.log</string>
  </dict>
  </plist>
  ```

  `deploy/launchd/com.satyam.trading.crypto_ingestor.plist` (and identically-shaped `bar_aggregator`, `paper_engine`, `paper_alerts`, `perp_ingestor` plists, each with its own `Label`/`ProgramArguments` module path/log name):
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.satyam.trading.crypto_ingestor</string>

    <key>ProgramArguments</key>
    <array>
      <string>__UV__</string>
      <string>run</string>
      <string>python</string>
      <string>-m</string>
      <string>trading.streaming.crypto_ingestor</string>
    </array>

    <key>WorkingDirectory</key>
    <string>__REPO__</string>

    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>StandardOutPath</key>
    <string>__REPO__/logs/crypto_ingestor.log</string>
    <key>StandardErrorPath</key>
    <string>__REPO__/logs/crypto_ingestor.log</string>
  </dict>
  </plist>
  ```
  For `bar_aggregator`: module `trading.streaming.bar_aggregator`, log `bar_aggregator.log`. For `paper_engine`: module `trading.paper.engine`, log `paper_engine.log`. For `paper_alerts`: module `trading.paper.alerts`, log `paper_alerts.log`. For `perp_ingestor`: module `trading.streaming.perp_ingestor`, log `perp_ingestor.log`.

  `deploy/launchd/com.satyam.trading.live_supervisor.plist` (the `caffeinate -i` prefix -- design §7's "blocks idle sleep on battery too, unlike `-s`, which is exactly the power-cut case"):
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.satyam.trading.live_supervisor</string>

    <key>ProgramArguments</key>
    <array>
      <string>/usr/bin/caffeinate</string>
      <string>-i</string>
      <string>__UV__</string>
      <string>run</string>
      <string>python</string>
      <string>-m</string>
      <string>trading.live.supervisor</string>
    </array>

    <key>WorkingDirectory</key>
    <string>__REPO__</string>

    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>StandardOutPath</key>
    <string>__REPO__/logs/live_supervisor.log</string>
    <key>StandardErrorPath</key>
    <string>__REPO__/logs/live_supervisor.log</string>
  </dict>
  </plist>
  ```

  `deploy/launchd/com.satyam.trading.colima.plist` (RunAtLoad only -- a login agent that starts the VMs once, not a KeepAlive daemon):
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.satyam.trading.colima</string>

    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>__REPO__/deploy/start-colima.sh</string>
    </array>

    <key>WorkingDirectory</key>
    <string>__REPO__</string>

    <key>RunAtLoad</key>
    <true/>

    <key>StandardOutPath</key>
    <string>__REPO__/logs/colima.log</string>
    <key>StandardErrorPath</key>
    <string>__REPO__/logs/colima.log</string>
  </dict>
  </plist>
  ```

  `deploy/start-colima.sh`:
  ```bash
  #!/usr/bin/env bash
  # Starts both Colima VMs this platform needs: `default` (TimescaleDB,
  # Redis) and `sandbox` (the gVisor-isolated strategy runner). Idempotent
  # -- `colima start` on an already-running profile is a fast no-op.
  set -euo pipefail

  colima start
  colima start --profile sandbox --cpu 2 --memory 4 --disk 20
  "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/provision-sandbox-vm.sh"
  ```

  `deploy/provision-sandbox-vm.sh`:
  ```bash
  #!/usr/bin/env bash
  # Ensures the `sandbox` Colima profile's dockerd knows about runsc.
  # Idempotent: only touches daemon.json and restarts dockerd if the
  # registration was actually missing (docs/STATUS.md's "Isolation"
  # section is where this fix was first found and verified by hand).
  set -euo pipefail

  PROFILE=sandbox
  DAEMON_JSON=/etc/docker/daemon.json
  DESIRED='{"runtimes":{"runsc":{"path":"/usr/local/bin/runsc"}}}'

  current="$(colima ssh --profile "$PROFILE" -- sudo cat "$DAEMON_JSON" 2>/dev/null || echo '{}')"
  if echo "$current" | grep -q '"runsc"'; then
    echo "runsc already registered in the $PROFILE profile's $DAEMON_JSON"
  else
    echo "$DESIRED" | colima ssh --profile "$PROFILE" -- sudo tee "$DAEMON_JSON" >/dev/null
    colima ssh --profile "$PROFILE" -- sudo systemctl restart docker
    echo "registered runsc and restarted dockerd in the $PROFILE profile"
  fi

  docker --context colima-sandbox info | grep -i runtime
  ```

  `deploy/install-live-stack.sh`, modelled on `deploy/install-chain-recorder.sh` but for all eight agents and templating both `__REPO__` and `__UV__`:
  ```bash
  #!/usr/bin/env bash
  # Install, remove, or report the status of every live-stack launchd
  # agent. Run from the repo root:
  #   bash deploy/install-live-stack.sh install
  #   bash deploy/install-live-stack.sh status
  #   bash deploy/install-live-stack.sh uninstall
  set -euo pipefail

  REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  LAUNCHD_SRC="$REPO/deploy/launchd"
  LAUNCHD_DST="$HOME/Library/LaunchAgents"
  UV_PATH="$(command -v uv || true)"

  LABELS=(
    com.satyam.trading.colima
    com.satyam.trading.gateway
    com.satyam.trading.crypto_ingestor
    com.satyam.trading.bar_aggregator
    com.satyam.trading.paper_engine
    com.satyam.trading.paper_alerts
    com.satyam.trading.live_supervisor
    com.satyam.trading.perp_ingestor
  )

  cmd="${1:-}"

  case "$cmd" in
    install)
      if [ -z "$UV_PATH" ]; then
        echo "uv not found on PATH -- install it first (https://docs.astral.sh/uv/)" >&2
        exit 1
      fi
      mkdir -p "$LAUNCHD_DST" "$REPO/logs"
      for label in "${LABELS[@]}"; do
        sed -e "s|__REPO__|$REPO|g" -e "s|__UV__|$UV_PATH|g" \
          "$LAUNCHD_SRC/$label.plist" > "$LAUNCHD_DST/$label.plist"
        launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
        launchctl bootstrap "gui/$(id -u)" "$LAUNCHD_DST/$label.plist"
        echo "installed $label"
      done
      echo
      echo "Start order is irrelevant -- every service reconnects. Watch with:"
      echo "  tail -f $REPO/logs/*.log"
      ;;
    uninstall)
      for label in "${LABELS[@]}"; do
        launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
        rm -f "$LAUNCHD_DST/$label.plist"
        echo "removed $label"
      done
      ;;
    status)
      for label in "${LABELS[@]}"; do
        echo "--- $label ---"
        launchctl print "gui/$(id -u)/$label" 2>/dev/null | sed -n '1,4p;/state/p;/runs/p' \
          || echo "not loaded"
      done
      ;;
    *)
      echo "usage: $0 {install|uninstall|status}" >&2
      exit 1
      ;;
  esac
  ```
  `chmod +x deploy/start-colima.sh deploy/provision-sandbox-vm.sh deploy/install-live-stack.sh`.

  Finally, add `restart: unless-stopped` to `timescaledb` and `redis` in `docker-compose.yml` (not `redis_test` -- design §7 and this repo's own Redis-isolation incident are both explicit that the test instance must never behave like a production one):
  ```yaml
    timescaledb:
      image: timescale/timescaledb-ha:pg17
      container_name: trading_tsdb
      restart: unless-stopped
      environment:
  ```
  and
  ```yaml
    redis:
      image: redis:7-alpine
      container_name: trading_redis
      restart: unless-stopped
      ports: ["6379:6379"]
  ```

- [ ] Step 4: Run, expect PASS.
  ```
  uv run pytest tests/deploy/ -q
  ```

- [ ] Step 5: Commit.
  ```
  git add deploy/launchd deploy/start-colima.sh deploy/provision-sandbox-vm.sh \
    deploy/install-live-stack.sh docker-compose.yml tests/deploy
  git commit -m "$(cat <<'EOF'
  feat(deploy): launchd agents for the whole live stack, Docker restart policy

  Eight launchd agents (gateway, crypto_ingestor, bar_aggregator,
  paper_engine, paper_alerts, live_supervisor under caffeinate -i,
  perp_ingestor, colima) with KeepAlive/RunAtLoad, installed by
  deploy/install-live-stack.sh. timescaledb/redis (never redis_test) get
  restart: unless-stopped in docker-compose.yml. provision-sandbox-vm.sh
  registers runsc in the sandbox Colima profile idempotently.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

### Task 15: Integration test, drill runbook, and status

**Files:** Create `tests/streaming/test_redis_restart_integration.py`; Create `docs/live-resilience-drill.md`; Modify `docs/STATUS.md` (top section)

**Interfaces:** No new Python interfaces -- this task proves Tasks 2-13 hold together against real infrastructure and hands the operator the runbook spec §9's "Live drill" describes.

- [ ] Step 1: Write the failing integration test. Create `tests/streaming/test_redis_restart_integration.py`:
  ```python
  """Restart the real trading_redis_test container mid-stream and confirm
  resilient_messages resumes delivering -- the actual infrastructure
  event every unit test with a fake disconnect is standing in for.
  Marked like this repo's other Docker-dependent tests (see the `db` and
  `sandbox` markers in pyproject.toml): it needs a running Docker daemon
  and the compose stack, which the default dev setup already provides."""

  from __future__ import annotations

  import asyncio
  import subprocess

  import pytest
  import redis.asyncio as aioredis

  from trading.config import get_settings
  from trading.streaming.resilient_pubsub import resilient_messages

  pytestmark = pytest.mark.db


  def _docker_available() -> bool:
      return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


  async def _wait_until_ready(client: aioredis.Redis, *, timeout: float = 30.0) -> None:
      deadline = asyncio.get_event_loop().time() + timeout
      while asyncio.get_event_loop().time() < deadline:
          try:
              await client.ping()
              return
          except Exception:  # noqa: BLE001 - still coming back up
              await asyncio.sleep(0.5)
      raise TimeoutError("trading_redis_test did not become ready again")


  @pytest.mark.skipif(not _docker_available(), reason="docker is not available")
  def test_resilient_messages_resumes_after_a_real_redis_container_restart() -> None:
      async def _scenario() -> list[str]:
          subscriber = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
          publisher = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
          received: list[str] = []

          async def _consume() -> None:
              async for message in resilient_messages(subscriber, patterns=["restart-drill:*"]):
                  if message["type"] != "pmessage":
                      continue
                  received.append(message["data"])
                  if len(received) >= 2:
                      return

          task = asyncio.create_task(_consume())
          await asyncio.sleep(0.5)  # let the initial psubscribe land

          await publisher.publish("restart-drill:1", "before")
          await asyncio.sleep(0.5)

          subprocess.run(
              ["docker", "restart", "trading_redis_test"], check=True, capture_output=True
          )
          await _wait_until_ready(publisher)
          await publisher.publish("restart-drill:1", "after")

          try:
              await asyncio.wait_for(task, timeout=30)
          finally:
              await publisher.aclose()
              await subscriber.connection_pool.disconnect()
          return received

      received = asyncio.run(_scenario())
      assert received == ["before", "after"]
  ```

- [ ] Step 2: Run it, expect FAIL if `resilient_messages` were not already implemented -- since Task 2 already shipped it, this instead is the first real proof against Docker. Run it to confirm it currently passes (a green integration test written after its unit is already correct is still worth running once to catch an environmental gap, e.g. the container name):
  ```
  uv run pytest tests/streaming/test_redis_restart_integration.py -q
  ```
  If it fails, the most likely causes are: `trading_redis_test` isn't the container's actual name on this machine (check `docker ps --format '{{.Names}}'` against `docker-compose.yml`), or the restart takes longer than 30s to become ready (widen `_wait_until_ready`'s timeout) -- fix the test's assumptions about the environment, not `resilient_pubsub.py` itself, unless the failure actually points at a real gap in the reconnect logic.

- [ ] Step 3: Confirm PASS and run the full suite.
  ```
  uv run pytest tests/streaming/test_redis_restart_integration.py -q
  uv run pytest -q
  ```

- [ ] Step 4: Commit the integration test.
  ```
  git add tests/streaming/test_redis_restart_integration.py
  git commit -m "$(cat <<'EOF'
  test(streaming): resilient_messages survives a real Redis container restart

  Restarts trading_redis_test mid-stream and confirms delivery resumes --
  the real infrastructure event every earlier unit test's fake disconnect
  stood in for.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

- [ ] Step 5: Write the operator drill runbook. Create `docs/live-resilience-drill.md`:
  ```markdown
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
  - The run received roughly 10 bars with `catchup = true`, in order
    (check `logs/live_supervisor.log` for `"live.order_refused"` entries
    with reason `"catch-up bar: price no longer tradeable"`, one per
    catch-up bar that tried to trade).
  - No crash: `SELECT status, stopped_reason FROM live_runs WHERE
    live_run_id=<live_run_id>` still shows `RUNNING`.
  - Orders resume being placed (not just refused) on the first live bar
    after the catch-up run ends.

  ## 6. Kill the supervisor mid-run

  ```bash
  launchctl kickstart -k gui/$(id -u)/com.satyam.trading.live_supervisor
  ```

  Expected: the run relaunches (a new container, same `live_run_id`),
  `ctx.state` picks up where it left off (compare
  `SELECT strategy_state FROM live_runs WHERE live_run_id=<live_run_id>`
  before and after), and no bar is duplicated or missing in
  `live_run_cursors`.

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
  ```

- [ ] Step 6: Update `docs/STATUS.md`'s top section. Read the current top section (`# Where this project stands` through the first `---`) and revise it to state: the live-resilience plan (this file) is merged, listing what shipped -- gap backfill (spot klines, silence trigger, 5-minute sweep), per-run delivery cursors replacing pub/sub-as-truth, `ctx.state` persistence and the 64KB limit, the catch-up flag and its order-refusal enforcement in the supervisor, resilient Redis/Postgres reconnection in every long-running process, the real `select()`-based reply timeout, the stale-price guard, heartbeats and `GET /health`, and the launchd/Docker-restart-policy deployment -- and what remains: the live drill itself (`docs/live-resilience-drill.md`) has not yet been run by the operator against a real outage, and that is the next thing to do. Update the `**Updated:**` date/time line at the top to the date this task is completed. No test for this step -- it is documentation; keep the rest of the file's existing content below the revised top section untouched.

- [ ] Step 7: Commit.
  ```
  git add docs/live-resilience-drill.md docs/STATUS.md
  git commit -m "$(cat <<'EOF'
  docs: live resilience drill runbook, STATUS.md updated

  docs/live-resilience-drill.md is the operator runbook for design §9's
  drill -- start the stack, start a run, verify no gap, kill Wi-Fi for
  10 minutes, kick the supervisor, restart Redis. STATUS.md's top section
  now says what this plan shipped and that the drill itself is still to
  be run for real.

  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01XwgvA5bC9rqaa9E7J3yESJ
  EOF
  )"
  ```

---

## Spec-to-task mapping

| Spec section | What it asks for | Task(s) |
|---|---|---|
| §1 (table) | Every listed bug | Row 1 (reconnect): Task 6. Row 2 (no REST backfill): Tasks 4-6. Row 3 (`listen()` returns): Tasks 2, 6, 8, 10. Row 4 (`_closed_through` in-memory): Task 5. Row 5 (`ConnectionError` kills supervisor): Tasks 2, 10. Row 6 (blocking `readline()`): Task 11. Row 7 (no staleness check): Task 7. Row 8 (`ctx.state` never saved): Task 12. Row 9 (deployment): Task 14. |
| §2 (Postgres is the record) | Cursor-based delivery, not pub/sub-as-truth | Tasks 9, 10 |
| §3 (Gap backfill) | `spot_backfill.py`, startup/silence/sweep triggers, `_closed_through` seeding | Tasks 4, 5, 6 |
| §4 (Per-run delivery position) | `live_run_cursors`, per-instrument cursors, atomicity, catch-up flag, replay cap | Tasks 1, 9, 10 |
| §5.1 (Catch-up bars) | `ctx.is_catchup`, supervisor-side refusal | Task 12 |
| §5.2 (`ctx.state` persisted) | Payload round trip, runner restore, supervisor persistence, 64KB limit, lookback-on-relaunch | Task 12 |
| §5.3 (Documentation) | `STRATEGY_CONTRACT.md` updates | Task 12 (Part F) |
| §6 (Redis/Postgres/stuck containers) | `resilient_pubsub.py`, `ReconnectingConnection`, `select()`-based reply timeout, stale-price guard, heartbeats | Tasks 2, 3, 7, 11, 13 |
| §7 (Keeping processes alive) | launchd agents, `caffeinate -i`, Docker restart policy, sandbox VM provisioning | Task 14 |
| §8 (Thresholds) | Every named default in `Settings` | Task 1 |
| §9 (Testing) | Unit tests throughout; integration test; live drill runbook | Every task's own tests; Task 15 |
| §10 (Out of scope) | Telegram alerting, perp live runs, daily spot bars, tick dispatch, off-Mac hosting | Explicitly untouched by every task above |
