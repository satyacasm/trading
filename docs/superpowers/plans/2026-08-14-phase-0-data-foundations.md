# Phase 0 Data Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an idempotent ingestion pipeline that backfills ten years of official NSE, BSE, and AMFI end-of-day data into TimescaleDB behind a canonical instrument master, plus a raw market recorder that begins archiving intraday option-chain frames immediately.

**Architecture:** Six typed pipeline stages — `Source → Parser → Normalizer → InstrumentResolver → Validator → Loader` — composed by a runner that commits data and a job-ledger row in one transaction, making any re-run a provable no-op. Typed models sit at every stage boundary; Polars does the bulk work inside `parse` and `normalize`. The raw market recorder runs on a separate track with zero coupling: it captures broker WebSocket frames to disk and parses nothing.

**Tech Stack:** Python 3.12 (via `uv`) · Polars · Pydantic v2 · psycopg3 · httpx · Alembic · structlog · pytest · ruff · mypy · TimescaleDB (Docker) · Redis

**Spec:** [`docs/superpowers/specs/2026-08-14-phase-0-data-foundations-design.md`](../specs/2026-08-14-phase-0-data-foundations-design.md)
**Format reference:** [`docs/data-formats/eod-source-formats.md`](../../data-formats/eod-source-formats.md) — verified against live downloads; the authority for every column name below.

---

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12** exactly, managed by `uv`. Not the system 3.14 (spec D13).
- **Money is never a float.** `pl.Decimal(18,4)` in frames, `NUMERIC(18,4)` in Postgres. Volumes/OI are `Int64`.
- **Timestamps are timezone-aware UTC** in storage. Exchange-local times are converted at the boundary. `Asia/Kolkata` is the only local zone in Phase 0.
- **Bar `ts` marks the bar's CLOSE time**, never its open (spec §4.5, parent §6).
- **No pandas.** Polars only (spec D14).
- **Nothing is silently dropped.** A row that cannot be loaded goes to `quarantine` with a reason.
- **No network in tests** except tests marked `@pytest.mark.live`, which are excluded from the default run.
- **Every task ends with a passing `pytest` run and a commit.**
- **Column names come from `docs/data-formats/eod-source-formats.md`.** Never invent one; if it is not in that document, download a sample and add it there first.
- **Untraded contracts are real.** ~49% of F&O rows have `OpnPric = HghPric = LwPric = 0.00` with a non-zero `ClsPric`. Never treat these as corrupt (finding F2).
- Lint/type gate: `ruff check . && ruff format --check . && mypy src/trading/contracts` must pass before any commit.

---

## File Structure

```
pyproject.toml                       uv/ruff/mypy/pytest config
docker-compose.yml                   timescaledb + redis (arm64)
.env.example                         documented env template
alembic.ini · migrations/            schema migrations

src/trading/
  config.py                          typed settings from env
  contracts/
    enums.py                         AssetClass, OptionType, DataSource, JobStatus
    models.py                        RawPayload, InstrumentRef, batches, results
    schemas.py                       CANONICAL_BAR_SCHEMA (Polars column contract)
    protocols.py                     the six stage Protocols
    errors.py                        ParseError, FetchError, ValidationAbort
  sources/
    http.py                          shared client: retries, NSE cookie priming
    nse_udiff.py · bse_udiff.py · nse_legacy.py · amfi.py
  parsers/
    registry.py                      ParserRegistry.select() via can_parse()
    udiff.py                         NSE CM + NSE FO + BSE CM (finding F1)
    nse_legacy.py · amfi.py
  normalizers/
    udiff.py · nse_legacy.py · amfi.py
  resolver/
    instruments.py                   InstrumentResolver + abort guard
  validation/
    bars.py                          invariants → valid / quarantined
  loaders/
    bars.py                          COPY to staging → ON CONFLICT upsert
  pipeline/
    runner.py                        Pipeline.run(source, date)
    ledger.py                        ingest_jobs claim/complete
    backfill.py                      BackfillRunner
  calendar/
    trading_days.py                  calendar queries + holiday ingestion
  corpactions/
    ingest.py · adjust.py            events + read-time adjustment factors
  recorder/
    session.py                       manifest, gap events, heartbeat
    upstox_ws.py                     WebSocket capture loop
    __main__.py                      standalone entrypoint

tests/
  fixtures/{udiff,nse_legacy,amfi}/  trimmed REAL files (spec D15)
  contracts/test_parser_contract.py  the shared suite every parser must pass
  ...mirrors src layout
scripts/
  fetch_recon_samples.sh             re-download format samples
```

---

## Task 1: Repo scaffold and running database

**Assignee:** Haiku · **Depends on:** nothing

**Files:**
- Create: `pyproject.toml`, `docker-compose.yml`, `.env.example`, `src/trading/__init__.py`, `src/trading/config.py`, `tests/conftest.py`, `scripts/fetch_recon_samples.sh`
- Modify: `.gitignore` (already exists)

**Interfaces:**
- Consumes: nothing
- Produces: `trading.config.Settings` with fields `database_url: str`, `redis_url: str`, `data_root: Path`, `upstox_api_key: str | None`, `upstox_api_secret: str | None`, `dhan_client_id: str | None`, `dhan_access_token: str | None`; and a module-level `get_settings() -> Settings` (cached).

- [ ] **Step 1: Install uv and pin Python 3.12**

```bash
brew install uv
cd /Users/satyam/claude/trading
uv python install 3.12
uv init --python 3.12 --no-workspace --bare
```

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "trading"
version = "0.1.0"
requires-python = "==3.12.*"
dependencies = [
    "polars>=1.0",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "psycopg[binary,pool]>=3.2",
    "httpx>=0.27",
    "alembic>=1.13",
    "sqlalchemy>=2.0",
    "structlog>=24.1",
    "websockets>=12.0",
]

[dependency-groups]
dev = ["pytest>=8.2", "pytest-cov>=5.0", "ruff>=0.5", "mypy>=1.10"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-m 'not live'"
markers = [
    "live: hits the real network; excluded from the default run",
    "db: requires a running TimescaleDB",
]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.mypy]
python_version = "3.12"
strict = true

[tool.hatch.build.targets.wheel]
packages = ["src/trading"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 3: Write `docker-compose.yml`**

Both images have native `arm64` builds, matching the M5 dev machine and the Ampere A1 server (spec D17) — no emulation.

```yaml
services:
  timescaledb:
    image: timescale/timescaledb-ha:pg17
    container_name: trading_tsdb
    environment:
      POSTGRES_USER: trading
      POSTGRES_PASSWORD: trading_local
      POSTGRES_DB: trading
    ports: ["5432:5432"]
    volumes: ["tsdb_data:/home/postgres/pgdata/data"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U trading -d trading"]
      interval: 5s
      timeout: 5s
      retries: 10
  redis:
    image: redis:7-alpine
    container_name: trading_redis
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10
volumes:
  tsdb_data:
```

- [ ] **Step 4: Write `.env.example`**

```bash
DATABASE_URL=postgresql://trading:trading_local@localhost:5432/trading
REDIS_URL=redis://localhost:6379/0
DATA_ROOT=./data
# Broker credentials — put real values in .env.local (gitignored)
UPSTOX_API_KEY=
UPSTOX_API_SECRET=
DHAN_CLIENT_ID=
DHAN_ACCESS_TOKEN=
```

- [ ] **Step 5: Write the failing config test**

`tests/test_config.py`:

```python
from pathlib import Path

from trading.config import Settings


def test_settings_read_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings()

    assert s.database_url == "postgresql://u:p@localhost:5432/db"
    assert s.data_root == Path(tmp_path)
    assert s.upstox_api_key is None  # optional until credentials arrive


def test_data_subdirectories_are_derived(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings()

    assert s.raw_archive_root == tmp_path / "raw"
    assert s.recordings_root == tmp_path / "recordings"
```

- [ ] **Step 6: Run it and confirm it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.config'`

- [ ] **Step 7: Write `src/trading/config.py`**

```python
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"), env_file_encoding="utf-8", extra="ignore"
    )

    database_url: str
    redis_url: str
    data_root: Path = Path("./data")

    upstox_api_key: str | None = None
    upstox_api_secret: str | None = None
    dhan_client_id: str | None = None
    dhan_access_token: str | None = None

    @property
    def raw_archive_root(self) -> Path:
        return self.data_root / "raw"

    @property
    def recordings_root(self) -> Path:
        return self.data_root / "recordings"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
```

- [ ] **Step 8: Run the test again**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS (2 passed)

- [ ] **Step 9: Bring the database up and verify TimescaleDB is present**

```bash
brew install docker docker-compose colima
colima start --cpu 4 --memory 8 --disk 100
docker compose up -d
docker compose exec timescaledb psql -U trading -d trading \
  -c "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT extversion FROM pg_extension WHERE extname='timescaledb';"
```
Expected: a version string (e.g. `2.x.x`). If this fails, stop — nothing downstream works without it.

- [ ] **Step 10: Write `scripts/fetch_recon_samples.sh`**

```bash
#!/usr/bin/env bash
# Re-download format samples into data/raw/_recon/ (gitignored).
set -euo pipefail
OUT="$(git rev-parse --show-toplevel)/data/raw/_recon"; mkdir -p "$OUT"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
D="${1:-20260813}"
JAR="$(mktemp)"
curl -s -c "$JAR" -A "$UA" "https://www.nseindia.com" -o /dev/null
for SEG in cm fo; do
  U=$(echo "$SEG" | tr '[:lower:]' '[:upper:]')
  curl -s -b "$JAR" -A "$UA" -H "Referer: https://www.nseindia.com/" \
    "https://nsearchives.nseindia.com/content/${SEG}/BhavCopy_NSE_${U}_0_0_0_${D}_F_0000.csv.zip" \
    -o "$OUT/nse_${SEG}_udiff_${D}.zip"
done
curl -sL -A "$UA" -H "Referer: https://www.bseindia.com/" \
  "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_${D}_F_0000.CSV" \
  -o "$OUT/bse_cm_udiff_${D}.csv"
curl -sL "https://portal.amfiindia.com/spages/NAVAll.txt" -o "$OUT/amfi_navall_${D}.txt"
echo "samples written to $OUT"
```

Then `chmod +x scripts/fetch_recon_samples.sh`.

- [ ] **Step 11: Verify the lint/type gate passes**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/trading`
Expected: no errors. Fix formatting with `uv run ruff format .` if needed.

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "chore: scaffold project, pin python 3.12, add timescale+redis compose"
```

---

## Task 2: Domain models and stage protocols

**Assignee:** Opus (single-authored — this is the contract everything else depends on) · **Depends on:** Task 1

**Files:**
- Create: `src/trading/contracts/{__init__,enums,models,schemas,protocols,errors}.py`
- Test: `tests/contracts/test_models.py`

**Interfaces:**
- Consumes: nothing
- Produces: everything below. Later tasks import **only** from `trading.contracts`.
  - `AssetClass`, `OptionType`, `DataSource`, `JobStatus` (enums)
  - `RawPayload`, `InstrumentRef`, `NormalizedBatch`, `ValidationOutcome`, `QuarantineRow`, `LoadResult`
  - `CANONICAL_BAR_SCHEMA: dict[str, pl.DataType]`
  - `Source`, `Parser`, `Normalizer`, `InstrumentResolver`, `Validator`, `Loader` (Protocols)
  - `ParseError`, `FetchError`, `ValidationAbort`

- [ ] **Step 1: Write the failing test for `InstrumentRef.canonical_key`**

`tests/contracts/test_models.py`:

```python
from datetime import date
from decimal import Decimal

import pytest

from trading.contracts import AssetClass, InstrumentRef, OptionType


def test_canonical_key_for_equity_omits_derivative_parts():
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")
    assert ref.canonical_key == "NSE:CM:RELIANCE"


def test_canonical_key_for_option_includes_all_parts():
    ref = InstrumentRef(
        exchange="NSE", segment="FO", symbol="NIFTY",
        expiry=date(2026, 8, 27), strike=Decimal("24500.00"),
        option_type=OptionType.CE,
    )
    assert ref.canonical_key == "NSE:FO:NIFTY:2026-08-27:24500:CE"


def test_strike_scale_does_not_change_the_key():
    """24500, 24500.0 and 24500.0000 are the same contract; one key."""
    keys = {
        InstrumentRef(
            exchange="NSE", segment="FO", symbol="NIFTY", expiry=date(2026, 8, 27),
            strike=Decimal(s), option_type=OptionType.CE,
        ).canonical_key
        for s in ("24500", "24500.0", "24500.0000")
    }
    assert keys == {"NSE:FO:NIFTY:2026-08-27:24500:CE"}


def test_fractional_strike_is_preserved():
    ref = InstrumentRef(
        exchange="NSE", segment="FO", symbol="BANKNIFTY", expiry=date(2026, 8, 27),
        strike=Decimal("52350.50"), option_type=OptionType.PE,
    )
    assert ref.canonical_key == "NSE:FO:BANKNIFTY:2026-08-27:52350.5:PE"


def test_ref_is_hashable_so_it_can_be_deduplicated():
    a = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    b = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    assert len({a, b}) == 1


def test_ref_is_immutable():
    ref = InstrumentRef(exchange="NSE", segment="CM", symbol="TCS")
    with pytest.raises(Exception):
        ref.symbol = "INFY"  # type: ignore[misc]
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/contracts/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.contracts'`

- [ ] **Step 3: Write `src/trading/contracts/enums.py`**

`DataSource` values are the `source SMALLINT` provenance codes stored on every bar row (spec §4.5). **Never renumber these** — they are persisted.

```python
from __future__ import annotations

from enum import IntEnum, StrEnum


class AssetClass(StrEnum):
    EQUITY = "EQUITY"
    INDEX = "INDEX"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    MF = "MF"
    CRYPTO = "CRYPTO"
    COMMODITY = "COMMODITY"


class OptionType(StrEnum):
    CE = "CE"
    PE = "PE"


class DataSource(IntEnum):
    """Persisted provenance codes. Append only; never renumber."""

    NSE_CM_UDIFF = 1
    NSE_FO_UDIFF = 2
    BSE_CM_UDIFF = 3
    NSE_CM_LEGACY = 4
    AMFI_NAV = 5


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED_HOLIDAY = "SKIPPED_HOLIDAY"
    SKIPPED_NO_DATA = "SKIPPED_NO_DATA"
```

- [ ] **Step 4: Write `src/trading/contracts/errors.py`**

```python
from __future__ import annotations


class TradingError(Exception):
    """Base for every error this package raises."""


class FetchError(TradingError):
    """A source could not retrieve data for a date it should have had."""


class ParseError(TradingError):
    """A payload could not be parsed. Never raised for a merely empty result."""


class ValidationAbort(TradingError):
    """A batch is so wrong that loading any of it would be unsafe."""
```

- [ ] **Step 5: Write `src/trading/contracts/models.py`**

The strike normaliser is the subtle part: `Decimal("24500.00")` and `Decimal("24500")` are equal but render differently, so without normalisation one contract yields two canonical keys and two instrument rows.

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict

from trading.contracts.enums import DataSource, OptionType


def _format_strike(strike: Decimal) -> str:
    """Render a strike canonically: no trailing zeros, no exponent.

    Decimal('24500.00') and Decimal('24500') must produce one key.
    """
    normalized = strike.normalize()
    sign, digits, exponent = normalized.as_tuple()
    if isinstance(exponent, int) and exponent > 0:  # 2.45E+4 -> 24500
        normalized = normalized.quantize(Decimal(1))
    return f"{normalized:f}"


class InstrumentRef(BaseModel):
    """A natural key for one tradable thing, before it has a database id."""

    model_config = ConfigDict(frozen=True)

    exchange: str
    segment: str
    symbol: str
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: OptionType | None = None

    @property
    def canonical_key(self) -> str:
        parts = [self.exchange, self.segment, self.symbol]
        if self.expiry is not None:
            parts.append(self.expiry.isoformat())
        if self.strike is not None:
            parts.append(_format_strike(self.strike))
        if self.option_type is not None:
            parts.append(self.option_type.value)
        return ":".join(parts)


class RawPayload(BaseModel):
    """Exactly what a source returned, before anyone interpreted it."""

    model_config = ConfigDict(frozen=True)

    source_key: str
    business_date: date
    content: bytes
    content_hash: str
    fetched_at: datetime
    archive_path: Path
    meta: dict[str, str] = {}


@dataclass(frozen=True)
class NormalizedBatch:
    """Canonical-schema rows for one (source, date), not yet resolved to ids."""

    source: DataSource
    business_date: date
    frame: pl.DataFrame


@dataclass(frozen=True)
class QuarantineRow:
    reason: str
    payload: dict[str, object]


@dataclass(frozen=True)
class ValidationOutcome:
    valid: pl.DataFrame
    quarantined: list[QuarantineRow] = field(default_factory=list)


@dataclass(frozen=True)
class LoadResult:
    rows_written: int
    instruments_created: int
```

- [ ] **Step 6: Write `src/trading/contracts/schemas.py`**

This is the single column contract every normalizer must satisfy. A parser may emit anything; a **normalizer must emit exactly this**.

```python
from __future__ import annotations

import polars as pl

CANONICAL_BAR_SCHEMA: dict[str, pl.DataType] = {
    # identity (natural key)
    "exchange": pl.String,
    "segment": pl.String,
    "symbol": pl.String,
    "asset_class": pl.String,
    "expiry": pl.Date,
    "strike": pl.Decimal(18, 4),
    "option_type": pl.String,
    # descriptive
    "isin": pl.String,
    "name": pl.String,
    # time
    "ts": pl.Datetime("us", "UTC"),
    # prices
    "open": pl.Decimal(18, 4),
    "high": pl.Decimal(18, 4),
    "low": pl.Decimal(18, 4),
    "close": pl.Decimal(18, 4),
    "prev_close": pl.Decimal(18, 4),
    "settle_price": pl.Decimal(18, 4),
    "underlying_price": pl.Decimal(18, 4),
    # activity
    "volume": pl.Int64,
    "turnover": pl.Decimal(22, 4),
    "trades": pl.Int32,
    "open_interest": pl.Int64,
    "oi_change": pl.Int64,
    "delivery_qty": pl.Int64,
    "delivery_pct": pl.Decimal(7, 4),
    # instrument attributes carried by the row (finding F3)
    "lot_size": pl.Int32,
    "tick_size": pl.Decimal(12, 6),
}


def empty_canonical_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=CANONICAL_BAR_SCHEMA)


def assert_canonical(frame: pl.DataFrame) -> None:
    """Raise if a frame does not match the canonical contract exactly."""
    expected = set(CANONICAL_BAR_SCHEMA)
    actual = set(frame.columns)
    if missing := expected - actual:
        raise ValueError(f"missing canonical columns: {sorted(missing)}")
    if extra := actual - expected:
        raise ValueError(f"unexpected columns: {sorted(extra)}")
    for name, dtype in CANONICAL_BAR_SCHEMA.items():
        if frame.schema[name] != dtype:
            raise ValueError(f"column {name}: expected {dtype}, got {frame.schema[name]}")
```

- [ ] **Step 7: Write `src/trading/contracts/protocols.py`**

```python
from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

import polars as pl
from psycopg import Connection

from trading.contracts.models import (
    InstrumentRef,
    LoadResult,
    NormalizedBatch,
    RawPayload,
    ValidationOutcome,
)


@runtime_checkable
class Source(Protocol):
    source_key: str

    def fetch(self, business_date: date) -> RawPayload | None:
        """Return the payload, or None when there is legitimately no data."""
        ...


@runtime_checkable
class Parser(Protocol):
    def can_parse(self, payload: RawPayload) -> bool:
        """True only for payloads this parser owns. Must be mutually exclusive."""
        ...

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        """Source-shaped frame. Raise ParseError on malformed input."""
        ...


@runtime_checkable
class Normalizer(Protocol):
    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        """Emit a frame matching CANONICAL_BAR_SCHEMA exactly."""
        ...


@runtime_checkable
class InstrumentResolver(Protocol):
    def resolve(
        self, refs: set[InstrumentRef], conn: Connection, *, bootstrap: bool = False
    ) -> dict[InstrumentRef, int]:
        """Map natural keys to instrument_ids, creating any that are new."""
        ...


@runtime_checkable
class Validator(Protocol):
    def validate(self, batch: NormalizedBatch) -> ValidationOutcome:
        """Split rows into loadable and quarantined. Never raises for bad rows."""
        ...


@runtime_checkable
class Loader(Protocol):
    def load(self, outcome: ValidationOutcome, conn: Connection) -> LoadResult:
        """Upsert valid rows. Must be idempotent."""
        ...
```

- [ ] **Step 8: Write `src/trading/contracts/__init__.py`**

```python
from trading.contracts.enums import AssetClass, DataSource, JobStatus, OptionType
from trading.contracts.errors import FetchError, ParseError, TradingError, ValidationAbort
from trading.contracts.models import (
    InstrumentRef,
    LoadResult,
    NormalizedBatch,
    QuarantineRow,
    RawPayload,
    ValidationOutcome,
)
from trading.contracts.protocols import (
    InstrumentResolver,
    Loader,
    Normalizer,
    Parser,
    Source,
    Validator,
)
from trading.contracts.schemas import (
    CANONICAL_BAR_SCHEMA,
    assert_canonical,
    empty_canonical_frame,
)

__all__ = [
    "CANONICAL_BAR_SCHEMA", "AssetClass", "DataSource", "FetchError", "InstrumentRef",
    "InstrumentResolver", "JobStatus", "LoadResult", "Loader", "NormalizedBatch",
    "Normalizer", "OptionType", "ParseError", "Parser", "QuarantineRow", "RawPayload",
    "Source", "TradingError", "ValidationAbort", "ValidationOutcome", "Validator",
    "assert_canonical", "empty_canonical_frame",
]
```

- [ ] **Step 9: Add a schema test and run everything**

Append to `tests/contracts/test_models.py`:

```python
def test_empty_canonical_frame_satisfies_its_own_contract():
    from trading.contracts import assert_canonical, empty_canonical_frame

    assert_canonical(empty_canonical_frame())  # must not raise


def test_assert_canonical_rejects_a_missing_column():
    import polars as pl

    from trading.contracts import assert_canonical, empty_canonical_frame

    frame = empty_canonical_frame().drop("close")
    with pytest.raises(ValueError, match="missing canonical columns"):
        assert_canonical(frame)
```

Run: `uv run pytest tests/contracts -v`
Expected: PASS (8 passed)

- [ ] **Step 10: Verify the strict type gate**

Run: `uv run mypy src/trading/contracts`
Expected: `Success: no issues found`

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "feat(contracts): domain models, canonical schema, six stage protocols"
```

---

## Task 3: Parser contract test suite

**Assignee:** Opus (single-authored) · **Depends on:** Task 2

This is the highest-leverage artifact in Phase 0. Three parsers written by three subagents will drift unless conformance is mechanical. **Parser tasks must not modify this file.**

**Files:**
- Create: `tests/contracts/test_parser_contract.py`, `tests/conftest.py` (extend), `tests/fixtures/README.md`
- Test: itself

**Interfaces:**
- Consumes: `trading.contracts.{Parser, RawPayload, ParseError}`
- Produces: a pytest fixture `parser_cases` that each parser task registers into, and `make_payload(...)` helper. Registration is via `PARSER_CASES.append(ParserCase(...))` in `tests/contracts/parser_cases.py`.

- [ ] **Step 1: Write `tests/contracts/parser_cases.py` (the registry)**

```python
"""Registry of parser conformance cases.

Each parser task appends exactly one ParserCase here. The contract suite
then runs every case against every parser, which is what enforces
mutual exclusivity of can_parse().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading.contracts import Parser

FIXTURE_ROOT = Path(__file__).parent.parent / "fixtures"


@dataclass(frozen=True)
class ParserCase:
    name: str
    parser: Parser
    fixture: Path            # the file this parser owns
    source_key: str
    min_rows: int            # sanity floor for the trimmed fixture
    required_columns: tuple[str, ...]


PARSER_CASES: list[ParserCase] = []
```

- [ ] **Step 2: Write `tests/contracts/test_parser_contract.py`**

```python
from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from trading.contracts import ParseError, RawPayload

from .parser_cases import PARSER_CASES, ParserCase


def make_payload(path: Path, source_key: str, business_date: date) -> RawPayload:
    content = path.read_bytes()
    return RawPayload(
        source_key=source_key,
        business_date=business_date,
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=path,
    )


def _ids(cases: list[ParserCase]) -> list[str]:
    return [c.name for c in cases]


@pytest.fixture(params=PARSER_CASES, ids=_ids(PARSER_CASES))
def case(request: pytest.FixtureRequest) -> ParserCase:
    return request.param


def test_can_parse_accepts_its_own_fixture(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.can_parse(payload) is True


def test_can_parse_rejects_every_other_parsers_fixture(case: ParserCase) -> None:
    """Mutual exclusivity. Without this, dispatch silently picks the wrong parser."""
    for other in PARSER_CASES:
        if other.name == case.name:
            continue
        payload = make_payload(other.fixture, other.source_key, date(2026, 8, 13))
        assert case.parser.can_parse(payload) is False, (
            f"{case.name} claims to parse {other.name}'s fixture"
        )


def test_parse_returns_the_declared_columns(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    frame = case.parser.parse(payload)
    missing = set(case.required_columns) - set(frame.columns)
    assert not missing, f"{case.name} did not emit {sorted(missing)}"


def test_parse_returns_enough_rows(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.parse(payload).height >= case.min_rows


def test_parse_is_pure(case: ParserCase) -> None:
    """Same bytes twice must give the same frame. Catches hidden state."""
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.parse(payload).equals(case.parser.parse(payload))


def test_parse_raises_on_empty_input(case: ParserCase, tmp_path: Path) -> None:
    empty = tmp_path / "empty.bin"
    empty.write_bytes(b"")
    payload = make_payload(empty, case.source_key, date(2026, 8, 13))
    with pytest.raises(ParseError):
        case.parser.parse(payload)


def test_parse_raises_on_truncated_input(case: ParserCase, tmp_path: Path) -> None:
    """A half-downloaded file must fail loudly, never return partial rows."""
    truncated = tmp_path / "truncated.bin"
    truncated.write_bytes(case.fixture.read_bytes()[:40])
    payload = make_payload(truncated, case.source_key, date(2026, 8, 13))
    with pytest.raises(ParseError):
        case.parser.parse(payload)


def test_at_least_one_parser_is_registered() -> None:
    """Guards against the suite silently passing with zero cases."""
    assert PARSER_CASES, "no parser registered a ParserCase"
```

- [ ] **Step 3: Run it and confirm it fails for the right reason**

Run: `uv run pytest tests/contracts/test_parser_contract.py -v`
Expected: FAIL — `test_at_least_one_parser_is_registered` fails with "no parser registered a ParserCase". Every parametrized test is skipped/absent because `PARSER_CASES` is empty. **This is the correct RED state**; it goes green as parsers register in Tasks 6–8.

- [ ] **Step 4: Write `tests/fixtures/README.md`**

```markdown
# Test fixtures

Real exchange files, trimmed to ~50 rows, committed deliberately (spec D15).

**Never hand-write a fixture.** Synthetic CSVs test the author's belief about a
format rather than the format itself, which is how parser bugs survive review.

To regenerate from live sources:

    ./scripts/fetch_recon_samples.sh 20260813

then trim with `scripts/trim_fixture.py`, preserving the header, at least one
ordinary row, and every edge case named in
`docs/data-formats/eod-source-formats.md` for that source.
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(contracts): parser conformance suite enforcing mutual exclusivity"
```

---

## Task 4: Database migrations

**Assignee:** Sonnet · **Depends on:** Task 1, Task 2

**Files:**
- Create: `alembic.ini`, `migrations/env.py`, `migrations/versions/0001_initial_schema.py`, `tests/test_migrations.py`
- Test: `tests/test_migrations.py`

**Interfaces:**
- Consumes: `trading.config.get_settings`, `trading.contracts.DataSource`
- Produces: the tables of spec §4 in a live database. Later tasks assume these exact names: `instruments`, `instrument_lot_history`, `corporate_actions`, `trading_calendar`, `bars_daily`, `bars_intraday`, `ingest_jobs`, `quarantine`, `users`, `data_sources`.

**Reference:** transcribe the DDL from spec §4.1–§4.6 exactly. Two details that are easy to get wrong and are load-bearing:

1. `instruments` natural key **must** use `UNIQUE NULLS NOT DISTINCT` (PG15+). Without it every equity row (NULL expiry/strike/option_type) is distinct from every other and the constraint enforces nothing.
2. `bars_daily.ck_ohlc_order` **must** be conditioned on `volume`. Finding F2: 49% of F&O rows are untraded with `OHLC = 0` and a non-zero close, and an unconditional constraint rejects them all.

- [ ] **Step 1: Write the failing migration test**

`tests/test_migrations.py`:

```python
import pytest

pytestmark = pytest.mark.db


def test_core_tables_exist(db_conn):
    rows = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {
        "instruments", "instrument_lot_history", "corporate_actions", "trading_calendar",
        "bars_daily", "bars_intraday", "ingest_jobs", "quarantine", "users", "data_sources",
    } <= names


def test_bars_daily_is_a_hypertable(db_conn):
    row = db_conn.execute(
        "SELECT hypertable_name FROM timescaledb_information.hypertables "
        "WHERE hypertable_name = 'bars_daily'"
    ).fetchone()
    assert row is not None


def test_untraded_option_row_is_accepted(db_conn):
    """Finding F2: OHLC=0 with a real close and zero volume is legitimate."""
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('OPTION','NSE','FO','TESTOPT','INR','ACTIVE',"
        "'NSE:FO:TESTOPT:2026-10-27:430:CE') RETURNING instrument_id"
    )
    iid = db_conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol='TESTOPT'"
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)"
        " VALUES (%s, '2026-08-13T10:00:00Z', 0, 0, 0, 19.45, 0, 2)",
        (iid,),
    )  # must not raise


def test_traded_row_with_impossible_ohlc_is_rejected(db_conn):
    from psycopg.errors import CheckViolation

    iid = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES ('EQUITY','NSE','CM','TESTEQ','INR','ACTIVE','NSE:CM:TESTEQ')"
        " RETURNING instrument_id"
    ).fetchone()[0]
    with pytest.raises(CheckViolation):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)"
            " VALUES (%s, '2026-08-13T10:00:00Z', 100, 90, 95, 99, 5000, 1)",
            (iid,),
        )  # high < low with real volume


def test_equity_natural_key_is_actually_unique(db_conn):
    """UNIQUE NULLS NOT DISTINCT: two RELIANCE rows must collide despite NULLs."""
    from psycopg.errors import UniqueViolation

    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
        " canonical_key) VALUES ('EQUITY','NSE','CM','DUPTEST','INR','ACTIVE','NSE:CM:DUPTEST')"
    )
    with pytest.raises(UniqueViolation):
        db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, status,"
            " canonical_key) VALUES ('EQUITY','NSE','CM','DUPTEST','INR','ACTIVE','other-key')"
        )


def test_data_sources_match_the_python_enum(db_conn):
    from trading.contracts import DataSource

    rows = db_conn.execute("SELECT source_id, source_key FROM data_sources").fetchall()
    assert {(r[0], r[1]) for r in rows} == {(s.value, s.name) for s in DataSource}
```

- [ ] **Step 2: Add the database fixture to `tests/conftest.py`**

Transaction-per-test rollback keeps tests from polluting each other (spec §6.1).

```python
from collections.abc import Iterator

import psycopg
import pytest

from trading.config import get_settings


@pytest.fixture(scope="session")
def db_url() -> str:
    return get_settings().database_url


@pytest.fixture
def db_conn(db_url: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(db_url, autocommit=False)
    try:
        yield conn
    finally:
        conn.rollback()   # nothing a test does is ever persisted
        conn.close()
```

- [ ] **Step 3: Run and confirm failure**

Run: `uv run pytest tests/test_migrations.py -v -m db`
Expected: FAIL — tables do not exist.

- [ ] **Step 4: Initialise Alembic and write the migration**

```bash
uv run alembic init -t generic migrations
```

Then write `migrations/versions/0001_initial_schema.py` with `op.execute(...)` blocks transcribing spec §4.1–§4.6 verbatim, in this order: `users` → `data_sources` (+ seed rows from `DataSource`) → `instruments` → `instrument_lot_history` → `corporate_actions` → `trading_calendar` → `bars_daily` → `create_hypertable` → compression policy → `bars_intraday` → `ingest_jobs` → `quarantine`.

The migration must begin with `op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")`.

Seed `data_sources` by iterating the enum so the two can never drift:

```python
from trading.contracts import DataSource

for source in DataSource:
    op.execute(
        f"INSERT INTO data_sources (source_id, source_key) "
        f"VALUES ({source.value}, '{source.name}')"
    )
```

- [ ] **Step 5: Apply and re-run**

Run: `uv run alembic upgrade head && uv run pytest tests/test_migrations.py -v -m db`
Expected: PASS (6 passed)

- [ ] **Step 6: Verify the migration is reversible**

Run: `uv run alembic downgrade base && uv run alembic upgrade head`
Expected: both succeed without error.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(db): initial schema with hypertables, volume-conditioned OHLC check"
```

---

## Task 5: HTTP sources

**Assignee:** Sonnet · **Depends on:** Task 2

**Files:**
- Create: `src/trading/sources/{__init__,http,nse_udiff,bse_udiff,nse_legacy,amfi}.py`
- Test: `tests/sources/test_http.py`, `tests/sources/test_live_sources.py`

**Interfaces:**
- Consumes: `trading.contracts.{Source, RawPayload, FetchError}`, `trading.config.get_settings`
- Produces:
  - `ArchivingClient(root: Path)` with `.get(url, *, headers, prime: str | None) -> bytes`
  - `NseUdiffSource(segment: Literal["cm","fo"])`, `BseUdiffSource()`, `NseLegacyCmSource()`, `AmfiNavSource()` — each satisfying `Source`, each with `source_key` ∈ `{"nse_cm_udiff","nse_fo_udiff","bse_cm_udiff","nse_cm_legacy","amfi_nav"}`

**Reference:** URL templates and access quirks are in `docs/data-formats/eod-source-formats.md` §1–§3. The two that will silently break you:

- **NSE 403s without cookie priming.** `GET https://www.nseindia.com` with a browser UA first, keep the cookie jar, then request the archive with `Referer: https://www.nseindia.com/`.
- **AMFI's documented URL 302s** to `portal.amfiindia.com`. Follow redirects.

- [ ] **Step 1: Write the failing test for archiving and hashing**

`tests/sources/test_http.py`:

```python
import hashlib
from pathlib import Path

import httpx
import pytest

from trading.contracts import FetchError
from trading.sources.http import ArchivingClient


def test_get_writes_the_body_to_the_archive_and_returns_it(tmp_path: Path) -> None:
    body = b"col_a,col_b\n1,2\n"
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=body))
    client = ArchivingClient(root=tmp_path, transport=transport)

    got, path, digest = client.get("https://example.test/f.csv", archive_name="f.csv")

    assert got == body
    assert path.read_bytes() == body
    assert digest == hashlib.sha256(body).hexdigest()


def test_get_retries_then_succeeds(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, content=b"ok")

    client = ArchivingClient(
        root=tmp_path, transport=httpx.MockTransport(handler), backoff_seconds=0.0
    )
    body, _, _ = client.get("https://example.test/f", archive_name="f")

    assert body == b"ok"
    assert calls["n"] == 3


def test_get_raises_fetch_error_after_exhausting_retries(tmp_path: Path) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(503))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    with pytest.raises(FetchError):
        client.get("https://example.test/f", archive_name="f")


def test_404_returns_none_rather_than_raising(tmp_path: Path) -> None:
    """A holiday is ordinary control flow, not an exception (spec 5.4)."""
    transport = httpx.MockTransport(lambda req: httpx.Response(404))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    assert client.get("https://example.test/f", archive_name="f") is None


def test_empty_body_on_200_is_a_fetch_error(tmp_path: Path) -> None:
    """An empty file on a trading day is suspicious, never success."""
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=b""))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    with pytest.raises(FetchError):
        client.get("https://example.test/f", archive_name="f")


def test_priming_request_is_made_before_the_real_one(tmp_path: Path) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, content=b"data")

    client = ArchivingClient(root=tmp_path, transport=httpx.MockTransport(handler))
    client.get(
        "https://nsearchives.nseindia.com/x.zip",
        archive_name="x.zip",
        prime="https://www.nseindia.com",
    )

    assert seen == ["https://www.nseindia.com", "https://nsearchives.nseindia.com/x.zip"]
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/sources/test_http.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.sources'`

- [ ] **Step 3: Implement `src/trading/sources/http.py`**

```python
from __future__ import annotations

import hashlib
import time
from pathlib import Path

import httpx
import structlog

from trading.contracts import FetchError

log = structlog.get_logger(__name__)

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)


class ArchivingClient:
    """Fetches a URL, archives the exact bytes, and returns them with a digest.

    Archiving before parsing is what lets a parser bug found in month four be
    fixed by re-reading disk instead of re-downloading 2,500 files.
    """

    def __init__(
        self,
        root: Path,
        *,
        transport: httpx.BaseTransport | None = None,
        attempts: int = 3,
        backoff_seconds: float = 2.0,
        timeout: float = 60.0,
    ) -> None:
        self._root = root
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._client = httpx.Client(
            transport=transport,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": BROWSER_UA},
        )

    def get(
        self,
        url: str,
        *,
        archive_name: str,
        headers: dict[str, str] | None = None,
        prime: str | None = None,
    ) -> tuple[bytes, Path, str] | None:
        """Return (body, archive_path, sha256), or None on a clean 404."""
        if prime is not None:
            self._client.get(prime)  # populates cookies; failures are non-fatal

        last: Exception | None = None
        for attempt in range(1, self._attempts + 1):
            try:
                response = self._client.get(url, headers=headers or {})
            except httpx.HTTPError as exc:
                last = exc
            else:
                if response.status_code == 404:
                    log.info("source.absent", url=url)
                    return None
                if response.status_code == 200:
                    if not response.content:
                        raise FetchError(f"empty body from {url}")
                    return self._archive(response.content, archive_name)
                last = FetchError(f"HTTP {response.status_code} from {url}")

            if attempt < self._attempts:
                time.sleep(self._backoff * attempt)

        raise FetchError(f"failed after {self._attempts} attempts: {url}") from last

    def _archive(self, body: bytes, name: str) -> tuple[bytes, Path, str]:
        path = self._root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return body, path, hashlib.sha256(body).hexdigest()
```

- [ ] **Step 4: Run and confirm the client tests pass**

Run: `uv run pytest tests/sources/test_http.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Implement the four sources**

Each builds its URL, calls `ArchivingClient.get`, and wraps the result in `RawPayload`. `nse_udiff.py` handles both CM and FO because only the path segment differs. Unzip is the **parser's** job, not the source's — the source archives exactly what the server sent.

Archive path convention (spec §4.6): `{data_root}/raw/{source_key}/{YYYY}/{MM}/{business_date}.{ext}`.

```python
# src/trading/sources/nse_udiff.py
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

NSE_PRIME = "https://www.nseindia.com"
URL = (
    "https://nsearchives.nseindia.com/content/{seg}/"
    "BhavCopy_NSE_{SEG}_0_0_0_{ymd}_F_0000.csv.zip"
)


class NseUdiffSource:
    def __init__(self, segment: Literal["cm", "fo"], client: ArchivingClient | None = None):
        self._segment = segment
        self.source_key = f"nse_{segment}_udiff"
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        ymd = business_date.strftime("%Y%m%d")
        url = URL.format(seg=self._segment, SEG=self._segment.upper(), ymd=ymd)
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.zip"
        )
        result = self._client.get(
            url, archive_name=name, headers={"Referer": f"{NSE_PRIME}/"}, prime=NSE_PRIME
        )
        if result is None:
            return None
        body, path, digest = result
        return RawPayload(
            source_key=self.source_key,
            business_date=business_date,
            content=body,
            content_hash=digest,
            fetched_at=datetime.now(UTC),
            archive_path=path,
            meta={"url": url},
        )
```

Apply the same shape to `bse_udiff.py` (plain `.CSV`, no prime, `Referer: https://www.bseindia.com/`), `nse_legacy.py` (uppercase-month URL template from the format doc, same NSE priming), and `amfi.py` (`https://portal.amfiindia.com/spages/NAVAll.txt`, no prime, `.txt`).

- [ ] **Step 6: Write the live smoke tests**

`tests/sources/test_live_sources.py`:

```python
from datetime import date

import pytest

pytestmark = pytest.mark.live

RECENT_TRADING_DAY = date(2026, 8, 13)


def test_nse_cm_udiff_url_is_still_valid(tmp_path):
    from trading.sources.http import ArchivingClient
    from trading.sources.nse_udiff import NseUdiffSource

    src = NseUdiffSource("cm", client=ArchivingClient(root=tmp_path))
    payload = src.fetch(RECENT_TRADING_DAY)
    assert payload is not None and len(payload.content) > 10_000


def test_amfi_url_is_still_valid(tmp_path):
    from trading.sources.amfi import AmfiNavSource
    from trading.sources.http import ArchivingClient

    payload = AmfiNavSource(client=ArchivingClient(root=tmp_path)).fetch(RECENT_TRADING_DAY)
    assert payload is not None and b"Scheme Code" in payload.content[:200]
```

- [ ] **Step 7: Run both the default and the live suite**

Run: `uv run pytest tests/sources -v` → PASS, live tests deselected.
Run: `uv run pytest tests/sources -v -m live` → PASS (requires network).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(sources): archiving http client with NSE cookie priming; four EOD sources"
```

---

## Task 6: UDiFF parser (NSE CM + NSE FO + BSE CM)

**Assignee:** Sonnet (parallel with Tasks 7, 8) · **Depends on:** Task 3, Task 5

**⚠️ Do not modify `tests/contracts/test_parser_contract.py`.** If you cannot pass it without editing it, the interface is wrong — stop and escalate.

**Files:**
- Create: `src/trading/parsers/{__init__,registry,udiff}.py`, `tests/fixtures/udiff/*`, `tests/parsers/test_udiff.py`
- Modify: `tests/contracts/parser_cases.py` (append one `ParserCase`)

**Interfaces:**
- Consumes: `trading.contracts.{Parser, RawPayload, ParseError}`
- Produces: `UdiffParser()` satisfying `Parser`; `ParserRegistry(parsers: list[Parser])` with `.select(payload) -> Parser` raising `ParseError` when none match.

**Reference:** `docs/data-formats/eod-source-formats.md` §1 — the full 34-column list, discriminators, and edge cases. **Read it before writing code.**

Key facts you must handle:
- One parser serves NSE CM, NSE FO **and** BSE CM — the headers are byte-identical (finding F1).
- **BSE uses CRLF, NSE uses LF.** Strip `\r` before comparing headers or the BSE file is rejected.
- NSE payloads are **ZIP**; BSE is **plain CSV**. Detect by magic bytes (`PK\x03\x04`), not by source key.
- Dates are `YYYY-MM-DD`. Empty values are empty strings, not `-`.
- Do **not** filter any `SctySrs`. `GS`, `GB`, `N0` etc. are legitimate instruments.

- [ ] **Step 1: Create the trimmed fixtures**

Take the first 50 data rows of each recon sample, preserving the header, and include at least one untraded option row (`OpnPric=0.00`, `ClsPric>0`) in the FO fixture.

```bash
mkdir -p tests/fixtures/udiff
python - <<'PY'
from pathlib import Path
import zipfile, io
recon = Path("data/raw/_recon"); out = Path("tests/fixtures/udiff")
# NSE CM + FO: keep header + 50 rows, re-zip
for name, src in [("nse_cm", "nse_cm_udiff_20260813.csv"), ("nse_fo", "nse_fo_udiff_20260813.csv")]:
    lines = (recon / src).read_text().splitlines()
    header, rows = lines[0], lines[1:]
    if name == "nse_fo":  # guarantee an untraded contract is present
        untraded = [r for r in rows if r.split(",")[14] == "0.00"][:5]
        rows = rows[:45] + untraded
    payload = "\n".join([header, *rows[:50]]) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"BhavCopy_{name.upper()}.csv", payload)
    (out / f"{name}_udiff.zip").write_bytes(buf.getvalue())
# BSE: plain CSV, preserve CRLF
bse = (recon / "bse_cm_udiff_20260813.csv").read_bytes().split(b"\r\n")
(out / "bse_cm_udiff.csv").write_bytes(b"\r\n".join(bse[:51]) + b"\r\n")
print("fixtures written")
PY
```

- [ ] **Step 2: Write the failing parser test**

`tests/parsers/test_udiff.py`:

```python
from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.udiff import UdiffParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "udiff"


@pytest.fixture
def parser() -> UdiffParser:
    return UdiffParser()


def test_parses_zipped_nse_cm(parser: UdiffParser) -> None:
    payload = make_payload(FIXTURES / "nse_cm_udiff.zip", "nse_cm_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.height == 50
    assert frame["Sgmt"].unique().to_list() == ["CM"]
    assert frame["Src"].unique().to_list() == ["NSE"]


def test_parses_plain_csv_bse_despite_crlf(parser: UdiffParser) -> None:
    """Finding F1: BSE differs from NSE only by line endings."""
    payload = make_payload(FIXTURES / "bse_cm_udiff.csv", "bse_cm_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.height == 50
    assert frame["Src"].unique().to_list() == ["BSE"]
    assert not frame.columns[-1].endswith("\r")


def test_untraded_option_rows_are_kept(parser: UdiffParser) -> None:
    """Finding F2: OHLC=0 with a real close is a valid untraded contract."""
    payload = make_payload(FIXTURES / "nse_fo_udiff.zip", "nse_fo_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    untraded = frame.filter(
        (frame["OpnPric"] == "0.00") & (frame["ClsPric"] != "0.00")
    )
    assert untraded.height > 0, "untraded contracts were dropped"


def test_all_34_columns_are_present(parser: UdiffParser) -> None:
    payload = make_payload(FIXTURES / "nse_fo_udiff.zip", "nse_fo_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.width == 34
    assert frame.columns[0] == "TradDt"
    assert frame.columns[28] == "NewBrdLotQty"


def test_rejects_a_file_with_a_foreign_header(parser: UdiffParser, tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_bytes(b"SYMBOL,SERIES,OPEN\nX,EQ,1\n")
    payload = make_payload(bad, "nse_cm_udiff", date(2026, 8, 13))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
```

- [ ] **Step 3: Run and confirm failure**

Run: `uv run pytest tests/parsers/test_udiff.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'trading.parsers'`

- [ ] **Step 4: Implement `src/trading/parsers/udiff.py`**

Parse every column as `String`. Type coercion is the **normalizer's** job — a parser that guesses types loses information (e.g. `"0.00"` vs `""`).

```python
from __future__ import annotations

import io
import zipfile

import polars as pl

from trading.contracts import ParseError, RawPayload

ZIP_MAGIC = b"PK\x03\x04"

UDIFF_COLUMNS: tuple[str, ...] = (
    "TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb",
    "SctySrs", "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp", "FinInstrmNm",
    "OpnPric", "HghPric", "LwPric", "ClsPric", "LastPric", "PrvsClsgPric", "UndrlygPric",
    "SttlmPric", "OpnIntrst", "ChngInOpnIntrst", "TtlTradgVol", "TtlTrfVal",
    "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty", "Rmks", "Rsvd1", "Rsvd2", "Rsvd3", "Rsvd4",
)


def _extract_csv(content: bytes) -> bytes:
    """Return CSV bytes whether the payload is zipped (NSE) or plain (BSE)."""
    if not content.startswith(ZIP_MAGIC):
        return content
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ParseError("zip contains no .csv member")
            return archive.read(names[0])
    except zipfile.BadZipFile as exc:
        raise ParseError("payload is not a readable zip") from exc


def _header_of(csv_bytes: bytes) -> tuple[str, ...]:
    first = csv_bytes.split(b"\n", 1)[0].replace(b"\r", b"")
    return tuple(first.decode("utf-8", errors="replace").split(","))


class UdiffParser:
    """Parses the UDiFF bhavcopy shared by NSE CM, NSE FO and BSE CM (finding F1)."""

    def can_parse(self, payload: RawPayload) -> bool:
        try:
            return _header_of(_extract_csv(payload.content)) == UDIFF_COLUMNS
        except ParseError:
            return False

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        csv_bytes = _extract_csv(payload.content)
        if _header_of(csv_bytes) != UDIFF_COLUMNS:
            raise ParseError("header is not UDiFF")
        try:
            frame = pl.read_csv(
                io.BytesIO(csv_bytes.replace(b"\r\n", b"\n")),
                schema_overrides={c: pl.String for c in UDIFF_COLUMNS},
                has_header=True,
                truncate_ragged_lines=False,
            )
        except Exception as exc:
            raise ParseError(f"unreadable UDiFF csv: {exc}") from exc
        if frame.height == 0:
            raise ParseError("UDiFF file has a header but no rows")
        return frame
```

- [ ] **Step 5: Implement `src/trading/parsers/registry.py`**

```python
from __future__ import annotations

from trading.contracts import ParseError, Parser, RawPayload


class ParserRegistry:
    """Selects the one parser that owns a payload."""

    def __init__(self, parsers: list[Parser]) -> None:
        self._parsers = parsers

    def select(self, payload: RawPayload) -> Parser:
        matches = [p for p in self._parsers if p.can_parse(payload)]
        if not matches:
            raise ParseError(f"no parser accepts {payload.source_key} {payload.business_date}")
        if len(matches) > 1:
            names = ", ".join(type(p).__name__ for p in matches)
            raise ParseError(f"ambiguous payload; multiple parsers matched: {names}")
        return matches[0]
```

- [ ] **Step 6: Register the contract case**

Append to `tests/contracts/parser_cases.py`:

```python
from trading.parsers.udiff import UdiffParser

PARSER_CASES.append(
    ParserCase(
        name="udiff",
        parser=UdiffParser(),
        fixture=FIXTURE_ROOT / "udiff" / "nse_fo_udiff.zip",
        source_key="nse_fo_udiff",
        min_rows=50,
        required_columns=("TradDt", "Sgmt", "FinInstrmTp", "ClsPric", "NewBrdLotQty"),
    )
)
```

- [ ] **Step 7: Run the parser tests and the contract suite**

Run: `uv run pytest tests/parsers/test_udiff.py tests/contracts -v`
Expected: PASS. The contract suite now runs its 7 parametrized checks against `udiff`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(parsers): UDiFF parser covering NSE CM, NSE FO and BSE CM"
```

---

## Task 7: NSE legacy CM parser

**Assignee:** Sonnet (parallel with Tasks 6, 8) · **Depends on:** Task 3, Task 5

**⚠️ Do not modify `tests/contracts/test_parser_contract.py`.**

**Files:**
- Create: `src/trading/parsers/nse_legacy.py`, `tests/fixtures/nse_legacy/cm_legacy.zip`, `tests/parsers/test_nse_legacy.py`
- Modify: `tests/contracts/parser_cases.py`

**Interfaces:**
- Consumes: `trading.contracts.{Parser, RawPayload, ParseError}`
- Produces: `NseLegacyCmParser()` satisfying `Parser`

**Reference:** `docs/data-formats/eod-source-formats.md` §2. Header is exactly:
`SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN` — **plus a trailing comma** producing a 14th empty field that Polars names `column_14`. Expect and drop it; it is not corruption. Dates are `DD-MON-YYYY` (`14-MAR-2019`).

- [ ] **Step 1: Create the fixture**

```bash
mkdir -p tests/fixtures/nse_legacy
python - <<'PY'
from pathlib import Path
import zipfile, io
lines = Path("data/raw/_recon/nse_cm_legacy_20190314.csv").read_text().splitlines()
payload = "\n".join(lines[:51]) + "\n"
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("cm14MAR2019bhav.csv", payload)
Path("tests/fixtures/nse_legacy/cm_legacy.zip").write_bytes(buf.getvalue())
print("fixture written")
PY
```

- [ ] **Step 2: Write the failing test**

`tests/parsers/test_nse_legacy.py`:

```python
from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.nse_legacy import NseLegacyCmParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "nse_legacy" / "cm_legacy.zip"


@pytest.fixture
def parser() -> NseLegacyCmParser:
    return NseLegacyCmParser()


def test_parses_the_legacy_format(parser: NseLegacyCmParser) -> None:
    frame = parser.parse(make_payload(FIXTURE, "nse_cm_legacy", date(2019, 3, 14)))
    assert frame.height == 50
    assert frame["SYMBOL"][0] == "20MICRONS"
    assert frame["TIMESTAMP"][0] == "14-MAR-2019"


def test_trailing_empty_column_is_dropped(parser: NseLegacyCmParser) -> None:
    """Every legacy line ends with a comma; the 14th field is not data."""
    frame = parser.parse(make_payload(FIXTURE, "nse_cm_legacy", date(2019, 3, 14)))
    assert frame.width == 13
    assert frame.columns[-1] == "ISIN"


def test_rejects_a_udiff_file(parser: NseLegacyCmParser, tmp_path: Path) -> None:
    bad = tmp_path / "udiff.csv"
    bad.write_bytes(b"TradDt,BizDt,Sgmt,Src\n2026-08-13,2026-08-13,CM,NSE\n")
    payload = make_payload(bad, "nse_cm_legacy", date(2019, 3, 14))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
```

- [ ] **Step 3: Run and confirm failure**

Run: `uv run pytest tests/parsers/test_nse_legacy.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 4: Implement `src/trading/parsers/nse_legacy.py`**

```python
from __future__ import annotations

import io

import polars as pl

from trading.contracts import ParseError, RawPayload
from trading.parsers.udiff import _extract_csv, _header_of

LEGACY_COLUMNS: tuple[str, ...] = (
    "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE",
    "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN",
)


class NseLegacyCmParser:
    """Parses NSE cash bhavcopy from before the UDiFF migration."""

    def can_parse(self, payload: RawPayload) -> bool:
        try:
            header = _header_of(_extract_csv(payload.content))
        except ParseError:
            return False
        # A trailing comma yields a final empty field; tolerate it.
        trimmed = header[:-1] if header and header[-1] == "" else header
        return trimmed == LEGACY_COLUMNS

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("header is not NSE legacy CM")
        csv_bytes = _extract_csv(payload.content)
        try:
            frame = pl.read_csv(
                io.BytesIO(csv_bytes.replace(b"\r\n", b"\n")),
                schema_overrides={c: pl.String for c in LEGACY_COLUMNS},
                has_header=True,
                truncate_ragged_lines=True,
            )
        except Exception as exc:
            raise ParseError(f"unreadable legacy csv: {exc}") from exc
        frame = frame.select([c for c in frame.columns if c in LEGACY_COLUMNS])
        if frame.height == 0:
            raise ParseError("legacy file has a header but no rows")
        return frame
```

- [ ] **Step 5: Register the contract case**

Append to `tests/contracts/parser_cases.py`:

```python
from trading.parsers.nse_legacy import NseLegacyCmParser

PARSER_CASES.append(
    ParserCase(
        name="nse_legacy",
        parser=NseLegacyCmParser(),
        fixture=FIXTURE_ROOT / "nse_legacy" / "cm_legacy.zip",
        source_key="nse_cm_legacy",
        min_rows=50,
        required_columns=("SYMBOL", "SERIES", "CLOSE", "TIMESTAMP", "ISIN"),
    )
)
```

- [ ] **Step 6: Run parser and contract tests**

Run: `uv run pytest tests/parsers tests/contracts -v`
Expected: PASS. `test_can_parse_rejects_every_other_parsers_fixture` now genuinely exercises UDiFF vs legacy.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(parsers): NSE legacy CM bhavcopy parser"
```

---

## Task 8: AMFI NAV parser

**Assignee:** Sonnet (parallel with Tasks 6, 7) · **Depends on:** Task 3, Task 5

**⚠️ Do not modify `tests/contracts/test_parser_contract.py`.**

**Files:**
- Create: `src/trading/parsers/amfi.py`, `tests/fixtures/amfi/navall.txt`, `tests/parsers/test_amfi.py`
- Modify: `tests/contracts/parser_cases.py`

**Interfaces:**
- Consumes: `trading.contracts.{Parser, RawPayload, ParseError}`
- Produces: `AmfiNavParser()` satisfying `Parser`, emitting columns
  `scheme_code, isin_growth, isin_reinvest, scheme_name, nav, nav_date, scheme_type, amc_name` (all `pl.String`)

**Reference:** `docs/data-formats/eod-source-formats.md` §3. **This file is not a CSV.** `pl.read_csv` on it returns garbage. Data rows are interleaved with blank lines, scheme-type headers, and AMC name lines; a stateful line scanner is required. The five parsing rules are listed in the format doc — follow them exactly.

- [ ] **Step 1: Create the fixture**

Must include at least two AMCs, two scheme types, one `-` ISIN, and one `N.A.` NAV if present.

```bash
mkdir -p tests/fixtures/amfi
python - <<'PY'
from pathlib import Path
lines = Path("data/raw/_recon/amfi_navall_20260813.txt").read_text().splitlines()
keep, amcs, data = [lines[0]], 0, 0
for line in lines[1:]:
    keep.append(line)
    if line.strip() and ";" not in line and not line.startswith(("Open Ended", "Close Ended")):
        amcs += 1
    if line.count(";") == 5:
        data += 1
    if amcs >= 3 and data >= 40:
        break
Path("tests/fixtures/amfi/navall.txt").write_text("\n".join(keep) + "\n")
print(f"fixture: {len(keep)} lines, {amcs} AMCs, {data} data rows")
PY
```

- [ ] **Step 2: Write the failing test**

`tests/parsers/test_amfi.py`:

```python
from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.amfi import AmfiNavParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "amfi" / "navall.txt"


@pytest.fixture
def parser() -> AmfiNavParser:
    return AmfiNavParser()


def test_extracts_only_data_rows(parser: AmfiNavParser) -> None:
    """Section headers and AMC names must not become rows."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame.height > 0
    assert frame["scheme_code"].str.contains(r"^\d+$").all()


def test_scheme_type_and_amc_are_carried_down(parser: AmfiNavParser) -> None:
    """Every data row inherits the section it appeared under."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame["amc_name"].null_count() == 0
    assert frame["scheme_type"].null_count() == 0
    assert frame["scheme_type"].str.starts_with("Open Ended").any()


def test_missing_isin_dash_becomes_null(parser: AmfiNavParser) -> None:
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert not frame["isin_reinvest"].str.contains(r"^-$").any()


def test_date_column_is_preserved_verbatim(parser: AmfiNavParser) -> None:
    """DD-Mon-YYYY; conversion is the normalizer's job, not the parser's."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame["nav_date"].str.contains(r"^\d{2}-[A-Za-z]{3}-\d{4}$").all()


def test_rejects_a_csv_file(parser: AmfiNavParser, tmp_path: Path) -> None:
    bad = tmp_path / "x.csv"
    bad.write_bytes(b"TradDt,BizDt,Sgmt\n2026-08-13,2026-08-13,CM\n")
    payload = make_payload(bad, "amfi_nav", date(2026, 8, 13))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
```

- [ ] **Step 3: Run and confirm failure**

Run: `uv run pytest tests/parsers/test_amfi.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 4: Implement `src/trading/parsers/amfi.py`**

```python
from __future__ import annotations

import polars as pl

from trading.contracts import ParseError, RawPayload

EXPECTED_HEADER = "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment"
SCHEME_TYPE_PREFIXES = ("Open Ended", "Close Ended", "Interval Fund")
FIELD_COUNT = 6  # five semicolons


class AmfiNavParser:
    """Scans AMFI's hierarchical NAV file.

    The file interleaves data rows with blank lines, scheme-type headers and
    AMC names, so it cannot be read as a CSV (finding F4).
    """

    def can_parse(self, payload: RawPayload) -> bool:
        head = payload.content[:200].decode("utf-8", errors="replace")
        return head.startswith(EXPECTED_HEADER)

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("not an AMFI NAVAll file")

        text = payload.content.decode("utf-8", errors="replace")
        rows: list[dict[str, str | None]] = []
        scheme_type: str | None = None
        amc_name: str | None = None

        for raw_line in text.splitlines()[1:]:   # skip the header
            line = raw_line.strip()
            if not line:
                continue
            if ";" not in line:
                if line.startswith(SCHEME_TYPE_PREFIXES):
                    scheme_type = line
                else:
                    amc_name = line
                continue
            fields = line.split(";")
            if len(fields) != FIELD_COUNT:
                continue  # defensive: unexpected shape is not data
            code, isin_g, isin_r, name, nav, nav_date = (f.strip() for f in fields)
            rows.append(
                {
                    "scheme_code": code,
                    "isin_growth": None if isin_g == "-" else isin_g,
                    "isin_reinvest": None if isin_r == "-" else isin_r,
                    "scheme_name": name,
                    "nav": nav,
                    "nav_date": nav_date,
                    "scheme_type": scheme_type,
                    "amc_name": amc_name,
                }
            )

        if not rows:
            raise ParseError("AMFI file contained no data rows")

        return pl.DataFrame(
            rows,
            schema={
                "scheme_code": pl.String, "isin_growth": pl.String,
                "isin_reinvest": pl.String, "scheme_name": pl.String,
                "nav": pl.String, "nav_date": pl.String,
                "scheme_type": pl.String, "amc_name": pl.String,
            },
        )
```

- [ ] **Step 5: Register the contract case**

Append to `tests/contracts/parser_cases.py`:

```python
from trading.parsers.amfi import AmfiNavParser

PARSER_CASES.append(
    ParserCase(
        name="amfi",
        parser=AmfiNavParser(),
        fixture=FIXTURE_ROOT / "amfi" / "navall.txt",
        source_key="amfi_nav",
        min_rows=20,
        required_columns=("scheme_code", "nav", "nav_date", "amc_name", "scheme_type"),
    )
)
```

- [ ] **Step 6: Run the full parser and contract suite**

Run: `uv run pytest tests/parsers tests/contracts -v`
Expected: PASS. All three parsers now participate; mutual exclusivity is verified across 3×2 cross-pairs.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(parsers): AMFI NAV stateful line scanner"
```

---

## Task 9: Trading calendar

**Assignee:** Sonnet · **Depends on:** Task 4

The calendar is what lets the ledger tell *"we missed a day"* from *"it was Diwali."* Everything downstream depends on it.

**Files:**
- Create: `src/trading/calendar/{__init__,trading_days,seed}.py`, `data/seed/nse_holidays.csv`, `tests/calendar/test_trading_days.py`

**Interfaces:**
- Consumes: `psycopg.Connection`
- Produces:
  - `is_trading_day(conn, exchange, segment, d) -> bool`
  - `trading_days(conn, exchange, segment, start, end) -> list[date]`
  - `seed_calendar(conn, exchange, segment, start, end, holidays: set[date]) -> int`

**Approach:** generate every weekday in range as a trading day, then mark listed holidays as non-trading. The holiday seed CSV is committed (`YYYY-MM-DD,description`); NSE publishes annual lists. Weekend-only inference is wrong for India — there are ~15 additional holidays a year plus occasional Muhurat sessions on a Sunday.

- [ ] **Step 1: Write the failing test**

`tests/calendar/test_trading_days.py`:

```python
from datetime import date

import pytest

from trading.calendar.trading_days import is_trading_day, seed_calendar, trading_days

pytestmark = pytest.mark.db


def test_weekends_are_not_trading_days(db_conn):
    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), holidays=set())
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 14)) is True   # Friday
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 15)) is False  # Saturday


def test_listed_holiday_is_not_a_trading_day(db_conn):
    seed_calendar(
        db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16),
        holidays={date(2026, 8, 13)},
    )
    assert is_trading_day(db_conn, "NSE", "CM", date(2026, 8, 13)) is False


def test_trading_days_returns_only_open_sessions_in_order(db_conn):
    seed_calendar(
        db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16),
        holidays={date(2026, 8, 13)},
    )
    days = trading_days(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16))
    assert days == [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 14)]


def test_seeding_is_idempotent(db_conn):
    first = seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), set())
    second = seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 16), set())
    assert first == second
    count = db_conn.execute(
        "SELECT count(*) FROM trading_calendar WHERE exchange='NSE' AND segment='CM'"
    ).fetchone()[0]
    assert count == 7
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/calendar -v -m db` → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/calendar/trading_days.py`**

```python
from __future__ import annotations

from datetime import date, timedelta

from psycopg import Connection

SESSION_OPEN = "09:15"
SESSION_CLOSE = "15:30"


def seed_calendar(
    conn: Connection,
    exchange: str,
    segment: str,
    start: date,
    end: date,
    holidays: set[date],
) -> int:
    """Insert one row per calendar day in range. Idempotent."""
    rows = []
    day = start
    while day <= end:
        is_open = day.weekday() < 5 and day not in holidays
        rows.append(
            (exchange, segment, day, is_open,
             SESSION_OPEN if is_open else None,
             SESSION_CLOSE if is_open else None,
             "holiday" if (day.weekday() < 5 and day in holidays) else None)
        )
        day += timedelta(days=1)

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO trading_calendar "
            "(exchange, segment, session_date, is_trading_day, session_open, session_close, note) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (exchange, segment, session_date) DO UPDATE SET "
            "is_trading_day = EXCLUDED.is_trading_day, note = EXCLUDED.note",
            rows,
        )
    return len(rows)


def is_trading_day(conn: Connection, exchange: str, segment: str, d: date) -> bool:
    row = conn.execute(
        "SELECT is_trading_day FROM trading_calendar "
        "WHERE exchange=%s AND segment=%s AND session_date=%s",
        (exchange, segment, d),
    ).fetchone()
    if row is None:
        raise LookupError(f"calendar has no entry for {exchange}/{segment} {d}")
    return bool(row[0])


def trading_days(
    conn: Connection, exchange: str, segment: str, start: date, end: date
) -> list[date]:
    rows = conn.execute(
        "SELECT session_date FROM trading_calendar "
        "WHERE exchange=%s AND segment=%s AND session_date BETWEEN %s AND %s "
        "AND is_trading_day ORDER BY session_date",
        (exchange, segment, start, end),
    ).fetchall()
    return [r[0] for r in rows]
```

- [ ] **Step 4: Run tests** → `uv run pytest tests/calendar -v -m db` → PASS (4 passed)

- [ ] **Step 5: Seed ten years of holidays**

Populate `data/seed/nse_holidays.csv` from NSE's published annual holiday lists (2016–2026), then:

```bash
uv run python -m trading.calendar.seed --exchange NSE --segment CM --from 2016-01-01 --to 2026-12-31
```

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(calendar): trading calendar with holiday seeding"
```

---

## Task 10: Normalizers

**Assignee:** Sonnet · **Depends on:** Tasks 6, 7, 8

**Files:**
- Create: `src/trading/normalizers/{__init__,udiff,nse_legacy,amfi}.py`, `tests/normalizers/test_udiff.py`, `tests/normalizers/test_nse_legacy.py`, `tests/normalizers/test_amfi.py`

**Interfaces:**
- Consumes: `trading.contracts.{Normalizer, NormalizedBatch, RawPayload, CANONICAL_BAR_SCHEMA, assert_canonical, AssetClass, DataSource}`
- Produces: `UdiffNormalizer()`, `NseLegacyNormalizer()`, `AmfiNormalizer()`, each satisfying `Normalizer`

**The mapping that matters** (`FinInstrmTp` → `asset_class`, finding F1):

| `FinInstrmTp` | `asset_class` |
|---|---|
| `STK` | `EQUITY` |
| `STO`, `IDO` | `OPTION` |
| `STF`, `IDF` | `FUTURE` |

`ts` is the **session close** in UTC: `TradDt` at 15:30 Asia/Kolkata → `10:00:00Z`. Empty strings become `null`, never `0`.

- [ ] **Step 1: Write the failing UDiFF normalizer test**

`tests/normalizers/test_udiff.py`:

```python
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import DataSource, assert_canonical
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.udiff import UdiffParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "udiff"


def _batch(fixture: str, source_key: str):
    payload = make_payload(FIXTURES / fixture, source_key, date(2026, 8, 13))
    return UdiffNormalizer().normalize(UdiffParser().parse(payload), payload)


def test_output_matches_the_canonical_contract():
    assert_canonical(_batch("nse_cm_udiff.zip", "nse_cm_udiff").frame)


def test_equity_rows_map_to_equity_asset_class():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert set(frame["asset_class"].unique()) == {"EQUITY"}
    assert frame["expiry"].null_count() == frame.height
    assert frame["strike"].null_count() == frame.height


def test_option_rows_carry_strike_expiry_and_type():
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    options = frame.filter(frame["asset_class"] == "OPTION")
    assert options.height > 0
    assert options["strike"].null_count() == 0
    assert options["expiry"].null_count() == 0
    assert set(options["option_type"].unique()) <= {"CE", "PE"}


def test_ts_is_session_close_in_utc():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert frame["ts"][0] == datetime.fromisoformat("2026-08-13T10:00:00+00:00")


def test_untraded_option_keeps_zero_ohlc_and_real_close():
    """Finding F2: must survive normalization, not be nulled or dropped."""
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    untraded = frame.filter((frame["volume"] == 0) & (frame["close"] > Decimal("0")))
    assert untraded.height > 0
    assert untraded["open"][0] == Decimal("0.0000")


def test_empty_strings_become_null_not_zero():
    frame = _batch("nse_cm_udiff.zip", "nse_cm_udiff").frame
    assert frame["open_interest"].null_count() == frame.height  # CM has no OI


def test_lot_size_is_carried_through():
    """Finding F3: NewBrdLotQty feeds instrument_lot_history for free."""
    frame = _batch("nse_fo_udiff.zip", "nse_fo_udiff").frame
    assert frame["lot_size"].null_count() == 0
    assert (frame["lot_size"] > 0).all()


def test_source_is_tagged_per_segment():
    assert _batch("nse_fo_udiff.zip", "nse_fo_udiff").source is DataSource.NSE_FO_UDIFF
    assert _batch("bse_cm_udiff.csv", "bse_cm_udiff").source is DataSource.BSE_CM_UDIFF
```

- [ ] **Step 2: Run and confirm failure** → `uv run pytest tests/normalizers -v` → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/normalizers/udiff.py`**

```python
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import polars as pl

from trading.contracts import (
    CANONICAL_BAR_SCHEMA,
    AssetClass,
    DataSource,
    NormalizedBatch,
    RawPayload,
)

IST = ZoneInfo("Asia/Kolkata")
SESSION_CLOSE = time(15, 30)

ASSET_CLASS_BY_TYPE = {
    "STK": AssetClass.EQUITY.value,
    "STO": AssetClass.OPTION.value,
    "IDO": AssetClass.OPTION.value,
    "STF": AssetClass.FUTURE.value,
    "IDF": AssetClass.FUTURE.value,
}

SOURCE_BY_KEY = {
    "nse_cm_udiff": DataSource.NSE_CM_UDIFF,
    "nse_fo_udiff": DataSource.NSE_FO_UDIFF,
    "bse_cm_udiff": DataSource.BSE_CM_UDIFF,
}


def _blank_to_null(name: str) -> pl.Expr:
    return pl.when(pl.col(name).str.strip_chars() == "").then(None).otherwise(pl.col(name))


def _dec(name: str, precision: int = 18, scale: int = 4) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Decimal(precision, scale))


def _int(name: str, dtype: pl.DataType = pl.Int64) -> pl.Expr:
    return _blank_to_null(name).cast(pl.Float64).cast(dtype)


class UdiffNormalizer:
    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        trade_date = date.fromisoformat(frame["TradDt"][0])
        ts = datetime.combine(trade_date, SESSION_CLOSE, tzinfo=IST).astimezone(
            ZoneInfo("UTC")
        )

        out = frame.select(
            exchange=pl.col("Src"),
            segment=pl.col("Sgmt"),
            symbol=pl.col("TckrSymb"),
            asset_class=pl.col("FinInstrmTp").replace_strict(
                ASSET_CLASS_BY_TYPE, default=AssetClass.EQUITY.value
            ),
            expiry=_blank_to_null("XpryDt").str.to_date("%Y-%m-%d", strict=False),
            strike=_dec("StrkPric"),
            option_type=_blank_to_null("OptnTp"),
            isin=_blank_to_null("ISIN"),
            name=_blank_to_null("FinInstrmNm"),
            ts=pl.lit(ts).cast(pl.Datetime("us", "UTC")),
            open=_dec("OpnPric"),
            high=_dec("HghPric"),
            low=_dec("LwPric"),
            close=_dec("ClsPric"),
            prev_close=_dec("PrvsClsgPric"),
            settle_price=_dec("SttlmPric"),
            underlying_price=_dec("UndrlygPric"),
            volume=_int("TtlTradgVol"),
            turnover=_dec("TtlTrfVal", 22, 4),
            trades=_int("TtlNbOfTxsExctd", pl.Int32),
            open_interest=_int("OpnIntrst"),
            oi_change=_int("ChngInOpnIntrst"),
            delivery_qty=pl.lit(None, dtype=pl.Int64),
            delivery_pct=pl.lit(None, dtype=pl.Decimal(7, 4)),
            lot_size=_int("NewBrdLotQty", pl.Int32),
            tick_size=pl.lit(None, dtype=pl.Decimal(12, 6)),
        ).select(list(CANONICAL_BAR_SCHEMA))

        return NormalizedBatch(
            source=SOURCE_BY_KEY[payload.source_key],
            business_date=payload.business_date,
            frame=out,
        )
```

- [ ] **Step 4: Run the UDiFF normalizer tests** → PASS (8 passed)

- [ ] **Step 5: Implement `nse_legacy.py` and `amfi.py` normalizers**

Legacy: `TIMESTAMP` parses with `%d-%b-%Y` (uppercase month works with `%b`); `asset_class` is always `EQUITY`; `open_interest`, `settle_price`, `underlying_price`, `lot_size`, `delivery_*` are all `null`; `exchange` is the literal `"NSE"`, `segment` `"CM"`.

AMFI: `nav_date` parses with `%d-%b-%Y`; `asset_class` is `MF`; `exchange` is the literal `"AMFI"`, `segment` `"MF"`, `symbol` is `scheme_code`; `close` is the NAV and `open`/`high`/`low` are set equal to it; a `nav` of `N.A.` yields a null close, which the validator will quarantine.

Write mirror tests for each asserting `assert_canonical` passes and the date/asset-class mapping is right.

- [ ] **Step 6: Run all normalizer tests** → `uv run pytest tests/normalizers -v` → PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(normalizers): UDiFF, legacy and AMFI to canonical schema"
```

---

## Task 11: Instrument resolver

**Assignee:** Sonnet · **Depends on:** Tasks 4, 10

The only stage that both reads and writes instrument state. It must create instruments it has never seen, because every trading day mints new option strikes — and it must refuse to do so at absurd scale, because a parser typo would otherwise silently mint garbage.

**Files:**
- Create: `src/trading/resolver/{__init__,instruments}.py`, `tests/resolver/test_instruments.py`

**Interfaces:**
- Consumes: `trading.contracts.{InstrumentRef, InstrumentResolver, ValidationAbort, AssetClass}`
- Produces: `DbInstrumentResolver(max_new_per_batch: int = 5000)` with
  `.resolve(refs, conn, *, bootstrap=False) -> dict[InstrumentRef, int]`
  and `.record_lot_sizes(conn, lot_rows: list[tuple[int, date, int]]) -> int`

- [ ] **Step 1: Write the failing test**

`tests/resolver/test_instruments.py`:

```python
from datetime import date
from decimal import Decimal

import pytest

from trading.contracts import InstrumentRef, OptionType, ValidationAbort
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db


def _ref(symbol: str = "RELIANCE") -> InstrumentRef:
    return InstrumentRef(exchange="NSE", segment="CM", symbol=symbol)


def test_creates_instruments_it_has_never_seen(db_conn):
    mapping = DbInstrumentResolver().resolve({_ref()}, db_conn)
    assert set(mapping) == {_ref()}
    assert isinstance(mapping[_ref()], int)


def test_resolving_twice_returns_the_same_id(db_conn):
    resolver = DbInstrumentResolver()
    first = resolver.resolve({_ref()}, db_conn)
    second = resolver.resolve({_ref()}, db_conn)
    assert first[_ref()] == second[_ref()]


def test_a_second_resolver_instance_reuses_the_stored_row(db_conn):
    """The cache must not be the only source of identity."""
    first = DbInstrumentResolver().resolve({_ref()}, db_conn)
    second = DbInstrumentResolver().resolve({_ref()}, db_conn)
    assert first[_ref()] == second[_ref()]


def test_option_refs_get_distinct_ids_per_strike(db_conn):
    refs = {
        InstrumentRef(
            exchange="NSE", segment="FO", symbol="NIFTY", expiry=date(2026, 8, 27),
            strike=Decimal(s), option_type=OptionType.CE,
        )
        for s in ("24500", "24600")
    }
    mapping = DbInstrumentResolver().resolve(refs, db_conn)
    assert len(set(mapping.values())) == 2


def test_strike_scale_does_not_create_a_duplicate(db_conn):
    """Decimal('24500') and Decimal('24500.00') are one contract."""
    resolver = DbInstrumentResolver()
    a = InstrumentRef(exchange="NSE", segment="FO", symbol="NIFTY",
                      expiry=date(2026, 8, 27), strike=Decimal("24500"),
                      option_type=OptionType.CE)
    b = a.model_copy(update={"strike": Decimal("24500.0000")})
    assert resolver.resolve({a}, db_conn)[a] == resolver.resolve({b}, db_conn)[b]


def test_aborts_when_a_batch_would_mint_absurdly_many_instruments(db_conn):
    """Guards against a parser typo generating garbage at scale."""
    refs = {_ref(f"JUNK{i}") for i in range(11)}
    with pytest.raises(ValidationAbort, match="new instruments"):
        DbInstrumentResolver(max_new_per_batch=10).resolve(refs, db_conn)


def test_bootstrap_flag_bypasses_the_abort_guard(db_conn):
    refs = {_ref(f"BOOT{i}") for i in range(11)}
    mapping = DbInstrumentResolver(max_new_per_batch=10).resolve(
        refs, db_conn, bootstrap=True
    )
    assert len(mapping) == 11


def test_lot_history_records_only_changes(db_conn):
    resolver = DbInstrumentResolver()
    iid = resolver.resolve({_ref("LOTTEST")}, db_conn)[_ref("LOTTEST")]
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 10), 75)])
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 11), 75)])   # unchanged
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 12), 50)])   # changed
    rows = db_conn.execute(
        "SELECT effective_from, lot_size FROM instrument_lot_history "
        "WHERE instrument_id=%s ORDER BY effective_from", (iid,),
    ).fetchall()
    assert rows == [(date(2026, 8, 10), 75), (date(2026, 8, 12), 50)]
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/resolver/instruments.py`**

```python
from __future__ import annotations

from datetime import date

import structlog
from psycopg import Connection

from trading.contracts import AssetClass, InstrumentRef, ValidationAbort

log = structlog.get_logger(__name__)


def _asset_class_for(ref: InstrumentRef) -> str:
    if ref.option_type is not None:
        return AssetClass.OPTION.value
    if ref.expiry is not None:
        return AssetClass.FUTURE.value
    if ref.segment == "MF":
        return AssetClass.MF.value
    return AssetClass.EQUITY.value


class DbInstrumentResolver:
    """Maps natural keys to instrument_ids, creating unseen instruments."""

    def __init__(self, max_new_per_batch: int = 5000) -> None:
        self._max_new = max_new_per_batch
        self._cache: dict[str, int] = {}

    def resolve(
        self, refs: set[InstrumentRef], conn: Connection, *, bootstrap: bool = False
    ) -> dict[InstrumentRef, int]:
        by_key = {r.canonical_key: r for r in refs}
        resolved = {k: self._cache[k] for k in by_key if k in self._cache}

        unknown = [k for k in by_key if k not in resolved]
        if unknown:
            rows = conn.execute(
                "SELECT canonical_key, instrument_id FROM instruments "
                "WHERE canonical_key = ANY(%s)", (unknown,),
            ).fetchall()
            for key, iid in rows:
                resolved[key] = iid
                self._cache[key] = iid

        missing = [k for k in by_key if k not in resolved]
        if missing:
            if not bootstrap and len(missing) > self._max_new:
                raise ValidationAbort(
                    f"batch would create {len(missing)} new instruments "
                    f"(limit {self._max_new}); suspected parser fault"
                )
            resolved.update(self._create(missing, by_key, conn))

        return {by_key[k]: resolved[k] for k in by_key}

    def _create(
        self, keys: list[str], by_key: dict[str, InstrumentRef], conn: Connection
    ) -> dict[str, int]:
        payload = [
            (
                _asset_class_for(by_key[k]), by_key[k].exchange, by_key[k].segment,
                by_key[k].symbol, by_key[k].expiry, by_key[k].strike,
                by_key[k].option_type.value if by_key[k].option_type else None,
                "INR", "ACTIVE", k,
            )
            for k in keys
        ]
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO instruments (asset_class, exchange, segment, symbol, expiry,"
                " strike, option_type, currency, status, canonical_key)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (canonical_key) DO NOTHING",
                payload,
            )
        rows = conn.execute(
            "SELECT canonical_key, instrument_id FROM instruments "
            "WHERE canonical_key = ANY(%s)", (keys,),
        ).fetchall()
        created = {key: iid for key, iid in rows}
        self._cache.update(created)
        log.info("instruments.created", count=len(created))
        return created

    def record_lot_sizes(
        self, conn: Connection, lot_rows: list[tuple[int, date, int]]
    ) -> int:
        """Append a lot-history row only when the lot size actually changed."""
        written = 0
        for instrument_id, effective_from, lot_size in lot_rows:
            current = conn.execute(
                "SELECT lot_size FROM instrument_lot_history WHERE instrument_id=%s "
                "ORDER BY effective_from DESC LIMIT 1", (instrument_id,),
            ).fetchone()
            if current is not None and current[0] == lot_size:
                continue
            conn.execute(
                "INSERT INTO instrument_lot_history (instrument_id, effective_from,"
                " lot_size, source) VALUES (%s,%s,%s,'udiff')"
                " ON CONFLICT (instrument_id, effective_from) DO NOTHING",
                (instrument_id, effective_from, lot_size),
            )
            written += 1
        return written
```

- [ ] **Step 4: Run tests** → `uv run pytest tests/resolver -v -m db` → PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(resolver): instrument resolution with abort guard and lot history"
```

---

## Task 12: Validator

**Assignee:** Sonnet · **Depends on:** Task 10

**Files:**
- Create: `src/trading/validation/{__init__,bars}.py`, `tests/validation/test_bars.py`

**Interfaces:**
- Consumes: `trading.contracts.{Validator, NormalizedBatch, ValidationOutcome, QuarantineRow}`
- Produces: `BarValidator()` satisfying `Validator`

**Invariants** — each violation quarantines the row with the named reason, and **never raises**:

| Reason | Condition |
|---|---|
| `close_missing` | `close` is null |
| `close_not_positive` | `close <= 0` |
| `ohlc_inconsistent` | `volume > 0` **and** OHLC ordering violated |
| `negative_volume` | `volume < 0` |
| `duplicate_key` | same `(instrument_id, ts)` twice in one batch |
| `nav_not_available` | AMFI `N.A.` NAV (null close from an MF row) |

**Critical:** untraded F&O rows (`volume = 0`, OHLC = 0, `close > 0`) **must pass**. Finding F2 — quarantining them discards half of every F&O day.

- [ ] **Step 1: Write the failing test**

`tests/validation/test_bars.py`:

```python
from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, NormalizedBatch
from trading.validation.bars import BarValidator

TS = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


def _frame(**overrides) -> pl.DataFrame:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    row.update(
        exchange="NSE", segment="CM", symbol="TEST", asset_class="EQUITY", ts=TS,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("95"),
        close=Decimal("105"), volume=1000,
    )
    row.update(overrides)
    return pl.DataFrame([row], schema=CANONICAL_BAR_SCHEMA)


def _validate(frame: pl.DataFrame):
    return BarValidator().validate(
        NormalizedBatch(DataSource.NSE_CM_UDIFF, date(2026, 8, 13), frame)
    )


def test_a_good_row_passes():
    outcome = _validate(_frame())
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_untraded_option_with_zero_ohlc_passes():
    """Finding F2 — 49% of F&O rows look like this."""
    outcome = _validate(
        _frame(open=Decimal("0"), high=Decimal("0"), low=Decimal("0"),
               close=Decimal("19.45"), volume=0)
    )
    assert outcome.valid.height == 1
    assert outcome.quarantined == []


def test_traded_row_with_high_below_low_is_quarantined():
    outcome = _validate(_frame(high=Decimal("90"), low=Decimal("95")))
    assert outcome.valid.height == 0
    assert outcome.quarantined[0].reason == "ohlc_inconsistent"


def test_missing_close_is_quarantined():
    outcome = _validate(_frame(close=None))
    assert outcome.quarantined[0].reason == "close_missing"


def test_negative_volume_is_quarantined():
    outcome = _validate(_frame(volume=-5))
    assert outcome.quarantined[0].reason == "negative_volume"


def test_duplicate_keys_quarantine_the_later_row_only():
    frame = pl.concat([_frame(), _frame(close=Decimal("106"))])
    outcome = _validate(frame)
    assert outcome.valid.height == 1
    assert outcome.quarantined[0].reason == "duplicate_key"


def test_one_bad_row_does_not_fail_the_batch():
    """A 3-in-100k failure must not block a 2,500-day backfill."""
    frame = pl.concat([_frame(symbol="GOOD"), _frame(symbol="BAD", close=None)])
    outcome = _validate(frame)
    assert outcome.valid.height == 1
    assert len(outcome.quarantined) == 1
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/validation/bars.py`**

```python
from __future__ import annotations

import polars as pl

from trading.contracts import NormalizedBatch, QuarantineRow, ValidationOutcome

_KEY = ("exchange", "segment", "symbol", "expiry", "strike", "option_type", "ts")


class BarValidator:
    def validate(self, batch: NormalizedBatch) -> ValidationOutcome:
        frame = batch.frame.with_row_index("_row")

        reason = (
            pl.when(pl.col("close").is_null())
            .then(pl.lit("close_missing"))
            .when(pl.col("close") <= 0)
            .then(pl.lit("close_not_positive"))
            .when(pl.col("volume") < 0)
            .then(pl.lit("negative_volume"))
            .when(
                (pl.col("volume") > 0)
                & (
                    (pl.col("high") < pl.col("low"))
                    | (pl.col("high") < pl.col("open"))
                    | (pl.col("high") < pl.col("close"))
                    | (pl.col("low") > pl.col("open"))
                    | (pl.col("low") > pl.col("close"))
                )
            )
            .then(pl.lit("ohlc_inconsistent"))
            .when(pl.col("_dup"))
            .then(pl.lit("duplicate_key"))
            .otherwise(None)
            .alias("_reason")
        )

        frame = frame.with_columns(
            pl.col("_row").cum_count().over(list(_KEY)).gt(1).alias("_dup")
        ).with_columns(reason)

        bad = frame.filter(pl.col("_reason").is_not_null())
        good = frame.filter(pl.col("_reason").is_null()).drop("_row", "_dup", "_reason")

        quarantined = [
            QuarantineRow(reason=row.pop("_reason"), payload=row)
            for row in bad.drop("_row", "_dup").to_dicts()
        ]
        return ValidationOutcome(valid=good, quarantined=quarantined)
```

- [ ] **Step 4: Run tests** → `uv run pytest tests/validation -v` → PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(validation): bar invariants with volume-aware OHLC rule"
```

---

## Task 13: Loader

**Assignee:** Sonnet · **Depends on:** Tasks 4, 11, 12

**Files:**
- Create: `src/trading/loaders/{__init__,bars}.py`, `tests/loaders/test_bars.py`

**Interfaces:**
- Consumes: `trading.contracts.{Loader, ValidationOutcome, LoadResult}`, `DbInstrumentResolver`
- Produces: `BarLoader(resolver: DbInstrumentResolver)` satisfying `Loader`

**Approach (spec §5.3):** `COPY` into an `UNLOGGED` staging table, then one `INSERT … SELECT … ON CONFLICT (instrument_id, ts) DO UPDATE`. Row-by-row upsert of a 100k-row F&O day takes minutes; this takes ~2 seconds.

- [ ] **Step 1: Write the failing test**

`tests/loaders/test_bars.py`:

```python
from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl
import pytest

from trading.contracts import CANONICAL_BAR_SCHEMA, ValidationOutcome
from trading.loaders.bars import BarLoader
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db
TS = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


def _outcome(close: str = "105", symbol: str = "LOADTEST") -> ValidationOutcome:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    row.update(
        exchange="NSE", segment="CM", symbol=symbol, asset_class="EQUITY", ts=TS,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("95"),
        close=Decimal(close), volume=1000, lot_size=1,
    )
    return ValidationOutcome(valid=pl.DataFrame([row], schema=CANONICAL_BAR_SCHEMA))


def test_load_writes_a_row(db_conn):
    result = BarLoader(DbInstrumentResolver()).load(_outcome(), db_conn)
    assert result.rows_written == 1
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 1


def test_loading_the_same_batch_twice_is_a_no_op(db_conn):
    loader = BarLoader(DbInstrumentResolver())
    loader.load(_outcome(), db_conn)
    loader.load(_outcome(), db_conn)
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 1


def test_reloading_with_a_corrected_price_overwrites(db_conn):
    """NSE restates files; the newer value must win, not duplicate."""
    loader = BarLoader(DbInstrumentResolver())
    loader.load(_outcome(close="105"), db_conn)
    loader.load(_outcome(close="107"), db_conn)
    rows = db_conn.execute("SELECT close FROM bars_daily").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == Decimal("107.0000")


def test_lot_size_is_recorded_in_history(db_conn):
    BarLoader(DbInstrumentResolver()).load(_outcome(), db_conn)
    count = db_conn.execute("SELECT count(*) FROM instrument_lot_history").fetchone()[0]
    assert count == 1


def test_empty_outcome_writes_nothing(db_conn):
    empty = ValidationOutcome(valid=pl.DataFrame(schema=CANONICAL_BAR_SCHEMA))
    assert BarLoader(DbInstrumentResolver()).load(empty, db_conn).rows_written == 0
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/loaders/bars.py`**

```python
from __future__ import annotations

import csv
import io

import polars as pl
from psycopg import Connection

from trading.contracts import DataSource, InstrumentRef, LoadResult, ValidationOutcome
from trading.resolver.instruments import DbInstrumentResolver

STAGING_COLUMNS = (
    "instrument_id", "ts", "open", "high", "low", "close", "prev_close", "volume",
    "turnover", "trades", "settle_price", "open_interest", "oi_change",
    "delivery_qty", "delivery_pct", "source",
)


class BarLoader:
    def __init__(self, resolver: DbInstrumentResolver, source: DataSource | None = None):
        self._resolver = resolver
        self._source = source

    def load(self, outcome: ValidationOutcome, conn: Connection) -> LoadResult:
        frame = outcome.valid
        if frame.height == 0:
            return LoadResult(rows_written=0, instruments_created=0)

        refs = {
            InstrumentRef(
                exchange=r["exchange"], segment=r["segment"], symbol=r["symbol"],
                expiry=r["expiry"], strike=r["strike"], option_type=r["option_type"],
            )
            for r in frame.select(
                "exchange", "segment", "symbol", "expiry", "strike", "option_type"
            ).to_dicts()
        }
        before = conn.execute("SELECT count(*) FROM instruments").fetchone()[0]
        mapping = self._resolver.resolve(refs, conn)
        created = conn.execute("SELECT count(*) FROM instruments").fetchone()[0] - before

        source_id = int(self._source) if self._source else int(DataSource.NSE_CM_UDIFF)
        rows = []
        lot_rows = []
        for record in frame.to_dicts():
            ref = InstrumentRef(
                exchange=record["exchange"], segment=record["segment"],
                symbol=record["symbol"], expiry=record["expiry"],
                strike=record["strike"], option_type=record["option_type"],
            )
            iid = mapping[ref]
            rows.append([iid] + [record[c] for c in STAGING_COLUMNS[1:-1]] + [source_id])
            if record["lot_size"]:
                lot_rows.append((iid, record["ts"].date(), int(record["lot_size"])))

        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _stage_bars "
                "(LIKE bars_daily INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.execute("TRUNCATE _stage_bars")
            buffer = io.StringIO()
            csv.writer(buffer).writerows(rows)
            buffer.seek(0)
            with cur.copy(
                f"COPY _stage_bars ({','.join(STAGING_COLUMNS)}) FROM STDIN WITH CSV"
            ) as copy:
                copy.write(buffer.read())
            cur.execute(
                f"INSERT INTO bars_daily ({','.join(STAGING_COLUMNS)}) "
                f"SELECT {','.join(STAGING_COLUMNS)} FROM _stage_bars "
                "ON CONFLICT (instrument_id, ts) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, prev_close=EXCLUDED.prev_close, "
                "volume=EXCLUDED.volume, turnover=EXCLUDED.turnover, "
                "trades=EXCLUDED.trades, settle_price=EXCLUDED.settle_price, "
                "open_interest=EXCLUDED.open_interest, oi_change=EXCLUDED.oi_change, "
                "delivery_qty=COALESCE(EXCLUDED.delivery_qty, bars_daily.delivery_qty), "
                "delivery_pct=COALESCE(EXCLUDED.delivery_pct, bars_daily.delivery_pct), "
                "source=EXCLUDED.source, ingested_at=now()"
            )

        self._resolver.record_lot_sizes(conn, lot_rows)
        return LoadResult(rows_written=frame.height, instruments_created=created)
```

Note the `COALESCE` on `delivery_*`: those arrive from a **separate** NSE file and must not be nulled by a later UDiFF upsert of the same row.

- [ ] **Step 4: Run tests** → `uv run pytest tests/loaders -v -m db` → PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(loaders): COPY-to-staging upsert loader for bars_daily"
```

---

## Task 14: Pipeline runner, ledger and backfill

**Assignee:** Sonnet · **Depends on:** Tasks 5, 6, 9, 10, 11, 12, 13

**Files:**
- Create: `src/trading/pipeline/{__init__,ledger,runner,backfill}.py`, `tests/pipeline/test_ledger.py`, `tests/pipeline/test_runner.py`, `tests/pipeline/test_backfill.py`

**Interfaces:**
- Consumes: every stage protocol, `trading.calendar.trading_days`
- Produces:
  - `claim_job(conn, source_key, business_date, content_hash: str | None = None) -> int | None` (None ⇒ already SUCCESS with an unchanged hash, or another runner holds it)
  - `complete_job(conn, job_id, status, *, rows, quarantined, content_hash, archive_path, error=None)`
  - `Pipeline(source, registry, normalizer, resolver, validator, loader)` with `.run(conn, business_date) -> JobStatus`
  - `BackfillRunner(pipeline, exchange, segment)` with `.missing_days(conn, start, end) -> list[date]` and `.run(conn, start, end) -> dict[JobStatus, int]`

- [ ] **Step 1: Write the failing ledger test**

`tests/pipeline/test_ledger.py`:

```python
from datetime import date

import pytest

from trading.contracts import JobStatus
from trading.pipeline.ledger import claim_job, complete_job

pytestmark = pytest.mark.db
D = date(2026, 8, 13)


def test_claim_creates_a_running_job(db_conn):
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    assert job_id is not None
    status = db_conn.execute(
        "SELECT status FROM ingest_jobs WHERE job_id=%s", (job_id,)
    ).fetchone()[0]
    assert status == "RUNNING"


def test_a_second_claim_on_a_running_job_is_refused(db_conn):
    claim_job(db_conn, "nse_cm_udiff", D)
    assert claim_job(db_conn, "nse_cm_udiff", D) is None


def test_a_failed_job_can_be_reclaimed(db_conn):
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    complete_job(db_conn, job_id, JobStatus.FAILED, rows=0, quarantined=0,
                 content_hash=None, archive_path=None, error="boom")
    assert claim_job(db_conn, "nse_cm_udiff", D) is not None


def test_a_successful_job_with_the_same_hash_is_not_reclaimed(db_conn):
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    complete_job(db_conn, job_id, JobStatus.SUCCESS, rows=10, quarantined=0,
                 content_hash="abc", archive_path="/x", error=None)
    assert claim_job(db_conn, "nse_cm_udiff", D, content_hash="abc") is None


def test_a_restated_file_reclaims_the_job(db_conn):
    """NSE restates files; a changed hash must force a re-parse."""
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    complete_job(db_conn, job_id, JobStatus.SUCCESS, rows=10, quarantined=0,
                 content_hash="abc", archive_path="/x", error=None)
    assert claim_job(db_conn, "nse_cm_udiff", D, content_hash="different") is not None


def test_attempt_counter_increments(db_conn):
    claim_job(db_conn, "nse_cm_udiff", D)
    db_conn.execute("UPDATE ingest_jobs SET status='FAILED' WHERE business_date=%s", (D,))
    claim_job(db_conn, "nse_cm_udiff", D)
    attempt = db_conn.execute(
        "SELECT attempt FROM ingest_jobs WHERE business_date=%s", (D,)
    ).fetchone()[0]
    assert attempt == 2
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/pipeline/ledger.py`**

```python
from __future__ import annotations

from datetime import date

from psycopg import Connection

from trading.contracts import JobStatus

_RECLAIMABLE = ("PENDING", "FAILED", "RUNNING")


def claim_job(
    conn: Connection, source_key: str, business_date: date, content_hash: str | None = None
) -> int | None:
    """Claim a job, or return None if it is already done and unchanged."""
    existing = conn.execute(
        "SELECT job_id, status, content_hash FROM ingest_jobs "
        "WHERE source_key=%s AND business_date=%s FOR UPDATE",
        (source_key, business_date),
    ).fetchone()

    if existing is not None:
        job_id, status, stored_hash = existing
        if status == JobStatus.SUCCESS.value:
            if content_hash is None or content_hash == stored_hash:
                return None            # unchanged: nothing to do
        elif status == JobStatus.RUNNING.value:
            return None                # another runner holds it
        conn.execute(
            "UPDATE ingest_jobs SET status='RUNNING', attempt=attempt+1, "
            "started_at=now(), error=NULL WHERE job_id=%s",
            (job_id,),
        )
        return int(job_id)

    row = conn.execute(
        "INSERT INTO ingest_jobs (source_key, business_date, status, attempt, started_at) "
        "VALUES (%s,%s,'RUNNING',1,now()) RETURNING job_id",
        (source_key, business_date),
    ).fetchone()
    return int(row[0])


def complete_job(
    conn: Connection,
    job_id: int,
    status: JobStatus,
    *,
    rows: int,
    quarantined: int,
    content_hash: str | None,
    archive_path: str | None,
    error: str | None = None,
) -> None:
    conn.execute(
        "UPDATE ingest_jobs SET status=%s, rows_written=%s, quarantine_count=%s, "
        "content_hash=%s, archive_path=%s, error=%s, finished_at=now() WHERE job_id=%s",
        (status.value, rows, quarantined, content_hash, archive_path, error, job_id),
    )
```

- [ ] **Step 4: Run ledger tests** → PASS (6 passed)

- [ ] **Step 5: Write the failing runner test**

`tests/pipeline/test_runner.py` — build a `Pipeline` from the real UDiFF stages plus a **stub source** that returns the committed fixture, so no network is touched:

```python
from datetime import date

import pytest

from trading.contracts import JobStatus

pytestmark = pytest.mark.db


def test_run_ingests_a_day_end_to_end(db_conn, udiff_pipeline):
    assert udiff_pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.SUCCESS
    count = db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0]
    assert count == 50


def test_running_the_same_day_twice_changes_nothing(db_conn, udiff_pipeline):
    """Spec 5.3: re-running any day must be a provable no-op."""
    udiff_pipeline.run(db_conn, date(2026, 8, 13))
    first = db_conn.execute(
        "SELECT md5(string_agg(instrument_id::text||close::text, ',' ORDER BY instrument_id))"
        " FROM bars_daily"
    ).fetchone()[0]
    udiff_pipeline.run(db_conn, date(2026, 8, 13))
    second = db_conn.execute(
        "SELECT md5(string_agg(instrument_id::text||close::text, ',' ORDER BY instrument_id))"
        " FROM bars_daily"
    ).fetchone()[0]
    assert first == second


def test_absent_source_marks_skipped_no_data(db_conn, absent_pipeline):
    assert absent_pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.SKIPPED_NO_DATA


def test_a_parse_failure_marks_the_job_failed_and_writes_no_bars(db_conn, broken_pipeline):
    assert broken_pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.FAILED
    assert db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0] == 0
```

- [ ] **Step 6: Implement `src/trading/pipeline/runner.py`**

```python
from __future__ import annotations

from datetime import date

import structlog
from psycopg import Connection

from trading.contracts import (
    JobStatus,
    Loader,
    Normalizer,
    ParseError,
    Source,
    Validator,
)
from trading.pipeline.ledger import claim_job, complete_job
from trading.resolver.instruments import DbInstrumentResolver

log = structlog.get_logger(__name__)


class Pipeline:
    """Runs the six stages for one (source, date) inside one transaction."""

    def __init__(
        self,
        source: Source,
        registry,
        normalizer: Normalizer,
        resolver: DbInstrumentResolver,
        validator: Validator,
        loader: Loader,
    ) -> None:
        self._source = source
        self._registry = registry
        self._normalizer = normalizer
        self._resolver = resolver
        self._validator = validator
        self._loader = loader

    def run(self, conn: Connection, business_date: date) -> JobStatus:
        key = self._source.source_key
        job_id = claim_job(conn, key, business_date)
        if job_id is None:
            log.info("pipeline.skipped_already_done", source=key, date=business_date)
            return JobStatus.SUCCESS

        try:
            payload = self._source.fetch(business_date)
            if payload is None:
                complete_job(conn, job_id, JobStatus.SKIPPED_NO_DATA, rows=0,
                             quarantined=0, content_hash=None, archive_path=None)
                return JobStatus.SKIPPED_NO_DATA

            parser = self._registry.select(payload)
            batch = self._normalizer.normalize(parser.parse(payload), payload)
            outcome = self._validator.validate(batch)
            result = self._loader.load(outcome, conn)

            complete_job(
                conn, job_id, JobStatus.SUCCESS,
                rows=result.rows_written, quarantined=len(outcome.quarantined),
                content_hash=payload.content_hash, archive_path=str(payload.archive_path),
            )
            self._write_quarantine(conn, job_id, outcome.quarantined)
            conn.commit()
            return JobStatus.SUCCESS

        except (ParseError, Exception) as exc:  # noqa: B014 - deliberate catch-all
            conn.rollback()
            job_id = claim_job(conn, key, business_date) or job_id
            complete_job(conn, job_id, JobStatus.FAILED, rows=0, quarantined=0,
                         content_hash=None, archive_path=None, error=str(exc)[:2000])
            conn.commit()
            log.error("pipeline.failed", source=key, date=business_date, error=str(exc))
            return JobStatus.FAILED

    @staticmethod
    def _write_quarantine(conn: Connection, job_id: int, rows) -> None:
        if not rows:
            return
        import json

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,%s,%s)",
                [(job_id, r.reason, json.dumps(r.payload, default=str)) for r in rows],
            )
```

- [ ] **Step 7: Implement `src/trading/pipeline/backfill.py`**

```python
from __future__ import annotations

from collections import Counter
from datetime import date

import structlog
from psycopg import Connection

from trading.calendar.trading_days import trading_days
from trading.contracts import JobStatus
from trading.pipeline.runner import Pipeline

log = structlog.get_logger(__name__)


class BackfillRunner:
    """Computes the missing days for a source and runs them in order."""

    def __init__(self, pipeline: Pipeline, exchange: str, segment: str) -> None:
        self._pipeline = pipeline
        self._exchange = exchange
        self._segment = segment

    def missing_days(self, conn: Connection, start: date, end: date) -> list[date]:
        expected = trading_days(conn, self._exchange, self._segment, start, end)
        done = {
            row[0]
            for row in conn.execute(
                "SELECT business_date FROM ingest_jobs WHERE source_key=%s "
                "AND status IN ('SUCCESS','SKIPPED_HOLIDAY','SKIPPED_NO_DATA') "
                "AND business_date BETWEEN %s AND %s",
                (self._pipeline._source.source_key, start, end),
            ).fetchall()
        }
        return [d for d in expected if d not in done]

    def run(self, conn: Connection, start: date, end: date) -> dict[JobStatus, int]:
        counts: Counter[JobStatus] = Counter()
        days = self.missing_days(conn, start, end)
        log.info("backfill.start", days=len(days), start=start, end=end)
        for index, day in enumerate(days, start=1):
            counts[self._pipeline.run(conn, day)] += 1
            if index % 50 == 0:
                log.info("backfill.progress", done=index, total=len(days))
        return dict(counts)
```

- [ ] **Step 8: Write the backfill test**

`tests/pipeline/test_backfill.py`:

```python
from datetime import date

import pytest

pytestmark = pytest.mark.db


def test_missing_days_excludes_completed_and_holidays(db_conn, udiff_backfill):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 14),
                  holidays={date(2026, 8, 12)})
    assert udiff_backfill.missing_days(db_conn, date(2026, 8, 10), date(2026, 8, 14)) == [
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 13), date(2026, 8, 14)
    ]


def test_a_completed_day_is_not_repeated(db_conn, udiff_backfill):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 13), date(2026, 8, 13), set())
    udiff_backfill.run(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert udiff_backfill.missing_days(db_conn, date(2026, 8, 13), date(2026, 8, 13)) == []
```

- [ ] **Step 9: Run the whole suite**

Run: `uv run pytest -v -m "db or not db"`
Expected: PASS across contracts, parsers, normalizers, resolver, validation, loaders, pipeline.

- [ ] **Step 10: Commit**

```bash
git add -A && git commit -m "feat(pipeline): transactional runner, job ledger and resumable backfill"
```

---

## Task 15: Raw market recorder

**Assignee:** Sonnet · **Depends on:** Task 1 and broker credentials only
**Independent track — schedule this FIRST among delegated work.** Its value is a function of wall-clock days elapsed, not effort (spec D16).

**Files:**
- Create: `src/trading/recorder/{__init__,session,upstox_ws,__main__}.py`, `tests/recorder/test_session.py`, `deploy/recorder.service`

**Interfaces:**
- Consumes: `trading.config.get_settings`
- Produces:
  - `RecordingSession(root: Path, source_key: str, session_date: date)` with `.open()`, `.write_frame(bytes)`, `.note_connect()`, `.note_disconnect(reason)`, `.heartbeat()`, `.close()`
  - `SessionManifest` serialised to `session.json`

**Governing principle (spec §5.6): record raw, parse later.** The recorder never parses, normalises, resolves, or touches the database. If capture-time interpretation is wrong the data is lost forever; raw frames can be re-read for a decade.

**The manifest is as important as the frames.** A disconnect from 11:32 to 11:35 is indistinguishable from three minutes of no trading unless gaps are recorded as explicit events.

- [ ] **Step 1: Write the failing session test**

`tests/recorder/test_session.py`:

```python
import gzip
import json
from datetime import date
from pathlib import Path

from trading.recorder.session import RecordingSession


def _session(tmp_path: Path) -> RecordingSession:
    return RecordingSession(root=tmp_path, source_key="upstox_chain",
                            session_date=date(2026, 8, 13))


def test_frames_are_written_gzipped_and_readable(tmp_path):
    session = _session(tmp_path)
    session.open()
    session.write_frame(b"frame-one")
    session.write_frame(b"frame-two")
    session.close()

    files = sorted((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    assert files
    with gzip.open(files[0], "rb") as handle:
        assert handle.read().count(b"frame-") == 2


def test_manifest_records_a_disconnect_as_an_explicit_gap(tmp_path):
    """Without this, a dropped socket looks like three minutes of no trades."""
    session = _session(tmp_path)
    session.open()
    session.note_connect()
    session.note_disconnect("socket closed")
    session.note_connect()
    session.close()

    manifest = json.loads(
        (tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text()
    )
    gaps = manifest["gaps"]
    assert len(gaps) == 1
    assert gaps[0]["reason"] == "socket closed"
    assert gaps[0]["started_at"] and gaps[0]["ended_at"]


def test_manifest_is_written_even_when_the_session_crashes(tmp_path):
    session = _session(tmp_path)
    session.open()
    session.write_frame(b"x")
    try:
        with session:
            raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    manifest_path = tmp_path / "upstox_chain" / "2026-08-13" / "session.json"
    assert manifest_path.exists()
    assert json.loads(manifest_path.read_text())["frame_count"] == 1


def test_heartbeat_file_is_touched(tmp_path):
    session = _session(tmp_path)
    session.open()
    session.heartbeat()
    assert (tmp_path / "upstox_chain" / "heartbeat").exists()


def test_frame_count_and_subscriptions_are_recorded(tmp_path):
    session = _session(tmp_path)
    session.open()
    session.record_subscriptions(requested=["NIFTY", "BANKNIFTY"], acknowledged=["NIFTY"])
    session.write_frame(b"a")
    session.close()
    manifest = json.loads(
        (tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text()
    )
    assert manifest["frame_count"] == 1
    assert manifest["subscriptions"]["requested"] == ["NIFTY", "BANKNIFTY"]
    assert manifest["subscriptions"]["acknowledged"] == ["NIFTY"]
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/recorder/session.py`**

Hourly rotation bounds crash loss to the current hour. The manifest is flushed on **every** mutation, not only at close — a killed process must still leave a truthful record.

```python
from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType


@dataclass
class Gap:
    started_at: str
    ended_at: str | None
    reason: str


@dataclass
class SessionManifest:
    source_key: str
    session_date: str
    started_at: str
    ended_at: str | None = None
    frame_count: int = 0
    subscriptions: dict[str, list[str]] = field(default_factory=dict)
    gaps: list[Gap] = field(default_factory=list)
    connects: list[str] = field(default_factory=list)


class RecordingSession:
    """Durably captures raw frames plus a manifest that makes gaps explicit."""

    def __init__(self, root: Path, source_key: str, session_date: date) -> None:
        self._dir = root / source_key / session_date.isoformat()
        self._heartbeat_path = root / source_key / "heartbeat"
        self._manifest_path = self._dir / "session.json"
        self._manifest = SessionManifest(
            source_key=source_key,
            session_date=session_date.isoformat(),
            started_at=datetime.now(UTC).isoformat(),
        )
        self._handle: gzip.GzipFile | None = None
        self._hour: int | None = None

    def open(self) -> RecordingSession:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        self._flush_manifest()
        return self

    def __enter__(self) -> RecordingSession:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.close()

    def _rotate_if_needed(self) -> gzip.GzipFile:
        hour = datetime.now(UTC).hour
        if self._handle is None or hour != self._hour:
            if self._handle is not None:
                self._handle.close()
            self._handle = gzip.open(self._dir / f"{hour:02d}.frames.gz", "ab")
            self._hour = hour
        return self._handle

    def write_frame(self, frame: bytes) -> None:
        handle = self._rotate_if_needed()
        handle.write(frame)
        handle.write(b"\n")
        self._manifest.frame_count += 1
        if self._manifest.frame_count % 500 == 0:
            handle.flush()
            self._flush_manifest()

    def record_subscriptions(self, requested: list[str], acknowledged: list[str]) -> None:
        self._manifest.subscriptions = {
            "requested": requested, "acknowledged": acknowledged
        }
        self._flush_manifest()

    def note_connect(self) -> None:
        now = datetime.now(UTC).isoformat()
        self._manifest.connects.append(now)
        for gap in self._manifest.gaps:
            if gap.ended_at is None:
                gap.ended_at = now
        self._flush_manifest()

    def note_disconnect(self, reason: str) -> None:
        self._manifest.gaps.append(
            Gap(started_at=datetime.now(UTC).isoformat(), ended_at=None, reason=reason)
        )
        self._flush_manifest()

    def heartbeat(self) -> None:
        self._heartbeat_path.write_text(datetime.now(UTC).isoformat())

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        self._manifest.ended_at = datetime.now(UTC).isoformat()
        self._flush_manifest()

    def _flush_manifest(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._manifest_path.write_text(json.dumps(asdict(self._manifest), indent=2))
```

- [ ] **Step 4: Run session tests** → `uv run pytest tests/recorder -v` → PASS (5 passed)

- [ ] **Step 5: Implement `upstox_ws.py` and `__main__.py`**

The WebSocket loop must: authorise via the Upstox feed endpoint, subscribe to the configured universe, call `record_subscriptions`, then loop writing every received frame verbatim. On any exception: `note_disconnect(reason)`, sleep with exponential backoff capped at 30 s, reconnect, `note_connect()`. Call `heartbeat()` once a minute. Exit cleanly after the session close time. **Never crash the session over a malformed frame** — record the anomaly and continue.

`__main__.py` reads the universe from config, builds a `RecordingSession`, and runs the loop under `if __name__ == "__main__"`.

- [ ] **Step 6: Write the deployment unit**

`deploy/recorder.service` (systemd, for the Oracle Ampere host per D17):

```ini
[Unit]
Description=Trading raw market recorder
After=network-online.target

[Service]
Type=simple
User=trading
WorkingDirectory=/opt/trading
EnvironmentFile=/opt/trading/.env.local
ExecStart=/opt/trading/.venv/bin/python -m trading.recorder
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 7: Verify manually against the live feed**

Once credentials exist, run for five minutes during market hours and confirm: frames accumulate, `session.json` shows a non-zero `frame_count`, the acknowledged subscription list matches the request, and `heartbeat` updates.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(recorder): raw frame capture with explicit gap tracking"
```

---

## Task 16: Corporate actions and read-time adjustment

**Assignee:** Sonnet · **Depends on:** Tasks 11, 13

**Files:**
- Create: `src/trading/corpactions/{__init__,ingest,adjust}.py`, `tests/corpactions/test_adjust.py`

**Interfaces:**
- Consumes: `psycopg.Connection`
- Produces:
  - `adjustment_factors(conn, instrument_id, start, end) -> pl.DataFrame` with columns `ex_date`, `factor`
  - `adjusted_bars(conn, instrument_id, start, end, *, as_of) -> pl.DataFrame`
  - `ingest_corporate_actions(conn, rows) -> int`

**Rule (spec §4.3, D10):** for a query as of `as_of`, a bar at `t` is multiplied by the product of `ratio_from/ratio_to` for every action with `ex_date` in `(t, as_of]`. A 1:5 split (`ratio_from=1`, `ratio_to=5`) scales pre-split prices by `1/5`. Volumes scale inversely. **Adjusted prices are never stored.**

- [ ] **Step 1: Write the failing test**

`tests/corpactions/test_adjust.py`:

```python
from datetime import date
from decimal import Decimal

import pytest

from trading.corpactions.adjust import adjusted_bars

pytestmark = pytest.mark.db


def test_a_split_scales_prices_before_the_ex_date(db_conn, seeded_instrument):
    """1:5 split on 2026-08-12 → the 08-11 close of 500 becomes 100."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500, date(2026, 8, 13): 100})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(db_conn, iid, date(2026, 8, 10), date(2026, 8, 14),
                          as_of=date(2026, 8, 14))
    by_date = {r["ts"].date(): r["close"] for r in frame.to_dicts()}
    assert by_date[date(2026, 8, 11)] == Decimal("100.0000")
    assert by_date[date(2026, 8, 13)] == Decimal("100.0000")


def test_bars_after_the_ex_date_are_untouched(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 13): 100})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(db_conn, iid, date(2026, 8, 13), date(2026, 8, 13),
                          as_of=date(2026, 8, 14))
    assert frame["close"][0] == Decimal("100.0000")


def test_volume_scales_inversely_to_price(db_conn, seeded_instrument):
    iid = seeded_instrument(closes={date(2026, 8, 11): 500}, volume=1000)
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date,"
        " ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2026-08-12',1,5,'test')",
        (iid,),
    )
    frame = adjusted_bars(db_conn, iid, date(2026, 8, 11), date(2026, 8, 11),
                          as_of=date(2026, 8, 14))
    assert frame["volume"][0] == 5000


def test_an_action_announced_after_as_of_is_ignored(db_conn, seeded_instrument):
    """Point-in-time: a backtest must not know about a split before announcement."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, ratio_from,"
        " ratio_to, announced_at, source)"
        " VALUES (%s,'SPLIT','2026-08-12',1,5,'2026-08-20T00:00:00Z','test')",
        (iid,),
    )
    frame = adjusted_bars(db_conn, iid, date(2026, 8, 11), date(2026, 8, 11),
                          as_of=date(2026, 8, 14))
    assert frame["close"][0] == Decimal("500.0000")


def test_unadjusted_prices_remain_in_storage(db_conn, seeded_instrument):
    """D10: adjustment is a read-time view, never a rewrite."""
    iid = seeded_instrument(closes={date(2026, 8, 11): 500})
    stored = db_conn.execute(
        "SELECT close FROM bars_daily WHERE instrument_id=%s", (iid,)
    ).fetchone()[0]
    assert stored == Decimal("500.0000")
```

- [ ] **Step 2: Run and confirm failure** → FAIL, module missing.

- [ ] **Step 3: Implement `src/trading/corpactions/adjust.py`**

The subtle part is the direction of the cumulative product: a bar at `t` must be scaled by every action *after* `t`, so the factor is a **reverse** cumulative product over ex-dates.

```python
from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl
from psycopg import Connection

_BAR_QUERY = """
    SELECT ts, open, high, low, close, prev_close, volume
    FROM bars_daily
    WHERE instrument_id = %s AND ts::date BETWEEN %s AND %s
    ORDER BY ts
"""

# announced_at IS NULL is treated as "always known" (documented choice).
_ACTION_QUERY = """
    SELECT ex_date, ratio_from, ratio_to
    FROM corporate_actions
    WHERE instrument_id = %s
      AND action_type IN ('SPLIT', 'BONUS')
      AND ex_date <= %s
      AND (announced_at IS NULL OR announced_at::date <= %s)
    ORDER BY ex_date
"""


def adjustment_factors(
    conn: Connection, instrument_id: int, start: date, end: date, as_of: date
) -> list[tuple[date, Decimal]]:
    rows = conn.execute(_ACTION_QUERY, (instrument_id, as_of, as_of)).fetchall()
    return [
        (ex_date, Decimal(ratio_from) / Decimal(ratio_to))
        for ex_date, ratio_from, ratio_to in rows
        if ratio_to
    ]


def adjusted_bars(
    conn: Connection, instrument_id: int, start: date, end: date, *, as_of: date
) -> pl.DataFrame:
    """Return bars scaled to as_of terms. Storage is never modified (D10)."""
    bars = conn.execute(_BAR_QUERY, (instrument_id, start, end)).fetchall()
    frame = pl.DataFrame(
        bars,
        schema={
            "ts": pl.Datetime("us", "UTC"), "open": pl.Decimal(18, 4),
            "high": pl.Decimal(18, 4), "low": pl.Decimal(18, 4),
            "close": pl.Decimal(18, 4), "prev_close": pl.Decimal(18, 4),
            "volume": pl.Int64,
        },
        orient="row",
    )
    if frame.height == 0:
        return frame

    actions = adjustment_factors(conn, instrument_id, start, end, as_of)
    if not actions:
        return frame

    def factor_for(bar_day: date) -> Decimal:
        """Product of every action strictly AFTER this bar."""
        result = Decimal(1)
        for ex_date, factor in actions:
            if ex_date > bar_day:
                result *= factor
        return result

    factors = [factor_for(ts.date()) for ts in frame["ts"]]
    factor_col = pl.Series("factor", factors, dtype=pl.Decimal(18, 8))

    return frame.with_columns(factor_col).with_columns(
        *[
            (pl.col(c) * pl.col("factor")).cast(pl.Decimal(18, 4)).alias(c)
            for c in ("open", "high", "low", "close", "prev_close")
        ],
        (pl.col("volume") / pl.col("factor")).round(0).cast(pl.Int64).alias("volume"),
    ).drop("factor")
```

- [ ] **Step 4: Run tests** → PASS (5 passed)

- [ ] **Step 5: Implement `ingest.py`**

Parse NSE's corporate-actions feed into `corporate_actions` rows, upserting on the unique index. Set `announced_at` from the announcement timestamp when available, otherwise leave null and treat null as "always known" — documenting that choice in the module docstring.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(corpactions): read-time adjustment with point-in-time announcements"
```

---

## Task 17: Execute the backfill and reconcile

**Assignee:** Opus drives; Sonnet writes the reconciliation queries · **Depends on:** all

**Files:**
- Create: `scripts/backfill.py`, `src/trading/reconcile.py`, `tests/test_reconcile.py`, `docs/phase-0-completion-report.md`

- [ ] **Step 1: Seed the calendar for the full range**

```bash
uv run python -m trading.calendar.seed --exchange NSE --segment CM --from 2016-01-01 --to 2026-12-31
uv run python -m trading.calendar.seed --exchange NSE --segment FO --from 2016-01-01 --to 2026-12-31
uv run python -m trading.calendar.seed --exchange BSE --segment CM --from 2016-01-01 --to 2026-12-31
```

- [ ] **Step 2: Run one day per source and inspect the results by hand**

```bash
uv run python scripts/backfill.py --source nse_cm_udiff --from 2026-08-13 --to 2026-08-13
uv run python scripts/backfill.py --source nse_fo_udiff --from 2026-08-13 --to 2026-08-13
```
Confirm row counts are in the expected range (~3,500 CM; ~35,000 FO) before committing to 2,500 days.

- [ ] **Step 3: Run the UDiFF era, newest first**

```bash
uv run python scripts/backfill.py --source nse_cm_udiff --from 2024-07-01 --to 2026-08-14
uv run python scripts/backfill.py --source nse_fo_udiff --from 2024-07-01 --to 2026-08-14
uv run python scripts/backfill.py --source bse_cm_udiff --from 2024-07-01 --to 2026-08-14
```

- [ ] **Step 4: Run the legacy era**

```bash
uv run python scripts/backfill.py --source nse_cm_legacy --from 2016-01-01 --to 2024-06-30
```

- [ ] **Step 5: Write and run the reconciliation checks (spec §8)**

`src/trading/reconcile.py` implements one function per check, each returning `(passed: bool, detail: str)`:

1. `check_calendar_completeness` — every trading day has a terminal job per active source.
2. `check_known_values` — a committed table of hand-verified closes matches the database.
3. `check_cross_source_agreement` — NIFTY spot from the index rows and `underlying_price` on F&O rows agree within one tick.
4. `check_continuity` — no unexplained single-day move beyond ±20% without a matching corporate action.
5. `check_idempotency` — re-running a random 30-day window changes no rows (compare a checksum before and after).
6. `check_quarantine_rate` — under 0.01% of rows, with every distinct reason enumerated.
7. `check_recorder_liveness` — every trading day since credentials landed has a manifest with under 1% total gap.

Run: `uv run python -m trading.reconcile --report docs/phase-0-completion-report.md`

- [ ] **Step 6: Review the report and resolve every failure**

A check that fails is either a bug to fix or a documented, justified exception. **Neither may be left silent.**

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(reconcile): phase 0 verification checks and completion report"
```

---

## Known gaps in this plan

Found during plan self-review against the spec. Neither is silent; both are decisions.

**G1 — Delivery data is not ingested.** Spec §4.5 gives `bars_daily` the columns `delivery_qty` and `delivery_pct`, noting they "arrive from a separate NSE file and land via upsert," and Task 13's loader correctly `COALESCE`s them so a later UDiFF upsert cannot null them. But **no task fetches that file** (`sec_bhavdata_full_DDMMYYYY.csv`). The columns will stay null through Phase 0.

Deferring is defensible: delivery percentage is a conviction proxy consumed by the intelligence layer in Phase 2.5, not by anything in Phase 0. Adding it later is one more `Source` + `Parser` + `Normalizer` triple through the same six stages — roughly Task 6's size — and the loader already handles the merge. **Raise this with the user; it is their call whether to add it now or in Phase 2.5.**

**G2 — `JobStatus.SKIPPED_HOLIDAY` is defined but never produced.** `BackfillRunner.missing_days` derives its work from `trading_days`, which already excludes holidays, so the pipeline never attempts one. The status is retained because a manually-triggered single-day run *can* hit a holiday, and Task 17's reconciliation check 1 accepts it as a terminal state. No action needed; documented so a future reader does not assume it is dead code and delete it.

---

## Appendix: dependency graph

```
Task 1  scaffold
  ├── Task 2  contracts ──┬── Task 3  contract suite ──┬── Task 6  UDiFF parser
  │                       │                            ├── Task 7  legacy parser
  │                       │                            └── Task 8  AMFI parser
  │                       └── Task 5  sources ─────────────────┘
  ├── Task 4  migrations ─┬── Task 9  calendar
  │                       └── Task 11 resolver
  ├── Task 15 RECORDER  ← independent; schedule FIRST once credentials exist
  │
  └── Tasks 6,7,8 ── Task 10 normalizers ── Task 12 validator ── Task 13 loader
                                                                      │
                          Tasks 5,9,10,11,12,13 ── Task 14 pipeline ──┤
                                                   Task 16 corpactions┤
                                                                      └── Task 17 backfill
```

**Parallelisable once Tasks 1–5 land:** 6, 7, 8 (three subagents), and 15 on its own track from the start.

