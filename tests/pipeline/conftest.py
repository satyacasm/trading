"""Ruling C3 (task-14 addendum): the brief's tests reference `udiff_pipeline`,
`absent_pipeline`, `broken_pipeline` and `udiff_backfill` but define them
nowhere -- they are defined here, wiring the real stages against a stub
`Source` that returns a committed fixture from `tests/fixtures/udiff/`
rather than hitting the network.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import pytest
from psycopg import Connection

from trading.contracts import DataSource, JobStatus, RawPayload
from trading.loaders.bars import BarLoader
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.registry import ParserRegistry
from trading.parsers.udiff import UdiffParser
from trading.pipeline.backfill import BackfillRunner
from trading.pipeline.ledger import claim_job, complete_job
from trading.pipeline.runner import Pipeline
from trading.resolver.instruments import DbInstrumentResolver
from trading.validation.bars import BarValidator

FIXTURES = Path(__file__).parent.parent / "fixtures" / "udiff"
BUSINESS_DATE = date(2026, 8, 13)


def _payload(source_key: str, content: bytes, business_date: date = BUSINESS_DATE) -> RawPayload:
    return RawPayload(
        source_key=source_key,
        business_date=business_date,
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=Path(f"/nonexistent/{source_key}.bin"),
    )


class _StubSource:
    """A `Source` that returns a fixed payload (or None) instead of hitting
    the network -- the no-network-in-tests constraint is absolute."""

    def __init__(self, source_key: str, payload: RawPayload | None) -> None:
        self.source_key = source_key
        self._payload = payload

    def fetch(self, business_date: date) -> RawPayload | None:
        return self._payload


def _udiff_pipeline_for(payload: RawPayload | None, source_key: str = "nse_cm_udiff") -> Pipeline:
    """Wires the real UDiFF stages -- parser, normalizer, resolver,
    validator, loader -- against a stub source returning `payload`."""
    resolver = DbInstrumentResolver()
    loader = BarLoader(resolver, DataSource.NSE_CM_UDIFF)
    return Pipeline(
        source=_StubSource(source_key, payload),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=loader,
    )


@pytest.fixture
def udiff_pipeline() -> Pipeline:
    content = (FIXTURES / "nse_cm_udiff.zip").read_bytes()
    return _udiff_pipeline_for(_payload("nse_cm_udiff", content))


@pytest.fixture
def absent_pipeline() -> Pipeline:
    """The no-data contract: the stub source returns None."""
    return _udiff_pipeline_for(None)


@pytest.fixture
def broken_pipeline() -> Pipeline:
    """Bytes no parser accepts -> ParserRegistry.select raises ParseError."""
    garbage = b"not,a,udiff,header\n1,2,3,4\n"
    return _udiff_pipeline_for(_payload("nse_cm_udiff", garbage))


class _StubBackfillPipeline:
    """A `_PipelineLike` stand-in for `BackfillRunner`'s own tests.

    `BackfillRunner.run` commits after every day (Ruling P1x) -- real
    durability, unlike `Pipeline.run`, and therefore NOT protected by
    `tests/conftest.py`'s rollback-at-teardown. A real `Pipeline` wired to
    the UDiFF stages here would permanently write bars/instruments into the
    same database a real backfill uses. This stub never touches
    `bars_daily` or `instruments`; the only write it performs is the ledger
    row `claim_job`/`complete_job` already need to exist for `missing_days`
    to see a day as done, and it performs that write under a `source_key`
    ("test_backfill_stub") that can never collide with a real source's
    ledger rows even if a test run's commit lands in a shared database.
    """

    source_key = "test_backfill_stub"

    def run(self, conn: Connection, business_date: date) -> JobStatus:
        job_id = claim_job(conn, self.source_key, business_date)
        if job_id is None:
            return JobStatus.SUCCESS
        complete_job(
            conn,
            job_id,
            JobStatus.SKIPPED_NO_DATA,
            rows=0,
            quarantined=0,
            content_hash=None,
            archive_path=None,
        )
        return JobStatus.SKIPPED_NO_DATA


@pytest.fixture
def udiff_backfill(db_url: str) -> Iterator[BackfillRunner]:
    yield BackfillRunner(_StubBackfillPipeline(), "NSE", "CM")
    # `BackfillRunner.run` commits real writes under this stub's dedicated
    # source_key (Ruling P1x: it owns durability, so `db_conn`'s
    # rollback-at-teardown cannot undo them). Left uncleaned, a committed
    # `test_backfill_stub` row for 2026-08-13 would silently corrupt
    # `test_ledger.py`'s `test_attempt_counter_increments`, whose own final
    # SELECT is scoped by `business_date` alone (verbatim from the brief,
    # not something this task may change) and so isn't guaranteed to pick
    # the right row when more than one exists for that date. A fresh
    # connection is used because `db_conn` may already be mid-transaction.
    with psycopg.connect(db_url, autocommit=True) as cleanup_conn:
        cleanup_conn.execute(
            "DELETE FROM ingest_jobs WHERE source_key=%s",
            (_StubBackfillPipeline.source_key,),
        )
