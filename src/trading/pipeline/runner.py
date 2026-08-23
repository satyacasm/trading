from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from typing import Protocol

import structlog
from psycopg import Connection

from trading.contracts import (
    DataSource,
    JobStatus,
    Loader,
    Normalizer,
    QuarantineRow,
    Source,
    ValidationAbort,
    Validator,
)
from trading.parsers.registry import ParserRegistry
from trading.pipeline.ledger import JobHeld, claim_job, complete_job
from trading.resolver.instruments import DbInstrumentResolver

log = structlog.get_logger(__name__)


class _SourcedLoader(Loader, Protocol):
    """A `Loader` that also exposes the `DataSource` it was configured for.

    `Loader` (trading.contracts.protocols) says nothing about provenance, and
    this task's scope forbids widening contracts.py to add it there. Ruling
    P3x's seam check is typed against this narrower, pipeline-local protocol
    instead. `BarLoader.source` (the one permitted addition outside this
    package) satisfies it structurally.
    """

    @property
    def source(self) -> DataSource: ...


class Pipeline:
    """Runs the six stages for one (source, date) inside one savepoint.

    Ruling P1x: `run` never calls `conn.commit()` or `conn.rollback()`.
    Callers own durability -- `BackfillRunner` commits after each day, and
    `tests/conftest.py`'s `db_conn` fixture rolls back at teardown against
    the same database a real backfill would use. Each day's stage work runs
    inside `conn.transaction()`, which opens a real transaction if the
    connection has none yet and a SAVEPOINT if one is already open -- which
    it always is here, because `claim_job` has already issued a statement.
    On failure the savepoint unwinds the day's partial writes; the ledger's
    FAILED update is written after the `with` block exits, so it survives
    the rollback.
    """

    def __init__(
        self,
        source: Source,
        registry: ParserRegistry,
        normalizer: Normalizer,
        resolver: DbInstrumentResolver,
        validator: Validator,
        loader: _SourcedLoader,
    ) -> None:
        self._source = source
        self._registry = registry
        self._normalizer = normalizer
        self._resolver = resolver
        self._validator = validator
        self._loader = loader

    @property
    def source_key(self) -> str:
        """Ruling C4: lets `BackfillRunner` avoid reaching into `_source`."""
        return self._source.source_key

    def run(self, conn: Connection, business_date: date) -> JobStatus:
        key = self._source.source_key

        # Ruling P4x: JobHeld must be caught here, before the day's stage
        # work even starts, and must NOT fall into the catch-all below --
        # this process does not own that job's ledger row, so it must
        # touch nothing and simply report that someone else is on it.
        try:
            job_id = claim_job(conn, key, business_date)
        except JobHeld:
            log.info("pipeline.job_held", source=key, date=business_date)
            return JobStatus.RUNNING

        if job_id is None:
            log.info("pipeline.skipped_already_done", source=key, date=business_date)
            return JobStatus.SUCCESS

        try:
            with conn.transaction():
                payload = self._source.fetch(business_date)
                if payload is None:
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

                parser = self._registry.select(payload)
                batch = self._normalizer.normalize(parser.parse(payload), payload)

                # Ruling P3x: the normalizer independently stamps a `source`
                # on the batch; nothing upstream of this guarantees it
                # agrees with the loader's own configured `DataSource`. A
                # mismatch here means an F&O batch about to be loaded and
                # permanently labelled as CM data (or similar) -- fail
                # loudly instead of writing it.
                if batch.source != self._loader.source:
                    raise ValidationAbort(
                        f"provenance mismatch for {key} {business_date}: "
                        f"normalizer produced batch.source={batch.source!r} but "
                        f"loader is configured for source={self._loader.source!r}"
                    )

                outcome = self._validator.validate(batch)
                result = self._loader.load(outcome, conn)

                complete_job(
                    conn,
                    job_id,
                    JobStatus.SUCCESS,
                    rows=result.rows_written,
                    quarantined=len(outcome.quarantined),
                    content_hash=payload.content_hash,
                    archive_path=str(payload.archive_path),
                )
                self._write_quarantine(conn, job_id, outcome.quarantined)
            return JobStatus.SUCCESS

        except Exception as exc:
            # Ruling P1x: the savepoint above has already unwound the day's
            # partial writes by the time we get here (`conn.transaction()`
            # rolls back to the savepoint and re-raises on error). This
            # FAILED update runs outside that savepoint, in whatever
            # transaction/connection scope the caller owns, so it survives.
            complete_job(
                conn,
                job_id,
                JobStatus.FAILED,
                rows=0,
                quarantined=0,
                content_hash=None,
                archive_path=None,
                error=str(exc)[:2000],
            )
            log.error("pipeline.failed", source=key, date=business_date, error=str(exc))
            return JobStatus.FAILED

    @staticmethod
    def _write_quarantine(conn: Connection, job_id: int, rows: Sequence[QuarantineRow]) -> None:
        if not rows:
            return
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,%s,%s)",
                [(job_id, r.reason, json.dumps(r.payload, default=str)) for r in rows],
            )
