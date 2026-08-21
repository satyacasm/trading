from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Protocol

import structlog
from psycopg import Connection

from trading.calendar.trading_days import trading_days
from trading.contracts import JobStatus

log = structlog.get_logger(__name__)


class _PipelineLike(Protocol):
    """What `BackfillRunner` needs from a pipeline.

    `Pipeline` satisfies this structurally, and so does the DB-write-free
    stub `BackfillRunner`'s own tests drive (Ruling C3) -- typing against
    this narrower, pipeline-local protocol instead of the concrete
    `Pipeline` class lets that substitution work without touching
    contracts.py.
    """

    source_key: str

    def run(self, conn: Connection, business_date: date) -> JobStatus: ...


class BackfillRunner:
    """Computes the missing days for a source and runs them in order.

    Ruling P1x: this is the layer that owns durability. `Pipeline.run`
    itself never commits or rolls back, so `BackfillRunner.run` commits
    after every day -- success, failure or skip alike -- so a process that
    dies mid-backfill keeps every day already completed instead of
    re-running it from scratch.
    """

    def __init__(self, pipeline: _PipelineLike, exchange: str, segment: str) -> None:
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
                (self._pipeline.source_key, start, end),
            ).fetchall()
        }
        return [d for d in expected if d not in done]

    def run(self, conn: Connection, start: date, end: date) -> dict[JobStatus, int]:
        counts: Counter[JobStatus] = Counter()
        days = self.missing_days(conn, start, end)
        log.info("backfill.start", days=len(days), start=start, end=end)
        for index, day in enumerate(days, start=1):
            counts[self._pipeline.run(conn, day)] += 1
            conn.commit()
            if index % 50 == 0:
                log.info("backfill.progress", done=index, total=len(days))
        return dict(counts)
