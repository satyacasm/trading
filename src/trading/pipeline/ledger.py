from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from psycopg import Connection

from trading.contracts import JobStatus

DEFAULT_STALE_AFTER = timedelta(minutes=30)


class JobHeld(Exception):
    """Raised by `claim_job` (Ruling P4x) when a `RUNNING` row exists and is
    NOT yet stale: another process genuinely holds this claim right now.

    This is the deliberate counterpart to `claim_job` returning `None`: the
    two used to be conflated (both meant "do nothing, report success"),
    which is exactly how a crashed process's stale claim was silently
    reported as a permanent success. `None` now means only "already done";
    a live claim raises this instead, loudly, so a caller can never mistake
    "someone else is working on it" for "nothing left to do".
    """

    def __init__(self, source_key: str, business_date: date, job_id: int) -> None:
        self.source_key = source_key
        self.business_date = business_date
        self.job_id = job_id
        super().__init__(
            f"job {job_id} for {source_key} {business_date} is RUNNING and held by another process"
        )


def claim_job(
    conn: Connection,
    source_key: str,
    business_date: date,
    content_hash: str | None = None,
    stale_after: timedelta = DEFAULT_STALE_AFTER,
) -> int | None:
    """Claim a job.

    Returns `None` to mean exactly one thing (Ruling P4x): the day is
    genuinely complete -- an existing `SUCCESS` row with an unchanged
    content hash. A `RUNNING` row is a different situation entirely and is
    never reported the same way: if it is older than `stale_after` it is
    presumed abandoned by a crashed process and taken over (`attempt`
    incremented, `started_at` reset, its `job_id` returned so the day
    actually runs); if it is still fresh, `JobHeld` is raised instead of
    returning `None`, because Phase 0 runs single-process and a fresh
    `RUNNING` row can only mean another runner genuinely holds this claim
    right now.
    """
    existing = conn.execute(
        "SELECT job_id, status, content_hash, started_at FROM ingest_jobs "
        "WHERE source_key=%s AND business_date=%s FOR UPDATE",
        (source_key, business_date),
    ).fetchone()

    if existing is not None:
        job_id, status, stored_hash, started_at = existing
        if status == JobStatus.SUCCESS.value:
            if content_hash is None or content_hash == stored_hash:
                return None  # unchanged: nothing to do
        elif status == JobStatus.RUNNING.value:
            is_stale = started_at is not None and started_at < datetime.now(UTC) - stale_after
            if not is_stale:
                raise JobHeld(source_key, business_date, int(job_id))
            # else: a crashed process's abandoned claim -- take it over
            # exactly like a FAILED row, via the reclaim UPDATE below.
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
    return int(row[0]) if row is not None else None


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
    cur = conn.execute(
        "UPDATE ingest_jobs SET status=%s, rows_written=%s, quarantine_count=%s, "
        "content_hash=%s, archive_path=%s, error=%s, finished_at=now() WHERE job_id=%s",
        (status.value, rows, quarantined, content_hash, archive_path, error, job_id),
    )
    # Ruling P2x: a caller passing a job id whose row no longer exists (or
    # somehow matches more than one row) must fail loudly here, not lose the
    # status update silently. Ruling P1x makes the specific scenario the
    # brief worried about (a rolled-back claim) unreachable, but this guards
    # every future caller too.
    if cur.rowcount != 1:
        raise LookupError(
            f"complete_job: expected to update exactly 1 ingest_jobs row for "
            f"job_id={job_id}, but updated {cur.rowcount}"
        )
