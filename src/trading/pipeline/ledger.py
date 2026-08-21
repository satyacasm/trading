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
                return None  # unchanged: nothing to do
        elif status == JobStatus.RUNNING.value:
            return None  # another runner holds it
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
