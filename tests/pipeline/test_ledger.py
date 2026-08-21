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
    complete_job(
        db_conn,
        job_id,
        JobStatus.FAILED,
        rows=0,
        quarantined=0,
        content_hash=None,
        archive_path=None,
        error="boom",
    )
    assert claim_job(db_conn, "nse_cm_udiff", D) is not None


def test_a_successful_job_with_the_same_hash_is_not_reclaimed(db_conn):
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    complete_job(
        db_conn,
        job_id,
        JobStatus.SUCCESS,
        rows=10,
        quarantined=0,
        content_hash="abc",
        archive_path="/x",
        error=None,
    )
    assert claim_job(db_conn, "nse_cm_udiff", D, content_hash="abc") is None


def test_a_restated_file_reclaims_the_job(db_conn):
    """NSE restates files; a changed hash must force a re-parse."""
    job_id = claim_job(db_conn, "nse_cm_udiff", D)
    complete_job(
        db_conn,
        job_id,
        JobStatus.SUCCESS,
        rows=10,
        quarantined=0,
        content_hash="abc",
        archive_path="/x",
        error=None,
    )
    assert claim_job(db_conn, "nse_cm_udiff", D, content_hash="different") is not None


def test_attempt_counter_increments(db_conn):
    claim_job(db_conn, "nse_cm_udiff", D)
    db_conn.execute("UPDATE ingest_jobs SET status='FAILED' WHERE business_date=%s", (D,))
    claim_job(db_conn, "nse_cm_udiff", D)
    attempt = db_conn.execute(
        "SELECT attempt FROM ingest_jobs WHERE business_date=%s", (D,)
    ).fetchone()[0]
    assert attempt == 2
