from datetime import date

import pytest

from trading.contracts import JobStatus
from trading.pipeline.ledger import claim_job

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


def test_a_stale_running_job_is_taken_over_and_ingests(db_conn, udiff_pipeline):
    """Ruling P4x: a RUNNING row left behind by a crashed process must not
    be permanently reported as SUCCESS -- once it's stale, the next run
    takes it over and actually ingests the day."""
    job_id = claim_job(db_conn, "nse_cm_udiff", date(2026, 8, 13))
    db_conn.execute(
        "UPDATE ingest_jobs SET started_at = now() - interval '2 hours' WHERE job_id=%s",
        (job_id,),
    )

    assert udiff_pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.SUCCESS

    status, attempt, rows_written = db_conn.execute(
        "SELECT status, attempt, rows_written FROM ingest_jobs WHERE job_id=%s", (job_id,)
    ).fetchone()
    assert status == "SUCCESS"
    assert attempt == 2
    assert rows_written == 50
    assert db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0] == 50


def test_a_fresh_running_job_returns_running_and_touches_nothing(db_conn, udiff_pipeline):
    """A live claim (not yet stale) must be reported as RUNNING, not SUCCESS
    -- and this process, which does not own that job, must not write to its
    ledger row or to bars_daily at all."""
    job_id = claim_job(db_conn, "nse_cm_udiff", date(2026, 8, 13))
    before = db_conn.execute(
        "SELECT status, attempt, rows_written, finished_at FROM ingest_jobs WHERE job_id=%s",
        (job_id,),
    ).fetchone()

    assert udiff_pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.RUNNING

    after = db_conn.execute(
        "SELECT status, attempt, rows_written, finished_at FROM ingest_jobs WHERE job_id=%s",
        (job_id,),
    ).fetchone()
    assert after == before
    assert db_conn.execute("SELECT count(*) FROM bars_daily").fetchone()[0] == 0


def test_a_genuinely_complete_day_returns_success_with_no_refetch(
    db_conn, udiff_pipeline_with_source
):
    """The one meaning `claim_job`'s `None` retains after Ruling P4x: an
    already-SUCCESS, unchanged-hash day is reported SUCCESS without ever
    calling the source again."""
    pipeline, source = udiff_pipeline_with_source

    assert pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.SUCCESS
    assert source.fetch_calls == 1

    assert pipeline.run(db_conn, date(2026, 8, 13)) is JobStatus.SUCCESS
    assert source.fetch_calls == 1
