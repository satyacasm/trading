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
