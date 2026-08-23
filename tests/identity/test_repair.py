"""Tests for `trading.identity` (the name/ISIN repair pass).

The warehouse was largely built before BarLoader recorded name and isin, and
the two long-running backfill legs held the pre-fix code in memory for their
whole run. Re-reading the archived bytes is the only way to describe those
instruments without re-downloading a decade of files.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from trading.contracts import DataSource, InstrumentRef
from trading.identity import repair_identity
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.registry import ParserRegistry
from trading.parsers.udiff import UdiffParser
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db

FIXTURE_ZIP = Path(__file__).parent.parent / "fixtures" / "udiff" / "nse_cm_udiff.zip"


def _seed_job(conn, *, source_key: str, business_date: date, archive: Path | str) -> None:
    conn.execute(
        "INSERT INTO ingest_jobs (source_key, business_date, status, attempt, rows_written,"
        " archive_path, started_at, finished_at) VALUES (%s,%s,'SUCCESS',1,0,%s,now(),now())",
        (source_key, business_date, str(archive)),
    )


def _bundle() -> tuple[ParserRegistry, UdiffNormalizer, DbInstrumentResolver]:
    return ParserRegistry([UdiffParser()]), UdiffNormalizer(), DbInstrumentResolver()


def _undescribed_sgb() -> InstrumentRef:
    """A sovereign gold bond the fixture archive also carries, created the way
    the live backfill created every instrument: key only, no name, no ISIN."""
    return InstrumentRef(exchange="NSE", segment="CM", symbol="SGBJUN28", series="GB")


def test_repair_describes_instruments_created_without_a_name(db_conn):
    registry, normalizer, resolver = _bundle()
    # An instrument that exists but was created before names were recorded --
    # exactly the state the live warehouse is in.
    resolver.resolve({_undescribed_sgb()}, db_conn)
    before = db_conn.execute(
        "SELECT name, isin FROM instruments WHERE symbol='SGBJUN28'"
    ).fetchone()
    assert before == (None, None)

    _seed_job(
        db_conn, source_key="nse_cm_udiff", business_date=date(2026, 8, 13), archive=FIXTURE_ZIP
    )
    result = repair_identity(
        db_conn, "nse_cm_udiff", DataSource.NSE_CM_UDIFF, registry, normalizer, resolver
    )

    assert result.archives_read == 1
    assert result.rows_updated > 0
    after = db_conn.execute("SELECT name, isin FROM instruments WHERE symbol='SGBJUN28'").fetchone()
    assert after is not None
    assert after[0] is not None and after[1] is not None


def test_repair_is_idempotent(db_conn):
    registry, normalizer, resolver = _bundle()
    # The repair only describes instruments that already exist -- it never
    # creates them -- so there must be one to describe.
    resolver.resolve({_undescribed_sgb()}, db_conn)
    _seed_job(
        db_conn, source_key="nse_cm_udiff", business_date=date(2026, 8, 13), archive=FIXTURE_ZIP
    )
    first = repair_identity(
        db_conn, "nse_cm_udiff", DataSource.NSE_CM_UDIFF, registry, normalizer, resolver
    )
    second = repair_identity(
        db_conn, "nse_cm_udiff", DataSource.NSE_CM_UDIFF, registry, normalizer, resolver
    )

    assert first.rows_updated > 0
    assert second.rows_updated == 0


def test_repair_reports_a_missing_archive_instead_of_dying(db_conn):
    registry, normalizer, resolver = _bundle()
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff",
        business_date=date(2026, 8, 13),
        archive="/nonexistent/archive.zip",
    )
    result = repair_identity(
        db_conn, "nse_cm_udiff", DataSource.NSE_CM_UDIFF, registry, normalizer, resolver
    )

    assert result.archives_read == 0
    assert result.archives_missing == 1
    assert result.rows_updated == 0


def test_repair_with_no_jobs_reports_nothing_read(db_conn):
    registry, normalizer, resolver = _bundle()
    result = repair_identity(
        db_conn, "nse_cm_udiff_absent", DataSource.NSE_CM_UDIFF, registry, normalizer, resolver
    )
    assert result.archives_read == 0
    assert result.rows_updated == 0
