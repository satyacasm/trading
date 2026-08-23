"""Tests for `trading.reconcile` (Task 17, task-17-addendum.md Constraints).

Every check is driven against small, seeded fixtures inside `db_conn`'s
rolled-back transaction -- no populated database, no network. Rows are
seeded through the real write path (`BarLoader`, like
`tests/loaders/test_bars.py` and `tests/corpactions/conftest.py`'s
`seeded_instrument`) rather than hand-rolled SQL, wherever that path can
reach the table under test.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from trading.contracts import CANONICAL_BAR_SCHEMA, DataSource, ValidationOutcome
from trading.loaders.bars import BarLoader
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.registry import ParserRegistry
from trading.parsers.udiff import UdiffParser
from trading.reconcile import (
    CapacitySnapshot,
    CheckResult,
    CheckStatus,
    KnownValue,
    SourceWindow,
    capacity_snapshot,
    check_calendar_completeness,
    check_continuity,
    check_cross_source_agreement,
    check_idempotency,
    check_known_values,
    check_quarantine_rate,
    check_recorder_liveness,
    pick_random_window,
    render_report,
)
from trading.resolver.instruments import DbInstrumentResolver
from trading.validation.bars import BarValidator

pytestmark = pytest.mark.db

FIXTURE_ZIP = Path(__file__).parent / "fixtures" / "udiff" / "nse_cm_udiff.zip"


def _bar_row(
    *,
    exchange: str = "NSE",
    segment: str = "CM",
    symbol: str = "RECONTEST",
    series: str | None = None,
    asset_class: str = "EQUITY",
    ts: datetime,
    close: str,
    prev_close: str | None = None,
    underlying_price: str | None = None,
    expiry: date | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {c: None for c in CANONICAL_BAR_SCHEMA}
    close_dec = Decimal(close)
    row.update(
        exchange=exchange,
        segment=segment,
        symbol=symbol,
        series=series,
        asset_class=asset_class,
        expiry=expiry,
        ts=ts,
        open=close_dec,
        high=close_dec,
        low=close_dec,
        close=close_dec,
        prev_close=Decimal(prev_close) if prev_close is not None else None,
        underlying_price=Decimal(underlying_price) if underlying_price is not None else None,
        volume=1000,
        lot_size=1,
    )
    return row


def _load(
    conn, rows: list[dict[str, object]], data_source: DataSource = DataSource.NSE_CM_UDIFF
) -> None:
    frame = pl.DataFrame(rows, schema=CANONICAL_BAR_SCHEMA)
    BarLoader(DbInstrumentResolver(), data_source).load(ValidationOutcome(valid=frame), conn)


def _instrument_id(conn, exchange: str, segment: str, symbol: str) -> int:
    row = conn.execute(
        "SELECT instrument_id FROM instruments WHERE exchange=%s AND segment=%s AND symbol=%s",
        (exchange, segment, symbol),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_job(
    conn,
    *,
    source_key: str,
    business_date: date,
    status: str = "SUCCESS",
    archive_path: str | None = None,
    rows_written: int = 0,
) -> int:
    row = conn.execute(
        "INSERT INTO ingest_jobs (source_key, business_date, status, attempt, rows_written, "
        "archive_path, started_at, finished_at) "
        "VALUES (%s,%s,%s,1,%s,%s,now(),now()) RETURNING job_id",
        (source_key, business_date, status, rows_written, archive_path),
    ).fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# check_calendar_completeness
# ---------------------------------------------------------------------------


def test_calendar_completeness_no_windows_is_not_applicable(db_conn):
    result = check_calendar_completeness(db_conn, [])
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_calendar_completeness_passes_with_no_gaps(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 12), holidays=set())
    for d in (date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)):
        _seed_job(db_conn, source_key="recon_cal", business_date=d)

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_cal", "NSE", "CM", date(2026, 8, 10), date(2026, 8, 12))]
    )
    assert result.status == CheckStatus.PASS


def test_calendar_completeness_reports_gaps(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 12), holidays=set())
    _seed_job(db_conn, source_key="recon_cal_gap", business_date=date(2026, 8, 10))
    # 2026-08-11 and 2026-08-12 have no ingest_jobs row at all.

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_cal_gap", "NSE", "CM", date(2026, 8, 10), date(2026, 8, 12))]
    )
    assert result.status == CheckStatus.FAIL
    assert "2026-08-11" in result.detail
    assert "2026-08-12" in result.detail


def test_calendar_completeness_accepts_skipped_as_terminal(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 10), holidays=set())
    _seed_job(
        db_conn, source_key="recon_skip", business_date=date(2026, 8, 10), status="SKIPPED_NO_DATA"
    )

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_skip", "NSE", "CM", date(2026, 8, 10), date(2026, 8, 10))]
    )
    assert result.status == CheckStatus.PASS


def test_calendar_completeness_a_failed_job_is_a_gap(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 10), holidays=set())
    _seed_job(db_conn, source_key="recon_failed", business_date=date(2026, 8, 10), status="FAILED")

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_failed", "NSE", "CM", date(2026, 8, 10), date(2026, 8, 10))]
    )
    assert result.status == CheckStatus.FAIL


def test_calendar_completeness_scopes_each_source_to_its_own_window(db_conn):
    """A source's window must not manufacture gaps outside its own era."""
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(2026, 8, 10), date(2026, 8, 12), holidays=set())
    _seed_job(db_conn, source_key="recon_era_a", business_date=date(2026, 8, 10))
    _seed_job(db_conn, source_key="recon_era_b", business_date=date(2026, 8, 12))

    result = check_calendar_completeness(
        db_conn,
        [
            SourceWindow("recon_era_a", "NSE", "CM", date(2026, 8, 10), date(2026, 8, 10)),
            SourceWindow("recon_era_b", "NSE", "CM", date(2026, 8, 12), date(2026, 8, 12)),
        ],
    )
    assert result.status == CheckStatus.PASS


# ---------------------------------------------------------------------------
# check_known_values
# ---------------------------------------------------------------------------


def test_known_values_empty_is_not_applicable(db_conn):
    assert check_known_values(db_conn, []).status == CheckStatus.NOT_APPLICABLE


def test_known_values_pass_when_db_matches(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="KNOWNPASS", ts=ts, close="123.45")])

    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="KNOWNPASS",
        business_date=date(2026, 8, 13),
        expected=Decimal("123.45"),
        archive_path="test-fixture",
        source_note="synthetic test row",
    )
    result = check_known_values(db_conn, [kv])
    assert result.status == CheckStatus.PASS


def test_known_values_fails_on_mismatch(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="KNOWNFAIL", ts=ts, close="100.00")])

    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="KNOWNFAIL",
        business_date=date(2026, 8, 13),
        expected=Decimal("999.99"),
        archive_path="test-fixture",
        source_note="synthetic test row",
    )
    result = check_known_values(db_conn, [kv])
    assert result.status == CheckStatus.FAIL
    assert "999.99" in result.detail


def test_known_values_fails_when_not_found(db_conn):
    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="DOESNOTEXIST",
        business_date=date(2026, 8, 13),
        expected=Decimal("1.00"),
        archive_path="test-fixture",
        source_note="synthetic test row",
    )
    result = check_known_values(db_conn, [kv])
    assert result.status == CheckStatus.FAIL
    assert "not found" in result.detail


def test_known_values_matches_a_derivative_by_expiry(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="KNOWNFUT",
                asset_class="FUTURE",
                ts=ts,
                close="500.50",
                expiry=date(2026, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )

    kv = KnownValue(
        exchange="NSE",
        segment="FO",
        symbol="KNOWNFUT",
        business_date=date(2026, 8, 13),
        expected=Decimal("500.50"),
        archive_path="test-fixture",
        source_note="synthetic FO row",
        expiry=date(2026, 8, 27),
    )
    assert check_known_values(db_conn, [kv]).status == CheckStatus.PASS

    wrong_expiry = KnownValue(
        exchange="NSE",
        segment="FO",
        symbol="KNOWNFUT",
        business_date=date(2026, 8, 13),
        expected=Decimal("500.50"),
        archive_path="test-fixture",
        source_note="synthetic FO row",
        expiry=date(2026, 9, 24),
    )
    assert check_known_values(db_conn, [wrong_expiry]).status == CheckStatus.FAIL


def test_known_values_rejects_an_unsupported_field(db_conn):
    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="X",
        business_date=date(2026, 8, 13),
        expected=Decimal("1"),
        archive_path="x",
        source_note="x",
        field="instrument_id",  # not a bars_daily price/activity field
    )
    with pytest.raises(ValueError, match="unsupported known-value field"):
        check_known_values(db_conn, [kv])


# ---------------------------------------------------------------------------
# check_cross_source_agreement
# ---------------------------------------------------------------------------


def test_cross_source_agreement_not_applicable_with_no_data(db_conn):
    result = check_cross_source_agreement(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_cross_source_agreement_passes_within_tolerance(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(exchange="NSE", segment="CM", symbol="XSRC", ts=ts, close="100.00")])
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="XSRC",
                asset_class="FUTURE",
                ts=ts,
                close="101.00",
                expiry=date(2026, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XSRC")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("100.02"), fo_id, ts),
    )

    result = check_cross_source_agreement(
        db_conn, date(2026, 8, 13), date(2026, 8, 13), tolerance=Decimal("0.05")
    )
    assert result.status == CheckStatus.PASS


def test_cross_source_agreement_fails_beyond_tolerance(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(
        db_conn, [_bar_row(exchange="NSE", segment="CM", symbol="XSRCBAD", ts=ts, close="100.00")]
    )
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="XSRCBAD",
                asset_class="FUTURE",
                ts=ts,
                close="101.00",
                expiry=date(2026, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XSRCBAD")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("105.00"), fo_id, ts),
    )

    result = check_cross_source_agreement(
        db_conn, date(2026, 8, 13), date(2026, 8, 13), tolerance=Decimal("0.05")
    )
    assert result.status == CheckStatus.FAIL
    assert "XSRCBAD" in result.detail


# ---------------------------------------------------------------------------
# check_continuity
# ---------------------------------------------------------------------------


def test_continuity_passes_with_no_large_moves(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="CONTOK", ts=ts, close="101.00", prev_close="100.00")])

    result = check_continuity(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.PASS


def test_continuity_fails_on_unexplained_large_move(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="CONTBAD", ts=ts, close="50.00", prev_close="100.00")])

    result = check_continuity(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.FAIL
    assert "CONTBAD" in result.detail


def test_continuity_passes_when_move_matches_a_corporate_action(db_conn):
    ts = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="CONTSPLIT", ts=ts, close="50.00", prev_close="100.00")])
    iid = _instrument_id(db_conn, "NSE", "CM", "CONTSPLIT")
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, ratio_from, "
        "ratio_to, source) VALUES (%s,'SPLIT',%s,1,2,'test')",
        (iid, date(2026, 8, 13)),
    )

    result = check_continuity(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.PASS
    assert "matched" in result.detail


# ---------------------------------------------------------------------------
# check_idempotency
# ---------------------------------------------------------------------------


def _udiff_stage_bundle() -> tuple[ParserRegistry, UdiffNormalizer, BarValidator, BarLoader]:
    resolver = DbInstrumentResolver()
    return (
        ParserRegistry([UdiffParser()]),
        UdiffNormalizer(),
        BarValidator(),
        BarLoader(resolver, DataSource.NSE_CM_UDIFF),
    )


def _load_fixture_archive(conn, registry, normalizer, validator, loader) -> None:
    import hashlib

    from trading.contracts import RawPayload

    content = FIXTURE_ZIP.read_bytes()
    payload = RawPayload(
        source_key="nse_cm_udiff",
        business_date=date(2026, 8, 13),
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=FIXTURE_ZIP,
    )
    parser = registry.select(payload)
    batch = normalizer.normalize(parser.parse(payload), payload)
    outcome = validator.validate(batch)
    loader.load(outcome, conn)


def test_idempotency_not_applicable_with_no_completed_jobs(db_conn):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    result = check_idempotency(
        db_conn,
        "nse_cm_udiff_noop",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(2026, 8, 1),
        date(2026, 8, 28),
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_idempotency_fails_when_archive_is_missing(db_conn):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff_missing",
        business_date=date(2026, 8, 13),
        archive_path="/nonexistent/archive.zip",
    )
    result = check_idempotency(
        db_conn,
        "nse_cm_udiff_missing",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(2026, 8, 1),
        date(2026, 8, 28),
    )
    assert result.status == CheckStatus.FAIL
    assert "missing" in result.detail


def test_idempotency_passes_when_reloading_changes_nothing(db_conn):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    # Prime bars_daily with exactly what the archive would produce. Must use
    # the real "nse_cm_udiff" source_key: UdiffNormalizer.SOURCE_BY_KEY only
    # recognises the real source keys, and check_idempotency re-normalizes
    # the archive under whatever source_key it is given.
    _load_fixture_archive(db_conn, registry, normalizer, validator, loader)
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff",
        business_date=date(2026, 8, 13),
        archive_path=str(FIXTURE_ZIP),
    )

    result = check_idempotency(
        db_conn,
        "nse_cm_udiff",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(2026, 8, 1),
        date(2026, 8, 28),
    )
    assert result.status == CheckStatus.PASS


def test_idempotency_fails_when_db_had_drifted_from_the_archive(db_conn):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    _load_fixture_archive(db_conn, registry, normalizer, validator, loader)
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff",
        business_date=date(2026, 8, 13),
        archive_path=str(FIXTURE_ZIP),
    )
    # Simulate drift: overwrite one instrument's row with a different close
    # via the real loader (so ck_ohlc_order/ck_close_positive stay satisfied
    # -- open=high=low=close all equal, exactly like `_bar_row`), so it no
    # longer matches what re-reading the archive will produce.
    #
    # series="GB" is required (Ruling S1, task-18-brief.md): the archive-
    # driven load above creates SGBJUN28 with series="GB" (its real SctySrs,
    # a gold bond), so this InstrumentRef must carry the same series to
    # collide onto that same instrument_id/ts row via ON CONFLICT rather
    # than resolving to a different (phantom) instrument and inserting an
    # unrelated extra row that leaves the real one un-drifted.
    _load(
        db_conn,
        [
            _bar_row(
                symbol="SGBJUN28",
                series="GB",
                ts=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
                close="1.00",
            )
        ],
    )

    result = check_idempotency(
        db_conn,
        "nse_cm_udiff",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(2026, 8, 1),
        date(2026, 8, 28),
    )
    assert result.status == CheckStatus.FAIL
    assert "changed" in result.detail


def test_pick_random_window_none_without_success_rows(db_conn):
    assert pick_random_window(db_conn, "no_such_source") is None


def test_pick_random_window_stays_within_bounds(db_conn):
    import random

    for d in (date(2026, 1, 1), date(2026, 6, 30), date(2026, 12, 31)):
        _seed_job(db_conn, source_key="pickwin", business_date=d)

    window = pick_random_window(db_conn, "pickwin", days=30, rng=random.Random(42))
    assert window is not None
    start, end = window
    assert date(2026, 1, 1) <= start <= date(2026, 12, 31)
    assert (end - start).days == 29


# ---------------------------------------------------------------------------
# check_quarantine_rate
# ---------------------------------------------------------------------------


def test_quarantine_rate_not_applicable_with_no_rows(db_conn):
    # A date range no other test and no live verification run in this task
    # touches -- unlike the tightly-scoped-by-symbol tests elsewhere in this
    # file, this check aggregates unscoped by source_key, so a wide range
    # here could otherwise pick up real committed rows from
    # `scripts/backfill.py`'s single-real-day proof (2026-08-20).
    result = check_quarantine_rate(db_conn, date(2030, 1, 1), date(2030, 1, 5))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_quarantine_rate_passes_below_threshold(db_conn):
    job_id = _seed_job(
        db_conn, source_key="qr_ok", business_date=date(2026, 8, 13), rows_written=100_000
    )
    db_conn.execute(
        "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,'ohlc_missing','{}')",
        (job_id,),
    )
    result = check_quarantine_rate(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.PASS


def test_quarantine_rate_fails_above_threshold(db_conn):
    job_id = _seed_job(
        db_conn, source_key="qr_bad", business_date=date(2026, 8, 13), rows_written=10
    )
    for _ in range(5):
        db_conn.execute(
            "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,'close_missing','{}')",
            (job_id,),
        )
    result = check_quarantine_rate(db_conn, date(2026, 8, 13), date(2026, 8, 13))
    assert result.status == CheckStatus.FAIL
    assert "close_missing" in result.detail


# ---------------------------------------------------------------------------
# check_recorder_liveness
# ---------------------------------------------------------------------------


def test_recorder_liveness_not_applicable_without_manifests(tmp_path):
    result = check_recorder_liveness(tmp_path, since=date(2016, 1, 1))
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "credentials" in result.detail


def _write_manifest(
    root: Path, session_date: date, *, gap_seconds: float, subs_match: bool
) -> None:
    session_dir = root / "upstox_v3" / session_date.isoformat()
    session_dir.mkdir(parents=True)
    started = datetime(session_date.year, session_date.month, session_date.day, 9, 15, tzinfo=UTC)
    ended = started.replace(hour=15, minute=30)
    gaps = []
    if gap_seconds:
        gap_end = started + timedelta(seconds=gap_seconds)
        gaps = [
            {
                "started_at": started.isoformat(),
                "ended_at": gap_end.isoformat(),
                "reason": "test",
            }
        ]
    manifest = {
        "source_key": "upstox_v3",
        "session_date": session_date.isoformat(),
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "frame_count": 100,
        "subscriptions": {
            "requested": ["NSE:RELIANCE"],
            "acknowledged": ["NSE:RELIANCE"] if subs_match else [],
        },
        "gaps": gaps,
        "connects": [started.isoformat()],
        "anomalies": [],
    }
    (session_dir / "session.json").write_text(json.dumps(manifest))


def test_recorder_liveness_passes_with_a_clean_session(tmp_path):
    _write_manifest(tmp_path, date(2026, 8, 13), gap_seconds=0, subs_match=True)
    result = check_recorder_liveness(tmp_path, since=date(2016, 1, 1))
    assert result.status == CheckStatus.PASS


def test_recorder_liveness_fails_when_subscriptions_do_not_match(tmp_path):
    _write_manifest(tmp_path, date(2026, 8, 13), gap_seconds=0, subs_match=False)
    result = check_recorder_liveness(tmp_path, since=date(2016, 1, 1))
    assert result.status == CheckStatus.FAIL


def test_recorder_liveness_fails_when_gap_ratio_too_high(tmp_path):
    # Session is 09:15-15:30 IST = 22,500 seconds; a 1,000s gap is well over 1%.
    _write_manifest(tmp_path, date(2026, 8, 13), gap_seconds=1000, subs_match=True)
    result = check_recorder_liveness(tmp_path, since=date(2016, 1, 1))
    assert result.status == CheckStatus.FAIL
    assert "gap_ratio" in result.detail


def test_recorder_liveness_not_applicable_when_no_session_is_in_scope(tmp_path):
    _write_manifest(tmp_path, date(2020, 1, 1), gap_seconds=1000, subs_match=True)
    result = check_recorder_liveness(tmp_path, since=date(2026, 1, 1))
    assert result.status == CheckStatus.NOT_APPLICABLE


# ---------------------------------------------------------------------------
# capacity_snapshot / render_report
# ---------------------------------------------------------------------------


def test_capacity_snapshot_counts_rows(db_conn, tmp_path):
    _load(
        db_conn,
        [_bar_row(symbol="CAPTEST", ts=datetime(2026, 8, 13, 10, 0, tzinfo=UTC), close="1")],
    )
    snapshot = capacity_snapshot(db_conn, tmp_path)
    assert snapshot.bars_daily_rows >= 1
    assert snapshot.instruments_rows >= 1


def test_render_report_counts_each_status_bucket():
    results = [
        CheckResult("a", CheckStatus.PASS, "ok"),
        CheckResult("b", CheckStatus.FAIL, "bad | pipe"),
        CheckResult("c", CheckStatus.NOT_APPLICABLE, "n/a"),
    ]
    capacity = CapacitySnapshot(
        disk_free_bytes=1_000_000_000,
        disk_total_bytes=2_000_000_000,
        raw_archive_bytes=12_000_000,
        bars_daily_rows=0,
        instruments_rows=0,
        ingest_jobs_rows=0,
    )
    report = render_report(results, capacity, generated_at=datetime(2026, 8, 20, tzinfo=UTC))
    assert "1 passed, 1 failed, 1 not applicable, out of 3" in report
    assert "bad \\| pipe" in report
    assert "PASS" in report and "FAIL" in report and "NOT_APPLICABLE" in report
