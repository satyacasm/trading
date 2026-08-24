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

    seed_calendar(db_conn, "NSE", "CM", date(1998, 8, 10), date(1998, 8, 12), holidays=set())
    for d in (date(1998, 8, 10), date(1998, 8, 11), date(1998, 8, 12)):
        _seed_job(db_conn, source_key="recon_cal", business_date=d)

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_cal", "NSE", "CM", date(1998, 8, 10), date(1998, 8, 12))]
    )
    assert result.status == CheckStatus.PASS


def test_calendar_completeness_reports_gaps(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(1998, 8, 10), date(1998, 8, 12), holidays=set())
    _seed_job(db_conn, source_key="recon_cal_gap", business_date=date(1998, 8, 10))
    # 1998-08-11 and 1998-08-12 have no ingest_jobs row at all.

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_cal_gap", "NSE", "CM", date(1998, 8, 10), date(1998, 8, 12))]
    )
    assert result.status == CheckStatus.FAIL
    assert "1998-08-11" in result.detail
    assert "1998-08-12" in result.detail


def test_calendar_completeness_accepts_skipped_as_terminal(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(1998, 8, 10), date(1998, 8, 10), holidays=set())
    _seed_job(
        db_conn, source_key="recon_skip", business_date=date(1998, 8, 10), status="SKIPPED_NO_DATA"
    )

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_skip", "NSE", "CM", date(1998, 8, 10), date(1998, 8, 10))]
    )
    assert result.status == CheckStatus.PASS


def test_calendar_completeness_a_failed_job_is_a_gap(db_conn):
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(1998, 8, 10), date(1998, 8, 10), holidays=set())
    _seed_job(db_conn, source_key="recon_failed", business_date=date(1998, 8, 10), status="FAILED")

    result = check_calendar_completeness(
        db_conn, [SourceWindow("recon_failed", "NSE", "CM", date(1998, 8, 10), date(1998, 8, 10))]
    )
    assert result.status == CheckStatus.FAIL


def test_calendar_completeness_scopes_each_source_to_its_own_window(db_conn):
    """A source's window must not manufacture gaps outside its own era."""
    from trading.calendar.trading_days import seed_calendar

    seed_calendar(db_conn, "NSE", "CM", date(1998, 8, 10), date(1998, 8, 12), holidays=set())
    _seed_job(db_conn, source_key="recon_era_a", business_date=date(1998, 8, 10))
    _seed_job(db_conn, source_key="recon_era_b", business_date=date(1998, 8, 12))

    result = check_calendar_completeness(
        db_conn,
        [
            SourceWindow("recon_era_a", "NSE", "CM", date(1998, 8, 10), date(1998, 8, 10)),
            SourceWindow("recon_era_b", "NSE", "CM", date(1998, 8, 12), date(1998, 8, 12)),
        ],
    )
    assert result.status == CheckStatus.PASS


# ---------------------------------------------------------------------------
# check_known_values
# ---------------------------------------------------------------------------


def test_known_values_empty_is_not_applicable(db_conn):
    assert check_known_values(db_conn, []).status == CheckStatus.NOT_APPLICABLE


def test_known_values_pass_when_db_matches(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="KNOWNPASS", ts=ts, close="123.45")])

    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="KNOWNPASS",
        business_date=date(1998, 8, 13),
        expected=Decimal("123.45"),
        archive_path="test-fixture",
        source_note="synthetic test row",
    )
    result = check_known_values(db_conn, [kv])
    assert result.status == CheckStatus.PASS


def test_known_values_fails_on_mismatch(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(symbol="KNOWNFAIL", ts=ts, close="100.00")])

    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="KNOWNFAIL",
        business_date=date(1998, 8, 13),
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
        business_date=date(1998, 8, 13),
        expected=Decimal("1.00"),
        archive_path="test-fixture",
        source_note="synthetic test row",
    )
    result = check_known_values(db_conn, [kv])
    assert result.status == CheckStatus.FAIL
    assert "not found" in result.detail


def test_known_values_matches_a_derivative_by_expiry(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
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
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )

    kv = KnownValue(
        exchange="NSE",
        segment="FO",
        symbol="KNOWNFUT",
        business_date=date(1998, 8, 13),
        expected=Decimal("500.50"),
        archive_path="test-fixture",
        source_note="synthetic FO row",
        expiry=date(1998, 8, 27),
    )
    assert check_known_values(db_conn, [kv]).status == CheckStatus.PASS

    wrong_expiry = KnownValue(
        exchange="NSE",
        segment="FO",
        symbol="KNOWNFUT",
        business_date=date(1998, 8, 13),
        expected=Decimal("500.50"),
        archive_path="test-fixture",
        source_note="synthetic FO row",
        expiry=date(1998, 9, 24),
    )
    assert check_known_values(db_conn, [wrong_expiry]).status == CheckStatus.FAIL


def test_known_values_rejects_an_unsupported_field(db_conn):
    kv = KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="X",
        business_date=date(1998, 8, 13),
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
    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_cross_source_agreement_passes_within_tolerance(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
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
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XSRC")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("100.02"), fo_id, ts),
    )

    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS


def test_cross_source_agreement_fails_beyond_tolerance(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
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
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XSRCBAD")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("105.00"), fo_id, ts),
    )

    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.FAIL
    assert "XSRCBAD" in result.detail


# ---------------------------------------------------------------------------
# check_continuity
# ---------------------------------------------------------------------------


def test_continuity_passes_when_move_matches_a_corporate_action(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(
        db_conn,
        [
            _bar_row(
                symbol="CONTSPLIT", ts=datetime(1998, 8, 12, 10, 0, tzinfo=UTC), close="100.00"
            ),
            _bar_row(symbol="CONTSPLIT", ts=ts, close="50.00"),
        ],
    )
    iid = _instrument_id(db_conn, "NSE", "CM", "CONTSPLIT")
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, ratio_from, "
        "ratio_to, source) VALUES (%s,'SPLIT',%s,1,2,'test')",
        (iid, date(1998, 8, 13)),
    )

    result = check_continuity(db_conn, date(1998, 8, 12), date(1998, 8, 13))
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


def _fixture_zip_dated(tmp_path: Path, business_date: date) -> Path:
    """Rewrite the committed UDiFF fixture's trade date into the test epoch.

    The fixture carries a real `TradDt` of 2026-08-13, and `UdiffNormalizer`
    takes each bar's `ts` from that column rather than from
    `RawPayload.business_date`. Loading it verbatim would therefore write
    rows into a date range the real backfill also occupies, so these tests
    would start reading (and upserting over) production rows the moment
    `bars_daily` stopped being empty. Re-dating the archive keeps them
    hermetic while leaving every price byte-identical.
    """
    import zipfile

    src = zipfile.ZipFile(FIXTURE_ZIP)
    name = src.namelist()[0]
    text = src.read(name).decode()
    lines = text.splitlines()
    header = lines[0].split(",")
    trad, biz = header.index("TradDt"), header.index("BizDt")
    out_lines = [lines[0]]
    for line in lines[1:]:
        if not line.strip():
            continue
        cells = line.split(",")
        cells[trad] = cells[biz] = business_date.isoformat()
        out_lines.append(",".join(cells))

    dest = tmp_path / "nse_cm_udiff_redated.zip"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, "\n".join(out_lines) + "\n")
    return dest


def _load_fixture_archive(conn, registry, normalizer, validator, loader, archive: Path) -> None:
    import hashlib

    from trading.contracts import RawPayload

    content = archive.read_bytes()
    payload = RawPayload(
        source_key="nse_cm_udiff",
        business_date=date(1998, 8, 13),
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=archive,
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
        date(1998, 8, 1),
        date(1998, 8, 28),
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_idempotency_fails_when_archive_is_missing(db_conn):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff_missing",
        business_date=date(1998, 8, 13),
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
        date(1998, 8, 1),
        date(1998, 8, 28),
    )
    assert result.status == CheckStatus.FAIL
    assert "missing" in result.detail


def test_idempotency_passes_when_reloading_changes_nothing(db_conn, tmp_path):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    archive = _fixture_zip_dated(tmp_path, date(1998, 8, 13))
    # Prime bars_daily with exactly what the archive would produce. Must use
    # the real "nse_cm_udiff" source_key: UdiffNormalizer.SOURCE_BY_KEY only
    # recognises the real source keys, and check_idempotency re-normalizes
    # the archive under whatever source_key it is given.
    _load_fixture_archive(db_conn, registry, normalizer, validator, loader, archive)
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff",
        business_date=date(1998, 8, 13),
        archive_path=str(archive),
    )

    result = check_idempotency(
        db_conn,
        "nse_cm_udiff",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(1998, 8, 1),
        date(1998, 8, 28),
    )
    assert result.status == CheckStatus.PASS


def test_idempotency_fails_when_db_had_drifted_from_the_archive(db_conn, tmp_path):
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    archive = _fixture_zip_dated(tmp_path, date(1998, 8, 13))
    _load_fixture_archive(db_conn, registry, normalizer, validator, loader, archive)
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff",
        business_date=date(1998, 8, 13),
        archive_path=str(archive),
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
                ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC),
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
        date(1998, 8, 1),
        date(1998, 8, 28),
    )
    assert result.status == CheckStatus.FAIL
    assert "changed" in result.detail


def test_pick_random_window_none_without_success_rows(db_conn):
    assert pick_random_window(db_conn, "no_such_source") is None


def test_pick_random_window_stays_within_bounds(db_conn):
    import random

    for d in (date(1998, 1, 1), date(1998, 6, 30), date(1998, 12, 31)):
        _seed_job(db_conn, source_key="pickwin", business_date=d)

    window = pick_random_window(db_conn, "pickwin", days=30, rng=random.Random(42))
    assert window is not None
    start, end = window
    assert date(1998, 1, 1) <= start <= date(1998, 12, 31)
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
        db_conn, source_key="qr_ok", business_date=date(1998, 8, 13), rows_written=100_000
    )
    db_conn.execute(
        "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,'ohlc_missing','{}')",
        (job_id,),
    )
    result = check_quarantine_rate(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS


def test_quarantine_rate_fails_above_threshold(db_conn):
    job_id = _seed_job(
        db_conn, source_key="qr_bad", business_date=date(1998, 8, 13), rows_written=10
    )
    for _ in range(5):
        db_conn.execute(
            "INSERT INTO quarantine (job_id, reason, row_payload) VALUES (%s,'close_missing','{}')",
            (job_id,),
        )
    result = check_quarantine_rate(db_conn, date(1998, 8, 13), date(1998, 8, 13))
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
        [_bar_row(symbol="CAPTEST", ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC), close="1")],
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
    report = render_report(results, capacity, generated_at=datetime(1998, 8, 20, tzinfo=UTC))
    assert "1 passed, 1 failed, 1 not applicable, out of 3" in report
    assert "bad \\| pipe" in report
    assert "PASS" in report and "FAIL" in report and "NOT_APPLICABLE" in report


# ---------------------------------------------------------------------------
# Ruling B8: a check may never return PASS on input it never examined, and a
# PASS detail must carry the count of what was examined so the report is
# self-evidencing rather than merely green.
# ---------------------------------------------------------------------------


def test_continuity_on_an_empty_range_is_not_applicable(db_conn):
    result = check_continuity(db_conn, date(1900, 1, 1), date(1900, 1, 2))
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "0 bar" in result.detail


def test_calendar_completeness_with_no_expected_trading_days_is_not_applicable(db_conn):
    # ("NSE", "XX") is never seeded, so the expected set is empty and the
    # window is unverifiable -- not clean.
    window = SourceWindow("nse_cm_udiff", "NSE", "XX", date(1998, 8, 13), date(1998, 8, 13))
    result = check_calendar_completeness(db_conn, [window])
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "0 trading day" in result.detail


def test_continuity_pass_reports_how_many_pairs_it_examined(db_conn):
    _load(
        db_conn,
        [
            _bar_row(symbol="CONTCNT", ts=datetime(1998, 8, 12, 10, 0, tzinfo=UTC), close="100.00"),
            _bar_row(symbol="CONTCNT", ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC), close="101.00"),
        ],
    )
    result = check_continuity(db_conn, date(1998, 8, 12), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS
    assert "1 " in result.detail


# ---------------------------------------------------------------------------
# check_continuity: compare against our own previous stored bar, never the
# source's prev_close field (proved untrustworthy on NSE's BL/BE series --
# METROPOLIS BL carried prev_close=1944.00 against a 564.00 close, and
# BURNPUR BE carried prev_close=1.00 against a 21.35 close).
# ---------------------------------------------------------------------------


def test_continuity_ignores_a_garbage_source_prev_close(db_conn):
    _load(
        db_conn,
        [
            _bar_row(
                symbol="CONTBL",
                ts=datetime(1998, 8, 12, 10, 0, tzinfo=UTC),
                close="564.00",
                prev_close="1944.00",
            ),
            _bar_row(
                symbol="CONTBL",
                ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC),
                close="566.25",
                prev_close="1944.00",
            ),
        ],
    )
    result = check_continuity(db_conn, date(1998, 8, 12), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS


def test_continuity_fails_on_a_real_jump_between_consecutive_stored_bars(db_conn):
    _load(
        db_conn,
        [
            _bar_row(
                symbol="CONTJUMP", ts=datetime(1998, 8, 12, 10, 0, tzinfo=UTC), close="100.00"
            ),
            _bar_row(symbol="CONTJUMP", ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC), close="50.00"),
        ],
    )
    result = check_continuity(db_conn, date(1998, 8, 12), date(1998, 8, 13))
    assert result.status == CheckStatus.FAIL
    assert "CONTJUMP" in result.detail


def test_continuity_does_not_compare_across_a_stale_gap(db_conn):
    # A hole in the loaded history must not manufacture a violation: two bars
    # a month apart are not a single-day move.
    _load(
        db_conn,
        [
            _bar_row(symbol="CONTGAP", ts=datetime(1998, 7, 10, 10, 0, tzinfo=UTC), close="100.00"),
            _bar_row(symbol="CONTGAP", ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC), close="50.00"),
        ],
    )
    result = check_continuity(db_conn, date(1998, 7, 10), date(1998, 8, 13))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_continuity_single_bar_instrument_has_no_pair_to_examine(db_conn):
    _load(
        db_conn,
        [_bar_row(symbol="CONTONE", ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC), close="100.00")],
    )
    result = check_continuity(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.NOT_APPLICABLE


# ---------------------------------------------------------------------------
# check_cross_source_agreement: the tolerance is relative, because NSE's
# UndrlygPric is a snapshot whose absolute distance from the CM close scales
# with the price (7709.84 vs 7710.00 is the same defect-free rounding as
# 1179.38 vs 1180.00, but 8x the absolute gap).
# ---------------------------------------------------------------------------


def test_cross_source_agreement_tolerates_a_rounding_gap_on_a_high_priced_stock(db_conn):
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(exchange="NSE", segment="CM", symbol="XABB", ts=ts, close="7710.00")])
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="XABB",
                asset_class="FUTURE",
                ts=ts,
                close="7715.00",
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XABB")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("7709.84"), fo_id, ts),
    )

    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS


def test_idempotency_not_applicable_when_the_window_holds_no_bars(db_conn, tmp_path):
    # A SUCCESS job with a readable archive, but nothing in `bars_daily` to
    # compare: re-loading "changed nothing" is vacuously true over zero rows.
    registry, normalizer, validator, loader = _udiff_stage_bundle()
    archive = _fixture_zip_dated(tmp_path, date(1998, 8, 13))
    _seed_job(
        db_conn,
        source_key="nse_cm_udiff_norows",
        business_date=date(1998, 8, 13),
        archive_path=str(archive),
    )

    result = check_idempotency(
        db_conn,
        "nse_cm_udiff_norows",
        DataSource.NSE_CM_UDIFF,
        registry,
        normalizer,
        validator,
        loader,
        date(1998, 8, 1),
        date(1998, 8, 28),
    )
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "0 row" in result.detail


def test_cross_source_agreement_tolerates_the_close_versus_last_traded_price_gap(db_conn):
    # NSE's UndrlygPric is a last-traded price; the CM `close` is a weighted
    # average of the closing session. Across the first real days loaded, the
    # widest gap between them was 19.8 bp (PREMIERENE 1028.13 vs 1026.10) --
    # a definitional difference, not a defect. The default tolerance must sit
    # above that noise floor, since the mismatch this check exists to catch
    # (a wrong symbol mapping, a stale underlying) is off by whole percent.
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(db_conn, [_bar_row(exchange="NSE", segment="CM", symbol="XLTP", ts=ts, close="1026.10")])
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="XLTP",
                asset_class="FUTURE",
                ts=ts,
                close="1030.00",
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XLTP")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("1028.13"), fo_id, ts),
    )

    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS


def test_cross_source_agreement_still_catches_a_stale_underlying(db_conn):
    # A percent-scale error -- the shape of a genuinely wrong or stale
    # underlying -- must stay a failure at the same default tolerance.
    ts = datetime(1998, 8, 13, 10, 0, tzinfo=UTC)
    _load(
        db_conn, [_bar_row(exchange="NSE", segment="CM", symbol="XSTALE", ts=ts, close="1000.00")]
    )
    _load(
        db_conn,
        [
            _bar_row(
                exchange="NSE",
                segment="FO",
                symbol="XSTALE",
                asset_class="FUTURE",
                ts=ts,
                close="1005.00",
                expiry=date(1998, 8, 27),
            )
        ],
        data_source=DataSource.NSE_FO_UDIFF,
    )
    fo_id = _instrument_id(db_conn, "NSE", "FO", "XSTALE")
    db_conn.execute(
        "UPDATE bars_daily SET underlying_price=%s WHERE instrument_id=%s AND ts=%s",
        (Decimal("1010.00"), fo_id, ts),
    )

    result = check_cross_source_agreement(db_conn, date(1998, 8, 13), date(1998, 8, 13))
    assert result.status == CheckStatus.FAIL


@pytest.mark.parametrize("action_type", ["RIGHTS", "DEMERGER", "CAPITAL_REDUCTION"])
def test_continuity_accepts_every_share_count_changing_action(db_conn, action_type: str):
    """A rights issue dilutes, a demerger carves value out, a capital
    reduction cancels shares -- all three genuinely move the price on the
    ex-date, so all three explain a jump. Recognising only SPLIT/BONUS/
    DIVIDEND left 396 real events unable to explain the move they caused.
    """
    _load(
        db_conn,
        [
            _bar_row(
                symbol=f"CONT{action_type[:4]}",
                ts=datetime(1998, 8, 12, 10, 0, tzinfo=UTC),
                close="100.00",
            ),
            _bar_row(
                symbol=f"CONT{action_type[:4]}",
                ts=datetime(1998, 8, 13, 10, 0, tzinfo=UTC),
                close="50.00",
            ),
        ],
    )
    iid = _instrument_id(db_conn, "NSE", "CM", f"CONT{action_type[:4]}")
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, source) "
        "VALUES (%s,%s,%s,'test')",
        (iid, action_type, date(1998, 8, 13)),
    )

    result = check_continuity(db_conn, date(1998, 8, 12), date(1998, 8, 13))
    assert result.status == CheckStatus.PASS
