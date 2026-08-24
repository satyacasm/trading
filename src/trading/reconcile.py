"""Phase-0 reconciliation checks (spec §8, task-17-brief.md Step 5,
task-17-addendum.md Rulings B3-B7).

One function per verification criterion, each returning a `CheckResult`
carrying a tri-state `CheckStatus` (Ruling B4): `PASS`, `FAIL`, or
`NOT_APPLICABLE` for a check whose preconditions are absent. `NOT_APPLICABLE`
is never counted as a pass -- `render_report` reports it as its own bucket.

Every check takes a `psycopg.Connection` and plain data (date ranges, known
values, tolerances) rather than reaching for global state, so
`tests/test_reconcile.py` can drive all of them against small, seeded
fixtures inside a rolled-back transaction (task-17-addendum.md, Constraints)
without a populated database or any network access.

Run for real with:
    uv run python -m trading.reconcile --report docs/phase-0-completion-report.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import DataSource, Loader, Normalizer, RawPayload, Validator
from trading.loaders.bars import BarLoader
from trading.normalizers.amfi import AmfiNormalizer
from trading.normalizers.nse_legacy import NseLegacyNormalizer
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.amfi_history import AmfiNavHistoryParser
from trading.parsers.nse_legacy import NseLegacyCmParser
from trading.parsers.registry import ParserRegistry
from trading.parsers.udiff import UdiffParser
from trading.resolver.instruments import DbInstrumentResolver
from trading.validation.bars import BarValidator

IST = ZoneInfo("Asia/Kolkata")


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: CheckStatus
    detail: str


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    lower = datetime(d.year, d.month, d.day, tzinfo=UTC)
    return lower, lower + timedelta(days=1)


def _range_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    lower, _ = _day_bounds(start)
    _, upper = _day_bounds(end)
    return lower, upper


# ---------------------------------------------------------------------------
# 1. Calendar completeness
# ---------------------------------------------------------------------------

_TERMINAL_OK = ("SUCCESS", "SKIPPED_HOLIDAY", "SKIPPED_NO_DATA")


@dataclass(frozen=True)
class SourceWindow:
    """One source's expected date range, for `check_calendar_completeness`.

    Deliberately per-source rather than one blanket range: `nse_cm_udiff`
    only exists from 2024-07-01 and `nse_cm_legacy` only up to 2024-06-30
    (task-17-report.md's run plan), so checking every source against the
    full 2016-2026 calendar would manufacture gaps for the era each source
    was never meant to cover.
    """

    source_key: str
    exchange: str
    segment: str
    start: date
    end: date


def check_calendar_completeness(conn: Connection, windows: Sequence[SourceWindow]) -> CheckResult:
    if not windows:
        return CheckResult(
            "calendar_completeness", CheckStatus.NOT_APPLICABLE, "no source windows configured"
        )

    gaps: list[str] = []
    total_expected = 0
    for w in windows:
        expected = {
            row[0]
            for row in conn.execute(
                "SELECT session_date FROM trading_calendar "
                "WHERE exchange=%s AND segment=%s AND session_date BETWEEN %s AND %s "
                "AND is_trading_day",
                (w.exchange, w.segment, w.start, w.end),
            ).fetchall()
        }
        total_expected += len(expected)
        have = {
            row[0]
            for row in conn.execute(
                "SELECT business_date FROM ingest_jobs WHERE source_key=%s "
                "AND business_date BETWEEN %s AND %s AND status = ANY(%s)",
                (w.source_key, w.start, w.end, list(_TERMINAL_OK)),
            ).fetchall()
        }
        missing = sorted(expected - have)
        if missing:
            sample = ", ".join(d.isoformat() for d in missing[:10])
            more = f" (+{len(missing) - 10} more)" if len(missing) > 10 else ""
            gaps.append(f"{w.source_key}: {len(missing)} gap(s), e.g. {sample}{more}")

    if gaps:
        return CheckResult("calendar_completeness", CheckStatus.FAIL, "; ".join(gaps))
    if total_expected == 0:
        # Ruling B8: a window whose expected trading-day set is empty (an
        # unseeded exchange/segment, or a range entirely outside the seeded
        # calendar) has nothing to be complete *about*. Reporting PASS there
        # states "zero gaps" about a comparison that never ran.
        return CheckResult(
            "calendar_completeness",
            CheckStatus.NOT_APPLICABLE,
            f"0 trading day(s) expected across {len(windows)} source window(s) -- "
            "the calendar is unseeded for every (exchange, segment, range) given, "
            "so completeness is unverifiable",
        )
    return CheckResult(
        "calendar_completeness",
        CheckStatus.PASS,
        f"{total_expected} trading day(s) across {len(windows)} source window(s), zero gaps",
    )


# ---------------------------------------------------------------------------
# 2. Known-value spot checks (Ruling B3: sourced from the raw archive, not
#    the database or a model's recall)
# ---------------------------------------------------------------------------

_KNOWN_VALUE_FIELDS = frozenset(
    {"open", "high", "low", "close", "prev_close", "settle_price", "underlying_price"}
)


@dataclass(frozen=True)
class KnownValue:
    """One hand-verified instrument-day, read directly out of the archived
    file the exchange actually served (Ruling B3) -- never out of our own
    database and never recalled from training data. `archive_path` and
    `source_note` exist so a human can re-open that exact file and check the
    number by hand.
    """

    exchange: str
    segment: str
    symbol: str
    business_date: date
    expected: Decimal
    archive_path: str
    source_note: str
    field: str = "close"
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None


def check_known_values(conn: Connection, known_values: Sequence[KnownValue]) -> CheckResult:
    if not known_values:
        return CheckResult(
            "known_values", CheckStatus.NOT_APPLICABLE, "no known-value rows configured"
        )

    mismatches: list[str] = []
    for kv in known_values:
        if kv.field not in _KNOWN_VALUE_FIELDS:
            raise ValueError(f"unsupported known-value field {kv.field!r}")
        lower, upper = _day_bounds(kv.business_date)
        row = conn.execute(
            f"SELECT b.{kv.field} FROM bars_daily b "  # noqa: S608 - kv.field is whitelist-checked above
            "JOIN instruments i ON i.instrument_id = b.instrument_id "
            "WHERE i.exchange=%s AND i.segment=%s AND i.symbol=%s "
            "AND i.expiry IS NOT DISTINCT FROM %s AND i.strike IS NOT DISTINCT FROM %s "
            "AND i.option_type IS NOT DISTINCT FROM %s AND b.ts >= %s AND b.ts < %s",
            (
                kv.exchange,
                kv.segment,
                kv.symbol,
                kv.expiry,
                kv.strike,
                kv.option_type,
                lower,
                upper,
            ),
        ).fetchone()
        label = f"{kv.exchange}/{kv.segment}/{kv.symbol} {kv.business_date} ({kv.field})"
        if row is None:
            mismatches.append(
                f"{label}: not found in database (expected {kv.expected}, source {kv.archive_path})"
            )
            continue
        actual = row[0]
        if actual is None or Decimal(actual) != kv.expected:
            mismatches.append(
                f"{label}: db={actual} expected={kv.expected} (source {kv.archive_path})"
            )

    if mismatches:
        return CheckResult("known_values", CheckStatus.FAIL, "; ".join(mismatches))
    return CheckResult(
        "known_values", CheckStatus.PASS, f"{len(known_values)} hand-verified value(s) matched"
    )


# Ruling B3: at least ten instrument-days spanning both eras (legacy and
# UDiFF) and all three of NSE CM, NSE FO and BSE CM. Every `expected` value
# below was read by hand out of the archived file at `archive_path` (all
# under `data/raw/`, gitignored but present in the environment this task ran
# in -- see task-17-report.md for how to re-fetch them) -- never copied from
# `bars_daily` and never recalled from training data.
KNOWN_VALUES: tuple[KnownValue, ...] = (
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2016, 10, 30),
        expected=Decimal("1051.2"),
        archive_path="data/raw/nse_cm_legacy/2016/10/2016-10-30.zip",
        source_note="CLOSE column, legacy NSE CM bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2017, 10, 19),
        expected=Decimal("909.9"),
        archive_path="data/raw/nse_cm_legacy/2017/10/2017-10-19.zip",
        source_note="CLOSE column, legacy NSE CM bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2018, 11, 7),
        expected=Decimal("1110.7"),
        archive_path="data/raw/nse_cm_legacy/2018/11/2018-11-07.zip",
        source_note="CLOSE column, legacy NSE CM bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2019, 10, 27),
        expected=Decimal("1434.25"),
        archive_path="data/raw/nse_cm_legacy/2019/10/2019-10-27.zip",
        source_note="CLOSE column, legacy NSE CM bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2020, 2, 1),
        expected=Decimal("1383.35"),
        archive_path="data/raw/nse_cm_legacy/2020/02/2020-02-01.zip",
        source_note="CLOSE column, legacy NSE CM bhavcopy CSV inside the zip "
        "(2020-02-01 was a Saturday special Budget-day session)",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2024, 1, 20),
        expected=Decimal("2713.30"),
        archive_path="data/raw/nse_cm_udiff/2024/01/2024-01-20.zip",
        source_note="ClsPric column, NSE CM UDiFF bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2024, 11, 1),
        expected=Decimal("1338.65"),
        archive_path="data/raw/nse_cm_udiff/2024/11/2024-11-01.zip",
        source_note="ClsPric column, NSE CM UDiFF bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2025, 2, 1),
        expected=Decimal("1264.60"),
        archive_path="data/raw/nse_cm_udiff/2025/02/2025-02-01.zip",
        source_note="ClsPric column, NSE CM UDiFF bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2025, 10, 21),
        expected=Decimal("1465.20"),
        archive_path="data/raw/nse_cm_udiff/2025/10/2025-10-21.zip",
        source_note="ClsPric column, NSE CM UDiFF bhavcopy CSV inside the zip",
    ),
    KnownValue(
        exchange="NSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2026, 8, 13),
        expected=Decimal("1317.00"),
        archive_path="data/raw/_recon/nse_cm_udiff_20260813.csv",
        source_note="ClsPric column, NSE CM UDiFF recon sample",
    ),
    KnownValue(
        exchange="NSE",
        segment="FO",
        symbol="RELIANCE",
        business_date=date(2026, 8, 13),
        expected=Decimal("1316.10"),
        archive_path="data/raw/_recon/nse_fo_udiff_20260813.csv",
        source_note="ClsPric column, NSE FO UDiFF recon sample, RELIANCE stock "
        "future expiring 2026-08-25",
        expiry=date(2026, 8, 25),
    ),
    KnownValue(
        exchange="BSE",
        segment="CM",
        symbol="RELIANCE",
        business_date=date(2026, 8, 13),
        expected=Decimal("1316.45"),
        archive_path="data/raw/_recon/bse_cm_udiff_20260813.csv",
        source_note="ClsPric column, BSE CM UDiFF recon sample",
    ),
)


# ---------------------------------------------------------------------------
# 3. Cross-source agreement (Ruling B5: substitutes FO underlying_price vs.
#    NSE CM close for the impossible NIFTY-spot comparison -- we ingest no
#    index rows at all, see task-17-addendum.md)
# ---------------------------------------------------------------------------


def check_cross_source_agreement(
    conn: Connection, start: date, end: date, tolerance: Decimal = Decimal("0.005")
) -> CheckResult:
    """Compare each NSE stock future's stored `underlying_price` against the
    same symbol's NSE CM close on the same session.

    `tolerance` is RELATIVE (a fraction), not an absolute rupee amount. NSE's
    `UndrlygPric` is a snapshot rather than the CM segment close, so the two
    legitimately differ by a rounding-scale amount that grows with the price:
    the first real run of this check flagged ABB at 7709.84 vs 7710.00 (0.16
    rupees, 0.2 bp) beside 360ONE at 1179.38 vs 1180.00 (0.62 rupees, 5.3 bp)
    as if they were different severities. They are the same defect-free
    rounding; an absolute threshold cannot express that, so 564 of 1,866 rows
    "disagreed" while nothing was actually wrong. A genuinely mismatched
    underlying -- the failure this check exists to catch -- is off by percent,
    not by basis points.

    The 50 bp default sits above the widest close-versus-last-traded-price gap
    seen across the first real days loaded (19.8 bp, PREMIERENE 1028.13 vs
    1026.10) and one to two orders of magnitude below a real mismatch. It is
    calibrated on days, not years, so it should be re-derived from the full
    distribution once the backfill completes -- tightened if the tail stays
    this narrow.
    """
    # Task 17 found `bars_daily` had no `underlying_price` column at all, so
    # this check could only report NOT_APPLICABLE (see task-17-report.md
    # finding F3). Task 18, Ruling S2 fixed the gap: a migration
    # (migrations/versions/0002_instrument_series_and_underlying_price.py)
    # adds the column and `BarLoader` (src/trading/loaders/bars.py) now
    # writes it, so the comparison below runs against real data.
    lower, upper = _range_bounds(start, end)
    rows = conn.execute(
        """
        SELECT fo.symbol, bfo.ts, bfo.underlying_price, bcm.close
        FROM bars_daily bfo
        JOIN instruments fo ON fo.instrument_id = bfo.instrument_id
        JOIN instruments cm ON cm.exchange = 'NSE' AND cm.segment = 'CM'
            AND cm.symbol = fo.symbol AND cm.asset_class = 'EQUITY'
        JOIN bars_daily bcm ON bcm.instrument_id = cm.instrument_id AND bcm.ts = bfo.ts
        WHERE fo.exchange = 'NSE' AND fo.segment = 'FO' AND fo.asset_class = 'FUTURE'
          AND bfo.underlying_price IS NOT NULL
          AND bfo.ts >= %s AND bfo.ts < %s
        """,
        (lower, upper),
    ).fetchall()

    if not rows:
        return CheckResult(
            "cross_source_agreement",
            CheckStatus.NOT_APPLICABLE,
            f"no NSE stock-future row with a matching NSE CM close in [{start}, {end}]",
        )

    violations = [
        (symbol, ts, u, c)
        for symbol, ts, u, c in rows
        if Decimal(c) > 0 and abs(Decimal(u) - Decimal(c)) / Decimal(c) > tolerance
    ]
    if violations:
        sample = "; ".join(
            f"{s} {t.astimezone(IST).date()} underlying={u} cm_close={c}"
            for s, t, u, c in violations[:10]
        )
        more = f" (+{len(violations) - 10} more)" if len(violations) > 10 else ""
        return CheckResult(
            "cross_source_agreement",
            CheckStatus.FAIL,
            f"{len(violations)}/{len(rows)} stock-future row(s) disagree with the NSE CM close "
            f"by more than {tolerance:.2%} (substituted per Ruling B5 for the unavailable "
            f"NIFTY-spot comparison -- see task-17-report.md for the empirical finding that "
            f"NSE's own UndrlygPric does not always equal the CM close): {sample}{more}",
        )
    return CheckResult(
        "cross_source_agreement",
        CheckStatus.PASS,
        f"{len(rows)} stock-future/underlying pair(s) agree within {tolerance:.2%}",
    )


# ---------------------------------------------------------------------------
# 4. Continuity
# ---------------------------------------------------------------------------

# Every action that changes the share count or carves value out of it, and
# so genuinely moves the price on its ex-date. RIGHTS, DEMERGER and
# CAPITAL_REDUCTION were added once the decade of NSE corporate actions was
# actually loaded: 396 real events could not explain the move they had caused
# because this tuple did not name them. `adjust.py` still applies only SPLIT
# and BONUS -- the others have no ratio in NSE's feed -- so naming them here
# explains a jump without repricing anything.
_CORP_ACTION_TYPES = (
    "SPLIT",
    "BONUS",
    "DIVIDEND",
    "RIGHTS",
    "DEMERGER",
    "CAPITAL_REDUCTION",
)


_CONTINUITY_PAIRS = """
WITH ordered AS (
    SELECT b.instrument_id,
           b.ts,
           b.close,
           LAG(b.close)  OVER (PARTITION BY b.instrument_id ORDER BY b.ts) AS prev_close,
           LAG(b.ts)     OVER (PARTITION BY b.instrument_id ORDER BY b.ts) AS prev_ts,
           LEAD(b.close) OVER (PARTITION BY b.instrument_id ORDER BY b.ts) AS next_close
    FROM bars_daily b
    JOIN instruments i ON i.instrument_id = b.instrument_id
    WHERE i.asset_class = 'EQUITY' AND b.ts >= %s AND b.ts < %s
),
pairs AS (
    SELECT * FROM ordered
    WHERE prev_close IS NOT NULL AND prev_close > 0 AND ts - prev_ts <= %s
)
"""


def check_continuity(
    conn: Connection,
    start: date,
    end: date,
    threshold: Decimal = Decimal("0.20"),
    max_gap_days: int = 7,
    tick_size: Decimal = Decimal("0.05"),
    revert_tolerance: Decimal = Decimal("0.10"),
) -> CheckResult:
    """Classify every large single-session equity move by its SHAPE.

    Scoped to EQUITY only. Verified live (task-17-report.md): run unscoped
    against one real NSE FO day, ~8,000 option rows tripped this threshold --
    options are leveraged instruments for which a >20% single-day move is
    routine, not a data defect.

    The move is computed from our own previous stored bar, never from the
    source's `prev_close`. That column is untrustworthy on NSE's non-EQ
    series: METROPOLIS's block-deal row carried `prev_close` 1944.00 against a
    564.00 close (-71%), BURNPUR's BE row carried 1.00 against 21.35 (+2035%),
    and between them they were most of this check's first ten "failures". A
    self-computed lag is also source-agnostic, so it keeps working for BSE and
    MCX. `max_gap_days` guards the other direction: a hole in the loaded
    history would otherwise make two bars a month apart look like one
    catastrophic session.

    A bare "moved more than `threshold`" rule is not enough, though, and the
    full decade proved it. With every NSE corporate action loaded, 5,030 moves
    still had no explanation -- and the largest of them was BIRLACOT going
    from 5 paise to 10 paise. So each jump is classified instead:

    - `tick`  -- the whole move is one tick or less. At 5 paise a single tick
                 IS 100%, so no percentage threshold can call this a defect
                 without condemning every penny stock. Counted, never failed.
    - `spike` -- the price returns to roughly where it started next session.
                 A bad print looks like this; a corporate action never does,
                 because a split does not un-split. Always a failure, and
                 deliberately NOT excusable by a corporate action on the same
                 date, or a real defect could hide behind a coincidental
                 ex-date.
    - `step`  -- the price stays at its new level. This is the shape of the
                 split-adjustment bug spec §8 item 4 is really about, so it
                 fails unless a corporate action explains it.

    Every pair lands in exactly one bucket and every bucket is counted in the
    result, so nothing is quietly dropped on the way to a green check.
    """
    lower, upper = _range_bounds(start, end)
    max_gap = timedelta(days=max_gap_days)

    examined_row = conn.execute(
        _CONTINUITY_PAIRS + "SELECT count(*) FROM pairs", (lower, upper, max_gap)
    ).fetchone()
    examined = int(examined_row[0]) if examined_row else 0

    if examined == 0:
        # Ruling B8: PASS must mean "I looked at N pairs and found no
        # violation", with N visible. Over zero pairs there is nothing to
        # find, and a green row here is what someone points at when deciding
        # the backfill can be trusted.
        return CheckResult(
            "continuity",
            CheckStatus.NOT_APPLICABLE,
            f"0 bar pair(s) at most {max_gap_days} day(s) apart to compare in [{start}, {end}]",
        )

    rows = conn.execute(
        _CONTINUITY_PAIRS
        + "SELECT p.instrument_id, i.exchange, i.segment, i.symbol, p.ts, p.close, p.prev_close,"
        " p.next_close FROM pairs p JOIN instruments i ON i.instrument_id = p.instrument_id "
        "WHERE abs(p.close - p.prev_close) / p.prev_close > %s",
        (lower, upper, max_gap, threshold),
    ).fetchall()

    ticks = explained = 0
    failures: list[str] = []
    spikes = steps = 0

    for instrument_id, exchange, segment, symbol, ts, close, prev_close, next_close in rows:
        move = abs(close - prev_close)
        if move <= tick_size:
            ticks += 1
            continue

        row_date = ts.astimezone(IST).date()
        pct = (close - prev_close) / prev_close
        reverted = (
            next_close is not None and abs(next_close - prev_close) / prev_close <= revert_tolerance
        )

        if reverted:
            spikes += 1
            failures.append(
                f"{exchange}/{segment}/{symbol} {row_date}: {pct:+.1%} then back (spike)"
            )
            continue

        matched = conn.execute(
            "SELECT 1 FROM corporate_actions WHERE instrument_id=%s "
            "AND action_type = ANY(%s) AND ex_date = %s LIMIT 1",
            (instrument_id, list(_CORP_ACTION_TYPES), row_date),
        ).fetchone()
        if matched is not None:
            explained += 1
            continue
        steps += 1
        failures.append(f"{exchange}/{segment}/{symbol} {row_date}: {pct:+.1%} (step)")

    census = (
        f"{examined} pair(s) examined; {len(rows)} beyond {threshold:.0%} "
        f"({ticks} tick, {explained} explained, {steps} step, {spikes} spike)"
    )

    if failures:
        sample = "; ".join(failures[:10])
        more = f" (+{len(failures) - 10} more)" if len(failures) > 10 else ""
        return CheckResult("continuity", CheckStatus.FAIL, f"{census}: {sample}{more}")
    return CheckResult("continuity", CheckStatus.PASS, census)


# ---------------------------------------------------------------------------
# 5. Idempotency
# ---------------------------------------------------------------------------

_IDEMPOTENCY_COLUMNS = (
    "instrument_id",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "volume",
    "turnover",
    "trades",
    "settle_price",
    "open_interest",
    "oi_change",
    "delivery_qty",
    "delivery_pct",
)


def _bars_checksum(
    conn: Connection, data_source: DataSource, start: date, end: date
) -> tuple[str, int]:
    lower, upper = _range_bounds(start, end)
    rows = conn.execute(
        f"SELECT {','.join(_IDEMPOTENCY_COLUMNS)} FROM bars_daily "
        "WHERE source=%s AND ts >= %s AND ts < %s ORDER BY instrument_id, ts",
        (int(data_source), lower, upper),
    ).fetchall()
    digest = hashlib.sha256("|".join(str(v) for row in rows for v in row).encode()).hexdigest()
    return digest, len(rows)


def check_idempotency(
    conn: Connection,
    source_key: str,
    data_source: DataSource,
    registry: ParserRegistry,
    normalizer: Normalizer,
    validator: Validator,
    loader: Loader,
    window_start: date,
    window_end: date,
) -> CheckResult:
    """Re-load every already-SUCCESS day in the window from its own archived
    bytes on disk (no network call -- Pipeline.run's own claim_job would
    short-circuit a re-fetch of an already-SUCCESS day anyway, since it never
    passes a content_hash) and assert `bars_daily` is unchanged.

    Never commits (task-17-addendum.md Constraints: reconciliation checks
    must be testable inside a rolled-back transaction) -- durability is the
    caller's decision, exactly like `Pipeline.run` (Ruling P1x).
    """
    jobs = conn.execute(
        "SELECT business_date, archive_path FROM ingest_jobs "
        "WHERE source_key=%s AND status='SUCCESS' AND archive_path IS NOT NULL "
        "AND business_date BETWEEN %s AND %s ORDER BY business_date",
        (source_key, window_start, window_end),
    ).fetchall()
    if not jobs:
        return CheckResult(
            "idempotency",
            CheckStatus.NOT_APPLICABLE,
            f"no completed {source_key} job with an archive in [{window_start}, {window_end}]",
        )

    # Archive existence is settled first, and outranks the empty-window guard
    # below: a SUCCESS job whose raw bytes have vanished can never be
    # re-verified by anyone, which is a failure whether or not the window
    # currently holds rows.
    missing_archives = [
        f"{business_date}: archive missing at {archive_path}"
        for business_date, archive_path in jobs
        if not Path(archive_path).exists()
    ]
    if missing_archives:
        return CheckResult(
            "idempotency",
            CheckStatus.FAIL,
            f"{len(missing_archives)} archive file(s) missing, cannot verify: "
            f"{'; '.join(missing_archives[:10])}",
        )

    # TimescaleDB compresses older chunks automatically (the Columnstore
    # policy), and then caps one transaction at 100,000 decompressed tuples.
    # This check deliberately re-loads a whole 30-day window -- ~700,000 rows
    # for F&O -- so it starts failing with ConfigurationLimitExceeded as soon
    # as compression catches up with the backfill, which says nothing about
    # whether the data is idempotent. SET LOCAL keeps the lift scoped to this
    # transaction, which the caller rolls back.
    conn.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")

    before_digest, before_count = _bars_checksum(conn, data_source, window_start, window_end)
    if before_count == 0:
        # Ruling B8: with no rows in the window, "re-loading changed nothing"
        # is vacuously true -- both digests are the SHA-256 of an empty
        # string. That is a green row proving nothing.
        return CheckResult(
            "idempotency",
            CheckStatus.NOT_APPLICABLE,
            f"0 row(s) in bars_daily for {source_key} in [{window_start}, {window_end}] -- "
            f"{len(jobs)} SUCCESS job(s) claim this window, so a re-load has nothing to "
            "compare against",
        )

    for business_date, archive_path in jobs:
        path = Path(archive_path)
        content = path.read_bytes()
        payload = RawPayload(
            source_key=source_key,
            business_date=business_date,
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            fetched_at=datetime.now(UTC),
            archive_path=path,
        )
        parser = registry.select(payload)
        batch = normalizer.normalize(parser.parse(payload), payload)
        outcome = validator.validate(batch)
        loader.load(outcome, conn)

    after_digest, after_count = _bars_checksum(conn, data_source, window_start, window_end)

    if before_digest != after_digest or before_count != after_count:
        return CheckResult(
            "idempotency",
            CheckStatus.FAIL,
            f"re-loading {len(jobs)} day(s) in [{window_start}, {window_end}] changed bars_daily: "
            f"before={before_count} row(s) ({before_digest[:12]}...), "
            f"after={after_count} row(s) ({after_digest[:12]}...)",
        )
    return CheckResult(
        "idempotency",
        CheckStatus.PASS,
        f"re-loading {len(jobs)} day(s) ({before_count} row(s)) in [{window_start}, {window_end}] "
        "changed nothing",
    )


def pick_random_window(
    conn: Connection, source_key: str, days: int = 30, rng: random.Random | None = None
) -> tuple[date, date] | None:
    """Pick a random `days`-day window from `source_key`'s completed date
    range, for `check_idempotency` (spec §8 item 5: "a randomly selected
    30-day window"). Returns None if the source has no SUCCESS rows at all.
    """
    _rng = rng if rng is not None else random.Random()
    row = conn.execute(
        "SELECT min(business_date), max(business_date) FROM ingest_jobs "
        "WHERE source_key=%s AND status='SUCCESS'",
        (source_key,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    lo, hi = row
    span = (hi - lo).days
    if span <= days:
        return lo, hi
    offset = _rng.randint(0, span - days)
    start = lo + timedelta(days=offset)
    return start, start + timedelta(days=days - 1)


# ---------------------------------------------------------------------------
# 6. Quarantine rate
# ---------------------------------------------------------------------------


def check_quarantine_rate(
    conn: Connection, start: date, end: date, max_rate: Decimal = Decimal("0.0001")
) -> CheckResult:
    row = conn.execute(
        "SELECT COALESCE(SUM(rows_written), 0) FROM ingest_jobs "
        "WHERE status = 'SUCCESS' AND business_date BETWEEN %s AND %s",
        (start, end),
    ).fetchone()
    total_written = int(row[0]) if row is not None else 0

    reasons = conn.execute(
        "SELECT q.reason, count(*) FROM quarantine q JOIN ingest_jobs j ON j.job_id = q.job_id "
        "WHERE j.business_date BETWEEN %s AND %s GROUP BY q.reason ORDER BY count(*) DESC",
        (start, end),
    ).fetchall()
    total_quarantined = sum(count for _, count in reasons)

    total = total_written + total_quarantined
    if total == 0:
        return CheckResult(
            "quarantine_rate", CheckStatus.NOT_APPLICABLE, f"no rows ingested in [{start}, {end}]"
        )

    rate = Decimal(total_quarantined) / Decimal(total)
    breakdown = ", ".join(f"{reason}={count}" for reason, count in reasons) or "none"
    status = CheckStatus.PASS if rate < max_rate else CheckStatus.FAIL
    return CheckResult(
        "quarantine_rate",
        status,
        f"{total_quarantined}/{total} row(s) quarantined ({rate:.4%}, threshold {max_rate:.4%}); "
        f"reasons: {breakdown}",
    )


# ---------------------------------------------------------------------------
# 7. Recorder liveness (Ruling B4: cannot pass or fail today; must say so
#    loudly rather than silently report success)
# ---------------------------------------------------------------------------


def check_recorder_liveness(recordings_root: Path, since: date) -> CheckResult:
    manifests = sorted(recordings_root.rglob("session.json")) if recordings_root.exists() else []
    if not manifests:
        return CheckResult(
            "recorder_liveness",
            CheckStatus.NOT_APPLICABLE,
            f"no recorder session manifests found under {recordings_root}: the Upstox WebSocket "
            "recorder (Task 15) has never run in this environment because Upstox credentials do "
            "not exist yet (Ruling B4, task-17-addendum.md). This check cannot pass or fail until "
            "the recorder has run at least one session; it must never be counted as a pass.",
        )

    bad_days: list[str] = []
    evaluated = 0
    for manifest_path in manifests:
        data = json.loads(manifest_path.read_text())
        session_date = date.fromisoformat(data["session_date"])
        if session_date < since:
            continue
        evaluated += 1
        started = datetime.fromisoformat(data["started_at"])
        ended = (
            datetime.fromisoformat(data["ended_at"]) if data.get("ended_at") else datetime.now(UTC)
        )
        session_len = (ended - started).total_seconds()

        gap_seconds = 0.0
        for gap in data.get("gaps", []):
            g_start = datetime.fromisoformat(gap["started_at"])
            g_end = datetime.fromisoformat(gap["ended_at"]) if gap.get("ended_at") else ended
            gap_seconds += (g_end - g_start).total_seconds()
        gap_ratio = gap_seconds / session_len if session_len > 0 else 1.0

        subs = data.get("subscriptions", {})
        subs_match = sorted(subs.get("requested", [])) == sorted(subs.get("acknowledged", []))

        if gap_ratio >= 0.01 or not subs_match:
            bad_days.append(
                f"{session_date}: gap_ratio={gap_ratio:.2%}, subscriptions_match={subs_match}"
            )

    if evaluated == 0:
        return CheckResult(
            "recorder_liveness",
            CheckStatus.NOT_APPLICABLE,
            f"{len(manifests)} session manifest(s) exist under {recordings_root}, but none on or "
            f"after {since}",
        )

    if bad_days:
        sample = "; ".join(bad_days[:10])
        more = f" (+{len(bad_days) - 10} more)" if len(bad_days) > 10 else ""
        return CheckResult(
            "recorder_liveness",
            CheckStatus.FAIL,
            f"{len(bad_days)} session(s) failed liveness: {sample}{more}",
        )
    return CheckResult(
        "recorder_liveness",
        CheckStatus.PASS,
        f"{evaluated} session manifest(s) since {since}, "
        "all under 1% gap with matching subscriptions",
    )


# ---------------------------------------------------------------------------
# Capacity snapshot (Ruling B7: not a pass/fail check, but numbers the next
# phase should size a host from rather than guess)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CapacitySnapshot:
    disk_free_bytes: int
    disk_total_bytes: int
    raw_archive_bytes: int
    bars_daily_rows: int
    instruments_rows: int
    ingest_jobs_rows: int


def capacity_snapshot(conn: Connection, data_root: Path) -> CapacitySnapshot:
    usage_root = data_root if data_root.exists() else Path(".")
    usage = shutil.disk_usage(usage_root)
    raw_root = data_root / "raw"
    raw_bytes = (
        sum(f.stat().st_size for f in raw_root.rglob("*") if f.is_file())
        if raw_root.exists()
        else 0
    )

    def _count(table: str) -> int:
        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608 - fixed table names
        return int(row[0]) if row is not None else 0

    return CapacitySnapshot(
        disk_free_bytes=usage.free,
        disk_total_bytes=usage.total,
        raw_archive_bytes=raw_bytes,
        bars_daily_rows=_count("bars_daily"),
        instruments_rows=_count("instruments"),
        ingest_jobs_rows=_count("ingest_jobs"),
    )


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def render_report(
    results: Sequence[CheckResult], capacity: CapacitySnapshot, *, generated_at: datetime
) -> str:
    passed = sum(1 for r in results if r.status == CheckStatus.PASS)
    failed = [r for r in results if r.status == CheckStatus.FAIL]
    not_applicable = [r for r in results if r.status == CheckStatus.NOT_APPLICABLE]

    lines = [
        "# Phase 0 completion report",
        "",
        f"Generated: {generated_at.isoformat()}",
        "",
        "## Verification checks (spec §8)",
        "",
        "| # | Check | Status | Detail |",
        "|---|-------|--------|--------|",
    ]
    for i, r in enumerate(results, start=1):
        detail = r.detail.replace("|", "\\|")
        lines.append(f"| {i} | {r.name} | {r.status.value} | {detail} |")

    lines += [
        "",
        f"**{passed} passed, {len(failed)} failed, {len(not_applicable)} not applicable, "
        f"out of {len(results)}.**",
        "",
        "A FAIL must be resolved as a bug fix or a documented, justified exception before Phase 0 "
        "is considered complete (task-17-brief.md Step 6). A NOT_APPLICABLE is never counted as a "
        "pass (task-17-addendum.md Ruling B4).",
        "",
        "## Capacity (Ruling B7)",
        "",
        f"- Disk free: {capacity.disk_free_bytes / 1e9:.1f} GB of "
        f"{capacity.disk_total_bytes / 1e9:.1f} GB total",
        f"- `data/raw` size: {capacity.raw_archive_bytes / 1e6:.1f} MB",
        f"- `bars_daily` rows: {capacity.bars_daily_rows:,}",
        f"- `instruments` rows: {capacity.instruments_rows:,}",
        f"- `ingest_jobs` rows: {capacity.ingest_jobs_rows:,}",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _default_windows(today: date) -> list[SourceWindow]:
    """Matches the run plan in task-17-report.md exactly -- keep both in sync."""
    return [
        SourceWindow("nse_cm_udiff", "NSE", "CM", date(2024, 7, 1), today),
        SourceWindow("nse_fo_udiff", "NSE", "FO", date(2024, 7, 1), today),
        SourceWindow("bse_cm_udiff", "BSE", "CM", date(2024, 7, 1), today),
        SourceWindow("nse_cm_legacy", "NSE", "CM", date(2016, 1, 1), date(2024, 6, 30)),
        SourceWindow("amfi_nav_history", "NSE", "CM", date(2016, 1, 1), today),
    ]


@dataclass(frozen=True)
class _IdempotencyTarget:
    source_key: str
    data_source: DataSource
    registry: ParserRegistry
    normalizer: Normalizer
    validator: Validator
    loader: Loader


def _idempotency_targets() -> list[_IdempotencyTarget]:
    resolver = DbInstrumentResolver()
    validator = BarValidator()
    udiff_registry = ParserRegistry([UdiffParser()])
    udiff_normalizer = UdiffNormalizer()
    return [
        _IdempotencyTarget(
            "nse_cm_udiff",
            DataSource.NSE_CM_UDIFF,
            udiff_registry,
            udiff_normalizer,
            validator,
            BarLoader(resolver, DataSource.NSE_CM_UDIFF),
        ),
        _IdempotencyTarget(
            "nse_fo_udiff",
            DataSource.NSE_FO_UDIFF,
            udiff_registry,
            udiff_normalizer,
            validator,
            BarLoader(resolver, DataSource.NSE_FO_UDIFF),
        ),
        _IdempotencyTarget(
            "bse_cm_udiff",
            DataSource.BSE_CM_UDIFF,
            udiff_registry,
            udiff_normalizer,
            validator,
            BarLoader(resolver, DataSource.BSE_CM_UDIFF),
        ),
        _IdempotencyTarget(
            "nse_cm_legacy",
            DataSource.NSE_CM_LEGACY,
            ParserRegistry([NseLegacyCmParser()]),
            NseLegacyNormalizer(),
            validator,
            BarLoader(resolver, DataSource.NSE_CM_LEGACY),
        ),
        _IdempotencyTarget(
            "amfi_nav_history",
            DataSource.AMFI_NAV,
            ParserRegistry([AmfiNavHistoryParser()]),
            AmfiNormalizer(),
            validator,
            BarLoader(resolver, DataSource.AMFI_NAV),
        ),
    ]


def run_all_checks(conn: Connection, *, today: date, recordings_root: Path) -> list[CheckResult]:
    results = [
        check_calendar_completeness(conn, _default_windows(today)),
        check_known_values(conn, KNOWN_VALUES),
        check_cross_source_agreement(conn, date(2016, 1, 1), today),
        check_continuity(conn, date(2016, 1, 1), today),
    ]

    for target in _idempotency_targets():
        window = pick_random_window(conn, target.source_key)
        if window is None:
            results.append(
                CheckResult(
                    f"idempotency[{target.source_key}]",
                    CheckStatus.NOT_APPLICABLE,
                    f"no completed {target.source_key} job to sample a window from",
                )
            )
            continue
        window_start, window_end = window
        result = check_idempotency(
            conn,
            target.source_key,
            target.data_source,
            target.registry,
            target.normalizer,
            target.validator,
            target.loader,
            window_start,
            window_end,
        )
        results.append(
            CheckResult(f"idempotency[{target.source_key}]", result.status, result.detail)
        )

    results.append(check_quarantine_rate(conn, date(2016, 1, 1), today))
    results.append(check_recorder_liveness(recordings_root, since=date(2016, 1, 1)))
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Phase-0 reconciliation checks.")
    parser.add_argument("--report", type=Path, default=Path("docs/phase-0-completion-report.md"))
    args = parser.parse_args(argv)

    settings = get_settings()
    today = datetime.now(IST).date()

    conn = psycopg.connect(settings.database_url, autocommit=False)
    try:
        results = run_all_checks(conn, today=today, recordings_root=settings.recordings_root)
        capacity = capacity_snapshot(conn, settings.data_root)
        # check_idempotency re-loads real rows via the real loader (that is
        # the point -- it proves the production upsert path is a no-op on
        # already-loaded data). Committing here is the intended durability
        # boundary for a real run; tests never reach this function.
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    report = render_report(results, capacity, generated_at=datetime.now(UTC))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report)
    print(report)

    return 1 if any(r.status == CheckStatus.FAIL for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
