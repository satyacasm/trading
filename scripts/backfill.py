#!/usr/bin/env python3
"""CLI driver for the Phase-0 backfill (Task 17, task-17-addendum.md).

Wires each `--source` key to its real pipeline (source -> parser registry ->
normalizer -> resolver -> validator -> loader) and drives it with
`BackfillRunner.missing_days` for day selection -- the ingest_jobs ledger is
the ONLY resume mechanism (Ruling B2); this script adds no skip logic of its
own on top of it.

Usage:
    uv run python scripts/backfill.py --source nse_cm_udiff \
        --from 2026-08-13 --to 2026-08-13 [--delay 1.0] [--dry-run]

`--dry-run` prints the days `BackfillRunner.missing_days` would fetch and
exits without a single network call (Ruling B2). A real run sleeps `--delay`
seconds (default 1.0) between days, prints one progress line per day, and
ends with a summary count per terminal `JobStatus`. Exit code is non-zero if
any day ended FAILED, so a wrapper script can tell success from partial
success (Ruling B2).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date

import psycopg
import structlog

from trading.config import get_settings
from trading.contracts import DataSource, JobStatus
from trading.loaders.bars import BarLoader
from trading.normalizers.amfi import AmfiNormalizer
from trading.normalizers.nse_legacy import NseLegacyNormalizer
from trading.normalizers.udiff import UdiffNormalizer
from trading.parsers.amfi import AmfiNavParser
from trading.parsers.amfi_history import AmfiNavHistoryParser
from trading.parsers.nse_legacy import NseLegacyCmParser
from trading.parsers.registry import ParserRegistry
from trading.parsers.udiff import UdiffParser
from trading.pipeline.backfill import BackfillRunner
from trading.pipeline.runner import Pipeline
from trading.resolver.instruments import DbInstrumentResolver
from trading.sources.amfi import AmfiNavSource
from trading.sources.amfi_history import AmfiNavHistorySource
from trading.sources.bse_udiff import BseUdiffSource
from trading.sources.nse_legacy import NseLegacyCmSource
from trading.sources.nse_udiff import NseUdiffSource
from trading.validation.bars import BarValidator

log = structlog.get_logger(__name__)

DEFAULT_DELAY = 1.0


def _pipeline_nse_cm_udiff() -> tuple[Pipeline, DbInstrumentResolver]:
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=NseUdiffSource(segment="cm"),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_CM_UDIFF),
    )
    return pipeline, resolver


def _pipeline_nse_fo_udiff() -> tuple[Pipeline, DbInstrumentResolver]:
    # Ruling S3 (task-18-brief.md): `max_new_per_batch` stays at its 5,000
    # default here -- it is a real guard against a parser fault minting an
    # absurd number of instruments on any of the ~2,500 days this pipeline
    # will run over, not just the first. Verified live (task-17-report.md):
    # the very first F&O day ever loaded creates ~35,750 new instruments --
    # the entire live NSE F&O contract universe, all genuinely new on day
    # one -- which the default guard correctly refuses to bulk-create; the
    # `--bootstrap` CLI flag (main(), below) bypasses the guard for exactly
    # that first day via `resolver.bootstrap_next_call()`, not by permanently
    # raising the limit for every day after it.
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=NseUdiffSource(segment="fo"),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_FO_UDIFF),
    )
    return pipeline, resolver


def _pipeline_bse_cm_udiff() -> tuple[Pipeline, DbInstrumentResolver]:
    # Ruling S3: same reasoning as `_pipeline_nse_fo_udiff` above -- the
    # first BSE CM day created exactly 5,000 new instruments (right at the
    # default boundary; task-17-report.md), so it also needs `--bootstrap`
    # on its first invocation rather than a permanently raised cap.
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=BseUdiffSource(),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.BSE_CM_UDIFF),
    )
    return pipeline, resolver


def _pipeline_nse_cm_legacy() -> tuple[Pipeline, DbInstrumentResolver]:
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=NseLegacyCmSource(),
        registry=ParserRegistry([NseLegacyCmParser()]),
        normalizer=NseLegacyNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_CM_LEGACY),
    )
    return pipeline, resolver


def _pipeline_amfi_nav_history() -> tuple[Pipeline, DbInstrumentResolver]:
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=AmfiNavHistorySource(),
        registry=ParserRegistry([AmfiNavHistoryParser()]),
        normalizer=AmfiNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.AMFI_NAV),
    )
    return pipeline, resolver


def _pipeline_amfi_nav() -> tuple[Pipeline, DbInstrumentResolver]:
    resolver = DbInstrumentResolver()
    pipeline = Pipeline(
        source=AmfiNavSource(),
        registry=ParserRegistry([AmfiNavParser()]),
        normalizer=AmfiNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.AMFI_NAV),
    )
    return pipeline, resolver


@dataclass(frozen=True)
class SourceSpec:
    """A source key's pipeline factory plus the calendar pair it runs against.

    `BackfillRunner.missing_days` needs an (exchange, segment) pair to look up
    `trading_calendar` -- Ruling H3 (task-9-addendum.md) only seeded NSE/CM,
    NSE/FO and BSE/CM, so both AMFI source keys are pinned to NSE/CM here: MF
    NAVs are published on the same business-day calendar NSE trading follows
    closely enough for day-selection purposes. This is a documented
    assumption, not a verified AMFI-specific calendar (see task-17-report.md).
    """

    build: Callable[[], tuple[Pipeline, DbInstrumentResolver]]
    exchange: str
    segment: str


SOURCE_SPECS: dict[str, SourceSpec] = {
    "nse_cm_udiff": SourceSpec(_pipeline_nse_cm_udiff, "NSE", "CM"),
    "nse_fo_udiff": SourceSpec(_pipeline_nse_fo_udiff, "NSE", "FO"),
    "bse_cm_udiff": SourceSpec(_pipeline_bse_cm_udiff, "BSE", "CM"),
    "nse_cm_legacy": SourceSpec(_pipeline_nse_cm_legacy, "NSE", "CM"),
    "amfi_nav_history": SourceSpec(_pipeline_amfi_nav_history, "NSE", "CM"),
    "amfi_nav": SourceSpec(_pipeline_amfi_nav, "NSE", "CM"),
}


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive the Phase-0 backfill for one source.")
    parser.add_argument("--source", required=True, choices=sorted(SOURCE_SPECS))
    parser.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between day fetches (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the days that would be fetched and exit without any network call.",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help=(
            "Bypass the instrument-creation abort guard (DbInstrumentResolver."
            "max_new_per_batch) for this run's FIRST day only (Ruling S3, "
            "task-18-brief.md). A source's real first day can legitimately mint "
            "tens of thousands of new instruments (e.g. the entire live NSE F&O "
            "universe); every subsequent day keeps the default 5,000-instrument "
            "guard, which is exactly what should catch a parser fault. Pass this "
            "on a source's first-ever invocation only -- never on a resumed or "
            "re-run of a later range, where an absurd batch is a real bug."
        ),
    )
    parser.add_argument(
        "--max-new",
        type=int,
        default=None,
        help=(
            "Raise DbInstrumentResolver.max_new_per_batch for this run "
            "(default: the resolver's own 5,000). --bootstrap covers only the "
            "first day; a from-empty F&O backfill needs a higher cap on every "
            "day, because an expiry rollover legitimately lists a whole new "
            "strike ladder. Pass this for a historical backfill only -- for "
            "daily incremental runs the 5,000 default is a real guard."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    spec = SOURCE_SPECS[args.source]
    pipeline, resolver = spec.build()
    if args.max_new is not None:
        resolver.set_creation_cap(args.max_new)
    # `Pipeline.source_key` is a read-only @property; `_PipelineLike` (a
    # pipeline-local Protocol in trading.pipeline.backfill, out of this
    # task's scope to widen) declares it as a plain settable attribute, so
    # mypy sees a structural mismatch even though `Pipeline` satisfies this
    # Protocol at runtime -- it is the only concrete `_PipelineLike`
    # implementation in this codebase.
    runner = BackfillRunner(pipeline, spec.exchange, spec.segment)  # type: ignore[arg-type]

    conn = psycopg.connect(get_settings().database_url, autocommit=False)
    try:
        days = runner.missing_days(conn, args.start, args.end)

        if args.dry_run:
            print(
                f"DRY RUN: {len(days)} day(s) would be fetched for {args.source} "
                f"[{args.start.isoformat()} .. {args.end.isoformat()}]",
                flush=True,
            )
            for day in days:
                print(f"  {day.isoformat()}", flush=True)
            return 0

        print(
            f"backfill: {args.source} -- {len(days)} day(s) pending "
            f"[{args.start.isoformat()} .. {args.end.isoformat()}], delay={args.delay}s",
            flush=True,
        )

        # Ruling S3: arm the resolver's abort-guard bypass for exactly the
        # first day this process runs (not "day 1 of the source's full
        # history" -- a resumed run's first pending day may be day 300).
        # `bootstrap_next_call` disarms itself after the one `resolve()`
        # call inside that day's `loader.load()`, so every later day in
        # this same loop keeps the real guard.
        if args.bootstrap and days:
            resolver.bootstrap_next_call()

        counts: Counter[JobStatus] = Counter()
        for index, day in enumerate(days, start=1):
            status = pipeline.run(conn, day)
            conn.commit()
            counts[status] += 1
            print(
                f"[{index}/{len(days)}] {args.source} {day.isoformat()}: {status.value}", flush=True
            )
            if index < len(days) and args.delay > 0:
                time.sleep(args.delay)

        print("---", flush=True)
        for status in JobStatus:
            if counts[status]:
                print(f"{status.value}: {counts[status]}", flush=True)
        print(f"total: {sum(counts.values())}", flush=True)

        return 1 if counts[JobStatus.FAILED] else 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
