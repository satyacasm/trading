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


def _pipeline_nse_cm_udiff() -> Pipeline:
    resolver = DbInstrumentResolver()
    return Pipeline(
        source=NseUdiffSource(segment="cm"),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_CM_UDIFF),
    )


def _pipeline_nse_fo_udiff() -> Pipeline:
    # `DbInstrumentResolver`'s default `max_new_per_batch=5000` guards against
    # a parser fault minting an absurd number of instruments (ruling I-series,
    # src/trading/resolver/instruments.py). Verified live (task-17-report.md):
    # the very first F&O day ever loaded creates ~35,750 new instruments --
    # the entire live NSE F&O contract universe, all genuinely new on day one
    # -- which trips that guard on every single day (the savepoint rolls back
    # on failure, so zero progress is ever retained and every subsequent day
    # hits the identical wall). A higher cap here is scoped to this one
    # pipeline; CM/legacy/AMFI keep the tighter default.
    resolver = DbInstrumentResolver(max_new_per_batch=50_000)
    return Pipeline(
        source=NseUdiffSource(segment="fo"),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_FO_UDIFF),
    )


def _pipeline_bse_cm_udiff() -> Pipeline:
    # Verified live (task-17-report.md): the first BSE CM day created exactly
    # 5,000 new instruments -- the default max_new_per_batch cap, hit right
    # at its boundary. A slightly larger universe on a different first day
    # would trip the same guard the FO pipeline above hit outright, so this
    # gets the same defensive headroom.
    resolver = DbInstrumentResolver(max_new_per_batch=10_000)
    return Pipeline(
        source=BseUdiffSource(),
        registry=ParserRegistry([UdiffParser()]),
        normalizer=UdiffNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.BSE_CM_UDIFF),
    )


def _pipeline_nse_cm_legacy() -> Pipeline:
    resolver = DbInstrumentResolver()
    return Pipeline(
        source=NseLegacyCmSource(),
        registry=ParserRegistry([NseLegacyCmParser()]),
        normalizer=NseLegacyNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.NSE_CM_LEGACY),
    )


def _pipeline_amfi_nav_history() -> Pipeline:
    resolver = DbInstrumentResolver()
    return Pipeline(
        source=AmfiNavHistorySource(),
        registry=ParserRegistry([AmfiNavHistoryParser()]),
        normalizer=AmfiNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.AMFI_NAV),
    )


def _pipeline_amfi_nav() -> Pipeline:
    resolver = DbInstrumentResolver()
    return Pipeline(
        source=AmfiNavSource(),
        registry=ParserRegistry([AmfiNavParser()]),
        normalizer=AmfiNormalizer(),
        resolver=resolver,
        validator=BarValidator(),
        loader=BarLoader(resolver, DataSource.AMFI_NAV),
    )


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

    build: Callable[[], Pipeline]
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    spec = SOURCE_SPECS[args.source]
    pipeline = spec.build()
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
