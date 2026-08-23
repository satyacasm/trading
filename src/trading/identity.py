"""Backfill instrument names and ISINs from already-archived bytes.

`BarLoader` records name and isin at load time (`record_identity`), but most
of the warehouse was built before it did, and the two long-running backfill
legs held the pre-fix code in memory for their entire run. Those instruments
exist, are correct, and are simply undescribed.

Re-reading the raw archives is the honest repair: the bytes on disk are the
same ones the exchange served, so this derives the name and ISIN from the
identical source the original load used -- no re-download, no second source
of truth, and no guessing. Only the two descriptive columns are touched;
prices are never rewritten.

Run for real with:
    uv run python -m trading.identity --source nse_cm_udiff
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import structlog
from psycopg import Connection

from trading.config import get_settings
from trading.contracts import DataSource, Normalizer, RawPayload
from trading.loaders.bars import identity_map
from trading.parsers.registry import ParserRegistry
from trading.resolver.instruments import DbInstrumentResolver

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class RepairResult:
    source_key: str
    archives_read: int
    archives_missing: int
    rows_updated: int

    def __str__(self) -> str:
        missing = f", {self.archives_missing} archive(s) missing" if self.archives_missing else ""
        return (
            f"{self.source_key}: {self.rows_updated} instrument(s) described "
            f"from {self.archives_read} archive(s){missing}"
        )


def repair_identity(
    conn: Connection,
    source_key: str,
    data_source: DataSource,
    registry: ParserRegistry,
    normalizer: Normalizer,
    resolver: DbInstrumentResolver,
    *,
    on_progress: object = None,
) -> RepairResult:
    """Walk `source_key`'s archived days oldest-first, filling in any name or
    ISIN still missing.

    Oldest-first matters: `record_identity` keeps the first value it sees, so
    walking forward through time means a delisted instrument is described by
    the era it actually traded in rather than by whichever day happened to run
    last. Stops touching an instrument as soon as it is described, so a second
    run over the same archives updates nothing.
    """
    jobs = conn.execute(
        "SELECT business_date, archive_path FROM ingest_jobs "
        "WHERE source_key = %s AND status = 'SUCCESS' AND archive_path IS NOT NULL "
        "ORDER BY business_date",
        (source_key,),
    ).fetchall()

    read = missing = updated = 0
    for business_date, archive_path in jobs:
        path = Path(archive_path)
        if not path.exists():
            missing += 1
            continue
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
        frame = normalizer.normalize(parser.parse(payload), payload).frame
        updated += resolver.record_identity(conn, identity_map(frame))
        read += 1
        if callable(on_progress):
            on_progress(read, len(jobs), business_date, updated)

    return RepairResult(source_key, read, missing, updated)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _bundle(source_key: str) -> tuple[ParserRegistry, Normalizer, DataSource]:
    from trading.normalizers.amfi import AmfiNormalizer
    from trading.normalizers.nse_legacy import NseLegacyNormalizer
    from trading.normalizers.udiff import UdiffNormalizer
    from trading.parsers.amfi_history import AmfiNavHistoryParser
    from trading.parsers.nse_legacy import NseLegacyCmParser
    from trading.parsers.udiff import UdiffParser

    if source_key == "nse_cm_legacy":
        return (
            ParserRegistry([NseLegacyCmParser()]),
            NseLegacyNormalizer(),
            DataSource.NSE_CM_LEGACY,
        )
    if source_key == "amfi_nav_history":
        return ParserRegistry([AmfiNavHistoryParser()]), AmfiNormalizer(), DataSource.AMFI_NAV
    udiff = {
        "nse_cm_udiff": DataSource.NSE_CM_UDIFF,
        "nse_fo_udiff": DataSource.NSE_FO_UDIFF,
        "bse_cm_udiff": DataSource.BSE_CM_UDIFF,
    }
    return ParserRegistry([UdiffParser()]), UdiffNormalizer(), udiff[source_key]


SOURCES = ("nse_cm_udiff", "nse_fo_udiff", "bse_cm_udiff", "nse_cm_legacy", "amfi_nav_history")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=sorted(SOURCES), action="append")
    args = parser.parse_args(argv)
    sources = args.source or list(SOURCES)

    with psycopg.connect(get_settings().database_url, autocommit=False) as conn:
        for source_key in sources:
            registry, normalizer, data_source = _bundle(source_key)
            resolver = DbInstrumentResolver()

            def progress(n: int, total: int, d: date, updated: int) -> None:
                if n % 100 == 0 or n == total:
                    print(f"  [{n}/{total}] {d} · {updated} described", flush=True)

            print(f"repairing {source_key} ...", flush=True)
            result = repair_identity(
                conn, source_key, data_source, registry, normalizer, resolver, on_progress=progress
            )
            conn.commit()  # one durable checkpoint per source
            print(result, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
