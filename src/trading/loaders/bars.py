from __future__ import annotations

import csv
import io

import polars as pl
from psycopg import Connection

from trading.contracts import (
    DataSource,
    InstrumentRef,
    LoadResult,
    ValidationAbort,
    ValidationOutcome,
)
from trading.resolver.instruments import DbInstrumentResolver

STAGING_COLUMNS = (
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
    "underlying_price",
    "open_interest",
    "oi_change",
    "delivery_qty",
    "delivery_pct",
    "source",
)

# Ruling L5: bars_daily declares these NOT NULL but the validator only
# guarantees `close`. A null slipping through here would abort the whole
# COPY with an opaque NotNullViolation, taking the entire day's batch down
# with it — exactly the failure mode this pipeline exists to avoid.
_REQUIRED_NOT_NULL = ("open", "high", "low", "close")


def identity_map(frame: pl.DataFrame) -> dict[str, tuple[str | None, str | None]]:
    """Extract canonical_key -> (name, isin) from a canonical bar frame.

    Rows carrying neither field are skipped so they cannot mask a row that
    does carry one. First row wins within a batch, matching
    `DbInstrumentResolver.record_identity`'s first-observation-wins rule.
    """
    identity: dict[str, tuple[str | None, str | None]] = {}
    for r in frame.select(
        "exchange", "segment", "symbol", "series", "expiry", "strike", "option_type", "name", "isin"
    ).to_dicts():
        if r["name"] is None and r["isin"] is None:
            continue
        key = InstrumentRef(
            exchange=r["exchange"],
            segment=r["segment"],
            symbol=r["symbol"],
            series=r["series"],
            expiry=r["expiry"],
            strike=r["strike"],
            option_type=r["option_type"],
        ).canonical_key
        identity.setdefault(key, (r["name"], r["isin"]))
    return identity


class BarLoader:
    """`Loader` for canonical bar rows: COPY to a staging table, one upsert.

    Row-by-row upsert of a 100k-row F&O day takes minutes; COPY into an
    unlogged staging table followed by a single
    `INSERT ... SELECT ... ON CONFLICT DO UPDATE` takes seconds.
    """

    def __init__(self, resolver: DbInstrumentResolver, source: DataSource) -> None:
        # Ruling L3: `source` is required, no default. A silent fallback
        # here would stamp every batch loaded without an explicit source as
        # NSE_CM_UDIFF in a NOT NULL provenance column, which is permanent
        # data corruption — provenance is exactly what you consult when you
        # already distrust a row.
        self._resolver = resolver
        self._source = source

    @property
    def source(self) -> DataSource:
        """Ruling P3x (task-14 addendum): lets the pipeline verify, before
        loading, that the normalizer's `batch.source` agrees with what this
        loader was configured to stamp every row with."""
        return self._source

    def load(self, outcome: ValidationOutcome, conn: Connection) -> LoadResult:
        frame = outcome.valid
        if frame.height == 0:
            return LoadResult(rows_written=0, instruments_created=0)

        _check_not_null(frame)

        refs = {
            InstrumentRef(
                exchange=r["exchange"],
                segment=r["segment"],
                symbol=r["symbol"],
                series=r["series"],
                expiry=r["expiry"],
                strike=r["strike"],
                option_type=r["option_type"],
            )
            for r in frame.select(
                "exchange", "segment", "symbol", "series", "expiry", "strike", "option_type"
            ).to_dicts()
        }

        # Ruling L4: one indexed pre-count over just this batch's keys
        # instead of two `count(*)` sequential scans of the whole
        # `instruments` table per load.
        canonical_keys = [ref.canonical_key for ref in refs]
        row = conn.execute(
            "SELECT count(*) FROM instruments WHERE canonical_key = ANY(%s)",
            (canonical_keys,),
        ).fetchone()
        existing = int(row[0]) if row is not None else 0

        mapping = self._resolver.resolve(refs, conn)
        created = len(refs) - existing

        # Every normalizer already computes name and isin into the canonical
        # frame; before this they were dropped here, leaving every instrument
        # in the warehouse holding a bare ticker. Recorded separately from the
        # natural key (see DbInstrumentResolver.record_identity) because a
        # rename must not mint a second instrument. Later days fill gaps left
        # by instruments created before a source carried either field.
        self._resolver.record_identity(conn, identity_map(frame))

        source_id = int(self._source)
        rows = []
        lot_rows = []
        for record in frame.to_dicts():
            ref = InstrumentRef(
                exchange=record["exchange"],
                segment=record["segment"],
                symbol=record["symbol"],
                series=record["series"],
                expiry=record["expiry"],
                strike=record["strike"],
                option_type=record["option_type"],
            )
            iid = mapping[ref]
            rows.append([iid, *(record[c] for c in STAGING_COLUMNS[1:-1]), source_id])
            if record["lot_size"]:
                lot_rows.append((iid, record["ts"].date(), int(record["lot_size"])))

        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _stage_bars "
                "(LIKE bars_daily INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.execute("TRUNCATE _stage_bars")
            buffer = io.StringIO()
            csv.writer(buffer).writerows(rows)
            buffer.seek(0)
            with cur.copy(
                f"COPY _stage_bars ({','.join(STAGING_COLUMNS)}) FROM STDIN WITH CSV"
            ) as copy:
                copy.write(buffer.read())
            cur.execute(
                f"INSERT INTO bars_daily ({','.join(STAGING_COLUMNS)}) "
                f"SELECT {','.join(STAGING_COLUMNS)} FROM _stage_bars "
                "ON CONFLICT (instrument_id, ts) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, prev_close=EXCLUDED.prev_close, "
                "volume=EXCLUDED.volume, turnover=EXCLUDED.turnover, "
                "trades=EXCLUDED.trades, settle_price=EXCLUDED.settle_price, "
                "underlying_price=EXCLUDED.underlying_price, "
                "open_interest=EXCLUDED.open_interest, oi_change=EXCLUDED.oi_change, "
                # Ruling: delivery_* arrives from a separate NSE file and must
                # not be nulled by a later UDiFF upsert of the same row.
                "delivery_qty=COALESCE(EXCLUDED.delivery_qty, bars_daily.delivery_qty), "
                "delivery_pct=COALESCE(EXCLUDED.delivery_pct, bars_daily.delivery_pct), "
                "source=EXCLUDED.source, ingested_at=now()"
            )

        self._resolver.record_lot_sizes(conn, lot_rows)
        return LoadResult(rows_written=frame.height, instruments_created=created)


def _check_not_null(frame: pl.DataFrame) -> None:
    """Defence in depth behind the validator's own NOT NULL guarantees.

    Raises `ValidationAbort` naming every offending column and how many
    rows are affected, rather than letting a null reach `COPY` and abort
    the whole statement with an opaque `NotNullViolation`.
    """
    null_counts = {col: frame[col].null_count() for col in _REQUIRED_NOT_NULL}
    bad = {col: count for col, count in null_counts.items() if count > 0}
    if bad:
        detail = ", ".join(f"{col} ({count} row(s))" for col, count in bad.items())
        raise ValidationAbort(
            f"bars_daily requires non-null open/high/low/close; found nulls in {detail} "
            f"out of {frame.height} row(s) in this batch"
        )
