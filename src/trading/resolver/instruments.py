from __future__ import annotations

from collections import defaultdict
from datetime import date

import structlog
from psycopg import Connection

from trading.contracts import AssetClass, InstrumentRef, ValidationAbort

log = structlog.get_logger(__name__)


def _asset_class_for(ref: InstrumentRef) -> str:
    if ref.option_type is not None:
        return AssetClass.OPTION.value
    if ref.expiry is not None:
        return AssetClass.FUTURE.value
    if ref.segment == "MF":
        return AssetClass.MF.value
    return AssetClass.EQUITY.value


class DbInstrumentResolver:
    """Maps natural keys to instrument_ids, creating unseen instruments.

    The only stage that both reads and writes instrument state: every
    trading day can mint new option strikes, so unseen refs must be
    created, but a batch that would mint an absurd number of them is
    refused (`max_new_per_batch`) since that almost always means a parser
    fault rather than a genuinely new universe of contracts.
    """

    def __init__(self, max_new_per_batch: int = 5000) -> None:
        self._max_new = max_new_per_batch
        self._cache: dict[str, int] = {}
        self._bootstrap_next_call = False

    def bootstrap_next_call(self) -> None:
        """Arm `bootstrap=True` for exactly the next `resolve()` call.

        Ruling S3 (task-18-brief.md): `scripts/backfill.py`'s `--bootstrap`
        flag must apply to a run's FIRST day only -- `max_new_per_batch`
        stays a real guard for every subsequent day, where an absurd batch
        means a parser fault, not a genuine first-day universe. `Pipeline`
        and `Loader` are out of this task's scope to widen with a `bootstrap`
        parameter of their own (`Pipeline.run` calls `loader.load(outcome,
        conn)` with no such argument, and `BarLoader.load` calls
        `self._resolver.resolve(refs, conn)` the same way), so this one-shot
        flag lets the caller that already constructs the resolver (`scripts/
        backfill.py`) arm exactly one `resolve()` call -- the one inside the
        first day's `loader.load()` -- without touching either of those.
        """
        self._bootstrap_next_call = True

    def resolve(
        self, refs: set[InstrumentRef], conn: Connection, *, bootstrap: bool = False
    ) -> dict[InstrumentRef, int]:
        if self._bootstrap_next_call:
            bootstrap = True
            self._bootstrap_next_call = False
        by_key = {r.canonical_key: r for r in refs}
        resolved = {k: self._cache[k] for k in by_key if k in self._cache}

        unknown = [k for k in by_key if k not in resolved]
        if unknown:
            rows = conn.execute(
                "SELECT canonical_key, instrument_id FROM instruments "
                "WHERE canonical_key = ANY(%s)",
                (unknown,),
            ).fetchall()
            for key, iid in rows:
                resolved[key] = iid
                self._cache[key] = iid

        missing = [k for k in by_key if k not in resolved]
        if missing:
            if not bootstrap and len(missing) > self._max_new:
                raise ValidationAbort(
                    f"batch would create {len(missing)} new instruments "
                    f"(limit {self._max_new}); suspected parser fault"
                )
            resolved.update(self._create(missing, by_key, conn))

            # Ruling I3: a row can be silently dropped by `_create`'s
            # ON CONFLICT DO NOTHING (e.g. it collides with an existing row
            # on the *natural* key rather than the canonical one). Surface
            # that as a clear abort instead of letting the dict lookup
            # below die with an opaque KeyError.
            unresolved = [k for k in missing if k not in resolved]
            if unresolved:
                raise ValidationAbort(
                    f"failed to resolve {len(unresolved)} instrument key(s) after "
                    f"creation, likely colliding with an existing row on the "
                    f"natural key under a different canonical_key: {unresolved}"
                )

        return {by_key[k]: resolved[k] for k in by_key}

    def _create(
        self, keys: list[str], by_key: dict[str, InstrumentRef], conn: Connection
    ) -> dict[str, int]:
        # Ruling I4: InstrumentRef itself allows option_type set with
        # strike=None (it is shared contract surface we must not tighten
        # here), but `instruments.ck_option_fields` forbids that
        # combination. Catching it before the insert keeps a single
        # malformed ref from surfacing a raw CheckViolation and aborting
        # the whole executemany batch's transaction.
        malformed = [
            k for k in keys if by_key[k].option_type is not None and by_key[k].strike is None
        ]
        if malformed:
            raise ValidationAbort(
                f"{len(malformed)} instrument ref(s) have option_type set without a "
                f"strike, which violates ck_option_fields: {malformed}"
            )

        payload = []
        for k in keys:
            ref = by_key[k]
            payload.append(
                (
                    _asset_class_for(ref),
                    ref.exchange,
                    ref.segment,
                    ref.symbol,
                    ref.series,
                    ref.expiry,
                    ref.strike,
                    ref.option_type.value if ref.option_type is not None else None,
                    "INR",
                    "ACTIVE",
                    k,
                )
            )
        with conn.cursor() as cur:
            # No conflict target: a row can legitimately collide on either
            # uq_instrument_canonical (already resolved by someone else) or
            # uq_instrument_natural (Ruling I3 above handles that case by
            # checking, after this returns, that every key actually
            # resolved). Naming just `canonical_key` here would instead let
            # a natural-key collision raise a raw UniqueViolation.
            cur.executemany(
                "INSERT INTO instruments (asset_class, exchange, segment, symbol, series,"
                " expiry, strike, option_type, currency, status, canonical_key)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT DO NOTHING",
                payload,
            )
        rows = conn.execute(
            "SELECT canonical_key, instrument_id FROM instruments WHERE canonical_key = ANY(%s)",
            (keys,),
        ).fetchall()
        created: dict[str, int] = dict(rows)
        self._cache.update(created)
        log.info("instruments.created", count=len(created))
        return created

    def record_identity(
        self, conn: Connection, meta: dict[str, tuple[str | None, str | None]]
    ) -> int:
        """Fill in each instrument's display name and ISIN.

        Deliberately separate from `InstrumentRef`: these are descriptive,
        not identifying. Folding them into the natural key would mint a
        second instrument the day a company renames itself, and would make
        `canonical_key` depend on a free-text field the exchange edits at
        will. `record_lot_sizes` draws the same line for lot size.

        First observation wins -- COALESCE only fills a column that is still
        NULL. The backfill walks each source's history in its own order and
        re-runs days freely, so last-write-wins would let a name flip back
        and forth depending on which leg ran last. A genuine rename needs
        point-in-time history the way lot size has it, in its own dated
        table; a single mutable column cannot express one honestly, so it
        does not try.

        Returns the number of rows actually updated, which is zero once
        every instrument in the batch is already described.
        """
        if not meta:
            return 0
        keys = list(meta)
        with conn.cursor() as cur:
            # One statement over arrays rather than an executemany: an F&O
            # day resolves ~35,000 contracts, and the NULL predicate keeps
            # this near-free on every day after the first time an instrument
            # is described.
            cur.execute(
                "UPDATE instruments i SET"
                " name = COALESCE(i.name, v.name), isin = COALESCE(i.isin, v.isin)"
                " FROM (SELECT unnest(%s::text[]) AS key, unnest(%s::text[]) AS name,"
                " unnest(%s::text[]) AS isin) v"
                " WHERE i.canonical_key = v.key"
                " AND (i.name IS NULL OR i.isin IS NULL)"
                " AND (v.name IS NOT NULL OR v.isin IS NOT NULL)",
                (keys, [meta[k][0] for k in keys], [meta[k][1] for k in keys]),
            )
            return cur.rowcount if cur.rowcount > 0 else 0

    def record_lot_sizes(self, conn: Connection, lot_rows: list[tuple[int, date, int]]) -> int:
        """Append a lot-history row only when the lot size actually changed.

        Set-based rather than one round trip per instrument (Ruling I1):
        NSE F&O alone carries ~35,000 contracts a day, so a per-instrument
        SELECT+INSERT would be tens of millions of round trips across a
        multi-year backfill.

        The comparison is against the lot size in effect ON each row's own
        `effective_from` date, not the latest row ever recorded for that
        instrument (Ruling I2) — the latter is only correct while days are
        loaded in strict chronological order, and a retried out-of-order
        day would otherwise write a spurious "change" row and corrupt the
        history that F&O notional value depends on.
        """
        if not lot_rows:
            return 0

        by_date: dict[date, list[tuple[int, int]]] = defaultdict(list)
        for instrument_id, effective_from, lot_size in lot_rows:
            by_date[effective_from].append((instrument_id, lot_size))

        to_insert: list[tuple[int, date, int]] = []
        for effective_from, rows in by_date.items():
            instrument_ids = [instrument_id for instrument_id, _ in rows]
            current: dict[int, int] = dict(
                conn.execute(
                    "SELECT DISTINCT ON (instrument_id) instrument_id, lot_size "
                    "FROM instrument_lot_history "
                    "WHERE instrument_id = ANY(%s) AND effective_from <= %s "
                    "ORDER BY instrument_id, effective_from DESC",
                    (instrument_ids, effective_from),
                ).fetchall()
            )
            for instrument_id, lot_size in rows:
                if current.get(instrument_id) == lot_size:
                    continue
                to_insert.append((instrument_id, effective_from, lot_size))

        if not to_insert:
            return 0

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO instrument_lot_history (instrument_id, effective_from,"
                " lot_size, source) VALUES (%s,%s,%s,'udiff')"
                " ON CONFLICT (instrument_id, effective_from) DO NOTHING",
                to_insert,
            )
        return len(to_insert)
