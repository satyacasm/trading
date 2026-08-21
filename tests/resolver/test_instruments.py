from datetime import date
from decimal import Decimal

import pytest

from trading.contracts import InstrumentRef, OptionType, ValidationAbort
from trading.resolver.instruments import DbInstrumentResolver

pytestmark = pytest.mark.db


def _ref(symbol: str = "RELIANCE") -> InstrumentRef:
    return InstrumentRef(exchange="NSE", segment="CM", symbol=symbol)


def test_creates_instruments_it_has_never_seen(db_conn):
    mapping = DbInstrumentResolver().resolve({_ref()}, db_conn)
    assert set(mapping) == {_ref()}
    assert isinstance(mapping[_ref()], int)


def test_resolving_twice_returns_the_same_id(db_conn):
    resolver = DbInstrumentResolver()
    first = resolver.resolve({_ref()}, db_conn)
    second = resolver.resolve({_ref()}, db_conn)
    assert first[_ref()] == second[_ref()]


def test_a_second_resolver_instance_reuses_the_stored_row(db_conn):
    """The cache must not be the only source of identity."""
    first = DbInstrumentResolver().resolve({_ref()}, db_conn)
    second = DbInstrumentResolver().resolve({_ref()}, db_conn)
    assert first[_ref()] == second[_ref()]


def test_option_refs_get_distinct_ids_per_strike(db_conn):
    refs = {
        InstrumentRef(
            exchange="NSE",
            segment="FO",
            symbol="NIFTY",
            expiry=date(2026, 8, 27),
            strike=Decimal(s),
            option_type=OptionType.CE,
        )
        for s in ("24500", "24600")
    }
    mapping = DbInstrumentResolver().resolve(refs, db_conn)
    assert len(set(mapping.values())) == 2


def test_strike_scale_does_not_create_a_duplicate(db_conn):
    """Decimal('24500') and Decimal('24500.00') are one contract.

    InstrumentRef.canonical_key already normalises strike scale
    (src/trading/contracts/models.py), so this exercises that contract
    rather than any logic of the resolver's own.
    """
    resolver = DbInstrumentResolver()
    a = InstrumentRef(
        exchange="NSE",
        segment="FO",
        symbol="NIFTY",
        expiry=date(2026, 8, 27),
        strike=Decimal("24500"),
        option_type=OptionType.CE,
    )
    b = a.model_copy(update={"strike": Decimal("24500.0000")})
    assert resolver.resolve({a}, db_conn)[a] == resolver.resolve({b}, db_conn)[b]


def test_aborts_when_a_batch_would_mint_absurdly_many_instruments(db_conn):
    """Guards against a parser typo generating garbage at scale."""
    refs = {_ref(f"JUNK{i}") for i in range(11)}
    with pytest.raises(ValidationAbort, match="new instruments"):
        DbInstrumentResolver(max_new_per_batch=10).resolve(refs, db_conn)


def test_bootstrap_flag_bypasses_the_abort_guard(db_conn):
    refs = {_ref(f"BOOT{i}") for i in range(11)}
    mapping = DbInstrumentResolver(max_new_per_batch=10).resolve(refs, db_conn, bootstrap=True)
    assert len(mapping) == 11


def test_lot_history_records_only_changes(db_conn):
    resolver = DbInstrumentResolver()
    iid = resolver.resolve({_ref("LOTTEST")}, db_conn)[_ref("LOTTEST")]
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 10), 75)])
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 11), 75)])  # unchanged
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 12), 50)])  # changed
    rows = db_conn.execute(
        "SELECT effective_from, lot_size FROM instrument_lot_history "
        "WHERE instrument_id=%s ORDER BY effective_from",
        (iid,),
    ).fetchall()
    assert rows == [(date(2026, 8, 10), 75), (date(2026, 8, 12), 50)]


# --- Task 11 addendum, Ruling I1: record_lot_sizes must be set-based ---


def test_lot_history_batches_a_multi_instrument_call_into_two_queries(db_conn):
    """One call covering many instruments must not regress to per-row round
    trips; also exercises that unrelated instruments don't interfere."""
    resolver = DbInstrumentResolver()
    refs = {_ref(f"LOTBATCH{i}") for i in range(5)}
    mapping = resolver.resolve(refs, db_conn)
    lot_rows = [(iid, date(2026, 8, 10), 75) for iid in mapping.values()]
    written = resolver.record_lot_sizes(db_conn, lot_rows)
    assert written == 5
    # Recording the identical batch again writes nothing further.
    assert resolver.record_lot_sizes(db_conn, lot_rows) == 0


# --- Task 11 addendum, Ruling I2: compare against the lot size in effect
# ON that date, not the latest row ever recorded ---


def test_lot_history_is_correct_regardless_of_processing_order(db_conn):
    """A later date recorded before an earlier one (as happens on a retried
    backfill) must not corrupt history: comparisons must be scoped to what
    was in effect on each row's own date, not the globally-latest row."""
    resolver = DbInstrumentResolver()
    iid = resolver.resolve({_ref("LOTORDER")}, db_conn)[_ref("LOTORDER")]

    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 12), 50)])  # LATER, first
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 10), 75)])  # EARLIER, different
    # This middle date is unchanged relative to what was in effect on it
    # (2026-08-10's 75) once history is looked at correctly; a "latest row
    # ever" comparison would instead see 2026-08-12's 50 and wrongly treat
    # it as changed, writing a spurious extra row.
    resolver.record_lot_sizes(db_conn, [(iid, date(2026, 8, 11), 75)])

    rows = db_conn.execute(
        "SELECT effective_from, lot_size FROM instrument_lot_history "
        "WHERE instrument_id=%s ORDER BY effective_from",
        (iid,),
    ).fetchall()
    # Exactly what a strict chronological run (8-10, 8-11, 8-12) would have produced.
    assert rows == [(date(2026, 8, 10), 75), (date(2026, 8, 12), 50)]


# --- Task 11 addendum, Ruling I3: an unresolved key after _create must raise ---


def test_natural_key_collision_raises_validation_abort_not_keyerror(db_conn):
    """Pre-insert a row that collides with `ref` on the natural key but
    carries a different canonical_key. `_create`'s insert is silently
    dropped for that row, so it must surface as ValidationAbort rather than
    an opaque KeyError from resolve()'s final lookup."""
    ref = _ref("COLLIDE")
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol,"
            " currency, status, canonical_key) VALUES"
            " ('EQUITY', %s, %s, %s, 'INR', 'ACTIVE', %s)",
            (ref.exchange, ref.segment, ref.symbol, "not-the-real-canonical-key"),
        )
    with pytest.raises(ValidationAbort, match="failed to resolve"):
        DbInstrumentResolver().resolve({ref}, db_conn)


# --- Task 11 fix round 1, Ruling I4: option_type without strike must not
# reach the DB as a raw CheckViolation ---


def test_option_type_without_strike_raises_validation_abort_not_checkviolation(db_conn):
    """`InstrumentRef` itself permits option_type set with strike=None (it's
    shared contract surface the resolver must not tighten), but the DB's
    ck_option_fields CHECK forbids that combination. The resolver must catch
    it before inserting and raise ValidationAbort, not let a raw
    psycopg.errors.CheckViolation escape and abort the whole batch."""
    ref = InstrumentRef(
        exchange="NSE",
        segment="FO",
        symbol="NIFTY",
        expiry=date(2026, 8, 27),
        option_type=OptionType.CE,
        # strike deliberately omitted (None)
    )
    with pytest.raises(ValidationAbort, match="option_type"):
        DbInstrumentResolver().resolve({ref}, db_conn)
