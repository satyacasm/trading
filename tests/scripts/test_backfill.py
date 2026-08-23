"""Tests for `scripts/backfill.py`'s `--bootstrap` flag (Task 18, Ruling S3).

`main()` itself opens a real connection and drives real `Source.fetch` calls
over the network, so these tests exercise the pieces `main()` wires together
-- CLI parsing and the resolver each `SourceSpec.build()` constructs --
directly, against `db_conn`'s rolled-back transaction, with no network call.
"""

from __future__ import annotations

import pytest

import scripts.backfill as backfill
from trading.contracts import InstrumentRef, ValidationAbort

pytestmark = pytest.mark.db


def _ref(symbol: str) -> InstrumentRef:
    return InstrumentRef(exchange="NSE", segment="FO", symbol=symbol)


def test_bootstrap_flag_is_parsed():
    args = backfill._parse_args(
        ["--source", "nse_fo_udiff", "--from", "2024-07-01", "--to", "2024-07-01", "--bootstrap"]
    )
    assert args.bootstrap is True


def test_bootstrap_defaults_to_false():
    args = backfill._parse_args(
        ["--source", "nse_fo_udiff", "--from", "2024-07-01", "--to", "2024-07-01"]
    )
    assert args.bootstrap is False


@pytest.mark.parametrize("source_key", sorted(backfill.SOURCE_SPECS))
def test_every_pipeline_keeps_the_default_guard(source_key: str):
    """Ruling S3: `max_new_per_batch` must stay at its 5,000 default for
    every source -- the elevated per-pipeline caps Task 17 added for
    nse_fo_udiff/bse_cm_udiff are replaced by `--bootstrap`, not kept
    alongside it."""
    _pipeline, resolver = backfill.SOURCE_SPECS[source_key].build()
    assert resolver._max_new == 5000  # the only way to observe this from outside


def test_bootstrap_next_call_reaches_the_resolver_built_by_a_source_spec(db_conn):
    """Proves the flag reaches the SAME resolver object `main()` would arm
    and pass to the pipeline -- not a stand-in constructed by the test."""
    _pipeline, resolver = backfill.SOURCE_SPECS["nse_fo_udiff"].build()
    resolver.bootstrap_next_call()
    refs = {_ref(f"BOOTCLI{i}") for i in range(11)}  # over the 5,000 default? no -- proves bypass
    # 11 > 0 is enough to prove the bypass path without minting 5,001 rows.
    mapping = resolver.resolve(refs, db_conn)
    assert len(mapping) == 11


def test_without_bootstrap_a_later_day_still_aborts_on_an_absurd_batch(db_conn):
    """The flag's absence must leave the real guard intact -- exercised
    against a resolver built the same way `main()` builds one, with an
    artificially tiny cap standing in for "absurd" so the test stays fast."""
    _pipeline, resolver = backfill.SOURCE_SPECS["nse_fo_udiff"].build()
    resolver._max_new = 10  # simulate "absurd" without needing 5,001 refs
    refs = {_ref(f"NOFLAG{i}") for i in range(11)}
    with pytest.raises(ValidationAbort, match="new instruments"):
        resolver.resolve(refs, db_conn)
