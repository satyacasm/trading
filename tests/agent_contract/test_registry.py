"""Registration -- stage 3 of the upload pipeline (contract §9).

The registry is what makes a strategy referable: a backtest report, a
forward paper run, and a leaderboard row all need to name the exact code
that produced them. That is why the load-bearing property here is not
"can we store a strategy" but **version immutability** -- see
`test_re_registering_a_version_with_different_source_is_a_conflict`.
"""

from __future__ import annotations

import textwrap

import pytest

from trading.agent_contract.registry import (
    StrategyRejected,
    VersionConflict,
    get_strategy,
    list_strategies,
    register_strategy,
)

pytestmark = pytest.mark.db


def src(text: str) -> str:
    return textwrap.dedent(text).strip() + "\n"


VALID = src(
    """
    from decimal import Decimal


    class MyStrategy(Strategy):
        def configure(self):
            return None

        def on_bar(self, ctx, bars):
            ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="demo")
    """
)

VALID_V2 = VALID.replace('quantity=Decimal("1")', 'quantity=Decimal("2")')

INVALID = src(
    """
    import requests


    class MyStrategy(Strategy):
        def configure(self):
            return None
    """
)


@pytest.fixture
def user_id(db_conn) -> int:
    row = db_conn.execute("SELECT user_id FROM users WHERE email='local@paper.trading'").fetchone()
    assert row is not None, "migration 0007 seeds the local user"
    return int(row[0])


# --- the table ----------------------------------------------------------------


def test_strategies_table_has_the_columns_the_registry_needs(db_conn) -> None:
    columns = {
        row[0]
        for row in db_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='strategies'"
        ).fetchall()
    }
    assert {
        "strategy_id",
        "user_id",
        "name",
        "version",
        "source",
        "source_sha256",
        "manifest",
        "status",
        "contract_version",
        "registered_at",
    } <= columns


# --- registering --------------------------------------------------------------


def test_registering_a_valid_strategy_stores_and_returns_it(db_conn, user_id: int) -> None:
    record = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)

    assert record.strategy_id > 0
    assert record.name == "demo"
    assert record.version == "1.0.0"
    assert record.status == "REGISTERED"
    assert record.source_sha256

    stored = db_conn.execute(
        "SELECT name, version, source FROM strategies WHERE strategy_id=%s", (record.strategy_id,)
    ).fetchone()
    assert stored == ("demo", "1.0.0", VALID)


def test_the_source_hash_is_stable_and_content_addressed(db_conn, user_id: int) -> None:
    """A backtest result references the hash, not just the id, so two runs
    can be shown to have used identical code."""
    first = register_strategy(db_conn, user_id=user_id, name="a", version="1.0.0", source=VALID)
    second = register_strategy(db_conn, user_id=user_id, name="b", version="1.0.0", source=VALID)
    assert first.source_sha256 == second.source_sha256

    different = register_strategy(
        db_conn, user_id=user_id, name="c", version="1.0.0", source=VALID_V2
    )
    assert different.source_sha256 != first.source_sha256


def test_a_strategy_failing_static_validation_is_not_stored(db_conn, user_id: int) -> None:
    """The registry runs stage 1 first and refuses to store what it
    rejects. A registry holding strategies that cannot run is worse than an
    empty one: every consumer downstream would have to re-validate, and the
    one that forgets ships a broken run."""
    with pytest.raises(StrategyRejected) as exc_info:
        register_strategy(db_conn, user_id=user_id, name="bad", version="1.0.0", source=INVALID)

    # The rejection carries the agent-facing report, so a caller can hand it
    # straight back without re-running validation.
    assert "IMPORT_NOT_ALLOWED" in exc_info.value.report.as_agent_feedback()

    stored = db_conn.execute("SELECT count(*) FROM strategies WHERE name='bad'").fetchone()
    assert stored == (0,)


# --- versioning: the load-bearing rule ----------------------------------------


def test_re_registering_identical_source_under_the_same_version_is_idempotent(
    db_conn, user_id: int
) -> None:
    """A retried upload must not create a second row, exactly like the
    order API's idempotency key."""
    first = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)
    again = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)

    assert again.strategy_id == first.strategy_id
    count = db_conn.execute(
        "SELECT count(*) FROM strategies WHERE name='demo' AND version='1.0.0'"
    ).fetchone()
    assert count == (1,)


def test_re_registering_a_version_with_different_source_is_a_conflict(
    db_conn, user_id: int
) -> None:
    """**A registered version is immutable.**

    If `demo 1.0.0` could be overwritten, every backtest report, equity
    curve, and forward run already referring to `demo 1.0.0` would silently
    describe code that no longer exists -- results attributed to a strategy
    nobody can reproduce. That is the same point-in-time discipline the
    data layer keeps, applied to code.

    Publishing a change means publishing a new version.
    """
    register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)

    with pytest.raises(VersionConflict) as exc_info:
        register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID_V2)

    message = str(exc_info.value)
    assert "1.0.0" in message
    assert "immutable" in message.lower()

    # The original survives untouched.
    stored = db_conn.execute(
        "SELECT source FROM strategies WHERE name='demo' AND version='1.0.0'"
    ).fetchone()
    assert stored == (VALID,)


def test_a_new_version_of_the_same_name_is_a_separate_row(db_conn, user_id: int) -> None:
    v1 = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)
    v2 = register_strategy(db_conn, user_id=user_id, name="demo", version="1.1.0", source=VALID_V2)

    assert v1.strategy_id != v2.strategy_id
    assert get_strategy(db_conn, v1.strategy_id).source == VALID
    assert get_strategy(db_conn, v2.strategy_id).source == VALID_V2


# --- reading back -------------------------------------------------------------


def test_get_strategy_returns_the_registered_record(db_conn, user_id: int) -> None:
    record = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)
    fetched = get_strategy(db_conn, record.strategy_id)
    assert fetched.strategy_id == record.strategy_id
    assert fetched.source == VALID


def test_get_strategy_raises_for_an_unknown_id(db_conn) -> None:
    with pytest.raises(KeyError, match="99999999"):
        get_strategy(db_conn, 99999999)


def test_list_strategies_returns_this_users_strategies_newest_first(db_conn, user_id: int) -> None:
    first = register_strategy(db_conn, user_id=user_id, name="a", version="1.0.0", source=VALID)
    second = register_strategy(db_conn, user_id=user_id, name="b", version="1.0.0", source=VALID_V2)

    listed = [s.strategy_id for s in list_strategies(db_conn, user_id=user_id)]
    assert listed[:2] == [second.strategy_id, first.strategy_id]


def test_registration_records_the_contract_version_it_was_validated_against(
    db_conn, user_id: int
) -> None:
    """The contract is a draft and will change. A strategy accepted under
    v0.1 is not automatically valid under v1.0, so the version it was
    checked against is stored with it rather than assumed to be current."""
    record = register_strategy(db_conn, user_id=user_id, name="demo", version="1.0.0", source=VALID)
    assert record.contract_version
