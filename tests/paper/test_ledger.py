"""The atomic fill-to-ledger write.

`apply_fill` records a fill and everything it implies -- ledger entry,
position, cash balance, order status -- as one atomic transaction. These
tests exercise the four decisions that carry real weight: charges are a
cash cost never folded into cost basis, charges always leave the account
on both sides of a trade, the cash floor is enforced by the database
(never pre-checked and clamped in Python), and `apply_fill` never commits
so the caller owns the transaction boundary.

All tests need the database (every fixture inserts real rows), so this
module marks itself with `pytestmark = pytest.mark.db`, matching
`tests/paper/test_migration.py`'s convention.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.paper.helpers import (
    _positions,
    decision_at,
    make_order,
    make_portfolio,
    simple_charges,
)
from trading.paper.enums import OrderStatus, Side
from trading.paper.ledger import apply_fill, replay_portfolio

pytestmark = pytest.mark.db


def test_buy_decreases_cash_by_notional_plus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    order = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    charges = simple_charges(brokerage=Decimal("20"))
    apply_fill(db_conn, order, decision_at(Decimal("100")), charges)

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert cash == Decimal("100000") - Decimal("1000") - Decimal("20")


def test_sell_increases_cash_by_notional_minus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    buy = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, buy, decision_at(Decimal("100")), simple_charges())
    sell = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("10"))
    apply_fill(
        db_conn,
        sell,
        decision_at(Decimal("110")),
        simple_charges(brokerage=Decimal("20")),
    )

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert cash == Decimal("100000") - Decimal("1000") + Decimal("1100") - Decimal("20")


def test_position_average_cost_after_two_buys(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    for price in (Decimal("100"), Decimal("120")):
        o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
        apply_fill(db_conn, o, decision_at(price), simple_charges())
    qty, avg = db_conn.execute(
        "SELECT quantity, avg_cost FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()
    assert qty == Decimal("20")
    assert avg == Decimal("110")


def test_sell_records_realised_pnl(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    b = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, b, decision_at(Decimal("100")), simple_charges())
    s = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("4"))
    apply_fill(db_conn, s, decision_at(Decimal("130")), simple_charges())
    realised = db_conn.execute(
        "SELECT realised_pnl FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert realised == Decimal("120")  # 4 * (130 - 100)


def test_order_status_advances_in_the_same_transaction(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, o, decision_at(Decimal("100")), simple_charges())
    status, filled = db_conn.execute(
        "SELECT status, filled_quantity FROM orders WHERE order_id=%s",
        (o.order_id,),
    ).fetchone()
    assert status == OrderStatus.FILLED
    assert filled == Decimal("10")


def test_buy_exceeding_cash_is_refused(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("500"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    with pytest.raises(Exception):  # noqa: B017 -- the DB's own constraint error type
        apply_fill(db_conn, o, decision_at(Decimal("100")), simple_charges())


def test_apply_fill_does_not_commit(db_conn) -> None:
    """The transaction-boundary contract, asserted directly: nothing
    `apply_fill` does survives past this test's rollback unless the
    caller (this test, standing in for the engine) commits it -- and
    this test never does. A stray `conn.commit()` inside `apply_fill`
    would only be caught by a test that checks durability, not by the
    behavioural assertions above, which pass identically either way."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, o, decision_at(Decimal("100")), simple_charges())
    db_conn.rollback()

    # The portfolio itself was created and rolled back away, so any
    # trace of the fill going through implies apply_fill committed.
    row = db_conn.execute("SELECT 1 FROM portfolios WHERE portfolio_id=%s", (pid,)).fetchone()
    assert row is None


@settings(
    max_examples=25,
    deadline=None,
    # `db_conn` is function-scoped and deliberately not reset between
    # generated examples: each example makes its own fresh portfolio_id,
    # so accumulating unrelated portfolios' rows in one rolled-back
    # transaction across examples is harmless -- the query in both
    # `_positions` and `replay_portfolio` is scoped by portfolio_id.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    prices=st.lists(
        st.decimals(min_value=Decimal("1"), max_value=Decimal("500"), places=2),
        min_size=1,
        max_size=8,
    )
)
def test_replay_reproduces_the_cached_cash_and_positions(db_conn, prices) -> None:
    """The invariant that earns `cash_balance` and `positions` their place
    as caches: replaying every fill must reproduce them exactly."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    for p in prices:
        o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("1"))
        apply_fill(db_conn, o, decision_at(Decimal(p)), simple_charges())

    cached_cash, cached_pos = (
        db_conn.execute(
            "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()[0],
        _positions(db_conn, pid),
    )
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == cached_pos
