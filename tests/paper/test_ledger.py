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
from trading.paper.ledger import OrderNoLongerFillable, apply_fill, replay_portfolio

pytestmark = pytest.mark.db


def test_buy_decreases_cash_by_notional_plus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    order = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    charges = simple_charges(brokerage=Decimal("20"))
    apply_fill(db_conn, order, decision_at(Decimal("100"), quantity=Decimal("10")), charges)

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert cash == Decimal("100000") - Decimal("1000") - Decimal("20")


def test_sell_increases_cash_by_notional_minus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    buy = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, buy, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges())
    sell = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("10"))
    apply_fill(
        db_conn,
        sell,
        decision_at(Decimal("110"), quantity=Decimal("10")),
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
        apply_fill(db_conn, o, decision_at(price, quantity=Decimal("10")), simple_charges())
    qty, avg = db_conn.execute(
        "SELECT quantity, avg_cost FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()
    assert qty == Decimal("20")
    assert avg == Decimal("110")


def test_sell_records_realised_pnl(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    b = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, b, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges())
    s = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("4"))
    apply_fill(db_conn, s, decision_at(Decimal("130"), quantity=Decimal("4")), simple_charges())
    realised = db_conn.execute(
        "SELECT realised_pnl FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert realised == Decimal("120")  # 4 * (130 - 100)


def test_order_status_advances_in_the_same_transaction(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, o, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges())
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
        apply_fill(
            db_conn, o, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges()
        )


def test_apply_fill_raises_and_rolls_back_when_order_status_changed_concurrently(
    db_conn,
) -> None:
    """Optimistic concurrency control (Task 8 fix round 2): a fill is
    decided against a snapshot of `order` that can go stale by the time
    `apply_fill`'s final, guarded UPDATE runs -- most concretely, a
    `cancel` that committed elsewhere in between. Simulated here by
    cancelling the order directly, on the same connection, before calling
    `apply_fill`. Proves both that `apply_fill` raises `OrderNoLongerFillable`
    AND that nothing it already wrote (fill row, ledger entry, cash,
    position) survives the caller's rollback -- run inside a savepoint
    (`conn.transaction()`, the same pattern `api.py`'s `_insert_order`
    uses) so the CANCELLED status set just above, *outside* the savepoint,
    stays visible afterward for inspection instead of also being wiped by
    a full rollback."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    db_conn.execute("UPDATE orders SET status='CANCELLED' WHERE order_id=%s", (o.order_id,))

    with pytest.raises(OrderNoLongerFillable), db_conn.transaction():
        apply_fill(
            db_conn, o, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges()
        )

    fills = db_conn.execute("SELECT 1 FROM fills WHERE order_id=%s", (o.order_id,)).fetchall()
    assert fills == []
    ledger = db_conn.execute(
        "SELECT 1 FROM ledger_entries WHERE portfolio_id=%s", (pid,)
    ).fetchall()
    assert ledger == []
    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()
    assert cash == (Decimal("100000.0000"),)  # untouched by the rolled-back attempt
    status = db_conn.execute(
        "SELECT status FROM orders WHERE order_id=%s", (o.order_id,)
    ).fetchone()
    assert status == ("CANCELLED",)  # never overwritten with FILLED


def test_apply_fill_does_not_commit(db_conn) -> None:
    """The transaction-boundary contract, asserted directly: nothing
    `apply_fill` does survives past this test's rollback unless the
    caller (this test, standing in for the engine) commits it -- and
    this test never does. A stray `conn.commit()` inside `apply_fill`
    would only be caught by a test that checks durability, not by the
    behavioural assertions above, which pass identically either way."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, o, decision_at(Decimal("100"), quantity=Decimal("10")), simple_charges())
    db_conn.rollback()

    # The portfolio itself was created and rolled back away, so any
    # trace of the fill going through implies apply_fill committed.
    row = db_conn.execute("SELECT 1 FROM portfolios WHERE portfolio_id=%s", (pid,)).fetchone()
    assert row is None


def test_apply_fill_quantizes_a_price_with_more_than_four_decimal_places(db_conn) -> None:
    """Fix-round-2 regression: `decide_fill` only ever hands `apply_fill` a
    price already at <=4dp (2dp for a market fill, or a limit price sourced
    straight from a `NUMERIC(18,4)` column), so this path is unreachable
    through today's only `FillDecision` producer -- but nothing in
    `ledger.py` stated that as a precondition. `decision_at` builds a
    `FillDecision` directly with no rounding of its own, standing in for a
    future producer (Task 8 or 11) that might not quantize either. A price
    carrying six decimal places must still round-trip exactly: `apply_fill`
    quantizes `decision.price` to 4dp before computing notional or
    avg_cost, so `replay_portfolio` -- which only ever sees the
    already-4dp value read back from `fills.price` -- reproduces both cash
    and avg_cost exactly rather than diverging on the price axis the same
    way the fix-round-1 cash defect diverged on the quantity axis."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(
        db_conn,
        o,
        decision_at(Decimal("100.123456"), quantity=Decimal("10")),
        simple_charges(),
    )

    cached_cash, cached_pos = (
        db_conn.execute(
            "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()[0],
        _positions(db_conn, pid),
    )
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == cached_pos


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
        apply_fill(db_conn, o, decision_at(Decimal(p), quantity=Decimal("1")), simple_charges())

    cached_cash, cached_pos = (
        db_conn.execute(
            "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()[0],
        _positions(db_conn, pid),
    )
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == cached_pos


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(data=st.data())
def test_replay_reproduces_cached_cash_and_positions_with_sells(db_conn, data) -> None:
    """COV-1: every property test above (and the fixed examples elsewhere
    in this file) only ever generates BUYs, so `_apply_position`'s and
    `replay_portfolio`'s SELL branches -- including the `realised_pnl`
    quantization that was one of the five previously-fixed defects on this
    branch -- are exercised only by round-number fixed examples like
    `test_sell_records_realised_pnl`.

    Uses `st.data()` (an interactive strategy) rather than pre-generating a
    list of sides, because a valid SELL quantity depends on the position
    *already built* by prior fills in the same example: filtering a
    pre-generated sequence down to only-valid sells (`assume(...)`) would
    make Hypothesis discard the large majority of examples and warn or
    fail on `too_slow`/`filter_too_much`. Drawing the sell quantity from
    `held` at each step, as this does, respects the long-only constraint
    by construction instead."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    held = Decimal("0")
    num_fills = data.draw(st.integers(min_value=1, max_value=8))
    for _ in range(num_fills):
        price = data.draw(st.decimals(min_value=Decimal("1"), max_value=Decimal("500"), places=2))
        if held >= Decimal("0.01") and data.draw(st.booleans()):
            side = Side.SELL
            quantity = data.draw(st.decimals(min_value=Decimal("0.01"), max_value=held, places=2))
        else:
            side = Side.BUY
            quantity = data.draw(
                st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100"), places=2)
            )
        o = make_order(db_conn, pid, side=side, quantity=quantity)
        apply_fill(db_conn, o, decision_at(price, quantity=quantity), simple_charges())
        held = held + quantity if side is Side.BUY else held - quantity

    cached_cash, cached_pos = (
        db_conn.execute(
            "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
        ).fetchone()[0],
        _positions(db_conn, pid),
    )
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == cached_pos


def test_replay_matches_cached_cash_for_a_fractional_crypto_fill(db_conn) -> None:
    """Fix-round-1 regression: `orders.quantity`/`fills.quantity` are
    `NUMERIC(18,8)` precisely because crypto fills are fractional. A single
    0.00000001 BTC fill at 79090.0100 produces a notional with twelve
    decimal places even though both inputs were exact -- Postgres rounds
    that to `cash_balance`'s `NUMERIC(18,4)` on write, but a
    `replay_portfolio` that summed in full precision and rounded only once
    at the end would not reproduce the same rounded number. This is the
    property test above's exact blind spot: it never generates a price
    below 2dp or a quantity below a whole unit, so it could not have
    caught this."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("0.00000001"))
    apply_fill(
        db_conn,
        o,
        decision_at(Decimal("79090.0100"), quantity=Decimal("0.00000001")),
        simple_charges(),
    )

    cached_cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    replayed_cash, _ = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash


def test_replay_matches_cached_cash_across_multiple_fractional_crypto_fills(db_conn) -> None:
    """The multi-fill version of the regression above: accumulated
    intermediate roundings, not a single rounding step, is what
    `replay_portfolio` must get right. Quantizing cash once at the end
    instead of after every fill would still diverge here even though it
    would happen to pass the single-fill test."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    for qty, price in (
        (Decimal("0.00000001"), Decimal("79090.0100")),
        (Decimal("0.00000003"), Decimal("31245.3333")),
        (Decimal("0.00000007"), Decimal("100.0001")),
    ):
        o = make_order(db_conn, pid, side=Side.BUY, quantity=qty)
        apply_fill(db_conn, o, decision_at(price, quantity=qty), simple_charges())

    cached_cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    replayed_cash, _ = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    fills=st.lists(
        st.tuples(
            # NUMERIC(18,8): quantity, at crypto precision.
            st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("2"), places=8),
            # NUMERIC(18,4): price.
            st.decimals(min_value=Decimal("1"), max_value=Decimal("1000"), places=4),
        ),
        min_size=1,
        max_size=6,
    )
)
def test_replay_reproduces_cached_cash_at_crypto_precision(db_conn, fills) -> None:
    """The 2dp-price/whole-share property test above can never generate the
    >4-decimal-place notional a fractional crypto fill produces; this
    widens the same invariant to quantity and price precision that
    actually appears in the crypto path (`NUMERIC(18,8)` quantity against
    `NUMERIC(18,4)` price). `max_examples` stays modest since each example
    performs up to six real fills against the database."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    for qty, price in fills:
        o = make_order(db_conn, pid, side=Side.BUY, quantity=qty)
        apply_fill(db_conn, o, decision_at(price, quantity=qty), simple_charges())

    cached_cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == _positions(db_conn, pid)
