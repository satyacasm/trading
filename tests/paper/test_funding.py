"""Funding: the carry that makes a perpetual track spot without an expiry.

The sign convention is the whole risk here. Get it backwards and every
carry strategy backtests as the exact opposite of what it is.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from trading.paper.funding import (
    SETTLEMENT_HOURS,
    funding_payment,
    settlements_between,
)

D = Decimal


def test_a_long_pays_when_the_rate_is_positive() -> None:
    """The normal state of a bull market: longs pay shorts to hold the
    perpetual above spot. Positive means the holder pays."""
    assert funding_payment(D("1"), mark=D("80000"), rate=D("0.0001")) == D("8")


def test_a_short_receives_when_the_rate_is_positive() -> None:
    """The other side of the same transfer. This is the income a carry
    strategy exists to collect, and modelling funding as a fee -- always a
    cost -- would erase it."""
    assert funding_payment(D("-1"), mark=D("80000"), rate=D("0.0001")) == D("-8")


def test_the_flow_reverses_when_the_rate_goes_negative() -> None:
    """Negative funding is not an edge case; it is what a bear market looks
    like, and a long collects through it."""
    assert funding_payment(D("1"), mark=D("80000"), rate=D("-0.0001")) == D("-8")
    assert funding_payment(D("-1"), mark=D("80000"), rate=D("-0.0001")) == D("8")


def test_a_flat_position_pays_and_receives_nothing() -> None:
    assert funding_payment(D("0"), mark=D("80000"), rate=D("0.0001")) == D("0")


def test_it_scales_with_notional_not_with_margin() -> None:
    """Funding is charged on the whole exposure, not on what was posted to
    hold it. This is why high leverage compounds a carry cost: the same
    margin carries a much larger funding bill."""
    assert funding_payment(D("10"), mark=D("80000"), rate=D("0.0001")) == D("80")


def test_settlements_land_on_the_eight_hour_boundaries() -> None:
    found = settlements_between(
        datetime(2026, 9, 5, 7, 0, tzinfo=UTC), datetime(2026, 9, 6, 1, 0, tzinfo=UTC)
    )
    assert found == [
        datetime(2026, 9, 5, 8, 0, tzinfo=UTC),
        datetime(2026, 9, 5, 16, 0, tzinfo=UTC),
        datetime(2026, 9, 6, 0, 0, tzinfo=UTC),
    ]
    assert SETTLEMENT_HOURS == (0, 8, 16)


def test_a_window_inside_one_interval_settles_nothing() -> None:
    assert (
        settlements_between(
            datetime(2026, 9, 5, 9, 0, tzinfo=UTC), datetime(2026, 9, 5, 15, 59, tzinfo=UTC)
        )
        == []
    )


def test_the_boundary_itself_settles_once_not_twice() -> None:
    """A poll landing exactly on 08:00 must not settle the same boundary
    that the previous poll's window already closed. Half-open `(since,
    until]` is what makes a sequence of adjacent windows partition the
    timeline instead of overlapping at every edge."""
    boundary = datetime(2026, 9, 5, 8, 0, tzinfo=UTC)
    first = settlements_between(boundary - timedelta(hours=1), boundary)
    second = settlements_between(boundary, boundary + timedelta(hours=1))
    assert first == [boundary]
    assert second == []


def test_a_backwards_window_is_refused() -> None:
    """A clock that went backwards is a bug worth hearing about, not a
    reason to silently settle nothing."""
    with pytest.raises(ValueError, match="backwards"):
        settlements_between(
            datetime(2026, 9, 5, 8, 0, tzinfo=UTC), datetime(2026, 9, 5, 7, 0, tzinfo=UTC)
        )


def _perp_with_position(db_conn, quantity: str, cash: str = "100000"):
    from datetime import date
    from uuid import uuid4

    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    instrument_id = seed_perp_instruments(
        db_conn,
        [
            PerpContractSpec(
                "BTCUSDT", "BTC", "USDT", D("0.10"), D("0.001"), D("0.001"), D("50"), D("0.0125")
            )
        ],
        on=date(2026, 9, 6),
    )["BTC-USDT"]
    db_conn.execute(
        "INSERT INTO users (user_id, email) VALUES (902, 'funding@test') ON CONFLICT DO NOTHING"
    )
    portfolio_id = db_conn.execute(
        "INSERT INTO portfolios (user_id, name, base_currency, initial_capital, cash_balance,"
        " status) VALUES (902, %s, 'USDT', %s, %s, 'ACTIVE') RETURNING portfolio_id",
        (f"funding-{uuid4()}", cash, cash),
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO perp_positions (portfolio_id, instrument_id, quantity, entry_price,"
        " leverage, reserved_margin) VALUES (%s,%s,%s,80000,10,8000)",
        (portfolio_id, instrument_id, quantity),
    )
    return instrument_id, portfolio_id


def test_a_long_held_across_a_settlement_pays_and_the_ledger_says_so(db_conn) -> None:
    """Task 4's demo. The payment leaves cash, accrues on the position, and
    lands as a FUNDING row -- because a P&L that accrues silently three
    times a day is exactly the kind nobody can explain later."""
    from trading.paper.funding import settle_funding

    instrument_id, portfolio_id = _perp_with_position(db_conn, "1")
    at = datetime(2026, 9, 5, 8, 0, tzinfo=UTC)

    result = settle_funding(db_conn, at, {instrument_id: (D("0.0001"), D("80000"))})

    assert result.positions == 1
    assert result.total_paid == D("8.0000")
    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert cash == D("99992.0000")

    entry_type, amount = db_conn.execute(
        "SELECT entry_type, amount FROM ledger_entries WHERE portfolio_id = %s"
        " ORDER BY entry_id DESC LIMIT 1",
        (portfolio_id,),
    ).fetchone()
    assert entry_type == "FUNDING"
    # Negative: it left the account. The ledger's sign is the direction of
    # cash, not the direction of the transfer.
    assert amount == D("-8.0000")

    accrued = db_conn.execute(
        "SELECT funding_paid FROM perp_positions WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert accrued == D("8.00000000")


def test_a_short_held_across_the_same_settlement_is_paid(db_conn) -> None:
    from trading.paper.funding import settle_funding

    instrument_id, portfolio_id = _perp_with_position(db_conn, "-1")
    settle_funding(
        db_conn, datetime(2026, 9, 5, 8, 0, tzinfo=UTC), {instrument_id: (D("0.0001"), D("80000"))}
    )
    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()[0]
    assert cash == D("100008.0000")


def test_a_position_whose_rate_is_unknown_is_skipped_not_settled_at_zero(db_conn) -> None:
    """Settling at zero is indistinguishable in the ledger from a genuine
    zero-rate settlement, and the difference matters when someone asks why
    a carry strategy earned less than the funding series says."""
    from trading.paper.funding import settle_funding

    _instrument_id, portfolio_id = _perp_with_position(db_conn, "1")
    result = settle_funding(db_conn, datetime(2026, 9, 5, 8, 0, tzinfo=UTC), {})

    assert result.positions == 0
    assert (
        db_conn.execute(
            "SELECT count(*) FROM ledger_entries WHERE portfolio_id = %s AND entry_type='FUNDING'",
            (portfolio_id,),
        ).fetchone()[0]
        == 0
    )
