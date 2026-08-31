"""The Indian cost model: dated charge schedules and pure charge computation.

`compute_charges` is the correctness heart of the paper-trading
sub-project -- Phase 3's backtest engine calls it over millions of fills,
so these tests exercise every rounding, capping, side-filtering, and
GST-base rule the brief specifies rather than merely restating the
implementation.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from psycopg import Connection

from trading.paper.charges import (
    MissingChargeSchedule,
    compute_charges,
    load_schedules,
)
from trading.paper.enums import (
    ChargeBasis,
    ChargeType,
    Product,
    Rounding,
    Side,
)
from trading.paper.models import ChargeSchedule


def _sched(
    charge_type,
    basis,
    side,
    rate,
    *,
    cap=None,
    rounding=Rounding.TWO_DECIMALS,
    gst_base=(),
    product=Product.DELIVERY,
):
    return ChargeSchedule(
        broker="UPSTOX",
        exchange="NSE",
        asset_class="EQUITY",
        product=product,
        charge_type=charge_type,
        basis=basis,
        applies_to_side=side,
        rate=Decimal(rate),
        cap=Decimal(cap) if cap else None,
        rounding=rounding,
        gst_base_types=gst_base,
        effective_from=date(2024, 10, 1),
        effective_to=None,
        source_note="test",
    )


def test_percent_of_turnover_charge() -> None:
    s = [
        _sched(
            ChargeType.STT,
            ChargeBasis.PERCENT_OF_TURNOVER,
            "BOTH",
            "0.001",
            rounding=Rounding.NEAREST_RUPEE,
        )
    ]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    # 100 * 1310.50 = 131050 turnover; 0.1% = 131.05 -> nearest rupee = 131
    assert b.stt == Decimal("131")


def test_flat_per_order_charge() -> None:
    s = [_sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "20")]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert b.brokerage == Decimal("20.00")


def test_percentage_brokerage_is_capped() -> None:
    """Upstox intraday: Rs 20 or 0.1%, whichever is LOWER."""
    s = [
        _sched(
            ChargeType.BROKERAGE,
            ChargeBasis.PERCENT_OF_TURNOVER,
            "BOTH",
            "0.001",
            cap="20",
            product=Product.INTRADAY,
        )
    ]
    big = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert big.brokerage == Decimal("20.00")  # 0.1% = 131.05, capped
    small = compute_charges(s, Side.BUY, Decimal("1"), Decimal("500"))
    assert small.brokerage == Decimal("0.50")  # 0.1% of 500, under the cap


def test_side_specific_charge_skips_wrong_side() -> None:
    """Stamp duty is buy-side only; intraday STT is sell-side only."""
    s = [_sched(ChargeType.STAMP_DUTY, ChargeBasis.PERCENT_OF_TURNOVER, "BUY", "0.00015")]
    assert compute_charges(s, Side.BUY, Decimal("10"), Decimal("100")).stamp_duty > 0
    assert compute_charges(s, Side.SELL, Decimal("10"), Decimal("100")).stamp_duty == 0


def test_gst_base_excludes_stt_and_stamp_duty() -> None:
    """The classic silent error: GST is levied on brokerage + transaction +
    demat + IPFT, never on STT or stamp duty."""
    s = [
        _sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "20"),
        _sched(ChargeType.STT, ChargeBasis.PERCENT_OF_TURNOVER, "BOTH", "0.001"),
        _sched(ChargeType.STAMP_DUTY, ChargeBasis.PERCENT_OF_TURNOVER, "BUY", "0.00015"),
        _sched(
            ChargeType.GST,
            ChargeBasis.PERCENT_OF_CHARGES,
            "BOTH",
            "0.18",
            gst_base=(ChargeType.BROKERAGE,),
        ),
    ]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1000"))
    assert b.gst == Decimal("3.60")  # 18% of brokerage 20 only


def test_dp_charge_is_flat_and_sell_side_only() -> None:
    s = [_sched(ChargeType.DP_CHARGES, ChargeBasis.FLAT_PER_SCRIP_PER_DAY, "SELL", "20")]
    sell = compute_charges(s, Side.SELL, Decimal("5"), Decimal("100"))
    buy = compute_charges(s, Side.BUY, Decimal("5"), Decimal("100"))
    assert sell.dp_charges == Decimal("20.00")
    assert buy.dp_charges == Decimal("0")


def test_empty_schedule_list_raises_rather_than_returning_zero() -> None:
    """A missing schedule must fail loudly. Treating it as zero yields a
    P&L that looks fine and is systematically optimistic."""
    with pytest.raises(MissingChargeSchedule):
        compute_charges([], Side.BUY, Decimal("10"), Decimal("100"))


def test_rounding_uses_half_up_not_banker_rounding() -> None:
    """Python's default Decimal rounding is ROUND_HALF_EVEN, which would
    round 0.125 down to 0.12. Brokers use ROUND_HALF_UP, so an exact-half
    case must round up to 0.13."""
    s = [_sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "0.125")]
    b = compute_charges(s, Side.BUY, Decimal("1"), Decimal("1"))
    assert b.brokerage == Decimal("0.13")


def test_nearest_rupee_rounds_half_up_too() -> None:
    """0.5 must round up to 1, not down to the even 0 that banker's
    rounding would produce."""
    s = [
        _sched(
            ChargeType.STT,
            ChargeBasis.PERCENT_OF_TURNOVER,
            "BOTH",
            "0.005",
            rounding=Rounding.NEAREST_RUPEE,
        )
    ]
    b = compute_charges(s, Side.BUY, Decimal("1"), Decimal("100"))
    # turnover 100 * 0.005 = 0.5 -> nearest rupee half-up = 1
    assert b.stt == Decimal("1")


def test_gst_schedule_row_itself_respects_side() -> None:
    """A GST schedule row scoped to one side (e.g. a broker that only
    levies GST on sell-side charges) must contribute zero on the other
    side, exactly like any other charge type."""
    s = [
        _sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "20"),
        _sched(
            ChargeType.GST,
            ChargeBasis.PERCENT_OF_CHARGES,
            "SELL",
            "0.18",
            gst_base=(ChargeType.BROKERAGE,),
        ),
    ]
    assert compute_charges(s, Side.BUY, Decimal("1"), Decimal("100")).gst == Decimal("0")
    assert compute_charges(s, Side.SELL, Decimal("1"), Decimal("100")).gst == Decimal("3.60")


@pytest.mark.db
def test_load_schedules_picks_the_regime_in_force_on_that_date(db_conn: Connection) -> None:
    """NSE transaction charges moved 0.00297% -> 0.00307% on 2026-03-01."""
    before = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY, date(2026, 2, 28))
    after = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY, date(2026, 3, 1))
    rate_before = next(s.rate for s in before if s.charge_type == ChargeType.EXCHANGE_TXN)
    rate_after = next(s.rate for s in after if s.charge_type == ChargeType.EXCHANGE_TXN)
    assert rate_before == Decimal("0.0000297")
    assert rate_after == Decimal("0.0000307")


@pytest.mark.db
def test_load_schedules_returns_exactly_one_row_per_charge_type(db_conn: Connection) -> None:
    """Overlapping date ranges would double-charge; the loader must never
    return two rows of the same charge type for one date."""
    got = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY, date(2026, 6, 1))
    types = [s.charge_type for s in got]
    assert len(types) == len(set(types)), f"duplicate charge types: {types}"


@pytest.mark.db
def test_load_schedules_splits_comma_separated_gst_base_types(db_conn: Connection) -> None:
    """The DB column is comma-separated text; the loader must split it
    into the tuple[ChargeType, ...] the model declares, in order."""
    got = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY, date(2026, 6, 1))
    gst = next(s for s in got if s.charge_type == ChargeType.GST)
    assert gst.gst_base_types == (
        ChargeType.BROKERAGE,
        ChargeType.EXCHANGE_TXN,
        ChargeType.DP_CHARGES,
        ChargeType.IPFT,
    )


@pytest.mark.db
def test_load_schedules_raises_missing_when_nothing_covers_the_query(
    db_conn: Connection,
) -> None:
    """An unknown broker has no rows at all; the empty list load_schedules
    returns must feed straight into compute_charges's MissingChargeSchedule,
    never be silently treated as zero cost upstream."""
    got = load_schedules(
        db_conn, "NOSUCHBROKER", "NSE", "EQUITY", Product.DELIVERY, date(2026, 6, 1)
    )
    assert got == []
    with pytest.raises(MissingChargeSchedule):
        compute_charges(got, Side.BUY, Decimal("1"), Decimal("100"))
