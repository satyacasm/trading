"""Writing perpetual history into the bar and funding tables."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trading.sources.binance_futures import FundingSettlement, PerpBar
from trading.streaming.perp_backfill import resume_from, write_bars, write_funding

_GENESIS_MS = 1567900800000  # 2019-09-08, the first perpetual kline


def _perp(db_conn) -> int:
    from datetime import date

    from trading.sources.binance_futures import PerpContractSpec
    from trading.streaming.seed_perp_instruments import seed_perp_instruments

    spec = PerpContractSpec(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("50"),
        liquidation_fee=Decimal("0.0125"),
    )
    return seed_perp_instruments(db_conn, [spec], on=date(2026, 9, 5))["BTC-USDT"]


def _bar(day: int, close: str = "10391.63") -> PerpBar:
    return PerpBar(
        ts=datetime(2019, 9, day, tzinfo=UTC),
        open=Decimal("10000"),
        high=Decimal("10412.65"),
        low=Decimal("10000"),
        close=Decimal(close),
        volume=Decimal("3096.291"),
        quote_volume=Decimal("32096280.44997"),
        trades=3754,
    )


def test_bars_land_in_bars_daily_with_the_futures_provenance(db_conn) -> None:
    iid = _perp(db_conn)
    assert write_bars(db_conn, iid, [_bar(8)]) == 1
    row = db_conn.execute(
        "SELECT ts, open, close, volume, trades, source FROM bars_daily WHERE instrument_id=%s",
        (iid,),
    ).fetchone()
    assert row[0] == datetime(2019, 9, 8, tzinfo=UTC)
    assert row[2] == Decimal("10391.6300")
    assert row[4] == 3754
    # 9 = BINANCE_FUTURES_KLINE. Provenance distinguishes a perp bar from
    # the spot bar of the same pair, which is a different price series.
    assert row[5] == 9


def test_rewriting_a_bar_updates_rather_than_duplicating(db_conn) -> None:
    """Binance revises a kline while its interval is still open, so a
    backfill re-run over today must correct the row, not collide."""
    iid = _perp(db_conn)
    write_bars(db_conn, iid, [_bar(8, close="10391.63")])
    # Inside the bar's own high/low: `ck_ohlc_order` refuses a revision
    # that is not internally consistent, which is the right answer and is
    # how this test was first written wrong.
    write_bars(db_conn, iid, [_bar(8, close="10400.00")])
    rows = db_conn.execute("SELECT close FROM bars_daily WHERE instrument_id=%s", (iid,)).fetchall()
    assert rows == [(Decimal("10400.0000"),)]


def test_funding_settlements_are_written_and_are_idempotent(db_conn) -> None:
    iid = _perp(db_conn)
    settlements = [
        FundingSettlement(
            "BTCUSDT", datetime(2026, 9, 4, 8, tzinfo=UTC), Decimal("0.00006498"), Decimal("80628")
        ),
        FundingSettlement("BTCUSDT", datetime(2019, 9, 10, 8, tzinfo=UTC), Decimal("0.0001"), None),
    ]
    assert write_funding(db_conn, iid, settlements) == 2
    assert write_funding(db_conn, iid, settlements) == 2
    rows = db_conn.execute(
        "SELECT funding_time, rate, mark_price FROM perp_funding"
        " WHERE instrument_id=%s ORDER BY funding_time",
        (iid,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][2] is None
    assert rows[1][1] == Decimal("0.000064980000")


def test_a_backfill_resumes_from_the_last_row_it_wrote(db_conn) -> None:
    """Seven years at 500 rows a page is a lot of requests to repeat every
    time. Resuming from the newest row already stored makes a re-run cost
    one page instead of sixteen."""
    iid = _perp(db_conn)
    assert resume_from(db_conn, iid, "perp_funding", "funding_time", _GENESIS_MS) == _GENESIS_MS

    write_funding(
        db_conn,
        iid,
        [
            FundingSettlement(
                "BTCUSDT", datetime(2026, 9, 4, 8, tzinfo=UTC), Decimal("0.0001"), None
            )
        ],
    )
    resumed = resume_from(db_conn, iid, "perp_funding", "funding_time", _GENESIS_MS)
    # One millisecond past the stored row, so the walk never re-reads it.
    assert resumed == int(datetime(2026, 9, 4, 8, tzinfo=UTC).timestamp() * 1000) + 1
