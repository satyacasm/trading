"""Where the ATM anchor comes from, and how loudly it says how old it is."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from trading.recorder.universe import Anchor, anchor_from_futures


def _seed_future(db_conn, symbol: str, expiry: str, ts: str, close: str) -> None:
    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency,"
        " status, canonical_key, expiry) VALUES"
        " ('FUTURE','NSE','FO',%s,'INR','ACTIVE',%s,%s)"
        " ON CONFLICT (canonical_key) DO NOTHING",
        (symbol, f"NSE:FO:{symbol}:{expiry}:FUT", expiry),
    )
    iid = db_conn.execute(
        "SELECT instrument_id FROM instruments WHERE canonical_key = %s",
        (f"NSE:FO:{symbol}:{expiry}:FUT",),
    ).fetchone()[0]
    db_conn.execute(
        "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, volume, source)"
        " VALUES (%s,%s,%s,%s,%s,%s,0,2) ON CONFLICT DO NOTHING",
        (iid, ts, close, close, close, close),
    )


def test_it_uses_the_nearest_unexpired_future_as_the_anchor(db_conn) -> None:
    """The index itself is not in our instrument master, and the near
    future's close is the best proxy we hold: its basis to spot is small
    next to the strike window the recorder subscribes to."""
    _seed_future(db_conn, "ANCHORIDX", "2026-09-29", "2026-09-04T10:00:00+00:00", "24391.10")
    # An expired contract with a later close must not win: it stopped
    # tracking spot the day it settled.
    _seed_future(db_conn, "ANCHORIDX", "2026-08-27", "2026-09-05T10:00:00+00:00", "99999.00")

    anchor = anchor_from_futures(db_conn, "ANCHORIDX", on=date(2026, 9, 7))
    assert isinstance(anchor, Anchor)
    assert anchor.price == Decimal("24391.1000")
    assert anchor.as_of == date(2026, 9, 4)


def test_an_underlying_with_no_futures_history_is_an_error(db_conn) -> None:
    with pytest.raises(LookupError, match="NOTANINDEX"):
        anchor_from_futures(db_conn, "NOTANINDEX", on=date(2026, 9, 7))


def test_the_anchor_reports_its_own_staleness() -> None:
    """A recorder that opened its window around a two-week-old close should
    say so in its log, not discover it in a backtest six months later."""
    anchor = Anchor(price=Decimal("24391.10"), as_of=date(2026, 8, 21))
    assert anchor.age_days(date(2026, 9, 7)) == 17
    assert anchor.is_stale(date(2026, 9, 7), tolerance_days=5) is True
    assert anchor.is_stale(date(2026, 8, 24), tolerance_days=5) is False


def test_the_index_close_is_preferred_to_the_futures_proxy() -> None:
    """The near future's basis is small but its close is only as fresh as
    EOD ingestion, which had stopped for two weeks the day this was
    written -- the anchor was 493 points off, enough to leave a 20-strike
    window covering only 10 strikes below the money."""
    import httpx

    from trading.recorder.universe import anchor_from_index

    def handler(request: httpx.Request) -> httpx.Response:
        assert "NSE_INDEX%7CNifty%2050" in str(request.url) or "Nifty 50" in str(request.url)
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "candles": [
                        ["2026-09-04T00:00:00+05:30", 23910.9, 24005.75, 23895.85, 23897.7, 0, 0],
                        ["2026-09-03T00:00:00+05:30", 23997.95, 24025.4, 23873.45, 23873.45, 0, 0],
                    ]
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        anchor = anchor_from_index(
            client,
            "NSE_INDEX|Nifty 50",
            token="t",
            on=date(2026, 9, 5),  # noqa: S106
        )
    # The close of the most recent candle, and the day it belongs to.
    assert anchor.price == Decimal("23897.7")
    assert anchor.as_of == date(2026, 9, 4)


def test_an_index_with_no_candles_is_an_error_so_the_caller_can_fall_back() -> None:
    import httpx

    from trading.recorder.universe import anchor_from_index

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "success", "data": {"candles": []}})

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(LookupError, match="Nifty 50"),
    ):
        anchor_from_index(
            client,
            "NSE_INDEX|Nifty 50",
            token="t",  # noqa: S106
            on=date(2026, 9, 5),
        )
