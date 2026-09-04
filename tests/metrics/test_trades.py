"""Round trips and cost drag, against answers computed by hand."""

from __future__ import annotations

from decimal import Decimal


def fill(side, qty, price, charges, *, instrument="1", ts="2024-01-01T10:00:00+00:00"):  # noqa: ANN001, ANN201
    return {
        "ts": ts,
        "instrument_id": instrument,
        "side": side,
        "product": "DELIVERY",
        "quantity": str(qty),
        "price": str(price),
        "total_charges": str(charges),
    }


def test_a_buy_then_a_sell_is_one_round_trip() -> None:
    from trading.metrics.trades import round_trips

    trips = round_trips([fill("BUY", 10, 100, 20), fill("SELL", 10, 110, 20)])
    assert len(trips) == 1
    trip = trips[0]
    assert trip.quantity == Decimal("10")
    # Gross is price movement alone; net carries both legs' charges.
    assert trip.gross_pnl == Decimal("100")
    assert trip.charges == Decimal("40")
    assert trip.net_pnl == Decimal("60")


def test_a_partly_closed_position_splits_the_entry_charges_proportionally() -> None:
    """A buy of 100 closed 60 then 40 must carry 60% and 40% of its charges.

    Attributing the whole entry charge to the first exit would make the
    first round trip look worse than it was and the second better -- and
    win rate, profit factor and expectancy all read those numbers.
    """
    from trading.metrics.trades import round_trips

    trips = round_trips(
        [
            fill("BUY", 100, 100, 100),
            fill("SELL", 60, 110, 30),
            fill("SELL", 40, 90, 20),
        ]
    )
    assert len(trips) == 2
    # 60% of the 100 entry charge, plus the 30 exit charge.
    assert trips[0].charges == Decimal("90")
    assert trips[0].gross_pnl == Decimal("600")
    # 40% of the entry, plus 20.
    assert trips[1].charges == Decimal("60")
    assert trips[1].gross_pnl == Decimal("-400")


def test_an_unclosed_position_is_an_open_trade_not_a_loss() -> None:
    """Counting it as a loss would understate win rate for every strategy
    that ends holding something -- which is most of them."""
    from trading.metrics.trades import round_trips

    trips = round_trips([fill("BUY", 10, 100, 20)])
    assert trips == []


def test_round_trips_never_cross_instruments() -> None:
    from trading.metrics.trades import round_trips

    trips = round_trips(
        [
            fill("BUY", 10, 100, 10, instrument="1"),
            fill("SELL", 10, 110, 10, instrument="2"),
        ]
    )
    assert trips == []


def test_trade_metrics_from_hand_computed_round_trips() -> None:
    """Two wins of +60 and +40 net, one loss of -50 net.

    win rate 2/3 = 0.6667; profit factor 100/50 = 2; average win 50;
    average loss -50; expectancy (2/3 * 50) + (1/3 * -50) = 16.6667.
    """
    from trading.metrics.trades import trade_metrics

    fills = [
        fill("BUY", 10, 100, 0),
        fill("SELL", 10, 106, 0),
        fill("BUY", 10, 100, 0),
        fill("SELL", 10, 104, 0),
        fill("BUY", 10, 100, 0),
        fill("SELL", 10, 95, 0),
    ]
    m = trade_metrics(fills)
    assert m["trades"] == 3
    assert m["wins"] == 2
    assert m["losses"] == 1
    assert Decimal(m["win_rate"]).quantize(Decimal("0.0001")) == Decimal("0.6667")
    assert Decimal(m["profit_factor"]).quantize(Decimal("0.01")) == Decimal("2.00")
    assert Decimal(m["average_win"]) == Decimal("50")
    assert Decimal(m["average_loss"]) == Decimal("-50")
    assert Decimal(m["expectancy"]).quantize(Decimal("0.0001")) == Decimal("16.6667")


def test_cost_drag_is_charges_against_the_gross_result() -> None:
    """The number this platform exists to show honestly: how much of the
    edge the Indian cost stack ate."""
    from trading.metrics.trades import cost_drag

    fills = [fill("BUY", 10, 100, 20), fill("SELL", 10, 110, 20)]
    drag = cost_drag(fills)
    assert Decimal(drag["total_charges"]) == Decimal("40")
    assert Decimal(drag["gross_pnl"]) == Decimal("100")
    assert Decimal(drag["net_pnl"]) == Decimal("60")
    # 40 of a 100 gross result went to costs.
    assert Decimal(drag["drag"]).quantize(Decimal("0.0001")) == Decimal("0.4000")


def test_cost_drag_is_undefined_rather_than_infinite_on_a_zero_gross() -> None:
    """A strategy that made nothing gross has no edge for costs to eat a
    share of. Reporting 1.0, or infinity, would both be claims."""
    from trading.metrics.trades import cost_drag

    fills = [fill("BUY", 10, 100, 20), fill("SELL", 10, 100, 20)]
    assert cost_drag(fills)["drag"] is None


def test_no_fills_yields_no_trade_metrics() -> None:
    from trading.metrics.trades import cost_drag, trade_metrics

    assert trade_metrics([])["trades"] == 0
    assert cost_drag([])["drag"] is None


def test_drag_is_undefined_when_the_gross_result_was_a_loss() -> None:
    """Found by a real run, not by a fixture.

    A five-session churn over six years produced a gross loss of 12,313 and
    charges of 68,227. `charges / gross` is then -5.54, and "costs took
    -554% of the gross result" is not a sentence. "What share of the edge
    did costs eat" has no answer when there was no edge, so the ratio is
    undefined and the caller is left to say the true thing instead: costs
    turned a small gross loss into a large net one.

    The original guard only caught `gross == 0`, which a hand-written
    fixture with a positive gross would never have exposed.
    """
    from trading.metrics.trades import cost_drag

    losing = [
        fill("BUY", 10, 100, 500),
        fill("SELL", 10, 90, 500),
    ]
    drag = cost_drag(losing)
    assert Decimal(drag["gross_pnl"]) == Decimal("-100")
    assert Decimal(drag["total_charges"]) == Decimal("1000")
    assert Decimal(drag["net_pnl"]) == Decimal("-1100")
    assert drag["drag"] is None
