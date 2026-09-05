from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.runtime.provider import BarRecord, InMemoryBars


def _bar(instrument_id: int, minute: int, close: str) -> BarRecord:
    return BarRecord(
        instrument_id=instrument_id,
        ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
        interval_sec=60,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("100"),
        trades=None,
        open_interest=None,
        oi_change=None,
    )


def test_close_ts_is_the_end_of_the_interval() -> None:
    bar = _bar(1, 15, "100")
    assert bar.close_ts == bar.ts + timedelta(seconds=60)


def test_groups_are_ordered_and_merge_instruments_printing_together() -> None:
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11")], 2: [_bar(2, 1, "20")]})
    groups = list(bars.groups())
    assert [ts for ts, _ in groups] == [
        datetime(2026, 9, 1, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 9, 2, tzinfo=UTC),
    ]
    assert [b.instrument_id for b in groups[0][1]] == [1]
    assert [b.instrument_id for b in groups[1][1]] == [1, 2]


def test_ties_break_by_instrument_id_so_ordering_is_total() -> None:
    # Determinism (D-S6) is only checkable if the merge order is total.
    bars = InMemoryBars({9: [_bar(9, 0, "1")], 2: [_bar(2, 0, "2")], 5: [_bar(5, 0, "3")]})
    _, group = next(iter(bars.groups()))
    assert [b.instrument_id for b in group] == [2, 5, 9]


def test_history_never_includes_the_bar_being_processed() -> None:
    # The anti-lookahead guarantee of contract §4, at the data layer.
    bars = InMemoryBars({1: [_bar(1, 0, "10"), _bar(1, 1, "11"), _bar(1, 2, "12")]})
    assert [b.close for b in bars.history(1, 0)] == []
    assert [b.close for b in bars.history(1, 2)] == [Decimal("10"), Decimal("11")]


def test_a_bar_can_be_appended_and_becomes_readable_history() -> None:
    """Live runs learn their bars one at a time.

    The backtest builds the whole series up front and never changes it; a
    forward run cannot, and `ctx.data.bars()` has to see each bar once the
    step that dispatched it is done. `append` returns the bar's index in its
    own series, which is exactly what the loop's cursor advances to.
    """
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.runtime.provider import BarRecord, InMemoryBars

    def bar(minute: int) -> BarRecord:
        return BarRecord(
            instrument_id=1,
            ts=datetime(2026, 9, 4, 10, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal(100 + minute),
        )

    store = InMemoryBars({})
    first, index = store.append(bar(0))
    assert index == 0
    assert store.instruments() == (1,)
    # Strictly before the cursor: the bar just dispatched is not history
    # until the step that dispatched it has finished.
    assert store.history(1, 0) == ()
    assert store.history(1, 1) == (first,)

    _second, index2 = store.append(bar(1))
    assert index2 == 1
    assert len(store.history(1, 2)) == 2


def test_a_late_bar_is_refused_rather_than_silently_reordered() -> None:
    """A bar arriving out of order would corrupt the history a strategy has
    already read -- it would change the past. Refusing is the only honest
    option: the alternative is a strategy whose lookback silently differs
    from what it saw a moment ago.
    """
    from datetime import UTC, datetime
    from decimal import Decimal

    import pytest as _pytest

    from trading.runtime.provider import BarRecord, InMemoryBars

    def bar(minute: int) -> BarRecord:
        return BarRecord(
            instrument_id=1,
            ts=datetime(2026, 9, 4, 10, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
        )

    store = InMemoryBars({})
    store.append(bar(5))
    with _pytest.raises(ValueError, match="out of order"):
        store.append(bar(4))


def _bar_at(minute: int, close: str = "1") -> object:
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.runtime.provider import BarRecord

    return BarRecord(
        instrument_id=1,
        ts=datetime(2026, 9, 4, 10, minute, tzinfo=UTC),
        interval_sec=60,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal(close),
    )


def test_an_identical_redelivered_bar_is_ignored_rather_than_refused() -> None:
    """A bar that repeats one already appended changes nothing, so refusing
    it kills a live run over a non-event. This is not hypothetical: a late
    tick reopened an already-flushed minute upstream, the same bar was
    published twice, and a run that had been trading for hours died on the
    second copy.

    `None` means "already known" -- distinct from a bar that was appended,
    which the caller must dispatch.
    """
    from trading.runtime.provider import InMemoryBars

    store = InMemoryBars({})
    store.append(_bar_at(5))
    assert store.append(_bar_at(5)) is None
    # Ignored, not appended twice: the strategy's lookback must be the same
    # length whether or not the transport hiccuped.
    assert len(store.history(1, 2)) == 1


def test_the_same_minute_with_different_prices_is_still_refused() -> None:
    """Two different bars claiming the same minute is a feed disagreeing
    with itself, not a redelivery, and silently keeping the first would
    hide it."""
    import pytest as _pytest

    from trading.runtime.provider import InMemoryBars

    store = InMemoryBars({})
    store.append(_bar_at(5, close="1"))
    with _pytest.raises(ValueError, match="disagrees"):
        store.append(_bar_at(5, close="2"))
