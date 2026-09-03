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
