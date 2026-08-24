"""Tests for `trading.corpactions.backfill` -- walking NSE's corporate-actions
endpoint across a decade.

The continuity check found 5,672 price moves beyond 20% that nothing
explains, because `corporate_actions` was empty: Task 16 built the parser and
the read-time adjustment but never a way to actually fetch history. NSE's
endpoint accepts from_date/to_date (verified live: 2,208 records for 2020,
388 for Q1 2016), so the history is reachable a window at a time.
"""

from __future__ import annotations

from datetime import date

import pytest

from trading.corpactions.backfill import backfill_corporate_actions, date_windows


def test_windows_split_a_decade_into_whole_years():
    windows = date_windows(date(2016, 1, 1), date(2018, 12, 31))
    assert windows == [
        (date(2016, 1, 1), date(2016, 12, 31)),
        (date(2017, 1, 1), date(2017, 12, 31)),
        (date(2018, 1, 1), date(2018, 12, 31)),
    ]


def test_the_last_window_stops_at_the_requested_end():
    windows = date_windows(date(2026, 1, 1), date(2026, 8, 21))
    assert windows == [(date(2026, 1, 1), date(2026, 8, 21))]


def test_a_range_inside_one_year_is_a_single_window():
    assert date_windows(date(2020, 3, 1), date(2020, 6, 30)) == [
        (date(2020, 3, 1), date(2020, 6, 30))
    ]


def test_an_inverted_range_yields_nothing():
    assert date_windows(date(2020, 6, 1), date(2020, 1, 1)) == []


def test_backfill_ingests_every_window_and_totals_the_rows(db_conn):
    """`fetch` is injected so this exercises the real parse and the real
    upsert against the database, with no network call."""
    payloads = {
        (date(2016, 1, 1), date(2016, 12, 31)): (
            b'[{"symbol":"CABTESTA","subject":"Bonus 1:2","exDate":"15-Jun-2016",'
            b'"caBroadcastDate":null}]'
        ),
        (date(2017, 1, 1), date(2017, 12, 31)): (
            b'[{"symbol":"CABTESTB","subject":"Dividend - Rs 2 Per Share",'
            b'"exDate":"20-Jul-2017","caBroadcastDate":null},'
            b'{"symbol":"CABTESTC","subject":"Buy Back",'
            b'"exDate":"21-Jul-2017","caBroadcastDate":null}]'
        ),
    }
    calls: list[tuple[date, date]] = []

    def fetch(start: date, end: date) -> bytes | None:
        calls.append((start, end))
        return payloads[(start, end)]

    result = backfill_corporate_actions(db_conn, fetch, date(2016, 1, 1), date(2017, 12, 31))

    assert calls == [
        (date(2016, 1, 1), date(2016, 12, 31)),
        (date(2017, 1, 1), date(2017, 12, 31)),
    ]
    assert result.ingested == 2  # the bonus and the dividend
    # A buy-back is a tender offer with no ex-date price adjustment, so it is
    # counted as unrecognised rather than recorded.
    assert result.skipped == 1
    stored = db_conn.execute(
        "SELECT count(*) FROM corporate_actions ca JOIN instruments i USING (instrument_id) "
        "WHERE i.symbol LIKE 'CABTEST%'"
    ).fetchone()
    assert stored is not None and stored[0] == 2


def test_backfill_is_idempotent(db_conn):
    payload = (
        b'[{"symbol":"CABIDEM","subject":"Bonus 1:1","exDate":"15-Jun-2016",'
        b'"caBroadcastDate":null}]'
    )

    def fetch(start: date, end: date) -> bytes | None:
        return payload

    first = backfill_corporate_actions(db_conn, fetch, date(2016, 1, 1), date(2016, 12, 31))
    second = backfill_corporate_actions(db_conn, fetch, date(2016, 1, 1), date(2016, 12, 31))

    assert first.ingested == 1
    stored = db_conn.execute(
        "SELECT count(*) FROM corporate_actions ca JOIN instruments i USING (instrument_id) "
        "WHERE i.symbol = 'CABIDEM'"
    ).fetchone()
    assert stored is not None and stored[0] == 1, "a re-run must upsert, not duplicate"
    assert second.windows_fetched == 1


def test_a_window_that_returns_nothing_is_counted_not_fatal(db_conn):
    def fetch(start: date, end: date) -> bytes | None:
        return None

    result = backfill_corporate_actions(db_conn, fetch, date(2016, 1, 1), date(2016, 12, 31))

    assert result.windows_failed == 1
    assert result.ingested == 0


pytestmark = pytest.mark.db
