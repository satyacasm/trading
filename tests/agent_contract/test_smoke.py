"""Tests for stage 2 of the upload pipeline: the smoke run."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.agent_contract.smoke import SmokeVerdict, build_verdict  # noqa: F401
from trading.config import get_settings


def _outcome(**overrides) -> dict:  # noqa: ANN003
    base = {
        "ok": True,
        "bar_calls": 1875,
        "orders": [],
        "fills": 0,
        "rejections": [],
        "final_cash": "100000",
        "final_equity": "100000",
        "breaker_reason": None,
        "logs": [],
        "error": None,
        "crashed_at": None,
    }
    base.update(overrides)
    return base


_WINDOW = {"start": "2026-08-27T00:00:00+00:00", "end": "2026-09-02T00:00:00+00:00", "sessions": 5}


def test_a_quiet_strategy_passes_with_a_warning() -> None:
    verdict = build_verdict(_outcome(), _outcome(), _WINDOW, "runc", False)
    assert verdict.passed is True
    assert verdict.warnings_only is True
    assert [f.code for f in verdict.report.findings] == ["NO_ORDERS"]
    assert "1,875" in verdict.as_agent_feedback()
    assert "PASSED WITH WARNINGS" in verdict.as_agent_feedback()


def test_a_crash_fails() -> None:
    crashed = _outcome(
        ok=False, error="ValueError: boom", crashed_at={"handler": "on_bar", "ts": "x"}
    )
    verdict = build_verdict(crashed, crashed, _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "SMOKE_CRASH" in verdict.as_agent_feedback()


def test_differing_order_sequences_fail_as_nondeterministic() -> None:
    first = _outcome(orders=[{"order_id": 1, "submitted_at": "09:31"}])
    second = _outcome(orders=[{"order_id": 1, "submitted_at": "14:22"}])
    verdict = build_verdict(first, second, _WINDOW, "runc", False)
    assert verdict.passed is False
    codes = [f.code for f in verdict.report.findings]
    assert "NONDETERMINISTIC" in codes


def test_identical_order_sequences_do_not_trip_the_determinism_check() -> None:
    orders = [{"order_id": 1, "submitted_at": "09:31"}]
    verdict = build_verdict(
        _outcome(orders=orders, fills=1), _outcome(orders=orders, fills=1), _WINDOW, "runc", False
    )
    assert [f.code for f in verdict.report.findings] == []
    assert verdict.passed is True


def test_all_orders_rejected_warns_and_names_the_reason() -> None:
    orders = [{"order_id": 1, "status": "REJECTED"}]
    outcome = _outcome(orders=orders, rejections=["insufficient funds"])
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert verdict.passed is True
    assert "insufficient funds" in verdict.as_agent_feedback()


def test_a_tripped_breaker_warns() -> None:
    outcome = _outcome(
        orders=[{"order_id": 1}],
        fills=1,
        breaker_reason="REASON_MAX_DAILY_LOSS: loss of 5000 exceeds 1000",
    )
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert verdict.passed is True
    assert "BREAKER_TRIPPED" in verdict.as_agent_feedback()


def test_the_report_records_how_well_isolated_the_run_was() -> None:
    # Carried forward from SandboxResult: a stored pass must never be
    # readable as better isolated than it was.
    verdict = build_verdict(_outcome(), _outcome(), _WINDOW, "runc", False)
    assert "host kernel is shared" in verdict.as_agent_feedback()
    gvisor = build_verdict(_outcome(), _outcome(), _WINDOW, "runsc", True)
    assert "runsc" in gvisor.as_agent_feedback()


def test_the_report_states_what_was_not_exercised() -> None:
    # decide_fill fills the full remaining quantity, so PARTIALLY_FILLED
    # never occurs. Saying so beats letting a reader infer coverage.
    verdict = build_verdict(
        _outcome(fills=1, orders=[{"order_id": 1}]),
        _outcome(fills=1, orders=[{"order_id": 1}]),
        _WINDOW,
        "runc",
        False,
    )
    assert "partial fill" in verdict.as_agent_feedback().lower()


def test_an_actually_nondeterministic_strategy_is_caught_end_to_end() -> None:
    import random
    from dataclasses import asdict
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
    from trading.paper.models import ChargeSchedule
    from trading.runtime.loop import run_loop
    from trading.runtime.provider import BarRecord, InMemoryBars

    class CoinFlipper:
        """Unseeded randomness -- the most common cause of a strategy that
        cannot be backtested, and invisible to static validation."""

        def on_bar(self, ctx, bars) -> None:  # noqa: ANN001
            if random.random() < 0.5:  # noqa: S311 - the defect under test
                ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="coin flip")

    bars = InMemoryBars(
        {
            1: [
                BarRecord(
                    instrument_id=1,
                    ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
                    interval_sec=60,
                    open=Decimal("100"),
                    high=Decimal("100"),
                    low=Decimal("100"),
                    close=Decimal("100"),
                    volume=Decimal("10"),
                )
                for minute in range(40)
            ]
        }
    )
    schedules = (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("0"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )

    def _once() -> dict:
        return asdict(
            run_loop(
                strategy=CoinFlipper(),
                bars=bars,
                schedules=schedules,
                starting_cash=Decimal("1000000"),
                slippage_bps=Decimal("0"),
            )
        )

    random.seed()  # explicitly unseeded-equivalent: fresh entropy per run
    verdict = build_verdict(_once(), _once(), _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "NONDETERMINISTIC" in [f.code for f in verdict.report.findings]


def test_a_single_run_compared_with_itself_never_flags() -> None:
    # The vacuity guard: if the pipeline were changed to run once and
    # compare the outcome with itself, the check above would silently
    # stop catching anything. This pins that a self-comparison is clean,
    # so the value of the check lives entirely in there being two runs.
    outcome = _outcome(orders=[{"order_id": 1, "submitted_at": "09:31"}], fills=1)
    verdict = build_verdict(outcome, outcome, _WINDOW, "runc", False)
    assert "NONDETERMINISTIC" not in [f.code for f in verdict.report.findings]


@pytest.mark.db
def test_select_window_finds_a_daily_only_instrument_when_asked_for_1d(db_conn) -> None:  # noqa: ANN001
    """585,261 of this database's 585,299 instruments have bars_daily rows
    and zero bars_intraday rows. select_window's default query would find
    nothing for one of them -- this is the gap Task 3's fetch_bars fix
    would otherwise ship silently inactive on.
    """
    from datetime import date

    from trading.agent_contract.smoke import select_window

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYONLY','INR',"
        "'ACTIVE','NSE:CM:DAILYONLY') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for day in (date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10)):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,101,99,100,10,1)",
            (instrument_id, day),
        )

    daily_window = select_window(db_conn, [instrument_id], sessions=5, interval_sec=86400)
    assert daily_window["sessions"] == 3

    intraday_window = select_window(db_conn, [instrument_id], sessions=5, interval_sec=60)
    assert intraday_window["sessions"] == 0


@pytest.mark.db
def test_select_window_finds_the_latest_sessions_every_instrument_shares(db_conn) -> None:  # noqa: ANN001
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.agent_contract.smoke import fetch_bars, select_window

    ids = []
    for symbol in ("SMOKEA", "SMOKEB"):
        row = db_conn.execute(
            "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
            "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
            "RETURNING instrument_id",
            (symbol, f"NSE:CM:{symbol}"),
        ).fetchone()
        ids.append(row[0])

    # A shares days 1-3, B shares days 2-4. The overlap is days 2-3.
    for instrument_id, days in ((ids[0], (1, 2, 3)), (ids[1], (2, 3, 4))):
        for day in days:
            db_conn.execute(
                "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
                "close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
                (instrument_id, datetime(2026, 7, day, 9, 15, tzinfo=UTC)),
            )

    window = select_window(db_conn, ids, sessions=5)
    assert window["sessions"] == 2
    bars = fetch_bars(db_conn, ids, window)
    assert set(bars) == set(ids)
    assert all(isinstance(bar.close, Decimal) for series in bars.values() for bar in series)


@pytest.mark.db
def test_fetch_bars_daily_branch_reconstructs_a_continuous_series_across_a_split(
    db_conn,  # noqa: ANN001
) -> None:
    """A 1:2 split printed on day 3. Unadjusted closes would show a ~50%
    drop between day 2 and day 3 -- the exact defect measured against real
    NSE splits in the spec (COLAB: -51.0%, VLL: -49.0%, NAVKARURB: -47.6%).
    With as_of set to the window's end (D3a-2: fixed for the whole run,
    after the split), the adjusted series must be continuous instead.
    """
    from datetime import date

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYSPLIT','INR',"
        "'ACTIVE','NSE:CM:DAILYSPLIT') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]

    # Day 3 is the ex-date. Its own bar is already printed post-split, per
    # adjusted_bars's factor_for: an action scales a bar only when the
    # ex_date is STRICTLY AFTER that bar's day.
    closes = {
        date(2024, 1, 8): 200,  # 2 days before ex-date
        date(2024, 1, 9): 210,  # 1 day before ex-date
        date(2024, 1, 10): 105,  # ex-date -- already post-split as printed
        date(2024, 1, 11): 106,  # 1 day after
        date(2024, 1, 12): 104,  # 2 days after
    }
    for day, close in closes.items():
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,%s,%s,%s,%s,10,1)",
            (instrument_id, day, close, close, close, close),
        )
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, "
        "ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2024-01-10',1,2,'test')",
        (instrument_id,),
    )

    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-12T23:59:59.999999+00:00",
        "sessions": 5,
        "instruments": {},
    }
    bars = fetch_bars(db_conn, [instrument_id], window, interval_sec=86400)

    series = [b.close for b in bars[instrument_id]]
    assert series == [
        Decimal("100.0000"),  # 200 * 0.5
        Decimal("105.0000"),  # 210 * 0.5
        Decimal("105.0000"),  # ex-date, unscaled
        Decimal("106.0000"),  # unscaled
        Decimal("104.0000"),  # unscaled
    ]
    # The regression itself: no jump anywhere near 50%.
    for a, b in zip(series, series[1:], strict=False):
        assert abs(b / a - 1) < Decimal("0.1"), (a, b)


@pytest.mark.db
def test_fetch_bars_daily_branch_uses_the_windows_end_as_as_of_not_some_other_date(
    db_conn,  # noqa: ANN001
) -> None:
    """Pins D3a-2's specific choice, not just that adjustment happens at
    all. `adjustment_factors`' query filters `ex_date <= as_of` -- an
    action whose ex_date is after `as_of` is not merely left unscaled, it
    is invisible entirely (confirmed by direct inspection of
    `_ACTION_QUERY`). So a window that ENDS before the split's ex-date
    must come back completely raw. If `_fetch_daily_bars` passed the
    wrong date here -- `date.today()`, the window's START, anything but
    `window["end"]` -- this is the test that would catch it: today's real
    calendar date is long after 2024, so a `date.today()` bug would make
    this test see the split as already known and silently pass adjusted
    values instead of raw ones.
    """
    from datetime import date

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','DAILYPRESPLIT','INR',"
        "'ACTIVE','NSE:CM:DAILYPRESPLIT') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for day, close in {date(2024, 1, 8): 200, date(2024, 1, 9): 210}.items():
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,%s,%s,%s,%s,10,1)",
            (instrument_id, day, close, close, close, close),
        )
    db_conn.execute(
        "INSERT INTO corporate_actions (instrument_id, action_type, ex_date, "
        "ratio_from, ratio_to, source) VALUES (%s,'SPLIT','2024-01-10',1,2,'test')",
        (instrument_id,),
    )

    # Window ends 2024-01-09 -- the DAY BEFORE the split's ex_date.
    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-09T23:59:59.999999+00:00",
        "sessions": 2,
        "instruments": {},
    }
    bars = fetch_bars(db_conn, [instrument_id], window, interval_sec=86400)

    series = [b.close for b in bars[instrument_id]]
    assert series == [Decimal("200.0000"), Decimal("210.0000")]  # raw, NOT halved


@pytest.mark.db
def test_fetch_bars_1m_path_is_unaffected_by_the_daily_branch(db_conn) -> None:  # noqa: ANN001
    """The default interval_sec=60 must produce identical output to before
    this plan -- the vacuity guard for every 1-minute strategy already on
    this platform."""
    from datetime import UTC, datetime

    from trading.agent_contract.smoke import fetch_bars

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','MINUTEONLY','INR',"
        "'ACTIVE','NSE:CM:MINUTEONLY') RETURNING instrument_id"
    ).fetchone()
    instrument_id = row[0]
    for minute in range(3):
        db_conn.execute(
            "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, "
            "low, close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
            (instrument_id, datetime(2024, 1, 8, 9, 15 + minute, tzinfo=UTC)),
        )
    window = {
        "start": "2024-01-08T00:00:00+00:00",
        "end": "2024-01-08T23:59:59.999999+00:00",
        "sessions": 1,
        "instruments": {},
    }

    bars = fetch_bars(db_conn, [instrument_id], window)  # interval_sec defaults to 60

    assert len(bars[instrument_id]) == 3
    assert all(b.interval_sec == 60 for b in bars[instrument_id])


@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_runs_a_real_strategy_end_to_end(db_conn) -> None:  # noqa: ANN001
    """`smoke_test()` is the public entry point of the whole feature and,
    until now, had no coverage of its own composition: `build_verdict` is
    exercised only against hand-built dicts, and `select_window`/
    `fetch_bars` only in isolation. Nothing proved that configure ->
    resolve_universe -> select_window -> fetch_bars -> _charge_key ->
    load_schedules -> two sandboxed smoke runs -> build_verdict actually
    fits together against a real container and a real database.

    Three container runs happen inside this one `smoke_test()` call (one
    configure, two smoke), so this is slow by nature.
    """
    import textwrap
    from datetime import UTC, datetime

    from trading.agent_contract.smoke import smoke_test

    symbol = "SMOKEE2E"
    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
        "RETURNING instrument_id",
        (symbol, f"NSE:CM:{symbol}"),
    ).fetchone()
    instrument_id = row[0]

    # Three distinct sessions, five 1-minute bars each -- enough for
    # select_window to find a real window and for a market order
    # submitted on the first bar to fill against a later one.
    for day in (25, 26, 27):
        for minute in range(5):
            db_conn.execute(
                "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
                "close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
                (instrument_id, datetime(2026, 8, day, 9, 15 + minute, tzinfo=UTC)),
            )

    # UPSTOX/NSE/EQUITY/DELIVERY is one of the three combinations the
    # seeded charge_schedules table actually carries (see the task brief);
    # NSE equity is picked so _charge_key resolves without inventing data.
    source = (
        textwrap.dedent(
            """
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="smoke-e2e",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="SMOKEE2E"),
                        ],
                        data=DataRequest(bars="1m", history_bars=10),
                        capital=Decimal("1000000"),
                        base_currency="INR",
                    )

                def initialize(self, ctx):
                    self._ordered = False

                def on_bar(self, ctx, bars):
                    if not self._ordered:
                        self._ordered = True
                        ctx.order(
                            list(bars)[0],
                            side="BUY",
                            quantity=Decimal("1"),
                            rationale="end-to-end smoke coverage",
                        )
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is True, verdict.as_agent_feedback()
    assert verdict.window["sessions"] == 3
    assert verdict.window["instruments"] == {str(instrument_id): {"bars": 15}}
    assert verdict.outcome is not None
    assert verdict.outcome["fills"] == 1
    assert len(verdict.outcome["orders"]) == 1
    assert verdict.outcome["orders"][0]["status"] == "FILLED"


def test_a_crash_report_names_the_exception_rather_than_its_list_repr() -> None:
    # The last line of the traceback is what an agent acts on. Slicing it
    # out with `[-1:]` yields a list, and an f-string renders a list as
    # `['ValueError: boom']` -- brackets, quotes and all -- which reads as
    # a formatting bug in the platform rather than a fault in the strategy.
    crashed = _outcome(
        ok=False,
        error="Traceback (most recent call last):\n  ...\nValueError: boom",
        crashed_at={"handler": "on_bar", "ts": "09:31"},
    )
    feedback = build_verdict(crashed, crashed, _WINDOW, "runc", False).as_agent_feedback()
    assert "ValueError: boom" in feedback
    assert "['ValueError: boom']" not in feedback


def test_a_crash_on_only_the_second_pass_is_caught_as_nondeterministic() -> None:
    # The whole point of running twice is that a strategy which fails
    # intermittently is a strategy nobody can backtest. `build_verdict`
    # branched on the FIRST pass alone, so when neither pass ordered, the
    # order-sequence comparison was [] == [] and a crashed second pass
    # produced a PASS.
    clean = _outcome()
    crashed = _outcome(
        ok=False,
        code="SMOKE_CRASH",
        error="ValueError: boom",
        crashed_at={"handler": "on_bar", "ts": "09:31"},
    )
    verdict = build_verdict(clean, crashed, _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "NONDETERMINISTIC" in [f.code for f in verdict.report.findings]
    feedback = verdict.as_agent_feedback()
    assert "ValueError: boom" in feedback
    assert "pass 2" in feedback


def test_a_timeout_on_only_the_second_pass_is_caught_too() -> None:
    # `_outcome_of` translates a timeout and an OOM kill into the same
    # crash shape, so they travel the same path -- but only if that path
    # looks at the second pass at all.
    clean = _outcome()
    timed_out = _outcome(
        ok=False,
        code="SMOKE_TIMEOUT",
        error="[SMOKE_TIMEOUT] the run exceeded its wall clock",
        crashed_at={"handler": "SMOKE_TIMEOUT", "ts": ""},
    )
    verdict = build_verdict(clean, timed_out, _WINDOW, "runc", False)
    assert verdict.passed is False
    assert "NONDETERMINISTIC" in [f.code for f in verdict.report.findings]


def test_only_one_nondeterminism_finding_is_raised_when_a_pass_crashes() -> None:
    # A crashed pass reports no orders, so the sequence comparison would
    # fire a SECOND NONDETERMINISTIC for the same underlying event and
    # read as two separate problems.
    clean = _outcome(orders=[{"order_id": 1, "submitted_at": "09:31"}], fills=1)
    crashed = _outcome(
        ok=False,
        code="SMOKE_CRASH",
        error="ValueError: boom",
        crashed_at={"handler": "on_bar", "ts": "09:31"},
    )
    codes = [f.code for f in build_verdict(clean, crashed, _WINDOW, "runc", False).report.findings]
    assert codes.count("NONDETERMINISTIC") == 1


@pytest.mark.db
def test_a_declared_instrument_that_does_not_resolve_is_named_not_dropped(db_conn) -> None:  # noqa: ANN001
    """`resolve_universe` appended only the refs that matched a row, so a
    typo'd symbol vanished and the strategy ran on a smaller universe than
    it declared -- and passed. Silently narrowing the universe is the same
    class of defect as a silently-zero charge: the run reports success for
    a market that is not the one the manifest asked for."""
    from datetime import date

    from trading.agent_contract.smoke import _UnresolvedUniverse, resolve_universe

    db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','RESOLVEOK','INR','ACTIVE',"
        "'NSE:CM:RESOLVEOK')"
    )
    manifest = {
        "universe": [
            {"exchange": "NSE", "segment": "CM", "symbol": "RESOLVEOK"},
            {"exchange": "NSE", "segment": "CM", "symbol": "NOSUCHSYM"},
        ]
    }

    with pytest.raises(_UnresolvedUniverse) as raised:
        resolve_universe(db_conn, manifest, date(2026, 9, 3))

    message = str(raised.value)
    assert "NSE:CM:NOSUCHSYM" in message
    assert "declares 2" in message
    assert "RESOLVEOK\n" not in message  # the one that DID resolve is not blamed


@pytest.mark.db
def test_a_fully_resolvable_universe_still_returns_its_ids(db_conn) -> None:
    # The vacuity guard for the test above: raising on every universe
    # would satisfy it just as well.
    from datetime import date

    from trading.agent_contract.smoke import resolve_universe

    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM','RESOLVEOK2','INR','ACTIVE',"
        "'NSE:CM:RESOLVEOK2') RETURNING instrument_id"
    ).fetchone()

    manifest = {"universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RESOLVEOK2"}]}
    assert resolve_universe(db_conn, manifest, date(2026, 9, 3)) == [row[0]]


@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_reports_an_unresolvable_instrument_as_a_finding(db_conn) -> None:  # noqa: ANN001
    """The exception is only useful if `smoke_test` turns it into a finding
    the agent reads, rather than letting it escape as a 500."""
    import textwrap

    from trading.agent_contract.smoke import smoke_test

    source = (
        textwrap.dedent(
            """
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="ghost-universe",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="NOSUCHSYM"),
                        ],
                        data=DataRequest(bars="1m", history_bars=10),
                        capital=Decimal("1000000"),
                        base_currency="INR",
                    )

                def initialize(self, ctx):
                    pass

                def on_bar(self, ctx, bars):
                    pass
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is False
    assert [f.code for f in verdict.report.findings] == ["MANIFEST_UNRESOLVABLE"]
    assert "NSE:CM:NOSUCHSYM" in verdict.as_agent_feedback()


def test_both_container_passes_run_under_the_configured_runtime(monkeypatch) -> None:  # noqa: ANN001
    """`SandboxLimits.runtime=None` means "whatever the daemon defaults to",
    and the gVisor-capable VM still defaults to runc -- so pointing
    DOCKER_CONTEXT at it is not enough to get kernel isolation. The runtime
    has to be asked for. All three container runs must ask, including
    `configure`, which was not passed limits at all: a configure pass
    confined only by namespaces while the smoke passes are confined by
    gVisor is strictly the weaker of the two, and it runs the strategy's
    code first.
    """
    from trading.agent_contract import smoke as smoke_module

    seen: list[object] = []

    def fake_configure(source, limits=None):  # noqa: ANN001, ANN202
        seen.append(limits)
        raise _StopEarly

    class _StopEarly(Exception):
        pass

    monkeypatch.setenv("STRATEGY_SANDBOX_RUNTIME", "runsc")
    get_settings.cache_clear()
    monkeypatch.setattr(smoke_module, "run_strategy_in_sandbox", fake_configure)

    with pytest.raises(_StopEarly):
        smoke_module.smoke_test(None, "irrelevant")  # type: ignore[arg-type]

    get_settings.cache_clear()
    assert [getattr(limit, "runtime", None) for limit in seen] == ["runsc"]


def test_nothing_about_the_runtime_is_hardcoded(monkeypatch) -> None:  # noqa: ANN001
    """The vacuity guard: hard-coding "runsc" would satisfy the test above
    and break every machine without gVisor installed.

    Asserted against a stubbed settings object rather than the real
    default, because `Settings` reads `.env.local` -- so a developer who
    configures gVisor on their own machine would otherwise fail this test
    for doing exactly what the docs tell them to. The claim here is about
    the code, not about the host.
    """
    from types import SimpleNamespace

    from trading.agent_contract import smoke as smoke_module

    monkeypatch.setattr(
        smoke_module,
        "get_settings",
        lambda: SimpleNamespace(
            strategy_sandbox_runtime=None, strategy_sandbox_docker_context=None
        ),
    )
    limits = smoke_module._resolve_limits(None)

    assert limits.runtime is None
    assert limits.docker_context is None


def test_the_configured_docker_context_reaches_every_container(monkeypatch) -> None:  # noqa: ANN001
    """One setting, both halves. gVisor lives on a particular daemon, so a
    runtime chosen without a daemon (or the reverse) produces a run that is
    confined differently than the settings claim."""
    from trading.agent_contract.smoke import _resolve_limits

    monkeypatch.setenv("STRATEGY_SANDBOX_DOCKER_CONTEXT", "colima-sandbox")
    monkeypatch.setenv("STRATEGY_SANDBOX_RUNTIME", "runsc")
    get_settings.cache_clear()
    try:
        limits = _resolve_limits(None)
    finally:
        get_settings.cache_clear()

    assert limits.docker_context == "colima-sandbox"
    assert limits.runtime == "runsc"


def test_an_explicit_limits_argument_still_wins() -> None:
    # The vacuity guard: settings fill in a default, they do not override a
    # caller that asked for something specific.
    from trading.agent_contract.sandbox import SandboxLimits
    from trading.agent_contract.smoke import _resolve_limits

    asked = SandboxLimits(runtime=None, docker_context=None, memory="512m")
    assert _resolve_limits(asked) is asked


# --- what the run was actually worth -------------------------------------------


def test_the_report_states_the_money_a_run_ended_with() -> None:
    """orders and fills say the code ran; only equity says whether it was
    worth running. The number an operator compares two agents on cannot
    live solely in a Postgres column."""
    outcome = _outcome(
        orders=[{"order_id": 1}], fills=1, final_cash="98688.46", final_equity="99912.30"
    )
    verdict = build_verdict(
        outcome,
        outcome,
        _WINDOW,
        "runc",
        False,
        starting_cash=Decimal("100000"),
        currency="INR",
    )
    feedback = verdict.as_agent_feedback()

    assert "99,912.30 INR" in feedback
    assert "-87.70" in feedback  # 99912.30 - 100000
    assert "-0.09%" in feedback


def test_a_profit_is_signed_so_it_cannot_be_misread() -> None:
    outcome = _outcome(orders=[{"order_id": 1}], fills=1, final_equity="101500")
    verdict = build_verdict(
        outcome,
        outcome,
        _WINDOW,
        "runc",
        False,
        starting_cash=Decimal("100000"),
        currency="INR",
    )
    assert "+1,500.00" in verdict.as_agent_feedback()
    assert verdict.pnl == Decimal("1500.00")


def test_no_pnl_is_claimed_when_the_starting_capital_is_unknown() -> None:
    # Defaulting the baseline to zero would report the entire equity as
    # profit -- a fabricated number, in the direction that flatters.
    verdict = build_verdict(_outcome(final_equity="99912.30"), _outcome(), _WINDOW, "runc", False)

    assert verdict.pnl is None
    assert verdict.pnl_pct is None
    assert "P&L" not in verdict.as_agent_feedback()


def test_zero_starting_capital_yields_no_percentage_rather_than_a_crash() -> None:
    verdict = build_verdict(
        _outcome(final_equity="0"),
        _outcome(final_equity="0"),
        _WINDOW,
        "runc",
        False,
        starting_cash=Decimal("0"),
        currency="INR",
    )
    assert verdict.pnl == Decimal("0")
    assert verdict.pnl_pct is None


def test_a_crashed_run_reports_no_money_at_all() -> None:
    # `outcome` is None on a crash, so there is nothing to report and
    # nothing to invent.
    crashed = _outcome(ok=False, error="boom", crashed_at={"handler": "on_bar", "ts": "x"})
    verdict = build_verdict(
        crashed, crashed, _WINDOW, "runc", False, starting_cash=Decimal("100000"), currency="INR"
    )
    assert verdict.pnl is None
    assert "P&L" not in verdict.as_agent_feedback()


def test_resolve_bar_interval_maps_every_schema_value_to_seconds() -> None:
    from trading.agent_contract.smoke import resolve_bar_interval

    expected = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
    for bars, seconds in expected.items():
        manifest = {"data": {"bars": bars}}
        assert resolve_bar_interval(manifest) == seconds


def test_resolve_bar_interval_rejects_anything_else() -> None:
    """No silent default. A typo'd interval reaching this function
    unvalidated -- nothing schema-checks it before smoke_test calls this,
    see the spec's testing-section correction -- must be a loud failure,
    not a quiet 60.
    """
    from trading.agent_contract.smoke import _InvalidBarInterval, resolve_bar_interval

    for manifest in (
        {"data": {"bars": "2m"}},
        {"data": {"bars": None}},
        {"data": {}},
        {},
        {"data": {"bars": ["1m"]}},  # unhashable type (list)
        {"data": "1m"},  # data is not a dict
    ):
        with pytest.raises(_InvalidBarInterval):
            resolve_bar_interval(manifest)


@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_serves_daily_bars_to_a_strategy_that_declares_them(db_conn) -> None:  # noqa: ANN001
    """End-to-end proof of the whole point of this plan: a strategy
    declaring bars="1d" gets real daily bars, not silently the 1-minute
    ones -- through a real container, a real manifest round trip, and the
    real adjustment layer."""
    import textwrap
    from datetime import date

    from trading.agent_contract.smoke import smoke_test

    symbol = "DAILYE2E"
    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
        "RETURNING instrument_id",
        (symbol, f"NSE:CM:{symbol}"),
    ).fetchone()
    instrument_id = row[0]

    # Dated to fall inside the seeded UPSTOX/NSE/EQUITY/DELIVERY charge
    # schedule's effective range (effective_from=2024-10-01) -- smoke_test
    # goes through _charge_key/load_schedules on the way to a fill, unlike
    # the unit-level fetch_bars tests elsewhere in this file that use
    # 2024-01 dates and never reach charges.
    for day in (date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27)):
        db_conn.execute(
            "INSERT INTO bars_daily (instrument_id, ts, open, high, low, close, "
            "volume, source) VALUES (%s,%s,100,101,99,100,1000,1)",
            (instrument_id, day),
        )

    source = (
        textwrap.dedent(
            f"""
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="daily-e2e",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="{symbol}"),
                        ],
                        data=DataRequest(bars="1d", history_bars=5),
                        capital=Decimal("1000000"),
                        base_currency="INR",
                    )

                def initialize(self, ctx):
                    self._ordered = False

                def on_bar(self, ctx, bars):
                    if not self._ordered:
                        self._ordered = True
                        ctx.order(
                            list(bars)[0],
                            side="BUY",
                            quantity=Decimal("1"),
                            rationale="daily interval end-to-end",
                        )
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is True, verdict.as_agent_feedback()
    assert verdict.window["sessions"] == 3
    assert verdict.outcome is not None
    assert verdict.outcome["fills"] == 1


@pytest.mark.db
@pytest.mark.sandbox
def test_smoke_test_reports_an_invalid_bar_interval_without_running_the_smoke_containers(
    db_conn,  # noqa: ANN001
) -> None:
    """DataRequest.bars is a Literal type hint, not a runtime-enforced one
    (platform_sdk.py: `BarInterval = Literal[...]`) -- a real strategy can
    genuinely pass a bad value, and this must surface as a finding after
    the configure() container alone, never reaching the two smoke
    containers."""
    import textwrap
    from datetime import UTC, datetime

    from trading.agent_contract.smoke import smoke_test

    symbol = "BADINTERVAL"
    row = db_conn.execute(
        "INSERT INTO instruments (asset_class, exchange, segment, symbol, currency, "
        "status, canonical_key) VALUES ('EQUITY','NSE','CM',%s,'INR','ACTIVE',%s) "
        "RETURNING instrument_id",
        (symbol, f"NSE:CM:{symbol}"),
    ).fetchone()
    instrument_id = row[0]

    for day in (25, 26, 27):
        for minute in range(5):
            db_conn.execute(
                "INSERT INTO bars_intraday (instrument_id, ts, interval_sec, open, high, low, "
                "close, volume, source) VALUES (%s,%s,60,100,101,99,100,10,1)",
                (instrument_id, datetime(2026, 8, day, 9, 15 + minute, tzinfo=UTC)),
            )

    source = (
        textwrap.dedent(
            f"""
            from decimal import Decimal
            from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


            class MyStrategy(Strategy):
                def configure(self):
                    return StrategyManifest(
                        name="bad-interval",
                        version="1.0.0",
                        universe=[
                            InstrumentRef(exchange="NSE", segment="CM", symbol="{symbol}"),
                        ],
                        data=DataRequest(bars="2m", history_bars=5),
                        capital=Decimal("1000000"),
                        base_currency="INR",
                    )

                def initialize(self, ctx):
                    pass

                def on_bar(self, ctx, bars):
                    pass
            """
        ).strip()
        + "\n"
    )

    verdict = smoke_test(db_conn, source)

    assert verdict.passed is False
    assert [f.code for f in verdict.report.findings] == ["MANIFEST_UNRESOLVABLE"]
    assert "2m" in verdict.as_agent_feedback()
