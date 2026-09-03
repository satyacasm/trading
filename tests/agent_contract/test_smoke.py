"""Tests for stage 2 of the upload pipeline: the smoke run."""

from __future__ import annotations

import pytest

from trading.agent_contract.smoke import SmokeVerdict, build_verdict  # noqa: F401


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
