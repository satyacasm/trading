"""Supervisor behaviour that a container test would be too slow to cover."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

from trading.live.protocol import FRAME_ERROR, FRAME_ORDERS, encode_frame
from trading.live.supervisor import MAX_ORDERS_PER_MINUTE, LiveRun, handle_bar


def _run(stdout_lines: list[str]) -> LiveRun:
    process = MagicMock(spec=subprocess.Popen)
    process.poll.return_value = None
    process.stdin = MagicMock()
    process.stdout = MagicMock()
    process.stdout.readline.side_effect = [line.encode() for line in stdout_lines] + [b""]
    return LiveRun(
        live_run_id=1,
        strategy_id=1,
        portfolio_id=1,
        process=process,
        instrument_ids={1},
        runtime="runsc",
        kernel_isolated=True,
    )


def _bar() -> dict[str, object]:
    return {
        "instrument_id": 1,
        "ts": "2026-09-04T10:00:00+00:00",
        "interval_sec": 60,
        "open": "100",
        "high": "100",
        "low": "100",
        "close": "100",
        "volume": None,
    }


def test_the_rate_limit_stops_a_runaway_and_says_so(monkeypatch) -> None:  # noqa: ANN001
    """§166 puts order-rate limiting in the supervisor, and a strategy
    emitting an order per bar across a wide universe is a plausible bug --
    the engine would faithfully fill every one of them."""
    from trading.live import supervisor

    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )

    orders = [
        {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": "1",
            "limit_price": None,
            "product": "DELIVERY",
            "rationale": "runaway",
        }
    ] * (MAX_ORDERS_PER_MINUTE + 5)
    run = _run([encode_frame(FRAME_ORDERS, ts="t", orders=orders, alive=True)])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "STOPPED"
    assert "order-rate limit" in (stopped[0][1] or "")


def test_a_crashed_strategy_stops_the_run_rather_than_restarting_it(monkeypatch) -> None:  # noqa: ANN001
    """A strategy whose in-memory state vanished mid-session is not the same
    strategy, and its next orders would not follow from what it saw."""
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run([encode_frame(FRAME_ERROR, error="ZeroDivisionError: division by zero")])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "CRASHED"
    assert "ZeroDivisionError" in (stopped[0][1] or "")


def test_a_silent_container_is_treated_as_crashed(monkeypatch) -> None:  # noqa: ANN001
    """No frames at all means the process is gone or wedged. Continuing to
    feed it bars would leave a run that looks alive and trades nothing."""
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run([])

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "CRASHED"


def test_a_latched_breaker_stops_the_run_with_its_reason(monkeypatch) -> None:  # noqa: ANN001
    from trading.live import supervisor

    stopped: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        supervisor, "stop_run", lambda conn, run, status, reason: stopped.append((status, reason))
    )
    run = _run(
        [
            encode_frame(
                FRAME_ORDERS, ts="t", orders=[], alive=False, breaker_reason="max_daily_loss: ..."
            )
        ]
    )

    assert handle_bar(MagicMock(), "http://x", run, _bar()) is False
    assert stopped and stopped[0][0] == "STOPPED"
    assert "max_daily_loss" in (stopped[0][1] or "")


def test_an_ordinary_bar_keeps_the_run_alive(monkeypatch) -> None:  # noqa: ANN001
    from trading.live import supervisor

    monkeypatch.setattr(supervisor, "place_order", lambda *a, **k: True)
    run = _run([encode_frame(FRAME_ORDERS, ts="t", orders=[], alive=True)])
    conn = MagicMock()

    assert handle_bar(conn, "http://x", run, _bar()) is True
    assert run.bars_seen == 1
