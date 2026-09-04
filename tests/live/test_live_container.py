"""One bar in, one order intent out — through a real container.

The riskiest claim in the live design is that a strategy can be driven a bar
at a time over pipes, with `--network none` intact. This asserts it against
the real image rather than a mock, because the failure modes that matter
(buffering, framing, a dead process) do not exist in a mock.
"""

from __future__ import annotations

import subprocess
import textwrap
from decimal import Decimal

import pytest

from trading.agent_contract.sandbox import SandboxLimits, _docker_args
from trading.live.protocol import (
    FRAME_BAR,
    FRAME_ORDERS,
    FRAME_READY,
    FRAME_STOP,
    decode_frame,
    encode_frame,
)
from trading.runtime.payload import MODE_LIVE, SmokePayload, encode_payload

pytestmark = pytest.mark.sandbox

LIVE_STRATEGY = (
    textwrap.dedent(
        """
        from decimal import Decimal
        from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


        class MyStrategy(Strategy):
            def configure(self):
                return StrategyManifest(
                    name="live-probe",
                    version="1.0.0",
                    universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")],
                    data=DataRequest(bars="1m", history_bars=1),
                    capital=Decimal("100000"),
                    base_currency="INR",
                )

            def on_bar(self, ctx, bars):
                for instrument_id in bars:
                    ctx.order(
                        instrument_id,
                        side="BUY",
                        quantity=Decimal("1"),
                        rationale="Live probe: buy on every bar.",
                    )
        """
    ).strip()
    + "\n"
)


def _bar_frame(minute: int, price: str) -> str:
    return encode_frame(
        FRAME_BAR,
        instrument_id=1,
        ts=f"2026-09-04T10:{minute:02d}:00+00:00",
        interval_sec=60,
        open=price,
        high=price,
        low=price,
        close=price,
    )


def test_a_live_container_takes_bars_on_stdin_and_returns_order_intents() -> None:
    """The whole transport, end to end, with no network and no mount."""
    payload = encode_payload(
        SmokePayload(
            mode=MODE_LIVE,
            source=LIVE_STRATEGY,
            starting_cash=Decimal("100000"),
            slippage_bps=Decimal("0"),
        )
    )
    limits = SandboxLimits()
    # Length header, payload, then the bar frames on the same pipe. Written
    # in one go: the runner reads the payload by length and then reads
    # frames line by line, so it does not matter that they all arrive at
    # once -- and `communicate` owns closing stdin.
    stream = (
        b"%d\n" % len(payload)
        + payload
        + _bar_frame(0, "100").encode()
        + _bar_frame(1, "101").encode()
        + encode_frame(FRAME_STOP).encode()
    )
    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        _docker_args(limits, "live-probe-test"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = proc.communicate(input=stream, timeout=90)
    finally:
        if proc.poll() is None:
            proc.kill()

    frames = [f for f in (decode_frame(line) for line in stdout.decode().splitlines()) if f]
    kinds = [f["type"] for f in frames]
    assert FRAME_READY in kinds, (stdout.decode()[:1500], stderr.decode()[:1500])
    order_frames = [f for f in frames if f["type"] == FRAME_ORDERS]
    assert order_frames, kinds
    # One order per dispatched bar, and each carries what the supervisor
    # needs to place a real one.
    first = order_frames[0]["orders"]
    assert len(first) == 1
    assert first[0]["side"] == "BUY"
    assert first[0]["quantity"] == "1"
    assert "Live probe" in first[0]["rationale"]
