"""Fake-socket tests for the recording loop.

No network is touched anywhere here: `UpstoxFeed` is a small protocol
(authorize/subscribe/iterate/close) and `ScriptedFeed` below is a fake that
implements it, driven entirely by data the test supplies. `FakeClock` and a
recording `sleep` stand in for wall-clock time so reconnect/backoff and the
graceful `until` exit are exercised deterministically and instantly.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from trading.recorder.session import RecordingSession, read_frames
from trading.recorder.upstox_ws import Frame, LiveUpstoxFeed, run_recording_loop


class FakeClock:
    """Deterministic stand-in for `datetime.now(UTC)`.

    Each call returns the current value, then advances by `step`. The loop
    under test calls `clock()` a fixed, known number of times per frame, so
    tests pick `until` to land exactly where they want the loop to stop.
    """

    def __init__(self, start: datetime, step: timedelta) -> None:
        self._now = start
        self._step = step

    def __call__(self) -> datetime:
        current = self._now
        self._now += self._step
        return current


class ScriptedFeed:
    """A fake `UpstoxFeed`: yields a scripted list of frames, then optionally fails."""

    def __init__(
        self,
        frames: Sequence[Frame],
        *,
        acknowledged: list[str] | None = None,
        fail_after: BaseException | None = None,
        authorize_error: BaseException | None = None,
    ) -> None:
        self.frames = list(frames)
        self.acknowledged = acknowledged if acknowledged is not None else []
        self.fail_after = fail_after
        self.authorize_error = authorize_error
        self.requested: list[str] | None = None
        self.closed = False

    async def authorize(self) -> None:
        if self.authorize_error is not None:
            raise self.authorize_error

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        self.requested = instrument_keys
        return self.acknowledged

    async def __aiter__(self) -> AsyncIterator[Frame]:
        for frame in self.frames:
            yield frame
        if self.fail_after is not None:
            raise self.fail_after

    async def aclose(self) -> None:
        self.closed = True


async def _no_sleep(seconds: float) -> None:
    return None


def _manifest(tmp_path: Path) -> dict:  # type: ignore[type-arg]
    return json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())


def _new_session(tmp_path: Path) -> RecordingSession:
    session = RecordingSession(
        root=tmp_path, source_key="upstox_chain", session_date=date(2026, 8, 13)
    )
    session.open()
    return session


def test_frames_are_captured_and_the_loop_exits_cleanly_at_until(tmp_path: Path) -> None:
    session = _new_session(tmp_path)
    feed = ScriptedFeed([b"f1", b"f2", b"f3", b"f4"], acknowledged=["NIFTY"])
    start = datetime(2026, 8, 13, 9, 15, tzinfo=UTC)
    clock = FakeClock(start=start, step=timedelta(seconds=1))
    # 4 seconds in: call0 (heartbeat init)=0, call1 (while)=1, frame1 until=2,
    # frame1 heartbeat=3, frame2 until=4 (>=4 -> break). Only frame1 lands.
    until = start + timedelta(seconds=4)

    asyncio.run(
        run_recording_loop(
            session,
            lambda: feed,
            universe=["NIFTY", "BANKNIFTY"],
            until=until,
            clock=clock,
            sleep=_no_sleep,
        )
    )

    manifest = _manifest(tmp_path)
    assert manifest["frame_count"] == 1
    assert manifest["subscriptions"] == {
        "requested": ["NIFTY", "BANKNIFTY"],
        "acknowledged": ["NIFTY"],
    }
    assert manifest["ended_at"] is not None
    assert manifest["gaps"] == []
    assert len(manifest["connects"]) == 1
    assert feed.closed is True

    frame_file = next((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    assert list(read_frames(frame_file)) == [b"f1"]


def test_disconnect_triggers_backoff_then_a_recorded_reconnect(tmp_path: Path) -> None:
    session = _new_session(tmp_path)
    feed1 = ScriptedFeed([b"a", b"b"], fail_after=RuntimeError("socket closed"))
    feed2 = ScriptedFeed([b"more"] * 60, acknowledged=["NIFTY"])
    feeds = iter([feed1, feed2])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    start = datetime(2026, 8, 13, 9, 15, tzinfo=UTC)
    clock = FakeClock(start=start, step=timedelta(seconds=1))
    until = start + timedelta(seconds=100)  # feed2's 60 frames comfortably outlast this

    asyncio.run(
        run_recording_loop(
            session,
            lambda: next(feeds),
            universe=["NIFTY"],
            until=until,
            clock=clock,
            sleep=fake_sleep,
        )
    )

    manifest = _manifest(tmp_path)
    assert len(manifest["gaps"]) == 1
    assert manifest["gaps"][0]["reason"] == "socket closed"
    assert manifest["gaps"][0]["ended_at"] is not None  # closed by the reconnect's note_connect
    assert len(manifest["connects"]) == 2
    assert sleep_calls == [1.0]
    assert feed1.closed is True
    assert feed2.closed is True
    assert manifest["frame_count"] >= 2  # "a", "b" from feed1 before the drop


def test_repeated_failures_back_off_exponentially_up_to_the_cap(tmp_path: Path) -> None:
    session = _new_session(tmp_path)
    attempts = {"n": 0}

    def factory() -> ScriptedFeed:
        attempts["n"] += 1
        if attempts["n"] <= 4:
            return ScriptedFeed([], authorize_error=RuntimeError(f"fail-{attempts['n']}"))
        return ScriptedFeed([b"ok"] * 60, acknowledged=["NIFTY"])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    start = datetime(2026, 8, 13, 9, 15, tzinfo=UTC)
    clock = FakeClock(start=start, step=timedelta(seconds=1))
    until = start + timedelta(seconds=1000)

    asyncio.run(
        run_recording_loop(
            session,
            factory,
            universe=["NIFTY"],
            until=until,
            clock=clock,
            sleep=fake_sleep,
            initial_backoff_seconds=1.0,
            max_backoff_seconds=4.0,
        )
    )

    assert sleep_calls == [1.0, 2.0, 4.0, 4.0]
    manifest = _manifest(tmp_path)
    assert len(manifest["gaps"]) == 4


def test_a_frame_the_handler_cannot_interpret_is_kept_raw_and_flagged(tmp_path: Path) -> None:
    """Ruling R2x: malformed frames never crash the session and never vanish."""
    session = _new_session(tmp_path)
    # 12345 is neither bytes nor str -- the handler cannot make sense of it,
    # but its raw representation must still land in the frame file.
    feed = ScriptedFeed([b"good-1", 12345, b"good-2"], acknowledged=["NIFTY"])
    start = datetime(2026, 8, 13, 9, 15, tzinfo=UTC)
    clock = FakeClock(start=start, step=timedelta(seconds=1))
    # Exactly one full pass over the 3 frames, then a clean exit -- see the
    # FakeClock docstring for how the call count maps to elapsed seconds.
    until = start + timedelta(seconds=7)

    asyncio.run(
        run_recording_loop(
            session,
            lambda: feed,
            universe=["NIFTY"],
            until=until,
            clock=clock,
            sleep=_no_sleep,
        )
    )

    manifest = _manifest(tmp_path)
    assert manifest["gaps"] == []  # a frame we can't interpret is not a disconnect
    assert manifest["frame_count"] == 3
    assert len(manifest["anomalies"]) == 1
    assert manifest["anomalies"][0]["reason"] == "unexpected frame type: int"
    assert manifest["anomalies"][0]["at"]

    frame_file = next((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    recovered = list(read_frames(frame_file))
    assert recovered == [b"good-1", b"12345", b"good-2"]  # repr() of the malformed frame


class FakeConnection:
    """Stands in for `websockets.asyncio.client.ClientConnection.send`.

    Only `send` is exercised by `LiveUpstoxFeed.subscribe`; it records
    exactly what object was passed so the test can assert on its Python
    type (str vs bytes decides the WebSocket frame type the real
    `websockets` library emits).
    """

    def __init__(self) -> None:
        self.sent: list[object] = []

    async def send(self, message: object) -> None:
        self.sent.append(message)


def test_subscribe_sends_the_control_message_as_a_binary_frame_not_text() -> None:
    """Regression guard for the BUG 1 root cause: Upstox V3 silently drops
    the subscribe control message when it arrives as a Text frame (which is
    what `websockets` sends for a Python `str`). It must be sent as `bytes`
    so `websockets` emits a Binary frame instead."""
    feed = LiveUpstoxFeed(access_token="fake-token")
    fake_connection = FakeConnection()
    feed._connection = fake_connection  # type: ignore[assignment]

    instrument_keys = ["NSE_EQ|INE002A01018", "NSE_EQ|INE467B01029"]
    acknowledged = asyncio.run(feed.subscribe(instrument_keys))

    assert acknowledged == instrument_keys
    assert len(fake_connection.sent) == 1
    sent = fake_connection.sent[0]
    assert isinstance(sent, bytes), f"expected bytes (Binary frame), got {type(sent).__name__}"

    payload = json.loads(sent)
    assert payload["method"] == "sub"
    assert payload["data"] == {"mode": "full", "instrumentKeys": instrument_keys}
    assert isinstance(payload["guid"], str) and payload["guid"]


class SilentFeed:
    """A feed that connects, subscribes, and then says nothing at all.

    Exactly what the real feed does outside market hours, on a holiday, or
    when the socket is alive but the far end has stopped sending.
    """

    def __init__(self, acknowledged: list[str] | None = None) -> None:
        self.acknowledged = acknowledged if acknowledged is not None else []
        self.closed = False

    async def authorize(self) -> None:
        return None

    async def subscribe(self, instrument_keys: list[str]) -> list[str]:
        return self.acknowledged

    async def __aiter__(self) -> AsyncIterator[Frame]:
        # Never yields. `asyncio.sleep` rather than a bare loop so control
        # returns to the event loop, which is what a real socket read does.
        while True:
            await asyncio.sleep(0.01)
        yield b""  # pragma: no cover - unreachable, present to type this as a generator

    async def aclose(self) -> None:
        self.closed = True


def test_a_silent_feed_still_reaches_its_deadline(tmp_path: Path) -> None:
    """The deadline used to be checked only when a frame arrived, so a feed
    that went quiet held the loop open forever. On a trading day frames
    arrive constantly and it never showed; on a holiday, after hours, or
    during a stall the process simply never exited -- and a recorder fired
    daily by a scheduler leaks one stuck process per day.
    """
    session = _new_session(tmp_path)
    feed = SilentFeed(acknowledged=["NIFTY"])
    start = datetime(2026, 8, 13, 9, 15, tzinfo=UTC)

    asyncio.run(
        asyncio.wait_for(
            run_recording_loop(
                session,
                lambda: feed,
                universe=["NIFTY"],
                until=start + timedelta(seconds=2),
                clock=FakeClock(start=start, step=timedelta(seconds=1)),
                sleep=_no_sleep,
                idle_poll_seconds=0.01,
            ),
            timeout=10,
        )
    )

    manifest = _manifest(tmp_path)
    assert manifest["frame_count"] == 0
    # Silence is not a gap. Recording one here would make a quiet market
    # indistinguishable from a dropped connection, which is the single
    # distinction this manifest exists to preserve.
    assert manifest["gaps"] == []
    assert feed.closed is True
