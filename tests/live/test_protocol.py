"""The supervisor/strategy wire format."""

from __future__ import annotations

from trading.live.protocol import FRAME_BAR, decode_frame, encode_frame


def test_a_frame_round_trips() -> None:
    line = encode_frame(FRAME_BAR, ts="2026-09-04T10:00:00+00:00", close="1326.5000")
    frame = decode_frame(line)
    assert frame == {
        "type": FRAME_BAR,
        "ts": "2026-09-04T10:00:00+00:00",
        "close": "1326.5000",
    }


def test_a_frame_is_exactly_one_line() -> None:
    """The reader splits on newlines, so an embedded one would truncate the
    frame and desynchronise everything after it."""
    line = encode_frame(FRAME_BAR, note="two\nlines")
    assert line.count("\n") == 1
    assert line.endswith("\n")
    assert decode_frame(line)["note"] == "two\nlines"


def test_strategy_chatter_is_skipped_not_treated_as_a_violation() -> None:
    """A strategy may print. The supervisor must ignore that rather than
    kill the run -- printing is not a protocol error, and a platform that
    treated it as one would punish ordinary debugging."""
    assert decode_frame("hello from the strategy") is None
    assert decode_frame("") is None
    assert decode_frame("   ") is None
    # Valid JSON, but not a frame.
    assert decode_frame("[1, 2, 3]") is None
    assert decode_frame('{"no":"type"}') is None
