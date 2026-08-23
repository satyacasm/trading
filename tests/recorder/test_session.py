import gzip
import json
from datetime import date
from pathlib import Path

from trading.recorder.session import FORMAT_VERSION, RecordingSession, read_frames


def _session(tmp_path: Path) -> RecordingSession:
    return RecordingSession(
        root=tmp_path, source_key="upstox_chain", session_date=date(2026, 8, 13)
    )


def test_frames_are_written_gzipped_and_readable(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.write_frame(b"frame-one")
    session.write_frame(b"frame-two")
    session.close()

    files = sorted((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    assert files
    with gzip.open(files[0], "rb") as handle:
        assert handle.read().count(b"frame-") == 2


def test_manifest_records_a_disconnect_as_an_explicit_gap(tmp_path: Path) -> None:
    """Without this, a dropped socket looks like three minutes of no trades."""
    session = _session(tmp_path)
    session.open()
    session.note_connect()
    session.note_disconnect("socket closed")
    session.note_connect()
    session.close()

    manifest = json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())
    gaps = manifest["gaps"]
    assert len(gaps) == 1
    assert gaps[0]["reason"] == "socket closed"
    assert gaps[0]["started_at"] and gaps[0]["ended_at"]


def test_manifest_is_written_even_when_the_session_crashes(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.write_frame(b"x")
    try:
        with session:
            raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    manifest_path = tmp_path / "upstox_chain" / "2026-08-13" / "session.json"
    assert manifest_path.exists()
    assert json.loads(manifest_path.read_text())["frame_count"] == 1


def test_heartbeat_file_is_touched(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.heartbeat()
    assert (tmp_path / "upstox_chain" / "heartbeat").exists()


def test_frame_count_and_subscriptions_are_recorded(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.record_subscriptions(requested=["NIFTY", "BANKNIFTY"], acknowledged=["NIFTY"])
    session.write_frame(b"a")
    session.close()
    manifest = json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())
    assert manifest["frame_count"] == 1
    assert manifest["subscriptions"]["requested"] == ["NIFTY", "BANKNIFTY"]
    assert manifest["subscriptions"]["acknowledged"] == ["NIFTY"]


def test_anomaly_is_recorded_with_timestamp_and_reason(tmp_path: Path) -> None:
    """Ruling R2x: a frame we could not interpret is the frame most worth
    keeping — it must show up in the manifest's event log, timestamped."""
    session = _session(tmp_path)
    session.open()
    session.note_anomaly("unexpected frame type: int")
    session.close()

    manifest = json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())
    anomalies = manifest["anomalies"]
    assert len(anomalies) == 1
    assert anomalies[0]["reason"] == "unexpected frame type: int"
    assert anomalies[0]["at"]


def test_manifest_reflects_mutations_without_close_and_is_valid_json(
    tmp_path: Path,
) -> None:
    """Ruling R3x: the manifest must be durable, not written only at close.

    A recorder that only writes session.json in close() loses the entire
    manifest when the process is killed -- which is the normal way a
    long-running capture ends.
    """
    session = _session(tmp_path)
    session.open()
    session.note_connect()
    session.note_disconnect("socket closed")
    session.record_subscriptions(requested=["NIFTY"], acknowledged=["NIFTY"])
    session.note_anomaly("bad frame")
    session.heartbeat()

    manifest_path = tmp_path / "upstox_chain" / "2026-08-13" / "session.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["gaps"][0]["reason"] == "socket closed"
    assert manifest["subscriptions"]["acknowledged"] == ["NIFTY"]
    assert manifest["anomalies"][0]["reason"] == "bad frame"
    assert manifest["last_heartbeat_at"]
    assert manifest["ended_at"] is None  # close() was never called


def test_manifest_flush_is_atomic_no_temp_files_left_behind(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.note_connect()
    session.close()

    session_dir = tmp_path / "upstox_chain" / "2026-08-13"
    leftovers = list(session_dir.glob(".session.json.*.tmp"))
    assert leftovers == []
    assert (session_dir / "session.json").exists()


def test_a_frame_containing_the_old_delimiter_byte_round_trips_exactly(tmp_path: Path) -> None:
    """Ruling R4x: a newline-delimited format tears this frame in two. The
    length-prefixed format must not."""
    session = _session(tmp_path)
    session.open()
    tricky = b"\x01\x02\n\x03\x04"
    session.write_frame(tricky)
    session.write_frame(b"\x05\x06\x07")
    session.close()

    files = sorted((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    recovered = list(read_frames(files[0]))
    assert recovered == [tricky, b"\x05\x06\x07"]


def test_a_frame_that_looks_like_a_length_prefix_round_trips_exactly(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    # Four bytes that would decode as a large big-endian length if a reader
    # mistook frame contents for another length prefix.
    look_alike = b"\x00\x00\x00\x2a" + b"payload"
    session.write_frame(look_alike)
    session.close()

    files = sorted((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    recovered = list(read_frames(files[0]))
    assert recovered == [look_alike]


def test_a_zero_length_frame_round_trips(tmp_path: Path) -> None:
    """A socket can legitimately hand back an empty message."""
    session = _session(tmp_path)
    session.open()
    session.write_frame(b"")
    session.write_frame(b"not-empty")
    session.close()

    files = sorted((tmp_path / "upstox_chain" / "2026-08-13").glob("*.frames.gz"))
    recovered = list(read_frames(files[0]))
    assert recovered == [b"", b"not-empty"]

    manifest = json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())
    assert manifest["frame_count"] == 2


def test_format_version_is_recorded_in_the_manifest(tmp_path: Path) -> None:
    session = _session(tmp_path)
    session.open()
    session.close()

    manifest = json.loads((tmp_path / "upstox_chain" / "2026-08-13" / "session.json").read_text())
    assert manifest["format_version"] == FORMAT_VERSION == 1
