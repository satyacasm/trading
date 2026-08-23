"""Durable recording session: gzip frame capture plus a truthful manifest.

Governing principle (spec 5.6): record raw, parse later. This module never
interprets a frame — it only stores bytes and records *when things happened*
(connects, disconnects, subscription acks, anomalies) so a gap in the frame
stream can always be told apart from three minutes of genuine silence.

The manifest is flushed to disk on every mutation, atomically (write to a
temp file in the session directory, then `os.replace`), so a process killed
mid-write never leaves a truncated or empty `session.json` (ruling R3x).
"""

from __future__ import annotations

import contextlib
import gzip
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Gap:
    started_at: str
    ended_at: str | None
    reason: str


@dataclass
class Anomaly:
    """A frame (or connection event) the recorder could not make sense of.

    Ruling R2x: a malformed frame must never crash the session and must
    never vanish either — the raw bytes are still written to the frame
    file, and this entry is the pointer that tells a future parser (and a
    human) exactly where to look.
    """

    at: str
    reason: str


@dataclass
class SessionManifest:
    source_key: str
    session_date: str
    started_at: str
    ended_at: str | None = None
    frame_count: int = 0
    subscriptions: dict[str, list[str]] = field(default_factory=dict)
    gaps: list[Gap] = field(default_factory=list)
    connects: list[str] = field(default_factory=list)
    anomalies: list[Anomaly] = field(default_factory=list)
    last_heartbeat_at: str | None = None


class RecordingSession:
    """Durably captures raw frames plus a manifest that makes gaps explicit."""

    def __init__(self, root: Path, source_key: str, session_date: date) -> None:
        self._dir = root / source_key / session_date.isoformat()
        self._heartbeat_path = root / source_key / "heartbeat"
        self._manifest_path = self._dir / "session.json"
        self._manifest = SessionManifest(
            source_key=source_key,
            session_date=session_date.isoformat(),
            started_at=_now(),
        )
        self._handle: gzip.GzipFile | None = None
        self._hour: int | None = None

    def open(self) -> RecordingSession:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        self._flush_manifest()
        return self

    def __enter__(self) -> RecordingSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _rotate_if_needed(self) -> gzip.GzipFile:
        hour = datetime.now(UTC).hour
        if self._handle is None or hour != self._hour:
            if self._handle is not None:
                self._handle.close()
            self._handle = gzip.open(  # noqa: SIM115 - handle outlives this call, rotated hourly
                self._dir / f"{hour:02d}.frames.gz", "ab"
            )
            self._hour = hour
        return self._handle

    def write_frame(self, frame: bytes) -> None:
        """Append `frame` verbatim to the current hour's gzip file.

        Never parses or validates the bytes — that is the whole point of
        "record raw, parse later". Callers (including the malformed-frame
        path in `upstox_ws.py`) are expected to call this unconditionally,
        even for frames they could not otherwise interpret.
        """
        handle = self._rotate_if_needed()
        handle.write(frame)
        handle.write(b"\n")
        self._manifest.frame_count += 1
        if self._manifest.frame_count % 500 == 0:
            handle.flush()
            self._flush_manifest()

    def record_subscriptions(self, requested: list[str], acknowledged: list[str]) -> None:
        self._manifest.subscriptions = {"requested": requested, "acknowledged": acknowledged}
        self._flush_manifest()

    def note_connect(self) -> None:
        now = _now()
        self._manifest.connects.append(now)
        for gap in self._manifest.gaps:
            if gap.ended_at is None:
                gap.ended_at = now
        self._flush_manifest()

    def note_disconnect(self, reason: str) -> None:
        self._manifest.gaps.append(Gap(started_at=_now(), ended_at=None, reason=reason))
        self._flush_manifest()

    def note_anomaly(self, reason: str) -> None:
        """Record a frame (or event) the recorder could not interpret.

        Does not touch the frame file — callers are responsible for still
        writing the raw bytes via `write_frame` (ruling R2x).
        """
        self._manifest.anomalies.append(Anomaly(at=_now(), reason=reason))
        self._flush_manifest()

    def heartbeat(self) -> None:
        now = _now()
        self._heartbeat_path.write_text(now)
        self._manifest.last_heartbeat_at = now
        self._flush_manifest()

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        self._manifest.ended_at = _now()
        self._flush_manifest()

    def _flush_manifest(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(asdict(self._manifest), indent=2)
        fd, tmp_name = tempfile.mkstemp(dir=self._dir, prefix=".session.json.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(payload)
            os.replace(tmp_name, self._manifest_path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.remove(tmp_name)
            raise
