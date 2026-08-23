"""Raw market recorder: capture live broker WebSocket frames to disk, verbatim.

Governing principle (spec 5.6): record raw, parse later. This package never
parses, normalises, resolves, or touches the database — if capture-time
interpretation is wrong the data is lost forever, whereas raw frames can be
re-read for a decade.
"""

from __future__ import annotations
