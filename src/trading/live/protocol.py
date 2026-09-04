"""The frames a supervisor and a live strategy exchange.

Newline-delimited JSON over the container's stdin and stdout.

**Why pipes and not a socket.** §166 specifies Unix socket RPC. Measured on
this machine, a bind-mounted Unix socket is unreachable from the container
(`OSError 95` -- domain sockets do not work across the macOS-to-Lima
filesystem share), the default bridge grants full internet, and the only
configuration that blocks the internet while reaching strategies puts the
supervisor inside the sandbox VM, which holds no database. Pipes need no
network and no mount, so the container keeps `--network none` and all
fifteen containment tests hold unchanged. The supervisor's role is exactly
§166's: it mediates every byte.

**Why newline-delimited rather than length-prefixed.** The existing runner
already writes a single marker-prefixed JSON line and the host already
scans lines for it, so this is the same discipline extended, and it stays
debuggable by eye -- which matters for a protocol whose failures happen
inside a container you cannot attach to.

Money crosses as strings here as everywhere else: JSON numbers are IEEE 754
doubles.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = [
    "FRAME_BAR",
    "FRAME_ERROR",
    "FRAME_ORDERS",
    "FRAME_READY",
    "FRAME_STOP",
    "decode_frame",
    "encode_frame",
]

# Supervisor -> strategy
FRAME_BAR = "bar"
FRAME_STOP = "stop"

# Strategy -> supervisor
FRAME_READY = "ready"
FRAME_ORDERS = "orders"
FRAME_ERROR = "error"


def encode_frame(kind: str, **fields: Any) -> str:
    """One frame, as a single line with a trailing newline.

    `separators` without spaces because these are machine-read and a live
    run emits one per bar per strategy; the bytes are not free.
    """
    return json.dumps({"type": kind, **fields}, separators=(",", ":")) + "\n"


def decode_frame(line: str) -> dict[str, Any] | None:
    """One frame, or `None` for anything unparseable.

    `None` rather than raising: a strategy is free to print to stdout, and
    the supervisor must skip that noise rather than treat it as a protocol
    violation. A frame is a line that parses to an object with a `type`;
    everything else is the strategy talking to itself.
    """
    line = line.strip()
    if not line:
        return None
    try:
        parsed = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or "type" not in parsed:
        return None
    return parsed
