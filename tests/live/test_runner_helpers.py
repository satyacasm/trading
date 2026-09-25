"""Unit tests for the pure helpers in sandbox/runner.py. Loaded by file
path rather than as a package -- sandbox/ has no __init__.py and is
never installed; its module-level imports are dependency-free (every
`trading.*` import inside it is deferred into a function body), so
loading the file directly is safe and needs no container."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_RUNNER_PATH = Path(__file__).resolve().parents[2] / "sandbox" / "runner.py"
_spec = importlib.util.spec_from_file_location("sandbox_runner_under_test", _RUNNER_PATH)
assert _spec is not None and _spec.loader is not None
runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(runner)


def test_a_json_serialisable_state_under_the_limit_encodes() -> None:
    text, error = runner._encode_state({"a": 1, "b": "x"}, max_bytes=65536)
    assert error is None
    assert text == '{"a": 1, "b": "x"}'


def test_a_state_over_the_byte_limit_is_refused_by_name() -> None:
    text, error = runner._encode_state({"big": "x" * 100}, max_bytes=50)
    assert text is None
    assert error is not None
    assert "ctx.state is" in error and "bytes" in error and "50" in error


def test_a_non_json_serialisable_state_is_refused_by_name() -> None:
    text, error = runner._encode_state({"obj": object()}, max_bytes=65536)
    assert text is None
    assert error is not None
    assert "not JSON-serialisable" in error
