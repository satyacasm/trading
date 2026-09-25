"""Unit tests for deploy/daemon_json_merge.py's merge_runsc -- the one
piece of real logic in provision-sandbox-vm.sh. Tested in isolation
from ssh/colima/the VM, which this suite never touches; the shell
script around it is only checked for syntax (test_deployment.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_PATH = REPO_ROOT / "deploy" / "daemon_json_merge.py"
_SPEC = importlib.util.spec_from_file_location("daemon_json_merge", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
daemon_json_merge = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(daemon_json_merge)

merge_runsc = daemon_json_merge.merge_runsc
RUNSC_RUNTIME = daemon_json_merge.RUNSC_RUNTIME


def test_adds_runsc_to_empty_dict() -> None:
    assert merge_runsc({}) == {"runtimes": {"runsc": RUNSC_RUNTIME}}


def test_preserves_other_keys_including_other_runtimes() -> None:
    original = {
        "log-driver": "json-file",
        "runtimes": {"nvidia": {"path": "/usr/bin/nvidia-container-runtime"}},
    }
    merged = merge_runsc(original)
    assert merged["log-driver"] == "json-file"
    assert merged["runtimes"]["nvidia"] == {"path": "/usr/bin/nvidia-container-runtime"}
    assert merged["runtimes"]["runsc"] == RUNSC_RUNTIME


def test_is_idempotent() -> None:
    original = {"log-driver": "json-file"}
    once = merge_runsc(original)
    twice = merge_runsc(once)
    assert once == twice


def test_does_not_mutate_input() -> None:
    original = {"runtimes": {"other": {"path": "/x"}}}
    merge_runsc(original)
    assert original == {"runtimes": {"other": {"path": "/x"}}}
