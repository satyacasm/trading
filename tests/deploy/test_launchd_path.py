"""Unit tests for deploy/resolve_launchd_path.sh's resolve_launchd_path --
computes the PATH launchd needs so colima/docker/uv resolve (C1: launchd's
own PATH is /usr/bin:/bin:/usr/sbin:/sbin only). Tested by sourcing the
function in a bash subprocess against a fake PATH; install-live-stack.sh
itself is only checked for syntax and wiring (test_deployment.py) and is
never executed here."""

from __future__ import annotations

import stat
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RESOLVE_SCRIPT = REPO_ROOT / "deploy" / "resolve_launchd_path.sh"


def _make_fake_bin(dir_path: Path, names: list[str]) -> Path:
    dir_path.mkdir(parents=True)
    for name in names:
        exe = dir_path / name
        exe.write_text("#!/bin/sh\necho fake\n")
        exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    return dir_path


def _run(fake_path: str) -> subprocess.CompletedProcess[str]:
    # /usr/bin is needed for the `dirname` the function shells out to -- a
    # real caller's PATH always has it (it's part of launchd's own default
    # PATH), so this matches reality rather than papering over a bug.
    script = f'source "{RESOLVE_SCRIPT}"; resolve_launchd_path'
    return subprocess.run(
        ["/bin/bash", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": f"{fake_path}:/usr/bin:/bin"},
    )


def test_joins_dirnames_of_docker_colima_uv_then_system_dirs(tmp_path: Path) -> None:
    docker_dir = _make_fake_bin(tmp_path / "a", ["docker"])
    colima_dir = _make_fake_bin(tmp_path / "b", ["colima"])
    uv_dir = _make_fake_bin(tmp_path / "c", ["uv"])
    fake_path = f"{docker_dir}:{colima_dir}:{uv_dir}"

    result = _run(fake_path)

    assert result.returncode == 0, result.stderr
    assert (
        result.stdout.strip()
        == f"{docker_dir}:{colima_dir}:{uv_dir}:/usr/bin:/bin:/usr/sbin:/sbin"
    )


def test_aborts_with_clear_error_when_colima_missing(tmp_path: Path) -> None:
    docker_dir = _make_fake_bin(tmp_path / "a", ["docker"])
    uv_dir = _make_fake_bin(tmp_path / "c", ["uv"])
    fake_path = f"{docker_dir}:{uv_dir}"

    result = _run(fake_path)

    assert result.returncode != 0
    assert "colima" in result.stderr.lower()


def test_aborts_with_clear_error_when_docker_missing(tmp_path: Path) -> None:
    colima_dir = _make_fake_bin(tmp_path / "b", ["colima"])
    uv_dir = _make_fake_bin(tmp_path / "c", ["uv"])
    fake_path = f"{colima_dir}:{uv_dir}"

    result = _run(fake_path)

    assert result.returncode != 0
    assert "docker" in result.stderr.lower()


def test_aborts_with_clear_error_when_uv_missing(tmp_path: Path) -> None:
    docker_dir = _make_fake_bin(tmp_path / "a", ["docker"])
    colima_dir = _make_fake_bin(tmp_path / "b", ["colima"])
    fake_path = f"{docker_dir}:{colima_dir}"

    result = _run(fake_path)

    assert result.returncode != 0
    assert "uv" in result.stderr.lower()
