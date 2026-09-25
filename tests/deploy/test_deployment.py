"""Deployment artifacts: launchd agents keep every process alive across
sleep and crash (design §7); Docker's own restart policy does the same
for Postgres and Redis. Parsed and asserted on directly rather than
installed -- these tests run on any machine, not just the one with
launchd and Colima configured."""

from __future__ import annotations

import plistlib
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LAUNCHD_DIR = REPO_ROOT / "deploy" / "launchd"

_SERVICE_NAMES = (
    "gateway",
    "crypto_ingestor",
    "bar_aggregator",
    "paper_engine",
    "paper_alerts",
    "live_supervisor",
    "perp_ingestor",
)

_ALL_LAUNCHD_NAMES = _SERVICE_NAMES + ("colima",)


def _plist(name: str) -> dict:
    path = LAUNCHD_DIR / f"com.satyam.trading.{name}.plist"
    with path.open("rb") as f:
        return plistlib.load(f)


@pytest.mark.parametrize("name", _SERVICE_NAMES)
def test_every_service_plist_has_keepalive_and_runatload(name: str) -> None:
    data = _plist(name)
    assert data["Label"] == f"com.satyam.trading.{name}"
    assert data["KeepAlive"] is True
    assert data["RunAtLoad"] is True
    assert data["ThrottleInterval"] == 10
    assert data["StandardOutPath"].endswith(f"logs/{name}.log")
    assert data["StandardErrorPath"].endswith(f"logs/{name}.log")


def test_live_supervisor_is_prefixed_with_caffeinate() -> None:
    args = _plist("live_supervisor")["ProgramArguments"]
    assert args[0] == "/usr/bin/caffeinate"
    assert args[1] == "-i"


def test_no_other_service_is_prefixed_with_caffeinate() -> None:
    for name in _SERVICE_NAMES:
        if name == "live_supervisor":
            continue
        args = _plist(name)["ProgramArguments"]
        assert args[0] != "/usr/bin/caffeinate"


def test_gateway_runs_uvicorn_on_loopback_8000() -> None:
    args = _plist("gateway")["ProgramArguments"]
    joined = " ".join(args)
    assert "uvicorn" in joined
    assert "trading.streaming.gateway:app" in joined
    assert "127.0.0.1" in joined
    assert "8000" in joined


@pytest.mark.parametrize("name", _ALL_LAUNCHD_NAMES)
def test_every_plist_sets_environment_path_token(name: str) -> None:
    """C1: launchd's own PATH is /usr/bin:/bin:/usr/sbin:/sbin, so colima
    (start-colima.sh) and docker (Popen in agent_contract/sandbox.py) are
    invisible to any launchd-run process without this. The token is filled
    in by install-live-stack.sh at install time (see
    test_install_script_substitutes_the_path_token)."""
    data = _plist(name)
    assert data["EnvironmentVariables"]["PATH"] == "__PATH__"


def test_install_script_substitutes_the_path_token() -> None:
    text = (REPO_ROOT / "deploy" / "install-live-stack.sh").read_text()
    assert "__PATH__" in text
    assert "resolve_launchd_path" in text


def test_colima_plist_runs_at_load_with_no_keepalive() -> None:
    """A login/boot agent that starts the VMs once, not a daemon that
    should be relaunched the moment it exits."""
    path = LAUNCHD_DIR / "com.satyam.trading.colima.plist"
    with path.open("rb") as f:
        data = plistlib.load(f)
    assert data["RunAtLoad"] is True
    assert "KeepAlive" not in data or data["KeepAlive"] is False


def test_docker_compose_restarts_timescaledb_and_redis_but_not_redis_test() -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert services["timescaledb"]["restart"] == "unless-stopped"
    assert services["redis"]["restart"] == "unless-stopped"
    assert "restart" not in services["redis_test"]


@pytest.mark.parametrize(
    "script",
    [
        "deploy/start-colima.sh",
        "deploy/provision-sandbox-vm.sh",
        "deploy/install-live-stack.sh",
    ],
)
def test_shell_scripts_are_syntactically_valid(script: str) -> None:
    result = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / script)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
