"""Proof that the sandbox confines what it claims to confine.

Every guarantee `sandbox.py` documents gets a test that *attempts the
thing* and asserts it fails. A docstring saying "no network" is worth
nothing; a strategy that tries to open a socket and cannot is worth
something.

The strategy sources below deliberately attempt to open sockets, write
outside the sandbox, exhaust memory, and spin forever. They are the point:
they run **inside** a container with no network, a read-only filesystem,
dropped capabilities, and hard resource ceilings, and each test asserts
the attempt was contained. None of this code runs on the host.

These tests spawn real containers, so they are slower than the rest of the
suite. They still run by default rather than behind an opt-in marker,
because an isolation guarantee nobody checks is one that quietly stops
holding.
"""

from __future__ import annotations

import textwrap

import pytest

from trading.agent_contract.sandbox import (
    SandboxLimits,
    run_strategy_in_sandbox,
)

pytestmark = pytest.mark.sandbox


def src(text: str) -> str:
    return textwrap.dedent(text).strip() + "\n"


CONFORMING = src(
    """
    from decimal import Decimal
    from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


    class MyStrategy(Strategy):
        def configure(self):
            return StrategyManifest(
                name="smoke",
                version="1.0.0",
                universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")],
                data=DataRequest(bars="1m", history_bars=50),
                capital=Decimal("1000000"),
                base_currency="INR",
            )
    """
)


def strategy_that(body: str) -> str:
    """A strategy whose configure() runs `body` -- the shape every
    containment test uses, so the thing under test is the body alone.

    Assembled explicitly rather than through a dedented f-string: the
    literal indentation before a substitution applies only to the
    substituted value's *first* line, so dedent then flattens the rest and
    silently produces source whose second line has fallen out of the
    method.
    """
    return (
        "from platform_sdk import Strategy\n"
        "\n"
        "\n"
        "class MyStrategy(Strategy):\n"
        "    def configure(self):\n" + textwrap.indent(src(body), " " * 8)
    )


# --- the happy path ----------------------------------------------------------


def test_a_conforming_strategy_runs_and_returns_its_manifest() -> None:
    result = run_strategy_in_sandbox(CONFORMING)

    assert result.ok, result.error
    assert result.stage == "configure"
    assert result.strategy_class == "MyStrategy"
    assert result.manifest is not None
    assert result.manifest["name"] == "smoke"
    assert result.manifest["base_currency"] == "INR"
    assert result.manifest["data"]["bars"] == "1m"


def test_the_allowlisted_analysis_libraries_are_importable() -> None:
    """The contract's allowlist promises numpy and pandas. If the image
    lacks them, the contract is lying to every agent that reads it."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            import numpy
            import pandas
            assert numpy.array([1, 2]).sum() == 3
            return None
            """
        )
    )
    assert result.ok, result.error


# --- containment -------------------------------------------------------------


def test_the_network_is_unreachable() -> None:
    """`--network none` means there is no interface, not a blocked port.
    A strategy must not be able to reach anything, including the host."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            import socket
            s = socket.socket()
            s.settimeout(3)
            s.connect(("1.1.1.1", 53))
            return None
            """
        )
    )
    assert not result.ok
    assert result.stage == "configure"
    assert result.error is not None
    # Unreachable at the network layer, not refused by a peer.
    assert "Network is unreachable" in result.error or "OSError" in result.error


def test_dns_does_not_resolve() -> None:
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            import socket
            socket.gethostbyname("pypi.org")
            return None
            """
        )
    )
    assert not result.ok
    assert result.error is not None


def test_the_filesystem_is_read_only() -> None:
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            with open("/evidence.txt", "w") as handle:
                handle.write("escaped")
            return None
            """
        )
    )
    assert not result.ok
    assert result.error is not None
    assert "Read-only file system" in result.error or "OSError" in result.error


def test_the_strategy_source_never_touches_a_filesystem() -> None:
    """Source arrives on stdin, so there is no file for a strategy to
    rewrite between validation and execution -- and no host path bound
    into the container at all."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            import os
            assert not os.path.exists("/strategy/strategy.py")
            assert not os.path.exists("/strategy")
            # cwd is the ephemeral tmpfs, so even a relative write dies
            # with the container.
            assert os.getcwd() == "/tmp"
            return None
            """
        )
    )
    assert result.ok, result.error


def test_tmp_is_writable_and_is_the_only_writable_surface() -> None:
    """One small tmpfs exists so a strategy can use scratch space. It
    vanishes with the container."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            with open("/tmp/scratch.txt", "w") as handle:
                handle.write("fine")
            with open("/tmp/scratch.txt") as handle:
                assert handle.read() == "fine"
            return None
            """
        )
    )
    assert result.ok, result.error


def test_the_strategy_does_not_run_as_root() -> None:
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            import os
            assert os.getuid() == 10001, f"running as uid {os.getuid()}"
            return None
            """
        )
    )
    assert result.ok, result.error


# --- resource ceilings -------------------------------------------------------


def test_exceeding_the_memory_limit_kills_the_container() -> None:
    """A runaway allocation must be the container's problem, not the
    host's. Reported as the strategy's outcome, not as a platform fault."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            blocks = []
            while True:
                blocks.append(bytearray(16 * 1024 * 1024))
            """
        ),
        SandboxLimits(memory="128m", timeout_seconds=60),
    )
    assert not result.ok
    # Either the kernel OOM-killed the container (stage "killed"), or
    # Python raised MemoryError inside it first. Both are containment.
    assert result.stage in {"killed", "configure"}
    if result.stage == "configure":
        assert result.error is not None
        assert "MemoryError" in result.error


def test_an_infinite_loop_hits_the_wall_clock_timeout() -> None:
    """The timeout is enforced by the host, so a strategy cannot decline
    it. The container is killed rather than left spinning."""
    result = run_strategy_in_sandbox(
        strategy_that(
            """
            while True:
                pass
            """
        ),
        SandboxLimits(timeout_seconds=5),
    )
    assert not result.ok
    assert result.timed_out
    assert result.stage == "timeout"
    assert result.error is not None
    assert "wall-clock" in result.error


# --- failures are outcomes, not exceptions -----------------------------------


def test_a_syntax_error_comes_back_as_a_structured_result() -> None:
    result = run_strategy_in_sandbox("def broken(:\n")
    assert not result.ok
    assert result.stage == "import"
    assert result.error is not None
    assert "SyntaxError" in result.error


def test_a_configure_that_raises_is_reported_with_its_traceback() -> None:
    result = run_strategy_in_sandbox(strategy_that('raise ValueError("deliberate")'))
    assert not result.ok
    assert result.stage == "configure"
    assert result.error is not None
    assert "deliberate" in result.error


def test_source_without_a_strategy_class_is_reported_at_the_discover_stage() -> None:
    result = run_strategy_in_sandbox(src("x = 1"))
    assert not result.ok
    assert result.stage == "discover"


# --- honesty about the isolation actually achieved ---------------------------


def test_the_result_records_which_runtime_confined_it() -> None:
    """A stored smoke-run record must say whether gVisor was involved.
    Inferring it later is impossible, and assuming it is how a platform
    ends up believing it has containment it never had."""
    result = run_strategy_in_sandbox(CONFORMING)
    assert result.runtime == "runc"
    assert result.kernel_isolated is False
    assert "host kernel is shared" in result.isolation_note


def test_selecting_gvisor_is_recorded_as_kernel_isolated() -> None:
    """The flag that changes this is one field. Asserted without running,
    because this host has no runsc -- the point is that the *bookkeeping*
    is right when a host does."""
    from trading.agent_contract.sandbox import SandboxResult

    result = SandboxResult(ok=True, stage="configure", runtime="runsc", kernel_isolated=True)
    assert "user-space kernel" in result.isolation_note
