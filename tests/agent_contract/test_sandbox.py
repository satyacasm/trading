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


# --- the universe crosses back -----------------------------------------------


@pytest.mark.sandbox
def test_the_manifest_carries_its_universe_back_to_the_host() -> None:
    # The host cannot fetch bars without it; a manifest that omits the
    # universe makes stage 2 impossible rather than merely degraded.
    from trading.agent_contract.sandbox import run_strategy_in_sandbox

    source = """
from decimal import Decimal
from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


class Named(Strategy):
    def configure(self):
        return StrategyManifest(
            name="named",
            version="1.0.0",
            universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE")],
            data=DataRequest(bars="1m", history_bars=10),
            capital=Decimal("100000"),
            base_currency="INR",
        )
"""
    result = run_strategy_in_sandbox(source)
    assert result.ok is True, result.error
    assert result.manifest is not None
    assert result.manifest["universe"] == [
        {"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}
    ]


@pytest.mark.sandbox
def test_the_manifest_carries_a_query_universe_as_criteria_not_a_resolved_list() -> None:
    # A Query crosses back as the criteria that produced it, not a list
    # the container guessed at -- the host resolves it point-in-time
    # against listed_on/delisted_on (the survivorship-bias guarantee).
    # Task 5's resolve_universe has a branch that consumes exactly this
    # shape, so the key names here are load-bearing.
    from trading.agent_contract.sandbox import run_strategy_in_sandbox

    source = """
from decimal import Decimal
from platform_sdk import DataRequest, Query, Strategy, StrategyManifest


class Queried(Strategy):
    def configure(self):
        return StrategyManifest(
            name="queried",
            version="1.0.0",
            universe=Query(asset_class="EQUITY", exchange="NSE"),
            data=DataRequest(bars="1m", history_bars=10),
            capital=Decimal("100000"),
            base_currency="INR",
        )
"""
    result = run_strategy_in_sandbox(source)
    assert result.ok is True, result.error
    assert result.manifest is not None
    assert result.manifest["universe"] == {
        "asset_class": "EQUITY",
        "exchange": "NSE",
        "index": None,
    }


# --- the event loop runs inside the container --------------------------------


@pytest.mark.sandbox
def test_a_smoke_run_returns_fills_from_inside_the_container() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from trading.agent_contract.sandbox import run_smoke_in_sandbox
    from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
    from trading.paper.models import ChargeSchedule
    from trading.runtime.payload import MODE_SMOKE, SmokePayload
    from trading.runtime.provider import BarRecord

    source = """
from decimal import Decimal
from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


class Buyer(Strategy):
    def configure(self):
        return StrategyManifest(
            name="buyer",
            version="1.0.0",
            universe=[InstrumentRef(exchange="NSE", segment="CM", symbol="TEST")],
            data=DataRequest(bars="1m", history_bars=10),
            capital=Decimal("100000"),
            base_currency="INR",
        )

    def on_bar(self, ctx, bars):
        if not ctx.state.get("done"):
            ctx.state["done"] = True
            ctx.order(1, side="BUY", quantity=Decimal("10"), rationale="smoke entry")
"""

    bars = tuple(
        BarRecord(
            instrument_id=1,
            ts=datetime(2026, 9, 1, 9, minute, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume=Decimal("10"),
        )
        for minute in range(5)
    )
    schedules = (
        ChargeSchedule(
            broker="TEST",
            exchange="NSE",
            asset_class="EQUITY",
            product=Product.DELIVERY,
            charge_type=ChargeType.BROKERAGE,
            basis=ChargeBasis.FLAT_PER_ORDER,
            applies_to_side="BOTH",
            rate=Decimal("20.00"),
            cap=None,
            rounding=Rounding.TWO_DECIMALS,
            gst_base_types=(),
            effective_from=datetime(2020, 1, 1).date(),
            effective_to=None,
            source_note="test",
        ),
    )

    result = run_smoke_in_sandbox(
        SmokePayload(
            mode=MODE_SMOKE,
            source=source,
            bars={1: bars},
            charge_schedules=schedules,
            starting_cash=Decimal("100000"),
            slippage_bps=Decimal("0"),
        )
    )

    assert result.ok is True, result.error
    assert result.outcome is not None
    assert result.outcome["bar_calls"] == 5
    assert result.outcome["fills"] == 1
    # BUY 10 @ 100 = 1000 notional, plus a flat 20 brokerage charge:
    # 100000 - 1000 - 20 = 98980. (The brief's own worked example says
    # 99980, which omits the notional and is arithmetically wrong; see
    # the task report.)
    assert Decimal(result.outcome["final_cash"]) == Decimal("98980")


# --- which daemon the containers go to ----------------------------------------


@pytest.mark.parametrize("subcommand_index", [0])
def test_the_docker_context_is_named_on_the_run(subcommand_index: int) -> None:
    """gVisor lives in a second Colima VM, so reaching it means naming the
    daemon. Passing `--context` explicitly rather than relying on the
    ambient DOCKER_CONTEXT means one setting controls the whole sandbox --
    a gateway started without the env var cannot end up running strategies
    on a daemon that has no runsc while a setting says it does."""
    from trading.agent_contract.sandbox import _docker_args

    args = _docker_args(SandboxLimits(docker_context="colima-sandbox"), "probe")

    assert args[0] == "docker"
    # `--context` is a global flag: it MUST precede the subcommand, or
    # docker parses it as an argument to `run` and errors.
    assert args[1:3] == ["--context", "colima-sandbox"]
    assert args[3] == "run"
    assert args.index("--context") < args.index("run", subcommand_index)


def test_no_context_flag_is_emitted_when_none_is_configured() -> None:
    # The vacuity guard: always emitting `--context` would break every
    # machine that has only the default daemon.
    from trading.agent_contract.sandbox import _docker_args

    args = _docker_args(SandboxLimits(), "probe")

    assert "--context" not in args
    assert args[:2] == ["docker", "run"]


def test_the_kill_after_a_timeout_targets_the_same_daemon(monkeypatch) -> None:  # noqa: ANN001
    """A `docker kill` without the context goes to the DEFAULT daemon,
    where the container does not exist -- so it fails silently (check=False)
    and the runaway strategy keeps burning CPU on the other daemon forever.
    The timeout path is exactly where a leak matters most."""
    import subprocess

    from trading.agent_contract import sandbox as sandbox_module

    calls: list[list[str]] = []

    def fake_run(args, **kwargs):  # noqa: ANN001, ANN003, ANN202
        calls.append(list(args))
        if args[-1] == sandbox_module.DEFAULT_IMAGE:
            raise subprocess.TimeoutExpired(cmd=args, timeout=1.0)
        return subprocess.CompletedProcess(args, 0, b"", b"")

    monkeypatch.setattr(sandbox_module.subprocess, "run", fake_run)

    result = sandbox_module._run_payload(
        b"", SandboxLimits(docker_context="colima-sandbox", timeout_seconds=1.0)
    )

    assert result.timed_out is True
    kill = next(c for c in calls if "kill" in c)
    assert kill[:3] == ["docker", "--context", "colima-sandbox"]
    assert kill[3] == "kill"


def test_the_backtest_profile_raises_limits_without_moving_the_smoke_defaults() -> None:
    """`SandboxLimits`' docstring refuses a raised global default: "A strategy
    that legitimately needs more should say so and be granted it explicitly,
    rather than every strategy inheriting the headroom the greediest one
    needed." A backtest is that explicit grant -- so it gets a profile, and
    the smoke path keeps today's values untouched.
    """
    backtest = SandboxLimits.for_backtest()
    default = SandboxLimits()

    assert int(backtest.memory.rstrip("m")) > int(default.memory.rstrip("m"))
    assert backtest.timeout_seconds > default.timeout_seconds
    # The refusal itself: the greediest run must not set everyone's default.
    assert default.memory == "256m"
    assert default.timeout_seconds == 30.0


def test_the_backtest_profile_inherits_runtime_and_docker_context() -> None:
    """A backtest must be confined by gVisor wherever a smoke run is.

    `runtime` and `docker_context` are inherited rather than defaulted here
    because choosing one without the other yields a run confined differently
    than it claims -- a daemon with runsc installed still defaults to runc.
    """
    base = SandboxLimits(runtime="runsc", docker_context="colima-sandbox")
    profile = SandboxLimits.for_backtest(base)

    assert profile.runtime == "runsc"
    assert profile.docker_context == "colima-sandbox"
