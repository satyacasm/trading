"""Running strategy code in a container -- the containment §5 calls for.

**What this actually provides, and what it does not.**

The plan specifies gVisor (`runsc`): a user-space kernel that intercepts
syscalls, so a container escape has to get through gVisor's own kernel
implementation before it reaches the host's. This module supports that --
`SandboxLimits.runtime` passes straight to `docker run --runtime` -- but
it does **not** assume it, because `runsc` is not available everywhere the
platform runs, and a host that lacks it must still be able to run a
strategy. On macOS the answer depends on the Docker backend: Colima's VM is
ordinary Linux and takes `runsc` normally, while Docker Desktop's does not.

Under `runc`, the guarantees below are real but of a different kind: the
strategy is confined by namespaces, cgroups, capabilities, and seccomp,
and it **shares the host kernel**. A kernel-level exploit that gVisor
would absorb reaches the host here.

That distinction is recorded on every result (`SandboxResult.runtime` and
`.kernel_isolated`) rather than left to be inferred, because "we think we
have gVisor" when we do not is precisely the false assurance this
subsystem must never offer. It also matches the actual threat model: V1
is single-user (plan §12 Q1), so the code being confined is the user's own
agent-generated strategies -- careless, not hostile. Hardened `runc` is
proportionate for that. **gVisor becomes a requirement before Phase 4**,
when strangers' code runs here; `SandboxLimits(runtime="runsc")` is the
whole change, on a host that has it.

**What is enforced, under either runtime:**

- no network at all (`--network none`) -- not a firewall, no interface
- read-only root filesystem, with a single small `tmpfs` at `/tmp`
- every Linux capability dropped, and `no-new-privileges`
- memory, CPU, and PID ceilings
- a wall-clock timeout enforced by the host, not by the container
- a non-root UID baked into the image

The strategy source is piped in over stdin rather than bind-mounted, so
no host path is exposed to the container at all -- and nothing depends on
which directories the Docker daemon is configured to share, which on
macOS is a fixed list that excludes the system temp directory. Nothing
the container writes can outlive it.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import dataclass, field
from typing import Any

from trading.runtime.payload import MODE_CONFIGURE, SmokePayload, encode_payload

__all__ = [
    "DEFAULT_IMAGE",
    "SMOKE_TIMEOUT_SECONDS",
    "SandboxLimits",
    "SandboxResult",
    "SandboxUnavailable",
    "run_smoke_in_sandbox",
    "run_strategy_in_sandbox",
]

DEFAULT_IMAGE = "trading-strategy-sandbox:0.1"

# A smoke run does far more work than `configure` alone -- it runs five
# simulated sessions through the real event loop -- so it gets a longer
# wall clock. A timeout here is still a hard failure, and a useful one:
# the same per-bar cost runs against years of bars in Phase 3.
SMOKE_TIMEOUT_SECONDS = 120.0

_RESULT_MARKER = "__SANDBOX_RESULT__"

# Runtimes that interpose their own kernel between the workload and the
# host. Anything else shares the host kernel, which the result records.
_KERNEL_ISOLATING_RUNTIMES = frozenset({"runsc", "gvisor", "kata-runtime", "io.containerd.kata.v2"})


class SandboxUnavailable(RuntimeError):
    """Docker or the sandbox image is not usable.

    Distinct from a strategy failing: this means the platform cannot run
    *any* strategy right now, and a caller should surface it as an outage
    rather than as a rejection of the user's code.
    """


@dataclass(frozen=True)
class SandboxLimits:
    """The ceilings a strategy runs under.

    Defaults are deliberately tight. A strategy that legitimately needs
    more should say so and be granted it explicitly, rather than every
    strategy inheriting the headroom the greediest one needed.
    """

    memory: str = "256m"
    cpus: str = "1.0"
    pids: int = 64
    timeout_seconds: float = 30.0
    tmpfs_size: str = "16m"
    # Passed to `docker run --runtime`. "runsc" selects gVisor where the
    # host provides it; None uses the daemon's default (runc).
    runtime: str | None = None


@dataclass(frozen=True)
class SandboxResult:
    """What happened, including how well isolated it was.

    `runtime` and `kernel_isolated` travel with every result so a caller --
    or a stored smoke-run record -- can tell later whether a run was
    confined by gVisor or merely by namespaces. A result that does not
    carry this cannot be interpreted after the fact.
    """

    ok: bool
    stage: str
    runtime: str
    kernel_isolated: bool
    strategy_class: str | None = None
    manifest: dict[str, Any] | None = None
    outcome: dict[str, Any] | None = None
    error: str | None = None
    stdout: str = ""
    stderr: str = ""
    exit_code: int | None = None
    timed_out: bool = False
    limits: SandboxLimits = field(default_factory=SandboxLimits)

    @property
    def isolation_note(self) -> str:
        if self.kernel_isolated:
            return f"runtime={self.runtime}: syscalls are mediated by a user-space kernel."
        return (
            f"runtime={self.runtime}: namespace/cgroup/seccomp confinement only -- the "
            "host kernel is shared. Adequate for single-user V1 (the code is the user's "
            "own); gVisor is required before untrusted code runs here."
        )


def _docker_args(limits: SandboxLimits, name: str) -> list[str]:
    args = [
        "docker",
        "run",
        "--rm",
        # Keep stdin open: the strategy source is written to it.
        "-i",
        "--name",
        name,
        # No network namespace at all. Not a blocked port, not a firewall
        # rule -- there is no interface to reach.
        "--network",
        "none",
        # Nothing the strategy writes survives, and nothing on the image
        # can be modified.
        "--read-only",
        "--tmpfs",
        f"/tmp:rw,noexec,nosuid,size={limits.tmpfs_size}",
        # Drop everything. The runner needs no capability whatsoever.
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        # Resource ceilings. --pids-limit is what stops a fork bomb from
        # becoming the host's problem.
        "--memory",
        limits.memory,
        "--memory-swap",
        limits.memory,  # equal to --memory means no swap at all
        "--cpus",
        limits.cpus,
        "--pids-limit",
        str(limits.pids),
        # The image already runs as uid 10001; stated again so a rebuilt
        # image that forgot USER cannot silently run as root.
        "--user",
        "10001:10001",
    ]
    if limits.runtime:
        args += ["--runtime", limits.runtime]
    args.append(DEFAULT_IMAGE)
    return args


def _parse_runner_output(stdout: str) -> dict[str, Any] | None:
    """Pull the runner's JSON line out of whatever the strategy printed.

    Scanned from the end: a strategy is free to print during import, and
    the marker line is always the last thing written.
    """
    for line in reversed(stdout.splitlines()):
        if line.startswith(_RESULT_MARKER):
            try:
                parsed: dict[str, Any] = json.loads(line[len(_RESULT_MARKER) :])
            except json.JSONDecodeError:
                return None
            return parsed
    return None


def _run_payload(raw: bytes, limits: SandboxLimits) -> SandboxResult:
    runtime = limits.runtime or "runc"
    kernel_isolated = runtime in _KERNEL_ISOLATING_RUNTIMES
    name = f"strategy-smoke-{uuid.uuid4().hex[:12]}"

    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
            _docker_args(limits, name),
            input=raw,
            capture_output=True,
            text=False,
            timeout=limits.timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        # The host timeout fired. The container is still running, so kill
        # it -- otherwise a strategy that ignores limits keeps consuming
        # its CPU share indefinitely.
        subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["docker", "kill", name],
            capture_output=True,
            check=False,
            timeout=15,
        )
        stdout = (
            exc.stdout.decode("utf-8", "replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or "")
        )
        stderr = (
            exc.stderr.decode("utf-8", "replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        return SandboxResult(
            ok=False,
            stage="timeout",
            runtime=runtime,
            kernel_isolated=kernel_isolated,
            error=f"exceeded the {limits.timeout_seconds}s wall-clock limit and was killed",
            stdout=stdout,
            stderr=stderr,
            timed_out=True,
            limits=limits,
        )
    except FileNotFoundError as exc:
        raise SandboxUnavailable("docker is not on PATH; the strategy sandbox cannot run") from exc

    stdout = completed.stdout.decode("utf-8", "replace")
    stderr = completed.stderr.decode("utf-8", "replace")

    payload = _parse_runner_output(stdout)
    if payload is None:
        # No structured result. Either the image is wrong, or the container
        # died before the runner could report -- an OOM kill (137) is the
        # common case, and it is reported as the strategy's outcome rather
        # than as a platform failure.
        if "Unable to find image" in stderr or "No such image" in stderr:
            raise SandboxUnavailable(
                f"sandbox image {DEFAULT_IMAGE!r} is not built; run "
                "`docker build -t trading-strategy-sandbox:0.1 sandbox/`"
            )
        oom = completed.returncode == 137
        return SandboxResult(
            ok=False,
            stage="killed" if oom else "no_result",
            runtime=runtime,
            kernel_isolated=kernel_isolated,
            error=(
                f"container exceeded its {limits.memory} memory limit and was killed"
                if oom
                else "the sandbox produced no structured result"
            ),
            stdout=stdout,
            stderr=stderr,
            exit_code=completed.returncode,
            limits=limits,
        )

    return SandboxResult(
        ok=bool(payload.get("ok")),
        stage=str(payload.get("stage", "unknown")),
        runtime=runtime,
        kernel_isolated=kernel_isolated,
        strategy_class=payload.get("strategy_class"),
        manifest=payload.get("manifest"),
        outcome=payload.get("outcome"),
        error=payload.get("error"),
        stdout=stdout,
        stderr=stderr,
        exit_code=completed.returncode,
        limits=limits,
    )


def run_strategy_in_sandbox(source: str, limits: SandboxLimits | None = None) -> SandboxResult:
    """Run one strategy's `configure()` inside the sandbox.

    The envelope is built here rather than by callers, so every existing
    call site keeps passing a source string. What must never happen is the
    *runner* guessing whether it received source or an envelope: format
    detection by sniffing is the compatibility shim that breaks silently a
    year later.

    Never raises on a bad strategy -- a crash, a timeout, and an
    out-of-memory kill are all outcomes, returned as a `SandboxResult`.
    Raises `SandboxUnavailable` only when Docker itself cannot be used,
    which is an outage rather than a verdict on the code.
    """
    return _run_payload(
        encode_payload(SmokePayload(mode=MODE_CONFIGURE, source=source)),
        limits or SandboxLimits(),
    )


def run_smoke_in_sandbox(
    payload: SmokePayload, limits: SandboxLimits | None = None
) -> SandboxResult:
    """Run five simulated sessions of a strategy (contract §9 stage 2).

    A longer wall clock than `configure` because it is doing far more
    work; a timeout here is still a hard failure, and a useful one -- the
    same per-bar cost runs against years of bars in Phase 3.
    """
    return _run_payload(
        encode_payload(payload),
        limits or SandboxLimits(timeout_seconds=SMOKE_TIMEOUT_SECONDS),
    )
