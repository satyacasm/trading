"""`SandboxLimits` as a value object -- no container involved.

Kept out of `test_sandbox.py` deliberately: that module carries a
file-level `pytest.mark.sandbox` because its tests spawn real containers,
and the default suite runs with `-m "not sandbox"`. These are pure unit
tests, and inheriting that marker would have quietly excluded them from
every default run -- passing when invoked directly and never running in
CI, which is worse than not having them.
"""

from __future__ import annotations

from trading.agent_contract.sandbox import SandboxLimits


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
