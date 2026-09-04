#!/usr/bin/env bash
# Assemble the image's build context from the real source tree, so the
# container runs the same bytes the host tests do rather than a copy that
# can drift. Run from the repo root.
#
# The image is built into EVERY docker context that might run it, not just
# the ambient one. This is not belt-and-braces: `STRATEGY_SANDBOX_DOCKER_
# CONTEXT` routes real runs to a second Colima VM with its own image store,
# so building only the ambient context leaves that VM holding an older
# image. Nothing fails when it does -- the payload envelope is JSON and
# additive changes decode fine against an older runner, which simply
# ignores the fields it does not know. The suite goes green while the
# container quietly runs last week's code. Rebuild both, always.
set -euo pipefail
rm -rf sandbox/trading
mkdir -p sandbox/trading/paper sandbox/trading/runtime sandbox/trading/agent_contract
touch sandbox/trading/__init__.py sandbox/trading/agent_contract/__init__.py
cp src/trading/paper/{__init__,enums,models,fills,charges,breaker}.py sandbox/trading/paper/
cp src/trading/runtime/*.py sandbox/trading/runtime/
# The SDK ships at its real package path, not as a second top-level copy.
cp src/trading/agent_contract/platform_sdk.py sandbox/trading/agent_contract/

build_into() {
  local ctx="$1"
  if docker context inspect "$ctx" >/dev/null 2>&1; then
    echo "building trading-strategy-sandbox:0.1 into context '$ctx'"
    docker --context "$ctx" build -t trading-strategy-sandbox:0.1 sandbox/
  fi
}

# The ambient context, whatever it is.
build_into "$(docker context show)"

# ...and the one strategy runs are actually routed to, if it differs.
sandbox_ctx="${STRATEGY_SANDBOX_DOCKER_CONTEXT:-}"
if [ -z "$sandbox_ctx" ] && [ -f .env.local ]; then
  sandbox_ctx="$(sed -n 's/^STRATEGY_SANDBOX_DOCKER_CONTEXT=//p' .env.local | tail -1)"
fi
if [ -n "$sandbox_ctx" ] && [ "$sandbox_ctx" != "$(docker context show)" ]; then
  build_into "$sandbox_ctx"
fi
