#!/usr/bin/env bash
# Assemble the image's build context from the real source tree, so the
# container runs the same bytes the host tests do rather than a copy that
# can drift. Run from the repo root.
set -euo pipefail
rm -rf sandbox/trading
mkdir -p sandbox/trading/paper sandbox/trading/runtime sandbox/trading/agent_contract
touch sandbox/trading/__init__.py sandbox/trading/agent_contract/__init__.py
cp src/trading/paper/{__init__,enums,models,fills,charges,breaker}.py sandbox/trading/paper/
cp src/trading/runtime/*.py sandbox/trading/runtime/
# The SDK ships at its real package path, not as a second top-level copy.
cp src/trading/agent_contract/platform_sdk.py sandbox/trading/agent_contract/
docker build -t trading-strategy-sandbox:0.1 sandbox/
