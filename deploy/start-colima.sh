#!/usr/bin/env bash
# Starts both Colima VMs this platform needs: `default` (TimescaleDB,
# Redis) and `sandbox` (the gVisor-isolated strategy runner). Idempotent
# -- `colima start` on an already-running profile is a fast no-op.
set -euo pipefail

colima start
colima start --profile sandbox --cpu 2 --memory 4 --disk 20
"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/provision-sandbox-vm.sh"
