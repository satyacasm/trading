#!/usr/bin/env bash
# Ensures the `sandbox` Colima profile's dockerd knows about runsc.
# Idempotent: only touches daemon.json and restarts dockerd if the
# registration was actually missing (docs/STATUS.md's "Isolation"
# section is where this fix was first found and verified by hand).
set -euo pipefail

PROFILE=sandbox
DAEMON_JSON=/etc/docker/daemon.json
DESIRED='{"runtimes":{"runsc":{"path":"/usr/local/bin/runsc"}}}'

current="$(colima ssh --profile "$PROFILE" -- sudo cat "$DAEMON_JSON" 2>/dev/null || echo '{}')"
if echo "$current" | grep -q '"runsc"'; then
  echo "runsc already registered in the $PROFILE profile's $DAEMON_JSON"
else
  echo "$DESIRED" | colima ssh --profile "$PROFILE" -- sudo tee "$DAEMON_JSON" >/dev/null
  colima ssh --profile "$PROFILE" -- sudo systemctl restart docker
  echo "registered runsc and restarted dockerd in the $PROFILE profile"
fi

docker --context colima-sandbox info | grep -i runtime
