#!/usr/bin/env bash
# Ensures the `sandbox` Colima profile's dockerd knows about runsc.
# Idempotent: only touches daemon.json and restarts dockerd if the
# registration was actually missing (docs/STATUS.md's "Isolation"
# section is where this fix was first found and verified by hand).
#
# The merge happens on the HOST with python3 (ships with macOS), not
# by overwriting the VM's daemon.json wholesale -- an earlier version
# of this script clobbered the file with just the runsc key, silently
# discarding anything else an operator had set. See
# deploy/daemon_json_merge.py for the merge logic itself (importable,
# unit-tested independent of this script's ssh/backup/restart plumbing).
set -euo pipefail

PROFILE=sandbox
DAEMON_JSON=/etc/docker/daemon.json
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MERGE_HELPER="$SCRIPT_DIR/daemon_json_merge.py"

# Reachability and existence are checked together: `test -f` over ssh
# exits 0 (file exists), 1 (file missing -- ssh itself still succeeded,
# so the VM answered), or something else entirely (ssh/connection
# failure, e.g. 255). Only exit 1 is safe to treat as "no file yet";
# anything else means we could not reliably reach the VM and must abort
# rather than silently proceed as if daemon.json were empty.
status=0
test_output="$(colima ssh --profile "$PROFILE" -- test -f "$DAEMON_JSON" 2>&1)" || status=$?

if [ "$status" -eq 0 ]; then
  file_exists=true
elif [ "$status" -eq 1 ]; then
  file_exists=false
else
  echo "ERROR: cannot reach the $PROFILE Colima VM (colima ssh exited $status): $test_output" >&2
  exit 1
fi

if [ "$file_exists" = true ]; then
  if ! current_json="$(colima ssh --profile "$PROFILE" -- sudo cat "$DAEMON_JSON")"; then
    echo "ERROR: cannot reach the $PROFILE Colima VM to read $DAEMON_JSON" >&2
    exit 1
  fi
else
  current_json='{}'
fi

current_canonical="$(printf '%s' "$current_json" | python3 -c \
  "import json, sys; print(json.dumps(json.load(sys.stdin), sort_keys=True))")"
merged_json="$(printf '%s' "$current_json" | python3 "$MERGE_HELPER")"

if [ "$current_canonical" == "$merged_json" ]; then
  echo "already provisioned"
  exit 0
fi

if [ "$file_exists" = true ]; then
  backup="$DAEMON_JSON.bak-$(date +%Y%m%d%H%M%S)"
  colima ssh --profile "$PROFILE" -- sudo cp "$DAEMON_JSON" "$backup"
  echo "backed up $DAEMON_JSON to $backup"
fi

printf '%s' "$merged_json" | colima ssh --profile "$PROFILE" -- sudo tee "$DAEMON_JSON" >/dev/null

# Assumes the sandbox VM's init system is systemd, which is what
# Colima's default Ubuntu-based guest runs; if that guest ever changes,
# this restart command needs to change with it.
colima ssh --profile "$PROFILE" -- sudo systemctl restart docker
echo "registered runsc and restarted dockerd in the $PROFILE profile"

docker --context colima-sandbox info | grep -i runtime
