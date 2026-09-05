#!/usr/bin/env bash
# Install (or reinstall) the daily option-chain recorder as a launchd agent.
# Run from the repo root: bash deploy/install-chain-recorder.sh
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.satyam.trading.chain-recorder"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"

if [ ! -x "$REPO/.venv/bin/python" ]; then
  echo "no interpreter at $REPO/.venv/bin/python -- run 'uv sync' first" >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$REPO/logs"
sed "s|__REPO__|$REPO|g" "$REPO/deploy/$LABEL.plist" > "$TARGET"

# bootout first so a reinstall picks up changes; it fails when nothing is
# loaded, which is the normal first-install case.
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$TARGET"

echo "installed $TARGET"
launchctl print "gui/$(id -u)/$LABEL" | sed -n '1,4p;/state/p;/runs/p'
echo
echo "It fires 09:10 on weekdays. The exchange calendar decides whether to record."
echo "Run it now with:   launchctl kickstart -k gui/$(id -u)/$LABEL"
echo "Watch it with:     tail -f $REPO/logs/chain_recorder.log"
echo "Stop it with:      launchctl bootout gui/$(id -u)/$LABEL"
