#!/usr/bin/env bash
# Install, remove, or report the status of every live-stack launchd
# agent. Run from the repo root:
#   bash deploy/install-live-stack.sh install
#   bash deploy/install-live-stack.sh status
#   bash deploy/install-live-stack.sh uninstall
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHD_SRC="$REPO/deploy/launchd"
LAUNCHD_DST="$HOME/Library/LaunchAgents"
UV_PATH="$(command -v uv || true)"

# shellcheck source=deploy/resolve_launchd_path.sh
source "$REPO/deploy/resolve_launchd_path.sh"

LABELS=(
  com.satyam.trading.colima
  com.satyam.trading.gateway
  com.satyam.trading.crypto_ingestor
  com.satyam.trading.bar_aggregator
  com.satyam.trading.paper_engine
  com.satyam.trading.paper_alerts
  com.satyam.trading.live_supervisor
  com.satyam.trading.perp_ingestor
  com.satyam.trading.mcp
)

# `launchctl bootout` returns before the job is actually gone, and
# bootstrapping the same label meanwhile fails with "Bootstrap failed: 5:
# Input/output error". Poll (bounded, ~10s) until launchd forgets it.
wait_until_unloaded() {
  local label="$1"
  for _ in $(seq 1 50); do
    launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1 || return 0
    sleep 0.2
  done
  echo "timed out waiting for $label to unload" >&2
  return 1
}

cmd="${1:-}"

case "$cmd" in
  install)
    if [ -z "$UV_PATH" ]; then
      echo "uv not found on PATH -- install it first (https://docs.astral.sh/uv/)" >&2
      exit 1
    fi
    LAUNCHD_PATH="$(resolve_launchd_path)" || exit 1
    mkdir -p "$LAUNCHD_DST" "$REPO/logs"
    for label in "${LABELS[@]}"; do
      sed -e "s|__REPO__|$REPO|g" -e "s|__UV__|$UV_PATH|g" -e "s|__PATH__|$LAUNCHD_PATH|g" \
        "$LAUNCHD_SRC/$label.plist" > "$LAUNCHD_DST/$label.plist"
      launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
      wait_until_unloaded "$label"
      launchctl bootstrap "gui/$(id -u)" "$LAUNCHD_DST/$label.plist"
      echo "installed $label"
    done
    echo
    echo "Start order is irrelevant -- every service reconnects. Watch with:"
    echo "  tail -f $REPO/logs/*.log"
    ;;
  uninstall)
    for label in "${LABELS[@]}"; do
      launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
      rm -f "$LAUNCHD_DST/$label.plist"
      echo "removed $label"
    done
    ;;
  status)
    for label in "${LABELS[@]}"; do
      echo "--- $label ---"
      launchctl print "gui/$(id -u)/$label" 2>/dev/null | sed -n '1,4p;/state/p;/runs/p' \
        || echo "not loaded"
    done
    ;;
  *)
    echo "usage: $0 {install|uninstall|status}" >&2
    exit 1
    ;;
esac
