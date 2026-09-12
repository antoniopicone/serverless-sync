#!/bin/sh
#
# Reverses install.sh: stops and removes the syncd service (systemd or
# launchd) and deletes the installed binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/uninstall.sh | sh
#   curl -fsSL .../uninstall.sh | sh -s -- --prefix /usr/local/bin

set -eu

PREFIX=""
while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

os="$(uname -s)"

if [ "$os" = "Linux" ] && command -v systemctl >/dev/null 2>&1; then
  if [ -f /etc/systemd/system/syncd.service ]; then
    log "Removing system service"
    sudo systemctl disable --now syncd 2>/dev/null || true
    sudo rm -f /etc/systemd/system/syncd.service
    sudo systemctl daemon-reload
  fi
  if [ -f "${HOME}/.config/systemd/user/syncd.service" ]; then
    log "Removing user service"
    systemctl --user disable --now syncd 2>/dev/null || true
    rm -f "${HOME}/.config/systemd/user/syncd.service"
    systemctl --user daemon-reload
  fi
elif [ "$os" = "Darwin" ] && command -v launchctl >/dev/null 2>&1; then
  label="com.syncd.agent"
  if [ -f "/Library/LaunchDaemons/${label}.plist" ]; then
    log "Removing LaunchDaemon"
    sudo launchctl bootout system "/Library/LaunchDaemons/${label}.plist" 2>/dev/null || true
    sudo rm -f "/Library/LaunchDaemons/${label}.plist"
  fi
  if [ -f "${HOME}/Library/LaunchAgents/${label}.plist" ]; then
    log "Removing LaunchAgent"
    launchctl bootout "gui/$(id -u)" "${HOME}/Library/LaunchAgents/${label}.plist" 2>/dev/null || true
    rm -f "${HOME}/Library/LaunchAgents/${label}.plist"
  fi
fi

for dir in "$PREFIX" /usr/local/bin "${HOME}/.local/bin"; do
  [ -n "$dir" ] || continue
  if [ -f "${dir}/syncd" ]; then
    log "Removing ${dir}/syncd"
    rm -f "${dir}/syncd" 2>/dev/null || sudo rm -f "${dir}/syncd"
  fi
done

log "Done. Data directories (e.g. ./data, --data) are left untouched."
