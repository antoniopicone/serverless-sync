#!/usr/bin/env bash
# Builds the daemon and installs it as a per-user systemd service on
# Linux, started at login and kept running (Restart=on-failure).
#
# This is a TEMPLATE for a fork of this project (see README.md's "Using
# this as a foundation") — fill in the variables below for your fork
# before using it; as shipped here it installs this repo's own demo
# `syncd` binary, which is only useful for trying the pattern out.
#
# Usage: ./install.sh [-- extra daemon args]
# Example: ./install.sh -- --device my-laptop --bootstrap 100.64.0.2:47100

set -euo pipefail

# ---- fork-specific: edit these ----
BIN_NAME="syncd"            # the [[bin]] name in your fork's Cargo.toml
SERVE_ARG=""                # "serve" if your fork uses a subcommand (see
                             # reading-list-syncd's bridge-mode pattern in
                             # README.md), empty if it takes flags directly
DESCRIPTION="syncd — background sync daemon"
# ------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
EXTRA_ARGS=("$@")

echo "Building $BIN_NAME (release)…"
(cd "$REPO_ROOT" && cargo build --release --bin "$BIN_NAME")

BINARY_PATH="$REPO_ROOT/target/release/$BIN_NAME"
if [ ! -x "$BINARY_PATH" ]; then
  echo "Expected binary not found at $BINARY_PATH" >&2
  exit 1
fi

UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"
UNIT_PATH="$UNIT_DIR/$BIN_NAME.service"

EXEC_START="$BINARY_PATH"
[ -n "$SERVE_ARG" ] && EXEC_START="$EXEC_START $SERVE_ARG"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
  EXEC_START="$EXEC_START ${EXTRA_ARGS[*]}"
fi

sed -e "s#__BINARY_PATH__#$BINARY_PATH#" -e "s#__SERVE_ARG__#$SERVE_ARG#" -e "s#__DESCRIPTION__#$DESCRIPTION#" \
  "$SCRIPT_DIR/syncd.service.template" > "$UNIT_PATH"
sed -i "s#^ExecStart=.*#ExecStart=$EXEC_START#" "$UNIT_PATH"

systemctl --user daemon-reload
systemctl --user enable --now "$BIN_NAME.service"

echo "Installed and started: $UNIT_PATH"
echo "Check status with: systemctl --user status $BIN_NAME"
echo "Follow logs with:   journalctl --user -u $BIN_NAME -f"
echo
echo "Running even when logged out (e.g. a headless box) needs lingering, once:"
echo "  loginctl enable-linger \$(whoami)"
