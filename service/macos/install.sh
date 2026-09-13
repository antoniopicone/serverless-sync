#!/usr/bin/env bash
# Builds the daemon and installs it as a per-user LaunchAgent on macOS,
# started at login and kept alive (KeepAlive) — see ../linux/install.sh
# for the systemd equivalent.
#
# This is a TEMPLATE for a fork of this project (see README.md's "Using
# this as a foundation") — fill in the variables below for your fork
# before using it.
#
# Usage: ./install.sh

set -euo pipefail

# ---- fork-specific: edit these ----
BIN_NAME="syncd"                   # the [[bin]] name in your fork's Cargo.toml
SERVE_ARG=""                       # "serve" if your fork uses a subcommand, else empty
LABEL="com.example.$BIN_NAME"      # reverse-DNS style, must be unique on the machine
# ------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Building $BIN_NAME (release)…"
(cd "$REPO_ROOT" && cargo build --release --bin "$BIN_NAME")

BINARY_PATH="$REPO_ROOT/target/release/$BIN_NAME"
if [ ! -x "$BINARY_PATH" ]; then
  echo "Expected binary not found at $BINARY_PATH" >&2
  exit 1
fi

LOG_DIR="$HOME/Library/Logs/$BIN_NAME"
mkdir -p "$LOG_DIR"

AGENTS_DIR="$HOME/Library/LaunchAgents"
mkdir -p "$AGENTS_DIR"
PLIST_PATH="$AGENTS_DIR/$LABEL.plist"

sed -e "s#__BINARY_PATH__#$BINARY_PATH#" -e "s#__SERVE_ARG__#$SERVE_ARG#" \
    -e "s#__LABEL__#$LABEL#" -e "s#__LOG_DIR__#$LOG_DIR#" -e "s#__BINARY_NAME__#$BIN_NAME#" \
  "$SCRIPT_DIR/syncd.plist.template" > "$PLIST_PATH"

# An empty SERVE_ARG still leaves an empty <string></string> in
# ProgramArguments from the template's fixed two-element array — strip it
# so launchd doesn't pass an empty argv entry to the daemon.
[ -z "$SERVE_ARG" ] && /usr/bin/sed -i '' '/<string><\/string>/d' "$PLIST_PATH"

launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load -w "$PLIST_PATH"

echo "Installed and started: $PLIST_PATH"
echo "Logs: $LOG_DIR/$BIN_NAME.out.log / $BIN_NAME.err.log"
echo "Stop with: launchctl unload $PLIST_PATH"
