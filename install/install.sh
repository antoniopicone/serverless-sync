#!/usr/bin/env bash
#
# Installs the latest (or a pinned) syncd release for macOS/Linux and
# registers it as a background service (systemd on Linux, launchd on
# macOS) unless --no-service is passed. Safe to re-run: an existing
# syncd service (system or per-user) is stopped first, then replaced.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.sh | bash
#   curl -fsSL .../install.sh | bash -s -- --device laptop-1 --port 47100 --bootstrap 100.64.0.1:47100
#
# Options:
#   --version vX.Y.Z     install a specific release instead of latest
#   --device NAME        value for syncd --device (default: linux-1)
#   --port N             value for syncd --port (default: 47100)
#   --bootstrap ADDRS    value for syncd --bootstrap (comma-separated host:port)
#   --data DIR           value for syncd --data
#   --peer-prefix STR    value for syncd --peer-prefix
#   --advertise ADDR     value for syncd --advertise
#   --telemetry URL      value for syncd --telemetry
#   --interval N         value for syncd --interval
#   --no-service         install the binary only, skip service registration
#   --prefix DIR         install directory (default: /usr/local/bin or ~/.local/bin)

set -euo pipefail

REPO="antoniopicone/serverless-sync"
VERSION="latest"
DEVICE="" PORT="" BOOTSTRAP="" DATA="" PEER_PREFIX="" ADVERTISE="" TELEMETRY="" INTERVAL=""
NO_SERVICE=0
PREFIX=""

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m==>\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# Idempotency: stop whatever this script previously installed — in either
# scope (system or per-user), since a re-run as a different user than last
# time would otherwise leave two services running two copies of syncd —
# so the binary underneath isn't in use when we overwrite it below, and so
# the service that gets (re)created after actually picks up the new build
# instead of continuing to run the old one it started with.
stop_existing_service() {
  local label="com.syncd.agent"
  if [ "$platform" = "linux" ] && command -v systemctl >/dev/null 2>&1; then
    if [ -f /etc/systemd/system/syncd.service ]; then
      log "Stopping previous system service"
      sudo systemctl disable --now syncd >/dev/null 2>&1 || true
    fi
    if [ -f "${HOME}/.config/systemd/user/syncd.service" ]; then
      log "Stopping previous user service"
      systemctl --user disable --now syncd >/dev/null 2>&1 || true
    fi
  elif [ "$platform" = "macos" ] && command -v launchctl >/dev/null 2>&1; then
    if [ -f "/Library/LaunchDaemons/${label}.plist" ]; then
      log "Stopping previous LaunchDaemon"
      sudo launchctl bootout system "/Library/LaunchDaemons/${label}.plist" >/dev/null 2>&1 || true
    fi
    if [ -f "${HOME}/Library/LaunchAgents/${label}.plist" ]; then
      log "Stopping previous LaunchAgent"
      launchctl bootout "gui/$(id -u)" "${HOME}/Library/LaunchAgents/${label}.plist" >/dev/null 2>&1 || true
    fi
  fi
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --device) DEVICE="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
    --data) DATA="$2"; shift 2 ;;
    --peer-prefix) PEER_PREFIX="$2"; shift 2 ;;
    --advertise) ADVERTISE="$2"; shift 2 ;;
    --telemetry) TELEMETRY="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --no-service) NO_SERVICE=1; shift ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    --help) grep '^#' "$0" | sed '1d;s/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Darwin)
    platform=macos
    case "$arch" in
      arm64) target="aarch64-apple-darwin" ;;
      *) die "unsupported: no macOS Intel build is published; build from source instead." ;;
    esac
    ;;
  Linux)
    platform=linux
    case "$arch" in
      x86_64|amd64) target="x86_64-unknown-linux-gnu" ;;
      aarch64|arm64) target="aarch64-unknown-linux-gnu" ;;
      *) die "unsupported architecture: $arch" ;;
    esac
    ;;
  *) die "unsupported OS: $os (this script covers macOS and Linux; see install.ps1 for Windows)" ;;
esac

asset="syncd-${target}.tar.gz"
if [ "$VERSION" = "latest" ]; then
  base_url="https://github.com/${REPO}/releases/latest/download"
else
  base_url="https://github.com/${REPO}/releases/download/${VERSION}"
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

log "Downloading ${asset} (${VERSION})"
curl -fsSL "${base_url}/${asset}" -o "${workdir}/${asset}" \
  || die "download failed: ${base_url}/${asset}"
curl -fsSL "${base_url}/${asset}.sha256" -o "${workdir}/${asset}.sha256" \
  || die "download failed: ${base_url}/${asset}.sha256"

log "Verifying checksum"
( cd "$workdir" && \
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${asset}.sha256"
  else
    shasum -a 256 -c "${asset}.sha256"
  fi
) || die "checksum verification failed"

log "Extracting"
tar xzf "${workdir}/${asset}" -C "$workdir"
binary="${workdir}/syncd-${target}/syncd"
[ -x "$binary" ] || chmod +x "$binary"

stop_existing_service

# Pick an install directory: prefer /usr/local/bin when we can write there
# (root, or sudo group on macOS), otherwise fall back to a per-user path.
if [ -z "$PREFIX" ]; then
  if [ -w /usr/local/bin ] || [ "$(id -u)" = "0" ]; then
    PREFIX="/usr/local/bin"
  else
    PREFIX="${HOME}/.local/bin"
  fi
fi

as_root=0
if [ "$(id -u)" = "0" ]; then
  as_root=1
elif [ "$PREFIX" = "/usr/local/bin" ] && command -v sudo >/dev/null 2>&1; then
  as_root=1
fi

mkdir -p "$PREFIX" 2>/dev/null || sudo mkdir -p "$PREFIX"
dest="${PREFIX}/syncd"
if [ -w "$PREFIX" ]; then
  cp "$binary" "$dest"
else
  sudo cp "$binary" "$dest"
fi
log "Installed syncd to ${dest}"

case ":$PATH:" in
  *":${PREFIX}:"*) ;;
  *) warn "${PREFIX} is not on your PATH. Add: export PATH=\"${PREFIX}:\$PATH\"" ;;
esac

if [ "$NO_SERVICE" = "1" ]; then
  log "Skipping service registration (--no-service). Run manually: ${dest} --device ... --port ..."
  exit 0
fi

# Build the syncd argument list from whichever options were provided;
# syncd falls back to its own defaults (device=linux-1, port=47100, ...)
# for anything left unset.
args=()
[ -n "$DEVICE" ]      && args+=(--device "$DEVICE")
[ -n "$PORT" ]        && args+=(--port "$PORT")
[ -n "$BOOTSTRAP" ]   && args+=(--bootstrap "$BOOTSTRAP")
[ -n "$DATA" ]        && args+=(--data "$DATA")
[ -n "$PEER_PREFIX" ] && args+=(--peer-prefix "$PEER_PREFIX")
[ -n "$ADVERTISE" ]   && args+=(--advertise "$ADVERTISE")
[ -n "$TELEMETRY" ]   && args+=(--telemetry "$TELEMETRY")
[ -n "$INTERVAL" ]    && args+=(--interval "$INTERVAL")

if [ "$platform" = "linux" ]; then
  if ! command -v systemctl >/dev/null 2>&1; then
    warn "systemd not found; skipping service registration. Run manually: ${dest} ${args[*]}"
    exit 0
  fi

  exec_line="${dest}"
  for a in "${args[@]+"${args[@]}"}"; do exec_line+=" $(printf '%q' "$a")"; done

  if [ "$as_root" = "1" ]; then
    wanted_by="multi-user.target"
  else
    wanted_by="default.target"
  fi

  unit_content="[Unit]
Description=syncd - serverless sync node
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=${exec_line}
Restart=on-failure
RestartSec=2

[Install]
WantedBy=${wanted_by}
"

  if [ "$as_root" = "1" ]; then
    unit_path="/etc/systemd/system/syncd.service"
    printf '%s' "$unit_content" > "${workdir}/syncd.service"
    sudo cp "${workdir}/syncd.service" "$unit_path"
    sudo systemctl daemon-reload
    sudo systemctl enable --now syncd
    log "Installed and started system service: systemctl status syncd"
  else
    unit_dir="${HOME}/.config/systemd/user"
    mkdir -p "$unit_dir"
    printf '%s' "$unit_content" > "${unit_dir}/syncd.service"
    systemctl --user daemon-reload
    systemctl --user enable --now syncd
    loginctl enable-linger "$(id -un)" 2>/dev/null || true
    log "Installed and started user service: systemctl --user status syncd"
  fi

elif [ "$platform" = "macos" ]; then
  if ! command -v launchctl >/dev/null 2>&1; then
    warn "launchctl not found; skipping service registration. Run manually: ${dest} ${args[*]}"
    exit 0
  fi

  args_xml=""
  for a in "${args[@]+"${args[@]}"}"; do
    args_xml+="        <string>${a}</string>
"
  done

  label="com.syncd.agent"
  plist_body="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key>
    <string>${label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${dest}</string>
${args_xml}    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/syncd.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/syncd.err.log</string>
</dict>
</plist>
"

  if [ "$as_root" = "1" ]; then
    plist_path="/Library/LaunchDaemons/${label}.plist"
    printf '%s' "$plist_body" > "${workdir}/${label}.plist"
    sudo cp "${workdir}/${label}.plist" "$plist_path"
    sudo launchctl bootstrap system "$plist_path" 2>/dev/null || sudo launchctl load -w "$plist_path"
    log "Installed and started LaunchDaemon: sudo launchctl print system/${label}"
  else
    plist_dir="${HOME}/Library/LaunchAgents"
    mkdir -p "$plist_dir"
    plist_path="${plist_dir}/${label}.plist"
    printf '%s' "$plist_body" > "$plist_path"
    launchctl bootstrap "gui/$(id -u)" "$plist_path" 2>/dev/null || launchctl load -w "$plist_path"
    log "Installed and started LaunchAgent: launchctl print gui/$(id -u)/${label}"
  fi
fi
