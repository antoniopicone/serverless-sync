#!/bin/sh
# Starts tailscaled only if a TS_AUTHKEY is set. The logger starts without
# one and stays off the tailnet: it's an observer, not a peer.
set -e

if [ -n "${TS_AUTHKEY:-}" ]; then
  mkdir -p /var/run/tailscale /var/lib/tailscale
  tailscaled --state=/var/lib/tailscale/tailscaled.state \
             --socket=/var/run/tailscale/tailscaled.sock &

  # --accept-dns=false is mandatory here.
  # tailscaled would rewrite /etc/resolv.conf toward MagicDNS (100.100.100.100)
  # and the container would stop resolving Docker-network names: nodes
  # would no longer find "logger". Discovery doesn't need it anyway, since
  # it reads the 100.x addresses from `tailscale status --json`, not names.
  tailscale up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${TS_HOSTNAME:-syncd-node}" \
    --accept-dns=false \
    --accept-routes=false \
    ${TS_EXTRA_ARGS:-}

  echo "tailnet: $(tailscale ip -4)  hostname=${TS_HOSTNAME:-syncd-node}"

  # the node announces itself in the peer exchange with its tailnet IP
  SYNCD_ADVERTISE="$(tailscale ip -4 | head -1):${SYNCD_PORT:-47100}"
  export SYNCD_ADVERTISE
fi

exec "$@"
