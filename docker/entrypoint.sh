#!/bin/sh
# Avvia tailscaled solo se c'e' una TS_AUTHKEY. Il logger parte senza,
# e resta fuori dalla tailnet: e' un osservatore, non un peer.
set -e

if [ -n "${TS_AUTHKEY:-}" ]; then
  mkdir -p /var/run/tailscale /var/lib/tailscale
  tailscaled --state=/var/lib/tailscale/tailscaled.state \
             --socket=/var/run/tailscale/tailscaled.sock &

  # --accept-dns=false e' obbligatorio qui.
  # tailscaled riscriverebbe /etc/resolv.conf verso MagicDNS (100.100.100.100)
  # e il container smetterebbe di risolvere i nomi della rete Docker: i nodi
  # non troverebbero piu' "logger". La discovery non ne ha bisogno, perche'
  # usa gli IP 100.x letti da `tailscale status --json`, non i nomi.
  tailscale up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${TS_HOSTNAME:-syncd-node}" \
    --accept-dns=false \
    --accept-routes=false \
    ${TS_EXTRA_ARGS:-}

  echo "tailnet: $(tailscale ip -4)  hostname=${TS_HOSTNAME:-syncd-node}"

  # il nodo si annuncia nel peer exchange con il suo IP tailnet
  SYNCD_ADVERTISE="$(tailscale ip -4 | head -1):${SYNCD_PORT:-47100}"
  export SYNCD_ADVERTISE
fi

exec "$@"
