# syncd — single image for both the nodes and the logger.
# Nodes also run tailscaled; the logger uses the same image but doesn't
# join the tailnet (it talks to nodes over the Docker network, out of band).

FROM rust:1-slim-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg iptables iproute2 procps \
 && curl -fsSL https://pkgs.tailscale.com/stable/debian/bookworm.noarmor.gpg \
      > /usr/share/keyrings/tailscale-archive-keyring.gpg \
 && curl -fsSL https://pkgs.tailscale.com/stable/debian/bookworm.tailscale-keyring.list \
      > /etc/apt/sources.list.d/tailscale.list \
 && apt-get update && apt-get install -y --no-install-recommends tailscale \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/syncd  /usr/local/bin/syncd
COPY --from=build /src/target/release/logger /usr/local/bin/logger
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["syncd"]
