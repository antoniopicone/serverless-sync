# Docker Compose Test Rig

## Installing syncd

Prebuilt binaries are published on [GitHub Releases](https://github.com/antoniopicone/serverless-sync/releases) for macOS (arm64), Linux (x86_64 and arm64), and Windows (x86_64) — no Rust toolchain needed. Every tag push triggers [.github/workflows/release.yml](.github/workflows/release.yml), which builds every target and attaches a checksummed archive per platform to that tag's release.

**macOS / Linux** — detects your OS and CPU, downloads the matching build, installs it, and registers it as a service (systemd on Linux, launchd on macOS):

```bash
curl -fsSL https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.sh | sh
```

**Windows** (PowerShell) — same, registering a Scheduled Task that starts it at boot/logon and restarts it on failure:

```powershell
irm https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.ps1 | iex
```

Both are safe to re-run — an existing syncd service is stopped and replaced, not duplicated — so the same command doubles as the upgrade path.

To pass options (`--device`, `--port`, `--bootstrap`, ...) rather than take the defaults, add them after `--`:

```bash
curl -fsSL .../install.sh | sh -s -- --device my-device --port 47100
```

PowerShell's piped `iex` can't take parameters, so download it first if you need them:

```powershell
iwr https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.ps1 -OutFile install.ps1
./install.ps1 -Device my-device -Port 47100
```

Pass `--no-service` / `-NoService` to install just the binary without touching systemd/launchd/Task Scheduler. See `install/install.sh --help` for every flag, and `install/uninstall.sh` / `install/uninstall.ps1` to remove the service and binary again.

## The test rig

Three devices on your real tailnet plus one observer. No addresses configured: nodes find each other by querying `tailscaled`.

```
docker-compose.yml     3 nodes + logger
Dockerfile              single image, two binaries
docker/entrypoint.sh    starts tailscaled if a TS_AUTHKEY is set
scenario.sh             test sequence with automatic assertions
```

## Start

```bash
sudo apt-get install -y libssl-dev
cp env.example .env       # put your auth key in here
docker compose up -d --build
open http://localhost:9000
```

The auth key must be generated from the Admin console (Settings → Keys) as **Reusable** (three containers share it) and **Ephemeral** (nodes disappear from the tailnet when you stop the compose stack, instead of piling up).

Ports: `47101`, `47102`, `47103` for the three devices, `9000` for the logger.

```bash
./scenario.sh    # exits non-zero if a phase fails
```

## What you see at :9000

Each card is one device you can act on directly: edit its key/value entries inline, add or delete a key, and flip it offline with the switch. Offline doesn't mean disconnected from the UI — it means the device keeps reading and writing locally while its peer-to-peer sync simply stops, so you can watch it diverge, then flip it back online and watch it reconverge on its own, no manual reconciliation.

The banner at the top is the **fingerprint**: a hash of the replicated state. If it matches across all nodes the replicas are aligned; if it stays different, the merge has diverged and the problem is in the CRDT, not the network. Below the cards, a live event feed scrolls: `op.local`, `sync.ok`, `peer.unreachable`, `node.online`/`node.offline`.

Green requires **every** node to respond and agree. The nuance matters: if a node is suspended and you only look at the ones still alive, the remaining two agree with each other and the banner would go green in the middle of a partition — a convergence that isn't real. A node that doesn't answer is an unknown state, not an agreeing one. The `live_aligned` field in `/api/state` reports separately whether the reachable subset is at least internally consistent.

## Two non-obvious choices

**`--accept-dns=false` is mandatory.** Without it, `tailscaled` rewrites `/etc/resolv.conf` toward MagicDNS and the container stops resolving Docker-network names: nodes would no longer find `logger`. Discovery doesn't need it anyway, since it reads the `100.x` addresses straight out of `tailscale status --json`.

**`--peer-prefix=syncd-`.** Without a filter, every node would probe *every* device on your tailnet — laptop, phone, Raspberry Pi — on every round. The hostname filter is a test-rig shortcut: in production the real filter is the roster of authorized devices, not the name. The same filter applies to the LAN broadcast discovery below, for the same reason.

## Discovery beyond the tailnet

A node doesn't need tailscale to be found: alongside `tailscale status --json`, every node also broadcasts a UDP announce on its local subnet and listens for the same from others (`discovery::lan_discovery_loop`, port 47188), merging whatever it hears into the same peer-exchange cache the tailnet path and PEX write into — so a LAN-only device shows up in the sync targets with no extra plumbing. `--advertise` (the address a node hands out to peers) now falls back from an explicit flag, to the tailnet IP, to the machine's own LAN IP, and only to `127.0.0.1` if none of those resolve — so a peer without tailscale gets an address it can actually reach. Pass `--no-lan-discovery` to turn the broadcast off (e.g. on a network that filters it, or if you only want the tailnet-gated behavior above).

## The logger is not in the sync path

It has no `TS_AUTHKEY`, doesn't join the tailnet, isn't a peer. It receives pushed events from the nodes (fire-and-forget, short timeout, every error ignored) and polls each node's `/v1/state` and edit endpoints over the Docker network.

I ruled out the two alternatives for a specific reason. A fourth peer that quietly syncs would see the ops but not the exchanges between the other three. A proxy sitting in the traffic path would introduce exactly the central point this architecture denies, and would skew the timings you're trying to measure.

Phase 5 of the scenario turns the logger off, writes to a node, and checks that the three still converge. An observer that becomes a dependency is a bug in the observer.

## Persistence

Every node keeps its state in a CSV file (`--data`, default `./data/<device>.csv`, `/data` in the Docker rig — one Docker volume per node so it survives a container restart).

The file isn't a snapshot of "current values" — it's a **log of operations**: one row per change, in the order it happened (`device,seq,entity,kind,value,hlc`), appended as it's accepted, never rewritten. What you actually read and write against at runtime is a `BTreeMap` kept in memory (`entries` in `src/core.rs`) — that's the fast part, O(log n) per lookup/insert. The CSV only exists so that map isn't lost when the process restarts: on startup, every row is replayed in order through the same `apply()` used for sync, which rebuilds the in-memory map from scratch.

So: disk = history (durable, human-readable, append-only), memory = current state (fast, disposable, rebuilt from disk on boot). This also means an op received from a peer is persisted exactly like a local one — durability doesn't depend on which device typed the value first.

One thing this doesn't do: compaction. The log only ever grows, so a very long-lived node would eventually pay an O(n) replay on every restart. It's not implemented here for a reason worth knowing — you can't just keep the latest row per entity and drop the rest, because a peer that's far behind still needs `/v1/ops/since` to answer with the *ops* it's missing, not just final values, and dropping history can leave gaps a stale peer can never fill in. Doing it correctly needs a snapshot format on top of the log, not just a shorter log.

## Where your application plugs in

The service running in the containers is a fake key/value store. The insertion point is `src/core.rs`, and only that:

```rust
pub fn apply(&mut self, op: Op) -> bool     // the merge rule
pub fn local_change(&mut self, ...) -> Op   // user edit -> op
```

`discovery.rs` and `main.rs` don't know what an `Op` contains: to them, its payload is an opaque blob. When your real data model replaces the key/value store, only the reduce function and the payload shape change — not the transport, not the discovery.

Two constraints to respect when you replace the reducer, or the rig will go red without telling you why:

- `apply` must stay **idempotent and commutative**. The same op applied twice, or ops applied out of order, must yield the same state. That's what lets the network layer skip guaranteeing either ordering or exactly-once delivery.
- the conflict tie-break must stay **deterministic**. The HLC includes the `device_id` for exactly this reason: two devices with the same timestamp must pick the same winner, or they diverge silently.

If you change `core.rs`, change the equivalent client-side reducer in parallel. They're the same rule written twice, and that's where divergence hides most easily. The fingerprint exists so you notice immediately.

## Integrating this layer into your own Rust application

This rig is really three independent pieces wired together in `main.rs`. Each one stands on its own:

- **`src/core.rs`** — the CRDT reducer: `Replica`, `Op`, `OpKind`, `VersionVector`, HLC-based deterministic conflict resolution. Self-contained; it has no dependency on HTTP, tailscale, or the key/value shape this demo happens to use.
- **`src/discovery.rs`** — peer discovery over a tailnet via `tailscale status --json`, a UDP broadcast announce/listen for plain-LAN peers, plus a bidirectional peer-exchange (PEX) cache. Entirely optional — swap it for whatever already tells your app where its peers are.
- **The wire protocol** — a handful of HTTP endpoints two replicas use to reconcile state. Framework-agnostic: this demo happens to use axum, but the shapes below are plain JSON.

### 1. Bring in the reducer

Copy `core.rs` into your crate (or depend on it, if you split it into its own published crate) and rewrite the two functions where your actual data model lives — everything else in the module (version vectors, the op log, `ops_since`, `state_fingerprint`) keeps working unmodified, whatever `Op`'s payload turns out to hold:

```rust
pub fn apply(&mut self, op: Op) -> bool
pub fn local_change(&mut self, entity: &str, kind: OpKind, value: &str) -> Op
```

Keep the two invariants from the section above — idempotent/commutative merge, deterministic tie-break — and you're done with this part.

### 2. Expose the sync endpoints

Two replicas reconcile by speaking four small endpoints. Add them to whatever HTTP server your app already runs — axum, actix-web, warp, a bare `TcpListener`, it doesn't matter — only the shapes matter:

| Method | Path             | Body                        | Returns                                                      |
|--------|------------------|------------------------------|----------------------------------------------------------------|
| GET    | `/v1/vv`         | —                            | your `VersionVector`                                            |
| POST   | `/v1/ops/since`  | `{ "vv": VersionVector }`     | `{ "ops": [Op, ...] }` — ops you have that the caller is missing |
| POST   | `/v1/ops`        | `{ "ops": [Op, ...] }`        | `{ "applied": usize, "vv": VersionVector }`                     |
| POST   | `/v1/peers`      | `{ "peers": [Peer, ...] }`    | the union of what you both know (PEX only — skip this one if you already have your own discovery) |

### 3. Drive it with a periodic sync round

`sync_round()` in `main.rs` is the whole algorithm, end to end: pull what the peer has that you don't (`/v1/ops/since`), push what you have that the peer doesn't (`/v1/ops`), then a symmetric peer exchange. It's about forty lines and touches nothing from `core`/`discovery` beyond the `Op`/`Peer` types — copy it as-is and point it at whichever HTTP client your app already uses.

Spawn it on a timer (`tokio::spawn` plus `sleep` in this demo) against your list of known peer addresses. Where those addresses come from is entirely up to you:

- keep `discovery.rs` as-is if a tailnet and/or a plain LAN (via its UDP broadcast) already covers where your peers are;
- swap it for mDNS/DNS-SD if you need LAN discovery across subnets broadcast can't reach;
- swap it for a static list from config, or a call into whatever service registry you already run (Kubernetes, Consul, anything).

The reducer and the sync protocol neither know nor care — they only ever see a `Vec<String>` of `host:port` targets.

### 4. Optional: a local-first "offline" toggle

The pattern this rig's UI demonstrates — a device stays fully usable, reads and writes never block, while only its peer-to-peer surface is turned off — is just: gate the four sync endpoints above (and any discovery probe) behind a flag, and never gate your own read/write endpoints on it. See `guard_online` in `main.rs` for the roughly ten-line version.

That's the entire surface. Nothing in `core.rs` or the wire protocol assumes axum, tailscale, or even Rust on both ends — a peer can be any language that speaks the same four JSON endpoints.

## What this rig doesn't prove

It doesn't test the iOS client, which needs to run against a real node (see the PoC's own README). It doesn't test encryption, which doesn't exist yet: `payload` is plaintext and ops aren't signed. And it doesn't test compaction, because in a test session the op log never grows long enough to become a problem.
