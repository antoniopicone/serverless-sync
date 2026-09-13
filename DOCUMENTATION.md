# syncd — documentation

See [README.md](README.md) for what this is, why it exists, and the short version of the architecture. This is the reference: every flag, the wire protocol, persistence internals, discovery, security notes, the fork recipe, and the Docker Compose demo in detail.

## Running it

```bash
cargo build --release
./target/release/syncd --device laptop-1 --port 47100
./target/release/syncd --device phone-1  --port 47100 --bootstrap 100.64.0.2:47100
```

`--device` is optional — see "Zero-config identity" below. With no flags at all, syncd picks a generated device id, binds `47100`, and stores its ledger at `~/.syncd/ledger.csv`.

| Flag | Default | Meaning |
|------|---------|---------|
| `--device NAME` | generated, persisted | this device's identity in the CRDT |
| `--port N` | `47100` | HTTP port, local and peer-to-peer alike |
| `--data PATH` | `~/.syncd/ledger.csv` | the op-log file |
| `--bootstrap host:port,...` | — | known peers, if you're not relying on discovery |
| `--peer-prefix STR` | — | only consider tailnet/LAN peers whose hostname starts with this |
| `--advertise ADDR` | auto-detected | address this node hands out to peers |
| `--interval N` | `5` | seconds between anti-entropy rounds |
| `--no-lan-discovery` | off | disable the UDP broadcast fallback |
| `--insecure-local-api` | off | drop the loopback check on `/write`/`/state`/`/v1/online` — only for a deployment where "not loopback" doesn't mean "not trusted" (this repo's own Docker demo; see below) |

### Zero-config identity

No `--device` flag needed: a device id is generated once (`resolve_device_id` in `main.rs`) and persisted next to the data directory, so there's nothing to misconfigure across machines. This matters more than it looks — two machines that both silently defaulted to the same fixed device id would corrupt their shared version vector (the CRDT under-counts one of their divergent writes as the other's), and that mistake is easy to make and hard to notice after the fact.

### Running it as a service

See [`service/`](service/) — templates, not a generic installer, because every real fork has its own binary name and defaults. See "Using this as a foundation" below for the fork recipe those templates assume.

## The wire protocol

Two tiers, matching the local/peer-to-peer split in the README. Everything is plain JSON — no envelope, no encryption at this layer (see "Security notes").

**Local** (loopback-only by default; see `--insecure-local-api`):

| Method | Path | Body | Returns |
|--------|------|------|---------|
| POST | `/write` | `{ "entity": "key", "value": "..." }` (`null` value deletes) | `{ "seq": u64, "vv": {...} }` |
| GET | `/state` | — | `{ "device", "entries": [{entity, value}, ...], "vv", "fingerprint", "online" }` |
| POST | `/v1/online` | `{ "online": bool }` | `{ "online": bool }` — flips the whole peer-to-peer surface below |

`entity` is your application's own key — syncd never interprets it, so it can be a flat name, a UUID, or anything else that fits in a string. `/write` and `/state` work no matter what `/v1/online` is currently set to: local reads/writes are never gated on the network.

**Peer-to-peer** (every interface — has to be, for peers to reach it):

| Method | Path | Body | Returns |
|--------|------|------|---------|
| GET | `/v1/node` | — | `{ proto, device_id, hostname, port, entries, fingerprint }` — a health/convergence probe |
| POST | `/v1/peers` | `{ "peers": [Peer, ...] }` | the union of what you both know (PEX) |
| POST | `/v1/vv` | — | this node's `VersionVector` |
| POST | `/v1/ops/since` | `{ "vv": VersionVector }` | `{ "ops": [Op, ...] }` — ops the caller is missing |
| POST | `/v1/ops` | `{ "ops": [Op, ...] }` | `{ "applied": usize, "vv": VersionVector }` |

Every endpoint in this tier returns `503` while `/v1/online` is set to `false` — a real two-way partition (this node stops both contacting peers and answering them), not just "stop calling out."

`sync_round()` in `main.rs` is the whole client-side algorithm, end to end, in about 40 lines: pull what the peer has that you don't, push what you have that they don't. Copy it as-is into your own fork — it touches nothing beyond the `Op`/`Peer` types.

## Where your data model plugs in

The insertion point is `core.rs`, and only that:

```rust
pub fn apply(&mut self, op: Op) -> bool     // the merge rule
pub fn local_change(&mut self, ...) -> Op   // user edit -> op
```

`main.rs` and `discovery.rs` don't know what an `Op` contains — its payload is an opaque blob to them. `Replica` is a generic last-writer-wins key/value store today; swap in a different CRDT and only these two functions (and the payload shape) change, never the transport or the discovery.

Two invariants to keep, or a node will silently diverge without telling you why:

- `apply` must stay **idempotent and commutative** — the same op applied twice, or ops applied out of order, must yield the same state.
- the conflict tie-break must stay **deterministic** — the HLC includes `device_id` for exactly this reason: two devices with the same timestamp must pick the same winner.

The fingerprint (`state_fingerprint()`) exists so you notice a divergence immediately: two aligned replicas always hash to the same value. If you replace the reducer, replace the equivalent client-side reducer in parallel — they're the same rule written twice, and that's where divergence hides.

## Using this as a foundation

This pattern has already been forked twice: once for a password manager's cross-device vault sync, once for a browser extension's local reading-list sync. Both followed the same recipe:

1. **Copy `core.rs` and `discovery.rs` over near-verbatim.** Neither knows anything about your data model or your app; `core.rs` needs no changes at all unless you're swapping the CRDT itself, and `discovery.rs` only needs its `LAN_DISCOVERY_PORT` changed if your fork's daemon might run on the same machine as this repo's own demo (or another fork's) — otherwise two daemons' broadcasts land in each other's peer cache and waste rounds on requests the other can't parse (harmless, just noise).
2. **Adapt `main.rs` and `persist.rs`.** Rename the binary, change `DEFAULT_PORT`/`default_data_dir()`, and decide whether your app talks to `serve` directly (one run mode, like this repo) or needs a second short-lived mode — a CLI, a browser Native Messaging host spawned per call — that finds the daemon via `persist::read_port_file` and relays over the loopback API (two run modes; a fork already does exactly this for browser Native Messaging framing on stdin/stdout).
3. **Decide what "value" means, and who encrypts it.** syncd relays whatever bytes it's given without needing to understand them (see "Security notes") — if your data is sensitive, encrypt client-side before calling `/write`, the way the password-manager fork does (AES-256-GCM, keyed by the vault's own master password) so this daemon never sees a plaintext credential.
4. **Copy `service/`, fill in the placeholders.** See [`service/README.md`](service/README.md).

## Discovery

A node doesn't need tailscale to be found: alongside `tailscale status --json`, every node also broadcasts a UDP announce on its local subnet and listens for the same from others (`discovery::lan_discovery_loop`, port 47188), merging whatever it hears into the same peer-exchange cache the tailnet path and PEX write into. `--advertise` (the address a node hands out to peers) falls back from an explicit flag, to the tailnet IP, to the machine's own LAN IP, and only to `127.0.0.1` if none of those resolve. Pass `--no-lan-discovery` to turn the broadcast off (e.g. on a network that filters it, or if you only want the tailnet-gated behavior).

`--peer-prefix` filters which tailnet/LAN hostnames are even considered — without it, every node probes *every* device it sees, on every round. `--bootstrap host:port,...` is the precise alternative when you already know exactly who your peers are.

## Persistence

One CSV op-log (`--data`, default `~/.syncd/ledger.csv`): one row per change, in the order it happened (`device,seq,entity,kind,value,hlc`), appended as it's accepted, never rewritten. What you actually read/write at runtime is a `BTreeMap` kept in memory (`entries` in `core.rs`) — the CSV only exists so that map survives a restart: on startup, syncd replays every row through the same `apply()` used for sync.

Two small sidecar files live next to it: `device_id` (this daemon's persisted identity — see "Zero-config identity" above) and `port` (the port `serve` actually bound, for a bridge-mode client that wasn't told `--port` directly).

Not implemented: compaction. The log only grows, so a very long-lived node eventually pays an O(n) replay on restart. This isn't a bug so much as a known limit — you can't just keep the latest row per entity and drop the rest, because a peer that's far behind still needs `/v1/ops/since` to answer with the *ops* it's missing, not just final values; doing it right needs a snapshot format on top of the log, not just a shorter log.

## Security notes

- **`/write`, `/state`, and `/v1/online` are loopback-only by default.** Nothing outside the machine should ever call them; a middleware rejects any non-loopback caller with `403`. `--insecure-local-api` turns this off — only for a deployment where that assumption doesn't hold (see below).
- **Peer-to-peer transport is unencrypted at this layer.** The primary transport (tailscale) already runs inside its own WireGuard tunnel; the LAN-broadcast fallback is opt-in and meant for a trusted network. A passive observer on an untrusted LAN can see traffic metadata (which entities changed, when, from which device) but not contents *if* your fork already encrypts values client-side (see "Using this as a foundation", point 3) — syncd relays bytes, it doesn't need to read them.
- **No secret, no registration.** An earlier version of this project had per-application secrets and an encrypted envelope; it added a registration handshake and a crypto dependency for a threat model most forks didn't actually have (see point above — if you need confidentiality, encrypt one layer up, where the data model actually lives).

## The Docker Compose demo

A small rig exercises the pattern end to end: three `syncd` nodes running this repo's own demo dataset, plus a `logger` container that watches all three from outside the sync path and renders a live dashboard — how the README's claims ("no leader", "reconverges automatically", "the logger isn't a dependency") get checked instead of just asserted.

```
docker-compose.yml     3 nodes + logger
Dockerfile              single image, two binaries
docker/entrypoint.sh    starts tailscaled if a TS_AUTHKEY is set
scenario.sh             test sequence with automatic assertions
```

No addresses are configured between the three nodes. `TS_AUTHKEY` is optional (see `env.example`): set it and the three join your real tailnet, finding each other by querying `tailscaled`; leave it unset and `docker/entrypoint.sh` skips tailscale entirely, falling back to the LAN broadcast discovery described above, over the compose file's own bridge network — no tailscale account needed for a quick local run.

```bash
docker compose up -d --build
open http://localhost:9000
./scenario.sh    # exits non-zero if a phase fails
```

Ports `47101`–`47103` for the three devices, `9000` for the logger dashboard.

### Connect / disconnect per device

Each card's switch calls `POST /v1/online` on that node directly (proxied through the logger, which is the only thing with network access to every node — see `device_online` in `logger.rs`). This is the daemon's own real partition mechanism (see "The wire protocol"), not a dashboard-only simulation: while off, that node's peer-to-peer endpoints return `503` and its anti-entropy loop stops contacting anyone, while `/write`/`/state` — and so the card's own entry list — keep working the entire time. Flip it back on and it reconverges with the others automatically.

`scenario.sh` tests the blunter failure mode instead: `docker compose pause <service>` freezes the whole container, local reads and writes included, which the `/v1/online` switch deliberately does not do. Both are useful, for different things: the switch demonstrates local-first (the point of this whole project), the pause demonstrates that a genuinely dead peer is correctly detected as unreachable rather than silently ignored.

### Why `--insecure-local-api` on the demo nodes

The logger reaches each node's (normally loopback-only) `/write`/`/state`/`/v1/online` over the Docker bridge network, which is why the three demo nodes pass `--insecure-local-api` in `docker-compose.yml` — safe here only because that bridge network has no other tenants; see "Security notes" above. Do not pass this flag on a real, non-demo install.

### The logger is not in the sync path

It has no `TS_AUTHKEY`, doesn't join the tailnet, isn't a peer, and never appears in any node's `--bootstrap`/peer list — it only polls `/state` and forwards commands, entirely outside the anti-entropy loop. `scenario.sh`'s phase 5 stops the logger, writes directly to a node, and checks that all three still converge purely via `/v1/node` (unauthenticated, no logger involved) — an observer that turns out to be a dependency would be a bug in the observer.

### The fingerprint banner

The banner at `:9000` is the **fingerprint**: a hash of the replicated state. If it matches across all nodes the replicas are aligned; if it stays different, the merge has diverged and the problem is in the CRDT, not the network.

Green requires **every** node to respond and agree. The nuance matters: if a node is suspended and you only look at the ones still alive, the remaining two agree with each other and the banner would go green in the middle of a partition — a convergence that isn't real. A node that doesn't answer is an unknown state, not an agreeing one; that's what `live_aligned` (in `/api/state`) reports separately — whether the reachable subset is at least internally consistent, a different piece of information from full convergence.
