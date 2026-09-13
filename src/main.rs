//! syncd — a foundation for an embedded, single-purpose sync daemon.
//!
//! This repo is meant to be forked, not installed: copy `core.rs` and
//! `discovery.rs` near-verbatim into your own application's daemon, adapt
//! `main.rs`/`persist.rs` to your data model and defaults (as this repo's
//! own two real forks did — see README.md's "Using this as a foundation"),
//! and you have a background daemon that keeps one dataset in sync across
//! a person's own devices, no server in the middle. This repo's own
//! `main.rs` hosts one generic key/value dataset so the pattern can be
//! demonstrated and tested (see docker-compose.yml) without inventing a
//! throwaway data model.
//!
//! One process, one dataset, no registry: unlike an earlier version of
//! this project, syncd does not multiplex several applications' data
//! behind a registration handshake. If you need several independent
//! datasets on one machine, run several copies of your fork (see
//! README.md) — simpler to reason about, and it's what every real fork of
//! this project has actually needed so far.
//!
//! Usage:
//!   syncd --device linux-1 --port 47100
//!   syncd --device linux-2 --port 47101 --bootstrap 127.0.0.1:47100
//!   syncd --device linux-3 --port 47100 --no-lan-discovery
//!
//! `--device` is optional: left unset, a device id is generated once and
//! persisted next to the data directory (see `resolve_device_id`), so
//! there's nothing to misconfigure on a fresh install.
//!
//! The peer-to-peer half of the HTTP API (`/v1/node`, `/v1/peers`,
//! `/v1/vv`, `/v1/ops/since`, `/v1/ops`) listens on 0.0.0.0, so it's
//! reachable from both the LAN and a tailnet interface — it has to be,
//! that's the whole point. The local-client half (`/write`, `/state`,
//! `/v1/online`) is rejected unless the caller is on loopback, since
//! nothing outside this machine should ever call those — see
//! `--insecure-local-api` below if that genuinely isn't true for your
//! deployment (e.g. containers on their own isolated bridge network, as
//! in this repo's own Docker demo).
//!
//! `POST /v1/online {"online": bool}` is a local switch: it starts
//! online, and flipping it off gates the whole
//! peer-to-peer surface above (both outbound anti-entropy and inbound
//! requests) while leaving `/write`/`/state` working exactly as before —
//! local-first, in other words: this device keeps reading and writing,
//! diverges from its peers while "offline", and reconverges with no
//! manual reconciliation once flipped back.
//!
//! Peers are found via tailscale (if present), a LAN broadcast
//! announce/listen (unless --no-lan-discovery), and peer exchange — see
//! discovery.rs. Transport is unencrypted at this layer: the primary
//! transport (tailscale) already runs inside its own WireGuard tunnel, and
//! the LAN-broadcast fallback is opt-in and meant for a trusted network.
//! If your data is sensitive, encrypt `value` client-side before calling
//! `/write` (see README.md's "Security notes") — this daemon relays
//! whatever bytes it's given without ever needing to understand them.

mod core;
mod discovery;
mod persist;

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use core::{Op, OpKind, Replica, VersionVector};
use discovery::{Peer, PexCache, AGENT_PORT};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const PROTO: u32 = 1;

fn arg(args: &[String], k: &str) -> Option<String> {
    let eq_prefix = format!("{k}=");
    args.iter().find_map(|a| a.strip_prefix(&eq_prefix).map(String::from)).or_else(|| {
        args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned()
    })
}

fn generate_device_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let pid = std::process::id();
    format!("device-{nanos:x}-{pid:x}")
}

/// Resolves this daemon's device id: `--device` always wins; otherwise a
/// persisted one from a previous run at this `--data` dir; otherwise a
/// freshly generated one, persisted for next time. Auto-generating by
/// default (rather than a fixed fallback like "device-1") matters because
/// this is meant to run as an unattended service: two machines that both
/// silently defaulted to the same device id would corrupt their shared
/// version vector (the CRDT under-counts one of their divergent writes as
/// the other's), and that mistake is easy to make and hard to notice.
fn resolve_device_id(args: &[String], data_dir: &Path) -> String {
    if let Some(explicit) = arg(args, "--device") {
        return explicit;
    }
    if let Some(persisted) = persist::read_device_id(data_dir) {
        return persisted;
    }
    let generated = generate_device_id();
    if let Err(e) = persist::write_device_id(data_dir, &generated) {
        eprintln!("[persist] could not persist generated device id: {e}");
    }
    generated
}

#[derive(Clone)]
struct App {
    replica: Arc<Mutex<Replica>>,
    log: Arc<persist::OpLog>,
    device: String,
    hostname: String,
    port: u16,
    /// Address this node announces itself with in the peer exchange.
    /// Must be the tailnet IP (100.x.y.z:port), not 127.0.0.1, otherwise
    /// PEX propagates addresses nobody else can use.
    advertise: String,
    peer_prefix: String,
    pex: PexCache,
    /// Local switch, flipped via `/v1/online` — loopback-only, like
    /// `/write`/`/state`. Gates the peer-to-peer surface only: local reads
    /// and writes never stop working, "disconnected" only means this
    /// device stops both contacting and answering other devices, so it
    /// diverges locally and reconverges automatically once flipped back.
    /// See docker-compose.yml's demo for why this exists alongside real
    /// `docker compose pause` — the two aren't redundant: pausing freezes
    /// the process entirely (no local reads/writes either), this simulates
    /// only losing connectivity.
    online: Arc<AtomicBool>,
}

fn guard_online(app: &App) -> Result<(), StatusCode> {
    if app.online.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

fn apply_and_persist(app: &App, op: Op) -> bool {
    let applied = app.replica.lock().unwrap().apply(op.clone());
    if applied {
        if let Err(e) = app.log.append(&op) {
            eprintln!("[persist] failed to append op ({} seq {}): {e}", op.device, op.seq);
        }
    }
    applied
}

// ---------------------------------------------------------- local control
// (loopback-only: this is what a client living on this same machine — a
// CLI, a browser extension's native-messaging bridge, a GUI app — calls
// on the local user's behalf. See --insecure-local-api in main() for the
// one legitimate exception.)

async fn require_loopback(ConnectInfo(addr): ConnectInfo<SocketAddr>, req: axum::extract::Request, next: Next) -> Result<Response, StatusCode> {
    if addr.ip().is_loopback() {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

#[derive(Deserialize)]
struct WriteReq {
    entity: String,
    value: Option<String>,
}

#[derive(Serialize)]
struct WriteResp {
    seq: u64,
    vv: VersionVector,
}

async fn local_write(State(app): State<App>, Json(req): Json<WriteReq>) -> Json<WriteResp> {
    let op = {
        let mut replica = app.replica.lock().unwrap();
        match req.value {
            Some(v) => replica.local_change(&req.entity, OpKind::Upsert, &v),
            None => replica.local_change(&req.entity, OpKind::Delete, ""),
        }
    };
    if let Err(e) = app.log.append(&op) {
        eprintln!("[persist] failed to append local op (seq {}): {e}", op.seq);
    }
    let vv = app.replica.lock().unwrap().version_vector();
    Json(WriteResp { seq: op.seq, vv })
}

#[derive(Serialize)]
struct StateResp {
    device: String,
    entries: Vec<serde_json::Value>,
    vv: VersionVector,
    fingerprint: String,
    online: bool,
}

async fn local_state(State(app): State<App>) -> Json<StateResp> {
    let replica = app.replica.lock().unwrap();
    Json(StateResp {
        device: app.device.clone(),
        entries: replica.entries(),
        vv: replica.version_vector(),
        fingerprint: format!("{:x}", replica.state_fingerprint()),
        online: app.online.load(Ordering::Relaxed),
    })
}

#[derive(Deserialize)]
struct OnlineReq {
    online: bool,
}

#[derive(Serialize)]
struct OnlineResp {
    online: bool,
}

/// Never gated by itself — it must stay reachable even while "offline",
/// otherwise there'd be no way to flip it back.
async fn set_online(State(app): State<App>, Json(req): Json<OnlineReq>) -> Json<OnlineResp> {
    app.online.store(req.online, Ordering::Relaxed);
    Json(OnlineResp { online: req.online })
}

// ------------------------------------------------------------ peer-to-peer
// (reachable from the LAN/tailnet — plain JSON, no envelope; see the
// module doc comment for why encryption belongs one layer up if you need
// it at all.)

#[derive(Serialize)]
struct NodeInfo {
    proto: u32,
    device_id: String,
    hostname: String,
    port: u16,
    entries: usize,
    fingerprint: String,
}

async fn node(State(app): State<App>) -> Result<Json<NodeInfo>, StatusCode> {
    guard_online(&app)?;
    let replica = app.replica.lock().unwrap();
    Ok(Json(NodeInfo {
        proto: PROTO,
        device_id: app.device.clone(),
        hostname: app.hostname.clone(),
        port: app.port,
        entries: replica.entries().len(),
        fingerprint: format!("{:x}", replica.state_fingerprint()),
    }))
}

#[derive(Serialize, Deserialize, Default)]
struct PexReq {
    #[serde(default)]
    peers: Vec<Peer>,
}

/// Peer exchange, BIDIRECTIONAL. The caller sends its list, the callee
/// absorbs it and returns the union — knowledge of the mesh must
/// propagate both ways on every contact, otherwise discovery depends on
/// startup order (see discovery.rs's own doc comment).
async fn peers(State(app): State<App>, Json(req): Json<PexReq>) -> Result<Json<Vec<Peer>>, StatusCode> {
    guard_online(&app)?;
    app.pex.merge(req.peers);
    let me = app.device.clone();
    let mut out: Vec<Peer> = app.pex.list().into_iter().filter(|p| p.device_id != me).collect();
    out.push(Peer { hostname: app.hostname.clone(), addr: app.advertise.clone(), device_id: me });
    Ok(Json(out))
}

async fn vv(State(app): State<App>) -> Result<Json<VersionVector>, StatusCode> {
    guard_online(&app)?;
    Ok(Json(app.replica.lock().unwrap().version_vector()))
}

#[derive(Serialize, Deserialize)]
struct SinceReq {
    vv: VersionVector,
}

#[derive(Serialize, Deserialize)]
struct OpsResp {
    ops: Vec<Op>,
}

async fn ops_since(State(app): State<App>, Json(req): Json<SinceReq>) -> Result<Json<OpsResp>, StatusCode> {
    guard_online(&app)?;
    let ops = app.replica.lock().unwrap().ops_since(&req.vv);
    Ok(Json(OpsResp { ops }))
}

#[derive(Serialize, Deserialize)]
struct PushReq {
    ops: Vec<Op>,
}

#[derive(Serialize, Deserialize)]
struct PushResp {
    applied: usize,
    vv: VersionVector,
}

async fn ops_push(State(app): State<App>, Json(req): Json<PushReq>) -> Result<Json<PushResp>, StatusCode> {
    guard_online(&app)?;
    let mut ops = req.ops;
    // sort by (device, seq): apply rejects causal gaps
    ops.sort_by_key(|o| (o.device.clone(), o.seq));
    let applied = ops.into_iter().filter(|o| apply_and_persist(&app, o.clone())).count();
    let vv = app.replica.lock().unwrap().version_vector();
    Ok(Json(PushResp { applied, vv }))
}

// ------------------------------------------------------------ sync client

/// One sync round with a peer. Symmetric and with no shared state: pull
/// whatever I'm missing, push whatever they're missing.
async fn sync_round(app: &App, addr: &str) -> Result<(usize, usize), String> {
    // Explicit timeout: a stale target (e.g. an old ephemeral node with the
    // same hostname, still visible but dead) must not block the
    // anti-entropy round long enough to delay syncing with live peers.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| e.to_string())?;

    let my_vv = app.replica.lock().unwrap().version_vector();
    let pulled: OpsResp = http
        .post(format!("http://{addr}/v1/ops/since"))
        .json(&SinceReq { vv: my_vv })
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let mut ops = pulled.ops;
    ops.sort_by_key(|o| (o.device.clone(), o.seq));
    let n_pull = ops.into_iter().filter(|o| apply_and_persist(app, o.clone())).count();

    let their_vv: VersionVector = http
        .post(format!("http://{addr}/v1/vv"))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let mine = app.replica.lock().unwrap().ops_since(&their_vv);
    let n_push = if mine.is_empty() {
        0
    } else {
        let resp: PushResp = http
            .post(format!("http://{addr}/v1/ops"))
            .json(&PushReq { ops: mine })
            .send().await.map_err(|e| e.to_string())?
            .json().await.map_err(|e| e.to_string())?;
        resp.applied
    };

    Ok((n_pull, n_push))
}

async fn exchange_peers(app: &App, addr: &str) {
    let Ok(http) = reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)).build() else { return };
    let mut mine_peers = app.pex.list();
    mine_peers.push(Peer { hostname: app.hostname.clone(), addr: app.advertise.clone(), device_id: app.device.clone() });
    if let Ok(resp) = http.post(format!("http://{addr}/v1/peers")).json(&PexReq { peers: mine_peers }).send().await {
        if let Ok(list) = resp.json::<Vec<Peer>>().await {
            app.pex.merge(list);
        }
    }
}

/// Anti-entropy: this is WHERE correctness lives, not in push
/// notifications. Every tick, this node exchanges peers with — then
/// reconciles against — every peer it knows about; if a peer was offline
/// at write time, the next tick catches up regardless.
async fn antientropy_loop(app: App, bootstrap: Vec<String>, interval_secs: u64) {
    loop {
        // "offline" (see /v1/online): don't contact anyone until back
        // online. Local writes keep working (see /write) — only the
        // peer-to-peer side stops, and the peer-facing endpoints reject
        // inbound requests too (see guard_online), so this is a real
        // two-way partition, not just "stop calling out". The device
        // diverges locally and reconverges automatically once flipped
        // back — no manual reconciliation.
        if app.online.load(Ordering::Relaxed) {
            let mut targets: Vec<String> = bootstrap.clone();
            for (host, ip) in discovery::tailnet_candidates(&app.peer_prefix) {
                let _ = host;
                // Assumes the peer runs on the same port as this node —
                // true by construction for two instances of the same
                // fork. `tailscale status` has no way to report the
                // peer's actual port, so this is the best available
                // guess; --bootstrap is the escape hatch for a peer that
                // (unusually) runs on a different one.
                targets.push(format!("{ip}:{}", app.port));
            }
            for p in app.pex.list() {
                targets.push(p.addr);
            }
            targets.sort();
            targets.dedup();

            for t in targets.into_iter().filter(|t| *t != app.advertise) {
                exchange_peers(&app, &t).await;
                match sync_round(&app, &t).await {
                    Ok((pulled, pushed)) => {
                        if pulled + pushed > 0 {
                            println!("[sync] <-> {t}: +{pulled} received, +{pushed} sent");
                        }
                    }
                    Err(e) => eprintln!("[sync] <-> {t}: unreachable ({e})"),
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Accepts both "--flag value" and "--flag=value": docker-compose.yml
    // uses the latter form.
    let data_path: PathBuf = arg(&args, "--data").map(PathBuf::from).unwrap_or_else(persist::default_ledger_path);
    let data_dir = data_path.parent().map(PathBuf::from).unwrap_or_else(persist::default_data_dir);

    let device = resolve_device_id(&args, &data_dir);
    let port: u16 = arg(&args, "--port").and_then(|p| p.parse().ok()).unwrap_or(AGENT_PORT);
    let bootstrap: Vec<String> = arg(&args, "--bootstrap")
        .map(|b| b.split(',').map(String::from).collect())
        .unwrap_or_default();
    let peer_prefix = arg(&args, "--peer-prefix").unwrap_or_default();
    let lan_discovery_enabled = !args.iter().any(|a| a == "--no-lan-discovery");
    let interval: u64 = arg(&args, "--interval").and_then(|i| i.parse().ok()).unwrap_or(5);
    // Turns off the loopback check on /write and /state. Only for
    // deployments where "the caller is on loopback" doesn't mean "the
    // caller is trusted" the way it does on a normal single-user machine
    // — e.g. this repo's own Docker demo, where the logger reaches each
    // node over the isolated Docker bridge network, not loopback, but
    // that network has no other tenants. Do not use this on a real install.
    let insecure_local_api = args.iter().any(|a| a == "--insecure-local-api");

    let hostname = std::process::Command::new("hostname")
        .output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| device.clone());

    // --advertise 100.x.y.z:47100  (tailnet IP). Default: first tailnet IP
    // found, else this machine's own LAN IP, else loopback for local
    // testing (nothing routable was found at all).
    let lan_ip = discovery::local_lan_ip();
    let advertise = arg(&args, "--advertise")
        .or_else(|| std::env::var("SYNCD_ADVERTISE").ok())
        .or_else(|| discovery::my_tailnet_ip().map(|ip| format!("{ip}:{port}")))
        .or_else(|| lan_ip.clone().map(|ip| format!("{ip}:{port}")))
        .unwrap_or_else(|| format!("127.0.0.1:{port}"));

    // Persistence: replay whatever this device already had on disk, then
    // keep appending to the same file. A fresh device with no file yet
    // just starts empty.
    let mut replica = Replica::new(device.clone());
    match persist::load(&data_path) {
        Ok(ops) => {
            let n = ops.len();
            for op in ops {
                replica.apply(op);
            }
            if n > 0 {
                println!("[persist] replayed {n} ops from {}", data_path.display());
            }
        }
        Err(e) => eprintln!("[persist] failed to read {}: {e} (starting empty)", data_path.display()),
    }
    let log = persist::OpLog::open(&data_path)
        .unwrap_or_else(|e| panic!("[persist] cannot open {}: {e}", data_path.display()));

    if let Err(e) = persist::write_port_file(&data_dir, port) {
        eprintln!("[persist] could not write port file next to {}: {e} (a bridge process may not find this daemon)", data_path.display());
    }

    let app = App {
        replica: Arc::new(Mutex::new(replica)),
        log: Arc::new(log),
        device: device.clone(),
        hostname,
        port,
        advertise: advertise.clone(),
        peer_prefix,
        pex: PexCache::default(),
        online: Arc::new(AtomicBool::new(true)),
    };

    println!(
        "syncd device={device} port={port} advertise={advertise} data={} lan_ip={} lan_discovery={} insecure_local_api={insecure_local_api}",
        data_path.display(),
        lan_ip.as_deref().unwrap_or("none"),
        lan_discovery_enabled,
    );
    if insecure_local_api {
        eprintln!("[security] --insecure-local-api: /write and /state accept non-loopback callers");
    }

    let mut local_api = Router::new()
        .route("/write", post(local_write))
        .route("/state", get(local_state))
        .route("/v1/online", post(set_online));
    if !insecure_local_api {
        local_api = local_api.layer(middleware::from_fn(require_loopback));
    }

    let sync_api = Router::new()
        .route("/v1/node", get(node))
        .route("/v1/peers", post(peers))
        .route("/v1/vv", post(vv))
        .route("/v1/ops/since", post(ops_since))
        .route("/v1/ops", post(ops_push));

    let router = local_api.merge(sync_api).with_state(app.clone());

    tokio::spawn(antientropy_loop(app.clone(), bootstrap, interval));
    if lan_discovery_enabled {
        match &lan_ip {
            Some(ip) => {
                let me = Peer { hostname: app.hostname.clone(), addr: format!("{ip}:{port}"), device_id: device.clone() };
                tokio::spawn(discovery::lan_discovery_loop(me, app.peer_prefix.clone(), app.pex.clone(), std::time::Duration::from_secs(interval)));
            }
            None => eprintln!("[lan-discovery] disabled: couldn't determine a local network address"),
        }
    }

    // 0.0.0.0 because the tailscale0 interface has its own dedicated 100.x
    // IP (and, same reasoning, any LAN interface's own address) — the
    // local-only routes stay protected by require_loopback regardless of
    // which interface the connection arrived on.
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await
        .unwrap_or_else(|e| panic!("cannot bind port {port}: {e}"));
    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}
