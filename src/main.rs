//! syncd — one background sync daemon per machine, shared by every
//! application that registers with it.
//!
//! Every node is identical: none is authoritative. Convergence comes from
//! the CRDT (core.rs) plus a periodic anti-entropy loop. A single syncd
//! process hosts many independent applications ("services"), each
//! identified by a (name, token) pair the application picks for itself —
//! see "Registering an application" in README.md for the protocol.
//!
//! Usage:
//!   syncd --device linux-1 --port 47100
//!   syncd --device linux-2 --port 47101 --bootstrap 127.0.0.1:47100
//!   syncd --device linux-3 --port 47100 --no-lan-discovery
//!
//! The HTTP API always listens on 0.0.0.0, so it's reachable from both the
//! LAN and a tailnet interface. Peers are found via tailscale (if present),
//! a LAN broadcast announce/listen (unless --no-lan-discovery), and peer
//! exchange — see discovery.rs.

mod core;
mod discovery;
mod persist;
mod telemetry;

use axum::extract::Path as AxPath;
use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use core::{Op, OpKind, Replica, VersionVector};
use discovery::{NodeInfo, Peer, PexCache, ServiceInfo, AGENT_PORT};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const PROTO: u32 = 1;

/// Restricts application names and tokens to a safe, unambiguous charset:
/// both end up in a file name (`persist::service_path`) and in the URL
/// path, so anything else would risk path traversal or an unroutable
/// request. Deliberately plain and easy to satisfy — this is namespacing,
/// not authentication.
fn valid_key_part(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// `~/.syncd` — a hidden per-user directory, so installing syncd doesn't
/// require deciding where each application's data goes; every registered
/// application gets its own file underneath (see `persist::service_path`).
/// Falls back to `.syncd` in the working directory if somehow neither
/// $HOME nor %USERPROFILE% is set.
fn default_data_dir() -> PathBuf {
    let home = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")).unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".syncd")
}

/// One registered application's CRDT dataset plus its durable log. Kept
/// together because they're always accessed together: every op that goes
/// into `replica` must also go into `log`, or a restart loses it.
struct ServiceEntry {
    replica: Replica,
    log: persist::OpLog,
}

impl ServiceEntry {
    /// Loads an existing log for (name, token) if there is one, replaying
    /// it into a fresh `Replica`; otherwise starts empty. Either way the
    /// result is immediately ready to accept ops — this doubles as
    /// "provisioning" a brand new application: there's no separate step,
    /// the first ops that show up for a never-seen (name, token) simply
    /// create it.
    fn load(device: &str, data_root: &Path, name: &str, token: &str) -> Self {
        let path = persist::service_path(data_root, name, token);
        let mut replica = Replica::new(device.to_string());
        match persist::load(&path) {
            Ok(ops) => {
                let n = ops.len();
                for op in ops {
                    replica.apply(op);
                }
                if n > 0 {
                    println!("[persist] {name}.{token}: replayed {n} ops from {}", path.display());
                }
            }
            Err(e) => eprintln!("[persist] {name}.{token}: failed to read {}: {e} (starting empty)", path.display()),
        }
        let log = persist::OpLog::open(&path)
            .unwrap_or_else(|e| panic!("[persist] cannot open {}: {e}", path.display()));
        ServiceEntry { replica, log }
    }
}

/// Every application currently registered on this machine, keyed by
/// `"{name}.{token}"` — see `App::key`. One process, one port, one
/// registry: this is what lets a single syncd install serve any number of
/// applications instead of one fixed, compiled-in dataset.
type Registry = HashMap<String, ServiceEntry>;

#[derive(Clone)]
struct App {
    registry: Arc<Mutex<Registry>>,
    data_root: PathBuf,
    /// This machine's identity, shared by every application's `Replica`:
    /// the same physical device authoring ops for several independent
    /// datasets is fine (each has its own version vector), so there's no
    /// need for a separate identity per application.
    device: String,
    pex: PexCache,
    hostname: String,
    port: u16,
    /// Address this node announces itself with in the peer exchange.
    /// Must be the tailnet IP (100.x.y.z:port), not 127.0.0.1, otherwise
    /// PEX propagates addresses nobody else can use.
    advertise: String,
    tel: telemetry::Telemetry,
    peer_prefix: String,
    /// Local switch, exposed via /v1/online. Doesn't touch the per-app
    /// write/state endpoints: the device stays fully usable locally while "offline",
    /// only the peer-to-peer side stops responding to and contacting
    /// others — this is how this rig simulates a partition from the UI
    /// instead of with `docker compose pause`.
    online: Arc<AtomicBool>,
}

impl App {
    fn key(name: &str, token: &str) -> Option<String> {
        if valid_key_part(name) && valid_key_part(token) {
            Some(format!("{name}.{token}"))
        } else {
            None
        }
    }

    /// Looks up (name, token), lazily creating it — from a local
    /// /v1/register call, a client hitting any other per-application
    /// endpoint, or a peer syncing an application this machine hasn't
    /// seen before — if it doesn't exist yet.
    fn with_service<T>(&self, name: &str, token: &str, f: impl FnOnce(&mut ServiceEntry) -> T) -> Result<T, StatusCode> {
        let key = Self::key(name, token).ok_or(StatusCode::BAD_REQUEST)?;
        let mut reg = self.registry.lock().unwrap();
        let entry = reg.entry(key).or_insert_with(|| ServiceEntry::load(&self.device, &self.data_root, name, token));
        Ok(f(entry))
    }
}

fn guard_online(a: &App) -> Result<(), StatusCode> {
    if a.online.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

#[derive(Deserialize)]
struct ServicePath {
    name: String,
    token: String,
}

#[derive(Deserialize)]
struct SinceReq {
    vv: VersionVector,
}

#[derive(Serialize, Deserialize)]
struct OpsResp {
    ops: Vec<Op>,
}

#[derive(Deserialize)]
struct PushReq {
    ops: Vec<Op>,
}

#[derive(Serialize, Deserialize)]
struct PushResp {
    applied: usize,
    vv: VersionVector,
}

// ---------------------------------------------------------------- endpoints

/// Handshake + discovery probe. Lists every application registered on
/// this machine so far: one probe per peer instead of one per
/// application.
async fn node(State(a): State<App>) -> Result<Json<NodeInfo>, StatusCode> {
    guard_online(&a)?;
    let reg = a.registry.lock().unwrap();
    let services = reg.iter().map(|(key, entry)| {
        let (name, token) = key.split_once('.').unwrap_or((key.as_str(), ""));
        ServiceInfo { name: name.to_string(), token: token.to_string(), entries: entry.replica.entries().len() }
    }).collect();
    Ok(Json(NodeInfo {
        proto: PROTO,
        device_id: a.device.clone(),
        hostname: a.hostname.clone(),
        port: a.port,
        services,
    }))
}

#[derive(Deserialize)]
struct RegisterReq {
    name: String,
    token: String,
}

#[derive(Serialize)]
struct RegisterResp {
    name: String,
    token: String,
    device_id: String,
    entries: usize,
    vv: VersionVector,
}

/// Registers a new application, or confirms an existing one — see
/// "Registering an application" in README.md. Every per-application
/// endpoint below also creates its dataset lazily on first use, so
/// calling this isn't strictly required, but it's the documented way for
/// a client to confirm it's talking to the right (name, token) and see
/// what's already there before writing anything. Not gated by
/// guard_online: like the per-app write endpoint, this is a local call an application
/// makes to its own syncd, not a peer-to-peer one.
async fn register(State(a): State<App>, Json(req): Json<RegisterReq>) -> Result<Json<RegisterResp>, StatusCode> {
    a.with_service(&req.name, &req.token, |entry| RegisterResp {
        name: req.name.clone(),
        token: req.token.clone(),
        device_id: a.device.clone(),
        entries: entry.replica.entries().len(),
        vv: entry.replica.version_vector(),
    }).map(Json)
}

async fn vv(State(a): State<App>, AxPath(p): AxPath<ServicePath>) -> Result<Json<VersionVector>, StatusCode> {
    guard_online(&a)?;
    a.with_service(&p.name, &p.token, |entry| entry.replica.version_vector()).map(Json)
}

async fn ops_since(
    State(a): State<App>,
    AxPath(p): AxPath<ServicePath>,
    Json(req): Json<SinceReq>,
) -> Result<Json<OpsResp>, StatusCode> {
    guard_online(&a)?;
    a.with_service(&p.name, &p.token, |entry| OpsResp { ops: entry.replica.ops_since(&req.vv) }).map(Json)
}

/// Push: for clients that can't stay listening (iOS in the background
/// doesn't keep a listener open), so they push their ops instead.
async fn ops_push(
    State(a): State<App>,
    AxPath(p): AxPath<ServicePath>,
    Json(req): Json<PushReq>,
) -> Result<Json<PushResp>, StatusCode> {
    guard_online(&a)?;
    a.with_service(&p.name, &p.token, |entry| {
        let mut ops = req.ops;
        // sort by (device, seq): apply rejects causal gaps
        ops.sort_by_key(|o| (o.device.clone(), o.seq));
        let applied = ops.into_iter().filter(|o| {
            let ok = entry.replica.apply(o.clone());
            if ok {
                persist_or_log(&entry.log, o, &p.name, &p.token);
            }
            ok
        }).count();
        PushResp { applied, vv: entry.replica.version_vector() }
    }).map(Json)
}

/// Best-effort durability, same posture as telemetry: an op that's already
/// merged into a replica must not be lost if the write to disk fails, so
/// this logs and moves on rather than turning a sync round into a 500.
fn persist_or_log(log: &persist::OpLog, op: &Op, name: &str, token: &str) {
    if let Err(e) = log.append(op) {
        eprintln!("[persist] {name}.{token}: failed to append op ({} seq {}): {e}", op.device, op.seq);
    }
}

#[derive(Serialize, Deserialize, Default)]
struct PexReq {
    #[serde(default)]
    peers: Vec<Peer>,
}

/// Peer exchange, BIDIRECTIONAL. The caller sends its list, the callee
/// absorbs it and returns the union. Host-level, not per-application: the
/// same set of peers applies regardless of which applications are
/// registered on either side.
///
/// A single direction isn't enough: if A never contacts B, A never learns
/// B exists, and a third node bootstrapping from A stays blind to B.
/// Knowledge of the mesh must propagate both ways on every contact,
/// otherwise discovery depends on startup order.
///
/// Whoever answers has no authority: it's just an address directory. It's
/// also how a client that can't see tailscaled (iOS) learns the whole
/// mesh by asking any single peer.
async fn peers(State(a): State<App>, Json(req): Json<PexReq>) -> Result<Json<Vec<Peer>>, StatusCode> {
    guard_online(&a)?;
    a.pex.merge(req.peers);
    let me = a.device.clone();
    let mut out: Vec<Peer> = a.pex.list().into_iter().filter(|p| p.device_id != me).collect();
    out.push(Peer { hostname: a.hostname.clone(), addr: a.advertise.clone(), device_id: me });
    Ok(Json(out))
}

#[derive(Deserialize)]
struct WriteReq {
    entity: String,
    value: Option<String>,
}

/// Local write (an application's user editing an entry). Not gated by
/// guard_online: the device stays fully usable locally while "offline",
/// only the peer-to-peer surface stops.
async fn write(
    State(a): State<App>,
    AxPath(p): AxPath<ServicePath>,
    Json(req): Json<WriteReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    a.with_service(&p.name, &p.token, |entry| {
        let op = match req.value {
            Some(v) => entry.replica.local_change(&req.entity, OpKind::Upsert, &v),
            None => entry.replica.local_change(&req.entity, OpKind::Delete, ""),
        };
        persist_or_log(&entry.log, &op, &p.name, &p.token);
        a.tel.emit("op.local", serde_json::json!({
            "service": format!("{}.{}", p.name, p.token), "entity": op.entity, "seq": op.seq, "kind": format!("{:?}", op.kind),
        }));
        serde_json::json!({ "seq": op.seq, "vv": entry.replica.version_vector() })
    }).map(Json)
}

async fn state(State(a): State<App>, AxPath(p): AxPath<ServicePath>) -> Result<Json<serde_json::Value>, StatusCode> {
    let online = a.online.load(Ordering::Relaxed);
    a.with_service(&p.name, &p.token, |entry| serde_json::json!({
        "device": a.device,
        "entries": entry.replica.entries(),
        "vv": entry.replica.version_vector(),
        "fingerprint": format!("{:x}", entry.replica.state_fingerprint()),
        "online": online,
    })).map(Json)
}

#[derive(Deserialize)]
struct OnlineReq {
    online: bool,
}

/// Local switch (never gated by itself: it must stay reachable even while
/// the device is "offline", otherwise the UI could never turn it back
/// on). Host-level: flips every registered application's sync at once,
/// same as a laptop's network connection doesn't go offline per-app.
async fn set_online(State(a): State<App>, Json(req): Json<OnlineReq>) -> Json<serde_json::Value> {
    a.online.store(req.online, Ordering::Relaxed);
    a.tel.emit(if req.online { "node.online" } else { "node.offline" }, serde_json::json!({}));
    Json(serde_json::json!({ "online": req.online }))
}

// ------------------------------------------------------------ sync client

/// One sync round with a peer for a single (name, token). Symmetric and
/// with no shared state: pull whatever I'm missing, push whatever they're
/// missing. The exact same algorithm runs on the iOS client.
async fn sync_round(app: &App, addr: &str, name: &str, token: &str) -> Result<(usize, usize), String> {
    // Explicit timeout: a stale target (e.g. an old ephemeral node with the
    // same hostname, still visible but dead) must not block the
    // anti-entropy round long enough to delay syncing with live peers.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| e.to_string())?;

    let bad_key = || "invalid service key".to_string();
    let my_vv = app.with_service(name, token, |entry| entry.replica.version_vector()).map_err(|_| bad_key())?;

    // 1. pull: what they have that I don't
    let pulled: OpsResp = http
        .post(format!("http://{addr}/v1/{name}/{token}/ops/since"))
        .json(&serde_json::json!({ "vv": my_vv }))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let n_pull = app.with_service(name, token, |entry| {
        let mut ops = pulled.ops;
        ops.sort_by_key(|o| (o.device.clone(), o.seq));
        ops.into_iter().filter(|o| {
            let ok = entry.replica.apply(o.clone());
            if ok {
                persist_or_log(&entry.log, o, name, token);
            }
            ok
        }).count()
    }).map_err(|_| bad_key())?;

    // 2. push: what I have that they don't
    let their_vv: VersionVector = http
        .get(format!("http://{addr}/v1/{name}/{token}/vv"))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let mine = app.with_service(name, token, |entry| entry.replica.ops_since(&their_vv)).map_err(|_| bad_key())?;
    let n_push = if mine.is_empty() {
        0
    } else {
        let resp: PushResp = http
            .post(format!("http://{addr}/v1/{name}/{token}/ops"))
            .json(&serde_json::json!({ "ops": mine }))
            .send().await.map_err(|e| e.to_string())?
            .json().await.map_err(|e| e.to_string())?;
        resp.applied
    };

    Ok((n_pull, n_push))
}

/// Bidirectional peer exchange with one peer: send what I know, absorb the
/// rest. Host-level — done once per peer per tick, not once per
/// application, since the mesh doesn't depend on which applications are
/// registered.
async fn exchange_peers(app: &App, addr: &str) {
    let Ok(http) = reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)).build() else { return };
    let mut mine_peers = app.pex.list();
    mine_peers.push(Peer { hostname: app.hostname.clone(), addr: app.advertise.clone(), device_id: app.device.clone() });
    if let Ok(resp) = http.post(format!("http://{addr}/v1/peers")).json(&serde_json::json!({ "peers": mine_peers })).send().await {
        if let Ok(list) = resp.json::<Vec<Peer>>().await {
            app.pex.merge(list);
        }
    }
}

/// Anti-entropy: this is WHERE correctness lives, not in push
/// notifications. Every tick, every node exchanges peers with — then
/// reconciles every locally registered application against — the peers it
/// knows about; if a notify is lost or a peer was offline at write time,
/// the next tick catches up regardless.
async fn antientropy_loop(app: App, bootstrap: Vec<String>, interval_secs: u64) {
    loop {
        // "offline" from the UI: don't contact anyone until back online.
        // Local writes keep working (see the per-app write endpoint) — only the
        // peer-to-peer side stops. This is the part that demonstrates
        // local-first: the device keeps working regardless, diverges, and
        // reconverges on its own at the next useful tick after recovery.
        if app.online.load(Ordering::Relaxed) {
            let mut targets: Vec<String> = bootstrap.clone();
            for (host, ip) in discovery::tailnet_candidates(&app.peer_prefix) {
                let _ = host;
                // Assumes the peer runs on the same port as this node —
                // true by construction now that one syncd install serves
                // every application on a single port. `tailscale status`
                // has no way to report the peer's actual port, so this is
                // the best available guess; --bootstrap is the escape
                // hatch for a peer that (unusually) runs on a different one.
                targets.push(format!("{ip}:{}", app.port));
            }
            for p in app.pex.list() {
                targets.push(p.addr);
            }
            targets.sort();
            targets.dedup();

            let services: Vec<(String, String)> = {
                let reg = app.registry.lock().unwrap();
                reg.keys().filter_map(|k| k.split_once('.').map(|(n, t)| (n.to_string(), t.to_string()))).collect()
            };

            for t in targets.into_iter().filter(|t| *t != app.advertise) {
                exchange_peers(&app, &t).await;

                for (name, token) in &services {
                    match sync_round(&app, &t, name, token).await {
                        Ok((pulled, pushed)) => {
                            if pulled + pushed > 0 {
                                println!("[sync] {name}.{token} <-> {t}: +{pulled} received, +{pushed} sent");
                                app.tel.emit("sync.ok", serde_json::json!({
                                    "service": format!("{name}.{token}"), "peer": t, "pulled": pulled, "pushed": pushed,
                                }));
                            }
                        }
                        Err(e) => {
                            eprintln!("[sync] {name}.{token} <-> {t}: unreachable ({e})");
                            app.tel.emit("peer.unreachable", serde_json::json!({
                                "peer": t, "service": format!("{name}.{token}"),
                            }));
                        }
                    }
                }
            }
        }

        // state snapshot per application: this is what the logger uses to
        // decide whether the system has converged. Two aligned replicas of
        // the same application share the same fingerprint; if they stay
        // different, the merge has diverged.
        {
            let reg = app.registry.lock().unwrap();
            for (key, entry) in reg.iter() {
                app.tel.emit("state", serde_json::json!({
                    "service": key,
                    "fingerprint": format!("{:x}", entry.replica.state_fingerprint()),
                    "entries": entry.replica.entries().len(),
                    "vv": entry.replica.version_vector(),
                }));
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Accepts both "--flag value" and "--flag=value": this rig's
    // docker-compose uses the latter form.
    let arg = |k: &str| -> Option<String> {
        let eq_prefix = format!("{k}=");
        args.iter().find_map(|a| a.strip_prefix(&eq_prefix).map(String::from)).or_else(|| {
            args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned()
        })
    };

    let device = arg("--device").unwrap_or_else(|| "linux-1".into());
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(AGENT_PORT);
    let bootstrap: Vec<String> = arg("--bootstrap")
        .map(|b| b.split(',').map(String::from).collect())
        .unwrap_or_default();
    let hostname = std::process::Command::new("hostname")
        .output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| device.clone());

    // --advertise 100.x.y.z:47100  (tailnet IP). Default: first tailnet IP
    // found, else this machine's own LAN IP, else loopback for local
    // testing (nothing routable was found at all).
    let lan_ip = discovery::local_lan_ip();
    let advertise = arg("--advertise")
        .or_else(|| std::env::var("SYNCD_ADVERTISE").ok())
        .or_else(|| discovery::my_tailnet_ip().map(|ip| format!("{ip}:{port}")))
        .or_else(|| lan_ip.clone().map(|ip| format!("{ip}:{port}")))
        .unwrap_or_else(|| format!("127.0.0.1:{port}"));

    let tel = telemetry::Telemetry::new(arg("--telemetry"), device.clone());
    let peer_prefix = arg("--peer-prefix").unwrap_or_default();
    let lan_discovery_enabled = !args.iter().any(|a| a == "--no-lan-discovery");

    // One data root for every application this machine will ever
    // register, instead of one file per device: see persist::service_path.
    let data_root = arg("--data").map(PathBuf::from).unwrap_or_else(default_data_dir);
    std::fs::create_dir_all(&data_root)
        .unwrap_or_else(|e| panic!("[persist] cannot create data directory {}: {e}", data_root.display()));

    // Resume every application that was already registered before this
    // restart (it has a log on disk already), so it keeps syncing in the
    // background even before any local client touches it again.
    let mut registry: Registry = HashMap::new();
    for (name, token) in persist::known_services(&data_root) {
        let entry = ServiceEntry::load(&device, &data_root, &name, &token);
        println!("[persist] resumed {name}.{token} ({} live entries)", entry.replica.entries().len());
        registry.insert(format!("{name}.{token}"), entry);
    }
    let n_resumed = registry.len();

    let app = App {
        registry: Arc::new(Mutex::new(registry)),
        data_root: data_root.clone(),
        device: device.clone(),
        pex: PexCache::default(),
        hostname,
        port,
        advertise: advertise.clone(),
        tel: tel.clone(),
        peer_prefix,
        online: Arc::new(AtomicBool::new(true)),
    };

    tel.emit("node.start", serde_json::json!({
        "advertise": advertise, "port": port, "services_resumed": n_resumed,
    }));

    let router = Router::new()
        .route("/v1/node", get(node))
        .route("/v1/register", post(register))
        .route("/v1/peers", post(peers))
        .route("/v1/online", post(set_online))
        .route("/v1/:name/:token/vv", get(vv))
        .route("/v1/:name/:token/ops/since", post(ops_since))
        .route("/v1/:name/:token/ops", post(ops_push))
        .route("/v1/:name/:token/write", post(write))
        .route("/v1/:name/:token/state", get(state))
        .with_state(app.clone());

    let interval: u64 = arg("--interval").and_then(|i| i.parse().ok()).unwrap_or(5);
    tokio::spawn(antientropy_loop(app.clone(), bootstrap, interval));

    if lan_discovery_enabled {
        match &lan_ip {
            Some(ip) => {
                let me = Peer {
                    hostname: app.hostname.clone(),
                    addr: format!("{ip}:{port}"),
                    device_id: device.clone(),
                };
                tokio::spawn(discovery::lan_discovery_loop(
                    me,
                    app.peer_prefix.clone(),
                    app.pex.clone(),
                    std::time::Duration::from_secs(interval),
                ));
            }
            None => eprintln!("[lan-discovery] disabled: couldn't determine a local network address"),
        }
    }

    // 0.0.0.0 because the tailscale0 interface has its own dedicated 100.x IP
    // (and, same reasoning, any LAN interface's own address)
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!(
        "syncd device={device} port={port} advertise={advertise} data={} services_resumed={n_resumed} lan_ip={} lan_discovery={}",
        data_root.display(),
        lan_ip.as_deref().unwrap_or("none"),
        lan_discovery_enabled,
    );
    axum::serve(listener, router).await.unwrap();
}
