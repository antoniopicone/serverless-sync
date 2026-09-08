//! syncd — serverless sync node over a tailnet.
//!
//! Every node is identical: none is authoritative. Convergence comes from
//! the CRDT (core.rs) plus a periodic anti-entropy loop.
//!
//! Usage:
//!   syncd --device linux-1 --port 47100
//!   syncd --device linux-2 --port 47101 --bootstrap 127.0.0.1:47100

mod core;
mod discovery;
mod telemetry;

use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use core::{Op, OpKind, Replica, VersionVector};
use discovery::{Peer, PexCache, AGENT_PORT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const PROTO: u32 = 1;
const SERVICE: &str = "pass";

#[derive(Clone)]
struct App {
    replica: Arc<Mutex<Replica>>,
    pex: PexCache,
    hostname: String,
    port: u16,
    /// Address this node announces itself with in the peer exchange.
    /// Must be the tailnet IP (100.x.y.z:port), not 127.0.0.1, otherwise
    /// PEX propagates addresses nobody else can use.
    advertise: String,
    tel: telemetry::Telemetry,
    peer_prefix: String,
    /// Local switch, exposed via /v1/online. Doesn't touch /v1/write or
    /// /v1/state: the device stays fully usable locally while "offline",
    /// only the peer-to-peer side stops responding to and contacting
    /// others — this is how this rig simulates a partition from the UI
    /// instead of with `docker compose pause`.
    online: Arc<AtomicBool>,
}

fn guard_online(a: &App) -> Result<(), StatusCode> {
    if a.online.load(Ordering::Relaxed) {
        Ok(())
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
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

/// Handshake + discovery probe. Lists ALL of the device's services:
/// one probe per peer instead of one per service.
async fn node(State(a): State<App>) -> Result<Json<discovery::NodeInfo>, StatusCode> {
    guard_online(&a)?;
    let r = a.replica.lock().unwrap();
    let mut services = BTreeMap::new();
    services.insert(r.service.clone(), a.port);
    Ok(Json(discovery::NodeInfo {
        proto: PROTO,
        device_id: r.device.clone(),
        hostname: a.hostname.clone(),
        services,
    }))
}

async fn vv(State(a): State<App>) -> Result<Json<VersionVector>, StatusCode> {
    guard_online(&a)?;
    Ok(Json(a.replica.lock().unwrap().version_vector()))
}

async fn ops_since(State(a): State<App>, Json(req): Json<SinceReq>) -> Result<Json<OpsResp>, StatusCode> {
    guard_online(&a)?;
    Ok(Json(OpsResp {
        ops: a.replica.lock().unwrap().ops_since(&req.vv),
    }))
}

/// Push: for clients that can't stay listening (iOS in the background
/// doesn't keep a listener open), so they push their ops instead.
async fn ops_push(State(a): State<App>, Json(req): Json<PushReq>) -> Result<Json<PushResp>, StatusCode> {
    guard_online(&a)?;
    let mut r = a.replica.lock().unwrap();
    let mut ops = req.ops;
    // sort by (device, seq): apply rejects causal gaps
    ops.sort_by_key(|o| (o.device.clone(), o.seq));
    let applied = ops.into_iter().filter(|o| r.apply(o.clone())).count();
    Ok(Json(PushResp { applied, vv: r.version_vector() }))
}

#[derive(Serialize, Deserialize, Default)]
struct PexReq {
    #[serde(default)]
    peers: Vec<Peer>,
}

/// Peer exchange, BIDIRECTIONAL. The caller sends its list, the callee
/// absorbs it and returns the union.
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
    let me = a.replica.lock().unwrap().device.clone();
    let mut out: Vec<Peer> = a.pex.list().into_iter().filter(|p| p.device_id != me).collect();
    out.push(Peer {
        hostname: a.hostname.clone(),
        addr: a.advertise.clone(),
        device_id: me,
    });
    Ok(Json(out))
}

#[derive(Deserialize)]
struct WriteReq {
    entity: String,
    value: Option<String>,
}

/// Local write (simulates the user editing an entry).
async fn write(State(a): State<App>, Json(req): Json<WriteReq>) -> Json<serde_json::Value> {
    let mut r = a.replica.lock().unwrap();
    let op = match req.value {
        Some(v) => r.local_change(&req.entity, OpKind::Upsert, &v),
        None => r.local_change(&req.entity, OpKind::Delete, ""),
    };
    a.tel.emit("op.local", serde_json::json!({
        "entity": op.entity, "seq": op.seq, "kind": format!("{:?}", op.kind),
    }));
    Json(serde_json::json!({ "seq": op.seq, "vv": r.version_vector() }))
}

async fn state(State(a): State<App>) -> Json<serde_json::Value> {
    let r = a.replica.lock().unwrap();
    Json(serde_json::json!({
        "device": r.device,
        "entries": r.entries(),
        "vv": r.version_vector(),
        "fingerprint": format!("{:x}", r.state_fingerprint()),
        "online": a.online.load(Ordering::Relaxed),
    }))
}

#[derive(Deserialize)]
struct OnlineReq {
    online: bool,
}

/// Local switch (never gated by itself: it must stay reachable even while
/// the device is "offline", otherwise the UI could never turn it back on).
async fn set_online(State(a): State<App>, Json(req): Json<OnlineReq>) -> Json<serde_json::Value> {
    a.online.store(req.online, Ordering::Relaxed);
    a.tel.emit(if req.online { "node.online" } else { "node.offline" }, serde_json::json!({}));
    Json(serde_json::json!({ "online": req.online }))
}

// ------------------------------------------------------------ sync client

/// One sync round with a peer. Symmetric and with no shared state:
/// pull whatever I'm missing, push whatever they're missing.
/// The exact same algorithm runs on the iOS client.
async fn sync_round(app: &App, addr: &str) -> Result<(usize, usize), String> {
    // Explicit timeout: a stale target (e.g. an old ephemeral node with the
    // same hostname, still visible but dead) must not block the
    // anti-entropy round long enough to delay syncing with live peers.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| e.to_string())?;

    let my_vv = app.replica.lock().unwrap().version_vector();

    // 1. pull: what they have that I don't
    let pulled: OpsResp = http
        .post(format!("http://{addr}/v1/ops/since"))
        .json(&serde_json::json!({ "vv": my_vv }))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let n_pull = {
        let mut r = app.replica.lock().unwrap();
        let mut ops = pulled.ops;
        ops.sort_by_key(|o| (o.device.clone(), o.seq));
        ops.into_iter().filter(|o| r.apply(o.clone())).count()
    };

    // 2. push: what I have that they don't
    let their_vv: VersionVector = http
        .get(format!("http://{addr}/v1/vv"))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let mine = app.replica.lock().unwrap().ops_since(&their_vv);
    let n_push = if mine.is_empty() {
        0
    } else {
        let resp: PushResp = http
            .post(format!("http://{addr}/v1/ops"))
            .json(&serde_json::json!({ "ops": mine }))
            .send().await.map_err(|e| e.to_string())?
            .json().await.map_err(|e| e.to_string())?;
        resp.applied
    };

    // 3. bidirectional peer exchange: send what I know, absorb the rest
    let mut mine_peers = app.pex.list();
    mine_peers.push(Peer {
        hostname: app.hostname.clone(),
        addr: app.advertise.clone(),
        device_id: app.replica.lock().unwrap().device.clone(),
    });
    if let Ok(resp) = http
        .post(format!("http://{addr}/v1/peers"))
        .json(&serde_json::json!({ "peers": mine_peers }))
        .send().await
    {
        if let Ok(list) = resp.json::<Vec<Peer>>().await {
            app.pex.merge(list);
        }
    }

    Ok((n_pull, n_push))
}

/// Anti-entropy: this is WHERE correctness lives, not in push
/// notifications. Every tick, every node reconciles with the peers it
/// knows about; if a notify is lost or a peer was offline at write time,
/// the next tick catches up regardless.
async fn antientropy_loop(app: App, bootstrap: Vec<String>, interval_secs: u64) {
    loop {
        // "offline" from the UI: don't contact anyone until back online.
        // Local writes keep working (see /v1/write) — only the
        // peer-to-peer side stops. This is the part that demonstrates
        // local-first: the device keeps working regardless, diverges, and
        // reconverges on its own at the next useful tick after recovery.
        if app.online.load(Ordering::Relaxed) {
            let mut targets: Vec<String> = bootstrap.clone();
            for (host, ip) in discovery::tailnet_candidates(&app.peer_prefix) {
                let _ = host;
                targets.push(format!("{ip}:{AGENT_PORT}"));
            }
            for p in app.pex.list() {
                targets.push(p.addr);
            }
            targets.sort();
            targets.dedup();

            for t in targets.into_iter().filter(|t| *t != app.advertise) {
                match sync_round(&app, &t).await {
                    Ok((pulled, pushed)) => {
                        if pulled + pushed > 0 {
                            println!("[sync] {t}: +{pulled} received, +{pushed} sent");
                            app.tel.emit("sync.ok", serde_json::json!({
                                "peer": t, "pulled": pulled, "pushed": pushed,
                            }));
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync] {t}: unreachable ({e})");
                        app.tel.emit("peer.unreachable", serde_json::json!({ "peer": t }));
                    }
                }
            }
        }

        // state snapshot: this is what the logger uses to decide whether
        // the system has converged. Two aligned nodes share the same
        // fingerprint; if they stay different, the merge has diverged.
        {
            let r = app.replica.lock().unwrap();
            app.tel.emit("state", serde_json::json!({
                "fingerprint": format!("{:x}", r.state_fingerprint()),
                "entries": r.entries().len(),
                "vv": r.version_vector(),
            }));
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
    // found, otherwise loopback for local testing.
    let advertise = arg("--advertise")
        .or_else(|| std::env::var("SYNCD_ADVERTISE").ok())
        .or_else(|| discovery::my_tailnet_ip().map(|ip| format!("{ip}:{port}")))
        .unwrap_or_else(|| format!("127.0.0.1:{port}"));

    let tel = telemetry::Telemetry::new(arg("--telemetry"), device.clone());
    let peer_prefix = arg("--peer-prefix").unwrap_or_default();

    let app = App {
        replica: Arc::new(Mutex::new(Replica::new(device.clone(), SERVICE.into()))),
        pex: PexCache::default(),
        hostname,
        port,
        advertise: advertise.clone(),
        tel: tel.clone(),
        peer_prefix,
        online: Arc::new(AtomicBool::new(true)),
    };

    tel.emit("node.start", serde_json::json!({
        "advertise": advertise, "port": port, "service": SERVICE,
    }));

    let router = Router::new()
        .route("/v1/node", get(node))
        .route("/v1/vv", get(vv))
        .route("/v1/ops/since", post(ops_since))
        .route("/v1/ops", post(ops_push))
        .route("/v1/peers", post(peers))
        .route("/v1/write", post(write))
        .route("/v1/state", get(state))
        .route("/v1/online", post(set_online))
        .with_state(app.clone());

    let interval: u64 = arg("--interval").and_then(|i| i.parse().ok()).unwrap_or(5);
    tokio::spawn(antientropy_loop(app.clone(), bootstrap, interval));

    // 0.0.0.0 because the tailscale0 interface has its own dedicated 100.x IP
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!("syncd device={device} service={SERVICE} port={port} advertise={advertise}");
    axum::serve(listener, router).await.unwrap();
}
