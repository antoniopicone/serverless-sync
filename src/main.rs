//! syncd — nodo di sync serverless su tailnet.
//!
//! Ogni nodo e' identico: nessuno e' autoritativo. La convergenza viene dal
//! CRDT (core.rs) piu' un loop di anti-entropy periodica.
//!
//! Uso:
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
    /// Indirizzo con cui questo nodo si annuncia nel peer exchange.
    /// Deve essere l'IP tailnet (100.x.y.z:porta), non 127.0.0.1, altrimenti
    /// il PEX propaga indirizzi che nessun altro puo' usare.
    advertise: String,
    tel: telemetry::Telemetry,
    peer_prefix: String,
    /// Interruttore locale, esposto via /v1/online. Non tocca /v1/write ne'
    /// /v1/state: il dispositivo resta pienamente usabile in locale mentre
    /// e' "offline", solo il lato peer-to-peer smette di rispondere e di
    /// contattare gli altri — e' cosi' che questo banco simula una
    /// partizione dalla UI invece che con `docker compose pause`.
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

/// Handshake + probe di discovery. Elenca TUTTI i servizi del device:
/// un solo probe per peer invece di uno per servizio.
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

/// Push: serve ai client che non possono restare in ascolto (iOS in
/// background non tiene un listener aperto), quindi spingono loro gli op.
async fn ops_push(State(a): State<App>, Json(req): Json<PushReq>) -> Result<Json<PushResp>, StatusCode> {
    guard_online(&a)?;
    let mut r = a.replica.lock().unwrap();
    let mut ops = req.ops;
    // ordina per (device, seq): apply rifiuta i buchi causali
    ops.sort_by_key(|o| (o.device.clone(), o.seq));
    let applied = ops.into_iter().filter(|o| r.apply(o.clone())).count();
    Ok(Json(PushResp { applied, vv: r.version_vector() }))
}

#[derive(Serialize, Deserialize, Default)]
struct PexReq {
    #[serde(default)]
    peers: Vec<Peer>,
}

/// Peer exchange, BIDIREZIONALE. Il chiamante manda la sua lista, il
/// chiamato la assorbe e restituisce l'unione.
///
/// Il verso singolo non basta: se A non contatta mai B, A non impara mai
/// che B esiste, e un terzo nodo che parte da A resta cieco su B. La
/// conoscenza della mesh deve propagarsi in entrambi i sensi a ogni
/// contatto, altrimenti la discovery dipende dall'ordine di avvio.
///
/// Chi risponde non ha alcuna autorita': e' solo una directory di indirizzi.
/// E' anche il modo in cui un client che non vede tailscaled (iOS) conosce
/// tutta la mesh chiedendo a un peer qualsiasi.
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

/// Scrittura locale (simula l'utente che modifica una entry).
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

/// Interruttore locale (mai gated da se stesso: deve restare raggiungibile
/// anche a dispositivo "offline", altrimenti non lo si potrebbe piu'
/// riaccendere dalla UI).
async fn set_online(State(a): State<App>, Json(req): Json<OnlineReq>) -> Json<serde_json::Value> {
    a.online.store(req.online, Ordering::Relaxed);
    a.tel.emit(if req.online { "node.online" } else { "node.offline" }, serde_json::json!({}));
    Json(serde_json::json!({ "online": req.online }))
}

// ------------------------------------------------------------ client di sync

/// Un round di sync con un peer. Simmetrico e senza stato condiviso:
/// pull di cio' che manca a me, push di cio' che manca a lui.
/// Lo stesso identico algoritmo gira sul client iOS.
async fn sync_round(app: &App, addr: &str) -> Result<(usize, usize), String> {
    // Timeout esplicito: un target stantio (es. un vecchio nodo ephemeral
    // con lo stesso hostname, ancora visibile ma morto) non deve bloccare
    // il giro di anti-entropy abbastanza a lungo da far slittare la sync
    // con i peer vivi.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| e.to_string())?;

    let my_vv = app.replica.lock().unwrap().version_vector();

    // 1. pull: cosa ha lui che io non ho
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

    // 2. push: cosa ho io che lui non ha
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

    // 3. peer exchange bidirezionale: mando quello che so, assorbo il resto
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

/// Anti-entropy: e' QUI che sta la correttezza, non nelle notifiche push.
/// Ogni tick ogni nodo si riconcilia con i peer che conosce; se un notify
/// si perde o un peer era offline al momento della scrittura, il tick
/// successivo recupera comunque.
async fn antientropy_loop(app: App, bootstrap: Vec<String>, interval_secs: u64) {
    loop {
        // "offline" dalla UI: non contatta nessuno finche' non torna online.
        // Le scritture locali continuano a funzionare (v.  /v1/write), solo
        // il lato peer-to-peer si ferma — e' la parte che dimostra il
        // local-first: il dispositivo lavora comunque, diverge, e riconverge
        // da solo al prossimo giro utile dopo il ripristino.
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
                            println!("[sync] {t}: +{pulled} ricevuti, +{pushed} inviati");
                            app.tel.emit("sync.ok", serde_json::json!({
                                "peer": t, "pulled": pulled, "pushed": pushed,
                            }));
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync] {t}: irraggiungibile ({e})");
                        app.tel.emit("peer.unreachable", serde_json::json!({ "peer": t }));
                    }
                }
            }
        }

        // istantanea di stato: e' il dato su cui il logger decide se il
        // sistema e' convergente. Due nodi allineati hanno lo stesso
        // fingerprint; se restano diversi, il merge e' divergente.
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
    // Accetta sia "--flag valore" che "--flag=valore": il docker-compose di
    // questo banco usa la seconda forma.
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

    // --advertise 100.x.y.z:47100  (IP tailnet). Default: primo IP tailnet
    // trovato, altrimenti loopback per i test locali.
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

    // 0.0.0.0 perche' l'interfaccia tailscale0 ha un IP 100.x dedicato
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!("syncd device={device} service={SERVICE} porta={port} annuncio={advertise}");
    axum::serve(listener, router).await.unwrap();
}
