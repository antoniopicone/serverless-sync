//! Discovery via tailnet + peer exchange.
//!
//! Non sa nulla del contenuto degli `Op` (vedi core.rs): per questo modulo
//! un peer e' solo un indirizzo. Due fonti si sommano:
//!   1. `tailscale status --json`, che trova gli altri nodi della tailnet
//!      reale filtrando per prefisso di hostname (--peer-prefix);
//!   2. il PEX bidirezionale scambiato a ogni round di sync, che serve ai
//!      client (es. iOS) che non vedono tailscaled e imparano la mesh
//!      chiedendo a un peer qualsiasi.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

pub const AGENT_PORT: u16 = 47100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub hostname: String,
    pub addr: String,
    pub device_id: String,
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub proto: u32,
    pub device_id: String,
    pub hostname: String,
    pub services: BTreeMap<String, u16>,
}

/// Directory condivisa di peer conosciuti, appresa via peer exchange.
/// Nessuna autorita': e' solo cache, per questo l'interior mutability
/// basta e i metodi prendono `&self`.
#[derive(Clone, Default)]
pub struct PexCache {
    inner: Arc<Mutex<BTreeMap<String, Peer>>>,
}

impl PexCache {
    pub fn merge(&self, peers: Vec<Peer>) {
        let mut g = self.inner.lock().unwrap();
        for p in peers {
            g.insert(p.device_id.clone(), p);
        }
    }

    pub fn list(&self) -> Vec<Peer> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}

fn tailscale_status_json() -> Option<serde_json::Value> {
    let out = Command::new("tailscale").args(["status", "--json"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

fn ipv4_of(node: &serde_json::Value) -> Option<String> {
    node.get("TailscaleIPs")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str())
        .find(|ip| ip.contains('.'))
        .map(str::to_string)
}

/// Il proprio IP tailnet (100.x.y.z), se tailscaled e' su e connesso.
pub fn my_tailnet_ip() -> Option<String> {
    let status = tailscale_status_json()?;
    ipv4_of(status.get("Self")?)
}

/// Altri nodi della tailnet il cui hostname inizia per `peer_prefix`.
/// Prefisso vuoto = nessun filtro. Ritorna (hostname, ip).
pub fn tailnet_candidates(peer_prefix: &str) -> Vec<(String, String)> {
    let Some(status) = tailscale_status_json() else { return Vec::new() };
    let Some(peers) = status.get("Peer").and_then(|p| p.as_object()) else { return Vec::new() };

    peers
        .values()
        .filter_map(|node| {
            // Un device ephemeral riavviato con lo stesso hostname lascia
            // per un po' il vecchio nodo nella tailnet, offline. Se non lo
            // scartiamo qui, il suo IP morto resta un target: senza timeout
            // sul client di sync basterebbe a bloccare il giro di
            // anti-entropy abbastanza da far slittare la sync coi peer vivi.
            if !node.get("Online").and_then(|v| v.as_bool()).unwrap_or(false) {
                return None;
            }
            let hostname = node.get("HostName")?.as_str()?.to_string();
            if !peer_prefix.is_empty() && !hostname.starts_with(peer_prefix) {
                return None;
            }
            let ip = ipv4_of(node)?;
            Some((hostname, ip))
        })
        .collect()
}
