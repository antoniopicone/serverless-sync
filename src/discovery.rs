//! Discovery via tailnet + peer exchange.
//!
//! Knows nothing about what an `Op` contains (see core.rs): to this
//! module a peer is just an address. Two sources add up:
//!   1. `tailscale status --json`, which finds other nodes on the real
//!      tailnet by filtering on hostname prefix (--peer-prefix);
//!   2. the bidirectional PEX exchanged on every sync round, which lets
//!      clients (e.g. iOS) that can't see tailscaled learn the mesh by
//!      asking any single peer.

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

/// Shared directory of known peers, learned via peer exchange. No
/// authority: it's just a cache, which is why interior mutability is
/// enough and the methods take `&self`.
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

/// This node's own tailnet IP (100.x.y.z), if tailscaled is up and connected.
pub fn my_tailnet_ip() -> Option<String> {
    let status = tailscale_status_json()?;
    ipv4_of(status.get("Self")?)
}

/// Other tailnet nodes whose hostname starts with `peer_prefix`.
/// Empty prefix = no filter. Returns (hostname, ip).
pub fn tailnet_candidates(peer_prefix: &str) -> Vec<(String, String)> {
    let Some(status) = tailscale_status_json() else { return Vec::new() };
    let Some(peers) = status.get("Peer").and_then(|p| p.as_object()) else { return Vec::new() };

    peers
        .values()
        .filter_map(|node| {
            // An ephemeral device restarted with the same hostname leaves
            // its old node lingering on the tailnet, offline, for a while.
            // If we don't filter it out here, its dead IP stays a sync
            // target: without a timeout on the sync client that alone
            // would be enough to block the anti-entropy round long enough
            // to delay syncing with live peers.
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
