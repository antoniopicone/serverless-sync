//! Discovery via tailnet + LAN broadcast + peer exchange.
//!
//! Knows nothing about what an `Op` contains (see core.rs): to this
//! module a peer is just an address. Three sources add up:
//!   1. `tailscale status --json`, which finds other nodes on the real
//!      tailnet by filtering on hostname prefix (--peer-prefix);
//!   2. a UDP broadcast announce/listen on the local subnet (see
//!      `lan_discovery_loop`), for nodes that share a LAN but aren't
//!      necessarily on the same tailnet;
//!   3. the bidirectional PEX exchanged on every sync round, which lets
//!      clients (e.g. iOS) that can't see tailscaled learn the mesh by
//!      asking any single peer.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;

pub const AGENT_PORT: u16 = 47100;

/// Port used for the LAN broadcast announce/listen (distinct from
/// AGENT_PORT so it doesn't collide with the HTTP sync API).
pub const LAN_DISCOVERY_PORT: u16 = 47188;

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

/// This machine's own local, non-loopback IPv4 address (its LAN
/// interface) — the address a same-subnet peer without tailscale can
/// actually reach us on. Uses the "connect a UDP socket, send nothing"
/// trick to ask the OS routing table which local address it would use to
/// reach the outside world, which avoids depending on any
/// platform-specific interface-enumeration API. Best-effort: `None` if
/// there's no route out (e.g. an isolated container with no network).
pub fn local_lan_ip() -> Option<String> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    match sock.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() => Some(ip.to_string()),
        _ => None,
    }
}

/// Announces this node's presence on the local subnet every `interval`,
/// and listens for the same broadcast from other nodes. Anything heard
/// (subject to the same `--peer-prefix` hostname filter tailnet discovery
/// uses) is merged straight into `pex` — the exact cache the tailnet peer
/// exchange also writes into — so `antientropy_loop`'s target list picks
/// new LAN peers up on its next tick with no discovery-specific plumbing
/// at the call site.
///
/// Best-effort and silent about it: a network that blocks broadcast (or a
/// sandboxed container with no broadcast-capable interface) just means
/// this source of peers contributes nothing, same as tailnet discovery
/// finding nothing when tailscaled isn't running.
fn bind_discovery_socket() -> std::io::Result<UdpSocket> {
    use socket2::{Domain, Socket, Type};

    // SO_REUSEADDR/SO_REUSEPORT so that multiple syncd instances on the
    // same host (e.g. running several devices locally without Docker, the
    // way this rig's own doc comment demonstrates) can each bind the same
    // discovery port instead of only the first one winning it — and, on
    // Linux/BSD, so a broadcast is delivered to every one of them rather
    // than load-balanced to just one.
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_broadcast(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&std::net::SocketAddr::from(([0, 0, 0, 0], LAN_DISCOVERY_PORT)).into())?;
    UdpSocket::from_std(socket.into())
}

pub async fn lan_discovery_loop(me: Peer, peer_prefix: String, pex: PexCache, interval: Duration) {
    let socket = match bind_discovery_socket() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[lan-discovery] disabled: can't bind :{LAN_DISCOVERY_PORT} ({e})");
            return;
        }
    };
    let socket = Arc::new(socket);

    let recv_socket = socket.clone();
    let recv_pex = pex;
    let my_id = me.device_id.clone();
    let recv_prefix = peer_prefix.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let Ok((n, _src)) = recv_socket.recv_from(&mut buf).await else { continue };
            let Ok(peer) = serde_json::from_slice::<Peer>(&buf[..n]) else { continue };
            if peer.device_id == my_id {
                continue;
            }
            if !recv_prefix.is_empty() && !peer.hostname.starts_with(&recv_prefix) {
                continue;
            }
            recv_pex.merge(vec![peer]);
        }
    });

    let payload = serde_json::to_vec(&me).unwrap_or_default();
    loop {
        let _ = socket.send_to(&payload, ("255.255.255.255", LAN_DISCOVERY_PORT)).await;
        tokio::time::sleep(interval).await;
    }
}
