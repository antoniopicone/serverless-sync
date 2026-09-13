//! logger — observer for the Docker demo (see docker-compose.yml).
//!
//! NOT a peer and NOT in the sync path: it never appears in any node's
//! `--bootstrap`/peer list, and reaches the nodes only by polling their
//! (normally loopback-only) `/state` and `/write` — which only works here
//! because the demo nodes run with `--insecure-local-api` on an isolated
//! Docker bridge network, not on a real one (see main.rs's own comment on
//! that flag).
//!
//! The fingerprint each node reports is the signal: the same across all
//! nodes means replicas are aligned, different means the merge has
//! diverged. GET /api/assert returns 200 if converged and 409 if not, so
//! scenario.sh can fail with an exit code instead of by eyeballing it.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const MAX_EVENTS: usize = 400;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Clone, serde::Serialize)]
struct Event {
    seq: u64,
    ts: u64,
    device: String,
    kind: String,
    data: serde_json::Value,
}

#[derive(Clone, serde::Serialize, Default)]
struct DeviceState {
    device: String,
    fingerprint: String,
    entries: Vec<serde_json::Value>,
    vv: serde_json::Value,
    reachable: bool,
    /// The node's own `/v1/online` switch (see main.rs), not reachability:
    /// a device can be "offline" and stay perfectly reachable and editable
    /// locally — that's the whole point of the demo. Only its sync toward
    /// the others stops.
    online: bool,
}

#[derive(Default)]
struct Inner {
    devices: BTreeMap<String, DeviceState>,
    /// polling target -> device name, learned on the first successful
    /// poll. Needed to correctly attribute a failure: without it, a node
    /// that stops responding stays marked reachable and the partition
    /// doesn't show up.
    by_target: BTreeMap<String, String>,
    /// The reverse: device name -> network target. The UI only talks to
    /// the logger (no CORS to configure on the nodes' ports); writes get
    /// forwarded to the right target through this map.
    target_of: BTreeMap<String, String>,
    /// Synthesized by diffing each poll against the previous one (see
    /// `poll_one`) — the logger never sees ops directly (it isn't in the
    /// sync path), so this is reconstructed from before/after state
    /// rather than pushed by the nodes.
    events: VecDeque<Event>,
    event_seq: u64,
}

impl Inner {
    fn push_event(&mut self, device: &str, kind: &str, data: serde_json::Value) {
        self.event_seq += 1;
        self.events.push_front(Event { seq: self.event_seq, ts: now(), device: device.to_string(), kind: kind.to_string(), data });
        while self.events.len() > MAX_EVENTS {
            self.events.pop_back();
        }
    }
}

/// entity -> value, from a `/state` response's `entries` array — used to
/// diff two snapshots down to per-entity write/delete events.
fn entries_map(entries: &[serde_json::Value]) -> BTreeMap<String, String> {
    entries.iter().filter_map(|e| {
        let entity = e.get("entity")?.as_str()?.to_string();
        let value = e.get("value")?.as_str()?.to_string();
        Some((entity, value))
    }).collect()
}

#[derive(Clone)]
struct Log {
    inner: Arc<Mutex<Inner>>,
    targets: Vec<String>,
}

/// Green only if ALL known nodes respond and share the same fingerprint.
///
/// The nuance matters. If a node is suspended and you only look at the
/// ones still alive, the remaining two agree with each other and the
/// signal would go green in the middle of a partition: a convergence that
/// isn't real. A node that doesn't answer is an unknown state, not an
/// agreeing one. That's why `live_aligned` is exposed separately: it
/// reports whether the reachable subset is at least internally
/// consistent, which is a different piece of information.
fn converged(devs: &BTreeMap<String, DeviceState>) -> (bool, bool, String) {
    if devs.is_empty() {
        return (false, false, "no node seen yet".into());
    }
    let live: Vec<&DeviceState> = devs.values().filter(|d| d.reachable).collect();
    let live_aligned = live.len() >= 2
        && live.iter().all(|d| d.fingerprint == live[0].fingerprint);

    let down: Vec<&str> = devs.values()
        .filter(|d| !d.reachable)
        .map(|d| d.device.as_str())
        .collect();
    if !down.is_empty() {
        return (false, live_aligned,
            format!("unreachable: {} — {}", down.join(", "),
                if live_aligned { "live nodes are aligned with each other" }
                else { "and live nodes are not aligned" }));
    }
    if devs.len() < 2 {
        return (false, false, "only one node: nothing to converge".into());
    }
    let first = &devs.values().next().unwrap().fingerprint;
    if devs.values().all(|d| &d.fingerprint == first) {
        (true, true, format!("{} nodes aligned on {}", devs.len(), first))
    } else {
        let diff: Vec<String> = devs.values()
            .map(|d| format!("{}={}", d.device, d.fingerprint))
            .collect();
        (false, false, diff.join("  "))
    }
}

async fn api_state(State(l): State<Log>) -> Json<serde_json::Value> {
    let g = l.inner.lock().unwrap();
    let (ok, live_aligned, detail) = converged(&g.devices);
    Json(json!({
        "converged": ok,
        "live_aligned": live_aligned,
        "detail": detail,
        "devices": g.devices.values().cloned().collect::<Vec<_>>(),
        "events": g.events.iter().take(120).cloned().collect::<Vec<_>>(),
    }))
}

async fn api_assert(State(l): State<Log>) -> impl IntoResponse {
    let g = l.inner.lock().unwrap();
    let (ok, live_aligned, detail) = converged(&g.devices);
    let code = if ok { StatusCode::OK } else { StatusCode::CONFLICT };
    (code, Json(json!({ "converged": ok, "live_aligned": live_aligned, "detail": detail })))
}

/// Polls one target's `/state` and folds the result into shared state,
/// diffing against whatever was cached for that device before this poll
/// to synthesize events (see `Inner::events`) — connectivity flips, the
/// `/v1/online` switch flipping, and per-entity writes/deletes. Shared by
/// the background `poller` loop and by `device_write`/`device_online`
/// (see below), which call this once, immediately, right after their own
/// action completes — otherwise the dashboard would only pick up a just
/// -made change on the *next* second-ly poll, and in the meantime could
/// easily show some other, unrelated device's own next poll landing
/// first, making the edit look like it "propagated to everyone else
/// before showing on its own card".
async fn poll_one(l: &Log, http: &reqwest::Client, t: &str) {
    // all the network work BEFORE taking the lock: holding a MutexGuard
    // across an await would block the whole logger
    let fetched: Option<serde_json::Value> = match http.get(format!("http://{t}/state")).send().await {
        Ok(r) => r.json::<serde_json::Value>().await.ok(),
        Err(_) => None,
    };

    let mut g = l.inner.lock().unwrap();
    match fetched {
        Some(v) => {
            let name = v.get("device").and_then(|d| d.as_str()).unwrap_or(t).to_string();
            let fingerprint: String = v.get("fingerprint").and_then(|f| f.as_str()).unwrap_or("").into();
            let entries: Vec<serde_json::Value> = v.get("entries").and_then(|e| e.as_array()).cloned().unwrap_or_default();
            let online = v.get("online").and_then(|o| o.as_bool()).unwrap_or(true);
            let vv = v.get("vv").cloned().unwrap_or(json!({}));

            g.by_target.insert(t.to_string(), name.clone());
            g.target_of.insert(name.clone(), t.to_string());

            match g.devices.get(&name).cloned() {
                None => {
                    g.push_event(&name, "first-seen", json!({}));
                }
                Some(old) => {
                    if !old.reachable {
                        g.push_event(&name, "reachable", json!({}));
                    }
                    if old.online && !online {
                        g.push_event(&name, "node.offline", json!({}));
                    } else if !old.online && online {
                        g.push_event(&name, "node.online", json!({}));
                    }
                    let old_map = entries_map(&old.entries);
                    let new_map = entries_map(&entries);
                    for (k, v) in &new_map {
                        if old_map.get(k) != Some(v) {
                            g.push_event(&name, "write", json!({ "entity": k, "value": v }));
                        }
                    }
                    for k in old_map.keys() {
                        if !new_map.contains_key(k) {
                            g.push_event(&name, "delete", json!({ "entity": k }));
                        }
                    }
                }
            }

            g.devices.insert(name.clone(), DeviceState { device: name, fingerprint, entries, vv, reachable: true, online });
        }
        None => {
            // mark unreachable without deleting it: during a partition
            // you still want to see its last fingerprint
            if let Some(name) = g.by_target.get(t).cloned() {
                let was_reachable = g.devices.get(&name).map(|d| d.reachable).unwrap_or(false);
                if let Some(d) = g.devices.get_mut(&name) {
                    d.reachable = false;
                }
                if was_reachable {
                    g.push_event(&name, "unreachable", json!({}));
                }
            }
        }
    }
}

/// Polls every node's `/state` once a second. The logger is the only
/// thing that talks to all of them: the nodes don't know it exists.
async fn poller(l: Log) {
    // Generous on purpose: a paused container (phase 2 of scenario.sh)
    // still needs to time out reliably, but Docker's own bridge network
    // and embedded DNS resolver occasionally add a second or so of
    // latency to an otherwise-healthy request — a tighter timeout here
    // flags a live node as unreachable on every such blip, cycling the
    // dashboard between aligned/not-aligned with nothing actually wrong.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();
    loop {
        for t in &l.targets {
            poll_one(&l, &http, t).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

async fn index() -> Html<&'static str> {
    Html(UI)
}

/// Forwards a command to its node, over the internal Docker network. The
/// UI only talks to the logger: no CORS to configure on the nodes, and
/// the devices' ports stay an implementation detail.
async fn proxy_post(l: &Log, id: &str, path: &str, body: serde_json::Value) -> Response {
    let target = { l.inner.lock().unwrap().target_of.get(id).cloned() };
    let Some(target) = target else { return StatusCode::NOT_FOUND.into_response() };
    let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(3)).build().unwrap();
    match http.post(format!("http://{target}{path}")).json(&body).send().await {
        Ok(r) => StatusCode::from_u16(r.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY)
            .into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

/// Re-polls one device right away, out of band from the once-a-second
/// background loop — see `poll_one`'s doc comment for why this matters.
async fn refresh_device(l: &Log, device_id: &str) {
    let target = { l.inner.lock().unwrap().target_of.get(device_id).cloned() };
    if let Some(t) = target {
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(3)).build().unwrap();
        poll_one(l, &http, &t).await;
    }
}

async fn device_write(
    State(l): State<Log>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let resp = proxy_post(&l, &id, "/write", body).await;
    refresh_device(&l, &id).await;
    resp
}

async fn device_online(
    State(l): State<Log>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let resp = proxy_post(&l, &id, "/v1/online", body).await;
    refresh_device(&l, &id).await;
    resp
}

#[tokio::main]
async fn main() {
    let targets: Vec<String> = std::env::var("DEVICES")
        .unwrap_or_else(|_| "device-a:47100,device-b:47100,device-c:47100".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(9000);

    let log = Log { inner: Arc::new(Mutex::new(Inner::default())), targets: targets.clone() };
    tokio::spawn(poller(log.clone()));

    let app = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/assert", get(api_assert))
        .route("/api/devices/:id/write", post(device_write))
        .route("/api/devices/:id/online", post(device_online))
        .with_state(log);

    println!("logger on :{port}, watching {}", targets.join(" "));
    let l = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    axum::serve(l, app).await.unwrap();
}

const UI: &str = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>syncd — demo</title>
<style>
  :root { --ink:#14181C; --muted:#667079; --line:#C9CFD3; --teal:#1B5E7E;
          --band:#DCEAF0; --paper:#F5F6F4; --warn:#8C5A20; --bad:#A03232; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--paper); color:var(--ink);
         font:14px/1.5 Inter,-apple-system,Segoe UI,Helvetica,Arial,sans-serif; }
  header { padding:22px 28px 14px; }
  h1 { margin:0; font-size:21px; font-weight:600; }
  .sub { color:var(--muted); font-size:13px; margin-top:4px; }
  #verdict { margin:16px 28px; padding:14px 18px; border-radius:6px;
             border:1px solid var(--teal); background:var(--band); }
  #verdict.bad { border-color:var(--bad); background:#F7E9E9; }
  #verdict .big { font-size:17px; font-weight:600; }
  #verdict .det { font-size:12.5px; color:var(--muted); margin-top:4px;
                  font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
  main { display:flex; flex-direction:column; gap:22px; padding:0 28px 28px; }
  section h2 { font-size:13px; font-weight:600; color:var(--muted); margin:0 0 10px; }

  .devices { display:grid; grid-template-columns:repeat(3, 1fr); gap:14px; }
  @media (max-width:900px){ .devices { grid-template-columns:1fr; } }

  .card { border:1px solid var(--line); border-radius:8px; background:#fff;
          padding:14px 16px; display:flex; flex-direction:column; gap:10px; }
  .card.offline { border-style:dashed; }
  .card.down { opacity:.55; border-style:dashed; }
  .card-head { display:flex; justify-content:space-between; align-items:center; }
  .card-head .name { font-weight:600; font-size:15px; }

  .switch { position:relative; display:inline-block; width:36px; height:21px; flex:none; }
  .switch input { opacity:0; width:0; height:0; }
  .switch .slider { position:absolute; inset:0; background:var(--line); border-radius:22px;
                     cursor:pointer; transition:background .15s; }
  .switch .slider::before { content:""; position:absolute; height:15px; width:15px; left:3px;
                             bottom:3px; background:#fff; border-radius:50%; transition:transform .15s; }
  .switch input:checked + .slider { background:var(--teal); }
  .switch input:checked + .slider::before { transform:translateX(15px); }
  .switch input:disabled + .slider { opacity:.4; cursor:not-allowed; }

  .meta { font-size:12px; color:var(--muted); display:flex; align-items:center; gap:6px; }
  .meta .fp { margin-left:auto; font-family:ui-monospace,Menlo,monospace; color:var(--teal); }
  .dot { width:7px; height:7px; border-radius:50%; background:var(--teal); flex:none; }
  .dot.warn { background:var(--warn); }
  .dot.bad { background:var(--bad); }

  .kv-list { display:flex; flex-direction:column; gap:6px; max-height:220px; overflow:auto; }
  .kv-row { display:flex; align-items:center; gap:6px; }
  .kv-row .k { min-width:70px; max-width:110px; overflow:hidden; text-overflow:ellipsis;
               white-space:nowrap; font-family:ui-monospace,Menlo,monospace; font-size:12px;
               color:var(--teal); }
  .kv-row .v-input { flex:1; min-width:0; font:12px ui-monospace,Menlo,monospace;
                      padding:4px 6px; border:1px solid var(--line); border-radius:4px;
                      background:var(--paper); color:var(--ink); }
  .kv-row .v-input:focus { outline:2px solid var(--teal); outline-offset:0; background:#fff; }
  .del-btn { border:none; background:transparent; color:var(--bad); font-size:15px;
             cursor:pointer; line-height:1; padding:2px 6px; border-radius:4px; }
  .del-btn:hover:not(:disabled) { background:#F7E9E9; }
  .empty { font-size:12px; color:var(--muted); font-style:italic; }

  .add-form { display:flex; gap:6px; border-top:1px solid var(--line); padding-top:8px; }
  .add-form input { flex:1; min-width:0; font:12px ui-monospace,Menlo,monospace;
                     padding:4px 6px; border:1px solid var(--line); border-radius:4px; }
  .add-form button { border:1px solid var(--teal); background:var(--band); color:var(--teal);
                      border-radius:4px; padding:4px 10px; cursor:pointer; font-weight:600; }
  .add-form button:disabled, .add-form input:disabled,
  .v-input:disabled, .del-btn:disabled { opacity:.5; cursor:not-allowed; }

  ul.ev { list-style:none; margin:0; padding:0; max-height:320px; overflow:auto;
          border:1px solid var(--line); border-radius:6px; background:#fff; }
  ul.ev li { padding:6px 12px; border-bottom:1px solid #EEF1F2;
             font-family:ui-monospace,Menlo,monospace; font-size:12px; }
  .k { display:inline-block; min-width:110px; color:var(--teal); }
  .k.warn { color:var(--warn); }
  .d { color:var(--muted); }
  .who { display:inline-block; min-width:78px; font-weight:600; }
</style></head><body>
<header>
  <h1>syncd — demo</h1>
  <div class="sub">Each card is an independent device: edit its entries, and flip it offline with the switch to see it work locally while diverging from the others, then flip it back on and watch it reconverge with no manual reconciliation. That switch calls <code>/v1/online</code> on the node itself (see main.rs) — a real two-way partition of the sync surface, not a UI-only simulation. Pausing a container (see scenario.sh) is the other, blunter way to test this: it freezes the whole process, local reads and writes included, where the switch only stops syncing.</div>
</header>
<div id="verdict"><div class="big">waiting for nodes…</div><div class="det"></div></div>
<main>
  <section>
    <h2>Devices</h2>
    <div class="devices" id="devs"></div>
  </section>
  <section>
    <h2>Events</h2>
    <ul class="ev" id="evs"></ul>
  </section>
</main>
<script>
const esc = s => String(s).replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
const escAttr = s => String(s).replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));

async function writeEntity(device, entity, value) {
  await fetch(`/api/devices/${encodeURIComponent(device)}/write`, {
    method: 'POST', headers: {'content-type': 'application/json'},
    body: JSON.stringify({ entity, value }),
  });
  tick();
}
async function deleteEntity(device, entity) {
  await fetch(`/api/devices/${encodeURIComponent(device)}/write`, {
    method: 'POST', headers: {'content-type': 'application/json'},
    body: JSON.stringify({ entity, value: null }),
  });
  tick();
}
async function setOnline(device, online) {
  await fetch(`/api/devices/${encodeURIComponent(device)}/online`, {
    method: 'POST', headers: {'content-type': 'application/json'},
    body: JSON.stringify({ online }),
  });
  tick();
}

// While the user is typing in a text field, don't touch THAT card:
// otherwise the poll every second would rip focus out from under their
// fingers. This is deliberately per-device, not global: if only device-c's
// card has focus, device-a and device-b must keep updating. The
// online/offline switch is deliberately excluded: it's a single click,
// not typing, so there's no focus-stealing risk to guard against.
const EDITABLE = '.v-input, .new-k, .new-v';
let focusedDevice = null;
document.addEventListener('focusin', e => {
  if (e.target.matches(EDITABLE)) focusedDevice = e.target.closest('[data-device]')?.dataset.device ?? null;
});
document.addEventListener('focusout', e => { if (e.target.matches(EDITABLE)) focusedDevice = null; });

const devs = document.getElementById('devs');

// The value at the moment of focus: without comparing it to the value at
// blur, every field that gets clicked (even just to look at it, without
// changing it) would resend a write with whatever value it held at that
// moment. If that value was still the old one (because sync from another
// device hadn't arrived yet), that phantom write wins the LWW race — it's
// more recent — and silently undoes the update that had just propagated
// elsewhere.
devs.addEventListener('focusin', e => {
  if (e.target.matches('.v-input')) e.target.dataset.orig = e.target.value;
});

devs.addEventListener('click', e => {
  const del = e.target.closest('.del-btn');
  if (del) deleteEntity(del.dataset.device, del.dataset.entity);
});
devs.addEventListener('change', e => {
  if (e.target.matches('.online-toggle')) setOnline(e.target.dataset.device, e.target.checked);
});
devs.addEventListener('submit', e => {
  const form = e.target.closest('.add-form');
  if (!form) return;
  e.preventDefault();
  const k = form.querySelector('.new-k').value.trim();
  const v = form.querySelector('.new-v').value;
  if (!k) return;
  writeEntity(form.dataset.device, k, v);
  form.reset();
  // form.reset() clears the fields' values but does NOT blur them, so
  // .new-k/.new-v (still focused) would otherwise keep focusedDevice set
  // to this device — the immediately-following tick() would then skip
  // re-rendering exactly the card that was just edited (see syncDevices),
  // making the edit appear to show up on every OTHER card before it shows
  // up on its own.
  if (document.activeElement) document.activeElement.blur();
  focusedDevice = null;
});
devs.addEventListener('focusout', e => {
  if (e.target.matches('.v-input') && e.target.value !== e.target.dataset.orig) {
    writeEntity(e.target.dataset.device, e.target.dataset.entity, e.target.value);
  }
});
devs.addEventListener('keydown', e => {
  if (e.target.matches('.v-input') && e.key === 'Enter') e.target.blur();
});

function renderCard(d) {
  const cls = !d.reachable ? 'down' : (!d.online ? 'offline' : '');
  const rows = d.entries.map(e => `
    <div class="kv-row">
      <span class="k">${esc(e.entity)}</span>
      <input class="v-input" data-device="${escAttr(d.device)}" data-entity="${escAttr(e.entity)}"
             value="${escAttr(e.value ?? '')}" ${d.reachable ? '' : 'disabled'} />
      <button class="del-btn" data-device="${escAttr(d.device)}" data-entity="${escAttr(e.entity)}"
              title="delete" ${d.reachable ? '' : 'disabled'}>×</button>
    </div>`).join('') || '<div class="empty">no entries</div>';

  const dot = !d.reachable ? 'bad' : (!d.online ? 'warn' : '');
  const status = !d.reachable ? 'unreachable' : (d.online ? 'online' : 'offline · working locally');

  return `
    <div class="card ${cls}" data-device="${escAttr(d.device)}">
      <div class="card-head">
        <span class="name">${esc(d.device)}</span>
        <label class="switch">
          <input type="checkbox" class="online-toggle" data-device="${escAttr(d.device)}"
                 ${d.online ? 'checked' : ''} ${d.reachable ? '' : 'disabled'} />
          <span class="slider"></span>
        </label>
      </div>
      <div class="meta"><span class="dot ${dot}"></span>${status}<span class="fp">${esc(d.fingerprint || '—')}</span></div>
      <div class="kv-list">${rows}</div>
      <form class="add-form" data-device="${escAttr(d.device)}">
        <input class="new-k" placeholder="key" ${d.reachable ? '' : 'disabled'} />
        <input class="new-v" placeholder="value" ${d.reachable ? '' : 'disabled'} />
        <button type="submit" ${d.reachable ? '' : 'disabled'}>+</button>
      </form>
    </div>`;
}

// Updates each card independently: only replaces the ones other than the
// card currently focused, so editing one device doesn't block the view of
// the other two.
function syncDevices(devices) {
  if (devices.length === 0) {
    if (!focusedDevice) devs.innerHTML = '<div class="empty">no devices</div>';
    return;
  }
  for (const d of devices) {
    if (d.device === focusedDevice) continue;
    const html = renderCard(d);
    const existing = devs.querySelector(`.card[data-device="${CSS.escape(d.device)}"]`);
    if (existing) existing.outerHTML = html;
    else devs.insertAdjacentHTML('beforeend', html);
  }
}

async function tick(){
  let s; try { s = await (await fetch('/api/state')).json(); } catch(e){ return; }

  const v = document.getElementById('verdict');
  v.className = s.converged ? '' : 'bad';
  v.querySelector('.big').textContent = s.converged
      ? 'Replicas aligned'
      : (s.live_aligned ? 'Aligned, but one node is not responding' : 'Replicas not aligned');
  v.querySelector('.det').textContent = s.detail;

  syncDevices(s.devices);

  document.getElementById('evs').innerHTML = s.events.map(e => {
    const warn = (e.kind === 'unreachable') ? ' warn' : '';
    let d = '';
    if (e.kind === 'write') d = `${e.data.entity} = ${e.data.value}`;
    else if (e.kind === 'delete') d = `${e.data.entity} deleted`;
    else if (e.kind === 'unreachable') d = 'stopped responding';
    else if (e.kind === 'reachable') d = 'responding again';
    else if (e.kind === 'node.online') d = 'back online';
    else if (e.kind === 'node.offline') d = 'goes offline, working locally';
    else if (e.kind === 'first-seen') d = 'first contact';
    else d = JSON.stringify(e.data);
    return `<li><span class="who">${esc(e.device)}</span><span class="k${warn}">${esc(e.kind)}</span><span class="d">${esc(d)}</span></li>`;
  }).join('');
}
tick(); setInterval(tick, 1000);
</script>
</body></html>
"##;
