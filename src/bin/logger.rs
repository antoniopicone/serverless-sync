//! logger — osservatore del banco di prova.
//!
//! NON e' un peer e NON sta nel percorso di sync. Fa due cose:
//!   1. riceve eventi in push dai nodi (POST /ev) per la timeline;
//!   2. interroga /v1/state di ogni nodo per il fingerprint autoritativo.
//!
//! Il fingerprint e' il semaforo: uguale su tutti i nodi vuol dire che le
//! repliche sono allineate, diverso vuol dire che il merge e' divergente.
//! GET /api/assert restituisce 200 se convergono e 409 se no, cosi' lo
//! script di scenario puo' fallire con un exit code invece che a occhio.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const MAX_EVENTS: usize = 400;

#[derive(Clone, Serialize)]
struct Event {
    seq: u64,
    ts: u64,
    device: String,
    kind: String,
    data: serde_json::Value,
}

#[derive(Clone, Serialize, Default)]
struct DeviceState {
    device: String,
    fingerprint: String,
    entries: Vec<serde_json::Value>,
    vv: serde_json::Value,
    reachable: bool,
    /// Stato dell'interruttore locale (v. /v1/online sul nodo), non la
    /// raggiungibilita': un dispositivo puo' essere "offline" e restare
    /// perfettamente raggiungibile e editabile in locale — e' il punto
    /// della demo. Solo la sua sync verso gli altri si ferma.
    online: bool,
    last_seen: u64,
}

#[derive(Default)]
struct Inner {
    events: VecDeque<Event>,
    seq: u64,
    devices: BTreeMap<String, DeviceState>,
    /// target di polling -> nome del device, imparato al primo poll riuscito.
    /// Serve per attribuire correttamente un fallimento: senza, un nodo che
    /// smette di rispondere resta marcato raggiungibile e la partizione
    /// non si vede.
    by_target: BTreeMap<String, String>,
    /// L'inverso: nome del device -> target di rete. La UI parla solo col
    /// logger (niente CORS verso le porte dei nodi); write e toggle online
    /// vengono inoltrati al target giusto passando da qui.
    target_of: BTreeMap<String, String>,
}

#[derive(Clone)]
struct Log {
    inner: Arc<Mutex<Inner>>,
    targets: Vec<String>,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Deserialize)]
struct EvIn {
    device: String,
    kind: String,
    #[serde(default)]
    data: serde_json::Value,
}

async fn ingest(State(l): State<Log>, Json(ev): Json<EvIn>) -> StatusCode {
    let mut g = l.inner.lock().unwrap();
    g.seq += 1;
    let seq = g.seq;
    g.events.push_front(Event {
        seq,
        ts: now(),
        device: ev.device,
        kind: ev.kind,
        data: ev.data,
    });
    while g.events.len() > MAX_EVENTS {
        g.events.pop_back();
    }
    StatusCode::NO_CONTENT
}

/// Verde solo se TUTTI i nodi conosciuti rispondono e hanno lo stesso
/// fingerprint.
///
/// La sfumatura conta. Se un nodo e' sospeso e ci si limita a guardare
/// quelli vivi, i due rimasti concordano e il semaforo diventa verde in
/// piena partizione: una convergenza che non c'e'. Un nodo che non
/// risponde e' uno stato ignoto, non uno stato d'accordo. Per questo
/// `live_aligned` resta esposto a parte: dice se il sottoinsieme
/// raggiungibile e' coerente, che e' un'informazione diversa.
fn converged(devs: &BTreeMap<String, DeviceState>) -> (bool, bool, String) {
    if devs.is_empty() {
        return (false, false, "nessun nodo ancora visto".into());
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
            format!("non raggiungibile: {} — {}", down.join(", "),
                if live_aligned { "i nodi vivi sono allineati fra loro" }
                else { "e i nodi vivi non sono allineati" }));
    }
    if devs.len() < 2 {
        return (false, false, "un solo nodo: niente da far convergere".into());
    }
    let first = &devs.values().next().unwrap().fingerprint;
    if devs.values().all(|d| &d.fingerprint == first) {
        (true, true, format!("{} nodi allineati su {}", devs.len(), first))
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

/// Interroga /v1/state di ogni nodo. Il logger e' l'unico che parla con
/// tutti: i nodi non sanno che esiste, oltre a spedirgli eventi alla cieca.
async fn poller(l: Log) {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(900))
        .build()
        .unwrap();
    loop {
        for t in &l.targets {
            // tutta la parte di rete PRIMA di prendere il lock: tenere un
            // MutexGuard attraverso un await bloccherebbe l'intero logger
            let fetched: Option<serde_json::Value> = match http
                .get(format!("http://{t}/v1/state"))
                .send()
                .await
            {
                Ok(r) => r.json::<serde_json::Value>().await.ok(),
                Err(_) => None,
            };

            let mut g = l.inner.lock().unwrap();
            match fetched {
                Some(v) => {
                    let name = v.get("device").and_then(|d| d.as_str()).unwrap_or(t).to_string();
                    g.by_target.insert(t.clone(), name.clone());
                    g.target_of.insert(name.clone(), t.clone());
                    g.devices.insert(
                        name.clone(),
                        DeviceState {
                            device: name,
                            fingerprint: v.get("fingerprint").and_then(|f| f.as_str()).unwrap_or("").into(),
                            entries: v.get("entries").and_then(|e| e.as_array()).cloned().unwrap_or_default(),
                            vv: v.get("vv").cloned().unwrap_or(json!({})),
                            reachable: true,
                            online: v.get("online").and_then(|o| o.as_bool()).unwrap_or(true),
                            last_seen: now(),
                        },
                    );
                }
                None => {
                    // marca irraggiungibile senza cancellarlo: durante una
                    // partizione vuoi ancora vedere il suo ultimo fingerprint
                    if let Some(name) = g.by_target.get(t).cloned() {
                        if let Some(d) = g.devices.get_mut(&name) {
                            d.reachable = false;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

async fn index() -> Html<&'static str> {
    Html(UI)
}

/// Inoltra un comando di un dispositivo verso il suo nodo, sulla rete
/// Docker interna. La UI parla solo col logger: niente CORS da configurare
/// sui nodi, e le porte dei device restano un dettaglio implementativo.
async fn proxy_post(l: &Log, id: &str, path: &str, body: serde_json::Value) -> Response {
    let target = { l.inner.lock().unwrap().target_of.get(id).cloned() };
    let Some(target) = target else { return StatusCode::NOT_FOUND.into_response() };
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(1500))
        .build()
        .unwrap();
    match http.post(format!("http://{target}{path}")).json(&body).send().await {
        Ok(r) => StatusCode::from_u16(r.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY)
            .into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn device_write(
    State(l): State<Log>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    proxy_post(&l, &id, "/v1/write", body).await
}

async fn device_online(
    State(l): State<Log>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    proxy_post(&l, &id, "/v1/online", body).await
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
        .route("/ev", post(ingest))
        .route("/api/state", get(api_state))
        .route("/api/assert", get(api_assert))
        .route("/api/devices/:id/write", post(device_write))
        .route("/api/devices/:id/online", post(device_online))
        .with_state(log);

    println!("logger su :{port}, osservo {}", targets.join(" "));
    let l = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    axum::serve(l, app).await.unwrap();
}

const UI: &str = r##"<!doctype html>
<html lang="it"><head><meta charset="utf-8">
<title>syncd — banco di prova</title>
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
  .k { display:inline-block; min-width:132px; color:var(--teal); }
  .k.warn { color:var(--warn); }
  .d { color:var(--muted); }
  .who { display:inline-block; min-width:78px; font-weight:600; }
</style></head><body>
<header>
  <h1>syncd — banco di prova</h1>
  <div class="sub">Ogni card e' un dispositivo indipendente: modifica le sue voci, spegnilo, e vedi come lavora in locale finche' non torna online. Il logger osserva soltanto: se lo spegni, i nodi continuano a sincronizzarsi da soli.</div>
</header>
<div id="verdict"><div class="big">in attesa dei nodi…</div><div class="det"></div></div>
<main>
  <section>
    <h2>Dispositivi</h2>
    <div class="devices" id="devs"></div>
  </section>
  <section>
    <h2>Eventi</h2>
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

// Mentre l'utente sta scrivendo in un campo testo non tocchiamo QUELLA card:
// altrimenti il poll ogni secondo gli strapperebbe il focus da sotto le
// dita. E' per-dispositivo apposta, non globale: se e' solo la card di
// device-c ad avere il focus, device-a e device-b devono continuare ad
// aggiornarsi — altrimenti resterebbero visivamente fermi a un istantanea
// vecchia (e magari non ancora convergente) anche se sul serio hanno gia'
// finito di sincronizzarsi. Lo switch online/offline resta escluso apposta:
// e' un click singolo, non una digitazione.
const EDITABLE = '.v-input, .new-k, .new-v';
let focusedDevice = null;
// .new-k/.new-v non portano data-device: sta sul <form class="add-form"> che
// li contiene, quindi si risale con closest (che copre anche .v-input, che
// invece ce l'ha gia' su di se').
document.addEventListener('focusin', e => {
  if (e.target.matches(EDITABLE)) focusedDevice = e.target.closest('[data-device]')?.dataset.device ?? null;
});
document.addEventListener('focusout', e => { if (e.target.matches(EDITABLE)) focusedDevice = null; });

const devs = document.getElementById('devs');

// Il valore al momento del focus: senza confrontarlo con quello al blur, ogni
// campo cliccato (anche solo per guardarlo, senza modificarlo) reinvierebbe
// una scrittura col valore che aveva in quel momento. Se quel valore era
// ancora quello vecchio (perche' la sync da un altro dispositivo non era
// ancora arrivata), quella scrittura fantasma vince la corsa LWW — e' piu'
// recente — e annulla in silenzio l'aggiornamento appena propagato altrove.
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
              title="elimina" ${d.reachable ? '' : 'disabled'}>×</button>
    </div>`).join('') || '<div class="empty">nessuna voce</div>';

  const dot = !d.reachable ? 'bad' : (!d.online ? 'warn' : '');
  const status = !d.reachable ? 'non raggiungibile' : (d.online ? 'online' : 'offline · lavora in locale');

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
        <input class="new-k" placeholder="chiave" ${d.reachable ? '' : 'disabled'} />
        <input class="new-v" placeholder="valore" ${d.reachable ? '' : 'disabled'} />
        <button type="submit" ${d.reachable ? '' : 'disabled'}>+</button>
      </form>
    </div>`;
}

// Aggiorna ogni card indipendentemente: sostituisce solo quelle diverse
// dalla card che ha il focus in questo momento, cosi' un dispositivo in
// modifica non blocca la vista sugli altri due.
function syncDevices(devices) {
  if (devices.length === 0) {
    if (!focusedDevice) devs.innerHTML = '<div class="empty">nessun dispositivo</div>';
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
      ? 'Repliche allineate'
      : (s.live_aligned ? 'Allineate, ma un nodo non risponde' : 'Repliche non allineate');
  v.querySelector('.det').textContent = s.detail;

  syncDevices(s.devices);

  document.getElementById('evs').innerHTML = s.events.map(e => {
    const warn = (e.kind === 'peer.unreachable') ? ' warn' : '';
    let d = '';
    if (e.kind === 'sync.ok') d = `+${e.data.pulled} ricevuti, +${e.data.pushed} inviati  da ${e.data.peer}`;
    else if (e.kind === 'op.local') d = `${e.data.entity}  seq ${e.data.seq}`;
    else if (e.kind === 'state') d = `${e.data.fingerprint}  (${e.data.entries} voci)`;
    else if (e.kind === 'peer.unreachable') d = e.data.peer;
    else if (e.kind === 'node.start') d = e.data.advertise || '';
    else if (e.kind === 'node.online') d = 'torna online';
    else if (e.kind === 'node.offline') d = 'passa offline, lavora in locale';
    else d = JSON.stringify(e.data);
    return `<li><span class="who">${esc(e.device)}</span><span class="k${warn}">${esc(e.kind)}</span><span class="d">${esc(d)}</span></li>`;
  }).join('');
}
tick(); setInterval(tick, 1000);
</script>
</body></html>
"##;
