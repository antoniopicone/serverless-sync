//! CRDT di esempio: registro LWW (last-writer-wins) per entita'.
//!
//! Punto di innesto dell'applicazione (vedi README.md): quando si sostituisce
//! il key/value fittizio con la logica vera, `apply` e `local_change` sono
//! le uniche due funzioni da riscrivere. Il resto del banco (rete, discovery,
//! anti-entropy) non sa cosa contiene un `Op`.
//!
//! Il tie-break dei conflitti usa un HLC (hybrid logical clock): timestamp
//! fisico + contatore logico + device_id. Il device_id nel confronto e'
//! obbligatorio, non un dettaglio: senza, due device con lo stesso
//! timestamp fisico potrebbero scegliere vincitori diversi e divergere in
//! silenzio. Con l'ordine totale (time, counter, device_id) tutti i
//! repliche convergono sullo stesso risultato indipendentemente dall'ordine
//! di applicazione — la proprieta' che rende `apply` idempotente e
//! commutativa.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type VersionVector = BTreeMap<String, u64>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hlc {
    pub time: u64,
    pub counter: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    Upsert,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Op {
    pub device: String,
    pub seq: u64,
    pub entity: String,
    pub kind: OpKind,
    pub value: String,
    pub hlc: Hlc,
}

#[derive(Clone, Debug, Serialize)]
struct Entry {
    entity: String,
    value: Option<String>,
    hlc: Hlc,
    device: String,
}

pub struct Replica {
    pub device: String,
    pub service: String,
    seq: u64,
    hlc_time: u64,
    hlc_counter: u32,
    entries: BTreeMap<String, Entry>,
    vv: VersionVector,
    log: Vec<Op>,
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl Replica {
    pub fn new(device: String, service: String) -> Self {
        Self {
            device,
            service,
            seq: 0,
            hlc_time: 0,
            hlc_counter: 0,
            entries: BTreeMap::new(),
            vv: VersionVector::new(),
            log: Vec::new(),
        }
    }

    /// Avanza l'HLC locale: mai indietro rispetto al wall clock ne' rispetto
    /// all'ultimo tick emesso da questo device.
    fn tick(&mut self) -> Hlc {
        let wall = now_millis();
        if wall > self.hlc_time {
            self.hlc_time = wall;
            self.hlc_counter = 0;
        } else {
            self.hlc_counter += 1;
        }
        Hlc { time: self.hlc_time, counter: self.hlc_counter }
    }

    /// Osserva l'HLC di un op remoto, cosi' il clock locale non resta mai
    /// indietro rispetto a quanto gia' visto: e' quello che garantisce che i
    /// prossimi op locali dominino causalmente quelli appena ricevuti.
    fn observe(&mut self, other: Hlc) {
        let wall = now_millis();
        if other.time > self.hlc_time.max(wall) {
            self.hlc_time = other.time;
            self.hlc_counter = other.counter + 1;
        } else if other.time == self.hlc_time {
            self.hlc_counter = self.hlc_counter.max(other.counter) + 1;
        } else if wall > self.hlc_time {
            self.hlc_time = wall;
            self.hlc_counter = 0;
        }
    }

    pub fn local_change(&mut self, entity: &str, kind: OpKind, value: &str) -> Op {
        self.seq += 1;
        let hlc = self.tick();
        let op = Op {
            device: self.device.clone(),
            seq: self.seq,
            entity: entity.to_string(),
            kind,
            value: value.to_string(),
            hlc,
        };
        self.merge_entry(&op);
        self.vv.insert(self.device.clone(), self.seq);
        self.log.push(op.clone());
        op
    }

    /// La regola di merge. Idempotente (riapplicare lo stesso op non cambia
    /// nulla oltre alla prima volta) e commutativa (l'ordine di arrivo non
    /// conta): entrambe derivano dal confronto totalmente ordinato
    /// (hlc, device) invece che dall'ordine di apply.
    pub fn apply(&mut self, op: Op) -> bool {
        let last = self.vv.get(&op.device).copied().unwrap_or(0);
        if op.seq <= last {
            return false; // gia' visto: idempotenza
        }
        self.observe(op.hlc);
        self.merge_entry(&op);
        self.vv.insert(op.device.clone(), op.seq);
        self.log.push(op);
        true
    }

    fn merge_entry(&mut self, op: &Op) {
        let winner = match self.entries.get(&op.entity) {
            Some(cur) => (op.hlc, op.device.clone()) > (cur.hlc, cur.device.clone()),
            None => true,
        };
        if winner {
            let value = match op.kind {
                OpKind::Upsert => Some(op.value.clone()),
                OpKind::Delete => None,
            };
            self.entries.insert(op.entity.clone(), Entry {
                entity: op.entity.clone(),
                value,
                hlc: op.hlc,
                device: op.device.clone(),
            });
        }
    }

    pub fn version_vector(&self) -> VersionVector {
        self.vv.clone()
    }

    /// Op non ancora visti dal chiamante, dato il suo version vector.
    pub fn ops_since(&self, vv: &VersionVector) -> Vec<Op> {
        self.log
            .iter()
            .filter(|o| o.seq > vv.get(&o.device).copied().unwrap_or(0))
            .cloned()
            .collect()
    }

    pub fn entries(&self) -> Vec<serde_json::Value> {
        self.entries
            .values()
            .filter(|e| e.value.is_some())
            .map(|e| serde_json::json!({ "entity": e.entity, "value": e.value }))
            .collect()
    }

    /// Hash deterministico dello stato visibile (entita' vive con il loro
    /// valore). E' il semaforo che il logger usa per decidere se le repliche
    /// sono allineate: due repliche con lo stesso stato devono produrre
    /// sempre lo stesso fingerprint, indipendentemente dall'ordine in cui
    /// hanno applicato gli op.
    pub fn state_fingerprint(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut h = DefaultHasher::new();
        for e in self.entries.values() {
            if let Some(v) = &e.value {
                e.entity.hash(&mut h);
                v.hash(&mut h);
            }
        }
        h.finish()
    }
}
