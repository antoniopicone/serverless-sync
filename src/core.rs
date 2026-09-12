//! Example CRDT: an LWW (last-writer-wins) register per entity.
//!
//! Application insertion point (see README.md): when the fake key/value
//! store is replaced with real logic, `apply` and `local_change` are the
//! only two functions to rewrite. The rest of the rig (network, discovery,
//! anti-entropy) doesn't know what an `Op` contains.
//!
//! Conflict tie-break uses an HLC (hybrid logical clock): physical
//! timestamp + logical counter + device_id. The device_id in the
//! comparison is mandatory, not a detail: without it, two devices with the
//! same physical timestamp could pick different winners and diverge
//! silently. With the total order (time, counter, device_id) every
//! replica converges on the same result regardless of application order —
//! the property that makes `apply` idempotent and commutative.

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

/// One independent CRDT dataset. `main.rs` keeps one `Replica` per
/// registered application (name+token) — see the registry in `main.rs` —
/// so this type itself doesn't need to know which application it belongs
/// to; that's purely a lookup key one layer up.
pub struct Replica {
    pub device: String,
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
    pub fn new(device: String) -> Self {
        Self {
            device,
            seq: 0,
            hlc_time: 0,
            hlc_counter: 0,
            entries: BTreeMap::new(),
            vv: VersionVector::new(),
            log: Vec::new(),
        }
    }

    /// Advances the local HLC: never goes backward relative to the wall
    /// clock nor to the last tick this device emitted.
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

    /// Observes a remote op's HLC, so the local clock never falls behind
    /// what it has already seen: this is what guarantees the next local
    /// ops causally dominate the ones just received.
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

    /// The merge rule. Idempotent (reapplying the same op changes nothing
    /// past the first time) and commutative (arrival order doesn't
    /// matter): both follow from comparing the totally ordered
    /// (hlc, device) pair instead of relying on apply order.
    pub fn apply(&mut self, op: Op) -> bool {
        let last = self.vv.get(&op.device).copied().unwrap_or(0);
        if op.seq <= last {
            return false; // already seen: idempotence
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

    /// Ops the caller hasn't seen yet, given its version vector.
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

    /// Deterministic hash of the visible state (live entities with their
    /// value). This is the signal the logger uses to decide whether
    /// replicas are aligned: two replicas with the same state must always
    /// produce the same fingerprint, regardless of the order in which they
    /// applied their ops.
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
