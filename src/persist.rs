//! CSV persistence for the op log.
//!
//! One line per accepted `Op` (local or received from a peer), appended as
//! it happens. `core.rs` stays the only source of truth for *state* (its
//! in-memory `entries`/`vv`/`log`) — this module only makes that state
//! survive a restart, by mirroring every op to disk and replaying them
//! back through `Replica::apply` at startup.
//!
//! Deliberately not doing compaction: like the rest of this rig (see
//! README), the log is expected to stay short enough in a demo/dev
//! session that replay cost never matters. A long-lived deployment would
//! need to periodically rewrite the file down to one row per live entity
//! — see the README for the reasoning that makes this non-trivial for a
//! CRDT log (it can't just drop old rows without also shrinking what
//! `/v1/ops/since` can still answer for a peer that's far behind).

use crate::core::{Hlc, Op, OpKind};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

fn kind_to_field(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Upsert => "upsert",
        OpKind::Delete => "delete",
    }
}

fn kind_from_field(s: &str) -> io::Result<OpKind> {
    match s {
        "upsert" => Ok(OpKind::Upsert),
        "delete" => Ok(OpKind::Delete),
        other => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unknown op kind {other:?}"))),
    }
}

fn to_record(op: &Op) -> [String; 6] {
    [
        op.device.clone(),
        op.seq.to_string(),
        op.entity.clone(),
        kind_to_field(op.kind).to_string(),
        op.value.clone(),
        format!("{}.{}", op.hlc.time, op.hlc.counter),
    ]
}

fn from_record(record: &csv::StringRecord) -> io::Result<Op> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed op row");
    let device = record.get(0).ok_or_else(bad)?.to_string();
    let seq: u64 = record.get(1).ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let entity = record.get(2).ok_or_else(bad)?.to_string();
    let kind = kind_from_field(record.get(3).ok_or_else(bad)?)?;
    let value = record.get(4).ok_or_else(bad)?.to_string();
    let (t, c) = record.get(5).ok_or_else(bad)?.split_once('.').ok_or_else(bad)?;
    let hlc = Hlc {
        time: t.parse().map_err(|_| bad())?,
        counter: c.parse().map_err(|_| bad())?,
    };
    Ok(Op { device, seq, entity, kind, value, hlc })
}

/// Reads every op currently on disk, in the order they were appended
/// (which is the order `Replica::apply` needs: within one device, seq
/// only ever increases). Returns an empty vec if the file doesn't exist
/// yet — that's just a brand new replica, not an error.
pub fn load(path: &Path) -> io::Result<Vec<Op>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_path(path)?;
    reader.records().map(|r| from_record(&r?)).collect()
}

/// The append-only writer half. Kept open for the process lifetime instead
/// of reopening per write: with thousands of ops this is the difference
/// between an O(1) and an O(n) write.
pub struct OpLog {
    file: Mutex<File>,
}

impl OpLog {
    /// Opens (creating if needed, header included only for a fresh file)
    /// the CSV log at `path` for appending.
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let is_new = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if is_new {
            writeln!(file, "device,seq,entity,kind,value,hlc")?;
            file.flush()?;
        }
        Ok(Self { file: Mutex::new(file) })
    }

    /// Appends one op and flushes: a write isn't durable to the caller
    /// until it's actually on disk, otherwise a crash right after
    /// responding 200 to `/v1/write` could still lose the op.
    pub fn append(&self, op: &Op) -> io::Result<()> {
        let record = to_record(op);
        let mut file = self.file.lock().unwrap();
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(&mut *file);
        writer.write_record(&record)?;
        writer.flush()?;
        drop(writer);
        file.flush()
    }
}

pub fn default_path(data_dir: &str, device: &str) -> PathBuf {
    Path::new(data_dir).join(format!("{device}.csv"))
}
