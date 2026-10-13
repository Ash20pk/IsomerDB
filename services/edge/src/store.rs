//! A replica's copy on disk, so a restarted edge serves at once and resumes from its cursor
//! instead of taking a new snapshot.
//!
//! Each projection has a directory holding `snapshot.json` (every document and the cursor they
//! are as of) and `log.jsonl` (each change applied since, one per line). A change is appended to
//! the log as it is applied and the log is synced once per batch the origin sends. Losing the
//! tail of the log in a crash only moves the cursor back: changes are keyed upserts and deletes,
//! so applying them again is harmless. Once the log outgrows the snapshot it is folded into a new
//! one, written to a temporary file and renamed over the old.
//!
//! The snapshot records the origin and the projection's incarnation (when its history was turned
//! on). A copy made from anything else is discarded, because a feed cursor means nothing across
//! them: a projection dropped and created again keeps its name but starts a new history.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const FORMAT: u32 = 1;
/// The log is folded into a new snapshot once it holds this many changes and more changes than
/// the snapshot has documents.
const MIN_COMPACT: usize = 10_000;

/// Where a copy came from: a cursor is only meaningful against the same origin and incarnation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Lineage {
    pub origin: String,
    pub projection: String,
    pub since: String,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    format: u32,
    lineage: Lineage,
    cursor: i64,
    docs: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct Logged {
    id: i64,
    key: String,
    /// None: the key was deleted.
    doc: Option<Value>,
}

/// What was read back from disk.
pub struct Restored {
    pub cursor: i64,
    pub docs: HashMap<String, Value>,
    pub replayed: usize,
}

pub struct Store {
    dir: PathBuf,
    lineage: Option<Lineage>,
    log: Option<BufWriter<File>>,
    logged: usize,
    dirty: bool,
}

fn sync_dir(dir: &Path) {
    // Makes a rename durable; not every platform can open a directory, which is fine.
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

impl Store {
    pub fn open(root: &Path, projection: &str) -> Result<Store> {
        let dir = root.join(projection);
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        Ok(Store { dir, lineage: None, log: None, logged: 0, dirty: false })
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot.json")
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("log.jsonl")
    }

    /// The copy on disk, if there is one from this lineage. Anything else is removed.
    pub fn restore(&mut self, lineage: &Lineage) -> Result<Option<Restored>> {
        let snap = match File::open(self.snapshot_path()) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let snap: Snapshot = match serde_json::from_reader(BufReader::new(snap)) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(dir = %self.dir.display(), "unreadable snapshot ({e}); starting afresh");
                self.discard()?;
                return Ok(None);
            }
        };
        if snap.format != FORMAT || &snap.lineage != lineage {
            tracing::warn!(dir = %self.dir.display(), stored = ?snap.lineage, current = ?lineage,
                "the copy on disk is from another origin or incarnation; starting afresh");
            self.discard()?;
            return Ok(None);
        }
        let (mut cursor, mut docs) = (snap.cursor, snap.docs);

        // Replay the log up to its first torn line (the tail of a write cut short), and cut it
        // there so later appends follow a whole line.
        let mut replayed = 0;
        let mut good = 0u64;
        if let Ok(f) = File::open(self.log_path()) {
            let mut reader = BufReader::new(f);
            let mut line = String::new();
            loop {
                line.clear();
                let n = reader.read_line(&mut line)?;
                if n == 0 || !line.ends_with('\n') {
                    break;
                }
                let Ok(entry) = serde_json::from_str::<Logged>(&line) else { break };
                good += n as u64;
                if entry.id <= cursor {
                    continue;
                }
                match entry.doc {
                    Some(doc) => docs.insert(entry.key, doc),
                    None => docs.remove(&entry.key),
                };
                cursor = entry.id;
                replayed += 1;
            }
            let f = OpenOptions::new().write(true).open(self.log_path())?;
            if f.metadata()?.len() != good {
                f.set_len(good)?;
                f.sync_all()?;
            }
        }
        self.lineage = Some(lineage.clone());
        self.logged = replayed;
        Ok(Some(Restored { cursor, docs, replayed }))
    }

    fn discard(&mut self) -> Result<()> {
        for p in [self.snapshot_path(), self.log_path()] {
            match fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.log = None;
        self.logged = 0;
        Ok(())
    }

    /// Replace everything with a new snapshot, and start an empty log after it.
    pub fn snapshot(&mut self, lineage: &Lineage, cursor: i64, docs: &HashMap<String, Value>) -> Result<()> {
        #[derive(Serialize)]
        struct SnapshotRef<'a> {
            format: u32,
            lineage: &'a Lineage,
            cursor: i64,
            docs: &'a HashMap<String, Value>,
        }
        let tmp = self.dir.join("snapshot.json.tmp");
        {
            let mut w = BufWriter::new(File::create(&tmp)?);
            serde_json::to_writer(&mut w, &SnapshotRef { format: FORMAT, lineage, cursor, docs })?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        fs::rename(&tmp, self.snapshot_path())?;
        // The old log is all at or before this cursor: empty it.
        let f = OpenOptions::new().create(true).write(true).truncate(true).open(self.log_path())?;
        f.sync_all()?;
        sync_dir(&self.dir);
        self.lineage = Some(lineage.clone());
        self.log = None;
        self.logged = 0;
        self.dirty = false;
        Ok(())
    }

    /// Append one applied change; durable at the next `sync`.
    pub fn append(&mut self, id: i64, key: &str, doc: Option<&Value>) -> Result<()> {
        if self.lineage.is_none() {
            return Ok(()); // nothing to append to until the first snapshot is written
        }
        if self.log.is_none() {
            let mut f = OpenOptions::new().create(true).append(true).open(self.log_path())?;
            f.seek(SeekFrom::End(0))?;
            self.log = Some(BufWriter::new(f));
        }
        let w = self.log.as_mut().unwrap();
        #[derive(Serialize)]
        struct LoggedRef<'a> {
            id: i64,
            key: &'a str,
            doc: Option<&'a Value>,
        }
        serde_json::to_writer(&mut *w, &LoggedRef { id, key, doc })?;
        w.write_all(b"\n")?;
        self.logged += 1;
        self.dirty = true;
        Ok(())
    }

    /// Make what was appended durable.
    pub fn sync(&mut self) -> Result<()> {
        if let (true, Some(w)) = (self.dirty, self.log.as_mut()) {
            w.flush()?;
            w.get_ref().sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Whether the log has grown enough to fold into a new snapshot of `documents` documents.
    pub fn wants_compaction(&self, documents: usize) -> bool {
        self.logged >= MIN_COMPACT && self.logged > documents
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lineage(since: &str) -> Lineage {
        Lineage { origin: "http://origin".into(), projection: "p".into(), since: since.into() }
    }

    fn tempdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("isomer-edge-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn snapshot_then_log_round_trips() {
        let root = tempdir("roundtrip");
        let l = lineage("t1");
        let mut s = Store::open(&root, "p").unwrap();
        assert!(s.restore(&l).unwrap().is_none());
        let docs: HashMap<String, Value> = [("1".to_string(), json!({"a": 1})), ("2".to_string(), json!({"a": 2}))].into();
        s.snapshot(&l, 10, &docs).unwrap();
        s.append(11, "1", Some(&json!({"a": 10}))).unwrap();
        s.append(12, "2", None).unwrap();
        s.append(13, "3", Some(&json!({"a": 3}))).unwrap();
        s.sync().unwrap();
        drop(s);

        let mut s = Store::open(&root, "p").unwrap();
        let r = s.restore(&l).unwrap().unwrap();
        assert_eq!(r.cursor, 13);
        assert_eq!(r.replayed, 3);
        assert_eq!(r.docs.get("1"), Some(&json!({"a": 10})));
        assert!(!r.docs.contains_key("2"));
        assert_eq!(r.docs.get("3"), Some(&json!({"a": 3})));
        // Appending after a restore continues the same log.
        s.append(14, "3", None).unwrap();
        s.sync().unwrap();
        let r = Store::open(&root, "p").unwrap().restore(&l).unwrap().unwrap();
        assert_eq!((r.cursor, r.docs.len()), (14, 1));
    }

    #[test]
    fn a_torn_tail_is_cut_off() {
        let root = tempdir("torn");
        let l = lineage("t1");
        let mut s = Store::open(&root, "p").unwrap();
        s.snapshot(&l, 1, &HashMap::new()).unwrap();
        s.append(2, "a", Some(&json!(1))).unwrap();
        s.sync().unwrap();
        drop(s);
        let mut f = OpenOptions::new().append(true).open(root.join("p/log.jsonl")).unwrap();
        f.write_all(br#"{"id": 3, "key": "b", "do"#).unwrap();
        drop(f);
        let mut s = Store::open(&root, "p").unwrap();
        let r = s.restore(&l).unwrap().unwrap();
        assert_eq!((r.cursor, r.docs.len()), (2, 1));
        s.append(3, "b", Some(&json!(2))).unwrap();
        s.sync().unwrap();
        let r = Store::open(&root, "p").unwrap().restore(&l).unwrap().unwrap();
        assert_eq!((r.cursor, r.docs.len()), (3, 2));
    }

    #[test]
    fn another_incarnation_is_discarded() {
        let root = tempdir("lineage");
        let mut s = Store::open(&root, "p").unwrap();
        s.snapshot(&lineage("t1"), 5, &[("k".to_string(), json!(1))].into()).unwrap();
        drop(s);
        let mut s = Store::open(&root, "p").unwrap();
        assert!(s.restore(&lineage("t2")).unwrap().is_none());
        assert!(!root.join("p/snapshot.json").exists());
    }
}
