//! Durable Raft log storage.
//!
//! Entries go into append-only segment files `log-<first index>.seg` in the
//! node's directory. Each record is `[u32 length][u32 crc32][bincode entry]`.
//! The vote and the last purged log id go into `meta`, replaced atomically
//! (write `meta.tmp`, fsync, rename, fsync the directory).
//!
//! One writer thread owns the files and runs every write in order. An append
//! calls `fdatasync` before it reports the entries flushed to openraft, a new
//! or deleted segment file is followed by an fsync of the directory, and a vote
//! is durable before `save_vote` returns. On startup a torn record at the end
//! of the last segment (a crash during a write) is cut off; a bad record
//! anywhere else is an error.
//!
//! Every entry after the purge point is also kept in memory, so readers never
//! touch the files. The snapshot policy bounds how many that are.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::RangeBounds;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::OptionalSend;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{LogEntry, LogId, TypeConfig, Vote};

const META_FILE: &str = "meta";
const RECORD_HEADER: usize = 8;

/// The Raft log of one node. Cloning gives another handle to the same log.
#[derive(Clone)]
pub struct LogStore {
    mem: Arc<RwLock<MemLog>>,
    writer: mpsc::Sender<WriteCmd>,
}

#[derive(Default)]
struct MemLog {
    entries: BTreeMap<u64, LogEntry>,
    meta: Meta,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Meta {
    vote: Option<Vote>,
    last_purged: Option<LogId>,
}

enum WriteCmd {
    Append {
        records: Vec<(u64, Vec<u8>)>,
        flushed: IOFlushed<TypeConfig>,
    },
    SaveMeta {
        meta: Meta,
        done: oneshot::Sender<io::Result<()>>,
    },
    Truncate {
        from: u64,
        done: oneshot::Sender<io::Result<()>>,
    },
    Purge {
        meta: Meta,
        upto: u64,
        done: oneshot::Sender<io::Result<()>>,
    },
}

impl LogStore {
    /// Opens the log in `dir`, creating the directory if needed.
    pub fn open(dir: &Path, segment_bytes: u64) -> io::Result<LogStore> {
        fs::create_dir_all(dir)?;
        let meta = read_meta(dir)?;
        let mut segments = Segments::open(dir, segment_bytes)?;
        let purged_index = meta.last_purged.as_ref().map(|l| l.index);
        let mut entries = BTreeMap::new();
        for (index, bytes) in segments.read_all()? {
            if purged_index.is_some_and(|p| index <= p) {
                continue;
            }
            let entry: LogEntry = bincode::deserialize(&bytes).map_err(invalid_data)?;
            if entry.log_id.index != index {
                return Err(invalid_data(format!(
                    "record at index {index} holds the entry of index {}",
                    entry.log_id.index
                )));
            }
            entries.insert(index, entry);
        }
        if let (Some(p), Some(first)) = (purged_index, entries.keys().next()) {
            if *first != p + 1 {
                return Err(invalid_data(format!(
                    "log starts at {first}, but was purged up to {p}"
                )));
            }
        }
        segments.drop_purged(purged_index)?;

        let (writer, rx) = mpsc::channel();
        let dir = dir.to_path_buf();
        std::thread::Builder::new()
            .name("raft-log-writer".into())
            .spawn(move || run_writer(dir, segments, rx))?;
        Ok(LogStore {
            mem: Arc::new(RwLock::new(MemLog { entries, meta })),
            writer,
        })
    }

    /// The index of the last entry, purged ones included.
    pub fn last_index(&self) -> Option<u64> {
        let mem = self.mem.read().unwrap();
        let last = mem.entries.keys().next_back().copied();
        last.or(mem.meta.last_purged.as_ref().map(|l| l.index))
    }

    /// The last purged log id, for checking that the state machine is not
    /// behind it at startup.
    pub fn last_purged(&self) -> Option<LogId> {
        self.mem.read().unwrap().meta.last_purged
    }

    async fn call(
        &self,
        cmd: impl FnOnce(oneshot::Sender<io::Result<()>>) -> WriteCmd,
    ) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.writer
            .send(cmd(tx))
            .map_err(|_| io::Error::other("log writer thread stopped"))?;
        rx.await
            .map_err(|_| io::Error::other("log writer thread stopped"))?
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<LogEntry>> {
        let mem = self.mem.read().unwrap();
        Ok(mem.entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        Ok(self.mem.read().unwrap().meta.vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        let mem = self.mem.read().unwrap();
        let last_purged_log_id = mem.meta.last_purged;
        let last_log_id = mem
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        let mut meta = self.mem.read().unwrap().meta.clone();
        meta.vote = Some(*vote);
        self.call(|done| WriteCmd::SaveMeta {
            meta: meta.clone(),
            done,
        })
        .await?;
        self.mem.write().unwrap().meta = meta;
        Ok(())
    }

    async fn append<I>(&mut self, entries: I, flushed: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = LogEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut records = Vec::new();
        {
            let mut mem = self.mem.write().unwrap();
            for entry in entries {
                let bytes = bincode::serialize(&entry).map_err(io::Error::other)?;
                records.push((entry.log_id.index, bytes));
                mem.entries.insert(entry.log_id.index, entry);
            }
        }
        self.writer
            .send(WriteCmd::Append { records, flushed })
            .map_err(|_| io::Error::other("log writer thread stopped"))?;
        Ok(())
    }

    async fn truncate_after(&mut self, last: Option<LogId>) -> io::Result<()> {
        let from = last.map_or(0, |l| l.index + 1);
        self.mem.write().unwrap().entries.split_off(&from);
        self.call(|done| WriteCmd::Truncate { from, done }).await
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
        let mut meta = self.mem.read().unwrap().meta.clone();
        meta.last_purged = Some(log_id);
        self.call(|done| WriteCmd::Purge {
            meta: meta.clone(),
            upto: log_id.index,
            done,
        })
        .await?;
        let mut mem = self.mem.write().unwrap();
        mem.entries = mem.entries.split_off(&(log_id.index + 1));
        mem.meta = meta;
        Ok(())
    }
}

fn run_writer(dir: PathBuf, mut segments: Segments, rx: mpsc::Receiver<WriteCmd>) {
    while let Ok(cmd) = rx.recv() {
        match cmd {
            WriteCmd::Append { records, flushed } => {
                flushed.io_completed(segments.append(&records))
            }
            WriteCmd::SaveMeta { meta, done } => {
                let _ = done.send(write_meta(&dir, &meta));
            }
            WriteCmd::Truncate { from, done } => {
                let _ = done.send(segments.truncate(from));
            }
            WriteCmd::Purge { meta, upto, done } => {
                let res = write_meta(&dir, &meta).and_then(|()| segments.drop_purged(Some(upto)));
                let _ = done.send(res);
            }
        }
    }
}

struct Segments {
    dir: PathBuf,
    segment_bytes: u64,
    segs: Vec<Segment>,
}

struct Segment {
    first: u64,
    path: PathBuf,
    file: File,
    /// Byte offset of the record of index `first + i`.
    offsets: Vec<u64>,
    len: u64,
}

impl Segment {
    fn next_index(&self) -> u64 {
        self.first + self.offsets.len() as u64
    }
}

impl Segments {
    fn open(dir: &Path, segment_bytes: u64) -> io::Result<Segments> {
        let mut found = Vec::new();
        for item in fs::read_dir(dir)? {
            let name = item?.file_name().to_string_lossy().into_owned();
            if let Some(first) = name
                .strip_prefix("log-")
                .and_then(|n| n.strip_suffix(".seg"))
            {
                let first: u64 = first.parse().map_err(invalid_data)?;
                found.push(first);
            }
        }
        found.sort_unstable();
        let mut segs = Vec::new();
        for first in found {
            let path = segment_path(dir, first);
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let len = file.metadata()?.len();
            segs.push(Segment {
                first,
                path,
                file,
                offsets: Vec::new(),
                len,
            });
        }
        Ok(Segments {
            dir: dir.to_path_buf(),
            segment_bytes,
            segs,
        })
    }

    /// Reads every record, fills in the offsets, and cuts a torn record off
    /// the end of the last segment.
    fn read_all(&mut self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let mut out = Vec::new();
        let count = self.segs.len();
        let mut expected_next: Option<u64> = None;
        for (i, seg) in self.segs.iter_mut().enumerate() {
            if let Some(next) = expected_next {
                if seg.first != next && seg.len > 0 {
                    return Err(invalid_data(format!(
                        "segment {} starts at {}, expected {next}",
                        seg.path.display(),
                        seg.first
                    )));
                }
            }
            let mut buf = Vec::with_capacity(seg.len as usize);
            io::Read::read_to_end(&mut &seg.file, &mut buf)?;
            let mut pos = 0usize;
            while let Some((payload, used)) = parse_record(&buf[pos..]) {
                out.push((seg.next_index(), payload.to_vec()));
                seg.offsets.push(pos as u64);
                pos += used;
            }
            if pos != buf.len() {
                if i + 1 != count {
                    return Err(invalid_data(format!(
                        "bad record at byte {pos} of {}, which is not the last segment",
                        seg.path.display()
                    )));
                }
                tracing::warn!(
                    "cutting {} bytes of a torn record off {}",
                    buf.len() - pos,
                    seg.path.display()
                );
                seg.file.set_len(pos as u64)?;
                seg.file.sync_all()?;
                seg.len = pos as u64;
            }
            expected_next = Some(seg.next_index());
        }
        Ok(out)
    }

    fn append(&mut self, records: &[(u64, Vec<u8>)]) -> io::Result<()> {
        let mut dir_changed = false;
        let mut to_sync: Vec<usize> = Vec::new();
        let mut buf = Vec::new();
        for (index, payload) in records {
            let need_new = match self.segs.last() {
                None => true,
                Some(seg) => {
                    if seg.next_index() != *index && !seg.offsets.is_empty() {
                        return Err(io::Error::other(format!(
                            "append of index {index}, but the log ends before {}",
                            seg.next_index()
                        )));
                    }
                    seg.offsets.is_empty() && seg.first != *index || seg.len >= self.segment_bytes
                }
            };
            if need_new {
                self.write_buffered(&mut buf, &mut to_sync)?;
                if let Some(seg) = self.segs.last() {
                    if seg.offsets.is_empty() {
                        fs::remove_file(&seg.path)?;
                        self.segs.pop();
                    }
                }
                let path = segment_path(&self.dir, *index);
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                self.segs.push(Segment {
                    first: *index,
                    path,
                    file,
                    offsets: Vec::new(),
                    len: 0,
                });
                dir_changed = true;
            }
            let seg = self.segs.last_mut().unwrap();
            seg.offsets.push(seg.len + buf.len() as u64);
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
            buf.extend_from_slice(payload);
        }
        self.write_buffered(&mut buf, &mut to_sync)?;
        for i in to_sync {
            self.segs[i].file.sync_data()?;
        }
        if dir_changed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Writes `buf` at the end of the last segment and remembers to sync it.
    fn write_buffered(&mut self, buf: &mut Vec<u8>, to_sync: &mut Vec<usize>) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let i = self.segs.len() - 1;
        let seg = &mut self.segs[i];
        seg.file.write_all_at(buf, seg.len)?;
        seg.len += buf.len() as u64;
        buf.clear();
        if !to_sync.contains(&i) {
            to_sync.push(i);
        }
        Ok(())
    }

    /// Removes every record at index `from` and after.
    fn truncate(&mut self, from: u64) -> io::Result<()> {
        let mut dir_changed = false;
        while let Some(seg) = self.segs.last_mut() {
            if seg.first >= from {
                fs::remove_file(&seg.path)?;
                self.segs.pop();
                dir_changed = true;
                continue;
            }
            if from < seg.next_index() {
                let keep = (from - seg.first) as usize;
                let len = seg.offsets[keep];
                seg.offsets.truncate(keep);
                seg.file.set_len(len)?;
                seg.file.sync_all()?;
                seg.len = len;
            }
            break;
        }
        if dir_changed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Deletes segments whose records are all at or below `upto`.
    fn drop_purged(&mut self, upto: Option<u64>) -> io::Result<()> {
        let Some(upto) = upto else { return Ok(()) };
        let mut dir_changed = false;
        while let Some(seg) = self.segs.first() {
            if seg.next_index() > upto + 1 {
                break;
            }
            fs::remove_file(&seg.path)?;
            self.segs.remove(0);
            dir_changed = true;
        }
        if dir_changed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }
}

/// Returns the payload and the number of bytes the record takes, or `None`
/// if `buf` does not start with a whole, intact record.
fn parse_record(buf: &[u8]) -> Option<(&[u8], usize)> {
    if buf.len() < RECORD_HEADER {
        return None;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let payload = buf.get(RECORD_HEADER..RECORD_HEADER + len)?;
    (crc32fast::hash(payload) == crc).then_some((payload, RECORD_HEADER + len))
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("log-{first:020}.seg"))
}

fn read_meta(dir: &Path) -> io::Result<Meta> {
    let bytes = match fs::read(dir.join(META_FILE)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Meta::default()),
        Err(e) => return Err(e),
    };
    let (payload, _) = parse_record(&bytes).ok_or_else(|| invalid_data("meta file is damaged"))?;
    bincode::deserialize(payload).map_err(invalid_data)
}

fn write_meta(dir: &Path, meta: &Meta) -> io::Result<()> {
    let payload = bincode::serialize(meta).map_err(invalid_data)?;
    let mut bytes = Vec::with_capacity(RECORD_HEADER + payload.len());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    let tmp = dir.join("meta.tmp");
    let file = File::create(&tmp)?;
    file.write_all_at(&bytes, 0)?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(META_FILE))?;
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn invalid_data(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}
