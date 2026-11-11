//! A [`StateMachine`] that only counts, for tests and benchmarks.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tursoraft_statemachine::{Applied, Entry, Outcome, Snapshot, StateMachine};

#[derive(Debug, Clone, Default)]
pub struct CountingOptions {
    /// Keep the state in this file, so a restarted node resumes from it. The
    /// file is replaced after every entry without an fsync, and with an fsync
    /// when a snapshot is built or installed. That survives a killed process,
    /// not a power loss. `None` keeps the state in memory only; such a node
    /// cannot restart once its log has been compacted.
    pub path: Option<PathBuf>,
    /// Keep every applied proposal with its log index, to compare nodes.
    pub record: bool,
    /// Reject proposals that start with these bytes.
    pub reject_prefix: Option<Vec<u8>>,
}

/// The replicated state, and what a snapshot carries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountingStats {
    pub applied: Option<u64>,
    /// Every applied entry, including blank and membership entries.
    pub entries: u64,
    /// Entries that carried at least one proposal.
    pub entries_with_proposals: u64,
    pub proposals: u64,
    pub rejected: u64,
    /// A hash over the index and proposals of every applied entry, in order.
    /// Two nodes at the same `applied` index with the same digest applied the
    /// same entries in the same order.
    pub digest: u64,
    /// `(index, proposal)` of every proposal that was not rejected, if
    /// [`CountingOptions::record`] is set.
    pub record: Option<Vec<(u64, Vec<u8>)>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    applied: Option<Applied>,
    stats: CountingStats,
}

/// Counts entries and proposals. Cloning gives another handle to the same
/// state, so a caller can read [`CountingStateMachine::stats`] after handing
/// the state machine to [`crate::Node::start`].
#[derive(Clone)]
pub struct CountingStateMachine {
    shared: Arc<Mutex<Shared>>,
}

struct Shared {
    opts: CountingOptions,
    state: State,
    snapshots_installed: u64,
}

const FNV_PRIME: u64 = 0x100_0000_01b3;

impl CountingStateMachine {
    pub fn new(opts: CountingOptions) -> io::Result<CountingStateMachine> {
        let mut state = match &opts.path {
            Some(path) => match fs::read(path) {
                Ok(bytes) => bincode::deserialize(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => State::default(),
                Err(e) => return Err(e),
            },
            None => State::default(),
        };
        if opts.record && state.stats.record.is_none() {
            state.stats.record = Some(Vec::new());
        }
        Ok(CountingStateMachine {
            shared: Arc::new(Mutex::new(Shared {
                opts,
                state,
                snapshots_installed: 0,
            })),
        })
    }

    pub fn stats(&self) -> CountingStats {
        self.shared.lock().unwrap().state.stats.clone()
    }

    /// How many snapshots this process installed.
    pub fn snapshots_installed(&self) -> u64 {
        self.shared.lock().unwrap().snapshots_installed
    }
}

impl Shared {
    fn save(&self, sync: bool) -> io::Result<()> {
        let Some(path) = &self.opts.path else {
            return Ok(());
        };
        let tmp = path.with_extension("tmp");
        let bytes = bincode::serialize(&self.state).map_err(io::Error::other)?;
        let file = fs::File::create(&tmp)?;
        io::Write::write_all(&mut &file, &bytes)?;
        if sync {
            file.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        if sync {
            if let Some(dir) = path.parent() {
                fs::File::open(dir)?.sync_all()?;
            }
        }
        Ok(())
    }
}

impl StateMachine for CountingStateMachine {
    type Error = io::Error;

    fn applied(&mut self) -> io::Result<Option<Applied>> {
        Ok(self.shared.lock().unwrap().state.applied.clone())
    }

    fn apply(&mut self, entry: &Entry) -> io::Result<Vec<Outcome>> {
        let mut sh = self.shared.lock().unwrap();
        if sh
            .state
            .applied
            .as_ref()
            .is_some_and(|a| a.index >= entry.index)
        {
            return Ok(vec![]);
        }
        let reject_prefix = sh.opts.reject_prefix.clone();
        let st = &mut sh.state.stats;
        let mut digest = (st.digest ^ entry.index).wrapping_mul(FNV_PRIME);
        let mut outcomes = Vec::with_capacity(entry.proposals.len());
        for p in &entry.proposals {
            for b in p {
                digest = (digest ^ u64::from(*b)).wrapping_mul(FNV_PRIME);
            }
            digest = (digest ^ 0xff).wrapping_mul(FNV_PRIME);
            st.proposals += 1;
            if reject_prefix.as_ref().is_some_and(|r| p.starts_with(r)) {
                st.rejected += 1;
                outcomes.push(Outcome::Rejected {
                    reason: "proposal starts with the reject prefix".into(),
                });
            } else {
                if let Some(record) = &mut st.record {
                    record.push((entry.index, p.clone()));
                }
                outcomes.push(Outcome::Applied);
            }
        }
        st.digest = digest;
        st.entries += 1;
        if !entry.proposals.is_empty() {
            st.entries_with_proposals += 1;
        }
        st.applied = Some(entry.index);
        sh.state.applied = Some(Applied {
            index: entry.index,
            raft_meta: entry.raft_meta.clone(),
        });
        sh.save(false)?;
        Ok(outcomes)
    }

    fn snapshot(&mut self) -> io::Result<Snapshot> {
        let sh = self.shared.lock().unwrap();
        let applied = sh
            .state
            .applied
            .clone()
            .ok_or_else(|| io::Error::other("no entry applied yet"))?;
        sh.save(true)?;
        let data = bincode::serialize(&sh.state.stats).map_err(io::Error::other)?;
        Ok(Snapshot { applied, data })
    }

    fn install_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        let mut sh = self.shared.lock().unwrap();
        let mut stats: CountingStats = bincode::deserialize(&snapshot.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if !sh.opts.record {
            stats.record = None;
        }
        sh.state = State {
            applied: Some(snapshot.applied.clone()),
            stats,
        };
        sh.snapshots_installed += 1;
        sh.save(true)
    }
}
