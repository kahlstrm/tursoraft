//! Counters of one node, and the state machine wrapper that times applying.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tursoraft_changeset::TursoStateMachine;
use tursoraft_statemachine::{Applied, Entry, Outcome, Snapshot, StateMachine};

/// What one node has done since it started.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub id: u64,
    /// The last log index whose effect is in the database file.
    pub applied_index: u64,
    /// The setting: most groups pending at once; 1 is no pipelining.
    pub max_pending_groups: u64,
    /// Leader loop: the most groups that were pending at once.
    pub max_groups_pending: u64,
    /// Leader loop: copies of the database file to the ahead file, and the
    /// time they took.
    pub resets: u64,
    pub reset_micros: u64,
    /// Leader loop: groups whose log index was not known when they were
    /// handed to Raft, because another entry was appended at the same time.
    /// Their async transactions are acknowledged when applied.
    pub unknown_indexes: u64,
    /// Leader loop: log entries proposed, one per group.
    pub groups: u64,
    /// Leader loop: transactions proposed.
    pub transactions: u64,
    /// Leader loop: transactions that failed at capture.
    pub failed: u64,
    /// Leader loop: transactions whose change set the commit-time check rejected.
    pub rejected: u64,
    /// Leader loop: transactions captured, failed ones included, and the
    /// time spent capturing them.
    pub captured_transactions: u64,
    pub capture_micros: u64,
    /// Async writes acknowledged before their entry was applied.
    pub async_acked_early: u64,
    /// Async writes acknowledged with a log index other than their entry's.
    pub async_index_mismatches: u64,
    /// Async writes acknowledged and then rejected or lost.
    pub async_acked_then_failed: u64,
    /// State machine: entries applied (blank and membership entries
    /// included), change sets in them, and the time spent applying.
    pub applied_entries: u64,
    pub applied_change_sets: u64,
    pub apply_micros: u64,
    pub snapshots_built: u64,
    pub snapshots_installed: u64,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub groups: AtomicU64,
    pub transactions: AtomicU64,
    pub failed: AtomicU64,
    pub rejected: AtomicU64,
    pub captured_transactions: AtomicU64,
    pub capture_micros: AtomicU64,
    pub async_acked_early: AtomicU64,
    pub async_index_mismatches: AtomicU64,
    pub async_acked_then_failed: AtomicU64,
    pub max_groups_pending: AtomicU64,
    pub resets: AtomicU64,
    pub reset_micros: AtomicU64,
    pub unknown_indexes: AtomicU64,
    applied_entries: AtomicU64,
    applied_change_sets: AtomicU64,
    apply_micros: AtomicU64,
    snapshots_built: AtomicU64,
    snapshots_installed: AtomicU64,
}

impl Counters {
    pub(crate) fn stats(&self, id: u64, applied_index: u64, max_pending_groups: usize) -> Stats {
        let get = |c: &AtomicU64| c.load(Relaxed);
        Stats {
            id,
            applied_index,
            max_pending_groups: max_pending_groups as u64,
            max_groups_pending: get(&self.max_groups_pending),
            resets: get(&self.resets),
            reset_micros: get(&self.reset_micros),
            unknown_indexes: get(&self.unknown_indexes),
            groups: get(&self.groups),
            transactions: get(&self.transactions),
            failed: get(&self.failed),
            rejected: get(&self.rejected),
            captured_transactions: get(&self.captured_transactions),
            capture_micros: get(&self.capture_micros),
            async_acked_early: get(&self.async_acked_early),
            async_index_mismatches: get(&self.async_index_mismatches),
            async_acked_then_failed: get(&self.async_acked_then_failed),
            applied_entries: get(&self.applied_entries),
            applied_change_sets: get(&self.applied_change_sets),
            apply_micros: get(&self.apply_micros),
            snapshots_built: get(&self.snapshots_built),
            snapshots_installed: get(&self.snapshots_installed),
        }
    }
}

/// [`TursoStateMachine`] with counters.
pub(crate) struct MeasuredStateMachine {
    inner: TursoStateMachine,
    counters: Arc<Counters>,
}

impl MeasuredStateMachine {
    pub(crate) fn new(inner: TursoStateMachine, counters: Arc<Counters>) -> Self {
        MeasuredStateMachine { inner, counters }
    }
}

impl StateMachine for MeasuredStateMachine {
    type Error = tursoraft_changeset::Error;

    fn applied(&mut self) -> Result<Option<Applied>, Self::Error> {
        self.inner.applied()
    }

    fn apply(&mut self, entry: &Entry) -> Result<Vec<Outcome>, Self::Error> {
        let started = Instant::now();
        let outcomes = self.inner.apply(entry)?;
        let c = &self.counters;
        c.apply_micros
            .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
        c.applied_entries.fetch_add(1, Relaxed);
        c.applied_change_sets
            .fetch_add(entry.proposals.len() as u64, Relaxed);
        Ok(outcomes)
    }

    fn snapshot(&mut self) -> Result<Snapshot, Self::Error> {
        let snapshot = self.inner.snapshot()?;
        self.counters.snapshots_built.fetch_add(1, Relaxed);
        Ok(snapshot)
    }

    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.inner.install_snapshot(snapshot)?;
        self.counters.snapshots_installed.fetch_add(1, Relaxed);
        Ok(())
    }
}
