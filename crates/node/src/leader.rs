//! The leader loop: captures the waiting transactions as one group and
//! proposes their change sets as one log entry.
//!
//! Without pipelining (`max_pending_groups` 1) a group is captured on the
//! node's own file, and only after this node has applied every entry in its
//! log, so each capture reads the result of every earlier change set.
//!
//! With pipelining, up to `max_pending_groups` groups wait for their entries
//! at once. Groups are captured on the [`AheadDb`], which keeps the effect of
//! every group captured since its last reset, so each group reads the ones
//! before it while they are still on their way through Raft. Each group is
//! handed to Raft before the next is captured, and only if this node still
//! leads in the term the ahead file was reset in, so the groups enter the log
//! in capture order. A group whose entry fails, or whose change sets are not
//! all applied, makes every later group fail too: those are lost with it
//! (the log keeps no entry without the ones before it) or rejected (the
//! change sets read pending ones; see `tursoraft_changeset::apply`). The loop
//! then waits until every pending group is decided, and resets the ahead file
//! from the node's file.
//!
//! Before it captures a group, the pipelined loop waits until this node has
//! applied the entries already committed; see [`wait_for_apply`].
//!
//! Change sets that other nodes captured in local transactions
//! ([`Work::ChangeSet`]) join the next group. With pipelining, the loop
//! first checks them on the ahead file, as apply would, and keeps the effect
//! of each that passes there, so the captures after it see it. One that
//! fails there is answered as rejected and never proposed. They read
//! committed state only, so they need no `reads_pending`: apply checks their
//! rows against the node's file, which holds exactly that state plus what
//! came after. They go into the entry before the group's captured change
//! sets, in the order they were put into the ahead file. Without
//! pipelining they are not checked by the loop and go after the captured
//! ones, which were captured without them.
//!
//! The node's own file changes only by applying committed entries, in both
//! modes.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Instant;

use openraft::alias::CommittedLeaderIdOf;
use openraft::async_runtime::WatchReceiver;
use openraft::error::ClientWriteError;
use openraft::impls::ProgressResponder;
use openraft::raft::ClientWriteResult;
use openraft::ReadPolicy;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tursoraft_changeset::{AheadDb, Captured, ChangeSet};
use tursoraft_raft::{Outcome, Proposals, TypeConfig};

use crate::{CommitMode, Error, Inner, Rows, Statement, Written};

/// Most transactions in one group.
const MAX_GROUP: usize = 1024;

type LeaderId = CommittedLeaderIdOf<TypeConfig>;

pub(crate) struct Job {
    pub work: Work,
    pub mode: CommitMode,
    pub reply: oneshot::Sender<Result<Written, Error>>,
}

/// What a transaction brings to the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Work {
    /// Statements for the leader to run.
    Statements(Vec<Statement>),
    /// The change set of a local transaction that ran on another node.
    ChangeSet(Vec<u8>),
}

/// A captured transaction waiting for its entry.
struct Waiting {
    mode: CommitMode,
    reply: Option<oneshot::Sender<Result<Written, Error>>>,
    results: Vec<Rows>,
    /// The change set came from a local transaction on some node.
    incoming: bool,
}

/// A transaction that changed nothing.
struct ReadOnly {
    mode: CommitMode,
    reply: oneshot::Sender<Result<Written, Error>>,
    results: Vec<Rows>,
}

/// A group handed to Raft: its log index, if known, and whether all its
/// change sets were applied (`None` while undecided).
#[derive(Clone)]
struct Group {
    index: Option<u64>,
    applied: watch::Receiver<Option<bool>>,
}

struct Pipeline {
    max_pending: usize,
    /// One permit per group that may be pending.
    permits: Arc<Semaphore>,
    /// `None` without pipelining.
    ahead: Option<AheadDb>,
    /// The leadership the ahead file was reset in; `None` until a reset.
    reset_in: Option<LeaderId>,
    /// The index the ahead file was reset at.
    reset_index: u64,
    /// Set when a pending group failed; the ahead file then needs a reset.
    failed: Arc<AtomicBool>,
    /// The last group handed to Raft since the reset.
    last: Option<Group>,
}

pub(crate) async fn run(
    node: Weak<Inner>,
    mut rx: mpsc::UnboundedReceiver<Job>,
    ahead: Option<AheadDb>,
    max_pending: usize,
) {
    let mut p = Pipeline {
        max_pending,
        permits: Arc::new(Semaphore::new(max_pending)),
        ahead,
        reset_in: None,
        reset_index: 0,
        failed: Arc::new(AtomicBool::new(false)),
        last: None,
    };
    loop {
        // Transactions that arrive while all permits are taken form the next
        // group.
        let permit = p
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");
        let Some(first) = rx.recv().await else { return };
        if p.ahead.is_some() {
            let Some(node) = node.upgrade() else { return };
            wait_for_apply(&node).await;
        }
        let mut jobs = vec![first];
        while jobs.len() < MAX_GROUP {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        let Some(node) = node.upgrade() else { return };
        p.run_group(&node, jobs, permit).await;
    }
}

/// Waits until this node has applied the entries committed so far. Each
/// entry has a fixed cost to apply, so when applying, not the network, holds
/// the groups back, the transactions that arrive meanwhile form one larger
/// group instead of one entry each. While the entries wait for a majority,
/// as at a large round trip, this returns at once.
async fn wait_for_apply(node: &Inner) {
    let committed = node.raft.raft().metrics().borrow_watched().local_committed;
    if let Some(committed) = committed {
        // On timeout, capture anyway; the leader check comes next.
        let _ = node.raft.wait_applied(committed.index, node.timeout).await;
    }
}

impl Pipeline {
    async fn run_group(&mut self, node: &Arc<Inner>, jobs: Vec<Job>, permit: OwnedSemaphorePermit) {
        let raft = &node.raft;
        let me = raft.id();
        let fail_all = |jobs: Vec<Job>, e: Error| {
            for job in jobs {
                let _ = job.reply.send(Err(e.clone()));
            }
        };
        let Some(leader) = self.leadership(node) else {
            let e = Error::Unavailable(format!(
                "node {me} is not the leader; the leader is {:?}",
                raft.leader()
            ));
            return fail_all(jobs, e);
        };
        let stale = self.ahead.is_none()
            || self.reset_in.as_ref() != Some(&leader)
            || self.failed.load(Relaxed);
        if stale {
            if let Err(e) = self.catch_up(node, &leader).await {
                return fail_all(jobs, e);
            }
        }

        let (incoming, jobs): (Vec<Job>, Vec<Job>) = jobs
            .into_iter()
            .partition(|j| matches!(j.work, Work::ChangeSet(_)));
        let mut proposals = Vec::new();
        let mut waiting = Vec::new();
        let unchecked = self
            .check_incoming(node, incoming, &mut proposals, &mut waiting)
            .await;
        let captured = self.capture(node, jobs).await;

        let mut read_only = Vec::new();
        for (job, captured) in captured {
            let Captured {
                results,
                change_set,
            } = match captured {
                Ok(c) => c,
                Err(e @ tursoraft_changeset::Error::Undone { .. }) => {
                    let _ = job.reply.send(Err(Error::Unavailable(e.to_string())));
                    continue;
                }
                Err(e) => {
                    node.counters.failed.fetch_add(1, Relaxed);
                    let _ = job.reply.send(Err(Error::Transaction(e.to_string())));
                    continue;
                }
            };
            let results: Vec<Rows> = results.into_iter().map(Rows::from_changeset).collect();
            let empty = ChangeSet::decode(&change_set)
                .expect("capture encodes a valid change set")
                .changes
                .is_empty();
            if empty {
                read_only.push(ReadOnly {
                    mode: job.mode,
                    reply: job.reply,
                    results,
                });
                continue;
            }
            proposals.push(change_set);
            waiting.push(Waiting {
                mode: job.mode,
                reply: Some(job.reply),
                results,
                incoming: false,
            });
        }
        for (change_set, w) in unchecked {
            proposals.push(change_set);
            waiting.push(w);
        }

        if !proposals.is_empty() {
            match self
                .propose(node, &leader, proposals, waiting, permit)
                .await
            {
                Ok(group) => self.last = Some(group),
                Err(()) => {
                    // Nothing entered the log; the transactions that read
                    // this group's effects get no answer they could rely on.
                    for r in read_only {
                        let _ = r.reply.send(Err(Error::Unavailable(
                            "the leader could not propose the group it read".into(),
                        )));
                    }
                    return;
                }
            }
        }
        // A transaction that changed nothing read the groups up to the last
        // one handed to Raft (in this group, it may have read earlier
        // transactions of the same group).
        let seen = self.last.clone();
        let index = seen
            .as_ref()
            .and_then(|g| g.index)
            .unwrap_or(self.reset_index);
        for r in read_only {
            match r.mode {
                CommitMode::Async => {
                    let _ = r.reply.send(Ok(Written {
                        index,
                        results: r.results,
                    }));
                }
                CommitMode::Quorum => {
                    tokio::spawn(confirm_read_only(
                        node.clone(),
                        leader,
                        seen.clone(),
                        index,
                        r,
                    ));
                }
            }
        }
    }

    /// With pipelining, applies the incoming change sets to the ahead file,
    /// adds those that pass to `proposals` and `waiting`, and answers the
    /// others as rejected. Without, returns them all unchecked.
    async fn check_incoming(
        &self,
        node: &Inner,
        incoming: Vec<Job>,
        proposals: &mut Vec<Vec<u8>>,
        waiting: &mut Vec<Waiting>,
    ) -> Vec<(Vec<u8>, Waiting)> {
        let mut jobs = Vec::with_capacity(incoming.len());
        for job in incoming {
            let Work::ChangeSet(change_set) = job.work else {
                unreachable!("only change sets come in")
            };
            let w = Waiting {
                mode: job.mode,
                reply: Some(job.reply),
                results: Vec::new(),
                incoming: true,
            };
            jobs.push((change_set, w));
        }
        node.counters.incoming.fetch_add(jobs.len() as u64, Relaxed);
        let Some(ahead) = self.ahead.clone() else {
            return jobs;
        };
        if jobs.is_empty() {
            return jobs;
        }
        let change_sets: Vec<Vec<u8>> = jobs.iter().map(|(c, _)| c.clone()).collect();
        let started = Instant::now();
        let outcomes = tokio::task::spawn_blocking(move || ahead.apply_incoming(&change_sets))
            .await
            .expect("apply does not panic");
        node.counters
            .incoming_micros
            .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
        let outcomes = match outcomes {
            Ok(o) => o,
            Err(e) => {
                // The ahead file is as it was; nothing was proposed.
                for (_, w) in jobs {
                    let reply = w.reply.expect("not answered yet");
                    let _ = reply.send(Err(Error::Unavailable(format!("check failed: {e}"))));
                }
                return Vec::new();
            }
        };
        for ((change_set, w), outcome) in jobs.into_iter().zip(outcomes) {
            match outcome {
                Outcome::Applied => {
                    proposals.push(change_set);
                    waiting.push(w);
                }
                Outcome::Rejected { reason } => {
                    node.counters.incoming_rejected_early.fetch_add(1, Relaxed);
                    let reply = w.reply.expect("not answered yet");
                    let _ = reply.send(Err(Error::Rejected(reason)));
                }
            }
        }
        Vec::new()
    }

    /// Captures the statement jobs on the ahead file, or on the node's file
    /// without pipelining. A batch that fails as a whole fails its jobs and
    /// leaves the ahead file as it was.
    async fn capture(
        &self,
        node: &Inner,
        jobs: Vec<Job>,
    ) -> Vec<(Job, Result<Captured, tursoraft_changeset::Error>)> {
        if jobs.is_empty() {
            return Vec::new();
        }
        let transactions: Vec<Vec<_>> = jobs
            .iter()
            .map(|j| match &j.work {
                Work::Statements(s) => s.iter().map(Statement::to_changeset).collect(),
                Work::ChangeSet(_) => unreachable!("only statements are captured"),
            })
            .collect();
        let main = node.db.clone();
        let ahead = self.ahead.clone();
        let started = Instant::now();
        let captured = tokio::task::spawn_blocking(move || match ahead {
            Some(ahead) => ahead.capture_batch(&transactions),
            None => main.capture_batch(&transactions),
        })
        .await
        .expect("capture does not panic");
        node.counters
            .capture_micros
            .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
        node.counters
            .captured_transactions
            .fetch_add(jobs.len() as u64, Relaxed);
        match captured {
            Ok(c) => jobs.into_iter().zip(c).collect(),
            Err(e) => {
                for job in jobs {
                    let e = Error::Unavailable(format!("capture failed: {e}"));
                    let _ = job.reply.send(Err(e));
                }
                Vec::new()
            }
        }
    }

    /// This node's leadership, if it leads.
    fn leadership(&self, node: &Inner) -> Option<LeaderId> {
        let leader = node.raft.raft().as_leader().ok()?;
        Some(leader.to_committed_leader_id())
    }

    /// Waits until no group is pending and this node has applied its whole
    /// log, a new leader's blank entry included, then resets the ahead file
    /// from the node's file. Without pipelining only waits.
    async fn catch_up(&mut self, node: &Inner, leader: &LeaderId) -> Result<(), Error> {
        let raft = &node.raft;
        // This group's permit is taken; take the others.
        let all = self
            .permits
            .clone()
            .acquire_many_owned(self.max_pending as u32 - 1)
            .await
            .expect("never closed");
        let metrics = raft.raft().metrics().borrow_watched().clone();
        let last = metrics.last_log_index.unwrap_or(0);
        raft.wait_applied(last, node.timeout)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        self.last = None;
        self.reset_index = last;
        let Some(ahead) = self.ahead.clone() else {
            return Ok(());
        };
        let main = node.db.clone();
        let started = Instant::now();
        let reset = tokio::task::spawn_blocking(move || ahead.reset(&main))
            .await
            .expect("reset does not panic")
            .map_err(|e| Error::Unavailable(format!("cannot copy the database: {e}")))?;
        node.counters.resets.fetch_add(1, Relaxed);
        node.counters
            .reset_micros
            .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
        self.reset_index = reset;
        self.reset_in = Some(*leader);
        self.failed.store(false, Relaxed);
        drop(all);
        Ok(())
    }

    /// Hands one entry to Raft and starts the task that answers its
    /// transactions. Returns `Err` if the entry did not enter the log.
    async fn propose(
        &mut self,
        node: &Arc<Inner>,
        leader: &LeaderId,
        proposals: Vec<Vec<u8>>,
        waiting: Vec<Waiting>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Group, ()> {
        let raft = node.raft.raft();
        let fail = |waiting: Vec<Waiting>, e: Error| {
            for w in waiting {
                let _ = w.reply.expect("not answered yet").send(Err(e.clone()));
            }
        };
        let last_log_id = || raft.with_raft_state(|st| st.log_ids.last().cloned());
        let (responder, mut done) = ProgressResponder::complete_only();
        // Openraft handles these three requests in order, so the entries
        // appended between the two reads are this one and any appended by
        // other callers in between, such as a membership change.
        let before = last_log_id().await;
        let sent = raft
            .write(Proposals(proposals.clone()))
            .with_leader(*leader)
            .responder(responder)
            .await;
        let after = last_log_id().await;
        let (Ok(before), Ok(()), Ok(after)) = (before, sent, after) else {
            self.failed.store(true, Relaxed);
            fail(waiting, Error::Unavailable("Raft has stopped".into()));
            return Err(());
        };
        // A refused write is answered while openraft handles it.
        if let Ok(Err(e)) = done.try_recv() {
            self.failed.store(true, Relaxed);
            fail(waiting, Error::Unavailable(format!("not proposed: {e}")));
            return Err(());
        }
        let index = after.filter(|after| {
            after.leader_id == *leader && after.index == before.as_ref().map_or(0, |b| b.index) + 1
        });
        let index = index.map(|l| l.index);
        if index.is_none() {
            node.counters.unknown_indexes.fetch_add(1, Relaxed);
        }

        node.counters.groups.fetch_add(1, Relaxed);
        node.counters
            .transactions
            .fetch_add(waiting.len() as u64, Relaxed);
        let in_flight = self.max_pending - self.permits.available_permits();
        node.counters
            .max_groups_pending
            .fetch_max(in_flight as u64, Relaxed);
        let (applied_tx, applied) = watch::channel(None);
        tokio::spawn(complete(
            node.clone(),
            *leader,
            index,
            waiting,
            done,
            applied_tx,
            self.failed.clone(),
            permit,
        ));
        Ok(Group { index, applied })
    }
}

/// Waits for one group's entry: acknowledges its async transactions once
/// this node has flushed the entry, and answers everything once the entry is
/// applied here or fails.
#[allow(clippy::too_many_arguments)]
async fn complete(
    node: Arc<Inner>,
    leader: LeaderId,
    index: Option<u64>,
    mut waiting: Vec<Waiting>,
    done: oneshot::Receiver<ClientWriteResult<TypeConfig>>,
    applied_tx: watch::Sender<Option<bool>>,
    failed: Arc<AtomicBool>,
    permit: OwnedSemaphorePermit,
) {
    let any_async = waiting.iter().any(|w| w.mode == CommitMode::Async);
    let flushed = async {
        match index {
            Some(index) if any_async => flushed(node.raft.raft(), &leader, index).await,
            _ => std::future::pending().await,
        }
    };
    tokio::pin!(done);
    tokio::pin!(flushed);
    let mut acked_early = false;
    let result = loop {
        tokio::select! {
            result = &mut done => break result,
            ok = &mut flushed, if !acked_early => {
                acked_early = true;
                if ok {
                    ack_async(&node, &mut waiting, index.expect("known"));
                }
            }
        }
    };
    let all_applied = finish(&node, waiting, result, index);
    if !all_applied {
        failed.store(true, Relaxed);
    }
    let _ = applied_tx.send(Some(all_applied));
    drop(permit);
}

/// Waits until this node, as leader `leader`, has flushed its log up to
/// `index`. Returns false if the progress channel closes.
async fn flushed(raft: &tursoraft_raft::Raft, leader: &LeaderId, index: u64) -> bool {
    let mut progress = raft.watch_log_progress();
    progress
        .wait_until(|point| {
            point
                .as_ref()
                .and_then(|p| p.last_log_id.as_ref())
                .is_some_and(|log_id| log_id.index >= index && log_id.leader_id == *leader)
        })
        .await
        .is_ok()
}

fn ack_async(node: &Inner, waiting: &mut [Waiting], index: u64) {
    for w in waiting.iter_mut().filter(|w| w.mode == CommitMode::Async) {
        let reply = w.reply.take().expect("not answered yet");
        node.counters.async_acked_early.fetch_add(1, Relaxed);
        let _ = reply.send(Ok(Written {
            index,
            results: w.results.clone(),
        }));
    }
}

/// Answers the transactions of one entry. Returns whether all its change
/// sets were applied.
fn finish(
    node: &Inner,
    waiting: Vec<Waiting>,
    result: Result<ClientWriteResult<TypeConfig>, oneshot::error::RecvError>,
    acked_index: Option<u64>,
) -> bool {
    let response = match result {
        Ok(Ok(response)) if response.data.len() == waiting.len() => Ok(response),
        Ok(Ok(response)) => Err(Error::Internal(format!(
            "the state machine returned {} outcomes for {} proposals at index {}",
            response.data.len(),
            waiting.len(),
            response.log_id.index
        ))),
        Ok(Err(ClientWriteError::ForwardToLeader(e))) => {
            Err(Error::Unavailable(format!("not proposed: {e}")))
        }
        Ok(Err(e)) => Err(Error::OutcomeUnknown(e.to_string())),
        Err(_) => Err(Error::OutcomeUnknown("Raft stopped".into())),
    };
    let response = match response {
        Ok(r) => r,
        Err(error) => {
            for w in waiting {
                match w.reply {
                    Some(reply) => {
                        let _ = reply.send(Err(error.clone()));
                    }
                    None => {
                        node.counters.async_acked_then_failed.fetch_add(1, Relaxed);
                        tracing::warn!("an acknowledged async write failed: {error}");
                    }
                }
            }
            return false;
        }
    };
    let index = response.log_id.index;
    let mut all_applied = true;
    for (w, outcome) in waiting.into_iter().zip(response.data) {
        let result = match outcome {
            Outcome::Applied => Ok(Written {
                index,
                results: w.results,
            }),
            Outcome::Rejected { reason } => {
                all_applied = false;
                node.counters.rejected.fetch_add(1, Relaxed);
                if w.incoming {
                    node.counters.incoming_rejected_late.fetch_add(1, Relaxed);
                }
                Err(Error::Rejected(reason))
            }
        };
        match w.reply {
            Some(reply) => {
                let _ = reply.send(result);
            }
            None => {
                if acked_index != Some(index) {
                    node.counters.async_index_mismatches.fetch_add(1, Relaxed);
                    tracing::warn!(
                        "an async write was acknowledged with index {acked_index:?} but is at {index}"
                    );
                }
                if let Err(e) = result {
                    node.counters.async_acked_then_failed.fetch_add(1, Relaxed);
                    tracing::warn!("an acknowledged async write was rejected: {e}");
                }
            }
        }
    }
    all_applied
}

/// Answers a quorum transaction that changed nothing: once a majority has
/// confirmed that this node still leads, as for a linearizable read, and the
/// groups it read are applied. If one of those was not, it read a state that
/// never existed, and fails.
async fn confirm_read_only(
    node: Arc<Inner>,
    leader: LeaderId,
    seen: Option<Group>,
    index: u64,
    r: ReadOnly,
) {
    let confirm = async {
        match node
            .raft
            .raft()
            .get_read_log_id(ReadPolicy::ReadIndex)
            .await
        {
            Ok((read, _)) if *read.committed_leader_id() == leader => Ok(()),
            Ok(_) => Err(Error::Unavailable(
                "the leadership changed after the transaction ran".into(),
            )),
            Err(e) => Err(Error::Unavailable(format!(
                "cannot confirm the leadership: {e}"
            ))),
        }
    };
    let applied = async {
        let Some(mut group) = seen else {
            return Ok(());
        };
        match group.applied.wait_for(Option::is_some).await.map(|v| *v) {
            Ok(Some(true)) => Ok(()),
            _ => Err(Error::Rejected(
                "it read writes that were rejected or lost; it had no effect".into(),
            )),
        }
    };
    let result = match tokio::join!(confirm, applied) {
        (_, Err(e)) | (Err(e), _) => Err(e),
        (Ok(()), Ok(())) => Ok(Written {
            index,
            results: r.results,
        }),
    };
    let _ = r.reply.send(result);
}
