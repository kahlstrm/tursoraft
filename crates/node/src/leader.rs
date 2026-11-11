//! The leader loop: captures the waiting transactions as one group and
//! proposes their change sets as one log entry, one group at a time.
//!
//! A group is captured only after this node has applied every entry in its
//! log, so each capture reads the result of every earlier change set and the
//! commit-time check has nothing to reject. Transactions that arrive while a
//! group is being captured or committed wait and form the next group.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Weak;
use std::time::Instant;

use openraft::async_runtime::WatchReceiver;
use tokio::sync::{mpsc, oneshot};
use tursoraft_changeset::ChangeSet;
use tursoraft_raft::{Outcome, Proposed};

use crate::{CommitMode, Error, Inner, Rows, Statement, Written};

/// Most transactions in one group.
const MAX_GROUP: usize = 1024;

pub(crate) struct Job {
    pub statements: Vec<Statement>,
    pub mode: CommitMode,
    pub reply: oneshot::Sender<Result<Written, Error>>,
}

/// A captured transaction waiting for its entry.
struct Waiting {
    mode: CommitMode,
    reply: Option<oneshot::Sender<Result<Written, Error>>>,
    results: Vec<Rows>,
}

pub(crate) async fn run(node: Weak<Inner>, mut rx: mpsc::UnboundedReceiver<Job>) {
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        while jobs.len() < MAX_GROUP {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        let Some(node) = node.upgrade() else { return };
        run_group(&node, jobs).await;
    }
}

async fn run_group(node: &Inner, jobs: Vec<Job>) {
    let raft = &node.raft;
    let me = raft.id();
    let fail_all = |jobs: Vec<Job>, e: Error| {
        for job in jobs {
            let _ = job.reply.send(Err(e.clone()));
        }
    };
    let metrics = raft.raft().metrics().borrow_watched().clone();
    if metrics.current_leader != Some(me) {
        return fail_all(
            jobs,
            Error::Unavailable(format!(
                "node {me} is not the leader; the leader is {:?}",
                metrics.current_leader
            )),
        );
    }
    // A new leader's log ends with entries of earlier leaders and its own
    // blank entry; once those are applied, capture sees all of them.
    let last = metrics.last_log_index.unwrap_or(0);
    if let Err(e) = raft.wait_applied(last, node.timeout).await {
        return fail_all(jobs, Error::Unavailable(e.to_string()));
    }

    let transactions: Vec<_> = jobs
        .iter()
        .map(|j| j.statements.iter().map(Statement::to_changeset).collect())
        .collect();
    let db = node.db.clone();
    let started = Instant::now();
    let captured = tokio::task::spawn_blocking(move || db.capture_batch(&transactions))
        .await
        .expect("capture does not panic");
    node.counters
        .capture_micros
        .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
    node.counters
        .captured_transactions
        .fetch_add(jobs.len() as u64, Relaxed);
    let captured = match captured {
        Ok(c) => c,
        Err(e) => return fail_all(jobs, Error::Unavailable(format!("capture failed: {e}"))),
    };

    let mut proposals = Vec::new();
    let mut waiting = Vec::new();
    for (job, captured) in jobs.into_iter().zip(captured) {
        let captured = match captured {
            Ok(c) => c,
            Err(e) => {
                node.counters.failed.fetch_add(1, Relaxed);
                let _ = job.reply.send(Err(Error::Transaction(e.to_string())));
                continue;
            }
        };
        let results = captured
            .results
            .into_iter()
            .map(Rows::from_changeset)
            .collect();
        let empty = ChangeSet::decode(&captured.change_set)
            .expect("capture encodes a valid change set")
            .changes
            .is_empty();
        if empty {
            // Nothing to replicate; its reads saw everything up to `last`.
            let _ = job.reply.send(Ok(Written {
                index: last,
                results,
            }));
            continue;
        }
        proposals.push(captured.change_set);
        waiting.push(Waiting {
            mode: job.mode,
            reply: Some(job.reply),
            results,
        });
    }
    if proposals.is_empty() {
        return;
    }
    node.counters.groups.fetch_add(1, Relaxed);
    node.counters
        .transactions
        .fetch_add(proposals.len() as u64, Relaxed);

    // This loop is the only proposer of normal entries on this node, and
    // everything in the log is applied, so the entry goes right after the
    // current last one unless a membership change comes between.
    let metrics = raft.raft().metrics().borrow_watched().clone();
    let term = metrics.current_term;
    let expected = metrics.last_log_index.unwrap_or(0) + 1;
    let any_async = waiting.iter().any(|w| w.mode == CommitMode::Async);

    let propose = raft.propose_many(proposals);
    tokio::pin!(propose);
    let flushed = flushed(raft.raft(), me, term, expected);
    tokio::pin!(flushed);
    let mut acked_early = false;
    let result = loop {
        tokio::select! {
            result = &mut propose => break result,
            done = &mut flushed, if any_async && !acked_early => {
                acked_early = true;
                if done {
                    ack_async(node, &mut waiting, expected);
                }
            }
        }
    };
    finish(node, waiting, result, expected);
}

/// Waits until this node, as leader in `term`, has flushed its log up to
/// `index`. Returns false if the progress channel closes.
async fn flushed(raft: &tursoraft_raft::Raft, me: u64, term: u64, index: u64) -> bool {
    let mut progress = raft.watch_log_progress();
    progress
        .wait_until(|point| {
            point
                .as_ref()
                .and_then(|p| p.last_log_id.as_ref())
                .is_some_and(|log_id| {
                    log_id.index >= index
                        && log_id.leader_id.term == term
                        && log_id.leader_id.node_id == me
                })
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

fn finish(
    node: &Inner,
    waiting: Vec<Waiting>,
    result: Result<Vec<Proposed>, tursoraft_raft::Error>,
    expected: u64,
) {
    use tursoraft_raft::Error as R;
    let proposed = match result {
        Ok(p) => p,
        Err(e) => {
            let error = match e {
                R::NotLeader { .. } | R::Unreachable(_) => Error::Unavailable(e.to_string()),
                e => Error::OutcomeUnknown(e.to_string()),
            };
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
            return;
        }
    };
    for (w, p) in waiting.into_iter().zip(proposed) {
        let result = match p.outcome {
            Outcome::Applied => Ok(Written {
                index: p.index,
                results: w.results,
            }),
            Outcome::Rejected { reason } => {
                node.counters.rejected.fetch_add(1, Relaxed);
                Err(Error::Rejected(reason))
            }
        };
        match w.reply {
            Some(reply) => {
                let _ = reply.send(result);
            }
            None => {
                if p.index != expected {
                    node.counters.async_index_mismatches.fetch_add(1, Relaxed);
                    tracing::warn!(
                        "an async write was acknowledged with index {expected} but is at {}",
                        p.index
                    );
                }
                if let Err(e) = result {
                    node.counters.async_acked_then_failed.fetch_add(1, Relaxed);
                    tracing::warn!("an acknowledged async write was rejected: {e}");
                }
            }
        }
    }
}
