//! Raft layer on openraft 0.10. Orders opaque proposals and applies them
//! through [`tursoraft_statemachine::StateMachine`]. Knows nothing about Turso.
//!
//! A [`Node`] holds one Raft member: its durable log ([`log_store`]), the
//! adaptor to the application's [`StateMachine`], and a TCP endpoint that
//! serves other nodes and clients. Proposals made on any node reach the
//! leader: a follower forwards them. With [`Batching`] on, the leader puts the
//! proposals of concurrent callers into one log entry; [`Node::propose_many`]
//! keeps a caller's group of proposals together and in order.
//!
//! The network layer adds a configured round-trip time per pair of nodes
//! ([`DelayTable`]), so a cluster on one machine behaves like one spread over
//! several places.

mod client;
mod config;
mod counting;
pub mod log_store;
mod network;
mod node;
mod state_machine;
#[cfg(test)]
mod storage_tests;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use client::Client;
pub use config::{
    Batching, ClusterConfig, DelayTable, LogConfig, NodeConfig, PairRtt, RaftTimeouts,
};
pub use counting::{CountingOptions, CountingStateMachine, CountingStats};
pub use node::{AppFuture, AppHandler, Node};
pub use state_machine::StateMachineAdaptor;
pub use tursoraft_statemachine::{Applied, Entry, Outcome, Snapshot, StateMachine};

openraft::declare_raft_types!(
    /// openraft type configuration. The application data of one log entry is
    /// a list of proposals (encoded change sets), opaque to this crate; the
    /// response has one [`Outcome`] per proposal.
    pub TypeConfig:
        D = Proposals,
        R = Vec<Outcome>,
);

/// The application data of one log entry: the proposals it orders, opaque
/// to this crate. A newtype because openraft 0.10 needs `Display` on it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Proposals(pub Vec<Vec<u8>>);

impl std::fmt::Display for Proposals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes: usize = self.0.iter().map(Vec::len).sum();
        write!(f, "{} proposals, {bytes} bytes", self.0.len())
    }
}

/// The Raft handle for this configuration.
pub type Raft = openraft::Raft<TypeConfig, StateMachineAdaptor>;
pub type NodeId = u64;
pub(crate) type LogEntry = openraft::alias::EntryOf<TypeConfig>;
pub(crate) type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub(crate) type Vote = openraft::alias::VoteOf<TypeConfig>;

/// The result of one proposal: the log index of the entry that holds it and
/// what the state machine did with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposed {
    pub index: u64,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Error {
    /// This node is not the leader and no leader could be reached in time.
    #[error("not the leader; leader is {leader:?}")]
    NotLeader { leader: Option<NodeId> },
    /// The request was never sent: the node could not be reached.
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// The request was sent but no answer came back, for example because the
    /// connection or the leadership was lost. A proposal may or may not take
    /// effect.
    #[error("outcome unknown: {0}")]
    OutcomeUnknown(String),
    #[error("timed out: {0}")]
    Timeout(String),
    /// An earlier membership change is not committed yet; this one had no
    /// effect. [`Node`] retries it until the proposal timeout.
    #[error("another membership change is in progress: {0}")]
    MembershipChangeInProgress(String),
    /// The application handler of the receiving node returned this error.
    #[error("application error: {0}")]
    App(String),
    /// Any other failure from openraft, the storage, or the state machine.
    #[error("{0}")]
    Raft(String),
}

/// A node's view of the cluster, for tests and benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub id: NodeId,
    /// openraft's server state: Leader, Follower, Candidate or Learner.
    pub state: String,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub last_log_index: Option<u64>,
    pub applied: Option<u64>,
    pub snapshot: Option<u64>,
    pub purged: Option<u64>,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// On the leader: the highest log index each other node has stored.
    pub matched: BTreeMap<NodeId, Option<u64>>,
    /// AppendEntries traffic this node sent, per target.
    pub sent: Vec<PeerStats>,
}

/// How this node's openraft sent AppendEntries to one target.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStats {
    pub target: NodeId,
    /// Requests sent, heartbeats included.
    pub append_requests: u64,
    /// Requests that carried at least one entry.
    pub append_requests_with_entries: u64,
    pub entries_sent: u64,
    pub max_entries_per_request: u64,
    /// The most AppendEntries requests to this target that were waiting for
    /// an answer at the same time.
    pub max_in_flight: u64,
}
