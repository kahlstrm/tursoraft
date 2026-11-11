//! The interface between the Raft layer (`tursoraft-raft`) and the replicated
//! database (`tursoraft-changeset`).
//!
//! The Raft layer orders proposals into a log and hands each committed entry to
//! a [`StateMachine`] on every node, the leader included. A proposal is opaque
//! bytes to the Raft layer (an encoded change set). The state machine never
//! looks inside `raft_meta`: it stores it in the same transaction as the
//! entry's effect and gives it back, so the Raft layer can recover its applied
//! position (for openraft: the last applied `LogId` and the membership) after a
//! restart or from a snapshot.
//!
//! Do not change this crate without agreeing with the owners of both sides.

use serde::{Deserialize, Serialize};

/// One committed Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Raft log index. Strictly increasing across calls to [`StateMachine::apply`].
    pub index: u64,
    /// Opaque to the state machine; recorded as the new [`Applied::raft_meta`].
    pub raft_meta: Vec<u8>,
    /// The proposals this entry orders, in order. The Raft layer may put
    /// proposals from several callers into one entry. Empty for entries without
    /// application data, such as blank or membership entries; those still
    /// advance the applied position.
    pub proposals: Vec<Vec<u8>>,
}

/// What applying one proposal did. The Raft layer returns it to the caller
/// that made the proposal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Applied,
    /// The proposal had no effect, for example because it conflicts with an
    /// earlier proposal. The rest of the entry is still applied.
    Rejected {
        reason: String,
    },
}

/// The last entry whose effect is in the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applied {
    pub index: u64,
    pub raft_meta: Vec<u8>,
}

/// A full copy of the state machine at an applied position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub applied: Applied,
    /// Opaque to the Raft layer (for example the database file's bytes).
    pub data: Vec<u8>,
}

/// The replicated state, as seen by the Raft layer.
///
/// Methods are blocking; the Raft layer calls them off its async executor
/// (for example with `tokio::task::spawn_blocking`).
pub trait StateMachine: Send + 'static {
    /// A failure that stops this node from applying entries, such as an I/O
    /// error. A rejected proposal is not an error; it is an [`Outcome`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The applied position, read from durable state. `None` before the first
    /// entry or snapshot.
    fn applied(&mut self) -> Result<Option<Applied>, Self::Error>;

    /// Applies one committed entry and records its position, atomically: after
    /// a crash either the whole entry and its position are visible, or neither.
    /// Returns one [`Outcome`] per proposal, in the same order.
    ///
    /// Every node must reach the same outcomes for the same entry, so a
    /// rejection may depend only on the entry and the state before it.
    ///
    /// An entry at or below the applied index is not applied again; it returns
    /// `Ok(vec![])`, so replaying the log after a restart is safe.
    fn apply(&mut self, entry: &Entry) -> Result<Vec<Outcome>, Self::Error>;

    /// Builds a snapshot of the current state.
    fn snapshot(&mut self) -> Result<Snapshot, Self::Error>;

    /// Replaces the whole state with `snapshot`. The Raft layer then applies
    /// the entries after `snapshot.applied.index`.
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error>;
}
