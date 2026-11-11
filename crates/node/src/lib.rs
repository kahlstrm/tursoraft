//! A tursoraft node: one database file ([`NodeDb`]) and one Raft member
//! ([`tursoraft_raft::Node`]), joined through the
//! [`tursoraft_statemachine::StateMachine`] interface.
//!
//! Writes. A write transaction is a list of statements sent in one call
//! ([`Node::execute`]). Any node accepts it; a follower sends it to the leader
//! with [`tursoraft_raft::Node::app_request`]. The leader runs the
//! transactions that are waiting as one group and proposes their change sets
//! as one log entry. Without pipelining it captures a group on its own file
//! with [`NodeDb::capture_batch`], after it has applied everything in its log.
//! With pipelining ([`Options::max_pending_groups`]) it captures the next
//! group while earlier ones are still on their way through Raft, on a second
//! file that runs ahead ([`tursoraft_changeset::AheadDb`]); see the `leader`
//! module. Either way, every capture sees every earlier change set, and no
//! node writes its database file except by applying a committed entry.
//!
//! Reads ([`Node::query`]) run one `SELECT` on this node's file, at the
//! [`ReadLevel`] the caller asks for.
//!
//! [`Client`] reaches a node process over TCP, for tests and tools.

mod client;
mod leader;
mod measured;
mod reads;

use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tursoraft_changeset::turso_core::{self, Numeric};
use tursoraft_changeset::{AheadDb, NodeDb};
use tursoraft_raft::NodeConfig;

pub use client::Client;
pub use measured::Stats;

/// One node. Cloning gives another handle to the same node.
#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
}

struct Inner {
    raft: tursoraft_raft::Node,
    db: NodeDb,
    reads: reads::Reads,
    /// Transactions waiting for the leader loop, when this node leads.
    queue: mpsc::UnboundedSender<leader::Job>,
    counters: Arc<measured::Counters>,
    /// How long a write waits for a leader, and a read for a log position.
    timeout: Duration,
    options: Options,
}

/// Settings of one node that the cluster configuration does not hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    /// Most groups of transactions the leader has proposed and not yet
    /// applied. 1 turns pipelining off: the leader captures a group only
    /// after it has applied the one before. Above 1, the leader keeps a
    /// second database file next to the node's own (`<name>.ahead.db` for
    /// `<name>.db`) to capture on.
    pub max_pending_groups: usize,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            max_pending_groups: 256,
        }
    }
}

/// When a write is acknowledged. Chosen per transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitMode {
    /// After a majority has stored the entry and the leader has applied it.
    /// On a follower, also after this follower has applied it.
    Quorum,
    /// After the leader has written the entry to its own log and flushed it,
    /// before any other node has it. Lost if the leader fails before a
    /// majority stores the entry; see [`Node::execute`].
    Async,
}

/// What a read waits for before it reads this node's file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadLevel {
    /// Read at once. May miss writes that other nodes already acknowledged.
    Local,
    /// Wait until this node has applied this log index, such as the
    /// [`Written::index`] of an earlier write, then read.
    AtIndex(u64),
    /// Ask the leader for its commit index, confirmed by a majority, wait
    /// until this node has applied it, then read. Sees every write
    /// acknowledged in [`CommitMode::Quorum`] before the read started.
    Linearizable,
}

/// One SQL statement and its parameters, bound by position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Statement {
        Statement {
            sql: sql.into(),
            params,
        }
    }
}

impl From<&str> for Statement {
    fn from(sql: &str) -> Statement {
        Statement::new(sql, Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// The columns and rows one statement returned.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// Rows the statement inserted, updated or deleted. 0 for reads.
    pub rows_changed: u64,
}

/// An acknowledged write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Written {
    /// The log index of the entry that holds the transaction. Read with
    /// [`ReadLevel::AtIndex`] to see it on any node. For a transaction that
    /// changed nothing, the index its reads saw.
    pub index: u64,
    /// One result per statement, as the leader ran them at capture.
    pub results: Vec<Rows>,
}

/// Why a write or read failed. Each variant says whether a write took effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum Error {
    /// A statement failed or is not allowed in a transaction. The
    /// transaction had no effect.
    #[error("transaction failed: {0}")]
    Transaction(String),
    /// The commit-time check rejected the transaction's change set because
    /// another one changed the same row after it was captured. It had no
    /// effect; running it again is safe.
    #[error("rejected: {0}")]
    Rejected(String),
    /// No leader took the transaction in time, or the leader's database
    /// failed before proposing it. It had no effect; running it again is safe.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// The transaction was proposed, or sent to the leader, and no answer came
    /// back. It may still take effect; running it again may apply it twice.
    #[error("outcome unknown: {0}")]
    OutcomeUnknown(String),
    /// A read failed: invalid SQL, a statement that is not a `SELECT`, or a
    /// database error.
    #[error("read failed: {0}")]
    Read(String),
    /// A read did not reach its log position, or could not confirm the
    /// leader, in time.
    #[error("read timed out: {0}")]
    ReadTimeout(String),
    #[error("{0}")]
    Internal(String),
}

impl Node {
    /// Opens (or creates) the database file at `db_path`, starts the Raft
    /// member with `config`, and serves requests from other nodes and from
    /// [`Client`]s on `config.listen`. A new node waits until it is made a
    /// member through [`tursoraft_raft::Node::initialize`] or
    /// [`tursoraft_raft::Node::add_follower`]; it then gets the database as a
    /// snapshot or as log entries.
    pub async fn start(config: NodeConfig, db_path: impl AsRef<Path>) -> Result<Node, Error> {
        Node::start_with(config, db_path, Options::default()).await
    }

    /// [`Node::start`] with `options`.
    pub async fn start_with(
        config: NodeConfig,
        db_path: impl AsRef<Path>,
        options: Options,
    ) -> Result<Node, Error> {
        assert!(
            options.max_pending_groups >= 1,
            "max_pending_groups is at least 1"
        );
        let db_path = db_path.as_ref();
        let db = NodeDb::open(db_path).map_err(|e| Error::Internal(e.to_string()))?;
        let ahead = if options.max_pending_groups > 1 {
            let path = db_path.with_extension("ahead.db");
            Some(AheadDb::open(path).map_err(|e| Error::Internal(e.to_string()))?)
        } else {
            None
        };
        let counters = Arc::new(measured::Counters::default());
        let state_machine =
            measured::MeasuredStateMachine::new(db.state_machine(), counters.clone());
        let timeout = Duration::from_millis(config.propose_timeout_ms);
        let raft = tursoraft_raft::Node::start(config, state_machine)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        let (queue, rx) = mpsc::unbounded_channel();
        let node = Node {
            inner: Arc::new(Inner {
                raft: raft.clone(),
                reads: reads::Reads::new(db.clone()),
                db,
                queue,
                counters,
                timeout,
                options: options.clone(),
            }),
        };
        tokio::spawn(leader::run(
            Arc::downgrade(&node.inner),
            rx,
            ahead,
            options.max_pending_groups,
        ));
        let weak: Weak<Inner> = Arc::downgrade(&node.inner);
        raft.set_app_handler(Arc::new(move |_, request| {
            let weak = weak.clone();
            Box::pin(async move {
                let inner = weak.upgrade().ok_or("the node has stopped")?;
                client::handle(Node { inner }, request).await
            })
        }));
        Ok(node)
    }

    pub fn id(&self) -> u64 {
        self.inner.raft.id()
    }

    /// The Raft member, for membership changes and status.
    pub fn raft(&self) -> &tursoraft_raft::Node {
        &self.inner.raft
    }

    /// Runs `statements` as one transaction on the leader and replicates its
    /// change set. Works on any node. Returns the statements' results and the
    /// log index of the entry, at the time `mode` sets.
    ///
    /// On a follower, the request goes to the leader; a [`CommitMode::Quorum`]
    /// write then also waits until this follower has applied the entry, so a
    /// [`ReadLevel::Local`] read here sees it. Should that wait time out, the
    /// write is still acknowledged: it is committed.
    ///
    /// A [`CommitMode::Async`] write that was acknowledged is lost when the
    /// leader stops being leader before a majority has stored the entry: it
    /// crashes, or it is cut off, and the new leader's log lacks the entry. A
    /// crash of the leader alone does not lose it if the leader restarts and
    /// wins the next election, since the entry is in its flushed log. An async
    /// write acknowledged and later rejected, because it read a pending write
    /// that was rejected, also has no effect; [`Stats::async_acked_then_failed`]
    /// counts both cases.
    ///
    /// A transaction that changes no rows gets no log entry. In
    /// [`CommitMode::Quorum`] it is answered once a majority has confirmed
    /// that the leader still leads, as for [`ReadLevel::Linearizable`], and
    /// once the pending writes it read are applied; if one of those was
    /// rejected or lost, it fails with [`Error::Rejected`]. In
    /// [`CommitMode::Async`] it is answered at once from the leader's state,
    /// which may include writes that no majority has stored yet; a leader that
    /// was replaced without knowing it may answer from a stale state.
    ///
    /// See [`Error`] for which failures had an effect.
    pub async fn execute(
        &self,
        statements: Vec<Statement>,
        mode: CommitMode,
    ) -> Result<Written, Error> {
        let deadline = Instant::now() + self.inner.timeout;
        loop {
            let res = match self.wait_leader(deadline).await {
                None => return Err(Error::Unavailable("no leader is known".into())),
                Some(leader) if leader == self.id() => self.lead(statements.clone(), mode).await,
                Some(leader) => self.forward(leader, &statements, mode).await,
            };
            match res {
                // Nothing was captured or proposed, so trying again is safe.
                Err(Error::Unavailable(_)) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                res => return res,
            }
        }
    }

    /// Runs one `SELECT` on this node's file at `level`. Refuses any other
    /// statement, so nothing can write the file outside the log.
    pub async fn query(&self, statement: Statement, level: ReadLevel) -> Result<Rows, Error> {
        let raft = &self.inner.raft;
        match level {
            ReadLevel::Local => {}
            ReadLevel::AtIndex(index) => raft
                .wait_applied(index, self.inner.timeout)
                .await
                .map_err(|e| Error::ReadTimeout(e.to_string()))?,
            ReadLevel::Linearizable => {
                raft.linearizable_read()
                    .await
                    .map_err(|e| Error::ReadTimeout(e.to_string()))?;
            }
        }
        let reads = self.inner.reads.clone();
        tokio::task::spawn_blocking(move || reads.query(&statement))
            .await
            .map_err(|e| Error::Internal(e.to_string()))?
    }

    /// Counters of this node, for tests and benchmarks.
    pub fn stats(&self) -> Stats {
        self.inner.counters.stats(
            self.id(),
            self.inner.db.applied_index(),
            self.inner.options.max_pending_groups,
        )
    }

    pub async fn shutdown(&self) {
        self.inner.raft.shutdown().await;
    }

    /// Hands the transaction to this node's leader loop.
    async fn lead(&self, statements: Vec<Statement>, mode: CommitMode) -> Result<Written, Error> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .queue
            .send(leader::Job {
                statements,
                mode,
                reply,
            })
            .map_err(|_| Error::Unavailable("the leader loop has stopped".into()))?;
        rx.await
            .map_err(|_| Error::OutcomeUnknown("the leader loop dropped the transaction".into()))?
    }

    /// Sends the transaction to the leader's loop.
    async fn forward(
        &self,
        leader: u64,
        statements: &[Statement],
        mode: CommitMode,
    ) -> Result<Written, Error> {
        use tursoraft_raft::Error as R;
        let request = client::encode(&client::Request::Lead {
            statements: statements.to_vec(),
            mode,
        });
        // The leader may hold the transaction for up to its own timeout
        // (waiting for a group, then for commit), so do not give up sooner.
        let answer = match self
            .inner
            .raft
            .app_request(leader, request, 2 * self.inner.timeout)
            .await
        {
            Ok(answer) => answer,
            // The request was not sent, or the leader's handler did not run it.
            Err(e @ (R::Unreachable(_) | R::Raft(_) | R::NotLeader { .. } | R::App(_))) => {
                return Err(Error::Unavailable(format!("leader {leader}: {e}")))
            }
            Err(e) => return Err(Error::OutcomeUnknown(format!("leader {leader}: {e}"))),
        };
        let written =
            client::decode::<Result<client::Answer, Error>>(&answer)?.and_then(|answer| {
                match answer {
                    client::Answer::Written(w) => Ok(w),
                    other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
                }
            })?;
        if mode == CommitMode::Quorum {
            let _ = self
                .inner
                .raft
                .wait_applied(written.index, self.inner.timeout)
                .await;
        }
        Ok(written)
    }

    async fn wait_leader(&self, deadline: Instant) -> Option<u64> {
        loop {
            if let Some(leader) = self.inner.raft.leader() {
                return Some(leader);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

impl Value {
    fn to_turso(&self) -> turso_core::Value {
        match self {
            Value::Null => turso_core::Value::Null,
            Value::Integer(i) => turso_core::Value::from_i64(*i),
            Value::Real(f) => turso_core::Value::from_f64(*f),
            Value::Text(s) => turso_core::Value::build_text(s.clone()),
            Value::Blob(b) => turso_core::Value::from_slice(b).expect("allocate a blob"),
        }
    }

    fn from_turso(v: &turso_core::Value) -> Value {
        match v {
            turso_core::Value::Null => Value::Null,
            turso_core::Value::Numeric(Numeric::Integer(i)) => Value::Integer(*i),
            turso_core::Value::Numeric(Numeric::Float(f)) => Value::Real(f64::from(*f)),
            turso_core::Value::Text(t) => Value::Text(t.as_str().to_string()),
            turso_core::Value::Blob(_) => Value::Blob(v.to_blob().expect("a blob").to_vec()),
        }
    }
}

impl Statement {
    fn to_changeset(&self) -> tursoraft_changeset::Statement {
        tursoraft_changeset::Statement::new(
            self.sql.clone(),
            self.params.iter().map(Value::to_turso).collect(),
        )
    }
}

impl Rows {
    fn from_changeset(r: tursoraft_changeset::StatementResult) -> Rows {
        Rows {
            columns: r.columns,
            rows: r
                .rows
                .iter()
                .map(|row| row.iter().map(Value::from_turso).collect())
                .collect(),
            rows_changed: r.rows_changed,
        }
    }
}
