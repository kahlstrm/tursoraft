//! Interactive transactions: begin, statements one at a time, then commit
//! or roll back.
//!
//! The statements run on this node's own file, in a local transaction with
//! change capture ([`tursoraft_changeset::LocalTx`]), and read the state at
//! the log index this node had applied when the transaction began. Commit
//! reads the change set, rolls the local transaction back, and sends the
//! change set to the leader in one message; the leader proposes it, and
//! every node, this one included, applies it in log order with the
//! commit-time check. So the node's file changes only by applying committed
//! entries, and a transaction that conflicts with one committed after its
//! base index is rejected on every node alike.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

use tursoraft_changeset::{ChangeSet, LocalTx};

use crate::client::{Answer, Request};
use crate::leader::Work;
use crate::measured::Counters;
use crate::{Client, CommitMode, Error, Node, Rows, Statement, Written};

/// An open interactive transaction on a [`Node`]; see [`Node::begin`].
/// Dropping it rolls it back.
pub struct Transaction {
    node: Node,
    tx: Option<LocalTx>,
}

impl Node {
    /// Begins an interactive transaction on this node. It holds the write
    /// lock of this node's file until it ends, so this node does not apply
    /// entries meanwhile; another interactive transaction here waits for it.
    /// Once an apply or another transaction has waited for it for
    /// [`crate::Options::local_hold_ms`], it is rolled back and its next call
    /// fails with [`Error::Aborted`]. Fails with [`Error::Aborted`] if another
    /// transaction holds the lock for the whole wait (the node's timeout).
    pub async fn begin(&self) -> Result<Transaction, Error> {
        let db = self.inner.db.clone();
        let wait = self.inner.timeout;
        let tx = tokio::task::spawn_blocking(move || db.begin(wait))
            .await
            .expect("begin does not panic")
            .map_err(|e| local_error(&self.inner.counters, e))?;
        self.inner.counters.local_begun.fetch_add(1, Relaxed);
        Ok(Transaction {
            node: self.clone(),
            tx: Some(tx),
        })
    }
}

impl Transaction {
    /// The log index of the state this transaction reads.
    pub fn base_index(&self) -> Option<u64> {
        self.tx.as_ref().map(LocalTx::base_index)
    }

    /// Runs one statement on this node's file and returns its rows. A
    /// statement that fails returns [`Error::Transaction`] and has no effect;
    /// the transaction stays open. Statements that control the transaction
    /// (BEGIN, COMMIT, SAVEPOINT, ...) are refused the same way.
    pub async fn execute(&mut self, statement: Statement) -> Result<Rows, Error> {
        let Some(mut tx) = self.tx.take() else {
            return Err(Error::Aborted("the transaction has ended".into()));
        };
        let (tx, result) = tokio::task::spawn_blocking(move || {
            let result = tx.execute(&statement.to_changeset());
            (tx, result)
        })
        .await
        .expect("a statement does not panic");
        self.tx = Some(tx);
        result
            .map(Rows::from_changeset)
            .map_err(|e| local_error(&self.node.inner.counters, e))
    }

    /// Ends the transaction and sends its change set to the leader, which
    /// proposes it. Answers at the time `mode` sets, as
    /// [`Node::execute`] does; a [`CommitMode::Quorum`] commit also waits
    /// until this node has applied the entry. The [`Written::results`] are
    /// empty: [`Transaction::execute`] returned them.
    ///
    /// [`Error::Rejected`]: a transaction committed after this one's base
    /// index changed a row this one changed, or a constraint fails on the
    /// rows as they are now. It had no effect; running it again is safe.
    ///
    /// A transaction that changed nothing sends nothing and is answered at
    /// once with its base index: like a [`crate::ReadLevel::Local`] read, it
    /// saw committed writes only, but maybe not the latest ones.
    pub async fn commit(mut self, mode: CommitMode) -> Result<Written, Error> {
        let Some(tx) = self.tx.take() else {
            return Err(Error::Aborted("the transaction has ended".into()));
        };
        let base_index = tx.base_index();
        let change_set = tokio::task::spawn_blocking(move || tx.commit())
            .await
            .expect("commit does not panic")
            .map_err(|e| local_error(&self.node.inner.counters, e))?;
        let empty = ChangeSet::decode(&change_set)
            .expect("a local transaction encodes a valid change set")
            .changes
            .is_empty();
        if empty {
            return Ok(Written {
                index: base_index,
                results: Vec::new(),
            });
        }
        self.node.on_leader(Work::ChangeSet(change_set), mode).await
    }

    /// Rolls the transaction back. Nothing is sent.
    pub fn rollback(self) {}
}

/// An open interactive transaction on the node a [`Client`] reaches; see
/// [`Client::begin`].
pub struct ClientTransaction<'a> {
    pub(crate) client: &'a Client,
    pub(crate) id: u64,
}

impl ClientTransaction<'_> {
    /// See [`Transaction::execute`].
    pub async fn execute(&mut self, statement: Statement) -> Result<Rows, Error> {
        let request = Request::TxExecute {
            id: self.id,
            statement,
        };
        match self.client.call(request).await? {
            Answer::Rows(rows) => Ok(rows),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    /// See [`Transaction::commit`]. A commit that was sent and got no
    /// answer returns [`Error::OutcomeUnknown`].
    pub async fn commit(self, mode: CommitMode) -> Result<Written, Error> {
        let request = Request::TxCommit { id: self.id, mode };
        match self.client.call(request).await? {
            Answer::Written(w) => Ok(w),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    /// See [`Transaction::rollback`].
    pub async fn rollback(self) -> Result<(), Error> {
        self.client
            .call(Request::TxRollback { id: self.id })
            .await
            .map(|_| ())
    }
}

/// The transactions [`Client`]s have open on this node, by id.
#[derive(Default)]
pub(crate) struct Open {
    last_id: AtomicU64,
    transactions: Mutex<HashMap<u64, Transaction>>,
}

pub(crate) async fn begin_for_client(node: &Node) -> Result<u64, Error> {
    let tx = node.begin().await?;
    let open = &node.inner.open;
    let id = open.last_id.fetch_add(1, Relaxed) + 1;
    open.transactions.lock().unwrap().insert(id, tx);
    Ok(id)
}

pub(crate) async fn execute_for_client(
    node: &Node,
    id: u64,
    statement: Statement,
) -> Result<Rows, Error> {
    let mut tx = take(node, id)?;
    let result = tx.execute(statement).await;
    node.inner.open.transactions.lock().unwrap().insert(id, tx);
    result
}

pub(crate) async fn commit_for_client(
    node: &Node,
    id: u64,
    mode: CommitMode,
) -> Result<Written, Error> {
    take(node, id)?.commit(mode).await
}

pub(crate) fn rollback_for_client(node: &Node, id: u64) {
    if let Ok(tx) = take(node, id) {
        tx.rollback();
    }
}

fn take(node: &Node, id: u64) -> Result<Transaction, Error> {
    node.inner
        .open
        .transactions
        .lock()
        .unwrap()
        .remove(&id)
        .ok_or_else(|| Error::Aborted(format!("no open transaction {id} on this node")))
}

fn local_error(counters: &Counters, e: tursoraft_changeset::Error) -> Error {
    use tursoraft_changeset::Error as C;
    match e {
        C::Aborted(m) | C::Busy(m) => {
            counters.local_aborted.fetch_add(1, Relaxed);
            Error::Aborted(m)
        }
        e @ (C::Statement { .. } | C::NotAllowed { .. }) => Error::Transaction(e.to_string()),
        e => Error::Internal(e.to_string()),
    }
}
