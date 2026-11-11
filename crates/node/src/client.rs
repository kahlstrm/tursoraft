//! Requests to a node's application handler, from other nodes and from
//! [`Client`]s, and the client itself.
//!
//! Wire format: a request is the bincode encoding of [`Request`]; the answer
//! is the bincode encoding of `Result<Answer, Error>`. Both travel inside the
//! Raft crate's `App` frames ([`tursoraft_raft::Node::app_request`] between
//! nodes, [`tursoraft_raft::Client::app`] from clients).

use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::leader::Work;
use crate::{CommitMode, Error, Node, ReadLevel, Rows, Statement, Stats, Written};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    /// From another node: run this transaction in the leader loop here. Fails
    /// with [`Error::Unavailable`] if this node is not the leader.
    Lead { work: Work, mode: CommitMode },
    /// From a client: [`Node::execute`].
    Execute {
        statements: Vec<Statement>,
        mode: CommitMode,
    },
    /// From a client: [`Node::query`].
    Query {
        statement: Statement,
        level: ReadLevel,
    },
    /// From a client: [`Node::stats`].
    Stats,
    /// From a client: [`Node::begin`]. Answers [`Answer::Began`].
    Begin,
    /// From a client: [`crate::Transaction::execute`] of an open transaction.
    TxExecute { id: u64, statement: Statement },
    /// From a client: [`crate::Transaction::commit`].
    TxCommit { id: u64, mode: CommitMode },
    /// From a client: [`crate::Transaction::rollback`].
    TxRollback { id: u64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Answer {
    Written(Written),
    Rows(Rows),
    Stats(Stats),
    Began(u64),
    Done,
}

pub(crate) async fn handle(node: Node, request: Vec<u8>) -> Result<Vec<u8>, String> {
    let request: Request = decode(&request).map_err(|e| e.to_string())?;
    let answer = match request {
        Request::Lead { work, mode } => node.lead(work, mode).await.map(Answer::Written),
        Request::Execute { statements, mode } => {
            node.execute(statements, mode).await.map(Answer::Written)
        }
        Request::Query { statement, level } => node.query(statement, level).await.map(Answer::Rows),
        Request::Stats => Ok(Answer::Stats(node.stats())),
        Request::Begin => crate::transaction::begin_for_client(&node)
            .await
            .map(Answer::Began),
        Request::TxExecute { id, statement } => {
            crate::transaction::execute_for_client(&node, id, statement)
                .await
                .map(Answer::Rows)
        }
        Request::TxCommit { id, mode } => crate::transaction::commit_for_client(&node, id, mode)
            .await
            .map(Answer::Written),
        Request::TxRollback { id } => {
            crate::transaction::rollback_for_client(&node, id);
            Ok(Answer::Done)
        }
    };
    Ok(encode(&answer))
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("requests and answers serialize")
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    bincode::deserialize(bytes).map_err(|e| Error::Internal(format!("cannot decode: {e}")))
}

/// A client of one node process over TCP. The delay table does not apply to
/// it: it counts as running next to its node.
pub struct Client {
    raft: tursoraft_raft::Client,
}

impl Client {
    /// Connects on first use. Requests time out after 30 s.
    pub fn new(addr: impl Into<String>) -> Client {
        Client {
            raft: tursoraft_raft::Client::new(addr),
        }
    }

    pub fn with_timeout(self, timeout: Duration) -> Client {
        Client {
            raft: self.raft.with_timeout(timeout),
        }
    }

    /// The Raft client of the same node, for status and membership changes.
    pub fn raft(&self) -> &tursoraft_raft::Client {
        &self.raft
    }

    /// See [`Node::execute`]. A request that was sent and got no answer
    /// returns [`Error::OutcomeUnknown`].
    pub async fn execute(
        &self,
        statements: Vec<Statement>,
        mode: CommitMode,
    ) -> Result<Written, Error> {
        match self.call(Request::Execute { statements, mode }).await? {
            Answer::Written(w) => Ok(w),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    /// See [`Node::query`].
    pub async fn query(&self, statement: Statement, level: ReadLevel) -> Result<Rows, Error> {
        match self.call(Request::Query { statement, level }).await? {
            Answer::Rows(r) => Ok(r),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn stats(&self) -> Result<Stats, Error> {
        match self.call(Request::Stats).await? {
            Answer::Stats(s) => Ok(s),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    /// See [`Node::begin`]. The transaction lives on the node: if it is
    /// dropped without [`ClientTransaction::commit`] or
    /// [`ClientTransaction::rollback`], it stays open there until it holds
    /// up someone for the node's hold limit.
    pub async fn begin(&self) -> Result<crate::ClientTransaction<'_>, Error> {
        match self.call(Request::Begin).await? {
            Answer::Began(id) => Ok(crate::ClientTransaction { client: self, id }),
            other => Err(Error::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub(crate) async fn call(&self, request: Request) -> Result<Answer, Error> {
        use tursoraft_raft::Error as R;
        match self.raft.app(encode(&request)).await {
            Ok(bytes) => decode::<Result<Answer, Error>>(&bytes)?,
            Err(e @ R::Unreachable(_)) => Err(Error::Unavailable(e.to_string())),
            Err(e) => Err(Error::OutcomeUnknown(e.to_string())),
        }
    }
}
