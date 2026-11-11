//! Requests to a node's application handler, from other nodes and from
//! [`Client`]s, and the client itself.
//!
//! Wire format: a request is the bincode encoding of [`Request`]; the answer
//! is the bincode encoding of `Result<Answer, Error>`. Both travel inside the
//! Raft crate's `App` frames ([`tursoraft_raft::Node::app_request`] between
//! nodes, [`tursoraft_raft::Client::app`] from clients).

use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{CommitMode, Error, Node, ReadLevel, Rows, Statement, Stats, Written};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    /// From another node: run this transaction in the leader loop here. Fails
    /// with [`Error::Unavailable`] if this node is not the leader.
    Lead {
        statements: Vec<Statement>,
        mode: CommitMode,
    },
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
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Answer {
    Written(Written),
    Rows(Rows),
    Stats(Stats),
}

pub(crate) async fn handle(node: Node, request: Vec<u8>) -> Result<Vec<u8>, String> {
    let request: Request = decode(&request).map_err(|e| e.to_string())?;
    let answer = match request {
        Request::Lead { statements, mode } => {
            node.lead(statements, mode).await.map(Answer::Written)
        }
        Request::Execute { statements, mode } => {
            node.execute(statements, mode).await.map(Answer::Written)
        }
        Request::Query { statement, level } => node.query(statement, level).await.map(Answer::Rows),
        Request::Stats => Ok(Answer::Stats(node.stats())),
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

    async fn call(&self, request: Request) -> Result<Answer, Error> {
        use tursoraft_raft::Error as R;
        match self.raft.app(encode(&request)).await {
            Ok(bytes) => decode::<Result<Answer, Error>>(&bytes)?,
            Err(e @ R::Unreachable(_)) => Err(Error::Unavailable(e.to_string())),
            Err(e) => Err(Error::OutcomeUnknown(e.to_string())),
        }
    }
}
