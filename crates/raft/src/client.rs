//! A client of one node over TCP, for tests, benchmarks and tools in other
//! processes. The client counts as co-located with its node: the delay table
//! does not apply to it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::network::{unexpected, LeaderOp, Peer, Request, Response};
use crate::{Error, NodeId, Proposed, Status};

/// One connection to one node. Requests may run concurrently on it.
pub struct Client {
    peer: Peer,
    timeout: Duration,
}

impl Client {
    /// Connects on first use. Requests time out after 30 s.
    pub fn new(addr: impl Into<String>) -> Client {
        Client {
            peer: Peer::new(addr.into(), None, Duration::ZERO, 0),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Client {
        self.timeout = timeout;
        self
    }

    /// See [`crate::Node::propose`].
    pub async fn propose(&self, proposal: Vec<u8>) -> Result<Proposed, Error> {
        let mut proposed = self.propose_many(vec![proposal]).await?;
        Ok(proposed.pop().expect("one result per proposal"))
    }

    /// See [`crate::Node::propose_many`].
    pub async fn propose_many(&self, proposals: Vec<Vec<u8>>) -> Result<Vec<Proposed>, Error> {
        if proposals.is_empty() {
            return Ok(Vec::new());
        }
        let count = proposals.len();
        match self
            .call(Request::Client(LeaderOp::Propose(proposals)))
            .await?
        {
            Response::Proposed(p) if p.len() == count => Ok(p),
            other => Err(unexpected(other)),
        }
    }

    /// See [`crate::Node::transfer_leader`]: returns once the connected node
    /// sees `to` as leader, or [`Error::Timeout`] after the client timeout.
    pub async fn transfer_leader(&self, to: NodeId) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        match self
            .call(Request::Client(LeaderOp::TransferLeader(to)))
            .await?
        {
            Response::Done => {}
            other => return Err(unexpected(other)),
        }
        loop {
            if self.status().await?.leader == Some(to) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout(format!(
                    "node {to} did not become leader in time"
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn status(&self) -> Result<Status, Error> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(*s),
            other => Err(unexpected(other)),
        }
    }

    /// See [`crate::Node::initialize`].
    pub async fn initialize(&self, voters: BTreeMap<NodeId, String>) -> Result<(), Error> {
        self.call(Request::Initialize(voters)).await.map(|_| ())
    }

    /// See [`crate::Node::add_follower`].
    pub async fn add_follower(&self, id: NodeId, addr: impl Into<String>) -> Result<u64, Error> {
        let addr = addr.into();
        self.index(Request::Client(LeaderOp::AddFollower { id, addr }))
            .await
    }

    /// See [`crate::Node::change_voters`].
    pub async fn change_voters(&self, voters: BTreeSet<NodeId>) -> Result<u64, Error> {
        self.index(Request::Client(LeaderOp::ChangeVoters(voters)))
            .await
    }

    /// See [`crate::Node::wait_applied`].
    pub async fn wait_applied(&self, index: u64, timeout: Duration) -> Result<(), Error> {
        let timeout_ms = timeout.as_millis() as u64;
        self.call(Request::WaitApplied { index, timeout_ms })
            .await
            .map(|_| ())
    }

    /// See [`crate::Node::linearizable_read`].
    pub async fn linearizable_read(&self) -> Result<u64, Error> {
        self.index(Request::LinearizableRead).await
    }

    /// Asks the node to measure its round trip to `peer`.
    pub async fn ping_peer(&self, peer: NodeId) -> Result<Duration, Error> {
        match self.call(Request::PingPeer(peer)).await? {
            Response::Rtt { micros } => Ok(Duration::from_micros(micros)),
            other => Err(unexpected(other)),
        }
    }

    /// Sends `request` to the node's [`crate::AppHandler`].
    pub async fn app(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.call(Request::App(request)).await? {
            Response::App(b) => Ok(b),
            other => Err(unexpected(other)),
        }
    }

    async fn index(&self, req: Request) -> Result<u64, Error> {
        match self.call(req).await? {
            Response::Index(i) => Ok(i),
            other => Err(unexpected(other)),
        }
    }

    async fn call(&self, req: Request) -> Result<Response, Error> {
        self.peer.call(&req, self.timeout).await
    }
}
