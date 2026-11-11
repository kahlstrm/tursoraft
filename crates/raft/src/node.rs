//! A Raft member: openraft, its log, the state machine adaptor, the batcher,
//! and the TCP endpoint that serves other nodes and clients.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use openraft::error::{ChangeMembershipError, ClientWriteError, LinearizableReadError, RaftError};
use openraft::storage::RaftStateMachine;
use openraft::{BasicNode, ReadPolicy, SnapshotPolicy};
use tokio::io::BufReader;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tursoraft_statemachine::{Outcome, StateMachine};

use crate::log_store::LogStore;
use crate::network::{
    delayed_writer, encode, read_frame, unexpected, DelayedSender, Hello, InboundStreams, LeaderOp,
    Network, NetworkFactory, Reply, Request, Response,
};
use crate::state_machine::StateMachineAdaptor;
use crate::{Batching, Error, NodeConfig, NodeId, Proposed, Raft, Status};

/// Answers application requests: [`Node::app_request`] from other nodes and
/// [`crate::Client::app`] from clients. It gets this node, so it can propose
/// or read through it. An `Err` reaches the caller as [`Error::App`].
pub type AppHandler = Arc<dyn Fn(Node, Vec<u8>) -> AppFuture + Send + Sync>;

/// The answer of an [`AppHandler`].
pub type AppFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>;

/// One Raft member. Cloning gives another handle to the same node.
#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
}

struct Inner {
    id: NodeId,
    raft: Raft,
    network: Arc<Network>,
    config: NodeConfig,
    batcher: Option<mpsc::UnboundedSender<Pending>>,
    app: RwLock<Option<AppHandler>>,
    local_addr: SocketAddr,
    server: Mutex<Option<JoinHandle<()>>>,
}

/// A group of proposals that must go into one log entry, in this order.
struct Pending {
    proposals: Vec<Vec<u8>>,
    tx: oneshot::Sender<Result<Vec<Proposed>, Error>>,
}

impl Node {
    /// Opens the log in `config.dir`, starts Raft, and listens on
    /// `config.listen`. A node that has no log yet waits until it is made a
    /// member, either by [`Node::initialize`] on one node or by
    /// [`Node::add_follower`] on the leader.
    pub async fn start<S: StateMachine>(
        config: NodeConfig,
        state_machine: S,
    ) -> Result<Node, Error> {
        let io_err = |e: std::io::Error| Error::Raft(e.to_string());
        let listener = TcpListener::bind(&config.listen).await.map_err(io_err)?;
        let local_addr = listener.local_addr().map_err(io_err)?;
        let log = LogStore::open(&config.dir, config.log.segment_bytes).map_err(io_err)?;
        let last_log_index = log.last_index();
        let mut sm = StateMachineAdaptor::new(state_machine)
            .await
            .map_err(raft_err)?;
        let (applied, _) = sm.applied_state().await.map_err(raft_err)?;
        if applied.map(|l| l.index) < log.last_purged().map(|l| l.index) {
            return Err(Error::Raft(format!(
                "the state machine is at {applied:?}, but the log was purged up to {:?}",
                log.last_purged()
            )));
        }

        let t = &config.timeouts;
        let raft_config = openraft::Config {
            cluster_name: "tursoraft".into(),
            heartbeat_interval: t.heartbeat_ms,
            election_timeout_min: t.election_timeout_min_ms,
            election_timeout_max: t.election_timeout_max_ms,
            install_snapshot_timeout: t.install_snapshot_ms,
            max_payload_entries: config.log.max_payload_entries,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.log.snapshot_every),
            max_in_snapshot_log_to_keep: config.log.keep_after_snapshot,
            // A voter whose log is behind finds out in a pre-vote that it
            // cannot win, before it raises its term and takes the votes of
            // the others for that term.
            enable_pre_vote: Some(true),
            ..Default::default()
        }
        .validate()
        .map_err(|e| Error::Raft(e.to_string()))?;

        let network = Arc::new(Network::new(
            config.id,
            config.delays.clone(),
            Duration::from_millis(t.heartbeat_ms),
        ));
        let raft = Raft::new(
            config.id,
            Arc::new(raft_config),
            NetworkFactory(network.clone()),
            log,
            sm,
        )
        .await
        .map_err(|e| Error::Raft(e.to_string()))?;
        wait_loaded(&raft, last_log_index).await?;
        let batcher = config.batching.clone().map(|b| {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(run_batcher(raft.clone(), b, rx));
            tx
        });
        let node = Node {
            inner: Arc::new(Inner {
                id: config.id,
                raft,
                network,
                config,
                batcher,
                app: RwLock::new(None),
                local_addr,
                server: Mutex::new(None),
            }),
        };
        let server = tokio::spawn(serve(listener, node.clone()));
        *node.inner.server.lock().unwrap() = Some(server);
        Ok(node)
    }

    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// The openraft handle, for metrics and anything this API does not cover.
    pub fn raft(&self) -> &Raft {
        &self.inner.raft
    }

    /// Forms a new cluster with these voters (id and listen address). Call it
    /// on one node, once.
    pub async fn initialize(&self, voters: BTreeMap<NodeId, String>) -> Result<(), Error> {
        let members: BTreeMap<NodeId, BasicNode> = voters
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.inner.raft.initialize(members).await.map_err(raft_err)
    }

    /// Orders `proposal` and waits until the leader's state machine has
    /// applied it. Returns the log index and the proposal's [`Outcome`]. Works
    /// on any node: a follower forwards the proposal to the leader. To read
    /// the effect on this node afterwards, call [`Node::wait_applied`] with
    /// the returned index.
    ///
    /// [`Error::NotLeader`] and [`Error::Unreachable`] mean the proposal had
    /// no effect; [`Error::OutcomeUnknown`] and [`Error::Timeout`] mean it may
    /// still take effect.
    pub async fn propose(&self, proposal: Vec<u8>) -> Result<Proposed, Error> {
        let mut proposed = self.propose_many(vec![proposal]).await?;
        Ok(proposed.pop().expect("one result per proposal"))
    }

    /// Orders `proposals` as one group: all of them go into the same log
    /// entry, in the given order, with no other proposal between them. With
    /// batching on, other callers' proposals may come before or after the
    /// group in that entry. Returns one [`Proposed`] per proposal, in order,
    /// all with the same index. Works on any node, as [`Node::propose`] does.
    /// An empty list proposes nothing and returns an empty list.
    pub async fn propose_many(&self, proposals: Vec<Vec<u8>>) -> Result<Vec<Proposed>, Error> {
        if proposals.is_empty() {
            return Ok(Vec::new());
        }
        let count = proposals.len();
        match self.run(LeaderOp::Propose(proposals)).await? {
            Response::Proposed(p) if p.len() == count => Ok(p),
            other => Err(unexpected(other)),
        }
    }

    /// Sends `request` to the [`AppHandler`] of node `to` and returns its
    /// answer. The request travels on the same connection as Raft traffic, so
    /// it pays the delay table's round trip between this node and `to`. A
    /// request to this node itself calls the handler directly.
    pub async fn app_request(
        &self,
        to: NodeId,
        request: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, Error> {
        if to == self.inner.id {
            return self.call_app_handler(request).await;
        }
        let addr = self
            .addr_of(to)
            .ok_or_else(|| Error::Raft(format!("node {to} is unknown")))?;
        let peer = self.inner.network.peer(to, &addr);
        match peer.call(&Request::App(request), timeout).await? {
            Response::App(answer) => Ok(answer),
            other => Err(unexpected(other)),
        }
    }

    /// Hands leadership to voter `to` and waits until this node sees `to` as
    /// leader. Works on any node: a follower forwards the request to the
    /// leader. While the transfer runs, the old leader refuses new proposals,
    /// and [`Node::propose`] retries them at the new leader. Fails with
    /// [`Error::Timeout`] if `to` has not taken over within the proposal
    /// timeout; openraft then lets the old leader step down and hold an
    /// election, which any voter may win.
    pub async fn transfer_leader(&self, to: NodeId) -> Result<(), Error> {
        match self.run(LeaderOp::TransferLeader(to)).await? {
            Response::Done => {}
            other => return Err(unexpected(other)),
        }
        let deadline = Instant::now() + Duration::from_millis(self.inner.config.propose_timeout_ms);
        let mut rx = self.inner.raft.metrics();
        loop {
            if rx.borrow_and_update().current_leader == Some(to) {
                return Ok(());
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {}
                _ => {
                    return Err(Error::Timeout(format!(
                        "node {to} did not become leader in time"
                    )))
                }
            }
        }
    }

    /// The current leader as this node knows it.
    pub fn leader(&self) -> Option<NodeId> {
        self.inner.raft.metrics().borrow_watched().current_leader
    }

    /// Adds a non-voting follower (an openraft learner). Returns the log index
    /// of the membership change. The leader never waits for it.
    pub async fn add_follower(&self, id: NodeId, addr: String) -> Result<u64, Error> {
        self.run_for_index(LeaderOp::AddFollower { id, addr }).await
    }

    /// Makes exactly `voters` the voting members. Each must already be a
    /// member; add new ones with [`Node::add_follower`] first. Voters that are
    /// left out stay on as non-voting followers. Returns the log index of the
    /// final membership entry.
    pub async fn change_voters(&self, voters: BTreeSet<NodeId>) -> Result<u64, Error> {
        self.run_for_index(LeaderOp::ChangeVoters(voters)).await
    }

    /// Waits until this node has applied log index `index`.
    pub async fn wait_applied(&self, index: u64, timeout: Duration) -> Result<(), Error> {
        self.inner
            .raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(index), "wait_applied")
            .await
            .map(|_| ())
            .map_err(|e| Error::Timeout(e.to_string()))
    }

    /// Confirms with a majority of voters that the leader is still the
    /// leader, then waits until this node has applied everything the leader
    /// had committed when asked. A read of this node's state machine after it
    /// returns is linearizable. Returns that log index. Costs one round trip
    /// from the leader to the nearest majority, plus one from this node to
    /// the leader if this node is a follower.
    pub async fn linearizable_read(&self) -> Result<u64, Error> {
        let index = self.run_for_index(LeaderOp::ReadIndex).await?;
        let timeout = Duration::from_millis(self.inner.config.propose_timeout_ms);
        self.wait_applied(index, timeout).await?;
        Ok(index)
    }

    /// Measures the round trip to another node through the network layer,
    /// configured delay included.
    pub async fn ping(&self, peer: NodeId) -> Result<Duration, Error> {
        let addr = self
            .addr_of(peer)
            .ok_or_else(|| Error::Raft(format!("node {peer} is unknown")))?;
        let peer = self.inner.network.peer(peer, &addr);
        let start = Instant::now();
        peer.call(&Request::Ping, Duration::from_secs(10)).await?;
        Ok(start.elapsed())
    }

    pub fn status(&self) -> Status {
        let m = self.inner.raft.metrics().borrow_watched().clone();
        let membership = m.membership_config.membership();
        Status {
            id: self.inner.id,
            state: format!("{:?}", m.state),
            leader: m.current_leader,
            term: m.current_term,
            last_log_index: m.last_log_index,
            applied: m.last_applied.as_ref().map(|l| l.index),
            snapshot: m.snapshot.as_ref().map(|l| l.index),
            purged: m.purged.as_ref().map(|l| l.index),
            voters: membership.voter_ids().collect(),
            learners: membership.non_write_voter_ids().collect(),
            matched: m
                .replication
                .as_ref()
                .map(|r| {
                    r.iter()
                        .map(|(id, l)| (*id, l.as_ref().map(|l| l.index)))
                        .collect()
                })
                .unwrap_or_default(),
            sent: self.inner.network.stats(),
        }
    }

    pub fn set_app_handler(&self, handler: AppHandler) {
        *self.inner.app.write().unwrap() = Some(handler);
    }

    pub async fn shutdown(&self) {
        if let Some(server) = self.inner.server.lock().unwrap().take() {
            server.abort();
        }
        let _ = self.inner.raft.shutdown().await;
    }

    async fn run_for_index(&self, op: LeaderOp) -> Result<u64, Error> {
        match self.run(op).await? {
            Response::Index(i) => Ok(i),
            other => Err(unexpected(other)),
        }
    }

    /// Runs `op` on the leader: here if this node leads, otherwise by
    /// forwarding it. Retries while there is no leader or the leader changes,
    /// as long as the earlier attempt surely had no effect.
    async fn run(&self, op: LeaderOp) -> Result<Response, Error> {
        let deadline = Instant::now() + Duration::from_millis(self.inner.config.propose_timeout_ms);
        loop {
            let Some(leader) = self.wait_leader(deadline).await else {
                return Err(Error::NotLeader { leader: None });
            };
            let res = if leader == self.inner.id {
                self.run_at_leader(op.clone()).await
            } else {
                match self.addr_of(leader) {
                    Some(addr) => {
                        let peer = self.inner.network.peer(leader, &addr);
                        let left = deadline.saturating_duration_since(Instant::now());
                        peer.call(&Request::AtLeader(op.clone()), left).await
                    }
                    None => Err(Error::NotLeader {
                        leader: Some(leader),
                    }),
                }
            };
            match res {
                Err(
                    Error::NotLeader { .. }
                    | Error::Unreachable(_)
                    | Error::MembershipChangeInProgress(_),
                ) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                res => return res,
            }
        }
    }

    async fn run_at_leader(&self, op: LeaderOp) -> Reply {
        let raft = &self.inner.raft;
        match op {
            LeaderOp::Propose(proposals) => self
                .propose_at_leader(proposals)
                .await
                .map(Response::Proposed),
            LeaderOp::ReadIndex => match raft.get_read_log_id(ReadPolicy::ReadIndex).await {
                Ok((read, _applied)) => Ok(Response::Index(read.log_id().index)),
                Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(f))) => {
                    Err(Error::NotLeader {
                        leader: f.leader_id,
                    })
                }
                Err(e) => Err(Error::Raft(e.to_string())),
            },
            LeaderOp::TransferLeader(to) => {
                if !raft.is_leader() {
                    return Err(Error::NotLeader {
                        leader: self.leader(),
                    });
                }
                if to == self.inner.id {
                    return Ok(Response::Done);
                }
                if !raft.voter_ids().any(|v| v == to) {
                    return Err(Error::Raft(format!("node {to} is not a voter")));
                }
                raft.trigger().transfer_leader(to).await.map_err(raft_err)?;
                Ok(Response::Done)
            }
            LeaderOp::AddFollower { id, addr } => raft
                .add_learner(id, BasicNode::new(addr), false)
                .await
                .map(|r| Response::Index(r.log_id.index))
                .map_err(client_write_err),
            LeaderOp::ChangeVoters(voters) => raft
                .change_membership(voters, true)
                .await
                .map(|r| Response::Index(r.log_id.index))
                .map_err(client_write_err),
        }
    }

    async fn propose_at_leader(&self, proposals: Vec<Vec<u8>>) -> Result<Vec<Proposed>, Error> {
        match &self.inner.batcher {
            Some(batcher) => {
                let (tx, rx) = oneshot::channel();
                batcher
                    .send(Pending { proposals, tx })
                    .map_err(|_| Error::Raft("batcher stopped".into()))?;
                rx.await
                    .map_err(|_| Error::Raft("batcher stopped".into()))?
            }
            None => {
                let (index, outcomes) = write_entry(&self.inner.raft, proposals).await?;
                Ok(outcomes
                    .into_iter()
                    .map(|outcome| Proposed { index, outcome })
                    .collect())
            }
        }
    }

    async fn call_app_handler(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let handler = self.inner.app.read().unwrap().clone();
        match handler {
            Some(h) => h(self.clone(), request).await.map_err(Error::App),
            None => Err(Error::Raft("no application handler".into())),
        }
    }

    /// Waits until this node knows a leader.
    async fn wait_leader(&self, deadline: Instant) -> Option<NodeId> {
        let mut rx = self.inner.raft.metrics();
        loop {
            if let Some(leader) = rx.borrow_and_update().current_leader {
                return Some(leader);
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {}
                _ => return None,
            }
        }
    }

    fn addr_of(&self, id: NodeId) -> Option<String> {
        let m = self.inner.raft.metrics();
        let m = m.borrow_watched();
        m.membership_config
            .membership()
            .get_node(&id)
            .map(|n| n.addr.clone())
            .or_else(|| self.inner.config.peers.get(&id).cloned())
    }

    async fn handle(&self, req: Request) -> Reply {
        let raft = &self.inner.raft;
        match req {
            Request::AppendEntries(rpc) => raft
                .append_entries(rpc)
                .await
                .map(|r| Response::Append(r.map_err(Into::into)))
                .map_err(raft_err),
            Request::StreamAppend { .. } | Request::StreamEnd { .. } => {
                unreachable!("serve_conn handles replication streams")
            }
            Request::Vote(rpc) => raft.vote(rpc).await.map(Response::Vote).map_err(raft_err),
            Request::PreVote(rpc) => raft
                .pre_vote(rpc)
                .await
                .map(Response::Vote)
                .map_err(raft_err),
            Request::TransferLeader(rpc) => raft
                .handle_transfer_leader(rpc)
                .await
                .map(Response::TransferLeader)
                .map_err(raft_err),
            Request::FullSnapshot { vote, meta, data } => {
                let snapshot = openraft::alias::SnapshotOf::<crate::TypeConfig, Vec<u8>> {
                    meta,
                    snapshot: data,
                };
                raft.install_full_snapshot(vote, snapshot)
                    .await
                    .map(Response::Snapshot)
                    .map_err(raft_err)
            }
            Request::AtLeader(op) => self.run_at_leader(op).await,
            Request::Client(op) => self.run(op).await,
            Request::Initialize(voters) => self.initialize(voters).await.map(|()| Response::Done),
            Request::Status => Ok(Response::Status(Box::new(self.status()))),
            Request::WaitApplied { index, timeout_ms } => self
                .wait_applied(index, Duration::from_millis(timeout_ms))
                .await
                .map(|()| Response::Done),
            Request::LinearizableRead => self.linearizable_read().await.map(Response::Index),
            Request::Ping => Ok(Response::Done),
            Request::PingPeer(peer) => self.ping(peer).await.map(|d| Response::Rtt {
                micros: d.as_micros() as u64,
            }),
            Request::App(bytes) => self.call_app_handler(bytes).await.map(Response::App),
        }
    }
}

/// Waits until openraft's metrics show the log loaded from disk, so that
/// [`Node::status`] right after [`Node::start`] reports it.
async fn wait_loaded(raft: &Raft, last_log_index: Option<u64>) -> Result<(), Error> {
    if last_log_index.is_none() {
        return Ok(());
    }
    raft.wait(Some(Duration::from_secs(30)))
        .metrics(|m| m.last_log_index >= last_log_index, "log loaded")
        .await
        .map(|_| ())
        .map_err(|e| Error::Raft(format!("openraft did not load the log: {e}")))
}

/// Proposes one log entry and waits until it is applied here.
async fn write_entry(raft: &Raft, proposals: Vec<Vec<u8>>) -> Result<(u64, Vec<Outcome>), Error> {
    let count = proposals.len();
    let resp = raft
        .client_write(crate::Proposals(proposals))
        .await
        .map_err(client_write_err)?;
    if resp.data.len() != count {
        return Err(Error::Raft(format!(
            "the state machine returned {} outcomes for {count} proposals at index {}",
            resp.data.len(),
            resp.log_id.index
        )));
    }
    Ok((resp.log_id.index, resp.data))
}

/// Collects proposals from concurrent callers into log entries. At most
/// `max_entries_in_flight` entries wait for commit at once; proposals that
/// arrive meanwhile queue up and go into the next entry together.
async fn run_batcher(raft: Raft, cfg: Batching, mut rx: mpsc::UnboundedReceiver<Pending>) {
    let in_flight = Arc::new(Semaphore::new(cfg.max_entries_in_flight.max(1)));
    let mut carried: Option<Pending> = None;
    loop {
        let permit = in_flight
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let first = match carried.take() {
            Some(p) => p,
            None => match rx.recv().await {
                Some(p) => p,
                None => return,
            },
        };
        let size = |p: &Pending| p.proposals.iter().map(Vec::len).sum::<usize>();
        let mut count = first.proposals.len();
        let mut bytes = size(&first);
        let mut batch = vec![first];
        // A group is never split; one that does not fit starts the next entry.
        while count < cfg.max_proposals && bytes < cfg.max_bytes {
            let Ok(p) = rx.try_recv() else { break };
            if count + p.proposals.len() > cfg.max_proposals || bytes + size(&p) > cfg.max_bytes {
                carried = Some(p);
                break;
            }
            count += p.proposals.len();
            bytes += size(&p);
            batch.push(p);
        }
        let mut proposals = Vec::with_capacity(count);
        let mut waiters = Vec::with_capacity(batch.len());
        for p in batch {
            waiters.push((p.proposals.len(), p.tx));
            proposals.extend(p.proposals);
        }
        let raft = raft.clone();
        tokio::spawn(async move {
            let res = write_entry(&raft, proposals).await;
            drop(permit);
            match res {
                Ok((index, outcomes)) => {
                    let mut outcomes = outcomes.into_iter();
                    for (n, tx) in waiters {
                        let mine = outcomes.by_ref().take(n);
                        let _ = tx.send(Ok(mine
                            .map(|outcome| Proposed { index, outcome })
                            .collect()));
                    }
                }
                Err(e) => {
                    for (_, tx) in waiters {
                        let _ = tx.send(Err(e.clone()));
                    }
                }
            }
        });
    }
}

/// Accepts connections and serves each in a task of its own. The tasks
/// belong to this one: when [`Node::shutdown`] aborts it, every connection
/// closes, so the other nodes connect again, to whichever node listens on
/// the address then.
async fn serve(listener: TcpListener, node: Node) {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let node = node.clone();
                    conns.spawn(async move {
                        if let Err(e) = node.serve_conn(stream).await {
                            tracing::debug!("connection closed: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            Some(_) = conns.join_next() => {}
        }
    }
}

impl Node {
    /// Reads requests and writes their answers in the same task, so that
    /// ending the task closes both directions of the connection.
    async fn serve_conn(self, stream: TcpStream) -> std::io::Result<()> {
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        let mut r = BufReader::new(r);
        let hello: Hello = read_frame(&mut r).await?;
        let delay = hello.from.map_or(Duration::ZERO, |from| {
            self.inner.config.delays.one_way(self.inner.id, from)
        });
        let (out, writer) = delayed_writer(w, delay);
        tokio::select! {
            res = self.serve_requests(r, &out) => res,
            res = writer => res,
        }
    }

    async fn serve_requests(
        &self,
        mut r: BufReader<OwnedReadHalf>,
        out: &DelayedSender,
    ) -> std::io::Result<()> {
        let mut streams = InboundStreams::default();
        loop {
            let (id, req): (u64, Request) = read_frame(&mut r).await?;
            let req = match req {
                Request::StreamAppend { stream, req } => {
                    streams.push(&self.inner.raft, out, stream, id, req);
                    continue;
                }
                Request::StreamEnd { stream } => {
                    streams.end(stream);
                    continue;
                }
                req => req,
            };
            let node = self.clone();
            let out = out.clone();
            tokio::spawn(async move {
                let reply = node.handle(req).await;
                out.send(encode(&(id, reply)));
            });
        }
    }
}

fn client_write_err(e: RaftError<crate::TypeConfig, ClientWriteError<crate::TypeConfig>>) -> Error {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => Error::NotLeader {
            leader: f.leader_id,
        },
        RaftError::APIError(ClientWriteError::LogEntryDiscarded(f)) => {
            Error::OutcomeUnknown(format!(
            "the entry was discarded on a leader change and may still be committed; leader is {:?}",
            f.leader_id
        ))
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(e),
        )) => Error::MembershipChangeInProgress(e.to_string()),
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::UncommittedLeaderLog(e),
        )) => Error::MembershipChangeInProgress(e.to_string()),
        e => Error::Raft(e.to_string()),
    }
}

fn raft_err(e: impl std::fmt::Display) -> Error {
    Error::Raft(e.to_string())
}
