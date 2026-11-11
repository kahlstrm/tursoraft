//! TCP transport between nodes and from clients.
//!
//! Every frame is `[u32 length][bincode]`. A connection starts with a
//! [`Hello`] naming the sending node (or none, for a client). After that the
//! connecting side sends `(request id, Request)` frames and the other side
//! answers with `(request id, Result<Response, Error>)` frames, in any order,
//! so a connection carries any number of requests at once.
//!
//! openraft replicates through [`RaftNetworkV2::stream_append`]: the leader
//! sends each AppendEntries as soon as openraft produces it, without waiting
//! for the answer to the previous one. The receiving node hands the requests
//! of one stream to [`openraft::Raft::stream_append`] in the order they
//! arrived, and answers them in that order.
//!
//! Delay: both sides hand each frame to a writer task stamped with
//! `now + one-way delay` for the pair of nodes, and the writer holds it until
//! then. The delay is the same for every frame on a connection, so frames keep
//! their order, and a frame's delay does not hold back the frames queued after
//! it beyond their own send time. A request and its answer together take the
//! table's round-trip time plus the real time on the wire.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use openraft::alias::SnapshotMetaOf;
use openraft::base::{BoxFuture, BoxStream};
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, StreamAppendResult,
    TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::{DelayTable, Error, NodeId, PeerStats, Proposed, Status, TypeConfig, Vote};

const MAX_FRAME: usize = 1 << 30;
const TIMER_LATENESS: Duration = Duration::from_millis(1);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Hello {
    pub from: Option<NodeId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// One AppendEntries of the replication stream `stream` on this
    /// connection.
    StreamAppend {
        stream: u64,
        req: AppendEntriesRequest<TypeConfig>,
    },
    /// The leader sends no more requests on replication stream `stream`.
    StreamEnd {
        stream: u64,
    },
    Vote(VoteRequest<TypeConfig>),
    PreVote(VoteRequest<TypeConfig>),
    TransferLeader(TransferLeaderRequest<TypeConfig>),
    FullSnapshot {
        vote: Vote,
        meta: SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
    },
    /// Run on the receiving node, which must be the leader.
    AtLeader(LeaderOp),
    /// Run on the leader; the receiving node forwards it if needed.
    Client(LeaderOp),
    Initialize(BTreeMap<NodeId, String>),
    Status,
    WaitApplied {
        index: u64,
        timeout_ms: u64,
    },
    LinearizableRead,
    Ping,
    /// Measure the round trip from the receiving node to another node.
    PingPeer(NodeId),
    /// Passed to the node's application handler.
    App(Vec<u8>),
}

/// Requests that only the leader can carry out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum LeaderOp {
    /// One group, to go into one entry in this order.
    Propose(Vec<Vec<u8>>),
    ReadIndex,
    AddFollower {
        id: NodeId,
        addr: String,
    },
    ChangeVoters(BTreeSet<NodeId>),
    TransferLeader(NodeId),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Response {
    /// The answer to an AppendEntries. A conflict loses openraft's conflict
    /// hint on the way, so the leader searches for the matching entry itself.
    Append(StreamAppendResult<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
    TransferLeader(TransferLeaderResponse<TypeConfig>),
    Snapshot(SnapshotResponse<TypeConfig>),
    Proposed(Vec<Proposed>),
    Index(u64),
    Status(Box<Status>),
    Rtt {
        micros: u64,
    },
    App(Vec<u8>),
    Done,
}

pub(crate) type Reply = Result<Response, Error>;

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let len = bincode::serialized_size(value).expect("frame serializes") as usize;
    let mut out = Vec::with_capacity(4 + len);
    out.extend_from_slice(&(len as u32).to_le_bytes());
    bincode::serialize_into(&mut out, value).expect("frame serializes");
    out
}

pub(crate) async fn read_frame<T: DeserializeOwned>(
    r: &mut (impl AsyncRead + Unpin),
) -> io::Result<T> {
    let len = r.read_u32_le().await? as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes"),
        ));
    }
    let mut buf = vec![0; len];
    r.read_exact(&mut buf).await?;
    bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Sends frames through a writer task that holds each one for `delay`.
#[derive(Clone)]
pub(crate) struct DelayedSender {
    tx: mpsc::UnboundedSender<(Instant, Vec<u8>)>,
    delay: Duration,
}

impl DelayedSender {
    /// Returns false if the connection is closed.
    pub fn send(&self, frame: Vec<u8>) -> bool {
        self.tx.send((Instant::now() + self.delay, frame)).is_ok()
    }
}

pub(crate) fn spawn_writer(
    w: OwnedWriteHalf,
    delay: Duration,
    closed: Arc<AtomicBool>,
) -> DelayedSender {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        if let Err(e) = write_frames(BufWriter::new(w), rx).await {
            tracing::debug!("connection writer stopped: {e}");
        }
        closed.store(true, Ordering::SeqCst);
    });
    DelayedSender { tx, delay }
}

async fn write_frames(
    mut w: BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::UnboundedReceiver<(Instant, Vec<u8>)>,
) -> io::Result<()> {
    loop {
        let (at, frame) = match rx.try_recv() {
            Ok(item) => item,
            Err(_) => {
                w.flush().await?;
                match rx.recv().await {
                    Some(item) => item,
                    None => return Ok(()),
                }
            }
        };
        if at > Instant::now() {
            w.flush().await?;
            // tokio's timer fires on the first whole millisecond tick after the
            // deadline, about 1 ms late on average; aim 1 ms early to cancel it.
            tokio::time::sleep_until(at - TIMER_LATENESS).await;
        }
        w.write_all(&frame).await?;
    }
}

/// The node's connections to other nodes, one per target, shared by
/// openraft's replication, its elections, and forwarded requests.
pub(crate) struct Network {
    id: NodeId,
    delays: DelayTable,
    /// The heartbeat interval. A replication stream waits this long, plus
    /// twice the round trip to the target, for the answer to one
    /// AppendEntries before it gives up on the stream.
    heartbeat: Duration,
    peers: Mutex<HashMap<NodeId, Arc<Peer>>>,
}

impl Network {
    pub fn new(id: NodeId, delays: DelayTable, heartbeat: Duration) -> Self {
        Network {
            id,
            delays,
            heartbeat,
            peers: Mutex::new(HashMap::new()),
        }
    }

    pub fn peer(&self, target: NodeId, addr: &str) -> Arc<Peer> {
        let mut peers = self.peers.lock().unwrap();
        if let Some(p) = peers.get(&target) {
            if p.addr == addr {
                return p.clone();
            }
        }
        let p = Arc::new(Peer::new(
            addr.to_string(),
            Some(self.id),
            self.delays.one_way(self.id, target),
        ));
        p.stats.lock().unwrap().stats.target = target;
        peers.insert(target, p.clone());
        p
    }

    pub fn stats(&self) -> Vec<PeerStats> {
        let peers = self.peers.lock().unwrap();
        let mut out: Vec<PeerStats> = peers
            .values()
            .map(|p| p.stats.lock().unwrap().stats.clone())
            .collect();
        out.sort_by_key(|s| s.target);
        out
    }
}

/// One TCP connection to an address, opened on first use and again after it
/// fails.
pub(crate) struct Peer {
    addr: String,
    from: Option<NodeId>,
    delay: Duration,
    conn: tokio::sync::Mutex<Option<Arc<Conn>>>,
    stats: Mutex<AppendStats>,
}

#[derive(Default)]
struct AppendStats {
    stats: PeerStats,
    in_flight: u64,
}

struct Conn {
    out: DelayedSender,
    pending: Mutex<HashMap<u64, oneshot::Sender<Reply>>>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
}

impl Conn {
    /// Sends `req` and returns the receiver of its answer, or `None` if the
    /// connection is closed and the request was not sent.
    fn send(&self, req: &Request) -> Option<oneshot::Receiver<Reply>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if self.closed.load(Ordering::SeqCst) || !self.out.send(encode(&(id, req))) {
            self.pending.lock().unwrap().remove(&id);
            return None;
        }
        Some(rx)
    }
}

impl Peer {
    pub fn new(addr: String, from: Option<NodeId>, delay: Duration) -> Self {
        Peer {
            addr,
            from,
            delay,
            conn: tokio::sync::Mutex::new(None),
            stats: Mutex::new(AppendStats::default()),
        }
    }

    /// Sends `req` and waits for its answer. [`Error::Unreachable`] means the
    /// request was not sent.
    pub async fn call(&self, req: &Request, timeout: Duration) -> Reply {
        let deadline = Instant::now() + timeout;
        let conn = tokio::time::timeout_at(deadline, self.conn())
            .await
            .map_err(|_| Error::Unreachable(format!("connecting to {} timed out", self.addr)))??;
        let Some(rx) = conn.send(req) else {
            return Err(Error::Unreachable(format!(
                "connection to {} is closed",
                self.addr
            )));
        };
        self.await_reply(rx, deadline, timeout).await
    }

    async fn await_reply(
        &self,
        rx: oneshot::Receiver<Reply>,
        deadline: Instant,
        timeout: Duration,
    ) -> Reply {
        match tokio::time::timeout_at(deadline, rx).await {
            Err(_) => Err(Error::Timeout(format!(
                "no answer from {} in {timeout:?}",
                self.addr
            ))),
            Ok(Err(_)) => Err(Error::OutcomeUnknown(format!(
                "connection to {} lost",
                self.addr
            ))),
            Ok(Ok(reply)) => reply,
        }
    }

    async fn conn(&self) -> Result<Arc<Conn>, Error> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref() {
            if !c.closed.load(Ordering::SeqCst) {
                return Ok(c.clone());
            }
        }
        let unreachable = |e: io::Error| Error::Unreachable(format!("{}: {e}", self.addr));
        let stream = TcpStream::connect(&self.addr).await.map_err(unreachable)?;
        stream.set_nodelay(true).map_err(unreachable)?;
        let (r, w) = stream.into_split();
        let closed = Arc::new(AtomicBool::new(false));
        let out = spawn_writer(w, self.delay, closed.clone());
        out.send(encode(&Hello { from: self.from }));
        let conn = Arc::new(Conn {
            out,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            closed,
        });
        tokio::spawn(read_replies(
            tokio::io::BufReader::new(r),
            Arc::downgrade(&conn),
        ));
        *slot = Some(conn.clone());
        Ok(conn)
    }

    fn begin_append(self: &Arc<Self>, entries: u64) -> InFlight {
        let mut s = self.stats.lock().unwrap();
        s.in_flight += 1;
        let in_flight = s.in_flight;
        let st = &mut s.stats;
        st.append_requests += 1;
        if entries > 0 {
            st.append_requests_with_entries += 1;
        }
        st.entries_sent += entries;
        st.max_entries_per_request = st.max_entries_per_request.max(entries);
        st.max_in_flight = st.max_in_flight.max(in_flight);
        InFlight(self.clone())
    }
}

/// One AppendEntries waiting for its answer; counted in [`PeerStats`].
struct InFlight(Arc<Peer>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.stats.lock().unwrap().in_flight -= 1;
    }
}

async fn read_replies(mut r: tokio::io::BufReader<OwnedReadHalf>, conn: Weak<Conn>) {
    loop {
        let res = read_frame::<(u64, Reply)>(&mut r).await;
        let Some(c) = conn.upgrade() else { return };
        match res {
            Ok((id, reply)) => {
                if let Some(tx) = c.pending.lock().unwrap().remove(&id) {
                    let _ = tx.send(reply);
                }
            }
            Err(e) => {
                tracing::debug!("connection reader stopped: {e}");
                c.closed.store(true, Ordering::SeqCst);
                c.pending.lock().unwrap().clear();
                return;
            }
        }
    }
}

pub(crate) struct NetworkFactory(pub Arc<Network>);

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> PeerClient {
        let net = &self.0;
        PeerClient {
            peer: net.peer(target, &node.addr),
            append_timeout: net.heartbeat + 2 * net.delays.rtt(net.id, target),
        }
    }
}

pub(crate) struct PeerClient {
    peer: Arc<Peer>,
    append_timeout: Duration,
}

type RpcResult<T> = Result<T, RPCError<TypeConfig>>;

impl RaftNetworkV2<TypeConfig> for PeerClient {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendResult<TypeConfig>> {
        let _in_flight = self.peer.begin_append(rpc.entries.len() as u64);
        let res = self
            .peer
            .call(&Request::AppendEntries(rpc), option.hard_ttl())
            .await;
        match res.map_err(rpc_error)? {
            Response::Append(r) => Ok(r.map_err(Into::into)),
            other => Err(rpc_error(unexpected(other))),
        }
    }

    /// Sends every request of `input` at once and yields the answers in
    /// order. Each answer must come within the append timeout of its
    /// request; openraft ends the stream at the first error or conflict.
    fn stream_append<'s, S>(
        &'s mut self,
        mut input: S,
        _option: RPCOption,
    ) -> BoxFuture<'s, RpcResult<BoxStream<'s, RpcResult<AppendResult<TypeConfig>>>>>
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        let peer = self.peer.clone();
        let timeout = self.append_timeout;
        Box::pin(async move {
            let conn = peer.conn().await.map_err(rpc_error)?;
            let stream = conn.next_id.fetch_add(1, Ordering::Relaxed);
            let (sent_tx, sent_rx) = mpsc::unbounded_channel();
            let sender_peer = peer.clone();
            tokio::spawn(async move {
                while let Some(req) = input.next().await {
                    let in_flight = sender_peer.begin_append(req.entries.len() as u64);
                    let rx = conn.send(&Request::StreamAppend { stream, req });
                    let Some(rx) = rx else {
                        let _ = sent_tx.send(None);
                        return;
                    };
                    let deadline = Instant::now() + timeout;
                    if sent_tx.send(Some((rx, deadline, in_flight))).is_err() {
                        break;
                    }
                }
                conn.send(&Request::StreamEnd { stream });
            });
            let answers = futures_util::stream::unfold(
                (sent_rx, peer),
                move |(mut sent_rx, peer)| async move {
                    let res = match sent_rx.recv().await? {
                        None => Err(Error::Unreachable(format!(
                            "connection to {} is closed",
                            peer.addr
                        ))),
                        Some((rx, deadline, _in_flight)) => {
                            peer.await_reply(rx, deadline, timeout).await
                        }
                    };
                    let res = match res.map_err(rpc_error) {
                        Ok(Response::Append(r)) => Ok(r.map_err(Into::into)),
                        Ok(other) => Err(rpc_error(unexpected(other))),
                        Err(e) => Err(e),
                    };
                    Some((res, (sent_rx, peer)))
                },
            );
            Ok(Box::pin(answers) as BoxStream<'s, _>)
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<TypeConfig>> {
        self.vote_call(Request::Vote(rpc), option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<TypeConfig>> {
        self.vote_call(Request::PreVote(rpc), option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: openraft::alias::SnapshotOf<TypeConfig, Vec<u8>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let req = Request::FullSnapshot {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot,
        };
        let res = tokio::select! {
            closed = cancel => return Err(StreamingError::Closed(closed)),
            res = self.peer.call(&req, option.hard_ttl()) => res,
        };
        let streaming_error = |e: Error| match e {
            Error::Unreachable(_) => StreamingError::Unreachable(Unreachable::new(&e)),
            _ => StreamingError::Network(NetworkError::new(&e)),
        };
        match res.map_err(streaming_error)? {
            Response::Snapshot(r) => Ok(r),
            other => Err(streaming_error(unexpected(other))),
        }
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<TransferLeaderResponse<TypeConfig>> {
        let res = self
            .peer
            .call(&Request::TransferLeader(req), option.hard_ttl())
            .await;
        match res.map_err(rpc_error)? {
            Response::TransferLeader(r) => Ok(r),
            other => Err(rpc_error(unexpected(other))),
        }
    }
}

impl PeerClient {
    async fn vote_call(
        &mut self,
        req: Request,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<TypeConfig>> {
        match self
            .peer
            .call(&req, option.hard_ttl())
            .await
            .map_err(rpc_error)?
        {
            Response::Vote(r) => Ok(r),
            other => Err(rpc_error(unexpected(other))),
        }
    }
}

fn rpc_error(e: Error) -> RPCError<TypeConfig> {
    match e {
        Error::Unreachable(_) => RPCError::Unreachable(Unreachable::new(&e)),
        _ => RPCError::Network(NetworkError::new(&e)),
    }
}

pub(crate) fn unexpected(r: Response) -> Error {
    Error::Raft(format!("unexpected response {r:?}"))
}

/// The receiving side of the replication streams of one connection.
#[derive(Default)]
pub(crate) struct InboundStreams {
    streams: HashMap<u64, Arc<Mutex<InboundStream>>>,
}

struct InboundStream {
    /// Request ids sent to openraft and not answered yet, in order.
    ids: VecDeque<u64>,
    /// `None` once the leader ended the stream.
    tx: Option<mpsc::UnboundedSender<AppendEntriesRequest<TypeConfig>>>,
    /// openraft ended the stream; later requests of it are refused.
    closed: bool,
}

impl InboundStreams {
    /// Hands request `id` of `stream` to openraft, starting the stream on its
    /// first request. Answers go to `out` in the order of the requests.
    pub fn push(
        &mut self,
        raft: &crate::Raft,
        out: &DelayedSender,
        stream: u64,
        id: u64,
        req: AppendEntriesRequest<TypeConfig>,
    ) {
        let state = self
            .streams
            .entry(stream)
            .or_insert_with(|| start_inbound_stream(raft.clone(), out.clone()));
        let mut st = state.lock().unwrap();
        if st.closed {
            let reply: Reply = Err(Error::Raft("replication stream ended".into()));
            out.send(encode(&(id, reply)));
            return;
        }
        st.ids.push_back(id);
        if let Some(tx) = &st.tx {
            let _ = tx.send(req);
        }
    }

    /// Ends the input of `stream`; openraft still answers what it received.
    pub fn end(&mut self, stream: u64) {
        if let Some(state) = self.streams.remove(&stream) {
            state.lock().unwrap().tx = None;
        }
    }
}

fn start_inbound_stream(raft: crate::Raft, out: DelayedSender) -> Arc<Mutex<InboundStream>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(Mutex::new(InboundStream {
        ids: VecDeque::new(),
        tx: Some(tx),
        closed: false,
    }));
    let task_state = state.clone();
    tokio::spawn(async move {
        let input = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx));
        let mut answers = std::pin::pin!(raft.stream_append(input));
        while let Some(res) = answers.next().await {
            let id = task_state.lock().unwrap().ids.pop_front();
            let id = id.expect("an answer for every request");
            let (reply, last): (Reply, bool) = match res {
                Ok(r) => {
                    let last = r.is_err();
                    (Ok(Response::Append(r.map_err(Into::into))), last)
                }
                Err(fatal) => (Err(Error::Raft(fatal.to_string())), true),
            };
            out.send(encode(&(id, reply)));
            if last {
                break;
            }
        }
        let mut st = task_state.lock().unwrap();
        st.closed = true;
        for id in st.ids.drain(..) {
            let reply: Reply = Err(Error::Raft("replication stream ended".into()));
            out.send(encode(&(id, reply)));
        }
    });
    state
}
