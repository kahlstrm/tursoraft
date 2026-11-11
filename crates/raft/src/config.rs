//! Configuration of one node and of a whole cluster.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// Everything [`crate::Node::start`] needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Address this node listens on, for other nodes and for clients.
    pub listen: String,
    /// Directory that holds this node's Raft log and vote.
    pub dir: PathBuf,
    /// Addresses of the other nodes. Used by [`crate::Node::initialize`];
    /// after that, openraft's membership holds the addresses.
    pub peers: BTreeMap<NodeId, String>,
    pub delays: DelayTable,
    pub timeouts: RaftTimeouts,
    /// `None` turns batching off: every proposal becomes its own log entry.
    pub batching: Option<Batching>,
    pub log: LogConfig,
    /// How long [`crate::Node::propose`] waits for a leader to appear and
    /// for a forwarded proposal, in milliseconds.
    pub propose_timeout_ms: u64,
}

/// Round-trip times between pairs of nodes that the network layer adds to
/// messages. Each message between `a` and `b` is held for half the round trip
/// before it is written to the socket, in both directions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DelayTable {
    /// Round trip for pairs not listed in `pairs`.
    #[serde(default)]
    pub default_rtt_ms: f64,
    #[serde(default)]
    pub pairs: Vec<PairRtt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairRtt {
    pub a: NodeId,
    pub b: NodeId,
    pub rtt_ms: f64,
}

impl DelayTable {
    /// The same round trip between every pair of nodes.
    pub fn uniform(rtt_ms: f64) -> Self {
        DelayTable {
            default_rtt_ms: rtt_ms,
            pairs: Vec::new(),
        }
    }

    pub fn rtt(&self, a: NodeId, b: NodeId) -> Duration {
        if a == b {
            return Duration::ZERO;
        }
        let ms = self
            .pairs
            .iter()
            .find(|p| (p.a, p.b) == (a, b) || (p.a, p.b) == (b, a))
            .map_or(self.default_rtt_ms, |p| p.rtt_ms);
        Duration::from_secs_f64(ms / 1000.0)
    }

    pub fn one_way(&self, a: NodeId, b: NodeId) -> Duration {
        self.rtt(a, b) / 2
    }

    /// The largest round trip in the table, for [`RaftTimeouts::for_max_rtt`].
    pub fn max_rtt(&self) -> Duration {
        let max_ms = self
            .pairs
            .iter()
            .map(|p| p.rtt_ms)
            .fold(self.default_rtt_ms, f64::max);
        Duration::from_secs_f64(max_ms / 1000.0)
    }
}

/// openraft's timing settings, in milliseconds.
///
/// A heartbeat must come back within `heartbeat_ms`, so it must be longer
/// than the round trip to the farthest voter. Replication to a node may take
/// `heartbeat_ms` plus twice that node's round trip in the delay table, so a
/// far non-voting follower does not need a longer heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftTimeouts {
    pub heartbeat_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Timeout for sending and installing one snapshot.
    pub install_snapshot_ms: u64,
}

impl RaftTimeouts {
    /// Settings that grow with the largest round trip between nodes:
    /// heartbeat 2 × RTT (at least 50 ms), election timeout 3 to 6 heartbeats.
    pub fn for_max_rtt(max_rtt: Duration) -> Self {
        let rtt_ms = max_rtt.as_millis() as u64;
        let heartbeat_ms = (2 * rtt_ms).max(50);
        RaftTimeouts {
            heartbeat_ms,
            election_timeout_min_ms: 3 * heartbeat_ms,
            election_timeout_max_ms: 6 * heartbeat_ms,
            install_snapshot_ms: (10 * rtt_ms).max(10_000),
        }
    }
}

impl Default for RaftTimeouts {
    fn default() -> Self {
        RaftTimeouts::for_max_rtt(Duration::ZERO)
    }
}

/// Puts proposals from concurrent callers into one log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batching {
    /// Most proposals in one entry.
    pub max_proposals: usize,
    /// An entry is closed once its proposals reach this many bytes.
    pub max_bytes: usize,
    /// Most entries this node has proposed and not yet seen applied. While
    /// that many are in flight, new proposals wait and join the next entry.
    pub max_entries_in_flight: usize,
}

impl Default for Batching {
    fn default() -> Self {
        Batching {
            max_proposals: 4096,
            max_bytes: 8 << 20,
            max_entries_in_flight: 2,
        }
    }
}

/// Log storage and compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Start a new segment file once the current one is this large.
    pub segment_bytes: u64,
    /// Build a snapshot after this many entries since the last one. The log
    /// before a snapshot is then removed, except `keep_after_snapshot` entries.
    pub snapshot_every: u64,
    pub keep_after_snapshot: u64,
    /// Most entries in one AppendEntries request.
    pub max_payload_entries: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            segment_bytes: 64 << 20,
            snapshot_every: 5000,
            keep_after_snapshot: 1000,
            max_payload_entries: 300,
        }
    }
}

/// A cluster description shared by all node processes of a test or benchmark,
/// read by the `tursoraft-raft-node` binary from a JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Listen address of every node, voters and non-voting followers.
    pub nodes: BTreeMap<NodeId, String>,
    #[serde(default)]
    pub delays: DelayTable,
    /// Defaults to [`RaftTimeouts::for_max_rtt`] of the delay table.
    #[serde(default)]
    pub timeouts: Option<RaftTimeouts>,
    #[serde(default)]
    pub batching: Option<Batching>,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default = "default_propose_timeout_ms")]
    pub propose_timeout_ms: u64,
}

fn default_propose_timeout_ms() -> u64 {
    10_000
}

impl ClusterConfig {
    /// The configuration of node `id`, or `None` if `nodes` does not list it.
    pub fn node_config(&self, id: NodeId, dir: PathBuf) -> Option<NodeConfig> {
        let listen = self.nodes.get(&id)?.clone();
        let peers = self
            .nodes
            .iter()
            .filter(|(n, _)| **n != id)
            .map(|(n, a)| (*n, a.clone()))
            .collect();
        Some(NodeConfig {
            id,
            listen,
            dir,
            peers,
            delays: self.delays.clone(),
            timeouts: self
                .timeouts
                .clone()
                .unwrap_or_else(|| RaftTimeouts::for_max_rtt(self.delays.max_rtt())),
            batching: self.batching.clone(),
            log: self.log.clone(),
            propose_timeout_ms: self.propose_timeout_ms,
        })
    }
}
