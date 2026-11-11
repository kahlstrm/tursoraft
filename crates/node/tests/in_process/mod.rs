//! A cluster of nodes in this process, so that tests can subscribe to them.
//!
//! Each node runs on its own tokio runtime, and stopping a node drops its
//! runtime, as a process exit would.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use tursoraft_node::{Node, Options};
use tursoraft_raft::{ClusterConfig, DelayTable, LogConfig, NodeId};

pub struct Cluster {
    pub dir: TempDir,
    pub config: ClusterConfig,
    pub options: Options,
    pub nodes: BTreeMap<NodeId, Node>,
    runtimes: BTreeMap<NodeId, tokio::runtime::Runtime>,
}

impl Cluster {
    /// A cluster description for nodes `ids`, each on a free local port.
    pub fn new(ids: &[NodeId], change: impl FnOnce(&mut ClusterConfig)) -> Cluster {
        let nodes = ids
            .iter()
            .map(|id| (*id, format!("127.0.0.1:{}", free_port())))
            .collect();
        let mut config = ClusterConfig {
            nodes,
            delays: DelayTable::default(),
            timeouts: None,
            batching: None,
            log: LogConfig::default(),
            propose_timeout_ms: 10_000,
        };
        change(&mut config);
        Cluster {
            dir: TempDir::new().unwrap(),
            config,
            options: Options::default(),
            nodes: BTreeMap::new(),
            runtimes: BTreeMap::new(),
        }
    }

    fn node_dir(&self, id: NodeId) -> PathBuf {
        self.dir.path().join(format!("node-{id}"))
    }

    /// Starts node `id`, reusing its files if it ran before.
    pub async fn start(&mut self, id: NodeId) -> Node {
        let dir = self.node_dir(id);
        std::fs::create_dir_all(&dir).unwrap();
        let config = self.config.node_config(id, dir.join("raft")).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let options = self.options.clone();
        let node = runtime
            .spawn(async move { Node::start_with(config, dir.join("data.db"), options).await })
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("node {id} did not start: {e}"));
        self.nodes.insert(id, node.clone());
        self.runtimes.insert(id, runtime);
        node
    }

    pub async fn start_all(&mut self) {
        let ids: Vec<NodeId> = self.config.nodes.keys().copied().collect();
        for id in ids {
            self.start(id).await;
        }
    }

    pub async fn stop(&mut self, id: NodeId) {
        let node = self.nodes.remove(&id).expect("node is running");
        node.shutdown().await;
        self.runtimes.remove(&id).unwrap().shutdown_background();
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[&id]
    }

    /// Forms the cluster with these voters and returns the leader.
    pub async fn initialize(&self, voters: &[NodeId]) -> NodeId {
        let members = voters
            .iter()
            .map(|id| (*id, self.config.nodes[id].clone()))
            .collect();
        self.node(voters[0])
            .raft()
            .initialize(members)
            .await
            .unwrap();
        self.wait_leader(voters).await
    }

    pub async fn add_follower(&self, leader: NodeId, id: NodeId) {
        self.node(leader)
            .raft()
            .add_follower(id, self.config.nodes[&id].clone())
            .await
            .unwrap();
    }

    /// Waits until all of `ids` name the same leader among them.
    pub async fn wait_leader(&self, ids: &[NodeId]) -> NodeId {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let leaders: Vec<Option<NodeId>> = ids
                .iter()
                .map(|id| self.node(*id).raft().leader())
                .collect();
            if let Some(Some(leader)) = leaders.first() {
                if ids.contains(leader) && leaders.iter().all(|l| l == &Some(*leader)) {
                    return *leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no common leader among {ids:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until each of `ids` has applied everything `leader` has.
    pub async fn converge(&self, leader: NodeId, ids: &[NodeId]) -> u64 {
        let index = self.node(leader).raft().status().last_log_index.unwrap();
        for id in ids {
            self.node(*id)
                .raft()
                .wait_applied(index, Duration::from_secs(30))
                .await
                .unwrap_or_else(|e| panic!("node {id} did not apply {index}: {e}"));
        }
        index
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, runtime) in std::mem::take(&mut self.runtimes) {
            runtime.shutdown_background();
        }
    }
}

/// A free port below the kernel's range for outgoing connections.
pub fn free_port() -> u16 {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let n = std::process::id() * 37 + NEXT.fetch_add(1, Ordering::Relaxed);
        let port = 20_000 + (n % 12_000) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}
