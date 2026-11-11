//! Runs a cluster of `tursoraft-raft-node` processes, one per node.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tursoraft_raft::{
    Client, ClusterConfig, CountingStats, DelayTable, Error, LogConfig, NodeId, Proposed, Status,
};

pub struct Cluster {
    pub dir: TempDir,
    pub config: ClusterConfig,
    config_path: PathBuf,
    args: Vec<String>,
    procs: BTreeMap<NodeId, Child>,
}

impl Cluster {
    /// A cluster description for nodes `ids`, each on a free local port.
    /// `change` may adjust the configuration before it is written.
    pub fn new(ids: &[NodeId], args: &[&str], change: impl FnOnce(&mut ClusterConfig)) -> Cluster {
        let dir = TempDir::new().unwrap();
        let nodes = ids
            .iter()
            .map(|id| {
                let port = free_port();
                (*id, format!("127.0.0.1:{port}"))
            })
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
        let config_path = dir.path().join("cluster.json");
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        eprintln!("cluster files in {}", dir.path().display());
        Cluster {
            dir,
            config,
            config_path,
            args: args.iter().map(|s| s.to_string()).collect(),
            procs: BTreeMap::new(),
        }
    }

    pub fn addr(&self, id: NodeId) -> String {
        self.config.nodes[&id].clone()
    }

    pub fn node_dir(&self, id: NodeId) -> PathBuf {
        self.dir.path().join(format!("node-{id}"))
    }

    /// Starts node `id` and waits until it listens. Reuses its directory, so
    /// a restarted node continues from its own disk.
    pub fn start(&mut self, id: NodeId) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.path().join(format!("node-{id}.log")))
            .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_tursoraft-raft-node"))
            .arg("--config")
            .arg(&self.config_path)
            .arg("--id")
            .arg(id.to_string())
            .arg("--dir")
            .arg(self.node_dir(id))
            .args(&self.args)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()),
            )
            .stdout(Stdio::piped())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert!(
            line.starts_with("listening"),
            "node {id} did not start: {line:?}; its log: {}",
            std::fs::read_to_string(self.dir.path().join(format!("node-{id}.log")))
                .unwrap_or_default()
        );
        self.procs.insert(id, child);
    }

    pub fn start_all(&mut self) {
        let ids: Vec<NodeId> = self.config.nodes.keys().copied().collect();
        for id in ids {
            self.start(id);
        }
    }

    /// Kills node `id` with SIGKILL.
    pub fn kill(&mut self, id: NodeId) {
        let mut child = self.procs.remove(&id).expect("node is running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    pub fn signal(&self, id: NodeId, signal: &str) {
        let pid = self.procs[&id].id().to_string();
        assert!(Command::new("kill")
            .args([signal, &pid])
            .status()
            .unwrap()
            .success());
    }

    pub fn client(&self, id: NodeId) -> Client {
        Client::new(self.addr(id))
    }

    /// Forms the cluster with these voters through node `voters[0]` and
    /// returns the leader.
    pub async fn initialize(&self, voters: &[NodeId]) -> NodeId {
        let members = voters.iter().map(|id| (*id, self.addr(*id))).collect();
        self.client(voters[0]).initialize(members).await.unwrap();
        self.wait_leader(voters).await
    }

    /// Waits until all of `ids` name the same leader, and returns it.
    pub async fn wait_leader(&self, ids: &[NodeId]) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut leaders = BTreeSet::new();
            for id in ids {
                leaders.insert(self.client(*id).status().await.ok().and_then(|s| s.leader));
            }
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                if ids.contains(&leader) {
                    return leader;
                }
            }
            assert!(Instant::now() < deadline, "no common leader among {ids:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn status(&self, id: NodeId) -> Status {
        self.client(id).status().await.unwrap()
    }

    pub async fn stats(&self, id: NodeId) -> CountingStats {
        serde_json::from_slice(&self.client(id).app(vec![]).await.unwrap()).unwrap()
    }

    pub async fn snapshots_installed(&self, id: NodeId) -> u64 {
        let v: serde_json::Value =
            serde_json::from_slice(&self.client(id).app(vec![]).await.unwrap()).unwrap();
        v["snapshots_installed"].as_u64().unwrap()
    }

    /// Waits until each of `ids` has applied `index`.
    pub async fn wait_applied(&self, ids: &[NodeId], index: u64) {
        for id in ids {
            self.client(*id)
                .wait_applied(index, Duration::from_secs(30))
                .await
                .unwrap_or_else(|e| panic!("node {id} did not apply {index}: {e}"));
        }
    }

    /// Waits until each of `ids` has applied everything `leader` has, and
    /// returns that index.
    pub async fn converge(&self, leader: NodeId, ids: &[NodeId]) -> u64 {
        let index = self.status(leader).await.last_log_index.unwrap();
        self.wait_applied(ids, index).await;
        index
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.procs.values_mut() {
            let _ = Command::new("kill")
                .args(["-CONT", &child.id().to_string()])
                .status();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Proposes `n` proposals one after another, and returns the time each took.
pub async fn sequential(client: &Client, prefix: &str, n: usize) -> Vec<Duration> {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        let p = client
            .propose(format!("{prefix}-{i}").into_bytes())
            .await
            .unwrap();
        times.push(start.elapsed());
        assert_eq!(p.outcome, tursoraft_raft::Outcome::Applied);
    }
    times
}

/// Runs `callers` concurrent proposals through `client`, one each.
pub async fn concurrent(
    client: &std::sync::Arc<Client>,
    proposals: Vec<Vec<u8>>,
) -> Vec<(Vec<u8>, Result<Proposed, Error>)> {
    let tasks: Vec<_> = proposals
        .into_iter()
        .map(|p| {
            let client = client.clone();
            tokio::spawn(async move {
                let res = client.propose(p.clone()).await;
                (p, res)
            })
        })
        .collect();
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.unwrap());
    }
    out
}

pub fn median(times: &[Duration]) -> Duration {
    let mut t = times.to_vec();
    t.sort();
    t[t.len() / 2]
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// A free port below the kernel's range for outgoing connections
/// (32768-60999 here), so that no connection made meanwhile takes it before
/// the node binds it.
pub fn free_port() -> u16 {
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
