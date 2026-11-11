//! A cluster of node processes over real TCP on this machine.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tursoraft_node::{Client, ReadLevel, Statement, Value};
use tursoraft_raft::{ClusterConfig, DelayTable, LogConfig, NodeId, Status};

pub struct Cluster {
    pub dir: TempDir,
    pub config: ClusterConfig,
    config_path: PathBuf,
    bin: PathBuf,
    /// Extra arguments of every node process, such as
    /// `--max-pending-groups 1`.
    pub node_args: Vec<String>,
    procs: BTreeMap<NodeId, Child>,
}

impl Cluster {
    /// A cluster description for nodes `ids`, each on a free local port. `bin`
    /// is the `tursoraft-proof` binary, which runs a node with `node`.
    pub fn new(
        bin: impl Into<PathBuf>,
        ids: &[NodeId],
        change: impl FnOnce(&mut ClusterConfig),
    ) -> Cluster {
        let dir = TempDir::new().unwrap();
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
        let config_path = dir.path().join("cluster.json");
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        Cluster {
            dir,
            config,
            config_path,
            bin: bin.into(),
            node_args: Vec::new(),
            procs: BTreeMap::new(),
        }
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.config.nodes.keys().copied().collect()
    }

    pub fn addr(&self, id: NodeId) -> String {
        self.config.nodes[&id].clone()
    }

    pub fn running(&self) -> Vec<NodeId> {
        self.procs.keys().copied().collect()
    }

    /// Starts node `id` and waits until it listens. Reuses its directory.
    pub fn start(&mut self, id: NodeId) {
        let log_path = self.dir.path().join(format!("node-{id}.log"));
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap();
        let mut child = Command::new(&self.bin)
            .arg("node")
            .arg("--config")
            .arg(&self.config_path)
            .arg("--id")
            .arg(id.to_string())
            .arg("--dir")
            .arg(self.dir.path().join(format!("node-{id}")))
            .args(&self.node_args)
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
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        self.procs.insert(id, child);
    }

    pub fn start_all(&mut self) {
        for id in self.ids() {
            self.start(id);
        }
    }

    /// Kills node `id` with SIGKILL and waits until it is gone.
    pub fn kill(&mut self, id: NodeId) {
        let mut child = self.procs.remove(&id).expect("node is running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Sends SIGSTOP to node `id`.
    pub fn pause(&self, id: NodeId) {
        self.signal(id, "-STOP");
    }

    /// Sends SIGCONT to node `id`.
    pub fn resume(&self, id: NodeId) {
        self.signal(id, "-CONT");
    }

    fn signal(&self, id: NodeId, signal: &str) {
        let pid = self.procs[&id].id();
        let status = Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill {signal} {pid} failed");
    }

    pub fn client(&self, id: NodeId) -> Client {
        Client::new(self.addr(id))
    }

    /// Forms the cluster with these voters and returns the leader.
    pub async fn initialize(&self, voters: &[NodeId]) -> NodeId {
        let members = voters.iter().map(|id| (*id, self.addr(*id))).collect();
        self.client(voters[0])
            .raft()
            .initialize(members)
            .await
            .unwrap();
        self.wait_leader(voters).await
    }

    /// Waits until all of `ids` name the same leader among them.
    pub async fn wait_leader(&self, ids: &[NodeId]) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut leaders = BTreeSet::new();
            for id in ids {
                let client = self.client(*id).with_timeout(Duration::from_secs(2));
                let status = client.raft().status().await;
                leaders.insert(status.ok().and_then(|s| s.leader));
            }
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                if ids.contains(&leader) {
                    return leader;
                }
            }
            assert!(Instant::now() < deadline, "no common leader among {ids:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn status(&self, id: NodeId) -> Status {
        self.client(id).raft().status().await.unwrap()
    }

    /// Waits until each of `ids` has applied everything `leader` has, and
    /// returns that index.
    pub async fn converge(&self, leader: NodeId, ids: &[NodeId]) -> u64 {
        let index = self.status(leader).await.last_log_index.unwrap();
        for id in ids {
            self.client(*id)
                .raft()
                .wait_applied(index, Duration::from_secs(60))
                .await
                .unwrap_or_else(|e| panic!("node {id} did not apply {index}: {e}"));
        }
        index
    }

    /// The schema and every row of every table of node `id`, read locally.
    pub async fn dump(&self, id: NodeId) -> String {
        dump(&self.client(id)).await
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.procs.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Schema (without root pages) and every row of every table, read locally.
pub async fn dump(client: &Client) -> String {
    let schema = rows(
        client,
        "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name",
        ReadLevel::Local,
    )
    .await;
    let mut out = String::new();
    for row in &schema {
        out += &format!("{row:?}\n");
    }
    for row in &schema {
        if row[0] == Value::Text("table".into()) {
            let Value::Text(name) = &row[1] else {
                unreachable!()
            };
            let table = rows(
                client,
                &format!("SELECT * FROM \"{name}\""),
                ReadLevel::Local,
            )
            .await;
            out += &format!("{name}: {table:?}\n");
        }
    }
    out
}

pub async fn rows(client: &Client, sql: &str, level: ReadLevel) -> Vec<Vec<Value>> {
    client
        .query(Statement::from(sql), level)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows
}

/// A free port below the kernel's range for outgoing connections.
pub fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let n = std::process::id() * 41 + NEXT.fetch_add(1, Ordering::Relaxed);
        let port = 20_000 + (n % 12_000) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
