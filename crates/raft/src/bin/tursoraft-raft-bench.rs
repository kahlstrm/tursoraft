//! Benchmark of the Raft layer alone: starts a cluster of
//! `tursoraft-raft-node` processes (one per node, [`CountingStateMachine`],
//! real fdatasync), runs one job, and appends one JSON line per job to a
//! result file.
//!
//! ```text
//! cargo build --release -p tursoraft-raft --bins
//! target/release/tursoraft-raft-bench run [--out FILE] [--only TEXT]... [--jobs FILE] [--list] [--again]
//! target/release/tursoraft-raft-bench proxy --listen ADDR --to ADDR --one-way-ms MS
//! ```
//!
//! `run` runs the jobs of the built-in plan ([`plan`]), or those of `--jobs`
//! (one JSON [`Job`] per line), whose name contains every `--only` text. It
//! skips jobs whose name is already in the result file unless `--again` is
//! given, so an interrupted run continues where it stopped. `--list` prints
//! the jobs instead of running them.
//!
//! Three kinds of job:
//! - `fsync`: appends records to files in the node directory's file system
//!   and calls `fdatasync` after each, from one or more threads at once.
//! - `cell`: forms a cluster, runs writers for a warmup and then a measured
//!   window, and records time to acknowledgement, writes per second, follower
//!   lag, and AppendEntries per write.
//! - `failover`: kills the leader of a three-voter cluster with SIGKILL
//!   several times while writers run on the other voters, and records the
//!   time from the kill to the next acknowledged write.
//!
//! Delay comes from the nodes' delay table, or, with `proxy_rtt_ms`, from a
//! `proxy` process in front of every node that holds each chunk of bytes for
//! half the round trip in each direction while the delay table is zero.
//!
//! [`CountingStateMachine`]: tursoraft_raft::CountingStateMachine

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tursoraft_raft::{
    Batching, Client, ClusterConfig, DelayTable, LogConfig, NodeId, PairRtt, RaftTimeouts, Status,
};

const AWS_TABLE: &str = include_str!("../../bench/aws-7-regions.json");
const STATUS_INTERVAL: Duration = Duration::from_millis(5);
const HIST_BASE: f64 = 1.02;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Job {
    Fsync(FsyncSpec),
    Cell(CellSpec),
    Failover(FailoverSpec),
    Transfer(TransferSpec),
}

impl Job {
    fn name(&self) -> &str {
        match self {
            Job::Fsync(s) => &s.name,
            Job::Cell(s) => &s.name,
            Job::Failover(s) => &s.name,
            Job::Transfer(s) => &s.name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FsyncSpec {
    name: String,
    /// Bytes appended before each fdatasync.
    size: usize,
    /// Threads appending and syncing at once, each to its own file.
    parallel: usize,
    /// fdatasync calls per thread.
    count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CellSpec {
    name: String,
    /// The table of the report this cell belongs to.
    group: String,
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
    /// Region name of each node, for cells with a table of real distances.
    #[serde(default)]
    regions: BTreeMap<NodeId, String>,
    /// The nodes' delay table.
    delays: DelayTable,
    /// Delay by proxy processes instead: this round trip between every pair
    /// of nodes. The delay table should then be zero.
    #[serde(default)]
    proxy_rtt_ms: Option<f64>,
    /// Move the leadership to this voter before measuring.
    #[serde(default)]
    leader: Option<NodeId>,
    issue_at: IssueAt,
    size: usize,
    writers: usize,
    batching: bool,
    /// [`Batching::max_entries_in_flight`]; `None` keeps the default.
    #[serde(default)]
    max_entries_in_flight: Option<usize>,
    warmup_s: f64,
    duration_s: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IssueAt {
    Leader,
    /// The node with the largest round trip to the leader; among equals, a
    /// voter before a non-voting follower, then the highest id.
    Farthest,
}

/// Moves the leadership of a three-voter cluster back and forth with
/// `transfer_leader` while 8 writers propose at the old leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransferSpec {
    name: String,
    rtt_ms: f64,
    max_entries_in_flight: usize,
    trials: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailoverSpec {
    name: String,
    voters: usize,
    rtt_ms: f64,
    trials: usize,
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => run_command(args.collect()),
        Some("proxy") => proxy_command(args.collect()),
        _ => usage("expected a command: run or proxy"),
    }
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}");
    eprintln!("usage: tursoraft-raft-bench run [--out FILE] [--only TEXT]... [--jobs FILE] [--list] [--again]");
    eprintln!("       tursoraft-raft-bench proxy --listen ADDR --to ADDR --one-way-ms MS");
    std::process::exit(2);
}

fn run_command(args: Vec<String>) {
    let mut out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench-results/phase1c.jsonl");
    let mut only = Vec::new();
    let mut jobs_file = None;
    let mut list = false;
    let mut again = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| usage(&format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "--out" => out = PathBuf::from(value()),
            "--only" => only.push(value()),
            "--jobs" => jobs_file = Some(PathBuf::from(value())),
            "--list" => list = true,
            "--again" => again = true,
            other => usage(&format!("unknown argument {other}")),
        }
    }
    let jobs: Vec<Job> = match jobs_file {
        Some(path) => std::fs::read_to_string(&path)
            .expect("read the jobs file")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("parse a job"))
            .collect(),
        None => plan(),
    };
    let jobs: Vec<Job> = jobs
        .into_iter()
        .filter(|j| only.iter().all(|o| j.name().contains(o.as_str())))
        .collect();
    if list {
        for j in &jobs {
            println!("{}", serde_json::to_string(j).unwrap());
        }
        return;
    }
    let done: BTreeSet<String> = std::fs::read_to_string(&out)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| v["name"].as_str().map(String::from))
        .collect();
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).expect("create the result directory");
    }
    let code = git_head();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let total = jobs.len();
    for (i, job) in jobs.into_iter().enumerate() {
        if !again && done.contains(job.name()) {
            continue;
        }
        eprintln!("[{}/{total}] {}", i + 1, job.name());
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let clock = std::time::Instant::now();
        let result = match &job {
            Job::Fsync(spec) => Ok(run_fsync(spec)),
            Job::Cell(spec) => rt.block_on(run_cell(spec)),
            Job::Failover(spec) => rt.block_on(run_failover(spec)),
            Job::Transfer(spec) => rt.block_on(run_transfer(spec)),
        };
        let mut row = serde_json::to_value(&job).unwrap();
        row["started_unix"] = started.into();
        row["wall_s"] = clock.elapsed().as_secs_f64().into();
        row["code"] = code.clone().into();
        match result {
            Ok(v) => row["result"] = v,
            Err(e) => {
                eprintln!("  failed: {e}");
                row["error"] = e.into();
            }
        }
        eprintln!("  {}", summary_line(&row));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&out)
            .expect("open the result file");
        writeln!(f, "{}", serde_json::to_string(&row).unwrap()).expect("write a result row");
    }
}

fn summary_line(row: &Value) -> String {
    let r = &row["result"];
    match row["kind"].as_str() {
        Some("cell") => format!(
            "p50 {:.2} ms, p99 {:.2} ms, {:.0} writes/s, {} samples, {:.2} entries/write",
            r["latency_ms"]["p50"].as_f64().unwrap_or(f64::NAN),
            r["latency_ms"]["p99"].as_f64().unwrap_or(f64::NAN),
            r["writes_per_s"].as_f64().unwrap_or(f64::NAN),
            r["samples"],
            r["entries_per_write"].as_f64().unwrap_or(f64::NAN),
        ),
        Some("failover") => format!("kill to ack ms: {}", r["kill_to_ack_ms"]),
        Some("transfer") => format!(
            "seen {}, write at call at new {}, write when seen at new {}, at old {}",
            r["old_leader_sees_new_ms"]["p50"],
            r["write_at_call_at_new_ms"]["p50"],
            r["write_when_seen_at_new_ms"]["p50"],
            r["write_when_seen_at_old_ms"]["p50"]
        ),
        Some("fsync") => format!("{}", r["latency_ms"]),
        _ => String::new(),
    }
}

fn git_head() -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output();
    let head = match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => return "unknown".into(),
    };
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--", "."])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    if dirty {
        format!("{head}+uncommitted")
    } else {
        head
    }
}

/// The phase 1c plan. Base case: 3 voters, no non-voting followers, 100 ms
/// between all pairs, 200-byte proposals, batching on, writes issued at the
/// leader, with 1 and with 16 writers. Two crosses are measured in full:
/// voters × round trip, and writers × batching × round trip.
fn plan() -> Vec<Job> {
    const RTTS: [f64; 6] = [0.0, 1.0, 10.0, 50.0, 100.0, 250.0];
    let mut jobs = Vec::new();

    for size in [200, 4096, 65536] {
        for parallel in [1, 3, 7] {
            jobs.push(Job::Fsync(FsyncSpec {
                name: format!("fsync/size={size}/parallel={parallel}"),
                size,
                parallel,
                count: 2000,
            }));
        }
    }

    for voters in [3, 5, 7] {
        for rtt in RTTS {
            let mut c = uniform("voters_x_rtt", voters, 0, rtt);
            c.batching = false;
            jobs.push(c.named());
        }
    }

    for rtt in RTTS {
        for writers in [1, 16, 128] {
            for batching in [false, true] {
                if writers == 1 && !batching {
                    // The same cell as voters_x_rtt with 3 voters.
                    continue;
                }
                let mut c = uniform("writers_x_batching_x_rtt", 3, 0, rtt);
                c.writers = writers;
                c.batching = batching;
                jobs.push(c.named());
            }
        }
    }

    for rtt in [0.0, 100.0] {
        for writers in [1, 16] {
            for size in [200, 4096, 65536] {
                let mut c = uniform("size", 3, 0, rtt);
                c.writers = writers;
                c.size = size;
                jobs.push(c.named());
            }
        }
    }

    for rtt in [10.0, 100.0] {
        for writers in [1, 16] {
            for learners in [0, 2, 4, 10] {
                let mut c = uniform("learners", 3, learners, rtt);
                c.writers = writers;
                jobs.push(c.named());
            }
        }
    }

    for rtt in [10.0, 100.0] {
        for writers in [1, 16] {
            for issue_at in [IssueAt::Leader, IssueAt::Farthest] {
                let mut c = uniform("issue_at", 3, 0, rtt);
                c.writers = writers;
                c.issue_at = issue_at;
                jobs.push(c.named());
            }
        }
    }

    for placement in ['a', 'b', 'c'] {
        for writers in [1, 16] {
            for issue_at in [IssueAt::Leader, IssueAt::Farthest] {
                let mut c = aws(placement);
                c.writers = writers;
                c.issue_at = issue_at;
                jobs.push(c.named());
            }
        }
    }

    for rtt in [10.0, 100.0] {
        for writers in [1, 16] {
            for proxy in [false, true] {
                let mut c = uniform("proxy", 3, 0, rtt);
                c.writers = writers;
                if proxy {
                    c.delays = DelayTable::uniform(0.0);
                    c.proxy_rtt_ms = Some(rtt);
                }
                jobs.push(c.named());
            }
        }
    }

    for rtt in RTTS {
        jobs.push(Job::Failover(FailoverSpec {
            name: format!("failover/rtt={rtt}"),
            voters: 3,
            rtt_ms: rtt,
            trials: 10,
        }));
    }

    // Added after the first run of the plan: how the batcher's limit on
    // entries in flight trades entries per write against latency, and what
    // it costs after a leader transfer.
    for rtt in [10.0, 100.0] {
        for writers in [16, 128] {
            for in_flight in [1, 8, 64] {
                let mut c = uniform("batching_in_flight", 3, 0, rtt);
                c.writers = writers;
                c.max_entries_in_flight = Some(in_flight);
                jobs.push(c.named());
            }
        }
    }
    for rtt in [0.0, 10.0, 100.0] {
        for in_flight in [2, 64] {
            jobs.push(Job::Transfer(TransferSpec {
                name: format!("transfer/rtt={rtt}/in_flight={in_flight}"),
                rtt_ms: rtt,
                max_entries_in_flight: in_flight,
                trials: 6,
            }));
        }
    }
    jobs
}

fn uniform(group: &str, voters: u64, learners: u64, rtt_ms: f64) -> CellSpec {
    CellSpec {
        name: String::new(),
        group: group.into(),
        voters: (1..=voters).collect(),
        learners: (voters + 1..=voters + learners).collect(),
        regions: BTreeMap::new(),
        delays: DelayTable::uniform(rtt_ms),
        proxy_rtt_ms: None,
        leader: None,
        issue_at: IssueAt::Leader,
        size: 200,
        writers: 1,
        batching: true,
        max_entries_in_flight: None,
        warmup_s: 0.0,
        duration_s: 0.0,
    }
}

/// Seven AWS regions with the round trips from cloudping.co. Placement `a`:
/// voters in Frankfurt, Ireland, London; `b`: Frankfurt, N. Virginia,
/// Singapore; `c`: three voters in Frankfurt, 1 ms apart. The other regions
/// hold non-voting followers. The leader is moved to Frankfurt (node 1).
fn aws(placement: char) -> CellSpec {
    const REGIONS: [(&str, &str); 7] = [
        ("eu-central-1", "Frankfurt"),
        ("eu-west-1", "Ireland"),
        ("eu-west-2", "London"),
        ("us-east-1", "N. Virginia"),
        ("us-west-2", "Oregon"),
        ("ap-southeast-1", "Singapore"),
        ("ap-southeast-2", "Sydney"),
    ];
    let table: Value = serde_json::from_str(AWS_TABLE).unwrap();
    let rtt = |a: usize, b: usize| {
        let (ra, rb) = (REGIONS[a].0, REGIONS[b].0);
        let x = table["data"][ra][rb].as_f64().unwrap();
        let y = table["data"][rb][ra].as_f64().unwrap();
        ((x + y) / 2.0 * 10.0).round() / 10.0
    };
    // Nodes 1..=7 are the regions in order; 8 and 9 are two more nodes in
    // Frankfurt for placement c.
    let region_of = |id: NodeId| if id >= 8 { 0 } else { id as usize - 1 };
    let (voters, learners): (Vec<NodeId>, Vec<NodeId>) = match placement {
        'a' => (vec![1, 2, 3], vec![4, 5, 6, 7]),
        'b' => (vec![1, 4, 6], vec![2, 3, 5, 7]),
        'c' => (vec![1, 8, 9], vec![2, 3, 4, 5, 6, 7]),
        _ => unreachable!(),
    };
    let ids: Vec<NodeId> = voters.iter().chain(&learners).copied().collect();
    let mut pairs = Vec::new();
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            let (ra, rb) = (region_of(*a), region_of(*b));
            let rtt_ms = if ra == rb { 1.0 } else { rtt(ra, rb) };
            pairs.push(PairRtt {
                a: *a,
                b: *b,
                rtt_ms,
            });
        }
    }
    let regions = ids
        .iter()
        .map(|id| (*id, REGIONS[region_of(*id)].1.to_string()))
        .collect();
    CellSpec {
        name: String::new(),
        group: format!("aws_{placement}"),
        voters,
        learners,
        regions,
        delays: DelayTable {
            default_rtt_ms: 0.0,
            pairs,
        },
        proxy_rtt_ms: None,
        leader: Some(1),
        issue_at: IssueAt::Leader,
        size: 200,
        writers: 1,
        batching: true,
        max_entries_in_flight: None,
        warmup_s: 0.0,
        duration_s: 0.0,
    }
}

impl CellSpec {
    /// Sets the name and the measuring time. One writer runs for about 1000
    /// times the expected latency (at least 10 s, at most 200 s); more
    /// writers run a fifth of that (at least 10 s, at most 60 s).
    fn named(mut self) -> Job {
        let leader = self.leader.unwrap_or(self.voters[0]);
        let expected_ms = {
            let rtt = self.rtt_fn();
            let mut expected = quorum_rtt(leader, &self.voters, &rtt);
            if self.issue_at == IssueAt::Farthest {
                let far = farthest(leader, &self.voters, &self.ids(), &rtt);
                expected += rtt(far, leader);
            }
            ms(expected)
        };
        self.duration_s = if self.writers == 1 {
            expected_ms.clamp(10.0, 200.0)
        } else {
            (expected_ms / 5.0).clamp(10.0, 60.0)
        };
        self.warmup_s = 2.0 + 5.0 * expected_ms / 1000.0;
        let rtt_label = match (&self.proxy_rtt_ms, self.regions.is_empty()) {
            (Some(p), _) => format!("proxy_rtt={p}"),
            (None, true) => format!("rtt={}", self.delays.default_rtt_ms),
            (None, false) => "aws".into(),
        };
        let in_flight = match self.max_entries_in_flight {
            Some(n) => format!("/in_flight={n}"),
            None => String::new(),
        };
        self.name = format!(
            "{}/voters={}/learners={}/{rtt_label}/size={}/writers={}/batching={}/at={}{in_flight}",
            self.group,
            self.voters.len(),
            self.learners.len(),
            self.size,
            self.writers,
            if self.batching { "on" } else { "off" },
            match self.issue_at {
                IssueAt::Leader => "leader",
                IssueAt::Farthest => "farthest",
            }
        );
        Job::Cell(self)
    }

    fn ids(&self) -> Vec<NodeId> {
        self.voters.iter().chain(&self.learners).copied().collect()
    }

    /// The round trip between two nodes that this cell injects.
    fn rtt_fn(&self) -> impl Fn(NodeId, NodeId) -> Duration + '_ {
        move |a, b| match self.proxy_rtt_ms {
            Some(ms) if a != b => Duration::from_secs_f64(ms / 1000.0),
            Some(_) => Duration::ZERO,
            None => self.delays.rtt(a, b),
        }
    }
}

/// The round trip from `leader` to the ⌊n/2⌋-th nearest other voter, which a
/// write must wait for.
fn quorum_rtt(
    leader: NodeId,
    voters: &[NodeId],
    rtt: &impl Fn(NodeId, NodeId) -> Duration,
) -> Duration {
    let mut others: Vec<Duration> = voters
        .iter()
        .filter(|v| **v != leader)
        .map(|v| rtt(leader, *v))
        .collect();
    others.sort();
    let k = voters.len() / 2;
    if k == 0 {
        Duration::ZERO
    } else {
        others[k - 1]
    }
}

fn farthest(
    leader: NodeId,
    voters: &[NodeId],
    ids: &[NodeId],
    rtt: &impl Fn(NodeId, NodeId) -> Duration,
) -> NodeId {
    *ids.iter()
        .filter(|id| **id != leader)
        .max_by_key(|id| (rtt(leader, **id), voters.contains(id), **id))
        .expect("a node besides the leader")
}

// ---------------------------------------------------------------------------
// fsync

fn run_fsync(spec: &FsyncSpec) -> Value {
    let dir = bench_dir(&spec.name);
    let threads: Vec<_> = (0..spec.parallel)
        .map(|t| {
            let path = dir.join(format!("file-{t}"));
            let size = spec.size;
            let count = spec.count;
            std::thread::spawn(move || {
                let mut f = std::fs::File::create(&path).unwrap();
                // A log record: 8 header bytes and the payload.
                let record = vec![0xabu8; size + 8];
                let mut times = Vec::with_capacity(count);
                for _ in 0..count {
                    let start = std::time::Instant::now();
                    f.write_all(&record).unwrap();
                    f.sync_data().unwrap();
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                times
            })
        })
        .collect();
    let mut all = Vec::new();
    for t in threads {
        all.extend(t.join().unwrap());
    }
    let _ = std::fs::remove_dir_all(&dir);
    json!({
        "samples": all.len(),
        "latency_ms": percentiles(&mut all),
        "note": "append to a growing file, then fdatasync, in a loop per thread",
    })
}

// ---------------------------------------------------------------------------
// A cluster of node processes

struct Cluster {
    dir: PathBuf,
    config_path: PathBuf,
    /// Where each node listens.
    listen: BTreeMap<NodeId, String>,
    /// The address other nodes use: the node's own, or its proxy's.
    advertised: BTreeMap<NodeId, String>,
    nodes: BTreeMap<NodeId, Child>,
    proxies: Vec<Child>,
    clients: BTreeMap<NodeId, Arc<Client>>,
}

impl Cluster {
    fn new(
        name: &str,
        ids: &[NodeId],
        delays: DelayTable,
        timeouts: RaftTimeouts,
        batching: Option<Batching>,
        proxy_rtt_ms: Option<f64>,
    ) -> Result<Cluster, String> {
        let dir = bench_dir(name);
        let listen: BTreeMap<NodeId, String> = ids
            .iter()
            .map(|id| (*id, format!("127.0.0.1:{}", free_port())))
            .collect();
        let config = ClusterConfig {
            nodes: listen.clone(),
            delays,
            timeouts: Some(timeouts),
            batching,
            log: LogConfig::default(),
            propose_timeout_ms: 30_000,
        };
        let config_path = dir.join("cluster.json");
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
            .map_err(|e| e.to_string())?;
        let mut c = Cluster {
            dir,
            config_path,
            advertised: listen.clone(),
            listen,
            nodes: BTreeMap::new(),
            proxies: Vec::new(),
            clients: BTreeMap::new(),
        };
        if let Some(rtt) = proxy_rtt_ms {
            for id in ids {
                let addr = format!("127.0.0.1:{}", free_port());
                let mut child = Command::new(sibling_binary("tursoraft-raft-bench"))
                    .args(["proxy", "--listen", &addr, "--to", &c.listen[id]])
                    .args(["--one-way-ms", &(rtt / 2.0).to_string()])
                    .stdout(Stdio::piped())
                    .spawn()
                    .map_err(|e| e.to_string())?;
                wait_listening(&mut child, &format!("proxy of node {id}"))?;
                c.proxies.push(child);
                c.advertised.insert(*id, addr);
            }
        }
        for id in ids {
            c.start(*id)?;
            c.clients.insert(
                *id,
                Arc::new(Client::new(c.listen[id].clone()).with_timeout(Duration::from_secs(60))),
            );
        }
        Ok(c)
    }

    /// Starts node `id`; a restarted node continues from its directory.
    fn start(&mut self, id: NodeId) -> Result<(), String> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("node-{id}.log")))
            .map_err(|e| e.to_string())?;
        let node_bin = sibling_binary("tursoraft-raft-node");
        let mut child = Command::new(&node_bin)
            .arg("--config")
            .arg(&self.config_path)
            .args(["--id", &id.to_string(), "--dir"])
            .arg(self.dir.join(format!("node-{id}")))
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "error".into()),
            )
            .stdout(Stdio::piped())
            .stderr(log)
            .spawn()
            .map_err(|e| format!("start {}: {e}", node_bin.display()))?;
        wait_listening(&mut child, &format!("node {id}"))?;
        self.nodes.insert(id, child);
        Ok(())
    }

    fn kill(&mut self, id: NodeId) {
        if let Some(mut child) = self.nodes.remove(&id) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn client(&self, id: NodeId) -> Arc<Client> {
        self.clients[&id].clone()
    }

    async fn initialize(&self, voters: &[NodeId]) -> Result<NodeId, String> {
        let members = voters
            .iter()
            .map(|id| (*id, self.advertised[id].clone()))
            .collect();
        self.client(voters[0])
            .initialize(members)
            .await
            .map_err(|e| format!("initialize: {e}"))?;
        self.wait_leader(voters).await
    }

    /// Waits until all of `ids` name the same leader among them.
    async fn wait_leader(&self, ids: &[NodeId]) -> Result<NodeId, String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let clients: Vec<_> = ids.iter().map(|id| self.client(*id)).collect();
            let statuses = join_all(clients.iter().map(|c| c.status())).await;
            let leaders: BTreeSet<Option<NodeId>> = statuses
                .into_iter()
                .map(|s| s.ok().and_then(|s| s.leader))
                .collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                if ids.contains(&leader) {
                    return Ok(leader);
                }
            }
            if Instant::now() > deadline {
                return Err(format!("no common leader among {ids:?}"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn status(&self, id: NodeId) -> Result<Status, String> {
        self.client(id).status().await.map_err(|e| e.to_string())
    }

    async fn wait_applied(&self, ids: &[NodeId], index: u64) -> Result<(), String> {
        for id in ids {
            self.client(*id)
                .wait_applied(index, Duration::from_secs(60))
                .await
                .map_err(|e| format!("node {id} did not apply {index}: {e}"))?;
        }
        Ok(())
    }

    /// The state machine's counters on node `id`.
    async fn stats(&self, id: NodeId) -> Result<Value, String> {
        let bytes = self
            .client(id)
            .app(vec![])
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.nodes.values_mut().chain(self.proxies.iter_mut()) {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_listening(child: &mut Child, what: &str) -> Result<(), String> {
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    if line.starts_with("listening") {
        Ok(())
    } else {
        Err(format!("{what} did not start: {line:?}"))
    }
}

/// A binary next to this one. By name, so that a rebuild while a run goes on
/// does not leave the run with a deleted path.
fn sibling_binary(name: &str) -> PathBuf {
    std::env::current_exe().unwrap().with_file_name(name)
}

fn bench_dir(name: &str) -> PathBuf {
    let safe: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let dir = std::env::temp_dir()
        .join("tursoraft-bench")
        .join(format!("{safe}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the benchmark directory");
    dir
}

/// A free port below the kernel's range for outgoing connections.
fn free_port() -> u16 {
    use std::sync::atomic::AtomicU32;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let n = std::process::id() * 37 + NEXT.fetch_add(1, Ordering::Relaxed);
        let port = 20_000 + (n % 12_000) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

// ---------------------------------------------------------------------------
// One cell

#[derive(Default)]
struct WriterLog {
    /// Time to acknowledgement of writes started in the window, in ms.
    latencies: Vec<f64>,
    /// Writes acknowledged in the window.
    acked_in_window: u64,
    rejected: u64,
    errors: BTreeMap<String, u64>,
}

struct Sample {
    at: Instant,
    leader_last: u64,
    leader_applied: u64,
    /// Per follower: its own applied index, and the index the leader knows
    /// it has stored.
    followers: BTreeMap<NodeId, (Option<u64>, Option<u64>)>,
}

async fn run_cell(spec: &CellSpec) -> Result<Value, String> {
    let ids = spec.ids();
    let rtt = spec.rtt_fn();
    let max_voter_rtt = spec
        .voters
        .iter()
        .flat_map(|a| spec.voters.iter().map(|b| rtt(*a, *b)))
        .max()
        .unwrap_or_default();
    let timeouts = RaftTimeouts::for_max_rtt(max_voter_rtt);
    let cluster = Cluster::new(
        &spec.name,
        &ids,
        spec.delays.clone(),
        timeouts.clone(),
        spec.batching.then(|| Batching {
            max_entries_in_flight: spec
                .max_entries_in_flight
                .unwrap_or(Batching::default().max_entries_in_flight),
            ..Batching::default()
        }),
        spec.proxy_rtt_ms,
    )?;
    let mut leader = cluster.initialize(&spec.voters).await?;
    if let Some(want) = spec.leader {
        if want != leader {
            cluster
                .client(leader)
                .transfer_leader(want)
                .await
                .map_err(|e| format!("transfer leader: {e}"))?;
            leader = cluster.wait_leader(&spec.voters).await?;
        }
    }
    for l in &spec.learners {
        cluster
            .client(leader)
            .add_follower(*l, cluster.advertised[l].clone())
            .await
            .map_err(|e| format!("add follower {l}: {e}"))?;
    }
    let index = cluster.status(leader).await?.last_log_index.unwrap_or(0);
    cluster.wait_applied(&ids, index).await?;

    let mut ping_ms = BTreeMap::new();
    for id in ids.iter().filter(|id| **id != leader) {
        let mut times = Vec::new();
        for _ in 0..5 {
            let d = cluster
                .client(leader)
                .ping_peer(*id)
                .await
                .map_err(|e| format!("ping {id}: {e}"))?;
            times.push(d.as_secs_f64() * 1000.0);
        }
        times.sort_by(f64::total_cmp);
        ping_ms.insert(id.to_string(), times[2]);
    }

    let issue = match spec.issue_at {
        IssueAt::Leader => leader,
        IssueAt::Farthest => farthest(leader, &spec.voters, &ids, &rtt),
    };
    let window_start = Instant::now() + Duration::from_secs_f64(spec.warmup_s);
    let window_end = window_start + Duration::from_secs_f64(spec.duration_s);
    let logs: Vec<_> = (0..spec.writers)
        .map(|w| {
            let client = Arc::new(
                Client::new(cluster.listen[&issue].clone()).with_timeout(Duration::from_secs(60)),
            );
            let size = spec.size;
            tokio::spawn(writer(client, w, size, window_start, window_end))
        })
        .collect();

    tokio::time::sleep_until(window_start).await;
    let before = cluster.status(leader).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let sampler = tokio::spawn(sample_lag(
        ids.iter().map(|id| (*id, cluster.client(*id))).collect(),
        leader,
        stop.clone(),
        samples.clone(),
    ));
    tokio::time::sleep_until(window_end).await;
    let after = cluster.status(leader).await?;
    stop.store(true, Ordering::SeqCst);
    sampler.await.unwrap();

    let mut log = WriterLog::default();
    for l in logs {
        let w = l.await.unwrap();
        log.latencies.extend(w.latencies);
        log.acked_in_window += w.acked_in_window;
        log.rejected += w.rejected;
        for (e, n) in w.errors {
            *log.errors.entry(e).or_default() += n;
        }
    }

    let last = cluster.status(leader).await?.last_log_index.unwrap_or(0);
    cluster.wait_applied(&ids, last).await?;
    let mut digests = BTreeMap::new();
    for id in &ids {
        let s = cluster.stats(*id).await?;
        digests.insert(*id, (s["applied"].as_u64(), s["digest"].as_u64()));
    }
    let same_entries = digests.values().collect::<BTreeSet<_>>().len() == 1;

    let window_s = spec.duration_s;
    let writes = log.acked_in_window;
    let per_write = |n: u64| {
        if writes == 0 {
            f64::NAN
        } else {
            n as f64 / writes as f64
        }
    };
    let mut append = BTreeMap::new();
    let (mut with_entries_total, mut requests_total) = (0, 0);
    for a in &after.sent {
        let b = before.sent.iter().find(|b| b.target == a.target);
        let delta = |f: fn(&tursoraft_raft::PeerStats) -> u64| f(a) - b.map_or(0, f);
        let with_entries = delta(|s| s.append_requests_with_entries);
        let requests = delta(|s| s.append_requests);
        let entries = delta(|s| s.entries_sent);
        if spec.voters.contains(&a.target) {
            with_entries_total += with_entries;
            requests_total += requests;
        }
        append.insert(
            a.target.to_string(),
            json!({
                "requests_with_entries_per_write": per_write(with_entries),
                "requests_per_write": per_write(requests),
                "entries_per_request": if with_entries == 0 { f64::NAN } else { entries as f64 / with_entries as f64 },
                "max_in_flight": a.max_in_flight,
                "max_entries_per_request": a.max_entries_per_request,
            }),
        );
    }
    let entries = after.last_log_index.unwrap_or(0) - before.last_log_index.unwrap_or(0);
    let samples = std::mem::take(&mut *samples.lock().unwrap());
    let lag = lag_summary(&samples, &ids, leader, &spec.voters, &rtt);
    let leader_changed = before.term != after.term || after.leader != Some(leader);

    Ok(json!({
        "leader": leader,
        "issue_node": issue,
        "issue_rtt_to_leader_ms": ms(rtt(issue, leader)),
        "quorum_rtt_ms": ms(quorum_rtt(leader, &spec.voters, &rtt)),
        "timeouts": timeouts,
        "ping_from_leader_ms": ping_ms,
        "samples": log.latencies.len(),
        "latency_ms": percentiles(&mut log.latencies.clone()),
        "latency_hist": histogram(&log.latencies),
        "writes_in_window": writes,
        "writes_per_s": writes as f64 / window_s,
        "rejected": log.rejected,
        "errors": log.errors,
        "log_entries_in_window": entries,
        "entries_per_write": per_write(entries),
        "voter_append_requests_with_entries_per_write": per_write(with_entries_total),
        "voter_append_requests_per_write": per_write(requests_total),
        "append": append,
        "lag": lag,
        "status_samples": samples.len(),
        "leader_changed": leader_changed,
        "all_nodes_applied_same_entries": same_entries,
    }))
}

/// Proposes `size`-byte proposals one after another until `end`.
async fn writer(
    client: Arc<Client>,
    w: usize,
    size: usize,
    start: Instant,
    end: Instant,
) -> WriterLog {
    let mut log = WriterLog::default();
    let mut seq = 0u64;
    loop {
        let t0 = Instant::now();
        if t0 >= end {
            break;
        }
        let mut p = format!("w{w}-{seq}-").into_bytes();
        p.resize(size.max(p.len()), b'x');
        seq += 1;
        let res = client.propose(p).await;
        let t1 = Instant::now();
        match res {
            Ok(proposed) => {
                if t0 >= start {
                    log.latencies.push((t1 - t0).as_secs_f64() * 1000.0);
                }
                if t1 >= start && t1 < end {
                    log.acked_in_window += 1;
                }
                if proposed.outcome != tursoraft_raft::Outcome::Applied {
                    log.rejected += 1;
                }
            }
            Err(e) => {
                let kind = format!("{e:?}");
                let kind = kind.split(['(', ' ', '{']).next().unwrap_or("").to_string();
                *log.errors.entry(kind).or_default() += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
    log
}

/// Every [`STATUS_INTERVAL`], records the leader's log and applied index and
/// each follower's applied index.
async fn sample_lag(
    clients: BTreeMap<NodeId, Arc<Client>>,
    leader: NodeId,
    stop: Arc<AtomicBool>,
    out: Arc<Mutex<Vec<Sample>>>,
) {
    let mut tick = tokio::time::interval(STATUS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        let at = Instant::now();
        let statuses = join_all(clients.iter().map(|(id, c)| {
            let id = *id;
            async move { (id, c.status().await.ok()) }
        }))
        .await;
        let statuses: BTreeMap<NodeId, Option<Status>> = statuses.into_iter().collect();
        let Some(Some(l)) = statuses.get(&leader) else {
            continue;
        };
        let followers = statuses
            .iter()
            .filter(|(id, _)| **id != leader)
            .map(|(id, s)| {
                let applied = s.as_ref().and_then(|s| s.applied);
                let matched = l.matched.get(id).copied().flatten();
                (*id, (applied, matched))
            })
            .collect();
        out.lock().unwrap().push(Sample {
            at,
            leader_last: l.last_log_index.unwrap_or(0),
            leader_applied: l.applied.unwrap_or(0),
            followers,
        });
    }
}

/// Per follower: how many entries it is behind the leader's log (by its own
/// applied index, and by what the leader knows it stored), and how long it
/// has been missing an entry the leader has applied.
fn lag_summary(
    samples: &[Sample],
    ids: &[NodeId],
    leader: NodeId,
    voters: &[NodeId],
    rtt: &impl Fn(NodeId, NodeId) -> Duration,
) -> Value {
    let mut out = BTreeMap::new();
    for id in ids.iter().filter(|id| **id != leader) {
        let mut applied_lag = Vec::new();
        let mut stored_lag = Vec::new();
        let mut time_lag = Vec::new();
        for s in samples {
            let Some((applied, matched)) = s.followers.get(id) else {
                continue;
            };
            if let Some(a) = applied {
                applied_lag.push(s.leader_last.saturating_sub(*a) as f64);
                time_lag.push(staleness_ms(samples, s, *a));
            }
            if let Some(m) = matched {
                stored_lag.push(s.leader_last.saturating_sub(*m) as f64);
            }
        }
        out.insert(
            id.to_string(),
            json!({
                "voter": voters.contains(id),
                "rtt_to_leader_ms": ms(rtt(*id, leader)),
                "applied_lag_entries": mean_max(&applied_lag),
                "stored_lag_entries": mean_max(&stored_lag),
                "applied_lag_ms": percentiles(&mut time_lag),
            }),
        );
    }
    serde_json::to_value(out).unwrap()
}

/// How long before `s` the leader had applied an entry after `applied`, or 0
/// if it had not.
fn staleness_ms(samples: &[Sample], s: &Sample, applied: u64) -> f64 {
    if s.leader_applied <= applied {
        return 0.0;
    }
    let first = samples.partition_point(|x| x.leader_applied <= applied);
    match samples.get(first) {
        Some(x) if x.at <= s.at => (s.at - x.at).as_secs_f64() * 1000.0,
        _ => 0.0,
    }
}

fn mean_max(v: &[f64]) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    json!({
        "mean": v.iter().sum::<f64>() / v.len() as f64,
        "max": v.iter().copied().fold(0.0, f64::max),
    })
}

fn percentiles(v: &mut [f64]) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_by(f64::total_cmp);
    let at = |q: f64| v[((v.len() - 1) as f64 * q).round() as usize];
    json!({
        "min": v[0],
        "p50": at(0.5),
        "p90": at(0.9),
        "p99": at(0.99),
        "max": v[v.len() - 1],
        "mean": v.iter().sum::<f64>() / v.len() as f64,
    })
}

/// Counts per bucket `b`, where a bucket holds values from `1.02^b` to
/// `1.02^(b+1)` microseconds.
fn histogram(ms: &[f64]) -> Value {
    let mut h: BTreeMap<i64, u64> = BTreeMap::new();
    for v in ms {
        let us = (v * 1000.0).max(1.0);
        *h.entry((us.ln() / HIST_BASE.ln()).floor() as i64)
            .or_default() += 1;
    }
    json!({
        "base": HIST_BASE,
        "unit": "us",
        "buckets": h.into_iter().collect::<Vec<_>>(),
    })
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Failover

async fn run_failover(spec: &FailoverSpec) -> Result<Value, String> {
    let voters: Vec<NodeId> = (1..=spec.voters as u64).collect();
    let rtt = Duration::from_secs_f64(spec.rtt_ms / 1000.0);
    let timeouts = RaftTimeouts::for_max_rtt(rtt);
    let mut cluster = Cluster::new(
        &spec.name,
        &voters,
        DelayTable::uniform(spec.rtt_ms),
        timeouts.clone(),
        None,
        None,
    )?;
    cluster.initialize(&voters).await?;
    let mut trials = Vec::new();
    for _ in 0..spec.trials {
        let leader = cluster.wait_leader(&voters).await?;
        let others: Vec<NodeId> = voters.iter().copied().filter(|v| *v != leader).collect();
        let killed_at: Arc<Mutex<Option<Instant>>> = Arc::default();
        let first_ack: Arc<Mutex<Option<Instant>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = others
            .iter()
            .map(|id| {
                let client = cluster.client(*id);
                let (killed_at, first_ack, stop) =
                    (killed_at.clone(), first_ack.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut seq = 0u64;
                    while !stop.load(Ordering::SeqCst) {
                        let t0 = Instant::now();
                        seq += 1;
                        let res = client.propose(format!("f-{seq}").into_bytes()).await;
                        let t1 = Instant::now();
                        if res.is_err() {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                        let killed = *killed_at.lock().unwrap();
                        if killed.is_some_and(|k| t0 >= k) {
                            let mut f = first_ack.lock().unwrap();
                            if f.is_none_or(|f| t1 < f) {
                                *f = Some(t1);
                            }
                        }
                    }
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(1) + 4 * rtt).await;
        let kill_at = Instant::now();
        *killed_at.lock().unwrap() = Some(kill_at);
        cluster.kill(leader);
        let deadline = kill_at + Duration::from_secs(60);
        let mut new_leader_at = None;
        let mut new_leader = None;
        while first_ack.lock().unwrap().is_none() && Instant::now() < deadline {
            if new_leader_at.is_none() {
                for id in &others {
                    if let Ok(s) = cluster.status(*id).await {
                        if s.leader.is_some_and(|l| l != leader) && s.state == "Leader" {
                            new_leader_at = Some(Instant::now());
                            new_leader = s.leader;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        stop.store(true, Ordering::SeqCst);
        for w in writers {
            let _ = w.await;
        }
        let ack = first_ack
            .lock()
            .unwrap()
            .ok_or("no write acknowledged after the kill")?;
        trials.push(json!({
            "killed": leader,
            "new_leader": new_leader,
            "kill_to_new_leader_ms": new_leader_at.map(|t| ms(t - kill_at)),
            "kill_to_ack_ms": ms(ack - kill_at),
        }));
        cluster.start(leader)?;
        let new = cluster.wait_leader(&voters).await?;
        let last = cluster.status(new).await?.last_log_index.unwrap_or(0);
        cluster.wait_applied(&voters, last).await?;
    }
    let mut acks: Vec<f64> = trials
        .iter()
        .map(|t| t["kill_to_ack_ms"].as_f64().unwrap())
        .collect();
    Ok(json!({
        "timeouts": timeouts,
        "kill_to_ack_ms": percentiles(&mut acks),
        "trials": trials,
    }))
}

// ---------------------------------------------------------------------------
// Leader transfer

/// Per trial, in ms from the call: when the target became leader; when one
/// write issued at the target at the moment of the call was acknowledged;
/// when writes issued at the old and at the new leader as soon as the old
/// leader sees the new one were acknowledged. Also the longest time without
/// any acknowledged write while 8 writers propose at the old leader.
async fn run_transfer(spec: &TransferSpec) -> Result<Value, String> {
    let voters: Vec<NodeId> = vec![1, 2, 3];
    let rtt = Duration::from_secs_f64(spec.rtt_ms / 1000.0);
    let timeouts = RaftTimeouts::for_max_rtt(rtt);
    let cluster = Cluster::new(
        &spec.name,
        &voters,
        DelayTable::uniform(spec.rtt_ms),
        timeouts.clone(),
        Some(Batching {
            max_entries_in_flight: spec.max_entries_in_flight,
            ..Batching::default()
        }),
        None,
    )?;
    let mut leader = cluster.initialize(&voters).await?;
    let settle = Duration::from_secs(1) + 5 * rtt;
    let mut trials = Vec::new();
    for _ in 0..spec.trials {
        let target = leader % 3 + 1;
        let acks: Arc<Mutex<Vec<Instant>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = (0..8)
            .map(|w| {
                let client = Client::new(cluster.listen[&leader].clone())
                    .with_timeout(Duration::from_secs(60));
                let (acks, stop) = (acks.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut seq = 0u64;
                    while !stop.load(Ordering::SeqCst) {
                        seq += 1;
                        if client
                            .propose(format!("t{w}-{seq}").into_bytes())
                            .await
                            .is_ok()
                        {
                            acks.lock().unwrap().push(Instant::now());
                        }
                    }
                })
            })
            .collect();
        tokio::time::sleep(settle).await;

        let called = Instant::now();
        let fresh = |at: NodeId| {
            let client = cluster.client(at);
            tokio::spawn(async move {
                client
                    .propose(b"fresh".to_vec())
                    .await
                    .map(|_| ms(called.elapsed()))
                    .map_err(|e| e.to_string())
            })
        };
        let at_call_at_new = fresh(target);
        let target_client = cluster.client(target);
        let became_leader = tokio::spawn(async move {
            loop {
                if let Ok(s) = target_client.status().await {
                    if s.state == "Leader" {
                        return ms(called.elapsed());
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        cluster
            .client(leader)
            .transfer_leader(target)
            .await
            .map_err(|e| format!("transfer leader: {e}"))?;
        let old_sees_new = ms(called.elapsed());
        let (seen_at_new, seen_at_old) = (fresh(target), fresh(leader));
        let became_leader = became_leader.await.unwrap();
        let at_call_at_new = at_call_at_new.await.unwrap()?;
        let seen_at_new = seen_at_new.await.unwrap()?;
        let seen_at_old = seen_at_old.await.unwrap()?;
        tokio::time::sleep(settle).await;
        stop.store(true, Ordering::SeqCst);
        for w in writers {
            let _ = w.await;
        }
        let mut acks = acks.lock().unwrap().clone();
        acks.sort();
        let gap = acks
            .windows(2)
            .filter(|w| w[1] > called)
            .map(|w| ms(w[1] - w[0]))
            .fold(0.0, f64::max);
        trials.push(json!({
            "from": leader,
            "to": target,
            "target_leader_ms": became_leader,
            "old_leader_sees_new_ms": old_sees_new,
            "write_at_call_at_new_ms": at_call_at_new,
            "write_when_seen_at_new_ms": seen_at_new,
            "write_when_seen_at_old_ms": seen_at_old,
            "longest_gap_between_acks_ms": gap,
        }));
        leader = cluster.wait_leader(&voters).await?;
        if leader != target {
            return Err(format!("leader is {leader} after the transfer to {target}"));
        }
    }
    let field = |f: &str| {
        let mut v: Vec<f64> = trials.iter().map(|t| t[f].as_f64().unwrap()).collect();
        percentiles(&mut v)
    };
    Ok(json!({
        "timeouts": timeouts,
        "target_leader_ms": field("target_leader_ms"),
        "old_leader_sees_new_ms": field("old_leader_sees_new_ms"),
        "write_at_call_at_new_ms": field("write_at_call_at_new_ms"),
        "write_when_seen_at_new_ms": field("write_when_seen_at_new_ms"),
        "write_when_seen_at_old_ms": field("write_when_seen_at_old_ms"),
        "longest_gap_between_acks_ms": field("longest_gap_between_acks_ms"),
        "trials": trials,
    }))
}

// ---------------------------------------------------------------------------
// Delay proxy

fn proxy_command(args: Vec<String>) {
    let mut listen = None;
    let mut to = None;
    let mut one_way_ms = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| usage(&format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "--listen" => listen = Some(value()),
            "--to" => to = Some(value()),
            "--one-way-ms" => {
                one_way_ms = Some(
                    value()
                        .parse::<f64>()
                        .unwrap_or_else(|_| usage("--one-way-ms must be a number")),
                )
            }
            other => usage(&format!("unknown argument {other}")),
        }
    }
    let listen = listen.unwrap_or_else(|| usage("--listen is required"));
    let to = to.unwrap_or_else(|| usage("--to is required"));
    let delay = Duration::from_secs_f64(
        one_way_ms.unwrap_or_else(|| usage("--one-way-ms is required")) / 1000.0,
    );
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(proxy(listen, to, delay));
}

/// Forwards every connection to `to`, holding each chunk of bytes read for
/// `delay` before writing it, in both directions.
async fn proxy(listen: String, to: String, delay: Duration) {
    let listener = TcpListener::bind(&listen).await.expect("bind the proxy");
    println!("listening {listen}");
    std::io::stdout().flush().unwrap();
    loop {
        let Ok((inbound, _)) = listener.accept().await else {
            continue;
        };
        let to = to.clone();
        tokio::spawn(async move {
            let Ok(outbound) = TcpStream::connect(&to).await else {
                return;
            };
            let _ = inbound.set_nodelay(true);
            let _ = outbound.set_nodelay(true);
            let (ir, iw) = inbound.into_split();
            let (or, ow) = outbound.into_split();
            tokio::spawn(pump(ir, ow, delay));
            tokio::spawn(pump(or, iw, delay));
        });
    }
}

async fn pump(
    mut r: tokio::net::tcp::OwnedReadHalf,
    mut w: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let writer = tokio::spawn(async move {
        while let Some((at, bytes)) = rx.recv().await {
            if at > Instant::now() {
                // The same correction as the nodes' delay: tokio's timer fires
                // about 1 ms after the deadline.
                tokio::time::sleep_until(at - Duration::from_millis(1)).await;
            }
            if w.write_all(&bytes).await.is_err() {
                return;
            }
        }
        let _ = w.shutdown().await;
    });
    let mut buf = vec![0u8; 256 << 10];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if tx
                    .send((Instant::now() + delay, buf[..n].to_vec()))
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    drop(tx);
    let _ = writer.await;
}
