//! Write latency and throughput against network delay, with the database
//! attached. Every transaction is one bank transfer (two updates and one
//! insert). Run it with
//!
//! ```text
//! cargo run --release -p tursoraft-proof -- bench [--quick] [--only uniform,followers,geo,failover] [--pending 1,256]
//! ```
//!
//! Each cell starts its writers, discards a warm-up, then measures until it
//! has both a minimum time and a minimum number of acknowledged transfers, or
//! a maximum time. Columns:
//!
//! - `p50`, `p99`: time from sending a transfer to its acknowledgement, of
//!   the transfers sent in the measured window.
//! - `tx/s`: transfers acknowledged in the window per second.
//! - `tx/entry`: transactions per log entry at the leader (its leader loop's
//!   `transactions / groups` over the cell).
//! - `lag p50`, `lag max`: every 50 ms a sampler reads the leader's last log
//!   index and waits until a follower has applied it; the time that took.
//!   Over all followers.
//! - `behind max`: the most log entries a follower had not applied when
//!   sampled.
//! - `rej`, `unk`, `unav`: transfers answered `Rejected`, `OutcomeUnknown`,
//!   `Unavailable`.
//! - `resets`: times in the cell the leader copied its file to its ahead
//!   file, which it does after a pending group failed.
//!
//! Every table is measured once per `--pending` value, the leader's
//! `max_pending_groups` (column `pending`; 1 is no pipelining, 256 the
//! node's default).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tursoraft_node::{Client, CommitMode, Error};
use tursoraft_raft::{DelayTable, NodeId, PairRtt, RaftTimeouts};

use crate::bank::{schema, Attempt, Load, LoadSpec};
use crate::cluster::{ms, Cluster};

const ACCOUNTS: i64 = 100;
const SECTIONS: [&str; 4] = ["uniform", "followers", "geo", "failover"];

pub fn main(args: &[String]) {
    let mut quick = false;
    let mut pendings = vec![1, 256];
    let mut only: Vec<String> = SECTIONS.iter().map(|s| s.to_string()).collect();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--quick" => quick = true,
            "--only" => {
                let list = args.next().expect("--only needs a list");
                only = list.split(',').map(str::to_string).collect();
                for s in &only {
                    assert!(SECTIONS.contains(&s.as_str()), "unknown section {s}");
                }
            }
            "--pending" => {
                let list = args.next().expect("--pending needs a list");
                pendings = list.split(',').map(|n| n.parse().unwrap()).collect();
                assert!(
                    pendings.iter().all(|n| *n >= 1),
                    "--pending values are at least 1"
                );
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let bench = Bench {
        exe: std::env::current_exe().expect("the path of this binary"),
        timing: if quick {
            Timing {
                warmup: Duration::from_millis(300),
                min: Duration::from_secs(1),
                max: Duration::from_secs(2),
                min_samples: 20,
            }
        } else {
            Timing {
                warmup: Duration::from_secs(1),
                min: Duration::from_secs(4),
                max: Duration::from_secs(12),
                min_samples: 200,
            }
        },
        quick,
        pendings,
    };
    if cfg!(debug_assertions) {
        println!("WARNING: debug build; use --release for numbers worth reading\n");
    }
    println!(
        "tursoraft bench: {} accounts, transfer = 2 updates + 1 insert; warm-up {:?}, \
         window {:?}..{:?} and at least {} transfers; max_pending_groups {:?}{}\n",
        ACCOUNTS,
        bench.timing.warmup,
        bench.timing.min,
        bench.timing.max,
        bench.timing.min_samples,
        bench.pendings,
        if quick { " (quick)" } else { "" }
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    rt.block_on(async {
        for section in &only {
            let t = Instant::now();
            match section.as_str() {
                "uniform" => bench.uniform().await,
                "followers" => bench.followers().await,
                "geo" => bench.geo().await,
                "failover" => bench.failover().await,
                _ => unreachable!(),
            }
            println!("({section} took {:.0} s)\n", t.elapsed().as_secs_f64());
        }
    });
    println!("total {:.0} s", started.elapsed().as_secs_f64());
}

struct Bench {
    exe: PathBuf,
    timing: Timing,
    quick: bool,
    /// The `max_pending_groups` values each table is measured with.
    pendings: Vec<usize>,
}

#[derive(Clone, Copy)]
struct Timing {
    warmup: Duration,
    min: Duration,
    max: Duration,
    min_samples: usize,
}

const HEADER: &str = "| p50 ms | p99 ms | n | tx/s | tx/entry | lag p50 ms | lag max ms | behind max | rej | unk | unav | resets |";
const RULE: &str = "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|";
const PLACES: [&str; 4] = ["Frankfurt", "Amsterdam", "N. Virginia", "Singapore"];

impl Bench {
    /// A cluster whose nodes run with `max_pending_groups = pending`.
    fn cluster(
        &self,
        ids: &[NodeId],
        pending: usize,
        change: impl FnOnce(&mut tursoraft_raft::ClusterConfig),
    ) -> Cluster {
        let mut c = Cluster::new(&self.exe, ids, change);
        c.node_args = vec!["--max-pending-groups".into(), pending.to_string()];
        c
    }

    async fn uniform(&self) {
        println!("## 3 voters, the same round trip between all pairs\n");
        println!("| rtt ms | pending | mode | at | writers {HEADER}");
        println!("|---:|---:|---|---|---:{RULE}");
        let rtts: &[f64] = if self.quick {
            &[0.0, 10.0]
        } else {
            &[0.0, 1.0, 10.0, 50.0, 100.0, 250.0]
        };
        let writer_counts: &[usize] = if self.quick { &[1, 16] } else { &[1, 16, 128] };
        for &rtt in rtts {
            for &pending in &self.pendings {
                let mut c = self.cluster(&[1, 2, 3], pending, |cfg| {
                    cfg.delays = DelayTable::uniform(rtt)
                });
                c.start_all();
                let leader = c.initialize(&[1, 2, 3]).await;
                create_schema(&c, leader).await;
                let followers: Vec<NodeId> =
                    [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
                for mode in [CommitMode::Quorum, CommitMode::Async] {
                    for (at_name, at) in [("leader", leader), ("follower", followers[0])] {
                        for &w in writer_counts {
                            let tag = format!("u{rtt}-{mode:?}-{at_name}-{w}");
                            let m = self
                                .measure(&c, leader, &followers, vec![(at, w)], mode, &tag)
                                .await;
                            println!(
                                "| {rtt} | {pending} | {mode:?} | {at_name} | {w} {}",
                                m.row(&m.all)
                            );
                        }
                    }
                }
            }
        }
        println!();
    }

    async fn followers(&self) {
        println!("## 3 voters at 10 ms, with non-voting followers (also 10 ms away), writers at the leader\n");
        println!("| non-voters | pending | mode | writers {HEADER}");
        println!("|---:|---:|---|---:{RULE}");
        let counts: &[u64] = if self.quick { &[0, 2] } else { &[0, 2, 4, 10] };
        let cells: &[(CommitMode, usize)] = if self.quick {
            &[(CommitMode::Quorum, 1)]
        } else {
            &[
                (CommitMode::Quorum, 1),
                (CommitMode::Quorum, 16),
                (CommitMode::Quorum, 128),
                (CommitMode::Async, 1),
                (CommitMode::Async, 128),
            ]
        };
        for &k in counts {
            for &pending in &self.pendings {
                let ids: Vec<NodeId> = (1..=3 + k).collect();
                let mut c =
                    self.cluster(&ids, pending, |cfg| cfg.delays = DelayTable::uniform(10.0));
                c.start_all();
                let leader = c.initialize(&[1, 2, 3]).await;
                for id in 4..=3 + k {
                    c.client(leader)
                        .raft()
                        .add_follower(id, c.addr(id))
                        .await
                        .unwrap();
                }
                create_schema(&c, leader).await;
                c.converge(leader, &ids).await;
                let followers: Vec<NodeId> = ids.iter().copied().filter(|n| *n != leader).collect();
                for &(mode, w) in cells {
                    let tag = format!("f{k}-{mode:?}-{w}");
                    let m = self
                        .measure(&c, leader, &followers, vec![(leader, w)], mode, &tag)
                        .await;
                    println!("| {k} | {pending} | {mode:?} | {w} {}", m.row(&m.all));
                }
            }
        }
        println!();
    }

    async fn geo(&self) {
        // 1 Frankfurt, 2 Amsterdam, 3 N. Virginia: voters. 4 Singapore: non-voter.
        let pairs = [
            (1, 2, 11.0),
            (2, 3, 85.0),
            (3, 4, 220.0),
            (1, 3, 92.0),
            (1, 4, 162.0),
            (2, 4, 160.0),
        ];
        let delays = DelayTable {
            default_rtt_ms: 0.0,
            pairs: pairs
                .iter()
                .map(|&(a, b, rtt_ms)| PairRtt { a, b, rtt_ms })
                .collect(),
        };
        let timeouts = RaftTimeouts::for_max_rtt(delays.max_rtt());
        println!("## Real distances\n");
        println!(
            "Voters 1 Frankfurt, 2 Amsterdam, 3 N. Virginia; non-voting follower 4 Singapore. \
             Timeouts: heartbeat {} ms, election {}..{} ms.\n",
            timeouts.heartbeat_ms,
            timeouts.election_timeout_min_ms,
            timeouts.election_timeout_max_ms
        );
        let mut table = Vec::new();
        for (i, &pending) in self.pendings.iter().enumerate() {
            table.extend(self.geo_cells(&delays, &pairs, pending, i == 0).await);
        }
        println!("| pending | leader | mode | writers per place | writers in {HEADER}");
        println!("|---:|---|---|---:|---{RULE}");
        for row in table {
            println!("{row}");
        }
        println!();
    }

    /// The rows of the geo table with one `max_pending_groups`. Prints the
    /// measured round trips first if `show_rtts`.
    async fn geo_cells(
        &self,
        delays: &DelayTable,
        pairs: &[(NodeId, NodeId, f64)],
        pending: usize,
        show_rtts: bool,
    ) -> Vec<String> {
        let mut c = self.cluster(&[1, 2, 3, 4], pending, |cfg| cfg.delays = delays.clone());
        c.start_all();
        let first = c.initialize(&[1, 2, 3]).await;
        c.client(first)
            .raft()
            .add_follower(4, c.addr(4))
            .await
            .unwrap();
        create_schema(&c, first).await;
        c.converge(first, &[1, 2, 3, 4]).await;
        if show_rtts {
            print_rtts(&c, pairs).await;
        }
        let mut table = Vec::new();
        let counts: &[usize] = if self.quick { &[1] } else { &[1, 16] };
        for leader in [1u64, 3] {
            c.client(leader)
                .raft()
                .transfer_leader(leader)
                .await
                .unwrap();
            assert_eq!(c.wait_leader(&[1, 2, 3, 4]).await, leader);
            let followers: Vec<NodeId> =
                [1, 2, 3, 4].into_iter().filter(|n| *n != leader).collect();
            for mode in [CommitMode::Quorum, CommitMode::Async] {
                for &w in counts {
                    let tag = format!("g{leader}-{mode:?}-{w}");
                    let m = self
                        .measure(&c, leader, &followers, vec![(1, w), (4, w)], mode, &tag)
                        .await;
                    for place in [1u64, 4] {
                        table.push(format!(
                            "| {pending} | {} | {mode:?} | {w} | {} {}",
                            PLACES[leader as usize - 1],
                            PLACES[place as usize - 1],
                            m.row(&m.by_node[&place])
                        ));
                    }
                }
            }
        }
        table
    }

    async fn failover(&self) {
        println!("## From SIGKILL of the leader to the next acknowledged write\n");
        println!(
            "3 voters, 4 quorum writers on every node. `leader known`: both survivors name the \
             same new leader. `next ack`: the first transfer sent after the kill is acknowledged.\n"
        );
        println!("| rtt ms | pending | heartbeat ms | election timeout ms | kills | leader known p50 ms | leader known max ms | next ack p50 ms | next ack max ms |");
        println!("|---:|---:|---:|---|---:|---:|---:|---:|---:|");
        let kills = if self.quick { 2 } else { 6 };
        for rtt in [0.0, 10.0, 100.0] {
            for &pending in &self.pendings {
                let timeouts = RaftTimeouts::for_max_rtt(Duration::from_secs_f64(rtt / 1000.0));
                let mut c = self.cluster(&[1, 2, 3], pending, |cfg| {
                    cfg.delays = DelayTable::uniform(rtt)
                });
                c.start_all();
                let mut leader = c.initialize(&[1, 2, 3]).await;
                create_schema(&c, leader).await;
                let mut spec = LoadSpec::new(
                    &format!("k{rtt}"),
                    vec![(1, 4), (2, 4), (3, 4)],
                    CommitMode::Quorum,
                    ACCOUNTS,
                );
                spec.timeout = Duration::from_secs(15);
                let load = Load::start(&c, &spec);
                let mut known = Vec::new();
                let mut acked = Vec::new();
                for _ in 0..kills {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    let survivors: Vec<NodeId> =
                        [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
                    let killed_at = Instant::now();
                    c.kill(leader);
                    let known_at = leader_known(&c, &survivors, leader).await;
                    let ack_at = loop {
                        let first = load
                            .attempts()
                            .iter()
                            .filter(|a| a.result.is_ok() && a.started >= killed_at)
                            .map(|a| a.finished)
                            .min();
                        if let Some(t) = first {
                            break t;
                        }
                        assert!(
                            killed_at.elapsed() < Duration::from_secs(60),
                            "no write after the kill"
                        );
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    };
                    known.push(known_at - killed_at);
                    acked.push(ack_at - killed_at);
                    let old = leader;
                    leader = c.wait_leader(&survivors).await;
                    c.start(old);
                    c.converge(leader, &[1, 2, 3]).await;
                }
                load.stop().await;
                known.sort();
                acked.sort();
                println!(
                    "| {rtt} | {pending} | {} | {}..{} | {kills} | {:.0} | {:.0} | {:.0} | {:.0} |",
                    timeouts.heartbeat_ms,
                    timeouts.election_timeout_min_ms,
                    timeouts.election_timeout_max_ms,
                    ms(known[known.len() / 2]),
                    ms(*known.last().unwrap()),
                    ms(acked[acked.len() / 2]),
                    ms(*acked.last().unwrap()),
                );
            }
        }
        println!();
    }

    async fn measure(
        &self,
        c: &Cluster,
        leader: NodeId,
        followers: &[NodeId],
        writers: Vec<(NodeId, usize)>,
        mode: CommitMode,
        tag: &str,
    ) -> Measured {
        let t = self.timing;
        let before = c.client(leader).stats().await.unwrap();
        let load = Load::start(c, &LoadSpec::new(tag, writers, mode, ACCOUNTS));
        tokio::time::sleep(t.warmup).await;
        let start = Instant::now();
        let stop_lag = Arc::new(AtomicBool::new(false));
        let lag = Arc::new(Mutex::new(Vec::new()));
        let samplers: Vec<_> = followers
            .iter()
            .map(|f| {
                tokio::spawn(sample_lag(
                    c.client(leader),
                    c.client(*f),
                    stop_lag.clone(),
                    lag.clone(),
                ))
            })
            .collect();
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let elapsed = start.elapsed();
            let n = load
                .attempts()
                .iter()
                .filter(|a| a.started >= start && a.result.is_ok())
                .count();
            if (elapsed >= t.min && n >= t.min_samples) || elapsed >= t.max {
                break;
            }
        }
        let end = Instant::now();
        stop_lag.store(true, Relaxed);
        for s in samplers {
            s.await.unwrap();
        }
        let finished = load.stop().await;
        let after = c.client(leader).stats().await.unwrap();
        let attempts = finished.attempts;
        let mut by_node = BTreeMap::new();
        let nodes: std::collections::BTreeSet<NodeId> = attempts.iter().map(|a| a.node).collect();
        for node in nodes {
            let mine: Vec<&Attempt> = attempts.iter().filter(|a| a.node == node).collect();
            by_node.insert(node, Latencies::of(&mine, start, end));
        }
        let all: Vec<&Attempt> = attempts.iter().collect();
        let count = |f: fn(&Error) -> bool| {
            attempts
                .iter()
                .filter(|a| a.result.as_ref().is_err_and(f))
                .count()
        };
        let groups = after.groups - before.groups;
        let mut lag = lag.lock().unwrap().clone();
        lag.sort();
        Measured {
            all: Latencies::of(&all, start, end),
            by_node,
            tx_per_entry: (after.transactions - before.transactions) as f64 / groups.max(1) as f64,
            lag_p50: lag.get(lag.len() / 2).map(|l| l.0),
            lag_max: lag.iter().map(|l| l.0).max(),
            behind_max: lag.iter().map(|l| l.1).max(),
            rejected: count(|e| matches!(e, Error::Rejected(_))),
            unknown: count(|e| matches!(e, Error::OutcomeUnknown(_))),
            unavailable: count(|e| matches!(e, Error::Unavailable(_))),
            resets: after.resets - before.resets,
        }
    }
}

async fn print_rtts(c: &Cluster, pairs: &[(NodeId, NodeId, f64)]) {
    println!("Round trips as configured / as measured through the network layer (ms):\n");
    println!("| pair | configured | measured |");
    println!("|---|---:|---:|");
    for &(a, b, rtt) in pairs {
        let mut best = Duration::MAX;
        for _ in 0..5 {
            best = best.min(c.client(a).raft().ping_peer(b).await.unwrap());
        }
        println!(
            "| {} – {} | {rtt} | {:.1} |",
            PLACES[a as usize - 1],
            PLACES[b as usize - 1],
            ms(best)
        );
    }
    println!();
}

async fn create_schema(c: &Cluster, leader: NodeId) {
    c.client(leader)
        .execute(schema(ACCOUNTS), CommitMode::Quorum)
        .await
        .unwrap();
}

/// Waits until all of `survivors` name the same leader other than `old`,
/// and returns when that was first seen.
async fn leader_known(c: &Cluster, survivors: &[NodeId], old: NodeId) -> Instant {
    let clients: Vec<Client> = survivors
        .iter()
        .map(|id| c.client(*id).with_timeout(Duration::from_secs(2)))
        .collect();
    let started = Instant::now();
    loop {
        let mut leaders = Vec::new();
        for client in &clients {
            leaders.push(client.raft().status().await.ok().and_then(|s| s.leader));
        }
        if let Some(Some(l)) = leaders.first() {
            if *l != old && leaders.iter().all(|x| *x == Some(*l)) {
                return Instant::now();
            }
        }
        assert!(started.elapsed() < Duration::from_secs(60), "no new leader");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Every 50 ms: how long until `follower` has applied the leader's last
/// log index, and how many entries it was behind.
async fn sample_lag(
    leader: Client,
    follower: Client,
    stop: Arc<AtomicBool>,
    out: Arc<Mutex<Vec<(Duration, u64)>>>,
) {
    while !stop.load(Relaxed) {
        let t0 = Instant::now();
        let (Ok(l), Ok(f)) = (leader.raft().status().await, follower.raft().status().await) else {
            continue;
        };
        let last = l.last_log_index.unwrap_or(0);
        let behind = last.saturating_sub(f.applied.unwrap_or(0));
        if follower
            .raft()
            .wait_applied(last, Duration::from_secs(30))
            .await
            .is_ok()
        {
            out.lock().unwrap().push((t0.elapsed(), behind));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct Measured {
    all: Latencies,
    by_node: BTreeMap<NodeId, Latencies>,
    tx_per_entry: f64,
    lag_p50: Option<Duration>,
    lag_max: Option<Duration>,
    behind_max: Option<u64>,
    rejected: usize,
    unknown: usize,
    unavailable: usize,
    resets: u64,
}

impl Measured {
    fn row(&self, l: &Latencies) -> String {
        let opt_ms = |d: Option<Duration>| d.map_or("-".to_string(), |d| format!("{:.1}", ms(d)));
        format!(
            "| {} | {} | {} | {:.0} | {:.1} | {} | {} | {} | {} | {} | {} | {} |",
            opt_ms(l.p50),
            opt_ms(l.p99),
            l.n,
            l.tps,
            self.tx_per_entry,
            opt_ms(self.lag_p50),
            opt_ms(self.lag_max),
            self.behind_max.map_or("-".into(), |b| b.to_string()),
            self.rejected,
            self.unknown,
            self.unavailable,
            self.resets
        )
    }
}

struct Latencies {
    n: usize,
    p50: Option<Duration>,
    p99: Option<Duration>,
    tps: f64,
}

impl Latencies {
    /// Of the acknowledged attempts sent in `[start, end]`; `tps` counts the
    /// ones acknowledged in it.
    fn of(attempts: &[&Attempt], start: Instant, end: Instant) -> Latencies {
        let mut lat: Vec<Duration> = attempts
            .iter()
            .filter(|a| a.result.is_ok() && a.started >= start && a.started <= end)
            .map(|a| a.latency())
            .collect();
        lat.sort();
        let acked = attempts
            .iter()
            .filter(|a| a.result.is_ok() && a.finished >= start && a.finished <= end)
            .count();
        let pct = |p: f64| {
            (!lat.is_empty())
                .then(|| lat[((p * lat.len() as f64).ceil() as usize).clamp(1, lat.len()) - 1])
        };
        Latencies {
            n: lat.len(),
            p50: pct(0.5),
            p99: pct(0.99),
            tps: acked as f64 / (end - start).as_secs_f64(),
        }
    }
}
