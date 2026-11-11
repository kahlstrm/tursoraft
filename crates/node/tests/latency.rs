//! Time to acknowledgement per commit mode, origin and round trip, and what a
//! transaction costs the leader, each with pipelining off
//! (`--max-pending-groups 1`) and on (the default, 256). Slow; run on a
//! release build:
//!
//! ```sh
//! cargo test -p tursoraft-node --release --test latency -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{median, ms, statements, Cluster};
use tursoraft_node::ReadLevel;
use tursoraft_node::{Client, CommitMode, Statement, Stats, Value};
use tursoraft_raft::{DelayTable, NodeId, PairRtt};

const WRITERS: usize = 16;

fn insert(tag: &str, i: usize) -> Vec<Statement> {
    vec![Statement::new(
        "INSERT INTO t(tag, v) VALUES (?, ?)",
        vec![
            Value::Text(format!("{tag}-{i}")),
            Value::Text("x".repeat(100)),
        ],
    )]
}

/// Pipelining off and on: the `--max-pending-groups` of every node.
const SETTINGS: [(&str, &str); 2] = [("off", "1"), ("on", "256")];

async fn start(rtt_ms: f64, max_pending: &str) -> (Cluster, NodeId, NodeId) {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(rtt_ms));
    c.node_args = vec!["--max-pending-groups".into(), max_pending.into()];
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT, v TEXT)"]),
            CommitMode::Quorum,
        )
        .await
        .unwrap();
    (c, leader, follower)
}

/// One writer, back-to-back: the time of each write.
async fn sequential(client: &Client, mode: CommitMode, tag: &str, n: usize) -> Vec<Duration> {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        client.execute(insert(tag, i), mode).await.unwrap();
        times.push(start.elapsed());
    }
    times
}

/// `WRITERS` writers, each `n` writes back-to-back: every write's time and
/// the wall time.
async fn concurrent(
    client: Arc<Client>,
    mode: CommitMode,
    tag: &str,
    n: usize,
) -> (Vec<Duration>, Duration) {
    let start = Instant::now();
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let client = client.clone();
            let tag = format!("{tag}-w{w}");
            tokio::spawn(async move { sequential(&client, mode, &tag, n).await })
        })
        .collect();
    let mut times = Vec::new();
    for t in tasks {
        times.extend(t.await.unwrap());
    }
    (times, start.elapsed())
}

fn p90(times: &[Duration]) -> Duration {
    let mut t = times.to_vec();
    t.sort();
    t[t.len() * 9 / 10]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn commit_mode_latency() {
    let mut table = Vec::new();
    for (rtt, n_seq, n_conc) in [(10.0, 100, 20), (100.0, 30, 6)] {
        for (pipelining, max_pending) in SETTINGS {
            let (c, leader, follower) = start(rtt, max_pending).await;
            for (origin, id) in [("leader", leader), ("follower", follower)] {
                for mode in [CommitMode::Quorum, CommitMode::Async] {
                    let client = Arc::new(c.client(id));
                    let tag = format!("{rtt}-{pipelining}-{origin}-{mode:?}");
                    sequential(&client, mode, &format!("{tag}-warm"), 5).await;
                    let b = c.client(leader).stats().await.unwrap();
                    let seq = sequential(&client, mode, &format!("{tag}-seq"), n_seq).await;
                    let m = c.client(leader).stats().await.unwrap();
                    let (conc, wall) =
                        concurrent(client, mode, &format!("{tag}-conc"), n_conc).await;
                    // Let the last groups be decided before reading the
                    // counters of acknowledged async writes.
                    c.converge(leader, &[leader]).await;
                    let a = c.client(leader).stats().await.unwrap();
                    let line = format!(
                        "| {rtt:>3} ms | {pipelining:<3} | {origin:<8} | {mode:<6?} | {:>7.1} | {:>7.1} | {:>5.1} | {:>7.1} | {:>7.1} | {:>7.0} | {:>5.1} | {:>4} | {:>3} | {:>3} |",
                        ms(median(&seq)),
                        ms(p90(&seq)),
                        (m.transactions - b.transactions) as f64 / (m.groups - b.groups) as f64,
                        ms(median(&conc)),
                        ms(p90(&conc)),
                        conc.len() as f64 / wall.as_secs_f64(),
                        (a.transactions - m.transactions) as f64 / (a.groups - m.groups) as f64,
                        a.max_groups_pending,
                        a.async_acked_then_failed,
                        a.async_index_mismatches,
                    );
                    println!("{line}");
                    table.push(line);
                }
            }
            let stats = c.client(leader).stats().await.unwrap();
            println!("leader at {rtt} ms, pipelining {pipelining}: {stats:?}");
            assert_eq!(stats.rejected, 0);
            assert_eq!(stats.async_index_mismatches, 0);
            assert_eq!(stats.async_acked_then_failed, 0);
        }
    }
    println!();
    println!("| RTT | pipelining | issued at | mode | 1 writer median ms | 1 writer p90 ms | 1 writer tx per entry | 16 writers median ms | 16 writers p90 ms | 16 writers tx/s | 16 writers tx per entry | most groups pending | async acked then failed | async wrong index |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for line in table {
        println!("{line}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn leader_cost_per_transaction_at_zero_rtt() {
    for (pipelining, max_pending) in SETTINGS {
        println!("--- pipelining {pipelining}");
        cost_at_zero_rtt(max_pending).await;
    }
}

async fn cost_at_zero_rtt(max_pending: &str) {
    let (c, leader, follower) = start(0.0, max_pending).await;
    let client = Arc::new(c.client(leader));
    sequential(&client, CommitMode::Quorum, "warm", 50).await;

    let report = |what: &str,
                  n: usize,
                  wall: Duration,
                  b: &Stats,
                  a: &Stats,
                  fb: &Stats,
                  fa: &Stats| {
        let captured = (a.captured_transactions - b.captured_transactions) as f64;
        let applied = (a.applied_change_sets - b.applied_change_sets) as f64;
        let f_applied = (fa.applied_change_sets - fb.applied_change_sets) as f64;
        println!(
            "{what}: {n} tx in {:.0} ms = {:.0} tx/s, {:.2} ms per tx; {} entries, up to {} pending; leader capture {:.1} µs/tx, leader apply {:.1} µs/tx ({:.1} µs/entry), follower apply {:.1} µs/tx; {} resets of the ahead file, {:.0} µs each; async acked then failed {}, wrong index {}",
            ms(wall),
            n as f64 / wall.as_secs_f64(),
            ms(wall) / n as f64,
            a.groups - b.groups,
            a.max_groups_pending,
            (a.capture_micros - b.capture_micros) as f64 / captured,
            (a.apply_micros - b.apply_micros) as f64 / applied,
            (a.apply_micros - b.apply_micros) as f64 / (a.applied_entries - b.applied_entries) as f64,
            (fa.apply_micros - fb.apply_micros) as f64 / f_applied,
            a.resets - b.resets,
            (a.reset_micros - b.reset_micros) as f64 / (a.resets - b.resets).max(1) as f64,
            a.async_acked_then_failed,
            a.async_index_mismatches,
        );
    };

    let (b, fb) = (
        c.client(leader).stats().await.unwrap(),
        c.client(follower).stats().await.unwrap(),
    );
    let start = Instant::now();
    let seq = sequential(&client, CommitMode::Quorum, "seq", 500).await;
    let wall = start.elapsed();
    c.converge(leader, &[follower]).await;
    let (a, fa) = (
        c.client(leader).stats().await.unwrap(),
        c.client(follower).stats().await.unwrap(),
    );
    println!(
        "1 writer: median {:.2} ms, p90 {:.2} ms",
        ms(median(&seq)),
        ms(p90(&seq))
    );
    report("1 writer, quorum", 500, wall, &b, &a, &fb, &fa);

    let (b, fb) = (a, fa);
    let (conc, wall) = concurrent(client.clone(), CommitMode::Quorum, "conc", 100).await;
    c.converge(leader, &[follower]).await;
    let (a, fa) = (
        c.client(leader).stats().await.unwrap(),
        c.client(follower).stats().await.unwrap(),
    );
    println!(
        "16 writers: median {:.2} ms, p90 {:.2} ms",
        ms(median(&conc)),
        ms(p90(&conc))
    );
    report("16 writers, quorum", conc.len(), wall, &b, &a, &fb, &fa);

    let (b, fb) = (a, fa);
    let start = Instant::now();
    let seq = sequential(&client, CommitMode::Async, "async", 500).await;
    let wall = start.elapsed();
    c.converge(leader, &[follower]).await;
    let (a, fa) = (
        c.client(leader).stats().await.unwrap(),
        c.client(follower).stats().await.unwrap(),
    );
    println!(
        "1 writer async: median {:.2} ms, p90 {:.2} ms",
        ms(median(&seq)),
        ms(p90(&seq))
    );
    report("1 writer, async", 500, wall, &b, &a, &fb, &fa);
}

/// How a five-statement transaction (reads between writes) is sent.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Shape {
    /// One interactive transaction on the instance; one message at commit.
    Interactive,
    /// One `execute` per statement: five trips to the leader, no isolation.
    Separate,
    /// One `execute` of all five, run by the leader.
    One,
}

fn five(key: &str) -> Vec<Statement> {
    let k = || Value::Text(key.to_string());
    vec![
        Statement::new("SELECT v FROM kv WHERE k = ?", vec![k()]),
        Statement::new(
            "INSERT INTO kv VALUES (?, 1, ?)",
            vec![k(), Value::Text("x".repeat(100))],
        ),
        Statement::new("SELECT v FROM kv WHERE k = ?", vec![k()]),
        Statement::new("UPDATE kv SET v = v + 1 WHERE k = ?", vec![k()]),
        Statement::new("SELECT v FROM kv WHERE k = ?", vec![k()]),
    ]
}

/// Runs one five-statement transaction and checks the last read.
async fn run_five(client: &Client, shape: Shape, mode: CommitMode, key: &str) {
    let stmts = five(key);
    let last = match shape {
        Shape::Interactive => {
            let mut tx = client.begin().await.unwrap();
            let mut last = None;
            for s in stmts {
                last = Some(tx.execute(s).await.unwrap());
            }
            tx.commit(mode).await.unwrap();
            last.unwrap()
        }
        Shape::Separate => {
            let mut last = None;
            for s in stmts {
                last = Some(
                    client
                        .execute(vec![s], mode)
                        .await
                        .unwrap()
                        .results
                        .remove(0),
                );
            }
            last.unwrap()
        }
        Shape::One => client.execute(stmts, mode).await.unwrap().results.remove(4),
    };
    assert_eq!(last.rows, vec![vec![Value::Integer(2)]], "{shape:?} {key}");
}

async fn five_sequential(
    client: &Client,
    shape: Shape,
    mode: CommitMode,
    tag: &str,
    n: usize,
) -> Vec<Duration> {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        run_five(client, shape, mode, &format!("{tag}-{i}")).await;
        times.push(start.elapsed());
    }
    times
}

async fn five_concurrent(
    client: Arc<Client>,
    shape: Shape,
    mode: CommitMode,
    tag: &str,
    n: usize,
) -> (Vec<Duration>, Duration) {
    let start = Instant::now();
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let client = client.clone();
            let tag = format!("{tag}-w{w}");
            tokio::spawn(async move { five_sequential(&client, shape, mode, &tag, n).await })
        })
        .collect();
    let mut times = Vec::new();
    for t in tasks {
        times.extend(t.await.unwrap());
    }
    (times, start.elapsed())
}

/// Nodes 1 and 2 are `near_rtt` apart; node 3, the instance, is `far_rtt`
/// from both. The leader is 1 or 2. Returns the cluster, the leader and the
/// instance.
async fn start_far(far_rtt: f64, near_rtt: f64, max_pending: &str) -> (Cluster, NodeId, NodeId) {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| {
        cfg.delays = DelayTable {
            default_rtt_ms: near_rtt,
            pairs: [1, 2]
                .map(|a| PairRtt {
                    a,
                    b: 3,
                    rtt_ms: far_rtt,
                })
                .to_vec(),
        }
    });
    c.node_args = vec!["--max-pending-groups".into(), max_pending.into()];
    c.start_all();
    let mut leader = c.initialize(&[1, 2, 3]).await;
    if leader == 3 {
        c.client(1).raft().transfer_leader(1).await.unwrap();
        leader = c.wait_leader(&[1, 2, 3]).await;
    }
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER, pad TEXT)"]),
            CommitMode::Quorum,
        )
        .await
        .unwrap();
    c.converge(leader, &[1, 2, 3]).await;
    (c, leader, 3)
}

/// A transaction of five statements with reads between them, issued at an
/// instance far from the leader: as one interactive transaction, as five
/// `execute` calls, and as one `execute`. One writer back to back, and
/// `WRITERS` concurrent writers on different rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn interactive_transaction_latency() {
    let mut table = Vec::new();
    for (label, far, near, n_seq, n_conc) in [
        ("220/10", 220.0, 10.0, 15, 4),
        ("10/10", 10.0, 10.0, 60, 15),
    ] {
        let (c, leader, instance) = start_far(far, near, "256").await;
        let client = Arc::new(c.client(instance));
        for mode in [CommitMode::Quorum, CommitMode::Async] {
            for shape in [Shape::Interactive, Shape::Separate, Shape::One] {
                let tag = format!("{label}-{mode:?}-{shape:?}");
                five_sequential(&client, shape, mode, &format!("{tag}-warm"), 2).await;
                let seq = five_sequential(&client, shape, mode, &format!("{tag}-seq"), n_seq).await;
                let b = c.client(leader).stats().await.unwrap();
                let (conc, wall) =
                    five_concurrent(client.clone(), shape, mode, &format!("{tag}-conc"), n_conc)
                        .await;
                c.converge(leader, &[leader]).await;
                let a = c.client(leader).stats().await.unwrap();
                let i = c.client(instance).stats().await.unwrap();
                let line = format!(
                    "| {label} | {mode:<6?} | {shape:<11?} | {:>7.1} | {:>7.1} | {:>7.1} | {:>7.1} | {:>6.0} | {:>5.1} | {:>3} | {:>3} |",
                    ms(median(&seq)),
                    ms(p90(&seq)),
                    ms(median(&conc)),
                    ms(p90(&conc)),
                    conc.len() as f64 / wall.as_secs_f64(),
                    (a.transactions - b.transactions) as f64 / (a.groups - b.groups) as f64,
                    a.rejected + a.incoming_rejected_early,
                    i.local_aborted,
                );
                println!("{line}");
                table.push(line);
            }
        }
        c.converge(leader, &[1, 2, 3]).await;
        assert_eq!(c.dump(1).await, c.dump(3).await);
        println!(
            "leader at {label}: {:?}",
            c.client(leader).stats().await.unwrap()
        );
        println!(
            "instance at {label}: {:?}",
            c.client(instance).stats().await.unwrap()
        );
    }
    println!();
    println!("| RTT instance-leader / leader-voter ms | mode | shape | 1 writer median ms | 1 writer p90 ms | 16 writers median ms | 16 writers p90 ms | 16 writers tx/s | 16 writers tx per entry | rejected | local aborted |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    for line in table {
        println!("{line}");
    }
}

/// What a change set from a local transaction costs the leader, next to
/// what `execute` of the same transaction costs it, at 0 ms.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn leader_cost_per_incoming_change_set_at_zero_rtt() {
    for (pipelining, max_pending) in SETTINGS {
        let (c, leader, instance) = start_far(0.0, 0.0, max_pending).await;
        let client = Arc::new(c.client(instance));
        for shape in [Shape::One, Shape::Interactive] {
            five_sequential(
                &client,
                shape,
                CommitMode::Quorum,
                &format!("{shape:?}-warm"),
                50,
            )
            .await;
            for writers in [1, WRITERS] {
                let (b, ib) = (
                    c.client(leader).stats().await.unwrap(),
                    c.client(instance).stats().await.unwrap(),
                );
                let tag = format!("{shape:?}-{writers}");
                let (times, wall) = if writers == 1 {
                    let start = Instant::now();
                    let t = five_sequential(&client, shape, CommitMode::Quorum, &tag, 500).await;
                    (t, start.elapsed())
                } else {
                    five_concurrent(client.clone(), shape, CommitMode::Quorum, &tag, 50).await
                };
                c.converge(leader, &[instance]).await;
                let (a, ia) = (
                    c.client(leader).stats().await.unwrap(),
                    c.client(instance).stats().await.unwrap(),
                );
                let per = |micros: u64, n: u64| micros as f64 / n.max(1) as f64;
                println!(
                    "pipelining {pipelining}, {shape:?}, {writers} writers: median {:.2} ms, p90 {:.2} ms, {:.0} tx/s; {} tx in {} entries; leader per tx: capture {:.1} µs, check on the ahead file {:.1} µs, apply {:.1} µs; instance apply {:.1} µs/tx",
                    ms(median(&times)),
                    ms(p90(&times)),
                    times.len() as f64 / wall.as_secs_f64(),
                    a.transactions - b.transactions,
                    a.groups - b.groups,
                    per(a.capture_micros - b.capture_micros, a.captured_transactions - b.captured_transactions),
                    per(a.incoming_micros - b.incoming_micros, a.incoming - b.incoming),
                    per(a.apply_micros - b.apply_micros, a.applied_change_sets - b.applied_change_sets),
                    per(ia.apply_micros - ib.apply_micros, ia.applied_change_sets - ib.applied_change_sets),
                );
            }
        }
    }
}

/// Interactive transactions on two instances and `execute` at the leader
/// all read and increment four hot rows. Counts how many commits are
/// rejected, where (on the leader's ahead file, or at apply in the log), and
/// what the rejections cost in commits per second. The sum of the rows must
/// equal the number of commits: no lost update.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn conflicting_interactive_transactions() {
    for (leader_writers, (pipelining, max_pending)) in
        [0, 4].into_iter().flat_map(|w| SETTINGS.map(|s| (w, s)))
    {
        let (c, leader, instance) = start_far(10.0, 10.0, max_pending).await;
        let other = [1, 2].into_iter().find(|n| *n != leader).unwrap();
        c.client(leader)
            .execute(
                statements(&[
                    "CREATE TABLE hot(id INTEGER PRIMARY KEY, v INTEGER)",
                    "INSERT INTO hot VALUES (0, 0), (1, 0), (2, 0), (3, 0)",
                ]),
                CommitMode::Quorum,
            )
            .await
            .unwrap();
        c.converge(leader, &[1, 2, 3]).await;
        let b = c.client(leader).stats().await.unwrap();
        let start = Instant::now();
        let mut tasks = Vec::new();
        for (node, writers) in [(instance, 8), (other, 8), (leader, leader_writers)] {
            for w in 0..writers {
                let client = c.client(node);
                let interactive = node != leader;
                tasks.push(tokio::spawn(async move {
                    let (mut ok, mut rejected) = (0u64, 0u64);
                    for i in 0..30i64 {
                        let id = Value::Integer((w + i) % 4);
                        let result = if interactive {
                            let mut tx = client.begin().await.unwrap();
                            let read = tx
                                .execute(Statement::new(
                                    "SELECT v FROM hot WHERE id = ?",
                                    vec![id.clone()],
                                ))
                                .await
                                .unwrap();
                            let Value::Integer(v) = read.rows[0][0] else {
                                panic!()
                            };
                            tx.execute(Statement::new(
                                "UPDATE hot SET v = ? WHERE id = ?",
                                vec![Value::Integer(v + 1), id],
                            ))
                            .await
                            .unwrap();
                            tx.commit(CommitMode::Quorum).await.map(|_| ())
                        } else {
                            client
                                .execute(
                                    vec![Statement::new(
                                        "UPDATE hot SET v = v + 1 WHERE id = ?",
                                        vec![id],
                                    )],
                                    CommitMode::Quorum,
                                )
                                .await
                                .map(|_| ())
                        };
                        match result {
                            Ok(()) => ok += 1,
                            Err(tursoraft_node::Error::Rejected(_)) => rejected += 1,
                            Err(e) => panic!("{e:?}"),
                        }
                    }
                    (interactive, ok, rejected)
                }));
            }
        }
        let (mut i_ok, mut i_rej, mut e_ok, mut e_rej) = (0, 0, 0, 0);
        for t in tasks {
            let (interactive, ok, rejected) = t.await.unwrap();
            if interactive {
                i_ok += ok;
                i_rej += rejected;
            } else {
                e_ok += ok;
                e_rej += rejected;
            }
        }
        let wall = start.elapsed();
        c.converge(leader, &[1, 2, 3]).await;
        let a = c.client(leader).stats().await.unwrap();
        let sum = common::count(
            &c.client(leader),
            "SELECT sum(v) FROM hot",
            ReadLevel::Local,
        )
        .await;
        println!(
            "{leader_writers} execute writers at the leader, pipelining {pipelining}: {:.0} commits/s; interactive {i_ok} committed, {i_rej} rejected; execute {e_ok} committed, {e_rej} rejected; at the leader: {} incoming, {} rejected on the ahead file, {} incoming rejected at apply, {} rejected at apply in all, {} resets of the ahead file, {} groups; sum {sum}",
            (i_ok + e_ok) as f64 / wall.as_secs_f64(),
            a.incoming - b.incoming,
            a.incoming_rejected_early - b.incoming_rejected_early,
            a.incoming_rejected_late - b.incoming_rejected_late,
            a.rejected - b.rejected,
            a.resets - b.resets,
            a.groups - b.groups,
        );
        assert_eq!(sum as u64, i_ok + e_ok, "lost update");
        assert_eq!(e_rej, 0);
        assert_eq!(c.dump(1).await, c.dump(2).await);
        assert_eq!(c.dump(1).await, c.dump(3).await);
    }
}
