//! Time to acknowledgement per commit mode, origin and round trip, and what a
//! transaction costs the leader. Slow; run on a release build:
//!
//! ```sh
//! cargo test -p tursoraft-node --release --test latency -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{median, ms, statements, Cluster};
use tursoraft_node::{Client, CommitMode, Statement, Stats, Value};
use tursoraft_raft::{DelayTable, NodeId};

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

async fn start(rtt_ms: f64) -> (Cluster, NodeId, NodeId) {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(rtt_ms));
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
        let (c, leader, follower) = start(rtt).await;
        for (origin, id) in [("leader", leader), ("follower", follower)] {
            for mode in [CommitMode::Quorum, CommitMode::Async] {
                let client = Arc::new(c.client(id));
                let tag = format!("{rtt}-{origin}-{mode:?}");
                sequential(&client, mode, &format!("{tag}-warm"), 5).await;
                let seq = sequential(&client, mode, &format!("{tag}-seq"), n_seq).await;
                let before = c.client(leader).stats().await.unwrap();
                let (conc, wall) = concurrent(client, mode, &format!("{tag}-conc"), n_conc).await;
                let after = c.client(leader).stats().await.unwrap();
                let groups = after.groups - before.groups;
                let line = format!(
                    "| {rtt:>3} ms | {origin:<8} | {mode:<6?} | {:>7.1} | {:>7.1} | {:>7.1} | {:>7.1} | {:>7.0} | {:>5.1} |",
                    ms(median(&seq)),
                    ms(p90(&seq)),
                    ms(median(&conc)),
                    ms(p90(&conc)),
                    conc.len() as f64 / wall.as_secs_f64(),
                    conc.len() as f64 / groups as f64,
                );
                println!("{line}");
                table.push(line);
            }
        }
        let stats = c.client(leader).stats().await.unwrap();
        println!("leader at {rtt} ms: {stats:?}");
        assert_eq!(stats.rejected, 0);
        assert_eq!(stats.async_index_mismatches, 0);
        assert_eq!(stats.async_acked_then_failed, 0);
    }
    println!();
    println!("| RTT | issued at | mode | 1 writer median ms | 1 writer p90 ms | 16 writers median ms | 16 writers p90 ms | 16 writers tx/s | tx per entry |");
    println!("|---|---|---|---|---|---|---|---|---|");
    for line in table {
        println!("{line}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn leader_cost_per_transaction_at_zero_rtt() {
    let (c, leader, follower) = start(0.0).await;
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
            "{what}: {n} tx in {:.0} ms = {:.0} tx/s, {:.2} ms per tx; {} entries; leader capture {:.1} µs/tx, leader apply {:.1} µs/tx ({:.1} µs/entry), follower apply {:.1} µs/tx",
            ms(wall),
            n as f64 / wall.as_secs_f64(),
            ms(wall) / n as f64,
            a.groups - b.groups,
            (a.capture_micros - b.capture_micros) as f64 / captured,
            (a.apply_micros - b.apply_micros) as f64 / applied,
            (a.apply_micros - b.apply_micros) as f64 / (a.applied_entries - b.applied_entries) as f64,
            (fa.apply_micros - fb.apply_micros) as f64 / f_applied,
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
    let (conc, wall) = concurrent(client, CommitMode::Quorum, "conc", 100).await;
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
}
