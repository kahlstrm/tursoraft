//! What change subscriptions cost: throughput of 16 concurrent writers and
//! the apply time per change set, with 0, 1 and 16 subscribers on every
//! node, at 0 ms round trip. Nodes run in this process. Slow; run on a
//! release build:
//!
//! ```sh
//! cargo test -p tursoraft-node --release --test subscribe_cost -- --ignored --nocapture
//! ```
//!
//! `SUBSCRIBERS` sets the counts to measure, such as `SUBSCRIBERS=0,16`
//! (default `0,1,16`).

mod in_process;

use std::time::Instant;

use in_process::Cluster;
use tursoraft_node::{CommitMode, Node, Statement, Stats, Value};

const WRITERS: usize = 16;
const PER_WRITER: usize = 500;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn subscription_cost_at_zero_rtt() {
    let counts = std::env::var("SUBSCRIBERS").unwrap_or_else(|_| "0,1,16".into());
    println!("| subscribers per node | tx/s | leader apply µs per change set | follower apply µs per change set | leader apply µs per entry | change sets per entry | rows read by subscribers | events missed |");
    println!("|---|---|---|---|---|---|---|---|");
    for n in counts
        .split(',')
        .map(|n| n.trim().parse::<usize>().unwrap())
    {
        measure(n).await;
    }
}

async fn measure(subscribers: usize) {
    let mut c = Cluster::new(&[1, 2, 3], |_| {});
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    let node = c.node(leader).clone();
    node.execute(
        vec![Statement::from(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT, v TEXT)",
        )],
        CommitMode::Quorum,
    )
    .await
    .unwrap();
    let mut readers = Vec::new();
    for id in [1, 2, 3] {
        readers.extend(spawn_subscribers(c.node(id), subscribers));
    }
    writers(&node, "warm", 20).await;
    c.converge(leader, &[1, 2, 3]).await;

    let (b, fb) = (node.stats(), c.node(follower).stats());
    let started = Instant::now();
    writers(&node, "run", PER_WRITER).await;
    let wall = started.elapsed();
    c.converge(leader, &[1, 2, 3]).await;
    let (a, fa) = (node.stats(), c.node(follower).stats());

    for id in [1, 2, 3] {
        c.stop(id).await;
    }
    let (mut rows, mut lagged) = (0, 0);
    for r in readers {
        let (n, l) = r.await.unwrap();
        rows += n;
        lagged += l;
    }
    let tx = (WRITERS * PER_WRITER) as f64;
    println!(
        "| {subscribers:>2} | {:>6.0} | {:>5.1} | {:>5.1} | {:>5.1} | {:>4.1} | {rows} | {lagged} |",
        tx / wall.as_secs_f64(),
        per_change_set(&b, &a),
        per_change_set(&fb, &fa),
        (a.apply_micros - b.apply_micros) as f64 / (a.applied_entries - b.applied_entries) as f64,
        (a.applied_change_sets - b.applied_change_sets) as f64
            / (a.applied_entries - b.applied_entries) as f64,
    );
}

/// Apply time per change set, µs.
fn per_change_set(before: &Stats, after: &Stats) -> f64 {
    (after.apply_micros - before.apply_micros) as f64
        / (after.applied_change_sets - before.applied_change_sets) as f64
}

/// `WRITERS` writers on `node`, `n` inserts each, back to back.
async fn writers(node: &Node, tag: &str, n: usize) {
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let node = node.clone();
            let tag = format!("{tag}-{w}");
            tokio::spawn(async move {
                for i in 0..n {
                    let insert = Statement::new(
                        "INSERT INTO t(tag, v) VALUES (?, ?)",
                        vec![
                            Value::Text(format!("{tag}-{i}")),
                            Value::Text("x".repeat(100)),
                        ],
                    );
                    node.execute(vec![insert], CommitMode::Quorum)
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
}

// Subscribers: the only code that uses the subscription API, so that the
// baseline copy of this file replaces this function alone.
/// `n` subscribers of `node`, each reading every row of every event until
/// the node stops. Each returns the rows it read and the events it missed.
fn spawn_subscribers(node: &Node, n: usize) -> Vec<tokio::task::JoinHandle<(u64, u64)>> {
    (0..n)
        .map(|_| {
            let (_, mut events) = node.subscribe();
            tokio::spawn(async move {
                let (mut rows, mut missed) = (0, 0);
                while let Some(event) = events.recv().await {
                    match event {
                        tursoraft_node::Event::Changes(c) => {
                            rows += c.rows.iter().filter(|r| r.after.is_some()).count() as u64
                        }
                        tursoraft_node::Event::Lagged { missed: m } => missed += m,
                        tursoraft_node::Event::Reset { .. } => {}
                    }
                }
                (rows, missed)
            })
        })
        .collect()
}
