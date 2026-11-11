//! Measurements of a relay and a follower Durable Object under Miniflare, on
//! one machine: how long after a write's acknowledgement the follower has
//! applied it, how fast it keeps up with many writers, and how long a full
//! copy takes. These say nothing about Cloudflare's network. Slow; run on a
//! release build:
//!
//! ```sh
//! cargo test -p tursoraft-relay --release --test measure -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The time "until applied" ends when the relay receives the follower's
//! acknowledgement, which the follower sends after its transaction has
//! committed; it is an upper bound of the time until applied.

mod common;

use std::time::{Duration, Instant};

use common::*;
use tursoraft_node::{CommitMode, Node, Statement, Value};
use tursoraft_relay::Relay;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn lag_from_acknowledgement_to_applied() {
    const WRITES: usize = 500;
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    println!("| relay on | writes | median ms | p90 ms | p99 ms | max ms |");
    println!("|---|---|---|---|---|---|");
    for (name, on) in [("the leader", leader), ("a follower voter", other)] {
        let relay = edge.relay(c.node(on), FOLLOWER);
        wait_copies(&relay, 1).await;
        let mut lags = Vec::with_capacity(WRITES);
        for i in 0..WRITES {
            let index = write(
                c.node(leader),
                vec![st(
                    "UPDATE products SET stock = ? WHERE product_id = ?",
                    vec![int(i as i64), int(1 + (i as i64 % PRODUCTS))],
                )],
            )
            .await
            .unwrap();
            let acked = Instant::now();
            relay
                .wait_follower(index, Duration::from_secs(10))
                .await
                .unwrap();
            lags.push(acked.elapsed());
            // Writes one at a time, a little apart, as a site that mostly reads.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        lags.sort();
        let ms = |q: f64| lags[((lags.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e3;
        println!(
            "| {name} | {WRITES} | {:.2} | {:.2} | {:.2} | {:.2} |",
            ms(0.5),
            ms(0.9),
            ms(0.99),
            ms(1.0)
        );
        assert_same(&edge, FOLLOWER, c.node(on)).await;
        drop(relay);
    }
}

/// 16 writers on the leader, each inserting `ROWS` rows per transaction:
/// whether one follower keeps up, and its rate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn one_follower_keeps_up_with_16_writers() {
    const WRITERS: usize = 16;
    const PER_WRITER: usize = 300;
    println!("| rows per transaction | transactions | cluster tx/s | follower done after s | follower tx/s | follower rows/s | lag at the end ms | messages | change sets per message |");
    println!("|---|---|---|---|---|---|---|---|---|");
    for rows in [1, 10] {
        let (c, leader, other) = three_nodes(|_| {}).await;
        let edge = Edge::start().await;
        write(
            c.node(leader),
            vec![Statement::from(
                "CREATE TABLE events(id INTEGER PRIMARY KEY, writer INTEGER, n INTEGER, body TEXT)",
            )],
        )
        .await
        .unwrap();
        let relay = edge.relay(c.node(other), FOLLOWER);
        wait_copies(&relay, 1).await;
        let before = relay.stats();
        let started = Instant::now();
        let tasks: Vec<_> = (0..WRITERS)
            .map(|w| {
                let node = c.node(leader).clone();
                tokio::spawn(async move { writer(&node, w, PER_WRITER, rows).await })
            })
            .collect();
        let mut last = 0;
        for t in tasks {
            last = last.max(t.await.unwrap());
        }
        let written = started.elapsed();
        let done = Instant::now();
        relay
            .wait_follower(last, Duration::from_secs(300))
            .await
            .unwrap();
        let lag = done.elapsed();
        let total = started.elapsed();
        let after = relay.stats();
        let txs = (WRITERS * PER_WRITER) as f64;
        let messages = after.acks - before.acks;
        println!(
            "| {rows} | {} | {:.0} | {:.2} | {:.0} | {:.0} | {:.0} | {messages} | {:.1} |",
            WRITERS * PER_WRITER,
            txs / written.as_secs_f64(),
            total.as_secs_f64(),
            txs / total.as_secs_f64(),
            txs * rows as f64 / total.as_secs_f64(),
            lag.as_secs_f64() * 1e3,
            (after.events - before.events) as f64 / messages as f64,
        );
        assert_same(&edge, FOLLOWER, c.node(other)).await;
    }
}

async fn writer(node: &Node, w: usize, n: usize, rows: usize) -> u64 {
    let body = "x".repeat(100);
    let mut index = 0;
    for i in 0..n {
        let marks = vec!["(?, ?, ?)"; rows].join(", ");
        let mut params = Vec::with_capacity(3 * rows);
        for r in 0..rows {
            params.extend([
                int(w as i64),
                int((i * rows + r) as i64),
                Value::Text(body.clone()),
            ]);
        }
        index = node
            .execute(
                vec![st(
                    &format!("INSERT INTO events(writer, n, body) VALUES {marks}"),
                    params,
                )],
                CommitMode::Quorum,
            )
            .await
            .unwrap()
            .index;
    }
    index
}

/// Full copies of databases of rows of about 120 bytes. `COPY_ROWS` sets
/// the sizes, such as `COPY_ROWS=1000000` (default `10000,100000`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; run on a release build with --ignored"]
async fn full_copy() {
    println!(
        "| rows | bytes of row values | database and WAL files MB | copy s | rows/s | messages |"
    );
    println!("|---|---|---|---|---|---|");
    let sizes = std::env::var("COPY_ROWS").unwrap_or_else(|_| "10000,100000".into());
    for rows in sizes.split(',').map(|n| n.trim().parse::<usize>().unwrap()) {
        let (c, leader, other) = three_nodes(|_| {}).await;
        write(
            c.node(leader),
            vec![Statement::from(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, sku TEXT, price INTEGER, title TEXT)",
            )],
        )
        .await
        .unwrap();
        const PER_TX: usize = 200;
        for start in (0..rows).step_by(PER_TX) {
            let marks = vec!["(?, ?, ?)"; PER_TX].join(", ");
            let params = (start..start + PER_TX)
                .flat_map(|i| {
                    [
                        txt(&format!("sku-{i:08}")),
                        int(i as i64 * 7919),
                        txt(&format!("{i:0>100}")),
                    ]
                })
                .collect();
            write(
                c.node(leader),
                vec![st(
                    &format!("INSERT INTO items(sku, price, title) VALUES {marks}"),
                    params,
                )],
            )
            .await
            .unwrap();
        }
        c.converge(leader, &[other]).await;
        let file: u64 = ["data.db", "data.db-wal"]
            .iter()
            .filter_map(|f| std::fs::metadata(c.dir.path().join(format!("node-{other}/{f}"))).ok())
            .map(|m| m.len())
            .sum();
        let edge = Edge::start().await;
        let index = applied(c.node(other)).await;
        let started = Instant::now();
        let relay: Relay = edge.relay(c.node(other), FOLLOWER);
        relay
            .wait_follower(index, Duration::from_secs(600))
            .await
            .unwrap();
        let took = started.elapsed();
        let stats = relay.stats();
        assert_eq!(stats.copies, 1);
        println!(
            "| {rows} | {} | {:.1} | {:.2} | {:.0} | {} |",
            rows * (12 + 8 + 100),
            file as f64 / 1e6,
            took.as_secs_f64(),
            rows as f64 / took.as_secs_f64(),
            stats.acks
        );
        // A dump of more rows does not fit in the follower's memory.
        if rows <= 100_000 {
            assert_same(&edge, FOLLOWER, c.node(other)).await;
        } else {
            assert_eq!(edge.status(FOLLOWER).await.through, index);
        }
    }
}
