//! Clusters of `tursoraft-node` processes over real TCP.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{count, rows, statements, Cluster};
use tursoraft_node::{CommitMode, Error, ReadLevel, Statement, Value};
use tursoraft_raft::{DelayTable, LogConfig, NodeId, PairRtt};

const QUORUM: CommitMode = CommitMode::Quorum;

#[tokio::test(flavor = "multi_thread")]
async fn writes_on_every_node_reach_every_node() {
    let mut c = Cluster::new(&[1, 2, 3], |_| {});
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let followers: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
    println!("leader {leader}, followers {followers:?}");

    // A schema change on a follower.
    let w = c
        .client(followers[0])
        .execute(
            statements(&[
                "CREATE TABLE accounts(id INTEGER PRIMARY KEY, owner TEXT NOT NULL UNIQUE, balance INTEGER NOT NULL)",
                "CREATE INDEX accounts_balance ON accounts(balance)",
            ]),
            QUORUM,
        )
        .await
        .unwrap();
    println!("schema at index {}", w.index);

    // A transaction of several statements on the other follower, with
    // parameters and a read of its own write.
    let w = c
        .client(followers[1])
        .execute(
            vec![
                Statement::new(
                    "INSERT INTO accounts(owner, balance) VALUES (?, ?)",
                    vec![Value::Text("alice".into()), Value::Integer(100)],
                ),
                Statement::new(
                    "INSERT INTO accounts(owner, balance) VALUES (?, ?)",
                    vec![Value::Text("bob".into()), Value::Integer(50)],
                ),
                "UPDATE accounts SET balance = balance - 30 WHERE owner = 'alice'".into(),
                "UPDATE accounts SET balance = balance + 30 WHERE owner = 'bob'".into(),
                "SELECT owner, balance FROM accounts ORDER BY owner".into(),
            ],
            QUORUM,
        )
        .await
        .unwrap();
    println!("transfer at index {}: {:?}", w.index, w.results[4].rows);
    assert_eq!(w.results[0].rows_changed, 1);
    assert_eq!(
        w.results[4].rows,
        vec![
            vec![Value::Text("alice".into()), Value::Integer(70)],
            vec![Value::Text("bob".into()), Value::Integer(80)],
        ]
    );
    // A quorum write on a follower is applied there when it returns.
    assert_eq!(
        count(
            &c.client(followers[1]),
            "SELECT count(*) FROM accounts",
            ReadLevel::Local
        )
        .await,
        2
    );

    // On the leader: another schema change, then rows in the new column.
    c.client(leader)
        .execute(
            statements(&["ALTER TABLE accounts ADD COLUMN note TEXT"]),
            QUORUM,
        )
        .await
        .unwrap();
    for id in [1, 2, 3] {
        c.client(id)
            .execute(
                vec![Statement::new(
                    "INSERT INTO accounts(owner, balance, note) VALUES (?, 0, ?)",
                    vec![
                        Value::Text(format!("from-{id}")),
                        Value::Text(format!("written on node {id}")),
                    ],
                )],
                QUORUM,
            )
            .await
            .unwrap();
    }

    c.converge(leader, &[1, 2, 3]).await;
    let dumps = [c.dump(1).await, c.dump(2).await, c.dump(3).await];
    println!("node 1:\n{}", dumps[0]);
    assert!(dumps[0].contains("written on node 3"));
    assert!(dumps[0].contains("accounts_balance"));
    assert_eq!(dumps[0], dumps[1], "nodes 1 and 2 differ");
    assert_eq!(dumps[0], dumps[2], "nodes 1 and 3 differ");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_transaction_returns_its_error_and_leaves_nothing() {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(20.0));
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    c.client(leader)
        .execute(
            statements(&[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
                "CREATE TABLE side(x INTEGER)",
                "INSERT INTO t(name) VALUES ('taken')",
            ]),
            QUORUM,
        )
        .await
        .unwrap();
    let before = c.client(leader).stats().await.unwrap();

    // Twenty transactions at once from the follower, so they reach the
    // leader together; number 7 writes a row and then violates UNIQUE.
    let client = Arc::new(c.client(follower));
    let tasks: Vec<_> = (0..20)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                let stmts = if i == 7 {
                    statements(&[
                        "INSERT INTO side VALUES (-7)",
                        "INSERT INTO t(name) VALUES ('taken')",
                    ])
                } else {
                    vec![
                        Statement::new("INSERT INTO side VALUES (?)", vec![Value::Integer(i)]),
                        Statement::new(
                            "INSERT INTO t(name) VALUES (?)",
                            vec![Value::Text(format!("n{i}"))],
                        ),
                    ]
                };
                (i, client.execute(stmts, QUORUM).await)
            })
        })
        .collect();
    let mut failed = Vec::new();
    for t in tasks {
        let (i, res) = t.await.unwrap();
        match res {
            Ok(_) => {}
            Err(e) => failed.push((i, e)),
        }
    }
    println!("failed: {failed:?}");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].0, 7);
    let Error::Transaction(msg) = &failed[0].1 else {
        panic!("expected a transaction error, got {:?}", failed[0].1)
    };
    assert!(msg.contains("UNIQUE"), "{msg}");

    let after = c.client(leader).stats().await.unwrap();
    let groups = after.groups - before.groups;
    println!(
        "leader: {} groups for {} transactions, {} failed, {} rejected",
        groups,
        after.transactions - before.transactions,
        after.failed - before.failed,
        after.rejected
    );
    assert!(groups < 19, "the transactions did not share groups");
    assert_eq!(after.rejected, 0);

    c.converge(leader, &[1, 2, 3]).await;
    for id in [1, 2, 3] {
        let client = c.client(id);
        assert_eq!(
            count(
                &client,
                "SELECT count(*) FROM side WHERE x = -7",
                ReadLevel::Local
            )
            .await,
            0
        );
        assert_eq!(
            count(&client, "SELECT count(*) FROM side", ReadLevel::Local).await,
            19
        );
        assert_eq!(
            count(&client, "SELECT count(*) FROM t", ReadLevel::Local).await,
            20
        );
    }
    assert_eq!(c.dump(1).await, c.dump(2).await);
    assert_eq!(c.dump(1).await, c.dump(3).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_without_keys_all_commit_in_fewer_entries() {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(10.0));
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE events(id INTEGER PRIMARY KEY, node INTEGER, writer INTEGER, seq INTEGER)"]),
            QUORUM,
        )
        .await
        .unwrap();
    let before = c.client(leader).stats().await.unwrap();
    let first_index = c.status(leader).await.last_log_index.unwrap();

    // Four writers per node, 25 transactions each: 100 per node.
    let mut tasks = Vec::new();
    for node in [1u64, 2, 3] {
        for writer in 0..4i64 {
            let client = c.client(node);
            tasks.push(tokio::spawn(async move {
                let mut errors = Vec::new();
                for seq in 0..25i64 {
                    let stmt = Statement::new(
                        "INSERT INTO events(node, writer, seq) VALUES (?, ?, ?)",
                        vec![
                            Value::Integer(node as i64),
                            Value::Integer(writer),
                            Value::Integer(seq),
                        ],
                    );
                    if let Err(e) = client.execute(vec![stmt], QUORUM).await {
                        errors.push(e);
                    }
                }
                errors
            }));
        }
    }
    let mut errors = Vec::new();
    for t in tasks {
        errors.extend(t.await.unwrap());
    }
    assert!(errors.is_empty(), "{errors:?}");

    let after = c.client(leader).stats().await.unwrap();
    let last_index = c.converge(leader, &[1, 2, 3]).await;
    let transactions = after.transactions - before.transactions;
    let groups = after.groups - before.groups;
    println!(
        "{transactions} transactions in {groups} entries (log {first_index}..{last_index}), {} rejected",
        after.rejected
    );
    assert_eq!(transactions, 300);
    assert_eq!(after.rejected, 0);
    assert!(
        last_index - first_index < 300,
        "the log holds one entry per transaction"
    );
    for id in [1, 2, 3] {
        let client = c.client(id);
        assert_eq!(
            count(&client, "SELECT count(*) FROM events", ReadLevel::Local).await,
            300
        );
        assert_eq!(
            count(
                &client,
                "SELECT count(DISTINCT node || '-' || writer || '-' || seq) FROM events",
                ReadLevel::Local
            )
            .await,
            300
        );
    }
    assert_eq!(c.dump(1).await, c.dump(2).await);
    assert_eq!(c.dump(1).await, c.dump(3).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_levels_on_a_far_follower() {
    // Node 3 is 400 ms away from nodes 1 and 2; 1 and 2 are 10 ms apart.
    let mut c = Cluster::new(&[1, 2, 3], |cfg| {
        cfg.delays = DelayTable {
            default_rtt_ms: 10.0,
            pairs: vec![
                PairRtt {
                    a: 1,
                    b: 3,
                    rtt_ms: 400.0,
                },
                PairRtt {
                    a: 2,
                    b: 3,
                    rtt_ms: 400.0,
                },
            ],
        };
    });
    c.start_all();
    let mut leader = c.initialize(&[1, 2, 3]).await;
    if leader == 3 {
        c.client(1).raft().transfer_leader(1).await.unwrap();
        leader = c.wait_leader(&[1, 2, 3]).await;
    }
    println!("leader {leader}");
    let far = c.client(3);
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER)"]),
            QUORUM,
        )
        .await
        .unwrap();
    c.converge(leader, &[3]).await;

    // Local: stale right after a write acknowledged by the majority 1+2.
    let w = c
        .client(leader)
        .execute(statements(&["INSERT INTO kv VALUES ('a', 1)"]), QUORUM)
        .await
        .unwrap();
    let local = rows(&far, "SELECT v FROM kv WHERE k = 'a'", ReadLevel::Local).await;
    println!(
        "local read on node 3 right after the write at {}: {local:?}",
        w.index
    );
    assert!(local.is_empty(), "the far follower already had the write");

    // At the write's index: waits for it.
    let start = std::time::Instant::now();
    let at = rows(
        &far,
        "SELECT v FROM kv WHERE k = 'a'",
        ReadLevel::AtIndex(w.index),
    )
    .await;
    println!(
        "read at index {} on node 3: {at:?} after {:?}",
        w.index,
        start.elapsed()
    );
    assert_eq!(at, vec![vec![Value::Integer(1)]]);

    // Linearizable: sees the latest write without being told its index.
    c.client(leader)
        .execute(statements(&["UPDATE kv SET v = 2 WHERE k = 'a'"]), QUORUM)
        .await
        .unwrap();
    let local = rows(&far, "SELECT v FROM kv WHERE k = 'a'", ReadLevel::Local).await;
    let start = std::time::Instant::now();
    let lin = rows(
        &far,
        "SELECT v FROM kv WHERE k = 'a'",
        ReadLevel::Linearizable,
    )
    .await;
    println!(
        "local {local:?}, linearizable {lin:?} after {:?}",
        start.elapsed()
    );
    assert_eq!(local, vec![vec![Value::Integer(1)]]);
    assert_eq!(lin, vec![vec![Value::Integer(2)]]);

    // The read path refuses anything that is not one SELECT.
    for sql in [
        "INSERT INTO kv VALUES ('b', 1)",
        "UPDATE kv SET v = 3",
        "DELETE FROM kv",
        "CREATE TABLE x(a)",
        "DROP TABLE kv",
        "PRAGMA query_only = 0",
        "PRAGMA journal_mode = delete",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "BEGIN IMMEDIATE",
        "SELECT 1; DELETE FROM kv",
        "WITH x AS (SELECT 1) DELETE FROM kv",
    ] {
        let res = far.query(Statement::from(sql), ReadLevel::Local).await;
        println!("{sql:<40} -> {res:?}");
        assert!(matches!(res, Err(Error::Read(_))), "{sql}: {res:?}");
    }
    c.converge(leader, &[1, 2, 3]).await;
    for id in [1, 2, 3] {
        let all = rows(&c.client(id), "SELECT k, v FROM kv", ReadLevel::Local).await;
        assert_eq!(all, vec![vec![Value::Text("a".into()), Value::Integer(2)]]);
    }
    assert_eq!(c.dump(1).await, c.dump(3).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_node_joins_from_a_snapshot_and_a_killed_node_catches_up() {
    let mut c = Cluster::new(&[1, 2, 3, 4], |cfg| {
        cfg.log = LogConfig {
            snapshot_every: 20,
            keep_after_snapshot: 50,
            ..LogConfig::default()
        };
    });
    for id in [1, 2, 3] {
        c.start(id);
    }
    let leader = c.initialize(&[1, 2, 3]).await;
    let client = c.client(leader);
    client
        .execute(
            statements(&["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"]),
            QUORUM,
        )
        .await
        .unwrap();
    for i in 0..60 {
        client
            .execute(
                vec![Statement::new(
                    "INSERT INTO t(v) VALUES (?)",
                    vec![Value::Text(format!("v{i}"))],
                )],
                QUORUM,
            )
            .await
            .unwrap();
    }
    let status = c.status(leader).await;
    println!(
        "leader before join: snapshot {:?}, purged {:?}, last {:?}",
        status.snapshot, status.purged, status.last_log_index
    );
    assert!(status.purged.is_some(), "the log was not compacted");

    c.start(4);
    c.client(leader)
        .raft()
        .add_follower(4, c.addr(4))
        .await
        .unwrap();
    for i in 60..70 {
        client
            .execute(
                vec![Statement::new(
                    "INSERT INTO t(v) VALUES (?)",
                    vec![Value::Text(format!("v{i}"))],
                )],
                QUORUM,
            )
            .await
            .unwrap();
    }
    c.converge(leader, &[1, 2, 3, 4]).await;
    let joined = c.client(4).stats().await.unwrap();
    println!("node 4: {joined:?}");
    assert!(joined.snapshots_installed >= 1, "node 4 got no snapshot");
    assert_eq!(
        count(&c.client(4), "SELECT count(*) FROM t", ReadLevel::Local).await,
        70
    );
    assert_eq!(c.dump(4).await, c.dump(leader).await);

    // SIGKILL a follower, write on, restart it from its files.
    let victim = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    let before = c.status(victim).await.applied;
    c.kill(victim);
    for i in 70..100 {
        client
            .execute(
                vec![Statement::new(
                    "INSERT INTO t(v) VALUES (?)",
                    vec![Value::Text(format!("v{i}"))],
                )],
                QUORUM,
            )
            .await
            .unwrap();
    }
    c.start(victim);
    let restarted = c.status(victim).await;
    println!(
        "node {victim} applied {before:?} when killed, {:?} right after restart",
        restarted.applied
    );
    assert!(restarted.applied >= before, "the node lost applied entries");
    c.converge(leader, &[1, 2, 3, 4]).await;
    assert_eq!(
        count(
            &c.client(victim),
            "SELECT count(*) FROM t",
            ReadLevel::Local
        )
        .await,
        100
    );
    let caught_up = c.client(victim).stats().await.unwrap();
    println!(
        "node {victim} after catching up: {} entries applied, {} snapshots installed since restart",
        caught_up.applied_entries, caught_up.snapshots_installed
    );
    // The log still held everything after its position, so it caught up
    // from entries alone.
    assert_eq!(caught_up.snapshots_installed, 0);
    let reference = c.dump(leader).await;
    for id in [1, 2, 3, 4] {
        assert_eq!(c.dump(id).await, reference, "node {id} differs");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_killed_while_clients_write_loses_no_quorum_write() {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(10.0));
    c.start_all();
    let old_leader = c.initialize(&[1, 2, 3]).await;
    c.client(old_leader)
        .execute(statements(&["CREATE TABLE w(k TEXT PRIMARY KEY)"]), QUORUM)
        .await
        .unwrap();

    let acked = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let unknown = Arc::new(Mutex::new(Vec::<String>::new()));
    let acked_after_kill = Arc::new(Mutex::new(0u64));
    let killed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for node in [1u64, 2, 3] {
        for w in 0..3 {
            let client = c.client(node).with_timeout(Duration::from_secs(15));
            let (acked, unknown, acked_after_kill, killed, stop) = (
                acked.clone(),
                unknown.clone(),
                acked_after_kill.clone(),
                killed.clone(),
                stop.clone(),
            );
            writers.push(tokio::spawn(async move {
                let mut seq = 0;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("n{node}-w{w}-{seq}");
                    seq += 1;
                    let stmt =
                        Statement::new("INSERT INTO w VALUES (?)", vec![Value::Text(key.clone())]);
                    match client.execute(vec![stmt], QUORUM).await {
                        Ok(_) => {
                            acked.lock().unwrap().insert(key);
                            if killed.load(Ordering::Relaxed) {
                                *acked_after_kill.lock().unwrap() += 1;
                            }
                        }
                        Err(Error::OutcomeUnknown(_)) => unknown.lock().unwrap().push(key),
                        Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                    }
                }
            }));
        }
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before_kill = acked.lock().unwrap().len();
    c.kill(old_leader);
    killed.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        let _ = w.await;
    }

    let survivors: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != old_leader).collect();
    let new_leader = c.wait_leader(&survivors).await;
    c.converge(new_leader, &survivors).await;
    let acked = acked.lock().unwrap().clone();
    let after_kill = *acked_after_kill.lock().unwrap();
    println!(
        "old leader {old_leader}, new leader {new_leader}; {before_kill} acknowledged before the kill, {} in all, {after_kill} after the kill; {} with unknown outcome",
        acked.len(),
        unknown.lock().unwrap().len()
    );
    assert_ne!(new_leader, old_leader);
    assert!(
        after_kill > 0,
        "no write was acknowledged through the new leader"
    );
    for id in &survivors {
        let present: BTreeSet<String> = rows(&c.client(*id), "SELECT k FROM w", ReadLevel::Local)
            .await
            .into_iter()
            .map(|r| match &r[0] {
                Value::Text(s) => s.clone(),
                v => panic!("{v:?}"),
            })
            .collect();
        let missing: Vec<_> = acked.difference(&present).collect();
        assert!(
            missing.is_empty(),
            "node {id} lacks acknowledged writes {missing:?}"
        );
    }
    assert_eq!(c.dump(survivors[0]).await, c.dump(survivors[1]).await);
}
