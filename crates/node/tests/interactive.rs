//! Interactive transactions over real TCP: statements run one at a time on
//! the node that began the transaction, and the commit sends its change set
//! to the leader.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{count, rows, statements, Cluster};
use tursoraft_node::{Client, CommitMode, Error, ReadLevel, Statement, Value};
use tursoraft_raft::{DelayTable, NodeId};

const QUORUM: CommitMode = CommitMode::Quorum;

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

async fn start(rtt_ms: f64, args: &[&str]) -> (Cluster, NodeId, Vec<NodeId>) {
    let mut c = Cluster::new(&[1, 2, 3], |cfg| cfg.delays = DelayTable::uniform(rtt_ms));
    c.node_args = args.iter().map(|a| a.to_string()).collect();
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let followers = [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
    (c, leader, followers)
}

/// Runs `sqls` in one interactive transaction on `client` and commits it.
async fn interactive(
    client: &Client,
    sqls: Vec<Statement>,
    mode: CommitMode,
) -> Result<u64, Error> {
    let mut tx = client.begin().await?;
    for s in sqls {
        tx.execute(s).await?;
    }
    tx.commit(mode).await.map(|w| w.index)
}

/// Two instances change the same row at the same time: one commits and the
/// other is rejected. Changes to different rows both commit. A unique key
/// written on two instances, and a key the database picked on two instances,
/// give one rejection. Every node agrees, with the leader's pipelining on
/// (the leader rejects on its ahead file) and off (apply rejects).
#[tokio::test(flavor = "multi_thread")]
async fn interactive_transactions_on_two_instances_conflict_by_rows() {
    for max_pending in ["256", "1"] {
        let (c, leader, f) = start(20.0, &["--max-pending-groups", max_pending]).await;
        c.client(leader)
            .execute(
                statements(&[
                    "CREATE TABLE acct(id INTEGER PRIMARY KEY, balance INTEGER)",
                    "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
                    "CREATE TABLE events(id INTEGER PRIMARY KEY, what TEXT)",
                    "INSERT INTO acct VALUES (1, 100), (2, 100)",
                ]),
                QUORUM,
            )
            .await
            .unwrap();
        c.converge(leader, &[1, 2, 3]).await;
        let (a, b) = (c.client(f[0]), c.client(f[1]));

        // Read, compute in the application, write: both read 100.
        let (r30, r50) = tokio::join!(withdraw(&a, 30), withdraw(&b, 50));
        println!("pipelining {max_pending}: same row: {r30:?} / {r50:?}");
        let committed = [&r30, &r50].iter().filter(|r| r.is_ok()).count();
        assert_eq!(committed, 1, "{r30:?} {r50:?}");
        let rejected = [&r30, &r50]
            .iter()
            .filter(|r| matches!(r, Err(Error::Rejected(_))))
            .count();
        assert_eq!(rejected, 1);

        // Both instances have applied the winner before they begin.
        c.converge(leader, &[1, 2, 3]).await;
        let (r1, r2) = tokio::join!(
            interactive(
                &a,
                statements(&["UPDATE acct SET balance = 1 WHERE id = 1"]),
                QUORUM
            ),
            interactive(
                &b,
                statements(&["UPDATE acct SET balance = 2 WHERE id = 2"]),
                QUORUM
            ),
        );
        println!("different rows: {r1:?} / {r2:?}");
        r1.unwrap();
        r2.unwrap();

        let (u1, u2) = tokio::join!(
            interactive(
                &a,
                statements(&["INSERT INTO users VALUES (10, 'e@x')"]),
                QUORUM
            ),
            interactive(
                &b,
                statements(&["INSERT INTO users VALUES (20, 'e@x')"]),
                QUORUM
            ),
        );
        println!("unique: {u1:?} / {u2:?}");
        assert_eq!([&u1, &u2].iter().filter(|r| r.is_ok()).count(), 1);
        let unique = [&u1, &u2]
            .into_iter()
            .find_map(|r| r.as_ref().err())
            .unwrap();
        assert!(
            matches!(unique, Error::Rejected(m) if m.contains("UNIQUE") || m.contains("conflict")),
            "{unique:?}"
        );

        // LIMIT: keys the database picks collide across instances.
        let (k1, k2) = tokio::join!(
            interactive(
                &a,
                statements(&["INSERT INTO events(what) VALUES ('a')"]),
                QUORUM
            ),
            interactive(
                &b,
                statements(&["INSERT INTO events(what) VALUES ('b')"]),
                QUORUM
            ),
        );
        println!("database-picked keys: {k1:?} / {k2:?}");
        assert_eq!([&k1, &k2].iter().filter(|r| r.is_ok()).count(), 1);

        c.converge(leader, &[1, 2, 3]).await;
        let stats = c.client(leader).stats().await.unwrap();
        println!(
            "leader: incoming {}, rejected on the ahead file {}, rejected at apply {}",
            stats.incoming, stats.incoming_rejected_early, stats.rejected
        );
        assert_eq!(stats.incoming, 8);
        if max_pending == "1" {
            assert_eq!(stats.incoming_rejected_early, 0);
            assert_eq!(stats.rejected, 3);
        } else {
            assert_eq!(stats.incoming_rejected_early + stats.rejected, 3);
        }
        let balance = rows(
            &c.client(leader),
            "SELECT balance FROM acct ORDER BY id",
            ReadLevel::Local,
        )
        .await;
        assert_eq!(
            balance,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
        assert_eq!(c.dump(1).await, c.dump(2).await);
        assert_eq!(c.dump(1).await, c.dump(3).await);
    }
}

/// Reads the balance of account 1, then writes it less `amount`.
async fn withdraw(client: &Client, amount: i64) -> Result<u64, Error> {
    let mut tx = client.begin().await?;
    let read = tx
        .execute("SELECT balance FROM acct WHERE id = 1".into())
        .await?;
    let Value::Integer(balance) = read.rows[0][0] else {
        panic!("{read:?}")
    };
    tx.execute(Statement::new(
        "UPDATE acct SET balance = ? WHERE id = 1",
        vec![Value::Integer(balance - amount)],
    ))
    .await?;
    tx.commit(QUORUM).await.map(|w| w.index)
}

/// Interactive commits from a follower and `execute` at the leader take
/// turns on one row. Each reads the other's write, so none is rejected. The
/// leader must put the follower's change sets into its ahead file, or its
/// next capture there reads the row as it was before them.
#[tokio::test(flavor = "multi_thread")]
async fn interactive_commits_and_leader_writes_take_turns_without_conflict() {
    let (c, leader, f) = start(10.0, &[]).await;
    let (l, fc) = (c.client(leader), c.client(f[0]));
    l.execute(
        statements(&[
            "CREATE TABLE c(id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO c VALUES (1, 0)",
        ]),
        QUORUM,
    )
    .await
    .unwrap();
    let mut index = c.converge(leader, &[f[0]]).await;
    for i in 0..20 {
        // The follower reads the leader's last write before it begins.
        fc.query("SELECT 1".into(), ReadLevel::AtIndex(index))
            .await
            .unwrap();
        let mut tx = fc.begin().await.unwrap();
        let n = tx
            .execute("SELECT n FROM c WHERE id = 1".into())
            .await
            .unwrap();
        assert_eq!(n.rows, vec![vec![Value::Integer(2 * i)]]);
        tx.execute(Statement::new(
            "UPDATE c SET n = ? WHERE id = 1",
            vec![Value::Integer(2 * i + 1)],
        ))
        .await
        .unwrap();
        let mode = if i % 2 == 0 {
            QUORUM
        } else {
            CommitMode::Async
        };
        tx.commit(mode)
            .await
            .unwrap_or_else(|e| panic!("interactive {i}: {e:?}"));
        let w = l
            .execute(
                statements(&["UPDATE c SET n = n + 1 WHERE id = 1", "SELECT n FROM c"]),
                mode,
            )
            .await
            .unwrap_or_else(|e| panic!("execute {i}: {e:?}"));
        assert_eq!(w.results[1].rows, vec![vec![Value::Integer(2 * i + 2)]]);
        index = w.index;
    }
    c.converge(leader, &[1, 2, 3]).await;
    let stats = l.stats().await.unwrap();
    println!("leader: {stats:?}");
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.incoming_rejected_early, 0);
    assert_eq!(stats.async_acked_then_failed, 0);
    assert_eq!(count(&l, "SELECT n FROM c", ReadLevel::Local).await, 40);
    assert_eq!(c.dump(1).await, c.dump(2).await);
    assert_eq!(c.dump(1).await, c.dump(3).await);
}

/// The leader is killed with SIGKILL while every node commits interactive
/// transactions. No transaction acknowledged in quorum mode is lost, and the
/// survivors end equal.
#[tokio::test(flavor = "multi_thread")]
async fn leader_killed_while_instances_commit_loses_no_quorum_transaction() {
    let (mut c, old_leader, _) = start(10.0, &[]).await;
    c.client(old_leader)
        .execute(
            statements(&["CREATE TABLE w(k TEXT PRIMARY KEY, seen INTEGER)"]),
            QUORUM,
        )
        .await
        .unwrap();
    c.converge(old_leader, &[1, 2, 3]).await;

    let acked = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let unknown = Arc::new(Mutex::new(Vec::<String>::new()));
    let after_kill = Arc::new(Mutex::new(0u64));
    let killed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for node in [1u64, 2, 3] {
        for w in 0..3 {
            let client = c.client(node).with_timeout(Duration::from_secs(15));
            let (acked, unknown, after_kill, killed, stop) = (
                acked.clone(),
                unknown.clone(),
                after_kill.clone(),
                killed.clone(),
                stop.clone(),
            );
            writers.push(tokio::spawn(async move {
                let mut seq = 0;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("n{node}-w{w}-{seq}");
                    seq += 1;
                    let sqls = vec![
                        Statement::new(
                            "INSERT INTO w VALUES (?, (SELECT count(*) FROM w))",
                            vec![Value::Text(key.clone())],
                        ),
                        "SELECT count(*) FROM w".into(),
                    ];
                    match interactive(&client, sqls, QUORUM).await {
                        Ok(_) => {
                            acked.lock().unwrap().insert(key);
                            if killed.load(Ordering::Relaxed) {
                                *after_kill.lock().unwrap() += 1;
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
    let stats = c.client(old_leader).stats().await.unwrap();
    println!(
        "old leader before the kill: {} incoming, {} groups, up to {} pending",
        stats.incoming, stats.groups, stats.max_groups_pending
    );
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
    let after = *after_kill.lock().unwrap();
    println!(
        "old leader {old_leader}, new leader {new_leader}; {before_kill} acknowledged before the kill, {} in all, {after} after; {} with unknown outcome",
        acked.len(),
        unknown.lock().unwrap().len()
    );
    assert!(before_kill > 0 && after > 0);
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
            "node {id} lacks acknowledged transactions {missing:?}"
        );
    }
    assert_eq!(c.dump(survivors[0]).await, c.dump(survivors[1]).await);
}

/// A local transaction holds its node's write lock, so the node applies
/// nothing while it is open. With a hold limit above the time it stays open,
/// the node falls behind by every entry written meanwhile and catches up
/// after the commit. With a hold limit below it, the first waiting apply
/// ends the transaction after the limit.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_local_transaction_holds_back_its_node() {
    for (hold_ms, aborts) in [("2000", false), ("300", true)] {
        let (c, leader, f) = start(10.0, &["--local-hold-ms", hold_ms]).await;
        let (l, fc) = (c.client(leader), c.client(f[0]));
        l.execute(statements(&["CREATE TABLE t(k TEXT PRIMARY KEY)"]), QUORUM)
            .await
            .unwrap();
        c.converge(leader, &[1, 2, 3]).await;

        let mut tx = fc.begin().await.unwrap();
        tx.execute("INSERT INTO t VALUES ('local')".into())
            .await
            .unwrap();
        let opened = Instant::now();
        let writer = {
            let l = c.client(leader);
            tokio::spawn(async move {
                let mut i = 0;
                while opened.elapsed() < Duration::from_secs(1) {
                    l.execute(
                        vec![Statement::new(
                            "INSERT INTO t VALUES (?)",
                            vec![Value::Text(format!("l{i}"))],
                        )],
                        QUORUM,
                    )
                    .await
                    .unwrap();
                    i += 1;
                }
                i
            })
        };
        let written = writer.await.unwrap();
        let leader_index = c.status(leader).await.applied.unwrap();
        let follower_index = fc.stats().await.unwrap().applied_index;
        let still_open = tx.execute("SELECT count(*) FROM t".into()).await;
        let committed = tx.commit(QUORUM).await;
        let held = opened.elapsed();
        let caught_up = Instant::now();
        c.converge(leader, &[f[0]]).await;
        println!(
            "hold limit {hold_ms} ms: open {held:?}; {written} writes meanwhile; leader applied {leader_index}, the instance {follower_index} ({} behind); statement after 1 s: {:?}; commit: {committed:?}; caught up {:?} after the commit",
            leader_index - follower_index,
            still_open.as_ref().map(|r| r.rows.clone()),
            caught_up.elapsed()
        );
        if aborts {
            assert!(
                matches!(still_open, Err(Error::Aborted(_))),
                "{still_open:?}"
            );
            assert!(matches!(committed, Err(Error::Aborted(_))), "{committed:?}");
            assert!(
                leader_index - follower_index < 10,
                "it applied after the limit"
            );
            assert!(fc.stats().await.unwrap().local_aborted >= 1);
        } else {
            // The instance read its own state: none of the leader's writes.
            assert_eq!(still_open.unwrap().rows, vec![vec![Value::Integer(1)]]);
            committed.unwrap();
            assert!(leader_index - follower_index >= written as u64 - 1);
            assert_eq!(
                count(&l, "SELECT count(*) FROM t", ReadLevel::Local).await,
                written + 1
            );
        }
        assert_eq!(c.dump(1).await, c.dump(2).await);
        assert_eq!(c.dump(1).await, c.dump(3).await);
    }
}

/// Several interactive transactions on one instance run one after another:
/// each holds the instance's write lock from begin to commit. Writing
/// different rows, all commit.
#[tokio::test(flavor = "multi_thread")]
async fn several_transactions_on_one_instance_take_turns() {
    let (c, leader, f) = start(10.0, &[]).await;
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)"]),
            QUORUM,
        )
        .await
        .unwrap();
    c.converge(leader, &[1, 2, 3]).await;
    let client = Arc::new(c.client(f[0]));
    let started = Instant::now();
    let tasks: Vec<_> = (0..8)
        .map(|w| {
            let client = client.clone();
            tokio::spawn(async move {
                for i in 0..5 {
                    let key = format!("w{w}-{i}");
                    let sqls = vec![
                        Statement::new(
                            "INSERT INTO t VALUES (?, 0)",
                            vec![Value::Text(key.clone())],
                        ),
                        "SELECT count(*) FROM t".into(),
                        Statement::new(
                            "UPDATE t SET v = v + 1 WHERE k = ?",
                            vec![Value::Text(key)],
                        ),
                    ];
                    interactive(&client, sqls, QUORUM).await.unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    println!(
        "8 x 5 transactions on one instance in {:?}",
        started.elapsed()
    );
    c.converge(leader, &[1, 2, 3]).await;
    assert_eq!(
        count(&c.client(leader), "SELECT sum(v) FROM t", ReadLevel::Local).await,
        40
    );
    assert_eq!(c.dump(1).await, c.dump(2).await);
    assert_eq!(c.dump(1).await, c.dump(3).await);
}

/// A rolled-back transaction, and one that changed nothing, send nothing.
#[tokio::test(flavor = "multi_thread")]
async fn rollback_and_read_only_transactions_send_nothing() {
    let (c, leader, f) = start(10.0, &[]).await;
    c.client(leader)
        .execute(
            statements(&["CREATE TABLE t(v TEXT)", "INSERT INTO t VALUES ('a')"]),
            QUORUM,
        )
        .await
        .unwrap();
    let index = c.converge(leader, &[1, 2, 3]).await;
    let fc = c.client(f[0]);
    let mut tx = fc.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES ('rolled back')".into())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut tx = fc.begin().await.unwrap();
    let r = tx.execute("SELECT v FROM t".into()).await.unwrap();
    assert_eq!(r.rows, vec![vec![text("a")]]);
    let failed = tx.execute("INSERT INTO nope VALUES (1)".into()).await;
    assert!(matches!(failed, Err(Error::Transaction(_))), "{failed:?}");
    let w = tx.commit(QUORUM).await.unwrap();
    assert_eq!(w.index, index);
    assert_eq!(c.client(leader).stats().await.unwrap().incoming, 0);
    assert_eq!(
        count(&fc, "SELECT count(*) FROM t", ReadLevel::Local).await,
        1
    );
}

/// An interactive commit and an `execute` of the same row reach the leader
/// at the same time, many times over, so that some land in one group. The
/// `execute` is captured on the ahead file after the incoming change set and
/// may read it, so the incoming change set must come first in the entry.
/// Every outcome must be one of a serial order: the local transaction then
/// the `execute` (it reads 11), the `execute` then a local transaction that
/// began after it (it reads 1, the row ends at 10), or a local transaction
/// that read the row before the `execute` and is rejected (the row ends at
/// 1). The `execute` is never rejected.
#[tokio::test(flavor = "multi_thread")]
async fn an_incoming_change_set_goes_before_the_captures_that_read_it() {
    let (c, leader, _) = start(10.0, &[]).await;
    let l = Arc::new(c.client(leader));
    l.execute(
        statements(&["CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)"]),
        QUORUM,
    )
    .await
    .unwrap();
    let keys = 40i64;
    let values: Vec<String> = (0..keys).map(|k| format!("({k}, 0)")).collect();
    l.execute(
        statements(&[&format!("INSERT INTO t VALUES {}", values.join(", "))]),
        QUORUM,
    )
    .await
    .unwrap();
    let tasks: Vec<_> = (0..keys)
        .map(|k| {
            let (a, b) = (l.clone(), l.clone());
            tokio::spawn(async move {
                let mut tx = a.begin().await.unwrap();
                tx.execute(Statement::new(
                    "UPDATE t SET v = 10 WHERE k = ?",
                    vec![Value::Integer(k)],
                ))
                .await
                .unwrap();
                // The commit and the execute race to the leader loop.
                let local = tx.commit(QUORUM);
                let leader_write = b.execute(
                    vec![
                        Statement::new(
                            "UPDATE t SET v = v + 1 WHERE k = ?",
                            vec![Value::Integer(k)],
                        ),
                        Statement::new("SELECT v FROM t WHERE k = ?", vec![Value::Integer(k)]),
                    ],
                    QUORUM,
                );
                let (local, leader_write) = tokio::join!(local, leader_write);
                (k, local, leader_write)
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for t in tasks {
        outcomes.push(t.await.unwrap());
    }
    c.converge(leader, &[1, 2, 3]).await;
    let (mut local_first, mut execute_first, mut local_rejected) = (0, 0, 0);
    for (k, local, leader_write) in outcomes {
        let w = leader_write.unwrap_or_else(|e| panic!("key {k}: the execute failed: {e:?}"));
        let Value::Integer(read) = w.results[1].rows[0][0] else {
            panic!()
        };
        let sql = format!("SELECT v FROM t WHERE k = {k}");
        let end = count(&l, &sql, ReadLevel::Local).await;
        match (&local, read, end) {
            (Ok(_), 11, 11) => local_first += 1,
            (Ok(_), 1, 10) => execute_first += 1,
            (Err(Error::Rejected(_)), 1, 1) => local_rejected += 1,
            _ => panic!("key {k}: local {local:?}, the execute read {read}, the row ends at {end}"),
        }
    }
    let stats = l.stats().await.unwrap();
    println!(
        "local then execute {local_first}, execute then local {execute_first}, local rejected {local_rejected}; {} groups for {} transactions",
        stats.groups, stats.transactions
    );
    assert!(local_first > 0, "no execute read an incoming change set");
    assert_eq!(stats.rejected, 0);
    assert_eq!(c.dump(1).await, c.dump(2).await);
    assert_eq!(c.dump(1).await, c.dump(3).await);
}
