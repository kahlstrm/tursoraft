//! Change subscriptions on nodes in this process: copies of a table kept
//! from events alone, the same events on every node, `Reset` after a
//! snapshot, `Lagged` for a subscriber that reads nothing, subscriptions
//! started during writes, and schema changes.

mod in_process;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use std::collections::BTreeMap;

use in_process::Cluster;
use tursoraft_node::{
    ChangeKind, Changes, CommitMode, Error, Event, Node, ReadLevel, Statement, Subscription, Value,
};
use tursoraft_raft::{LogConfig, NodeId, PairRtt, RaftTimeouts};

const QUORUM: CommitMode = CommitMode::Quorum;
/// Rows every writer updates, so that transactions conflict.
const HOT_ROWS: i64 = 6;

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

async fn execute(node: &Node, sql: &str, params: Vec<Value>) -> Result<u64, Error> {
    node.execute(vec![Statement::new(sql, params)], QUORUM)
        .await
        .map(|w| w.index)
}

/// Creates `t` with `HOT_ROWS` rows, and `marker`.
async fn create_tables(node: &Node) {
    execute(
        node,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, note TEXT)",
        vec![],
    )
    .await
    .unwrap();
    execute(node, "CREATE TABLE marker(id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap();
    for id in 1..=HOT_ROWS {
        execute(
            node,
            "INSERT INTO t(id, n, note) VALUES (?, 0, 'start')",
            vec![Value::Integer(id)],
        )
        .await
        .unwrap();
    }
}

/// Writes a row of `marker`. Every subscriber that receives its event has
/// received the events of everything committed before it.
async fn write_marker(node: &Node, id: i64) -> u64 {
    execute(
        node,
        "INSERT INTO marker(id) VALUES (?)",
        vec![Value::Integer(id)],
    )
    .await
    .unwrap()
}

/// A small deterministic random number generator.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

#[derive(Default)]
struct Tally {
    committed: AtomicU64,
    rejected: AtomicU64,
    aborted: AtomicU64,
    /// The note each transaction wrote, and the log index of its entry if it
    /// committed.
    notes: std::sync::Mutex<Vec<(String, Option<u64>)>>,
}

/// `n` transactions through [`Node::execute`] on `node`: updates of the hot
/// rows, inserts, and deletes of inserted rows.
async fn execute_writer(node: Node, seed: u64, n: usize, tally: Arc<Tally>) {
    let mut rng = Rng(seed);
    for i in 0..n {
        let note = format!("e{seed}-{i}");
        let (statements, note) = match rng.below(4) {
            0 | 1 => (
                vec![Statement::new(
                    "UPDATE t SET n = n + 1, note = ? WHERE id = ?",
                    vec![
                        text(&note),
                        Value::Integer(1 + rng.below(HOT_ROWS as u64) as i64),
                    ],
                )],
                Some(note),
            ),
            2 => (
                vec![Statement::new(
                    "INSERT INTO t(n, note) VALUES (0, ?)",
                    vec![text(&note)],
                )],
                Some(note),
            ),
            _ => (
                vec![Statement::new(
                    "DELETE FROM t WHERE id = (SELECT max(id) FROM t WHERE id > ?)",
                    vec![Value::Integer(HOT_ROWS)],
                )],
                None,
            ),
        };
        let result = node.execute(statements, QUORUM).await.map(|w| w.index);
        count(&tally, note, result);
    }
}

/// `n` interactive transactions on `node`: each reads a hot row, then
/// writes it and inserts a row, so that concurrent ones conflict.
async fn interactive_writer(node: Node, seed: u64, n: usize, tally: Arc<Tally>) {
    let mut rng = Rng(seed);
    for i in 0..n {
        let id = Value::Integer(1 + rng.below(HOT_ROWS as u64) as i64);
        let note = format!("i{seed}-{i}");
        let result = async {
            let mut tx = node.begin().await?;
            let rows = tx
                .execute(Statement::new(
                    "SELECT n FROM t WHERE id = ?",
                    vec![id.clone()],
                ))
                .await?
                .rows;
            let Value::Integer(n) = rows[0][0] else {
                unreachable!()
            };
            // Hold the read a little, so that others commit in between.
            tokio::time::sleep(Duration::from_millis(rng.below(3))).await;
            tx.execute(Statement::new(
                "UPDATE t SET n = ?, note = ? WHERE id = ?",
                vec![Value::Integer(n + 1), text(&note), id],
            ))
            .await?;
            tx.execute(Statement::new(
                "INSERT INTO t(n, note) VALUES (?, ?)",
                vec![Value::Integer(n), text(&note)],
            ))
            .await?;
            tx.commit(QUORUM).await.map(|w| w.index)
        }
        .await;
        count(&tally, Some(note), result);
    }
}

fn count(tally: &Tally, note: Option<String>, result: Result<u64, Error>) {
    if let Some(note) = note {
        let index = result.as_ref().ok().copied();
        tally.notes.lock().unwrap().push((note, index));
    }
    match result {
        Ok(_) => tally.committed.fetch_add(1, Relaxed),
        Err(Error::Rejected(_)) => tally.rejected.fetch_add(1, Relaxed),
        Err(Error::Aborted(_)) => tally.aborted.fetch_add(1, Relaxed),
        Err(e) => panic!("unexpected error: {e}"),
    };
}

/// Runs execute writers on `execute_on` and interactive writers on
/// `interactive_on`, all at once, until they are done.
async fn concurrent_writes(
    c: &Cluster,
    execute_on: &[NodeId],
    interactive_on: &[NodeId],
    n: usize,
) -> Arc<Tally> {
    let tally = Arc::new(Tally::default());
    let mut tasks = Vec::new();
    for (i, id) in execute_on.iter().enumerate() {
        let node = c.node(*id).clone();
        tasks.push(tokio::spawn(execute_writer(
            node,
            100 + i as u64,
            n,
            tally.clone(),
        )));
    }
    for (i, id) in interactive_on.iter().enumerate() {
        let node = c.node(*id).clone();
        tasks.push(tokio::spawn(interactive_writer(
            node,
            200 + i as u64,
            n,
            tally.clone(),
        )));
    }
    for task in tasks {
        task.await.unwrap();
    }
    println!(
        "committed {}, rejected {}, aborted {}",
        tally.committed.load(Relaxed),
        tally.rejected.load(Relaxed),
        tally.aborted.load(Relaxed)
    );
    tally
}

/// Voters 1, 2, 3 and the non-voting follower 4, with `t` and `marker`.
async fn four_nodes(max_pending_groups: usize) -> (Cluster, NodeId) {
    let mut c = Cluster::new(&[1, 2, 3, 4], |_| {});
    c.options.max_pending_groups = max_pending_groups;
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    c.add_follower(leader, 4).await;
    create_tables(c.node(leader)).await;
    c.converge(leader, &[1, 2, 3, 4]).await;
    (c, leader)
}

/// Pipelining off and on. Off, the leader proposes the change sets of
/// interactive transactions unchecked, and apply rejects those that lost a
/// write conflict. On, the leader rejects most of them before proposing.
const PIPELINING: [usize; 2] = [1, 256];

/// Write-conflict rejections at apply on `node`, which leads.
fn rejected_at_apply(node: &Node) -> u64 {
    node.stats().rejected
}

#[tokio::test(flavor = "multi_thread")]
async fn copies_from_events_equal_the_table_on_leader_voter_and_follower() {
    for max_pending_groups in PIPELINING {
        println!("--- max_pending_groups {max_pending_groups}");
        copies_equal_the_table(max_pending_groups).await;
    }
}

async fn copies_equal_the_table(max_pending_groups: usize) {
    let (c, leader) = four_nodes(max_pending_groups).await;
    let voter = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    let mut mirrors = Vec::new();
    for id in [leader, voter, 4] {
        mirrors.push(Mirror::start(c.node(id)).await);
    }
    // Follow the events while the writes run.
    let followers: Vec<_> = mirrors
        .into_iter()
        .map(|mut m| {
            tokio::spawn(async move {
                m.follow_until_marker().await;
                m
            })
        })
        .collect();
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
    let tally = concurrent_writes(&c, &[leader, others[0], 4], &[others[1], 4, leader], 40).await;
    assert!(
        tally.rejected.load(Relaxed) > 0,
        "no transaction lost a write conflict"
    );
    if max_pending_groups == 1 {
        assert!(
            rejected_at_apply(c.node(leader)) > 0,
            "apply rejected nothing"
        );
    }
    write_marker(c.node(leader), 1).await;
    for f in followers {
        let m = f.await.unwrap();
        let (_, table) = read_table(&m.node).await;
        println!(
            "node {}: {} events, {} reads, {} rows",
            m.node.id(),
            m.log.len(),
            m.reads,
            m.rows.len()
        );
        assert!(m.changes().len() > 100, "too few events to mean anything");
        assert_whole_transactions(&m);
        assert_eq!(
            m.rows,
            table,
            "node {}: copy differs from the table",
            m.node.id()
        );
    }
}

/// Each interactive transaction updates a row and inserts one, both with its
/// own note: an event holds both or neither.
fn assert_whole_transactions(m: &Mirror) {
    let mut interactive = 0;
    for changes in m.changes() {
        let mut notes = std::collections::BTreeMap::new();
        for row in changes.rows.iter().filter(|r| r.table == "t") {
            if let Some(Value::Text(note)) = row.after.as_ref().map(|a| &a[2]) {
                if note.starts_with('i') {
                    *notes.entry(note.clone()).or_insert(0) += 1;
                }
            }
        }
        for (note, n) in notes {
            assert_eq!(n, 2, "entry {}: part of transaction {note}", changes.index);
            interactive += 1;
        }
    }
    assert!(interactive > 10, "too few interactive transactions");
}

#[tokio::test(flavor = "multi_thread")]
async fn every_node_sends_the_same_events_in_the_same_order() {
    for max_pending_groups in PIPELINING {
        println!("--- max_pending_groups {max_pending_groups}");
        same_events_on_every_node(max_pending_groups).await;
    }
}

async fn same_events_on_every_node(max_pending_groups: usize) {
    let (c, leader) = four_nodes(max_pending_groups).await;
    let mut mirrors = Vec::new();
    for id in [1, 2, 3, 4] {
        mirrors.push(Mirror::start(c.node(id)).await);
    }
    let followers: Vec<_> = mirrors
        .into_iter()
        .map(|mut m| {
            tokio::spawn(async move {
                m.follow_until_marker().await;
                m
            })
        })
        .collect();
    let tally = concurrent_writes(&c, &[1, 2, 4], &[3, 4, 2], 30).await;
    assert!(tally.rejected.load(Relaxed) > 0);
    if max_pending_groups == 1 {
        assert!(
            rejected_at_apply(c.node(leader)) > 0,
            "apply rejected nothing"
        );
    }
    write_marker(c.node(leader), 1).await;
    let mut mirrors = Vec::new();
    for f in followers {
        mirrors.push(f.await.unwrap());
    }
    let from = mirrors.iter().map(|m| m.started_after).max().unwrap();
    let events = |m: &Mirror| -> Vec<Changes> {
        assert!(
            m.log.iter().all(|e| matches!(e, Event::Changes(_))),
            "node {}: only changes expected",
            m.node.id()
        );
        m.changes()
            .into_iter()
            .filter(|c| c.index > from)
            .cloned()
            .collect()
    };
    let reference = events(&mirrors[0]);
    assert!(reference.len() > 100);
    // Log order: indexes rise, positions rise within an entry.
    for pair in reference.windows(2) {
        assert!((pair[0].index, pair[0].position) < (pair[1].index, pair[1].position));
    }
    for m in &mirrors[1..] {
        assert!(
            events(m) == reference,
            "node {} sent other events than node {}",
            m.node.id(),
            mirrors[0].node.id()
        );
    }
    // The events are the committed transactions, each at the index its
    // write returned, and no rejected one.
    let mut written = std::collections::BTreeMap::new();
    for changes in &reference {
        for row in changes.rows.iter().filter(|r| r.table == "t") {
            if let Some(Value::Text(note)) = row.after.as_ref().map(|a| &a[2]) {
                written.insert(note.clone(), changes.index);
            }
        }
    }
    let notes = tally.notes.lock().unwrap();
    for (note, index) in notes.iter() {
        assert_eq!(written.get(note), index.as_ref(), "transaction {note}");
    }
    assert_eq!(
        written.len(),
        notes.iter().filter(|(_, i)| i.is_some()).count()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_back_by_snapshot_sends_reset_and_its_copy_is_equal_after_reading_again() {
    // Node 4 is a non-voting follower far away: what the leader sends it
    // arrives half a second later, after the subscriber has subscribed.
    let mut c = Cluster::new(&[1, 2, 3, 4], |cfg| {
        cfg.log = LogConfig {
            snapshot_every: 20,
            keep_after_snapshot: 5,
            ..LogConfig::default()
        };
        cfg.delays.pairs = [1, 2, 3]
            .into_iter()
            .map(|a| PairRtt {
                a,
                b: 4,
                rtt_ms: 1000.0,
            })
            .collect();
        cfg.timeouts = Some(RaftTimeouts::default());
    });
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    c.add_follower(leader, 4).await;
    create_tables(c.node(leader)).await;
    c.converge(leader, &[4]).await;
    c.stop(4).await;

    let tally = concurrent_writes(&c, &[leader], &[], 60).await;
    assert_eq!(tally.rejected.load(Relaxed), 0);
    // Wait for the leader to purge its log past node 4's position.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while c.node(leader).raft().status().purged.unwrap_or(0) < 20 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader kept its log"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let node = c.start(4).await;
    let mut m = Mirror::start(&node).await;
    println!("node 4 restarted: subscribed after {}", m.started_after);
    write_marker(c.node(leader), 1).await;
    m.follow_until_marker().await;

    let stats = node.stats();
    assert!(
        stats.snapshots_installed >= 1,
        "node 4 caught up without a snapshot"
    );
    let resets: Vec<(usize, u64)> = m
        .log
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match e {
            Event::Reset { index } => Some((i, *index)),
            _ => None,
        })
        .collect();
    println!(
        "node 4 events: {:?}",
        m.log.iter().map(short).collect::<Vec<_>>()
    );
    let [(at, index)] = resets[..] else {
        panic!("expected one Reset, got {resets:?}")
    };
    // No row changes of the entries the snapshot covered come after it.
    for e in &m.log[at + 1..] {
        if let Event::Changes(changes) = e {
            assert!(
                changes.index > index,
                "{changes:?} after Reset {{ index: {index} }}"
            );
        }
    }
    assert_eq!(m.reads, 2, "read at start and after the Reset");
    let (_, table) = read_table(&node).await;
    assert_eq!(m.rows, table);
    assert_eq!(table, read_table(c.node(leader)).await.1);
}

fn short(e: &Event) -> String {
    match e {
        Event::Changes(c) => format!("{}.{}", c.index, c.position),
        other => format!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_that_reads_nothing_lags_while_apply_and_others_go_on() {
    let mut c = Cluster::new(&[1, 2, 3], |_| {});
    c.options.subscriber_queue = 8;
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    create_tables(c.node(leader)).await;
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    c.converge(leader, &[follower]).await;

    let node = c.node(follower).clone();
    let (_, mut idle) = node.subscribe();
    let mut active = Mirror::start(&node).await;
    let active = tokio::spawn(async move {
        active.follow_until_marker().await;
        active
    });
    // Apply does not wait for the idle subscriber: every write is applied
    // on its node.
    let writes = async {
        for i in 0..50 {
            let index = execute(
                c.node(leader),
                "INSERT INTO t(n, note) VALUES (?, 'x')",
                vec![Value::Integer(i)],
            )
            .await
            .unwrap();
            node.raft()
                .wait_applied(index, Duration::from_secs(10))
                .await
                .unwrap();
        }
        write_marker(c.node(leader), 1).await;
    };
    tokio::time::timeout(Duration::from_secs(60), writes)
        .await
        .expect("writes stalled behind a subscriber that reads nothing");

    let active = active.await.unwrap();
    assert_eq!(active.changes().len(), 51);
    assert!(active.log.iter().all(|e| matches!(e, Event::Changes(_))));
    assert_eq!(active.rows, read_table(&node).await.1);

    // 51 events were sent and 8 kept: 43 missed, then the last 8.
    assert_eq!(idle.recv().await, Some(Event::Lagged { missed: 43 }));
    let mut kept = Vec::new();
    while let Some(event) = idle.try_recv() {
        let Event::Changes(changes) = event else {
            panic!("{event:?} after Lagged")
        };
        kept.push(changes);
    }
    let last_8: Vec<Changes> = active.changes()[43..]
        .iter()
        .map(|c| (*c).clone())
        .collect();
    assert_eq!(kept, last_8);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_started_during_writes_misses_nothing_after_its_index() {
    let mut c = Cluster::new(&[1, 2, 3], |_| {});
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    create_tables(c.node(leader)).await;
    let follower = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    c.converge(leader, &[follower]).await;
    let node = c.node(follower).clone();

    // Every event from before the writes, to compare with.
    let mut reference = Mirror::start(&node).await;
    let reference = tokio::spawn(async move {
        reference.follow_until_marker().await;
        reference
    });

    let writes = {
        let c_nodes: Vec<Node> = [1, 2, 3].iter().map(|id| c.node(*id).clone()).collect();
        tokio::spawn(async move {
            let tally = Arc::new(Tally::default());
            let tasks: Vec<_> = c_nodes
                .iter()
                .enumerate()
                .flat_map(|(i, n)| {
                    [
                        tokio::spawn(execute_writer(n.clone(), i as u64, 150, tally.clone())),
                        tokio::spawn(execute_writer(n.clone(), 10 + i as u64, 150, tally.clone())),
                    ]
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
        })
    };

    // Subscribe again and again while the writes run. Each subscription
    // keeps its start index and its first event.
    let mut firsts = Vec::new();
    let mut mid_mirror = None;
    while !writes.is_finished() {
        let (after, mut events) = node.subscribe();
        firsts.push(tokio::spawn(async move { (after, events.recv().await) }));
        if firsts.len() == 100 {
            mid_mirror = Some(Mirror::start(&node).await);
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    writes.await.unwrap();
    write_marker(c.node(leader), 1).await;

    let reference = reference.await.unwrap();
    let all = reference.changes();
    println!("{} subscriptions during {} events", firsts.len(), all.len());
    assert!(firsts.len() > 200);
    for first in firsts {
        let (after, event) = first.await.unwrap();
        let expected = all
            .iter()
            .find(|c| c.index > after)
            .map(|c| Event::Changes((*c).clone()));
        assert_eq!(event, expected, "subscription after {after}");
    }
    let mut mid = mid_mirror.expect("a subscription mid-way");
    mid.follow_until_marker().await;
    assert_eq!(mid.reads, 1);
    assert_eq!(mid.rows, read_table(&node).await.1);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_changes_arrive_as_sqlite_schema_rows_and_later_rows_carry_new_columns() {
    let mut c = Cluster::new(&[1], |_| {});
    c.start_all().await;
    let leader = c.initialize(&[1]).await;
    let node = c.node(leader).clone();
    let (_, mut events) = node.subscribe();
    let mut next = || {
        let e = events.try_recv();
        match e {
            Some(Event::Changes(c)) => c.rows.to_vec(),
            other => panic!("expected changes, got {other:?}"),
        }
    };
    let create = "CREATE TABLE s(id INTEGER PRIMARY KEY, x TEXT)";
    execute(&node, create, vec![]).await.unwrap();
    execute(&node, "INSERT INTO s VALUES (1, 'one')", vec![])
        .await
        .unwrap();
    execute(
        &node,
        "ALTER TABLE s ADD COLUMN y INTEGER DEFAULT 7",
        vec![],
    )
    .await
    .unwrap();
    execute(&node, "INSERT INTO s(id, x) VALUES (2, 'two')", vec![])
        .await
        .unwrap();
    execute(&node, "UPDATE s SET x = 'uno' WHERE id = 1", vec![])
        .await
        .unwrap();
    // Events are sent right after apply; the write may answer first.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // CREATE TABLE: an insert of its sqlite_schema row.
    let rows = next();
    println!("CREATE TABLE: {rows:?}");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].table.as_str(), rows[0].kind),
        ("sqlite_schema", ChangeKind::Insert)
    );
    let schema_rowid = rows[0].rowid;
    let after = rows[0].after.as_ref().unwrap();
    assert_eq!(after[..3], [text("table"), text("s"), text("s")]);
    assert_eq!(
        after[4],
        text("CREATE TABLE s (id INTEGER PRIMARY KEY, x TEXT)")
    );

    let rows = next();
    assert_eq!(rows[0].after, Some(vec![Value::Integer(1), text("one")]));

    // ALTER TABLE: an update of the sql of that row.
    let rows = next();
    println!("ALTER TABLE: {rows:?}");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].table.as_str(), rows[0].kind),
        ("sqlite_schema", ChangeKind::Update)
    );
    assert_eq!(rows[0].rowid, schema_rowid);
    assert_eq!(rows[0].before.as_ref().unwrap()[4], after[4]);
    assert_eq!(
        rows[0].after.as_ref().unwrap()[4],
        text("CREATE TABLE s (id INTEGER PRIMARY KEY, x TEXT, y INTEGER DEFAULT 7)")
    );

    // Rows written after the ALTER carry the new column, with its default.
    let rows = next();
    assert_eq!(
        rows[0].after,
        Some(vec![Value::Integer(2), text("two"), Value::Integer(7)])
    );
    let rows = next();
    println!("UPDATE of a row written before the ALTER: {rows:?}");
    assert_eq!(rows[0].kind, ChangeKind::Update);
    assert_eq!(
        rows[0].after,
        Some(vec![Value::Integer(1), text("uno"), Value::Integer(7)])
    );
    assert_eq!(
        rows[0].before,
        Some(vec![Value::Integer(1), text("one"), Value::Integer(7)])
    );
    assert_eq!(events.try_recv(), None);
    assert_eq!(
        node.query(
            Statement::from("SELECT * FROM s ORDER BY id"),
            ReadLevel::Local
        )
        .await
        .unwrap()
        .rows,
        vec![
            vec![Value::Integer(1), text("uno"), Value::Integer(7)],
            vec![Value::Integer(2), text("two"), Value::Integer(7)],
        ]
    );
}

/// A copy of table `t(id INTEGER PRIMARY KEY, ...)` of one node, kept from
/// that node's events. Applies each row change strictly: an insert must find
/// no row, an update or delete must find the row equal to its before-image.
/// So a missed, repeated or misordered event shows as a panic or a
/// difference from the table.
struct Mirror {
    node: Node,
    events: Subscription,
    /// The index the subscription started after.
    started_after: u64,
    /// The applied index of the last read of the table.
    read_at: u64,
    rows: BTreeMap<i64, Vec<Value>>,
    /// Every event received, in order.
    log: Vec<Event>,
    reads: u64,
}

const READ_T: &str = "SELECT a.log_index, t.* FROM tursoraft_applied a LEFT JOIN t";

impl Mirror {
    /// Subscribes, then reads the table.
    async fn start(node: &Node) -> Mirror {
        let (started_after, events) = node.subscribe();
        let mut mirror = Mirror {
            node: node.clone(),
            events,
            started_after,
            read_at: 0,
            rows: BTreeMap::new(),
            log: Vec::new(),
            reads: 0,
        };
        mirror.read().await;
        assert!(mirror.read_at >= started_after);
        mirror
    }

    /// Replaces the copy with the table and the index it was read at, in
    /// one read transaction.
    async fn read(&mut self) {
        let (read_at, rows) = read_table(&self.node).await;
        self.read_at = read_at;
        self.rows = rows;
        self.reads += 1;
    }

    /// Receives and handles the next event. `None` when the subscription
    /// ended or no event came within `wait`.
    async fn next(&mut self, wait: Duration) -> Option<Event> {
        let event = tokio::time::timeout(wait, self.events.recv())
            .await
            .ok()??;
        self.log.push(event.clone());
        match &event {
            Event::Changes(changes) => {
                // A read started after the event sees its entry.
                let applied = read_applied(&self.node).await;
                assert!(
                    applied >= changes.index,
                    "node {}: event of entry {} while a read sees {applied}",
                    self.node.id(),
                    changes.index
                );
                self.apply(changes)
            }
            Event::Reset { .. } | Event::Lagged { .. } => self.read().await,
        }
        Some(event)
    }

    /// Handles events until one holds a change of table `marker`.
    async fn follow_until_marker(&mut self) {
        loop {
            match self.next(Duration::from_secs(30)).await {
                Some(Event::Changes(c)) if c.rows.iter().any(|r| r.table == "marker") => return,
                Some(_) => {}
                None => panic!("node {} sent no marker", self.node.id()),
            }
        }
    }

    fn apply(&mut self, changes: &Changes) {
        if changes.index <= self.read_at {
            return;
        }
        for row in changes.rows.iter().filter(|r| r.table == "t") {
            let current = self.rows.get(&row.rowid);
            let here = format!(
                "node {} entry {} position {}: {row:?}, copy has {current:?}",
                self.node.id(),
                changes.index,
                changes.position
            );
            match row.kind {
                ChangeKind::Insert => {
                    assert!(current.is_none(), "insert of an existing row: {here}");
                    self.rows.insert(row.rowid, row.after.clone().unwrap());
                }
                ChangeKind::Update => {
                    assert_eq!(current, row.before.as_ref(), "{here}");
                    let after = row.after.clone().unwrap();
                    // An update that changes the INTEGER PRIMARY KEY moves the row.
                    let Value::Integer(new_id) = after[0] else {
                        panic!("id is an integer: {here}")
                    };
                    self.rows.remove(&row.rowid);
                    self.rows.insert(new_id, after);
                }
                ChangeKind::Delete => {
                    assert_eq!(current, row.before.as_ref(), "{here}");
                    self.rows.remove(&row.rowid);
                }
            }
        }
    }

    /// The `Changes` events received, in order.
    fn changes(&self) -> Vec<&Changes> {
        self.log
            .iter()
            .filter_map(|e| match e {
                Event::Changes(c) => Some(c),
                _ => None,
            })
            .collect()
    }
}

/// The applied index a read on `node` sees.
async fn read_applied(node: &Node) -> u64 {
    let rows = node
        .query(
            Statement::from("SELECT log_index FROM tursoraft_applied"),
            ReadLevel::Local,
        )
        .await
        .unwrap()
        .rows;
    let Value::Integer(index) = rows[0][0] else {
        panic!("no applied index: {rows:?}")
    };
    index as u64
}

/// Table `t` of `node` and the applied index it was read at.
async fn read_table(node: &Node) -> (u64, BTreeMap<i64, Vec<Value>>) {
    let rows = node
        .query(Statement::from(READ_T), ReadLevel::Local)
        .await
        .unwrap()
        .rows;
    let Value::Integer(read_at) = rows[0][0] else {
        panic!("no applied index: {rows:?}")
    };
    let rows = rows
        .into_iter()
        .filter(|r| r[1] != Value::Null)
        .map(|mut r| {
            r.remove(0);
            let Value::Integer(id) = r[0] else {
                unreachable!()
            };
            (id, r)
        })
        .collect();
    (read_at as u64, rows)
}
