//! End to end: a tursoraft cluster in this process, a relay on one node, and
//! the Worker and follower Durable Object under Miniflare. Each test writes
//! through tursoraft and checks that the follower's tables equal the relay
//! node's tables at the same log index.
//!
//! Needs Node.js and `npm ci` in `edge/`.

mod common;

use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use common::*;
use tursoraft_node::{Statement, Value};
use tursoraft_raft::{LogConfig, PairRtt, RaftTimeouts};

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_ordinary_writes_and_schema_changes() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    store_writes(c.node(leader), 1, 60).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;

    // CREATE TABLE and CREATE INDEX are replayed from events; a trigger is
    // left out, and its writes arrive as rows.
    write(
        c.node(leader),
        vec![
            Statement::from("CREATE TABLE tags(sku TEXT PRIMARY KEY, label TEXT)"),
            Statement::from("CREATE INDEX tags_by_label ON tags(label)"),
            Statement::from(
                "CREATE TRIGGER tag_orders AFTER INSERT ON tags BEGIN \
                 INSERT INTO order_notes VALUES (NULL, 'tagged ' || new.sku); END",
            ),
            st(
                "INSERT INTO tags VALUES (?, ?), (?, ?)",
                vec![txt("a"), txt("red"), txt("b"), txt("blue")],
            ),
        ],
    )
    .await
    .unwrap();
    write(
        c.node(leader),
        vec![st("UPDATE tags SET sku = 'c' WHERE sku = 'a'", vec![])],
    )
    .await
    .unwrap();
    store_writes(c.node(leader), 2, 20).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let stats = relay.stats();
    assert_eq!((stats.copies, stats.schema_changes), (1, 0), "{stats:?}");

    // ALTER TABLE is not replayed: the relay copies again.
    write(
        c.node(leader),
        vec![Statement::from(
            "ALTER TABLE products ADD COLUMN color TEXT DEFAULT 'none'",
        )],
    )
    .await
    .unwrap();
    store_writes(c.node(leader), 3, 20).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let stats = relay.stats();
    assert_eq!((stats.copies, stats.schema_changes), (2, 1), "{stats:?}");
    assert_eq!(stats.failures, 0, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn values_are_exact_through_events_and_through_a_full_copy() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    write(
        c.node(leader),
        vec![Statement::from(
            "CREATE TABLE vals(k INTEGER PRIMARY KEY, v, r REAL, big INTEGER)",
        )],
    )
    .await
    .unwrap();
    let values = [
        Value::Integer(i64::MAX),
        Value::Integer(i64::MIN),
        Value::Integer((1 << 53) + 1),
        Value::Integer(-(1 << 53) - 3),
        Value::Integer(-1),
        Value::Integer(0),
        Value::Real(0.1),
        Value::Real(-2.5e-300),
        Value::Real(1e300),
        Value::Real(3.0),
        Value::Real(f64::INFINITY),
        Value::Real(f64::NEG_INFINITY),
        Value::Text(String::new()),
        Value::Text("O'Brien \"quoted\" ä 😀 {\"i\": \"1\"}".into()),
        Value::Text("123".into()),
        Value::Blob(vec![0, 1, 2, 255, 0]),
        Value::Blob(vec![]),
        Value::Null,
    ];
    for (k, v) in values.iter().enumerate() {
        write(
            c.node(leader),
            vec![st(
                "INSERT INTO vals VALUES (?, ?, ?, ?)",
                vec![int(k as i64), v.clone(), v.clone(), v.clone()],
            )],
        )
        .await
        .unwrap();
    }
    // Updates and deletes found by keys beyond 2^53.
    write(
        c.node(leader),
        vec![
            st(
                "INSERT INTO vals VALUES (?, 'big key', NULL, NULL), (?, 'big key 2', NULL, NULL)",
                vec![int(i64::MAX), int((1 << 53) + 1)],
            ),
            st(
                "UPDATE vals SET v = 'updated' WHERE k = ?",
                vec![int(i64::MAX)],
            ),
        ],
    )
    .await
    .unwrap();
    write(
        c.node(leader),
        vec![st("DELETE FROM vals WHERE k = ?", vec![int((1 << 53) + 1)])],
    )
    .await
    .unwrap();
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let dump = edge
        .dump(FOLLOWER, applied(c.node(other)).await, 1000)
        .await
        .unwrap();
    let (_, rows) = &dump.tables["vals"];
    for (k, v) in values.iter().enumerate() {
        let row = rows.iter().find(|r| r[0] == int(k as i64)).unwrap();
        assert_eq!(&row[1], v, "value {k}, untyped column");
    }
    assert!(rows
        .iter()
        .any(|r| r[..2] == [int(i64::MAX), txt("updated")]));
    assert_eq!(relay.stats().copies, 1, "the values came as events");

    // The same values again through a full copy.
    edge.close_relays(FOLLOWER).await;
    wait_copies(&relay, 2).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pages_show_committed_writes_and_reads_wait_for_their_index() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    wait_copies(&relay, 1).await;
    let index = write(
        c.node(leader),
        vec![st(
            "UPDATE products SET title = ? WHERE product_id = 3",
            vec![txt("Café <au lait>")],
        )],
    )
    .await
    .unwrap();
    let (status, body, through) = edge.page(&format!("/?at={index}")).await;
    assert_eq!(status, 200, "{body}");
    assert!(through.unwrap() >= index);
    assert!(body.contains("Café &lt;au lait&gt;"), "{body}");
    assert!(body.contains("Product 8"), "{body}");

    // A read at an index nothing has reached yet waits for it.
    let next = applied(c.node(other)).await + 1;
    let read = {
        let path = format!("/products/42?at={next}&wait_ms=10000");
        let url = edge.url.clone();
        tokio::spawn(async move {
            let response = reqwest::get(format!("{url}{path}")).await.unwrap();
            (response.status().as_u16(), response.text().await.unwrap())
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!read.is_finished(), "the read did not wait");
    let index = write(
        c.node(leader),
        vec![st(
            "INSERT INTO products VALUES (42, 'The answer', 4200, 7)",
            vec![],
        )],
    )
    .await
    .unwrap();
    assert_eq!(index, next, "another entry came between");
    let (status, body) = read.await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<h1>The answer</h1>"), "{body}");
    assert!(body.contains("7 in stock"), "{body}");

    // One that never gets there times out.
    let (status, _, through) = edge
        .page(&format!("/?at={}&wait_ms=300", next + 1000))
        .await;
    assert_eq!(status, 504);
    assert!(through.unwrap() >= next);
    assert_eq!(edge.page("/products/43").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_concurrent_writers_with_rejected_change_sets() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    wait_copies(&relay, 1).await;
    let third = [1, 2, 3]
        .into_iter()
        .find(|n| *n != leader && *n != other)
        .unwrap();
    let tally = concurrent_writes(&c, &[leader, other, third], 40).await;
    assert!(tally.rejected.load(Relaxed) > 0, "nothing was rejected");
    c.converge(leader, &[other]).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    println!("{:?}", relay.stats());
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_the_relay_node_caught_up_by_snapshot() {
    // Node 4 is a non-voting follower far away: what the leader sends it
    // arrives half a second later, after the relay has copied.
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
    create_store(c.node(leader)).await;
    c.converge(leader, &[4]).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(4), FOLLOWER);
    assert_same(&edge, FOLLOWER, c.node(4)).await;
    drop(relay);
    c.stop(4).await;

    store_writes(c.node(leader), 1, 60).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while c.node(leader).raft().status().purged.unwrap_or(0) < 20 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader kept its log"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let node = c.start(4).await;
    let relay = edge.relay(&node, FOLLOWER);
    let index = write(
        c.node(leader),
        vec![Statement::from("DELETE FROM order_notes")],
    )
    .await
    .unwrap();
    node.raft()
        .wait_applied(index, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(node.stats().snapshots_installed >= 1, "no snapshot");
    assert_same(&edge, FOLLOWER, &node).await;
    let stats = relay.stats();
    assert!(stats.resets >= 1, "no Reset: {stats:?}");
    assert_eq!(stats.copies, 1 + stats.resets, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_the_relay_subscription_lagged() {
    // Two events of queue: an entry that holds more change sets than that
    // overruns a subscriber that is busy for a moment.
    let (c, leader, other) = three_nodes(|c| c.options.subscriber_queue = 2).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    wait_copies(&relay, 1).await;
    let third = [1, 2, 3]
        .into_iter()
        .find(|n| *n != leader && *n != other)
        .unwrap();
    concurrent_writes(&c, &[leader, other, third], 30).await;
    c.converge(leader, &[other]).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let stats = relay.stats();
    assert!(stats.lags >= 1, "the subscription never lagged: {stats:?}");
    assert_eq!(stats.copies, 1 + stats.lags, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_a_relay_restart() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    store_writes(c.node(leader), 1, 30).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    drop(relay);
    // Writes while no relay runs, then a new relay on another node.
    store_writes(c.node(leader), 2, 30).await;
    let third = [1, 2, 3]
        .into_iter()
        .find(|n| *n != leader && *n != other)
        .unwrap();
    let relay = edge.relay(c.node(third), FOLLOWER);
    store_writes(c.node(leader), 3, 30).await;
    c.converge(leader, &[third]).await;
    assert_same(&edge, FOLLOWER, c.node(third)).await;
    assert_eq!(relay.stats().copies, 1);
    assert_eq!(edge.status(FOLLOWER).await.relays, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_a_dropped_websocket() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    wait_copies(&relay, 1).await;
    let writes = {
        let node = c.node(leader).clone();
        tokio::spawn(async move { store_writes(&node, 1, 120).await })
    };
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        edge.close_relays(FOLLOWER).await;
    }
    writes.await.unwrap();
    c.converge(leader, &[other]).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let stats = relay.stats();
    assert!(stats.connects >= 2, "{stats:?}");
    assert!(stats.copies >= 2, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_equals_the_node_after_it_lost_its_memory() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let mut edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    store_writes(c.node(leader), 1, 30).await;
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let before = edge.status(FOLLOWER).await;
    for round in 0..3 {
        edge.evict(FOLLOWER).await;
        store_writes(c.node(leader), 10 + round, 20).await;
    }
    assert_same(&edge, FOLLOWER, c.node(other)).await;
    let after = edge.status(FOLLOWER).await;
    assert_ne!(
        after.instance, before.instance,
        "the object stayed in memory"
    );
    assert!(after.constructed > before.constructed);
    // Its WebSocket survived in hibernation: no new connection, no copy.
    let stats = relay.stats();
    assert_eq!((stats.connects, stats.copies), (1, 1), "{stats:?}");
}

/// Answers an open question: Miniflare does remove an idle follower from
/// memory while its relay's WebSocket stays open, and the next message
/// constructs it again.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_follower_leaves_memory_and_the_next_message_brings_it_back() {
    let (c, leader, other) = three_nodes(|_| {}).await;
    let edge = Edge::start().await;
    let relay = edge.relay(c.node(other), FOLLOWER);
    let index = store_writes(c.node(leader), 1, 10).await;
    relay
        .wait_follower(index, Duration::from_secs(30))
        .await
        .unwrap();
    // Status calls construct the object too, so look only before and after.
    let before = edge.status(FOLLOWER).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let index = store_writes(c.node(leader), 2, 1).await;
    relay
        .wait_follower(index, Duration::from_secs(30))
        .await
        .unwrap();
    let after = edge.status(FOLLOWER).await;
    println!("before {before:?}\nafter {after:?}");
    assert_ne!(after.instance, before.instance, "it stayed in memory");
    assert_eq!(after.constructed, before.constructed + 1);
    assert_eq!(relay.stats().connects, 1);
    assert_same(&edge, FOLLOWER, c.node(other)).await;
}
