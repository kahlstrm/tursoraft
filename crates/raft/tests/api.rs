//! `Node` API tests with several nodes in this process, over real TCP.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tursoraft_raft::{
    Batching, ClusterConfig, CountingOptions, CountingStateMachine, DelayTable, Error, LogConfig,
    Node, NodeId, Outcome,
};

struct InProcess {
    dir: TempDir,
    config: ClusterConfig,
    nodes: BTreeMap<NodeId, Node>,
    sms: BTreeMap<NodeId, CountingStateMachine>,
}

async fn start(ids: &[NodeId], change: impl FnOnce(&mut ClusterConfig)) -> (InProcess, NodeId) {
    let dir = TempDir::new().unwrap();
    let mut config = ClusterConfig {
        nodes: ids
            .iter()
            .map(|id| {
                let port = common::free_port();
                (*id, format!("127.0.0.1:{port}"))
            })
            .collect(),
        delays: DelayTable::default(),
        timeouts: None,
        batching: None,
        log: LogConfig::default(),
        propose_timeout_ms: 10_000,
    };
    change(&mut config);
    let mut nodes = BTreeMap::new();
    let mut sms = BTreeMap::new();
    for id in ids {
        let (node, sm) = start_node(&dir, &config, *id).await;
        nodes.insert(*id, node);
        sms.insert(*id, sm);
    }
    nodes[&ids[0]]
        .initialize(config.nodes.clone())
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        let leaders: Vec<_> = nodes.values().map(|n| n.leader()).collect();
        if let Some(l) = leaders[0] {
            if leaders.iter().all(|x| *x == Some(l)) {
                break l;
            }
        }
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    (
        InProcess {
            dir,
            config,
            nodes,
            sms,
        },
        leader,
    )
}

/// Starts node `id` in its directory under `dir`, with a new state machine.
async fn start_node(
    dir: &TempDir,
    config: &ClusterConfig,
    id: NodeId,
) -> (Node, CountingStateMachine) {
    let sm = CountingStateMachine::new(CountingOptions {
        record: true,
        reject_prefix: Some(b"reject".to_vec()),
        ..Default::default()
    })
    .unwrap();
    let node_dir = dir.path().join(format!("node-{id}"));
    let node = Node::start(config.node_config(id, node_dir).unwrap(), sm.clone())
        .await
        .unwrap();
    (node, sm)
}

/// Groups proposed concurrently, half of them through followers, each land in
/// one entry, in order, with nothing between their members, and each caller
/// gets its own outcomes. Small batches make the batcher close entries in the
/// middle of the queue, where a group could be split.
#[tokio::test(flavor = "multi_thread")]
async fn propose_many_keeps_each_group_together_and_in_order() {
    for batching in [
        None,
        Some(Batching {
            max_proposals: 16,
            max_bytes: 8 << 20,
            max_entries_in_flight: 2,
        }),
    ] {
        let label = format!("batching {:?}", batching.as_ref().map(|b| b.max_proposals));
        let (c, leader) = start(&[1, 2, 3], |cfg| cfg.batching = batching).await;
        let ids: Vec<NodeId> = c.nodes.keys().copied().collect();
        let mut tasks = Vec::new();
        for g in 0..60u64 {
            let node = c.nodes[&ids[g as usize % 3]].clone();
            let size = 1 + (g % 7) as usize;
            let group: Vec<Vec<u8>> = (0..size)
                .map(|i| {
                    let kind = if (g + i as u64) % 5 == 0 {
                        "reject"
                    } else {
                        "ok"
                    };
                    format!("{kind}-g{g}-{i}").into_bytes()
                })
                .collect();
            tasks.push(tokio::spawn(async move {
                let res = node.propose_many(group.clone()).await;
                (group, res)
            }));
        }
        let mut indexes = std::collections::BTreeSet::new();
        for t in tasks {
            let (group, res) = t.await.unwrap();
            let proposed = res.unwrap();
            assert_eq!(proposed.len(), group.len());
            let index = proposed[0].index;
            indexes.insert(index);
            for (p, r) in group.iter().zip(&proposed) {
                assert_eq!(r.index, index, "{label}: group split over entries");
                let rejected = matches!(r.outcome, Outcome::Rejected { .. });
                assert_eq!(rejected, p.starts_with(b"reject"), "{label}: wrong outcome");
            }
        }
        let last = c.nodes[&leader].status().last_log_index.unwrap();
        for n in c.nodes.values() {
            n.wait_applied(last, Duration::from_secs(10)).await.unwrap();
        }
        // Every node recorded the accepted proposals in log order; within each
        // index, a group's accepted members are adjacent and in order.
        let record = c.sms[&leader].stats().record.unwrap();
        for g in 0..60u64 {
            let mine: Vec<(usize, &(u64, Vec<u8>))> = record
                .iter()
                .enumerate()
                .filter(|(_, (_, p))| String::from_utf8_lossy(p).contains(&format!("-g{g}-")))
                .collect();
            let positions: Vec<usize> = mine.iter().map(|(i, _)| *i).collect();
            let members: Vec<usize> = mine
                .iter()
                .map(|(_, (_, p))| {
                    let s = String::from_utf8_lossy(p).into_owned();
                    s.rsplit('-').next().unwrap().parse().unwrap()
                })
                .collect();
            assert!(
                members.windows(2).all(|w| w[0] < w[1]),
                "{label}: group {g} out of order"
            );
            assert!(
                positions.windows(2).all(|w| w[1] == w[0] + 1),
                "{label}: group {g} interleaved with other proposals"
            );
        }
        for id in &ids {
            assert_eq!(c.sms[id].stats().record.unwrap(), record);
        }
        println!("{label}: 60 groups in {} log entries", indexes.len());
        for n in c.nodes.values() {
            n.shutdown().await;
        }
    }
}

/// `app_request` crosses the delayed node-to-node connection, and a handler
/// can await `propose` on its own node.
#[tokio::test(flavor = "multi_thread")]
async fn app_request_pays_the_round_trip_and_the_handler_can_propose() {
    let (c, leader) = start(&[1, 2, 3], |cfg| {
        cfg.delays = DelayTable {
            default_rtt_ms: 20.0,
            pairs: vec![tursoraft_raft::PairRtt {
                a: 1,
                b: 3,
                rtt_ms: 60.0,
            }],
        };
    })
    .await;
    for node in c.nodes.values() {
        node.set_app_handler(Arc::new(|node: Node, request: Vec<u8>| {
            Box::pin(async move {
                if request == b"echo" {
                    return Ok(format!("echo from {}", node.id()).into_bytes());
                }
                let p = node.propose(request).await.map_err(|e| e.to_string())?;
                Ok(p.index.to_le_bytes().to_vec())
            })
        }));
    }
    for (from, to, rtt_ms) in [(1, 2, 20.0), (1, 3, 60.0), (3, 2, 20.0)] {
        let mut times = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            let answer = c.nodes[&from]
                .app_request(to, b"echo".to_vec(), Duration::from_secs(5))
                .await
                .unwrap();
            times.push(start.elapsed());
            assert_eq!(answer, format!("echo from {to}").into_bytes());
        }
        times.sort();
        let median = times[times.len() / 2];
        let rtt = Duration::from_secs_f64(rtt_ms / 1000.0);
        println!("app_request {from}->{to}, table {rtt_ms} ms: median {median:?}");
        // The delay writer sends 1 ms early to make up for tokio's timer
        // firing late, so a measurement may land just under the table. The
        // upper bound is below two round trips, which take at least 2 × RTT
        // less the 1 ms the delay writer saves per round trip; the rest is
        // room for CPU sharing, such as debug builds of all workspace tests
        // at once.
        assert!(
            median >= rtt - Duration::from_millis(1) && median < 2 * rtt - Duration::from_millis(2),
            "median app_request {from}->{to} {median:?}, table {rtt:?}"
        );
    }

    // A follower sends a request to the leader's handler, which proposes it.
    let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();
    let answer = c.nodes[&follower]
        .app_request(
            leader,
            b"through the handler".to_vec(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let index = u64::from_le_bytes(answer.try_into().unwrap());
    c.nodes[&follower]
        .wait_applied(index, Duration::from_secs(5))
        .await
        .unwrap();
    let record = c.sms[&follower].stats().record.unwrap();
    assert!(record.contains(&(index, b"through the handler".to_vec())));

    // A handler error reaches the caller as Error::App.
    let err = c.nodes[&follower]
        .app_request(leader, b"reject this".to_vec(), Duration::from_secs(5))
        .await;
    assert!(
        err.is_ok(),
        "a rejected proposal is an Outcome, not an error: {err:?}"
    );
    for n in c.nodes.values() {
        n.set_app_handler(Arc::new(|_, _| Box::pin(async { Err("nope".to_string()) })));
    }
    let err = c.nodes[&follower]
        .app_request(leader, b"x".to_vec(), Duration::from_secs(5))
        .await;
    assert_eq!(err, Err(Error::App("nope".into())));
    for n in c.nodes.values() {
        n.shutdown().await;
    }
}

/// A node shut down and started again in the same process, on the same
/// address and directory, gets the entries proposed after it came back. The
/// old node's runtime stays alive, as in an application that restarts its
/// node without exiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_restarted_in_the_same_process_gets_new_entries() {
    let (mut c, leader) = start(&[1, 2, 3], |_| {}).await;
    let follower = *c.nodes.keys().find(|id| **id != leader).unwrap();
    c.nodes[&leader].propose(b"before".to_vec()).await.unwrap();
    let old = c.nodes.remove(&follower).unwrap();
    old.shutdown().await;
    drop(old);
    c.nodes[&leader]
        .propose(b"while down".to_vec())
        .await
        .unwrap();
    let (node, sm) = start_node(&c.dir, &c.config, follower).await;
    let p = c.nodes[&leader].propose(b"after".to_vec()).await.unwrap();
    let applied = node.wait_applied(p.index, Duration::from_secs(5)).await;
    assert!(
        applied.is_ok(),
        "node {follower} did not apply index {}: {applied:?}; its status: {:?}",
        p.index,
        node.status()
    );
    let record = sm.stats().record.unwrap();
    assert!(record.contains(&(p.index, b"after".to_vec())));
    c.nodes.insert(follower, node);
    for n in c.nodes.values() {
        n.shutdown().await;
    }
}
