//! Clusters of node processes over real TCP with real fsyncs. Timings are
//! printed; run with `--release --nocapture -- --test-threads=1` for the
//! numbers in the phase 1b report.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{concurrent, median, ms, sequential, Cluster};
use tursoraft_raft::{
    Batching, DelayTable, LogConfig, NodeId, Outcome, PairRtt, PeerStats, RaftTimeouts,
};

fn proposals_of(record: &Option<Vec<(u64, Vec<u8>)>>) -> BTreeSet<Vec<u8>> {
    record
        .as_ref()
        .unwrap()
        .iter()
        .map(|(_, p)| p.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_elect_replicate_and_apply_in_the_same_order() {
    let mut c = Cluster::new(&[1, 2, 3], &["--record"], |_| {});
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let client = Arc::new(c.client(leader));

    sequential(&client, "seq", 50).await;
    let batch = (0..200).map(|i| format!("conc-{i}").into_bytes()).collect();
    for (p, res) in concurrent(&client, batch).await {
        assert_eq!(res.unwrap().outcome, Outcome::Applied, "{p:?}");
    }

    let index = c.converge(leader, &[1, 2, 3]).await;
    let stats: Vec<_> = futures_join(&c, &[1, 2, 3]).await;
    for (id, s) in [1, 2, 3].iter().zip(&stats) {
        println!(
            "node {id}: applied {:?}, entries {}, proposals {}, digest {:x}",
            s.applied, s.entries, s.proposals, s.digest
        );
        assert_eq!(s.applied, Some(index));
        assert_eq!(s.proposals, 250);
        assert_eq!(
            s.record, stats[0].record,
            "node {id} applied different proposals or order"
        );
        assert_eq!(s.digest, stats[0].digest);
    }
}

async fn futures_join(c: &Cluster, ids: &[NodeId]) -> Vec<tursoraft_raft::CountingStats> {
    let mut out = Vec::new();
    for id in ids {
        out.push(c.stats(*id).await);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_killed_with_sigkill_loses_no_acknowledged_proposal_and_rejoins_from_disk() {
    let mut c = Cluster::new(&[1, 2, 3], &["--record"], |cfg| {
        cfg.batching = Some(Batching::default());
    });
    c.start_all();
    let old_leader = c.initialize(&[1, 2, 3]).await;

    // (sent, acknowledged, proposal)
    let acked = Arc::new(Mutex::new(Vec::<(Instant, Instant, Vec<u8>)>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..8u64 {
        let clients: Vec<_> = [1, 2, 3]
            .iter()
            .map(|id| c.client(*id).with_timeout(Duration::from_secs(5)))
            .collect();
        let (acked, stop) = (acked.clone(), stop.clone());
        writers.push(tokio::spawn(async move {
            let mut target = w as usize % 3;
            let mut seq = 0;
            while !stop.load(Ordering::Relaxed) {
                let p = format!("w{w}-{seq}").into_bytes();
                seq += 1;
                let sent = Instant::now();
                match clients[target].propose(p.clone()).await {
                    Ok(r) => {
                        assert_eq!(r.outcome, Outcome::Applied);
                        acked.lock().unwrap().push((sent, Instant::now(), p));
                    }
                    Err(_) => {
                        target = (target + 1) % 3;
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let before_kill = c.stats(old_leader).await.applied.unwrap();
    let killed_at = Instant::now();
    c.kill(old_leader);
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }

    let survivors: Vec<NodeId> = [1, 2, 3]
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();
    let new_leader = c.wait_leader(&survivors).await;
    assert_ne!(new_leader, old_leader);
    let acked = acked.lock().unwrap().clone();
    let first_after = acked
        .iter()
        .filter(|(sent, _, _)| *sent > killed_at)
        .map(|(_, a, _)| *a)
        .min()
        .unwrap();
    println!(
        "killed leader {old_leader}; new leader {new_leader}; {} proposals acknowledged; first \
         acknowledgement of a proposal sent after the kill came {:.0} ms after the kill",
        acked.len(),
        ms(first_after - killed_at)
    );
    let acked: BTreeSet<Vec<u8>> = acked.into_iter().map(|(_, _, p)| p).collect();

    let index = c.converge(new_leader, &survivors).await;
    for id in &survivors {
        let s = c.stats(*id).await;
        let applied = proposals_of(&s.record);
        let missing = acked.difference(&applied).count();
        println!(
            "node {id}: applied {:?}, {} proposals, {missing} acknowledged missing",
            s.applied,
            applied.len()
        );
        assert_eq!(missing, 0, "node {id} lost acknowledged proposals");
    }

    // The killed node restarts from its own log and state machine file.
    c.start(old_leader);
    let st = c.status(old_leader).await;
    let s = c.stats(old_leader).await;
    println!(
        "restarted node {old_leader}: last log index {:?}, applied {:?}, applied before the kill {before_kill}",
        st.last_log_index, s.applied
    );
    assert!(s.applied.unwrap() >= before_kill);
    assert!(st.last_log_index.unwrap() >= before_kill);

    c.wait_applied(&[old_leader], index).await;
    let s = c.stats(old_leader).await;
    assert_eq!(
        s.digest,
        c.stats(new_leader).await.digest,
        "the rejoined node diverged"
    );
    assert!(acked.is_subset(&proposals_of(&s.record)));
    let p = c
        .client(old_leader)
        .propose(b"after rejoin".to_vec())
        .await
        .unwrap();
    c.wait_applied(&[1, 2, 3], p.index).await;
}

/// Three voters with 10 ms between them and one non-voting follower that is
/// (a) running, (b) stopped with SIGSTOP. Acknowledgement time must not
/// change.
#[tokio::test(flavor = "multi_thread")]
async fn stopped_non_voting_follower_does_not_slow_writes() {
    let mut c = Cluster::new(&[1, 2, 3, 4], &["--record"], |cfg| {
        cfg.delays = DelayTable::uniform(10.0);
    });
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    c.client(leader).add_follower(4, c.addr(4)).await.unwrap();
    let st = c.status(leader).await;
    assert_eq!(st.voters, vec![1, 2, 3]);
    assert_eq!(st.learners, vec![4]);
    let client = c.client(leader);

    let running = sequential(&client, "running", 50).await;
    let index = c.converge(leader, &[1, 2, 3, 4]).await;
    assert_eq!(c.stats(4).await.digest, c.stats(leader).await.digest);

    c.signal(4, "-STOP");
    let stopped = sequential(&client, "stopped", 50).await;
    println!(
        "median time to acknowledge at the leader: follower running {:.2} ms, follower stopped {:.2} ms",
        ms(median(&running)),
        ms(median(&stopped))
    );
    assert!(median(&stopped) < median(&running) + Duration::from_millis(3));

    c.signal(4, "-CONT");
    let index2 = c.converge(leader, &[1, 2, 3, 4]).await;
    assert!(index2 >= index + 50);
    assert_eq!(c.stats(4).await.digest, c.stats(leader).await.digest);
}

/// The same with a follower that is 1 s away from every voter, and timeouts
/// set for the voters alone. On openraft 0.9 this needed a heartbeat longer
/// than 1 s; now the follower still receives every entry.
#[tokio::test(flavor = "multi_thread")]
async fn slow_non_voting_follower_does_not_slow_writes() {
    let mut c = Cluster::new(&[1, 2, 3, 4], &["--record"], |cfg| {
        let far = |a| PairRtt {
            a,
            b: 4,
            rtt_ms: 1000.0,
        };
        cfg.delays = DelayTable {
            default_rtt_ms: 10.0,
            pairs: vec![far(1), far(2), far(3)],
        };
        // Timeouts for the 10 ms between voters only: heartbeat 50 ms,
        // election timeout 150 to 300 ms. The network layer gives replication
        // to the far follower its own, longer timeout.
        cfg.timeouts = Some(RaftTimeouts::for_max_rtt(Duration::from_millis(10)));
    });
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let client = c.client(leader);
    let near_only = sequential(&client, "before", 30).await;
    c.client(leader).add_follower(4, c.addr(4)).await.unwrap();
    let with_far = sequential(&client, "after", 30).await;
    println!(
        "median time to acknowledge: without the far follower {:.2} ms, with it {:.2} ms",
        ms(median(&near_only)),
        ms(median(&with_far))
    );
    assert!(median(&with_far) < median(&near_only) + Duration::from_millis(3));
    let index = c.status(leader).await.last_log_index.unwrap();
    let start = Instant::now();
    c.wait_applied(&[4], index).await;
    println!("far follower caught up {:.0} ms later", ms(start.elapsed()));
    assert_eq!(c.stats(4).await.digest, c.stats(leader).await.digest);
}

/// Round trip and time to acknowledge for three voters with the same round
/// trip between all pairs.
///
/// Every write costs one round trip, whether it follows a pause or comes
/// right after the previous one. openraft 0.10 pipelines AppendEntries, so a
/// new entry does not wait for the answer to the commit-index-only request
/// that follows each commit. (On openraft 0.9, with one request in flight per
/// follower, back-to-back writes cost two round trips.)
#[tokio::test(flavor = "multi_thread")]
async fn delay_table_sets_round_trip_and_acknowledgement_time() {
    for rtt_ms in [10.0, 100.0] {
        let mut c = Cluster::new(&[1, 2, 3], &[], |cfg| {
            cfg.delays = DelayTable::uniform(rtt_ms)
        });
        c.start_all();
        let leader = c.initialize(&[1, 2, 3]).await;
        let client = c.client(leader);
        let rtt = Duration::from_secs_f64(rtt_ms / 1000.0);
        for peer in [1, 2, 3].into_iter().filter(|p| *p != leader) {
            let mut pings = Vec::new();
            for _ in 0..10 {
                pings.push(client.ping_peer(peer).await.unwrap());
            }
            let m = median(&pings);
            println!(
                "table {rtt_ms} ms: measured round trip {leader}->{peer}: median {:.2} ms",
                ms(m)
            );
            // Up to about 1 ms of real time on top of the table in a release
            // build run alone; more when debug builds of several clusters
            // share the CPUs. The delay writer sends 1 ms early to make up
            // for tokio's timer firing late, so a measurement may land just
            // under the table.
            assert!(
                m >= rtt - Duration::from_millis(1) && m < rtt + Duration::from_millis(3),
                "{m:?}"
            );
        }

        let sent_before = c.status(leader).await.sent;
        let mut isolated = Vec::new();
        for i in 0..30 {
            tokio::time::sleep(2 * rtt).await;
            let start = Instant::now();
            client
                .propose(format!("isolated-{i}").into_bytes())
                .await
                .unwrap();
            isolated.push(start.elapsed());
        }
        let sent_between = c.status(leader).await.sent;
        let back_to_back = sequential(&client, "back-to-back", 30).await;
        let sent_after = c.status(leader).await.sent;
        let per_write = |a: &[PeerStats], b: &[PeerStats]| {
            (b[0].append_requests - a[0].append_requests) as f64 / 30.0
        };
        let summary = |t: &[Duration]| {
            let mut t = t.to_vec();
            t.sort();
            format!(
                "min {:.2} / median {:.2} / max {:.2} ms",
                ms(t[0]),
                ms(t[t.len() / 2]),
                ms(t[t.len() - 1])
            )
        };
        println!(
            "table {rtt_ms} ms: time to acknowledge at the leader, 30 writes each:\n  \
             after a pause of 2 RTT: {} ({:.1} AppendEntries per follower per write)\n  \
             back-to-back:           {} ({:.1} AppendEntries per follower per write)",
            summary(&isolated),
            per_write(&sent_before, &sent_between),
            summary(&back_to_back),
            per_write(&sent_between, &sent_after),
        );
        let lower = rtt - Duration::from_millis(1);
        let m = median(&isolated);
        assert!(m >= lower && m < rtt + Duration::from_millis(8), "{m:?}");
        let m = median(&back_to_back);
        assert!(m >= lower && m < rtt + Duration::from_millis(8), "{m:?}");
    }
}

/// How openraft sends entries to a follower when many unbatched writes wait
/// at once: how many AppendEntries are in flight per follower, and how many
/// entries each carries. openraft 0.9 kept one in flight; 0.10 pipelines.
#[tokio::test(flavor = "multi_thread")]
async fn openraft_append_entries_in_flight_per_follower() {
    let mut c = Cluster::new(&[1, 2, 3], &[], |cfg| {
        cfg.delays = DelayTable::uniform(100.0)
    });
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let client = Arc::new(c.client(leader));
    let start = Instant::now();
    let results = concurrent(
        &client,
        (0..500).map(|i| format!("p{i}").into_bytes()).collect(),
    )
    .await;
    let elapsed = start.elapsed();
    assert!(results.iter().all(|(_, r)| r.is_ok()));
    let st = c.status(leader).await;
    println!(
        "500 concurrent unbatched proposals at 100 ms: {:.0} ms, {:.0} per second",
        ms(elapsed),
        500.0 / elapsed.as_secs_f64()
    );
    for s in &st.sent {
        println!(
            "to node {}: {} AppendEntries ({} with entries), {} entries, at most {} per request, \
             at most {} in flight",
            s.target,
            s.append_requests,
            s.append_requests_with_entries,
            s.entries_sent,
            s.max_entries_per_request,
            s.max_in_flight
        );
        assert!(s.max_in_flight > 1, "no pipelining to node {}", s.target);
        assert!(s.max_entries_per_request > 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn batching_puts_concurrent_proposals_in_fewer_entries_and_answers_each_caller() {
    for batching in [true, false] {
        let mut c = Cluster::new(&[1, 2, 3], &["--reject-prefix", "reject"], |cfg| {
            cfg.delays = DelayTable::uniform(10.0);
            cfg.batching = batching.then(Batching::default);
        });
        c.start_all();
        let leader = c.initialize(&[1, 2, 3]).await;
        let client = Arc::new(c.client(leader));
        let before = c.stats(leader).await;
        let proposals = (0..300)
            .map(|i| {
                if i % 7 == 0 {
                    format!("reject-{i}")
                } else {
                    format!("ok-{i}")
                }
                .into_bytes()
            })
            .collect();
        let results = concurrent(&client, proposals).await;
        let mut indexes = BTreeSet::new();
        for (p, res) in &results {
            let r = res.as_ref().unwrap();
            indexes.insert(r.index);
            let rejected = matches!(r.outcome, Outcome::Rejected { .. });
            assert_eq!(
                rejected,
                p.starts_with(b"reject"),
                "wrong outcome for {:?}",
                String::from_utf8_lossy(p)
            );
        }
        c.converge(leader, &[1, 2, 3]).await;
        let after = c.stats(leader).await;
        let entries = after.entries_with_proposals - before.entries_with_proposals;
        println!(
            "batching {batching}: 300 proposals ({} rejected) in {entries} log entries at {} distinct indexes",
            after.rejected,
            indexes.len()
        );
        assert_eq!(after.rejected, 43);
        assert_eq!(indexes.len() as u64, entries);
        if batching {
            assert!(entries < 300);
        } else {
            assert_eq!(entries, 300);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn node_joining_after_compaction_gets_a_snapshot_then_later_entries() {
    let mut c = Cluster::new(&[1, 2, 3, 4], &["--record"], |cfg| {
        cfg.log = LogConfig {
            snapshot_every: 20,
            keep_after_snapshot: 0,
            ..LogConfig::default()
        };
    });
    for id in [1, 2, 3] {
        c.start(id);
    }
    let leader = c.initialize(&[1, 2, 3]).await;
    let client = c.client(leader);
    sequential(&client, "early", 100).await;
    let st = c.status(leader).await;
    println!(
        "leader before join: snapshot {:?}, purged {:?}",
        st.snapshot, st.purged
    );
    assert!(st.purged.is_some_and(|p| p > 50));

    c.start(4);
    c.client(leader).add_follower(4, c.addr(4)).await.unwrap();
    c.converge(leader, &[4]).await;
    assert!(
        c.snapshots_installed(4).await >= 1,
        "node 4 did not install a snapshot"
    );
    sequential(&client, "late", 10).await;
    c.converge(leader, &[1, 2, 3, 4]).await;
    let s4 = c.stats(4).await;
    assert_eq!(s4.digest, c.stats(leader).await.digest);
    assert_eq!(s4.record, c.stats(leader).await.record);
    assert_eq!(proposals_of(&s4.record).len(), 110);

    // Make the new node a voter and the old voter 3 a non-voting follower.
    c.client(leader)
        .change_voters([1, 2, 4].into())
        .await
        .unwrap();
    let st = c.status(leader).await;
    println!(
        "after change_voters: voters {:?}, learners {:?}",
        st.voters, st.learners
    );
    assert_eq!(st.voters, vec![1, 2, 4]);
    assert_eq!(st.learners, vec![3]);
    let p = c
        .client(4)
        .propose(b"after membership change".to_vec())
        .await
        .unwrap();
    c.wait_applied(&[1, 2, 3, 4], p.index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proposal_made_on_a_follower_takes_effect() {
    let mut c = Cluster::new(&[1, 2, 3], &["--record"], |cfg| {
        cfg.delays = DelayTable::uniform(10.0)
    });
    c.start_all();
    let leader = c.initialize(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let fc = c.client(follower);

    let start = Instant::now();
    let p = fc.propose(b"from a follower".to_vec()).await.unwrap();
    println!(
        "proposal on follower {follower}: index {}, {:.2} ms",
        p.index,
        ms(start.elapsed())
    );
    assert_eq!(p.outcome, Outcome::Applied);
    c.wait_applied(&[1, 2, 3], p.index).await;
    for id in [1, 2, 3] {
        let s = c.stats(id).await;
        assert!(s
            .record
            .unwrap()
            .contains(&(p.index, b"from a follower".to_vec())));
    }
    let start = Instant::now();
    let read = fc.linearizable_read().await.unwrap();
    println!(
        "linearizable read on follower: index {read}, {:.2} ms",
        ms(start.elapsed())
    );
    assert!(read >= p.index);
    assert_eq!(c.status(follower).await.leader, Some(leader));
    let err = c.client(follower).add_follower(9, "127.0.0.1:1").await;
    assert!(err.is_ok(), "membership changes are forwarded too: {err:?}");
}

/// `transfer_leader` hands leadership to a chosen voter while writers run,
/// and no acknowledged proposal is lost.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_leader_moves_leadership_without_losing_writes() {
    for rtt_ms in [0.0, 100.0] {
        let mut c = Cluster::new(&[1, 2, 3], &["--record"], |cfg| {
            cfg.delays = DelayTable::uniform(rtt_ms);
            cfg.batching = Some(Batching::default());
        });
        c.start_all();
        let old = c.initialize(&[1, 2, 3]).await;
        let target = [1, 2, 3].into_iter().find(|id| *id != old).unwrap();

        // (sent, acknowledged, proposal) of every acknowledged proposal.
        let acked = Arc::new(Mutex::new(Vec::<(Instant, Instant, Vec<u8>)>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for w in 0..8u64 {
            let client = c.client(old).with_timeout(Duration::from_secs(10));
            let (acked, stop) = (acked.clone(), stop.clone());
            writers.push(tokio::spawn(async move {
                let mut seq = 0;
                while !stop.load(Ordering::Relaxed) {
                    let p = format!("w{w}-{seq}").into_bytes();
                    seq += 1;
                    let sent = Instant::now();
                    if let Ok(r) = client.propose(p.clone()).await {
                        assert_eq!(r.outcome, Outcome::Applied);
                        acked.lock().unwrap().push((sent, Instant::now(), p));
                    }
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(500).max(Duration::from_secs_f64(rtt_ms / 100.0)))
            .await;
        let called = Instant::now();
        c.client(old).transfer_leader(target).await.unwrap();
        let seen = called.elapsed();
        // The writers above are still waiting for proposals made during the
        // transfer, so time a new one issued now.
        let fresh = c.client(old).propose(b"fresh".to_vec()).await.unwrap();
        assert_eq!(fresh.outcome, Outcome::Applied);
        let fresh_acked = called.elapsed();
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.await.unwrap();
        }
        assert_eq!(c.wait_leader(&[1, 2, 3]).await, target);

        let acked = acked.lock().unwrap().clone();
        let mut ack_times: Vec<Instant> = acked.iter().map(|(_, a, _)| *a).collect();
        ack_times.sort();
        let stall = ack_times
            .windows(2)
            .filter(|w| w[1] > called)
            .map(|w| w[1] - w[0])
            .max()
            .unwrap();
        println!(
            "transfer {old}->{target} at {rtt_ms} ms: new leader seen after {:.1} ms; a write \
             issued at the old leader then was acknowledged {:.1} ms after the call; longest \
             gap between acknowledgements {:.1} ms; {} proposals acknowledged",
            ms(seen),
            ms(fresh_acked),
            ms(stall),
            acked.len()
        );
        let mut acked: BTreeSet<Vec<u8>> = acked.into_iter().map(|(_, _, p)| p).collect();
        acked.insert(b"fresh".to_vec());
        let index = c.converge(target, &[1, 2, 3]).await;
        for id in [1, 2, 3] {
            let s = c.stats(id).await;
            assert_eq!(s.applied, Some(index));
            assert!(
                acked.is_subset(&proposals_of(&s.record)),
                "node {id} lost acknowledged proposals"
            );
        }
    }
}
