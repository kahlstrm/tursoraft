//! Moving a running cluster's voters and leader from Europe to Asia and back
//! under a bank-transfer load, with failures in the middle of the move. Six
//! node processes with the round trips of `tursoraft_proof::moves::RTTS`.
//!
//! ```text
//! cargo test -p tursoraft-proof --test moves -- --nocapture
//! ```

use std::collections::BTreeSet;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tursoraft_node::{CommitMode, Error};
use tursoraft_proof::bank::{
    check, read_state, schema, transfer, Attempt, Load, LoadSpec, ReadTally, Told, Verdict,
    INITIAL_BALANCE,
};
use tursoraft_proof::cluster::{ms, Cluster};
use tursoraft_proof::moves::{delays, move_voters, place, Seen, ALL, ASIA, EUROPE};
use tursoraft_raft::NodeId;

const BIN: &str = env!("CARGO_BIN_EXE_tursoraft-proof");
const ACCOUNTS: i64 = 50;
/// One node in each region with writers: Frankfurt and Singapore.
const NEAR_FAR: [NodeId; 2] = [1, 4];

/// Done-when 1 and 3: the voters and the leader go from Europe (leader
/// Frankfurt) to Asia (leader Singapore) and back, while quorum, async and
/// interactive quorum writers run in Frankfurt and in Singapore and a reader
/// in each sums all balances. Nothing acknowledged may be missing, async
/// included, since no node fails.
#[test]
fn voters_and_leader_move_between_regions_under_load() {
    run(async {
        let c = start_geo().await;
        let loads = Loads::start(&c, 2);
        let mut last = Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;
        for (voters, leader) in [(ASIA, 4), (EUROPE, 1)] {
            let before = Instant::now();
            let moved = move_voters(&c, &voters, leader, true).await;
            let settled = Instant::now() + Duration::from_secs(1);
            tokio::time::sleep(Duration::from_secs(3)).await;
            let status = c.status(leader).await;
            println!(
                "move to {:?}, leader {}: {}; voters now {:?}, non-voters {:?}",
                voters,
                place(leader),
                moved.summary(),
                status.voters,
                status.learners
            );
            let want: BTreeSet<NodeId> = voters.iter().copied().collect();
            assert_eq!(status.voters.iter().copied().collect::<BTreeSet<_>>(), want);
            let attempts = loads.attempts();
            print_window("  steady before", &attempts, last, before);
            print_window("  during the move + 1 s", &attempts, before, settled);
            let v = check_at(&c, leader, &attempts).await;
            println!("  node {leader}: {}", v.summary());
            assert!(
                v.missing_acknowledged.is_empty(),
                "{:?}",
                v.missing_acknowledged
            );
            last = settled;
        }
        let (attempts, reads) = loads.stop().await;
        final_check(&c, &attempts, false).await;
        check_readers(&reads);
    });
}

/// Done-when 4: the old leader (Frankfurt) is killed with SIGKILL at three
/// moments of the change from the European to the Asian voters, under load:
/// right after it appended the joint configuration, after the joint
/// configuration was committed, and right after it appended the final one.
/// The move is then finished from Singapore by calling the same membership
/// calls again, and the old leader restarted. Each round ends by moving back
/// to Europe. Acknowledged async transfers may be lost; they are counted.
#[test]
fn the_old_leader_killed_in_the_middle_of_the_move() {
    run(async {
        let mut c = start_geo().await;
        let loads = Loads::start(&c, 2);
        for moment in [
            Moment::JointAppended,
            Moment::JointCommitted,
            Moment::FinalAppended,
        ] {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let old = 1;
            let target: BTreeSet<NodeId> = ASIA.into_iter().collect();
            let client = c.client(4);
            let started = Instant::now();
            let call = tokio::spawn(async move { client.raft().change_voters(target).await });
            let seen = moment.wait(&c, old).await;
            let killed = Instant::now();
            c.kill(old);
            let leader = c.wait_leader(&c.running()).await;
            let elected = killed.elapsed();
            let voters_then = c.status(leader).await.voters;
            let first = call.await.unwrap();
            let finish = move_voters(&c, &ASIA, 4, false).await;
            let done = Instant::now();
            let attempts = loads.attempts();
            let v = check_at(&c, 4, &attempts).await;
            let lost_async = v
                .missing_acknowledged
                .iter()
                .filter(|k| k.starts_with("a-"))
                .count();
            println!(
                "{moment:?}: killed node {old} {:.0} ms into the move, when it had {seen}; node {leader} led after {:.0} ms \
                 with voters {voters_then:?}; the first change_voters returned {first:?}; finishing the move: {}; \
                 node 4: {}; acknowledged async transfers missing so far: {lost_async}",
                ms(killed - started),
                ms(elected),
                finish.summary(),
                v.summary()
            );
            print_window(
                "  from the start of the move to its end",
                &attempts,
                started,
                done,
            );
            let quorum_missing: Vec<&String> = v
                .missing_acknowledged
                .iter()
                .filter(|k| !k.starts_with("a-"))
                .collect();
            assert!(quorum_missing.is_empty(), "{quorum_missing:?}");
            assert_eq!(v.violations_during_load(), Vec::<String>::new());
            c.start(old);
            c.converge(4, &ALL).await;
            let back = move_voters(&c, &EUROPE, 1, true).await;
            println!("  back to Europe: {}", back.summary());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (attempts, reads) = loads.stop().await;
        final_check(&c, &attempts, true).await;
        check_readers(&reads);
    });
}

/// Done-when 4, other side: Asian nodes that are to become voters are
/// killed right after the leader appended the joint configuration. With one
/// of three down, the move goes on. With two of three down, the joint
/// configuration has no Asian majority, so nothing commits until one of them
/// is back; the test counts how long writers wait.
#[test]
fn new_voters_killed_in_the_middle_of_the_move() {
    run(async {
        let mut c = start_geo().await;
        let loads = Loads::start(&c, 2);
        for victims in [vec![5], vec![5, 6]] {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let target: BTreeSet<NodeId> = ASIA.into_iter().collect();
            let client = c.client(4);
            let started = Instant::now();
            let call = tokio::spawn(async move { client.raft().change_voters(target).await });
            let seen = Moment::JointAppended.wait(&c, 1).await;
            for id in &victims {
                c.kill(*id);
            }
            let down = Duration::from_secs(4);
            tokio::time::sleep(down).await;
            // Quorum and interactive writes; async ones are acknowledged
            // before they commit.
            let during = Seen::of(
                &loads.attempts(),
                |a| !a.key.starts_with("a-"),
                started + Duration::from_millis(500),
                Instant::now(),
            );
            // Bring back one, then finish the move.
            c.start(victims[victims.len() - 1]);
            let restarted = Instant::now();
            let first = call.await.unwrap();
            let finish = move_voters(&c, &ASIA, 4, false).await;
            let done = Instant::now();
            for id in &victims[..victims.len() - 1] {
                c.start(*id);
            }
            c.converge(4, &ALL).await;
            let attempts = loads.attempts();
            let v = check_at(&c, 4, &attempts).await;
            println!(
                "killed {victims:?} when the leader had {seen}, for {:.0} ms; the first change_voters returned {first:?}; \
                 the move ended {:.0} ms after the restart; finishing the move: {}; node 4: {}",
                ms(down),
                ms(done - restarted),
                finish.summary(),
                v.summary()
            );
            println!(
                "  quorum and interactive writers, from 0.5 s after the start of the move until a victim restarted: {}",
                during.summary()
            );
            print_window(
                "  from the start of the move to its end",
                &attempts,
                started,
                done,
            );
            assert!(
                v.missing_acknowledged.is_empty(),
                "{:?}",
                v.missing_acknowledged
            );
            assert_eq!(v.violations_during_load(), Vec::<String>::new());
            if victims.len() == 2 {
                assert_eq!(
                    during.acked, 0,
                    "a write was acknowledged without an Asian majority"
                );
            }
            let back = move_voters(&c, &EUROPE, 1, true).await;
            println!("  back to Europe: {}", back.summary());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (attempts, reads) = loads.stop().await;
        final_check(&c, &attempts, false).await;
        check_readers(&reads);
    });
}

/// Done-when 5, a break the checker must catch. After the move to Asia, a
/// voter loses its disk: Tokyo is killed and falls behind while Singapore
/// and Hong Kong acknowledge quorum transfers; then both are killed, Hong
/// Kong restarts with an empty directory and votes for Tokyo, which leads
/// with a log that lacks those transfers. The checker must report them as
/// lost on Tokyo and Hong Kong. The test then writes 20 transfers through
/// Tokyo, restarts Singapore, and prints what Singapore and Frankfurt do with
/// the new leader's log, which conflicts with entries they know are
/// committed.
#[test]
fn a_voter_that_forgets_its_log_loses_acknowledged_writes_and_the_checker_sees_it() {
    run(async {
        let mut c = start_geo().await;
        move_voters(&c, &ASIA, 4, true).await;
        let spec = LoadSpec::new("q", vec![(4, 4)], CommitMode::Quorum, ACCOUNTS);
        let load = Load::start(&c, &spec);
        tokio::time::sleep(Duration::from_secs(1)).await;
        c.kill(6);
        let behind = Instant::now();
        tokio::time::sleep(Duration::from_secs(2)).await;
        c.kill(4);
        c.kill(5);
        let attempts = load.stop().await.attempts;
        let acked_while_6_down = attempts
            .iter()
            .filter(|a| a.result.is_ok() && a.started > behind)
            .count();
        assert!(acked_while_6_down > 0);
        c.wipe(5);
        c.start(5);
        c.start(6);
        let leader = c.wait_leader(&[5, 6]).await;
        c.converge(leader, &[5, 6]).await;
        let reference = c.dump(leader).await;
        for id in [5, 6] {
            let v = check(&read_state(&c.client(id)).await, &attempts, ACCOUNTS);
            println!("node {id} ({}): {}", place(id), v.summary());
            assert_eq!(v.violations(), Vec::<String>::new(), "node {id}");
            assert!(
                !v.missing_acknowledged.is_empty(),
                "node {id}: the checker found no lost transfer"
            );
            assert!(c.dump(id).await == reference, "node {id} differs");
        }
        println!(
            "{acked_while_6_down} transfers were acknowledged while Tokyo was down; node {leader} leads now"
        );
        // Transfers under the new leader, then Singapore comes back.
        for n in 0..20 {
            c.client(leader)
                .execute(transfer(&format!("after-{n}"), 1, 2, 1), CommitMode::Quorum)
                .await
                .unwrap();
        }
        c.start(4);
        tokio::time::sleep(Duration::from_secs(5)).await;
        let new_log = read_state(&c.client(leader)).await;
        for id in [1, 4] {
            let status = c.status(id).await;
            let log = std::fs::read_to_string(c.dir.path().join(format!("node-{id}.log"))).unwrap();
            let panic = log.lines().find(|l| l.starts_with("assertion failed"));
            let here = read_state(&c.client(id)).await;
            let only_here = here
                .transfers
                .keys()
                .filter(|k| !new_log.transfers.contains_key(*k))
                .count();
            let only_there = new_log
                .transfers
                .keys()
                .filter(|k| !here.transfers.contains_key(*k))
                .count();
            println!(
                "node {id} ({}) 5 s later: {} term {} leader {:?} last {:?} applied {:?}; its Raft core panicked: {panic:?}; \
                 it holds {only_here} transfers that node {leader} lacks and lacks {only_there} that node {leader} holds",
                place(id),
                status.state,
                status.term,
                status.leader,
                status.last_log_index,
                status.applied
            );
        }
    });
}

/// The writers of the move tests: quorum, async and interactive quorum,
/// `per_node` of each in Frankfurt and in Singapore, and a one-SELECT reader
/// of the total in each.
struct Loads(Vec<Load>);

impl Loads {
    fn start(c: &Cluster, per_node: usize) -> Loads {
        let writers: Vec<(NodeId, usize)> = NEAR_FAR.iter().map(|n| (*n, per_node)).collect();
        let mut quorum = LoadSpec::new("q", writers.clone(), CommitMode::Quorum, ACCOUNTS);
        quorum.readers = NEAR_FAR.to_vec();
        let asynchronous = LoadSpec::new("a", writers.clone(), CommitMode::Async, ACCOUNTS);
        let mut interactive = LoadSpec::new("i", writers, CommitMode::Quorum, ACCOUNTS);
        interactive.interactive = true;
        Loads(
            [quorum, asynchronous, interactive]
                .iter()
                .map(|spec| Load::start(c, spec))
                .collect(),
        )
    }

    fn attempts(&self) -> Vec<Attempt> {
        self.0.iter().flat_map(|l| l.attempts()).collect()
    }

    async fn stop(self) -> (Vec<Attempt>, Vec<(NodeId, ReadTally)>) {
        let mut attempts = Vec::new();
        let mut reads = Vec::new();
        for load in self.0 {
            let fin = load.stop().await;
            attempts.extend(fin.attempts);
            reads.extend(fin.reads);
        }
        (attempts, reads)
    }
}

/// When to kill the old leader during a change of voters, by what its
/// status says.
#[derive(Debug, Clone, Copy)]
enum Moment {
    /// Its voters are the old and the new ones together.
    JointAppended,
    /// Also, two new voters and one old one have stored the joint entry.
    JointCommitted,
    /// Its voters are the new ones.
    FinalAppended,
}

impl Moment {
    /// Polls node `leader` until the moment, and says what it saw.
    async fn wait(self, c: &Cluster, leader: NodeId) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut joint_index = None;
        loop {
            let s = c.status(leader).await;
            let last = s.last_log_index.unwrap_or(0);
            let voters: BTreeSet<NodeId> = s.voters.iter().copied().collect();
            let stored = |ids: &[NodeId], at: u64| {
                ids.iter()
                    .filter(|id| s.matched.get(id).copied().flatten().unwrap_or(0) >= at)
                    .count()
            };
            if voters.len() == 6 && joint_index.is_none() {
                joint_index = Some(last);
            }
            let reached = match self {
                Moment::JointAppended => joint_index.is_some(),
                Moment::JointCommitted => {
                    joint_index.is_some_and(|j| stored(&ASIA, j) >= 2 && stored(&EUROPE, j) >= 1)
                }
                Moment::FinalAppended => voters == ASIA.into_iter().collect(),
            };
            // A final configuration seen first ends the wait as well.
            if reached || voters == ASIA.into_iter().collect() {
                return format!(
                    "voters {:?}, last log index {last}, joint configuration at or before {joint_index:?}, \
                     stored by Asian nodes up to {:?}",
                    s.voters,
                    ASIA.map(|id| s.matched.get(&id).copied().flatten())
                );
            }
            assert!(Instant::now() < deadline, "{self:?} never came");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// Starts the six nodes, makes Europe the voters and Asia non-voting
/// followers, Frankfurt the leader, and creates the bank.
async fn start_geo() -> Cluster {
    let mut c = Cluster::new(BIN, &ALL, |cfg| cfg.delays = delays(1.0));
    println!("cluster files in {}", c.dir.path().display());
    c.start_all();
    let first = c.initialize(&EUROPE).await;
    for id in ASIA {
        c.client(first)
            .raft()
            .add_follower(id, c.addr(id))
            .await
            .unwrap();
    }
    c.client(1).raft().transfer_leader(1).await.unwrap();
    assert_eq!(c.wait_leader(&ALL).await, 1);
    c.client(1)
        .execute(schema(ACCOUNTS), CommitMode::Quorum)
        .await
        .unwrap();
    c.converge(1, &ALL).await;
    c
}

/// Waits until the running nodes have applied what `leader` has, and checks
/// `leader`'s state while transfers are still being sent.
async fn check_at(c: &Cluster, leader: NodeId, attempts: &[Attempt]) -> Verdict {
    c.converge(leader, &c.running()).await;
    let v = check(&read_state(&c.client(leader)).await, attempts, ACCOUNTS);
    assert_eq!(v.violations_during_load(), Vec::<String>::new());
    v
}

/// Waits until every node has applied everything, checks each against
/// `attempts` and compares full dumps. Acknowledged async transfers may be
/// missing if `async_may_be_lost`; nothing else may.
async fn final_check(c: &Cluster, attempts: &[Attempt], async_may_be_lost: bool) {
    let leader = c.wait_leader(&ALL).await;
    c.converge(leader, &ALL).await;
    let reference = c.dump(leader).await;
    for id in ALL {
        let v = check(&read_state(&c.client(id)).await, attempts, ACCOUNTS);
        println!("end, node {id} ({}): {}", place(id), v.summary());
        assert_eq!(v.violations(), Vec::<String>::new(), "node {id}");
        let missing: Vec<&String> = v
            .missing_acknowledged
            .iter()
            .filter(|k| !(async_may_be_lost && k.starts_with("a-")))
            .collect();
        assert!(missing.is_empty(), "node {id} lacks {missing:?}");
        assert!(
            c.dump(id).await == reference,
            "node {id} differs from node {leader}"
        );
    }
    let count = |prefix: &str, told: Told| {
        attempts
            .iter()
            .filter(|a| a.key.starts_with(prefix) && a.told() == told)
            .count()
    };
    for (prefix, name) in [("q-", "quorum"), ("a-", "async"), ("i-", "interactive")] {
        println!(
            "{name}: {} acknowledged, {} without effect, {} unknown",
            count(prefix, Told::Acknowledged),
            count(prefix, Told::NoEffect),
            count(prefix, Told::Unknown)
        );
    }
    println!("dumps of all six nodes equal");
}

/// Prints what the writers of each mode on each node were told in a window.
fn print_window(label: &str, attempts: &[Attempt], from: Instant, to: Instant) {
    println!("{label} ({:.0} ms):", ms(to - from));
    for node in NEAR_FAR {
        for (prefix, name) in [("q-", "quorum"), ("a-", "async"), ("i-", "interactive")] {
            let s = Seen::of(
                attempts,
                |a| a.node == node && a.key.starts_with(prefix),
                from,
                to,
            );
            let errors: Vec<String> = attempts
                .iter()
                .filter(|a| {
                    a.node == node
                        && a.key.starts_with(prefix)
                        && a.finished >= from
                        && a.finished <= to
                })
                .filter_map(|a| a.result.as_ref().err())
                .filter(|e| !matches!(e, Error::Rejected(_)))
                .map(|e| e.to_string())
                .take(2)
                .collect();
            println!(
                "    {} {name}: {}{}",
                place(node),
                s.summary(),
                if errors.is_empty() {
                    String::new()
                } else {
                    format!("; e.g. {errors:?}")
                }
            );
        }
    }
}

fn check_readers(reads: &[(NodeId, ReadTally)]) {
    let expected = ACCOUNTS * INITIAL_BALANCE;
    for (node, tally) in reads {
        println!(
            "reader in {}: {} reads, totals {:?}, {} failed",
            place(*node),
            tally.reads(),
            tally.totals,
            tally.errors
        );
        assert!(tally.reads() > 0);
        assert_eq!(
            tally.totals.keys().copied().collect::<Vec<_>>(),
            vec![expected]
        );
    }
}

/// Runs `f` on a new runtime, one test at a time.
fn run<F: std::future::Future>(f: F) -> F::Output {
    static SERIAL: Mutex<()> = Mutex::new(());
    let _serial: MutexGuard<'_, ()> = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}
