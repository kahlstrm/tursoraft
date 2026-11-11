//! What a cluster keeps and loses when nodes fail under a bank-transfer
//! load. One node process per node, over real TCP, with the delay table.
//!
//! The tests share the machine's timing, so they run one at a time.
//!
//! ```text
//! cargo test -p tursoraft-proof --test failures -- --nocapture
//! cargo test --release -p tursoraft-proof --test failures -- --ignored --nocapture   # measurement and defect
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tursoraft_changeset::{turso_core, NodeDb};
use tursoraft_node::{CommitMode, Error, ReadLevel, Statement, Value};
use tursoraft_proof::bank::{
    check, read_state, schema, transfer, Attempt, Finished, Load, LoadSpec, Told, Verdict,
    INITIAL_BALANCE,
};
use tursoraft_proof::cluster::{ms, Cluster};
use tursoraft_raft::{ClusterConfig, DelayTable, LogConfig, NodeId};
use tursoraft_statemachine::{Entry, Outcome, StateMachine};

const BIN: &str = env!("CARGO_BIN_EXE_tursoraft-proof");
const ACCOUNTS: i64 = 20;
const QUORUM: CommitMode = CommitMode::Quorum;
const ASYNC: CommitMode = CommitMode::Async;

/// Claims 1 and 2. The leader is killed with SIGKILL `PROOF_KILLS` times
/// (default 3) and restarted each time, while 12 quorum writers (4 per node)
/// send transfers and a reader on each node sums all balances locally.
#[test]
fn quorum_transfers_survive_repeated_leader_kills() {
    run(async {
        let kills = env_or("PROOF_KILLS", 3);
        let all = [1, 2, 3];
        let (mut c, mut leader) = start_bank(10.0, &all, &all, |_| {}).await;
        let mut spec = LoadSpec::new("q", vec![(1, 4), (2, 4), (3, 4)], QUORUM, ACCOUNTS);
        spec.readers = all.to_vec();
        spec.split_reader = Some(1);
        spec.timeout = Duration::from_secs(15);
        let load = Load::start(&c, &spec);
        let mut last_kill = Instant::now();
        for round in 1..=kills {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let old = leader;
            c.kill(old);
            last_kill = Instant::now();
            let survivors = others(&all, old);
            leader = c.wait_leader(&survivors).await;
            let elected = last_kill.elapsed();
            // Every transfer acknowledged so far is committed, so it is in
            // the new leader's log.
            let attempts = load.attempts();
            c.converge(leader, &survivors).await;
            let v = check(&read_state(&c.client(leader)).await, &attempts, ACCOUNTS);
            println!(
                "kill {round}: node {old} killed, node {leader} leads after {:.0} ms; node {leader}: {}",
                ms(elected),
                v.summary()
            );
            assert!(
                v.missing_acknowledged.is_empty(),
                "acknowledged transfers missing on node {leader}: {:?}",
                v.missing_acknowledged
            );
            assert_eq!(v.violations_during_load(), Vec::<String>::new());
            c.start(old);
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let fin = load.stop().await;
        let after_last_kill = fin
            .attempts
            .iter()
            .filter(|a| a.result.is_ok() && a.started > last_kill)
            .count();
        println!("{after_last_kill} transfers acknowledged after the last kill");
        assert!(after_last_kill > 0);
        let verdicts = final_check(&c, &fin.attempts, &all, false).await;
        for (id, v) in &verdicts {
            println!("end, node {id}: {}", v.summary());
        }
        check_readers(&fin);

        // The reader check can fail: a reader that sums the balances in two
        // SELECTs sees totals other than the real one.
        let split = fin.split.unwrap();
        println!(
            "two-SELECT reader on node 1: {} reads, {} distinct totals: {:?}",
            split.reads(),
            split.totals.len(),
            first_totals(&split.totals)
        );
        assert!(
            split.totals.len() > 1,
            "the two-SELECT reader saw only one total"
        );
    });
}

/// Claim 3. The same load in async mode at round trips `PROOF_RTTS`
/// (default 0,10,100 ms), `PROOF_KILLS` leader kills each (default 4),
/// `PROOF_WRITERS` async writers per node (default 4).
/// After each kill, counts the acknowledged transfers the new leader lacks.
#[test]
#[ignore = "measurement, about 1 minute: cargo test --release -p tursoraft-proof --test failures async_ -- --ignored --nocapture"]
fn async_transfers_lost_per_leader_kill() {
    run(async {
        let kills = env_or("PROOF_KILLS", 4);
        let rtts: Vec<f64> = std::env::var("PROOF_RTTS")
            .unwrap_or_else(|_| "0,10,100".into())
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect();
        let all = [1, 2, 3];
        let mut rows = Vec::new();
        for rtt in rtts {
            let (mut c, mut leader) = start_bank(rtt, &all, &all, |_| {}).await;
            let w = env_or("PROOF_WRITERS", 4);
            let mut spec = LoadSpec::new(
                &format!("a{rtt}"),
                vec![(1, w), (2, w), (3, w)],
                ASYNC,
                ACCOUNTS,
            );
            spec.readers = all.to_vec();
            spec.timeout = Duration::from_secs(15);
            let load = Load::start(&c, &spec);
            let mut lost = BTreeSet::new();
            let window = Duration::from_secs_f64(rtt.max(1.0) / 1000.0);
            for round in 1..=kills {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let old = leader;
                c.kill(old);
                // Every attempt that finished by now was answered before the
                // process was gone.
                let killed_at = Instant::now();
                let attempts = load.attempts();
                let acked: Vec<&Attempt> = attempts
                    .iter()
                    .filter(|a| a.result.is_ok() && a.finished <= killed_at)
                    .collect();
                let in_window = acked
                    .iter()
                    .filter(|a| killed_at - a.finished <= window)
                    .count();
                let survivors = others(&all, old);
                leader = c.wait_leader(&survivors).await;
                c.converge(leader, &survivors).await;
                let state = read_state(&c.client(leader)).await;
                let missing: Vec<&&Attempt> = acked
                    .iter()
                    .filter(|a| !state.transfers.contains_key(&a.key) && !lost.contains(&a.key))
                    .collect();
                let oldest = missing.iter().map(|a| killed_at - a.finished).max();
                let at_old = missing.iter().filter(|a| a.node == old).count();
                let v = check(&state, &attempts, ACCOUNTS);
                assert!(v.wrong_balances.is_empty(), "{:?}", v.wrong_balances);
                assert_eq!(v.total, v.expected_total);
                rows.push(format!(
                    "| {rtt} | {round} | {old}→{leader} | {} | {in_window} | {} | {at_old} | {} |",
                    acked.len(),
                    missing.len(),
                    oldest.map_or("-".into(), |d| format!("{:.1}", ms(d)))
                ));
                println!("{}", rows.last().unwrap());
                lost.extend(missing.iter().map(|a| a.key.clone()));
                c.start(old);
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let fin = load.stop().await;
            let verdicts = final_check(&c, &fin.attempts, &all, true).await;
            // Nothing is lost except at the kills, and every node lost the
            // same transfers.
            for (id, v) in &verdicts {
                let missing: BTreeSet<String> = v.missing_acknowledged.iter().cloned().collect();
                assert_eq!(missing, lost, "node {id}");
            }
            let acked = fin.attempts.iter().filter(|a| a.result.is_ok()).count();
            println!(
                "rtt {rtt} ms: {acked} acknowledged, {} lost in {kills} kills",
                lost.len()
            );
            check_readers(&fin);
        }
        println!(
            "\n| rtt ms | kill | leader | acked before kill | acked in last max(rtt, 1 ms) | lost | lost from writers at the killed node | oldest lost ack, ms before kill |"
        );
        println!("|---:|---:|---|---:|---:|---:|---:|---:|");
        for r in rows {
            println!("{r}");
        }
    });
}

/// The phase 2 reading that a killed leader which restarts and wins the next
/// election keeps the async writes it flushed. At 100 ms round trip, async
/// writers at the leader; one follower is stopped and the leader killed, so
/// the other follower can only elect the restarted leader.
#[test]
fn a_killed_leader_that_wins_again_keeps_its_flushed_async_writes() {
    run(async {
        let all = [1, 2, 3];
        let (mut c, leader) = start_bank(100.0, &all, &all, |_| {}).await;
        let rest = others(&all, leader);
        let (b, stopped) = (rest[0], rest[1]);
        let load = Load::start(&c, &LoadSpec::new("w", vec![(leader, 16)], ASYNC, ACCOUNTS));
        tokio::time::sleep(Duration::from_millis(1500)).await;
        c.pause(stopped);
        c.kill(leader);
        let killed_at = Instant::now();
        let fin = load.stop().await;
        let acked: Vec<&Attempt> = fin
            .attempts
            .iter()
            .filter(|a| a.result.is_ok() && a.finished <= killed_at)
            .collect();
        // What the remaining follower had applied: the acknowledged writes
        // beyond it were at risk.
        let on_b = read_state(&c.client(b)).await;
        let at_risk = acked
            .iter()
            .filter(|a| !on_b.transfers.contains_key(&a.key))
            .count();
        c.start(leader);
        let winner = c.wait_leader(&[leader, b]).await;
        let elected = killed_at.elapsed();
        c.converge(winner, &[leader, b]).await;
        let on_b = read_state(&c.client(b)).await;
        let v = check(&on_b, &fin.attempts, ACCOUNTS);
        println!(
            "leader {leader} killed with node {stopped} stopped; node {winner} leads after {:.0} ms. \
             {} acknowledged, {at_risk} of them not applied on node {b} at the kill; after: {}",
            ms(elected),
            acked.len(),
            v.summary()
        );
        assert_eq!(winner, leader);
        assert!(at_risk > 0, "no acknowledged write was at risk");
        assert!(
            v.missing_acknowledged.is_empty(),
            "{:?}",
            v.missing_acknowledged
        );
        c.resume(stopped);
        final_check(&c, &fin.attempts, &all, false).await;
    });
}

/// Leadership handed over with `transfer_leader` `PROOF_ROUNDS` times
/// (default 6) while quorum and async writers on every node keep groups
/// pending. The old leader stays up, so no acknowledged write of either mode
/// may be lost.
#[test]
fn leadership_transfers_under_load_lose_nothing() {
    run(async {
        let all = [1, 2, 3];
        let (c, mut leader) = start_bank(10.0, &all, &all, |_| {}).await;
        let mut quorum = LoadSpec::new("tq", vec![(1, 4), (2, 4), (3, 4)], QUORUM, ACCOUNTS);
        quorum.readers = all.to_vec();
        quorum.timeout = Duration::from_secs(15);
        let mut asynchronous = LoadSpec::new("ta", vec![(1, 4), (2, 4), (3, 4)], ASYNC, ACCOUNTS);
        asynchronous.timeout = Duration::from_secs(15);
        let loads = [Load::start(&c, &quorum), Load::start(&c, &asynchronous)];
        let mut max_pending = 0;
        for round in 1..=env_or("PROOF_ROUNDS", 6) {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            max_pending =
                max_pending.max(c.client(leader).stats().await.unwrap().max_groups_pending);
            let to = all[(all.iter().position(|n| *n == leader).unwrap() + 1) % 3];
            let started = Instant::now();
            let res = c.client(leader).raft().transfer_leader(to).await;
            leader = c.wait_leader(&all).await;
            println!(
                "transfer {round} to node {to}: {res:?}, node {leader} leads after {:.0} ms",
                ms(started.elapsed())
            );
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut attempts = Vec::new();
        let mut reads = Finished::default();
        for load in loads {
            let fin = load.stop().await;
            for mode in ["tq", "ta"] {
                let mine: Vec<&Attempt> = fin
                    .attempts
                    .iter()
                    .filter(|a| a.key.starts_with(mode))
                    .collect();
                if !mine.is_empty() {
                    println!(
                        "{}: {} acknowledged, {} without effect, {} unknown",
                        if mode == "tq" { "quorum" } else { "async" },
                        mine.iter()
                            .filter(|a| a.told() == Told::Acknowledged)
                            .count(),
                        mine.iter().filter(|a| a.told() == Told::NoEffect).count(),
                        mine.iter().filter(|a| a.told() == Told::Unknown).count()
                    );
                }
            }
            attempts.extend(fin.attempts);
            reads.reads.extend(fin.reads);
        }
        println!("most groups pending at a leader: {max_pending}");
        let verdicts = final_check(&c, &attempts, &all, false).await;
        for (id, v) in &verdicts {
            println!("end, node {id}: {}", v.summary());
        }
        check_readers(&reads);
    });
}

/// Interactive transactions (`Client::begin`) under leader changes: 3
/// interactive quorum writers per node each read both balances, write the
/// new balances as values and commit, while leadership is handed over with
/// `transfer_leader` `PROOF_ROUNDS` times (default 3) and then the leader is
/// killed with SIGKILL and restarted `PROOF_KILLS` times (default 2). No
/// acknowledged transfer may be missing, no transfer may have committed on a
/// stale read (the balances would not add up), and the dumps must be equal.
#[test]
fn interactive_quorum_commits_survive_leader_handovers_and_kills() {
    run(async {
        let all = [1, 2, 3];
        let (mut c, mut leader) = start_bank(10.0, &all, &all, |_| {}).await;
        let mut spec = LoadSpec::new("i", vec![(1, 3), (2, 3), (3, 3)], QUORUM, ACCOUNTS);
        spec.interactive = true;
        spec.readers = all.to_vec();
        spec.timeout = Duration::from_secs(15);
        let load = Load::start(&c, &spec);
        let mut events = Vec::new();
        let handovers = env_or("PROOF_ROUNDS", 3);
        let kills = env_or("PROOF_KILLS", 2);
        for round in 1..=handovers + kills {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let old = leader;
            let incoming = c.client(old).stats().await.unwrap().incoming;
            let at = Instant::now();
            let what = if round <= handovers {
                let to = all[(all.iter().position(|n| *n == old).unwrap() + 1) % 3];
                let res = c.client(old).raft().transfer_leader(to).await;
                leader = c.wait_leader(&all).await;
                format!("transfer_leader to {to}: {res:?}")
            } else {
                c.kill(old);
                leader = c.wait_leader(&others(&all, old)).await;
                "SIGKILL".to_string()
            };
            let took = at.elapsed();
            let survivors: Vec<NodeId> = c.running();
            c.converge(leader, &survivors).await;
            let attempts = load.attempts();
            let v = check(&read_state(&c.client(leader)).await, &attempts, ACCOUNTS);
            let in_flight: Vec<&Attempt> = attempts
                .iter()
                .filter(|a| a.started < at && a.finished >= at)
                .collect();
            let line = format!(
                "event {round}: {what} on node {old} ({incoming} incoming change sets so far); node {leader} leads after {:.0} ms; \
                 {} transfers in flight at the event: {} acknowledged, {} without effect, {} unknown; node {leader}: {}",
                ms(took),
                in_flight.len(),
                in_flight.iter().filter(|a| a.told() == Told::Acknowledged).count(),
                in_flight.iter().filter(|a| a.told() == Told::NoEffect).count(),
                in_flight.iter().filter(|a| a.told() == Told::Unknown).count(),
                v.summary()
            );
            println!("{line}");
            events.push(line);
            assert!(
                v.missing_acknowledged.is_empty(),
                "acknowledged transfers missing on node {leader}: {:?}",
                v.missing_acknowledged
            );
            assert_eq!(v.violations_during_load(), Vec::<String>::new());
            if round > handovers {
                c.start(old);
            }
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let fin = load.stop().await;
        let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
        for a in &fin.attempts {
            if let Err(e) = &a.result {
                let kind = format!("{e:?}");
                *reasons
                    .entry(kind.split('(').next().unwrap().to_string())
                    .or_default() += 1;
            }
        }
        let last = fin.attempts.iter().map(|a| a.started).max().unwrap();
        let acked_late = fin
            .attempts
            .iter()
            .filter(|a| a.result.is_ok() && last - a.started < Duration::from_millis(1500))
            .count();
        println!("errors by kind: {reasons:?}; {acked_late} acknowledged in the last 1.5 s");
        assert!(
            acked_late > 0,
            "no interactive transfer acknowledged at the end"
        );
        let verdicts = final_check(&c, &fin.attempts, &all, false).await;
        for (id, v) in &verdicts {
            println!("end, node {id}: {}", v.summary());
        }
        check_readers(&fin);
    });
}

/// Claim 4, first half. The leader is stopped with SIGSTOP for 1.5 s (the
/// election timeout at 10 ms is 150..300 ms) and resumed, `PROOF_ROUNDS`
/// times (default 3), under quorum writers on every node.
#[test]
fn a_paused_leader_holds_nothing_extra_and_lost_no_quorum_write() {
    run(async {
        let report = pause_leader_rounds(10.0, true, env_or("PROOF_ROUNDS", 3)).await;
        for line in &report.lines {
            println!("{line}");
        }
    });
}

/// Claim 4, second half: what a resumed leader answers to a transaction that
/// changes no rows. It reads a transfer the new leader acknowledged while
/// the old one was stopped. A quorum transaction must not miss it; an async
/// one may (`Node::execute` says so), and the test counts how often it does.
#[test]
fn a_resumed_leader_does_not_answer_from_its_old_state() {
    run(async {
        let mut report = PauseReport::default();
        for (rtt, load) in [(10.0, true), (10.0, false), (100.0, false)] {
            let r = pause_leader_rounds(rtt, load, env_or("PROOF_ROUNDS", 3)).await;
            report.lines.extend(r.lines);
            report.probes.extend(r.probes);
        }
        for line in &report.lines {
            println!("{line}");
        }
        for mode in [QUORUM, ASYNC] {
            let mine: Vec<&Probe> = report
                .probes
                .iter()
                .filter(|p| p.mode == mode && !p.when.starts_with("control"))
                .collect();
            println!(
                "{mode:?}: {} probes at the resumed leader, {} answered, {} stale",
                mine.len(),
                mine.iter().filter(|p| p.answer.is_ok()).count(),
                mine.iter().filter(|p| p.stale()).count()
            );
        }
        let stale: Vec<&Probe> = report
            .probes
            .iter()
            .filter(|p| p.mode == QUORUM && p.stale())
            .collect();
        assert!(stale.is_empty(), "stale quorum answers: {stale:#?}");
    });
}

/// Claim 5. A change set captured on a separate database with older rows is
/// proposed into the log of a running cluster, in one entry with a change
/// set that is not stale: once on a quiet cluster, once under load.
#[test]
fn a_stale_change_set_is_rejected_alike_on_every_node() {
    run(async {
        let report = foreign_change_sets().await;
        for line in &report.lines {
            println!("{line}");
        }
        // Writes go on after each rejection, through every node.
        assert!(
            report.touch_acked,
            "no transfer of account 6 was acknowledged"
        );
    });
}

/// What the leader's own transactions see right after a change set entered
/// the log without passing through the leader loop (proposed through
/// `Client::raft()`), and whether writes are refused after it.
#[test]
#[ignore = "defect in tursoraft-node (pipelined tree): the leader's ahead file misses a change set that was proposed through Raft directly, so quorum transactions answer without it and writes to its rows are rejected until a reset"]
fn transactions_at_the_leader_see_a_change_set_proposed_through_raft() {
    run(async {
        let report = foreign_change_sets().await;
        for line in &report.lines {
            println!("{line}");
        }
        assert_eq!(report.stale_quorum_reads, 0, "stale quorum transactions");
        assert_eq!(report.refused_touches, 0, "refused transfers of account 6");
    });
}

#[derive(Default)]
struct ForeignReport {
    lines: Vec<String>,
    /// Quorum transactions at the leader, after the foreign change set was
    /// applied there, that did not see it.
    stale_quorum_reads: usize,
    /// Transfers of the account the foreign change set wrote that were
    /// refused before one was acknowledged.
    refused_touches: usize,
    touch_acked: bool,
}

/// The runs of claim 5; asserts that every node rejects the stale change sets
/// alike, also after a restart, and keeps everything else.
async fn foreign_change_sets() -> ForeignReport {
    let all = [1, 2, 3];
    let (mut c, leader) = start_bank(10.0, &all, &all, |_| {}).await;
    let mut report = ForeignReport::default();
    let mut attempts = Vec::new();

    // A separate database with the same schema at the same schema epoch,
    // all accounts at the initial balance.
    let dir = tempfile::TempDir::new().unwrap();
    let side = NodeDb::open(dir.path().join("side.db")).unwrap();
    let schema_set = side.capture(&to_changeset(&schema(ACCOUNTS))).unwrap();
    let outcomes = side
        .state_machine()
        .apply(&Entry {
            index: 1,
            raft_meta: Vec::new(),
            proposals: vec![schema_set.change_set],
        })
        .unwrap();
    assert_eq!(outcomes, vec![Outcome::Applied]);
    let capture =
        |statements: Vec<Statement>| side.capture(&to_changeset(&statements)).unwrap().change_set;

    // The cluster moves money from account 1 to 2; the side database
    // does not see it.
    let w = c
        .client(leader)
        .execute(transfer("live-1", 1, 2, 10), QUORUM)
        .await
        .unwrap();
    attempts.push(attempt("live-1", leader, Ok(w.index)));
    let stale = capture(transfer("stale-1", 1, 3, 7));
    let fresh = capture(transfer("fresh-1", 5, 6, 3));

    // A follower that is down while the entry commits applies it from
    // the log after its restart.
    let late = others(&all, leader)[0];
    c.kill(late);
    let before = c.client(leader).stats().await.unwrap();
    let proposed = c
        .client(leader)
        .raft()
        .propose_many(vec![stale, fresh])
        .await
        .unwrap();
    report.lines.push(format!("quiet: proposed {proposed:?}"));
    let Outcome::Rejected { reason } = &proposed[0].outcome else {
        panic!("the stale change set was applied: {proposed:?}")
    };
    assert!(reason.contains("write conflict on accounts"), "{reason}");
    assert_eq!(proposed[1].outcome, Outcome::Applied);
    assert_eq!(proposed[0].index, proposed[1].index);
    let index = proposed[0].index;
    attempts.push(attempt(
        "stale-1",
        leader,
        Err(Error::Rejected(reason.clone())),
    ));
    attempts.push(attempt("fresh-1", leader, Ok(index)));
    c.start(late);

    // What transactions at the leader see of fresh-1 (accounts 5 and 6 at
    // 997 and 1003) right after it was applied there.
    let see = vec![Statement::from(
        "SELECT (SELECT balance FROM accounts WHERE id = 6), \
         (SELECT count(*) FROM transfers WHERE key = 'fresh-1')",
    )];
    let fresh_seen = |rows: &[Vec<Value>]| rows[0] == [Value::Integer(1003), Value::Integer(1)];
    let local = c
        .client(leader)
        .query(see[0].clone(), ReadLevel::Local)
        .await
        .unwrap();
    report.lines.push(format!(
        "quiet: local read at the leader: {:?}",
        local.rows[0]
    ));
    for mode in [QUORUM, ASYNC] {
        let r = c.client(leader).execute(see.clone(), mode).await;
        let seen = r.as_ref().map(|w| fresh_seen(&w.results[0].rows));
        if mode == QUORUM && seen == Ok(false) {
            report.stale_quorum_reads += 1;
        }
        report.lines.push(format!(
            "quiet: {mode:?} transaction at the leader that changes no rows: {:?} -> {}",
            r.as_ref().map(|w| &w.results[0].rows[0]),
            match seen {
                Ok(true) => "sees fresh-1",
                Ok(false) => "DOES NOT see fresh-1",
                Err(_) => "failed",
            }
        ));
    }

    // Transfers out of account 6, which fresh-1 wrote, until one is
    // acknowledged.
    let started = Instant::now();
    for n in 0..20 {
        let key = format!("touch-6-{n}");
        let r = c
            .client(leader)
            .execute(transfer(&key, 6, 7, 1), QUORUM)
            .await
            .map(|w| w.index);
        report.lines.push(format!(
            "quiet: transfer {key} at {:.0} ms: {r:?}",
            ms(started.elapsed())
        ));
        let ok = r.is_ok();
        attempts.push(attempt(&key, leader, r));
        if ok {
            report.touch_acked = true;
            break;
        }
        report.refused_touches += 1;
    }
    let r = c.client(leader).execute(see.clone(), QUORUM).await;
    report.lines.push(format!(
        "quiet: then a quorum transaction at the leader reads {:?}",
        r.map(|w| w.results[0].rows[0].clone())
    ));

    // The cluster keeps accepting writes, on every node.
    for i in 0..30 {
        let node = all[i % 3];
        let key = format!("after-{i}");
        let w = c
            .client(node)
            .execute(transfer(&key, 1 + i as i64 % 4, 7, 2), QUORUM)
            .await
            .unwrap_or_else(|e| panic!("{key} on node {node}: {e}"));
        attempts.push(attempt(&key, node, Ok(w.index)));
    }
    let after = c.client(leader).stats().await.unwrap();
    report.lines.push(format!(
        "quiet: leader resets {}, rejected {}",
        after.resets - before.resets,
        after.rejected - before.rejected
    ));

    // Under load: writers on every node, and another stale change set with
    // a fresh one that inserts a transfer row only (src = dst).
    let mut spec = LoadSpec::new("fl", vec![(1, 3), (2, 3), (3, 3)], QUORUM, ACCOUNTS);
    spec.timeout = Duration::from_secs(15);
    let load = Load::start(&c, &spec);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let stale = capture(transfer("stale-2", 1, 3, 7));
    let fresh = capture(vec![Statement::from(
        "INSERT INTO transfers(key, src, dst, amount) VALUES ('fresh-2', 9, 9, 5)",
    )]);
    let before = c.client(leader).stats().await.unwrap();
    let proposed_at = Instant::now();
    let proposed = c
        .client(leader)
        .raft()
        .propose_many(vec![stale, fresh])
        .await
        .unwrap();
    report.lines.push(format!("load: proposed {proposed:?}"));
    assert!(matches!(proposed[0].outcome, Outcome::Rejected { .. }));
    assert_eq!(proposed[1].outcome, Outcome::Applied);
    let r = c
        .client(leader)
        .execute(
            vec![Statement::from(
                "SELECT count(*) FROM transfers WHERE key = 'fresh-2'",
            )],
            QUORUM,
        )
        .await
        .map(|w| w.results[0].rows[0].clone());
    report.lines.push(format!(
        "load: right after, a quorum transaction at the leader counts fresh-2: {r:?}"
    ));
    if r == Ok(vec![Value::Integer(0)]) {
        report.stale_quorum_reads += 1;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let fin = load.stop().await;
    let after = c.client(leader).stats().await.unwrap();
    type InWindow = fn(&Attempt, Instant) -> bool;
    let windows: [(&str, InWindow); 3] = [
        (
            "sent and answered in the 1 s before the proposal",
            |a, t| a.finished < t && a.started >= t - Duration::from_secs(1),
        ),
        ("in flight at the proposal", |a, t| {
            a.started < t && a.finished >= t
        }),
        ("sent in the 1 s after the proposal", |a, t| {
            a.started >= t && a.started < t + Duration::from_secs(1)
        }),
    ];
    for (name, inside) in windows {
        let window: Vec<&Attempt> = fin
            .attempts
            .iter()
            .filter(|a| inside(a, proposed_at))
            .collect();
        let count = |f: fn(&Attempt) -> bool| window.iter().filter(|a| f(a)).count();
        report.lines.push(format!(
            "load: transfers {name}: {} acknowledged, {} rejected, {} unavailable, {} unknown",
            count(|a| a.result.is_ok()),
            count(|a| matches!(a.result, Err(Error::Rejected(_)))),
            count(|a| matches!(a.result, Err(Error::Unavailable(_)))),
            count(|a| a.told() == Told::Unknown),
        ));
    }
    let first_ack = fin
        .attempts
        .iter()
        .filter(|a| a.result.is_ok() && a.started >= proposed_at)
        .map(|a| a.finished - proposed_at)
        .min();
    report.lines.push(format!(
        "load: first transfer sent after the proposal acknowledged after {:?} ms; leader resets {}, rejected {}",
        first_ack.map(ms),
        after.resets - before.resets,
        after.rejected - before.rejected
    ));
    attempts.extend(fin.attempts);
    attempts.push(attempt(
        "stale-2",
        leader,
        Err(Error::Rejected("stale".into())),
    ));
    attempts.push(attempt("fresh-2", leader, Ok(proposed[1].index)));

    let verdicts = final_check(&c, &attempts, &all, false).await;
    for id in all {
        let state = read_state(&c.client(id)).await;
        report.lines.push(format!(
            "node {id}: stale-1 {}, fresh-1 {}, stale-2 {}, fresh-2 {}; {}",
            present(&state.transfers.contains_key("stale-1")),
            present(&state.transfers.contains_key("fresh-1")),
            present(&state.transfers.contains_key("stale-2")),
            present(&state.transfers.contains_key("fresh-2")),
            verdicts[&id].summary()
        ));
        for key in ["stale-1", "stale-2"] {
            assert!(!state.transfers.contains_key(key), "node {id} has {key}");
        }
        for key in ["fresh-1", "fresh-2"] {
            assert!(state.transfers.contains_key(key), "node {id} lacks {key}");
        }
    }

    // A restart of the whole cluster replays nothing differently. (Only
    // the applied position moves: the new leader's blank entry.)
    let before = read_state(&c.client(1)).await;
    for id in all {
        c.kill(id);
    }
    c.start_all();
    let leader = c.wait_leader(&all).await;
    c.converge(leader, &all).await;
    let reference = c.dump(leader).await;
    for id in all {
        assert_eq!(
            read_state(&c.client(id)).await,
            before,
            "node {id} after a full restart"
        );
        assert!(
            c.dump(id).await == reference,
            "node {id} differs after a full restart"
        );
    }
    let w = c
        .client(leader)
        .execute(transfer("after-restart", 2, 9, 1), QUORUM)
        .await
        .unwrap();
    report.lines.push(format!(
        "after a full restart: all nodes equal, written at index {}",
        w.index
    ));
    report
}

/// Claim 6. A voter that does not lead and a non-voting follower are killed
/// together, stay down while quorum writers on all four nodes go on, and are
/// restarted. The second, longer outage outlasts the log the leader keeps,
/// so the nodes catch up from a snapshot.
#[test]
fn a_killed_voter_and_non_voter_catch_up_and_lose_nothing() {
    run(async {
        let all = [1, 2, 3, 4];
        let voters = [1, 2, 3];
        let (mut c, _) = start_bank(10.0, &all, &voters, |cfg| {
            cfg.log = LogConfig {
                snapshot_every: 200,
                keep_after_snapshot: 50,
                ..LogConfig::default()
            }
        })
        .await;
        let mut spec = LoadSpec::new("f", all.iter().map(|n| (*n, 3)).collect(), QUORUM, ACCOUNTS);
        spec.readers = all.to_vec();
        spec.timeout = Duration::from_secs(15);
        let load = Load::start(&c, &spec);
        let mut stale_seen = 0;
        for (round, down) in [(1, 2), (2, 8)] {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let leader = c.wait_leader(&voters).await;
            let voter = others(&voters, leader)[0];
            let victims = [voter, 4];
            for id in victims {
                c.kill(id);
            }
            tokio::time::sleep(Duration::from_secs(down)).await;
            for id in victims {
                let applied_before = c.client(leader).stats().await.unwrap().applied_index;
                c.start(id);
                let started = Instant::now();
                let target = c.status(leader).await.last_log_index.unwrap();
                let attempts = load.attempts();
                let client = c.client(id);
                // Right after the restart the node lacks what was written
                // while it was down: the check notices a node behind.
                let early = check(&read_state(&client).await, &attempts, ACCOUNTS);
                let early_missing = early.missing_acknowledged.len();
                stale_seen += (early_missing > 0) as usize;
                let early_applied = c.status(id).await.applied.unwrap_or(0);
                client
                    .raft()
                    .wait_applied(target, Duration::from_secs(60))
                    .await
                    .unwrap();
                let took = started.elapsed();
                let stats = client.stats().await.unwrap();
                let v = check(&read_state(&client).await, &attempts, ACCOUNTS);
                println!(
                    "round {round}, node {id} down {down} s: right after restart it had applied {early_applied} \
                     (leader {applied_before}) and lacked {early_missing} acknowledged transfers; \
                     caught up to index {target} in {:.0} ms with {} entries applied and {} snapshots installed; then {}",
                    ms(took),
                    stats.applied_entries,
                    stats.snapshots_installed,
                    v.summary()
                );
                assert!(
                    v.missing_acknowledged.is_empty(),
                    "{:?}",
                    v.missing_acknowledged
                );
                assert_eq!(v.violations_during_load(), Vec::<String>::new());
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let fin = load.stop().await;
        let verdicts = final_check(&c, &fin.attempts, &voters, false).await;
        for (id, v) in &verdicts {
            println!("end, node {id}: {}", v.summary());
        }
        check_readers(&fin);
        assert!(stale_seen > 0, "no restarted node was ever seen behind");
    });
}

/// Runs the pause rounds of claim 4 and checks its first half.
/// With `writers` false, nothing but the markers is written, so the leader
/// has no entry in flight when it is stopped.
async fn pause_leader_rounds(rtt_ms: f64, writers: bool, rounds: usize) -> PauseReport {
    let all = [1, 2, 3];
    let (c, mut leader) = start_bank(rtt_ms, &all, &all, |_| {}).await;
    // Longer than the largest election timeout, 6 heartbeats of 2 RTT.
    let pause = Duration::from_millis(1500).max(Duration::from_secs_f64(rtt_ms * 15.0 / 1000.0));
    let writers = if writers {
        vec![(1, 3), (2, 3), (3, 3)]
    } else {
        Vec::new()
    };
    let mut spec = LoadSpec::new("p", writers.clone(), QUORUM, ACCOUNTS);
    spec.readers = all.to_vec();
    spec.timeout = Duration::from_secs(15);
    let load = Load::start(&c, &spec);
    let mut report = PauseReport::default();
    let mut markers = Vec::new();
    for round in 1..=rounds {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let old = leader;
        let survivors = others(&all, old);
        // Probe clients with an open connection, so that a probe waits in
        // the socket next to the new leader's messages.
        let probe_clients: Vec<_> = (0..2 * PROBES).map(|_| c.client(old)).collect();
        for client in &probe_clients {
            client.stats().await.unwrap();
        }
        c.pause(old);
        let paused_at = Instant::now();
        let new = c.wait_leader(&survivors).await;
        let elected = paused_at.elapsed();
        tokio::time::sleep_until((paused_at + pause).into()).await;

        // A transfer the new leader acknowledges while the old one is stopped.
        let key = format!("marker-{round}");
        let w = c
            .client(new)
            .execute(transfer(&key, 1, 2, 1), QUORUM)
            .await
            .unwrap();
        markers.push(attempt(&key, new, Ok(w.index)));
        let probe = vec![Statement::new(
            "SELECT count(*) FROM transfers WHERE key = ?",
            vec![Value::Text(key.clone())],
        )];
        // Probes sent while the old leader is stopped, so they wait in the
        // socket, half in each mode; one in each mode sent right after
        // SIGCONT.
        let early: Vec<_> = probe_clients
            .into_iter()
            .enumerate()
            .map(|(i, client)| {
                let probe = probe.clone();
                let mode = if i % 2 == 0 { QUORUM } else { ASYNC };
                tokio::spawn(async move { (mode, client.execute(probe, mode).await) })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stopped_for = paused_at.elapsed();
        c.resume(old);
        let (late_client_q, late_client_a) = (c.client(old), c.client(old));
        let (late_q, late_a) = tokio::join!(
            late_client_q.execute(probe.clone(), QUORUM),
            late_client_a.execute(probe.clone(), ASYNC)
        );
        let mut answers = Vec::new();
        for e in early {
            let (mode, answer) = e.await.unwrap();
            answers.push(("sent while stopped", mode, answer));
        }
        answers.push(("sent after SIGCONT", QUORUM, late_q));
        answers.push(("sent after SIGCONT", ASYNC, late_a));
        // What the old leader's file holds right after it resumed, against
        // what the new leader holds after that.
        let old_state = read_state(&c.client(old)).await;
        let new_state = read_state(&c.client(new)).await;
        let extra: Vec<&String> = old_state
            .transfers
            .keys()
            .filter(|k| !new_state.transfers.contains_key(*k))
            .collect();
        answers.push((
            "control at the new leader",
            QUORUM,
            c.client(new).execute(probe, QUORUM).await,
        ));
        leader = c.wait_leader(&all).await;
        report.lines.push(format!(
            "rtt {rtt_ms} ms, {} writers, round {round}: node {old} stopped for {:.0} ms, node {new} leads after {:.0} ms; marker at index {}; \
             old leader right after resume: {} transfers, {} the new leader lacks",
            writers.iter().map(|w| w.1).sum::<usize>(),
            ms(stopped_for),
            ms(elected),
            w.index,
            old_state.transfers.len(),
            extra.len()
        ));
        assert!(extra.is_empty(), "the resumed leader holds {extra:?}");
        for (when, mode, result) in answers {
            let probe = Probe {
                round,
                when,
                mode,
                node: if when.starts_with("control") {
                    new
                } else {
                    old
                },
                marker_index: w.index,
                answer: result.map(|w| {
                    let Value::Integer(n) = w.results[0].rows[0][0] else {
                        panic!("{:?}", w.results)
                    };
                    (w.index, n)
                }),
            };
            report.lines.push(format!("  {}", probe.describe()));
            report.probes.push(probe);
        }
    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let fin = load.stop().await;
    let mut attempts = fin.attempts.clone();
    attempts.extend(markers);
    let verdicts = final_check(&c, &attempts, &all, false).await;
    for (id, v) in &verdicts {
        report
            .lines
            .push(format!("end, node {id}: {}", v.summary()));
    }
    check_readers(&fin);
    report
}

/// Probes sent to a stopped leader in each round, per mode.
const PROBES: usize = 4;

#[derive(Default)]
struct PauseReport {
    lines: Vec<String>,
    probes: Vec<Probe>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Probe {
    round: usize,
    when: &'static str,
    mode: CommitMode,
    node: NodeId,
    marker_index: u64,
    /// The index of the answer and the marker count it read.
    answer: Result<(u64, i64), Error>,
}

impl Probe {
    /// Acknowledged, yet it did not see a transfer acknowledged before it.
    fn stale(&self) -> bool {
        matches!(self.answer, Ok((_, 0)))
    }

    fn describe(&self) -> String {
        let answer = match &self.answer {
            Ok((index, n)) => format!(
                "answered at index {index}, marker count {n}{}",
                if *n == 0 { " (STALE)" } else { "" }
            ),
            Err(e) => format!("failed: {e}"),
        };
        format!(
            "{:?} probe {} on node {}: {answer}",
            self.mode, self.when, self.node
        )
    }
}

/// Starts nodes `ids`, makes `voters` the voters and the rest non-voting
/// followers, creates the bank, and waits until every node has it.
async fn start_bank(
    rtt_ms: f64,
    ids: &[NodeId],
    voters: &[NodeId],
    change: impl FnOnce(&mut ClusterConfig),
) -> (Cluster, NodeId) {
    let mut c = Cluster::new(BIN, ids, |cfg| {
        cfg.delays = DelayTable::uniform(rtt_ms);
        change(cfg);
    });
    println!("cluster files in {}", c.dir.path().display());
    c.start_all();
    let leader = c.initialize(voters).await;
    for id in ids.iter().filter(|id| !voters.contains(id)) {
        c.client(leader)
            .raft()
            .add_follower(*id, c.addr(*id))
            .await
            .unwrap();
    }
    c.client(leader)
        .execute(schema(ACCOUNTS), QUORUM)
        .await
        .unwrap();
    c.converge(leader, ids).await;
    (c, leader)
}

/// Waits until every node has applied everything, then checks each node
/// against `attempts` and compares full dumps. Returns each node's verdict.
async fn final_check(
    c: &Cluster,
    attempts: &[Attempt],
    voters: &[NodeId],
    allow_missing: bool,
) -> BTreeMap<NodeId, Verdict> {
    let leader = c.wait_leader(voters).await;
    let ids = c.ids();
    c.converge(leader, &ids).await;
    let reference = c.dump(leader).await;
    let mut verdicts = BTreeMap::new();
    for id in ids {
        let v = check(&read_state(&c.client(id)).await, attempts, ACCOUNTS);
        assert_eq!(v.violations(), Vec::<String>::new(), "node {id}");
        if !allow_missing {
            assert!(
                v.missing_acknowledged.is_empty(),
                "node {id} lacks acknowledged transfers {:?}",
                v.missing_acknowledged
            );
        }
        assert!(
            c.dump(id).await == reference,
            "node {id} differs from leader {leader}"
        );
        verdicts.insert(id, v);
    }
    let unknown = attempts
        .iter()
        .filter(|a| a.told() == Told::Unknown)
        .count();
    let acked = attempts
        .iter()
        .filter(|a| a.told() == Told::Acknowledged)
        .count();
    println!("dumps of all nodes equal; {acked} acknowledged, {unknown} with unknown outcome");
    verdicts
}

/// Claim 2: every one-SELECT reader saw only the real total.
fn check_readers(fin: &Finished) {
    let expected = ACCOUNTS * INITIAL_BALANCE;
    for (node, tally) in &fin.reads {
        println!(
            "reader on node {node}: {} reads, totals {:?}, {} failed reads",
            tally.reads(),
            tally.totals,
            tally.errors
        );
        assert!(tally.reads() > 0, "the reader on node {node} read nothing");
        assert_eq!(
            tally.totals.keys().copied().collect::<Vec<_>>(),
            vec![expected],
            "the reader on node {node} saw another total"
        );
    }
}

fn first_totals(totals: &BTreeMap<i64, u64>) -> Vec<(i64, u64)> {
    let mut v: Vec<(i64, u64)> = totals.iter().map(|(t, n)| (*t, *n)).collect();
    v.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    v.truncate(5);
    v
}

fn others(ids: &[NodeId], not: NodeId) -> Vec<NodeId> {
    ids.iter().copied().filter(|n| *n != not).collect()
}

fn attempt(key: &str, node: NodeId, result: Result<u64, Error>) -> Attempt {
    let now = Instant::now();
    Attempt {
        key: key.to_string(),
        node,
        started: now,
        finished: now,
        result,
    }
}

fn present(yes: &bool) -> &'static str {
    if *yes {
        "present"
    } else {
        "absent"
    }
}

fn to_changeset(statements: &[Statement]) -> Vec<tursoraft_changeset::Statement> {
    statements
        .iter()
        .map(|s| {
            let params = s
                .params
                .iter()
                .map(|v| match v {
                    Value::Integer(i) => turso_core::Value::from_i64(*i),
                    Value::Text(t) => turso_core::Value::build_text(t.clone()),
                    v => panic!("not used here: {v:?}"),
                })
                .collect();
            tursoraft_changeset::Statement::new(s.sql.clone(), params)
        })
        .collect()
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
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
