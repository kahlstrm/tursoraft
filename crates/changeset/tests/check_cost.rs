//! What the write-conflict check costs at apply. Ignored by default; run in
//! a release build:
//! `cargo test --release -p tursoraft-changeset --test check_cost -- --ignored --nocapture`

mod common;

use std::time::{Duration, Instant};

use common::*;
use tempfile::TempDir;
use tursoraft_statemachine::StateMachine;

const BANK_ENTRIES: u64 = 500;
const BULK_ENTRIES: u64 = 30;

/// Applies `entries` entries of one change set each, captured on the same
/// node just before, and returns the total and the median time of `apply`.
fn time_apply(
    check: bool,
    setup: &[&str],
    entries: u64,
    txn: impl Fn(u64) -> Vec<String>,
) -> (Duration, Duration) {
    let dir = TempDir::new().unwrap();
    let mut n = node(&dir, "a.db");
    n.sm.set_conflict_check(check);
    let p = capture(&n, setup);
    n.sm.apply(&entry(1, vec![p])).unwrap();
    let mut times = Vec::new();
    for k in 0..entries {
        let sql = txn(k);
        let statements: Vec<&str> = sql.iter().map(String::as_str).collect();
        let p = capture(&n, &statements);
        let e = entry(k + 2, vec![p]);
        let start = Instant::now();
        assert_eq!(n.sm.apply(&e).unwrap(), applied_ok(1));
        times.push(start.elapsed());
    }
    let total = times.iter().sum();
    times.sort();
    (total, times[times.len() / 2])
}

#[test]
#[ignore]
fn conflict_check_cost() {
    let bank_setup = [
        "CREATE TABLE accounts(id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)",
        "CREATE TABLE events(id INTEGER PRIMARY KEY, at TEXT, kind TEXT, body TEXT)",
        "INSERT INTO accounts SELECT value, 'owner ' || value, 1000 FROM generate_series(1, 1000)",
    ];
    let transfer = |k: u64| {
        let (from, to) = (k % 1000 + 1, (k * 7 + 3) % 1000 + 1);
        vec![
            format!("UPDATE accounts SET balance = balance - 10 WHERE id = {from}"),
            format!("UPDATE accounts SET balance = balance + 10 WHERE id = {to}"),
            format!("INSERT INTO events(at, kind, body) VALUES ('2026-11-11', 'transfer', '{from} -> {to}: 10')"),
        ]
    };
    let bulk_setup = ["CREATE TABLE events(id INTEGER PRIMARY KEY, at TEXT, kind TEXT, body TEXT)"];
    let bulk = |_: u64| {
        vec![
            "INSERT INTO events(at, kind, body) SELECT '2026-11-11', 'bulk', 'row ' || value \
              FROM generate_series(1, 1000)"
                .to_string(),
        ]
    };
    eprintln!("| transaction | entries | check | total apply | median apply per entry |\n|---|---|---|---|---|");
    for check in [false, true] {
        let (total, median) = time_apply(check, &bank_setup, BANK_ENTRIES, transfer);
        eprintln!("| bank transfer (2 updates + 1 insert) | {BANK_ENTRIES} | {check} | {total:?} | {median:?} |");
    }
    for check in [false, true] {
        let (total, median) = time_apply(check, &bulk_setup, BULK_ENTRIES, bulk);
        eprintln!("| insert 1000 rows | {BULK_ENTRIES} | {check} | {total:?} | {median:?} |");
    }
}
