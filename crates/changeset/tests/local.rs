//! Local transactions ([`NodeDb::begin`]): statements run one at a time on
//! a node's own file, and commit returns a change set that the commit-time
//! check decides like any other. Also the leader's check of change sets from
//! other nodes on its ahead file ([`AheadDb::apply_incoming`]).

mod common;

use std::time::{Duration, Instant};

use common::*;
use tempfile::TempDir;
use tursoraft_changeset::{turso_core::Value, AheadDb, ChangeSet, Error, Statement};
use tursoraft_statemachine::{Outcome, StateMachine};

const APPLIED: Outcome = Outcome::Applied;
const WAIT: Duration = Duration::from_secs(5);

fn stmt(sql: &str) -> Statement {
    sql.into()
}

/// Nodes A and B, both with the setup entry applied at index 1.
fn two_nodes(dir: &TempDir, setup: &[&str]) -> (Node, Node) {
    let mut a = node(dir, "a.db");
    let mut b = node(dir, "b.db");
    let cs = capture(&a, setup);
    apply_all(&mut [&mut a, &mut b], &entry(1, vec![cs]), &[APPLIED]);
    (a, b)
}

/// Runs `sqls` as one local transaction on `n` and returns its change set.
fn local(n: &Node, sqls: &[&str]) -> Vec<u8> {
    let mut tx = n.db.begin(WAIT).unwrap();
    for sql in sqls {
        tx.execute(&stmt(sql)).unwrap();
    }
    tx.commit().unwrap()
}

#[test]
fn statements_see_each_other_and_commit_leaves_the_file_unchanged() {
    let dir = TempDir::new().unwrap();
    let (mut a, mut b) = two_nodes(
        &dir,
        &[
            "CREATE TABLE acct(id INTEGER PRIMARY KEY, balance INTEGER)",
            "INSERT INTO acct VALUES (1, 100), (2, 0)",
        ],
    );
    let before = dump(&a.db.connect().unwrap());
    let mut tx = a.db.begin(WAIT).unwrap();
    assert_eq!(tx.base_index(), 1);
    let read = tx
        .execute(&stmt("SELECT balance FROM acct WHERE id = 1"))
        .unwrap();
    assert_eq!(read.rows, vec![vec![Value::from_i64(100)]]);
    // The application computes, then writes.
    tx.execute(&stmt("UPDATE acct SET balance = 70 WHERE id = 1"))
        .unwrap();
    tx.execute(&stmt("UPDATE acct SET balance = balance + 30 WHERE id = 2"))
        .unwrap();
    // A failing statement is undone alone; the transaction goes on.
    let failed = tx.execute(&stmt("INSERT INTO acct VALUES (1, 5)"));
    assert!(failed.unwrap_err().to_string().contains("UNIQUE"));
    let refused = tx.execute(&stmt("COMMIT"));
    assert!(matches!(refused, Err(Error::NotAllowed { .. })));
    let sum = tx.execute(&stmt("SELECT sum(balance) FROM acct")).unwrap();
    assert_eq!(sum.rows, vec![vec![Value::from_i64(100)]]);
    // Other connections do not see the open transaction.
    assert_eq!(dump(&a.db.connect().unwrap()), before);
    let cs = tx.commit().unwrap();
    assert_eq!(
        dump(&a.db.connect().unwrap()),
        before,
        "commit left no trace"
    );

    let set = ChangeSet::decode(&cs).unwrap();
    assert_eq!(set.base_index, 1);
    assert!(!set.reads_pending);
    assert_eq!(set.changes.len(), 2);
    apply_all(&mut [&mut a, &mut b], &entry(2, vec![cs]), &[APPLIED]);
    assert_eq!(
        query(
            &b.db.connect().unwrap(),
            "SELECT balance FROM acct ORDER BY id"
        ),
        vec![vec![Value::from_i64(70)], vec![Value::from_i64(30)]]
    );
    assert_same(&a, &b);
}

/// Two nodes run local transactions at the same time: the commit-time check
/// gives one commit and one rejection when they change the same row or
/// violate a unique constraint together, and two commits on different rows.
/// Both nodes reach the same outcomes.
#[test]
fn local_transactions_on_two_nodes_conflict_by_rows() {
    let dir = TempDir::new().unwrap();
    let (mut a, mut b) = two_nodes(
        &dir,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE TABLE u(id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        ],
    );
    let same_a = local(&a, &["UPDATE t SET v = v || '+a' WHERE id = 1"]);
    let same_b = local(&b, &["UPDATE t SET v = v || '+b' WHERE id = 1"]);
    let row1 = local(&a, &["UPDATE t SET v = 'x' WHERE id = 1"]);
    let row2 = local(&b, &["UPDATE t SET v = 'y' WHERE id = 2"]);
    let email_a = local(&a, &["INSERT INTO u VALUES (10, 'e@x')"]);
    let email_b = local(&b, &["INSERT INTO u VALUES (20, 'e@x')"]);

    let o =
        a.sm.apply(&entry(2, vec![same_a.clone(), same_b.clone()]))
            .unwrap();
    assert_eq!(b.sm.apply(&entry(2, vec![same_a, same_b])).unwrap(), o);
    assert_eq!(o[0], APPLIED);
    assert!(reason(&o[1]).starts_with("write conflict on t rowid 1"));

    let fresh_a = local(&a, &["UPDATE t SET v = 'x' WHERE id = 1"]);
    // `row1` was captured before entry 2 changed row 1; `fresh_a` after.
    let o = vec![row1, row2, email_a, email_b, fresh_a];
    let oa = a.sm.apply(&entry(3, o.clone())).unwrap();
    assert_eq!(b.sm.apply(&entry(3, o)).unwrap(), oa);
    eprintln!("--- entry 3: {oa:?}");
    assert!(reason(&oa[0]).starts_with("write conflict on t rowid 1"));
    assert_eq!(oa[1], APPLIED);
    assert_eq!(oa[2], APPLIED);
    assert!(reason(&oa[3]).contains("UNIQUE"), "{:?}", oa[3]);
    assert_eq!(oa[4], APPLIED);
    assert_same(&a, &b);
}

/// LIMIT (shown, not solved): two nodes that insert without choosing the
/// key get the same rowid, so one insert is rejected although the rows are
/// different. Applications should choose their keys.
#[test]
fn limit_inserts_that_let_the_database_pick_the_key_collide() {
    let dir = TempDir::new().unwrap();
    let (mut a, mut b) = two_nodes(
        &dir,
        &["CREATE TABLE events(id INTEGER PRIMARY KEY, what TEXT)"],
    );
    let on_a = local(&a, &["INSERT INTO events(what) VALUES ('from a')"]);
    let on_b = local(&b, &["INSERT INTO events(what) VALUES ('from b')"]);
    let o =
        a.sm.apply(&entry(2, vec![on_a.clone(), on_b.clone()]))
            .unwrap();
    assert_eq!(b.sm.apply(&entry(2, vec![on_a, on_b])).unwrap(), o);
    eprintln!("--- {o:?}");
    assert_eq!(o[0], APPLIED);
    assert!(reason(&o[1]).starts_with("write conflict on events rowid 1"));
    assert_same(&a, &b);
}

/// An apply waits while a local transaction holds the write lock, and ends
/// the transaction once it has waited for the hold limit. The transaction
/// then fails with `Aborted`, and the next one reads the applied entry.
#[test]
fn an_apply_ends_a_local_transaction_after_the_hold_limit() {
    let dir = TempDir::new().unwrap();
    let (a, mut b) = two_nodes(&dir, &["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"]);
    let incoming = capture(&b, &["INSERT INTO t VALUES (1, 'from b')"]);
    a.db.set_local_hold_limit(Duration::from_millis(300));

    let mut tx = a.db.begin(WAIT).unwrap();
    tx.execute(&stmt("INSERT INTO t VALUES (2, 'local')"))
        .unwrap();
    let started = Instant::now();
    let mut sm = a.db.state_machine();
    let applier = std::thread::spawn(move || {
        let outcomes = sm.apply(&entry(2, vec![incoming])).unwrap();
        (outcomes, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));
    // Still open while the apply waits.
    tx.execute(&stmt("SELECT 1")).unwrap();
    let (outcomes, waited) = applier.join().unwrap();
    eprintln!("--- the apply waited {waited:?}");
    assert_eq!(outcomes, [APPLIED]);
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    assert!(waited < Duration::from_secs(2), "{waited:?}");
    let ended = tx.execute(&stmt("SELECT 1"));
    assert!(
        matches!(&ended, Err(Error::Aborted(r)) if r.contains("300 ms")),
        "{ended:?}"
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted(_))));

    let mut next = a.db.begin(WAIT).unwrap();
    assert_eq!(next.base_index(), 2);
    let rows = next.execute(&stmt("SELECT v FROM t")).unwrap().rows;
    assert_eq!(rows, vec![vec![Value::build_text("from b")]]);
    drop(next);
    b.sm.apply(&entry(
        2,
        vec![capture(&b, &["INSERT INTO t VALUES (1, 'from b')"])],
    ))
    .unwrap();
    assert_same(&a, &b);
}

/// Two local transactions on one node run one after the other: the second
/// begins when the first ends, or ends the first after the hold limit.
#[test]
fn local_transactions_on_one_node_run_one_at_a_time() {
    let dir = TempDir::new().unwrap();
    let (a, _b) = two_nodes(&dir, &["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"]);
    a.db.set_local_hold_limit(Duration::from_millis(200));
    let mut first = a.db.begin(WAIT).unwrap();
    first
        .execute(&stmt("INSERT INTO t VALUES (1, 'first')"))
        .unwrap();
    let busy = a.db.begin(Duration::from_millis(50));
    assert!(matches!(busy, Err(Error::Busy(_))), "{:?}", busy.err());
    let db = a.db.clone();
    let started = Instant::now();
    let second = std::thread::spawn(move || {
        let mut tx = db.begin(WAIT).unwrap();
        let waited = started.elapsed();
        tx.execute(&stmt("INSERT INTO t VALUES (1, 'second')"))
            .unwrap();
        (tx.commit().unwrap(), waited)
    });
    let (cs, waited) = second.join().unwrap();
    eprintln!("--- the second transaction waited {waited:?}");
    assert!(waited >= Duration::from_millis(200));
    assert!(matches!(first.commit(), Err(Error::Aborted(_))));
    assert_eq!(ChangeSet::decode(&cs).unwrap().changes.len(), 1);
}

/// `INSERT OR ROLLBACK` ends the whole local transaction.
#[test]
fn a_failure_that_ends_the_local_transaction() {
    let dir = TempDir::new().unwrap();
    let (a, _b) = two_nodes(
        &dir,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
    );
    let mut tx = a.db.begin(WAIT).unwrap();
    tx.execute(&stmt("INSERT INTO t VALUES (2, 'b')")).unwrap();
    let r = tx.execute(&stmt("INSERT OR ROLLBACK INTO t VALUES (3, 'a')"));
    assert!(
        matches!(&r, Err(Error::Aborted(m)) if m.contains("UNIQUE")),
        "{r:?}"
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted(_))));
    // The lock is free again.
    let tx = a.db.begin(Duration::from_millis(10)).unwrap();
    let cs = ChangeSet::decode(&tx.commit().unwrap()).unwrap();
    assert!(cs.changes.is_empty());
}

/// The leader checks a change set from another node on its ahead file
/// before proposing it. One that passes is kept there, so later captures
/// see it. It read only committed state, so it does not set
/// `reads_pending`: when a pending group before it is rejected at apply, it
/// still applies if its rows are as it read them. One that conflicts with a
/// pending group is rejected on the ahead file, although at apply it might
/// have passed: pessimistic, never wrong.
#[test]
fn incoming_change_sets_are_checked_on_the_ahead_file() {
    let dir = TempDir::new().unwrap();
    let (mut a, mut b) = two_nodes(
        &dir,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        ],
    );
    let ahead = AheadDb::open(dir.path().join("ahead.db")).unwrap();
    assert_eq!(ahead.reset(&a.db).unwrap(), 1);
    // A change set that will win against the leader's pending group.
    let foreign = local(&b, &["UPDATE t SET v = 'foreign' WHERE id = 1"]);
    let group = ahead
        .capture_batch(&[vec![stmt("UPDATE t SET v = v || '+g' WHERE id = 1")]])
        .unwrap()
        .remove(0)
        .unwrap()
        .change_set;
    // From node B, at base 1.
    let other_row = local(&b, &["UPDATE t SET v = 'b2' WHERE id = 2"]);
    let same_row = local(&b, &["UPDATE t SET v = 'a2' WHERE id = 1"]);
    let outcomes = ahead
        .apply_incoming(&[other_row.clone(), same_row])
        .unwrap();
    eprintln!("--- on the ahead file: {outcomes:?}");
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).starts_with("write conflict on t rowid 1"));
    // Later captures on the ahead file see the incoming change.
    let seen = ahead
        .capture_batch(&[vec![stmt("SELECT v FROM t WHERE id = 2")]])
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(seen.results[0].rows, vec![vec![Value::build_text("b2")]]);
    assert!(!ChangeSet::decode(&other_row).unwrap().reads_pending);

    apply_all(&mut [&mut a, &mut b], &entry(2, vec![foreign]), &[APPLIED]);
    let e3 = entry(3, vec![group, other_row]);
    let oa = a.sm.apply(&e3).unwrap();
    assert_eq!(b.sm.apply(&e3).unwrap(), oa);
    eprintln!("--- entry 3: {oa:?}");
    assert!(reason(&oa[0]).starts_with("write conflict on t rowid 1"));
    assert_eq!(oa[1], APPLIED, "it read no pending change set");
    assert_eq!(
        query(&a.db.connect().unwrap(), "SELECT v FROM t ORDER BY id"),
        vec![
            vec![Value::build_text("foreign")],
            vec![Value::build_text("b2")]
        ]
    );
    assert_same(&a, &b);
}
