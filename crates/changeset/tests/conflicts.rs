//! The write-conflict check in `apply`: a change set that another change set,
//! applied after its base, conflicts with is rejected; change sets that wrote
//! different rows both apply. Node A captures, A and B apply, and both must
//! reach the same outcomes.
//! Run with `cargo test -p tursoraft-changeset --test conflicts -- --nocapture`
//! to see the outcomes.

mod common;

use common::*;
use tempfile::TempDir;
use tursoraft_changeset::{turso_core::Value, AheadDb, ChangeKind, ChangeSet, Error, Statement};
use tursoraft_statemachine::{Outcome, StateMachine};

const APPLIED: Outcome = Outcome::Applied;

fn values(rows: Vec<Vec<Value>>) -> String {
    format!("{rows:?}")
}

fn txt(s: &str) -> Value {
    Value::build_text(s.to_string())
}

fn i(n: i64) -> Value {
    Value::from_i64(n)
}

#[test]
fn updates_of_one_row_conflict_and_of_different_rows_do_not() {
    let mut p = Pair::new(&[
        "CREATE TABLE acct(id INTEGER PRIMARY KEY, balance INTEGER)",
        "INSERT INTO acct VALUES (1, 100), (2, 100)",
    ]);
    let first = p.capture(&["UPDATE acct SET balance = balance - 10 WHERE id = 1"]);
    let second = p.capture(&["UPDATE acct SET balance = balance - 20 WHERE id = 1"]);
    let other_row = p.capture(&["UPDATE acct SET balance = balance + 5 WHERE id = 2"]);
    let next_entry = p.capture(&["UPDATE acct SET balance = 0 WHERE id = 1"]);

    let outcomes = p.apply(vec![first, second, other_row]);
    assert_eq!(outcomes[0], APPLIED);
    assert_eq!(
        reason(&outcomes[1]),
        "write conflict on acct rowid 1: another transaction changed the row after this one read it"
    );
    assert_eq!(outcomes[2], APPLIED);
    // Captured at the same base, applied one entry later: still a conflict.
    assert!(reason(&p.apply(vec![next_entry])[0]).contains("acct rowid 1"));
    assert_eq!(p.rows("SELECT * FROM acct"), ints(&[&[1, 90], &[2, 105]]));
    p.assert_same();
}

/// A row changed from integer 2 to real 2.0 has changed, although SQL
/// compares the two as equal.
#[test]
fn a_change_of_type_alone_is_a_conflict() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
        "INSERT INTO t VALUES (1, 2)",
    ]);
    let to_real = p.capture(&["UPDATE t SET v = 2.0 WHERE id = 1"]);
    let other = p.capture(&["UPDATE t SET v = 3 WHERE id = 1"]);
    let outcomes = p.apply(vec![to_real, other]);
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).starts_with("write conflict on t rowid 1"));
    assert_eq!(
        p.rows("SELECT typeof(v) FROM t"),
        values(vec![vec![txt("real")]])
    );
    p.assert_same();
}

/// LIMIT: the check compares row contents, not who wrote the row. A row
/// that later change sets changed and then changed back looks unchanged, so
/// a change set captured before both still applies.
#[test]
fn limit_a_row_changed_and_changed_back_is_not_a_conflict() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO t VALUES (1, 0)",
    ]);
    let stale = p.capture(&["UPDATE t SET v = 2 WHERE id = 1"]);
    let up = p.capture(&["UPDATE t SET v = 1 WHERE id = 1"]);
    assert_eq!(p.apply(vec![up]), [APPLIED]);
    let back = p.capture(&["UPDATE t SET v = 0 WHERE id = 1"]);
    assert_eq!(p.apply(vec![back]), [APPLIED]);
    assert_eq!(p.apply(vec![stale]), [APPLIED]);
    assert_eq!(p.rows("SELECT v FROM t"), ints(&[&[2]]));
    p.assert_same();
}

/// Snapshot isolation allows write skew: each transaction reads the row the
/// other writes, and both apply. Run one after the other, the second would
/// have read the first's write.
#[test]
fn transactions_that_read_each_others_rows_and_write_different_rows_both_apply() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    ]);
    let x = p.capture(&["UPDATE t SET n = (SELECT n FROM t WHERE id = 2) WHERE id = 1"]);
    let y = p.capture(&["UPDATE t SET n = (SELECT n FROM t WHERE id = 1) WHERE id = 2"]);
    assert_eq!(p.apply(vec![x, y]), [APPLIED, APPLIED]);
    assert_eq!(p.rows("SELECT * FROM t"), ints(&[&[1, 20], &[2, 10]]));
    p.assert_same();
}

#[test]
fn concurrent_inserts_of_one_key_conflict() {
    let cases = [
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (7, 1)",
            "INSERT INTO t VALUES (7, 2)",
            "write conflict on t rowid 7: another transaction inserted a row with this key",
        ),
        (
            "CREATE TABLE t(k TEXT PRIMARY KEY, v)",
            "INSERT INTO t VALUES ('x', 1)",
            "INSERT INTO t VALUES ('x', 2)",
            "write conflict on t (k) = ('x'): another transaction inserted a row with this key",
        ),
        // No PRIMARY KEY: both inserts got rowid 1, so they conflict on it.
        (
            "CREATE TABLE t(name TEXT UNIQUE, v)",
            "INSERT INTO t VALUES ('red', 1)",
            "INSERT INTO t VALUES ('red', 2)",
            "write conflict on t rowid 1: another transaction inserted a row with this key",
        ),
        // Different rowids: the UNIQUE index rejects the second.
        (
            "CREATE TABLE t(name TEXT UNIQUE, v)",
            "INSERT INTO t(rowid, name, v) VALUES (10, 'red', 1)",
            "INSERT INTO t(rowid, name, v) VALUES (20, 'red', 2)",
            "UNIQUE constraint failed: t.name",
        ),
    ];
    for (setup, first, second, expected) in cases {
        eprintln!("--- {setup}");
        let mut p = Pair::new(&[setup]);
        let first = p.capture(&[first]);
        let second = p.capture(&[second]);
        let outcomes = p.apply(vec![first, second]);
        assert_eq!(outcomes[0], APPLIED);
        assert!(reason(&outcomes[1]).contains(expected), "{outcomes:?}");
        assert_eq!(p.rows("SELECT v FROM t"), ints(&[&[1]]));
        p.assert_same();
    }
}

/// Two inserts that leave the key to the database get the same rowid from
/// the same base, so the second is rejected even though, run one after the
/// other, they would get rowids 1 and 2.
#[test]
fn inserts_without_a_key_get_the_same_rowid_and_conflict() {
    for setup in [
        "CREATE TABLE t(v TEXT)",
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    ] {
        let mut p = Pair::new(&[setup]);
        let x = p.capture(&["INSERT INTO t(v) VALUES ('x')"]);
        let y = p.capture(&["INSERT INTO t(v) VALUES ('y')"]);
        let rowids = |p: &[u8]| -> Vec<i64> {
            ChangeSet::decode(p)
                .unwrap()
                .written_rows()
                .map(|(_, id)| id)
                .collect()
        };
        eprintln!("--- {setup}: rowids {:?} and {:?}", rowids(&x), rowids(&y));
        assert_eq!(rowids(&x), [1]);
        assert_eq!(rowids(&y), [1]);
        let outcomes = p.apply(vec![x, y]);
        assert_eq!(outcomes[0], APPLIED);
        assert!(reason(&outcomes[1]).starts_with("write conflict on t rowid 1"));
        assert_eq!(
            p.rows("SELECT rowid, v FROM t"),
            values(vec![vec![i(1), txt("x")]])
        );
        p.assert_same();
    }
}

#[test]
fn delete_and_update_of_one_row_conflict_in_both_orders() {
    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ];
    let mut p = Pair::new(&setup);
    let delete = p.capture(&["DELETE FROM t WHERE id = 1"]);
    let update = p.capture(&["UPDATE t SET v = 'b' WHERE id = 1"]);
    let outcomes = p.apply(vec![delete, update]);
    assert_eq!(outcomes[0], APPLIED);
    assert_eq!(
        reason(&outcomes[1]),
        "write conflict on t rowid 1: another transaction deleted the row after this one read it"
    );
    assert_eq!(p.rows("SELECT * FROM t"), "[]");
    p.assert_same();

    let mut p = Pair::new(&setup);
    let delete = p.capture(&["DELETE FROM t WHERE id = 1"]);
    let update = p.capture(&["UPDATE t SET v = 'b' WHERE id = 1"]);
    let outcomes = p.apply(vec![update, delete]);
    assert_eq!(outcomes[0], APPLIED);
    assert_eq!(
        reason(&outcomes[1]),
        "write conflict on t rowid 1: another transaction changed the row after this one read it"
    );
    assert_eq!(
        p.rows("SELECT * FROM t"),
        values(vec![vec![i(1), txt("b")]])
    );
    p.assert_same();
}

/// A change set captured before a schema change is rejected, whatever it
/// wrote: its rows may not fit the tables any more. This also rejects
/// transactions on tables the schema change did not touch.
#[test]
fn change_sets_older_than_a_schema_change_are_rejected() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let insert = p.capture(&["INSERT INTO t VALUES (2, 'b')"]);
    let update = p.capture(&["UPDATE t SET v = 'z' WHERE id = 1"]);
    let into_u = p.capture(&["INSERT INTO u VALUES (5, 'x')"]);
    let alter = p.capture(&["ALTER TABLE t ADD COLUMN extra DEFAULT 0"]);
    let outcomes = p.apply(vec![alter, insert, update]);
    eprintln!("--- ALTER TABLE: {outcomes:?}");
    assert_eq!(outcomes[0], APPLIED);
    for o in &outcomes[1..] {
        assert_eq!(
            reason(o),
            "schema changed after the transaction read it: captured at schema epoch 1, \
             the database is at schema epoch 2"
        );
    }

    let drop = p.capture(&["DROP TABLE u"]);
    assert_eq!(p.apply(vec![drop]), [APPLIED]);
    assert!(reason(&p.apply(vec![into_u])[0]).starts_with("schema changed"));

    let unrelated = p.capture(&["INSERT INTO t VALUES (3, 'c', 1)"]);
    let index = p.capture(&["CREATE INDEX t_v ON t(v)"]);
    let outcomes = p.apply(vec![index, unrelated]);
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).starts_with("schema changed"));

    assert_eq!(
        p.rows("SELECT * FROM t"),
        values(vec![vec![i(1), txt("a"), i(0)]])
    );
    p.assert_same();
}

/// A conflict found after the change set already changed the schema (its
/// CREATE TABLE runs before the conflicting row is checked) still leaves no
/// trace.
#[test]
fn rejection_after_a_schema_change_in_the_same_change_set_leaves_no_trace() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let winner = p.capture(&["UPDATE t SET v = 'w' WHERE id = 1"]);
    let loser = p.capture(&[
        "CREATE TABLE side(x)",
        "INSERT INTO side VALUES ('must not survive')",
        "UPDATE t SET v = 'l' WHERE id = 1",
    ]);
    let set = ChangeSet::decode(&loser).unwrap();
    assert_eq!(set.changes[0].table, "sqlite_schema");
    let outcomes = p.apply(vec![winner, loser]);
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).starts_with("write conflict on t rowid 1"));
    assert_eq!(
        p.rows("SELECT count(*) FROM sqlite_schema WHERE name = 'side'"),
        ints(&[&[0]])
    );
    // The rejected change set did not advance the schema epoch: a change set
    // captured at the same base still applies.
    let later = p.capture(&["INSERT INTO t VALUES (2, 'b')"]);
    assert_eq!(p.apply(vec![later]), [APPLIED]);
    p.assert_same();
}

/// The decision rests on durable state only: a node that restarted, and a
/// node built from a snapshot, between the entries reach the same outcomes.
#[test]
fn outcomes_survive_restart_and_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let mut b = node(&dir, "b.db");
    let mut c = node(&dir, "c.db");
    let setup = capture(
        &a,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        ],
    );
    apply_all(
        &mut [&mut a, &mut b, &mut c],
        &entry(1, vec![setup]),
        &[APPLIED],
    );

    let stale_row = capture(&a, &["UPDATE t SET v = 'stale' WHERE id = 1"]);
    let other_row = capture(&a, &["UPDATE t SET v = 'other' WHERE id = 2"]);
    let winner = capture(&a, &["UPDATE t SET v = 'winner' WHERE id = 1"]);
    apply_all(
        &mut [&mut a, &mut b, &mut c],
        &entry(2, vec![winner]),
        &[APPLIED],
    );
    drop(c);
    let mut c = node(&dir, "c.db");
    let mut d = node(&dir, "d.db");
    d.sm.install_snapshot(&a.sm.snapshot().unwrap()).unwrap();
    let e3 = entry(3, vec![stale_row, other_row]);
    let mut outcomes = Vec::new();
    for n in [&mut a, &mut b, &mut c, &mut d] {
        outcomes.push(n.sm.apply(&e3).unwrap());
    }
    eprintln!("--- entry 3 on a, b, restarted c, snapshot d: {outcomes:?}");
    assert!(outcomes.iter().all(|o| o == &outcomes[0]));
    assert!(reason(&outcomes[0][0]).starts_with("write conflict on t rowid 1"));
    assert_eq!(outcomes[0][1], APPLIED);

    let stale_schema = capture(&a, &["INSERT INTO t VALUES (3, 'c')"]);
    let alter = capture(&a, &["ALTER TABLE t ADD COLUMN w"]);
    apply_all(
        &mut [&mut a, &mut b, &mut c, &mut d],
        &entry(4, vec![alter]),
        &[APPLIED],
    );
    drop(c);
    let mut c = node(&dir, "c.db");
    let mut e = node(&dir, "e.db");
    e.sm.install_snapshot(&a.sm.snapshot().unwrap()).unwrap();
    let e5 = entry(5, vec![stale_schema]);
    let mut outcomes = Vec::new();
    for n in [&mut a, &mut b, &mut c, &mut d, &mut e] {
        outcomes.push(n.sm.apply(&e5).unwrap());
    }
    eprintln!("--- entry 5 on a, b, restarted c, d, snapshot e: {outcomes:?}");
    assert!(outcomes.iter().all(|o| o == &outcomes[0]));
    assert!(reason(&outcomes[0][0]).starts_with("schema changed"));
    for n in [&b, &c, &d, &e] {
        assert_same(&a, n);
    }
}

/// Several transactions captured in a row, each on top of the ones before
/// it, then proposed as one entry: every one applies.
#[test]
fn a_batch_captured_in_a_row_applies_in_one_entry() {
    let mut p = Pair::new(&[
        "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
        "CREATE TABLE kv(k TEXT PRIMARY KEY, v)",
        "CREATE TABLE log(v TEXT)",
    ]);
    let before = dump(&p.a.db.connect().unwrap());
    let batch: Vec<Vec<Statement>> = [
        &[
            "INSERT INTO users VALUES (1, 'a@x')",
            "INSERT INTO log(v) VALUES ('t0')",
        ][..],
        &[
            "UPDATE users SET email = 'a2@x' WHERE id = 1",
            "SELECT email FROM users WHERE id = 1",
        ],
        // Fails: UNIQUE.
        &["INSERT INTO users VALUES (2, 'a2@x')"],
        // Fails after creating a table, which must not be seen by the next one.
        &["CREATE TABLE tmp(z)", "INSERT INTO nope VALUES (1)"],
        &["CREATE TABLE tmp(z, y)", "INSERT INTO tmp VALUES (1, 2)"],
        &[
            "ALTER TABLE users ADD COLUMN name TEXT",
            "UPDATE users SET name = 'Ann' WHERE id = 1",
        ],
        &[
            "INSERT INTO kv VALUES ('k', 1)",
            "INSERT INTO log(v) VALUES ('t6')",
        ],
        &[
            "DELETE FROM kv WHERE k = 'k'",
            "INSERT INTO kv VALUES ('k', 2)",
            "UPDATE users SET name = name || '!' WHERE id = 1",
        ],
        &[
            "SELECT count(*), max(name) FROM log, users",
            "DELETE FROM users WHERE id = 1",
        ],
    ]
    .iter()
    .map(|t| t.iter().map(|s| (*s).into()).collect())
    .collect();
    let results = p.a.db.capture_batch(&batch).unwrap();
    for (n, r) in results.iter().enumerate() {
        eprintln!(
            "--- transaction {n}: {:?}",
            r.as_ref().map(|c| &c.results).map_err(|e| e.to_string())
        );
    }
    assert!(results[2]
        .as_ref()
        .unwrap_err()
        .to_string()
        .contains("UNIQUE"));
    assert!(results[3]
        .as_ref()
        .unwrap_err()
        .to_string()
        .contains("no such table"));
    let ok: Vec<_> = results
        .iter()
        .enumerate()
        .filter(|(n, _)| ![2, 3].contains(n))
        .map(|(_, r)| r.as_ref().unwrap())
        .collect();
    assert_eq!(ok[1].results[1].rows, vec![vec![txt("a2@x")]]);
    assert_eq!(ok[6].results[0].rows, vec![vec![i(2), txt("Ann!")]]);
    assert_eq!(
        dump(&p.a.db.connect().unwrap()),
        before,
        "capture left no trace"
    );

    let proposals: Vec<Vec<u8>> = ok.iter().map(|c| c.change_set.clone()).collect();
    let epochs: Vec<u64> = proposals
        .iter()
        .map(|p| ChangeSet::decode(p).unwrap().schema_epoch)
        .collect();
    eprintln!("--- schema epochs {epochs:?}");
    assert_eq!(p.apply(proposals), applied_ok(7));
    assert_eq!(p.rows("SELECT count(*) FROM users"), ints(&[&[0]]));
    assert_eq!(
        p.rows("SELECT k, v FROM kv"),
        values(vec![vec![txt("k"), i(2)]])
    );
    assert_eq!(p.rows("SELECT * FROM tmp"), ints(&[&[1, 2]]));
    assert_eq!(p.rows("SELECT count(*) FROM log"), ints(&[&[2]]));
    p.assert_same();
}

/// `INSERT OR ROLLBACK` ends the whole database transaction. The batch goes
/// on: the transactions after it run on the state at the start of the batch.
#[test]
fn a_failure_that_ends_the_database_transaction() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let before = dump(&p.a.db.connect().unwrap());
    let batch: Vec<Vec<Statement>> = vec![
        vec!["INSERT INTO t VALUES (2, 'b')".into()],
        vec!["INSERT OR ROLLBACK INTO t VALUES (3, 'a')".into()],
        vec![
            "SELECT count(*) FROM t".into(),
            "INSERT INTO t VALUES (4, 'd')".into(),
        ],
    ];
    let results = p.a.db.capture_batch(&batch).unwrap();
    assert!(results[1]
        .as_ref()
        .unwrap_err()
        .to_string()
        .contains("UNIQUE"));
    let third = results[2].as_ref().unwrap();
    assert_eq!(
        third.results[0].rows,
        vec![vec![i(1)]],
        "does not see row 2"
    );
    assert_eq!(dump(&p.a.db.connect().unwrap()), before);
    let first = results[0].as_ref().unwrap().change_set.clone();
    assert_eq!(
        p.apply(vec![first, third.change_set.clone()]),
        applied_ok(2)
    );
    assert_eq!(p.rows("SELECT id FROM t"), ints(&[&[1], &[2], &[4]]));
    p.assert_same();
}

/// CDC records an UPDATE of an INTEGER PRIMARY KEY as a delete and an
/// insert whose image it writes into `before`; capture moves it to `after`.
#[test]
fn update_of_an_integer_primary_key() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let moved = p.capture(&["UPDATE t SET id = 10 WHERE id = 1"]);
    let set = ChangeSet::decode(&moved).unwrap();
    let kinds: Vec<_> = set.changes.iter().map(|c| (c.kind, c.id)).collect();
    assert_eq!(kinds, [(ChangeKind::Delete, 1), (ChangeKind::Insert, 10)]);
    assert!(set.changes[1].before.is_none() && set.changes[1].after.is_some());
    assert_eq!(p.apply(vec![moved]), [APPLIED]);
    assert_eq!(
        p.rows("SELECT * FROM t"),
        values(vec![vec![i(10), txt("a")]])
    );

    let move_again = p.capture(&["UPDATE t SET id = 20 WHERE id = 10"]);
    let update = p.capture(&["UPDATE t SET v = 'b' WHERE id = 10"]);
    let outcomes = p.apply(vec![move_again, update]);
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).contains("t rowid 10: another transaction deleted the row"));
    assert_eq!(
        p.rows("SELECT * FROM t"),
        values(vec![vec![i(20), txt("a")]])
    );
    p.assert_same();
}

/// An update that moves a TEXT PRIMARY KEY, and a concurrent insert of the
/// new key: the PRIMARY KEY rejects the second.
#[test]
fn update_of_a_text_primary_key() {
    let mut p = Pair::new(&[
        "CREATE TABLE kv(k TEXT PRIMARY KEY, v)",
        "INSERT INTO kv VALUES ('a', 1)",
    ]);
    let rename = p.capture(&[
        "UPDATE kv SET k = 'b' WHERE k = 'a'",
        "UPDATE kv SET v = 2 WHERE k = 'b'",
    ]);
    let insert = p.capture(&["INSERT INTO kv VALUES ('b', 9)"]);
    let outcomes = p.apply(vec![insert, rename]);
    eprintln!("--- {outcomes:?}");
    assert_eq!(outcomes[0], APPLIED);
    assert!(
        reason(&outcomes[1]).contains("UNIQUE constraint failed"),
        "{outcomes:?}"
    );
    assert_eq!(
        p.rows("SELECT k, v FROM kv ORDER BY k"),
        values(vec![vec![txt("a"), i(1)], vec![txt("b"), i(9)]])
    );
    let rename = p.capture(&[
        "UPDATE kv SET k = 'c' WHERE k = 'a'",
        "UPDATE kv SET v = 3 WHERE k = 'c'",
    ]);
    assert_eq!(p.apply(vec![rename]), [APPLIED]);
    assert_eq!(
        p.rows("SELECT k, v FROM kv ORDER BY k"),
        values(vec![vec![txt("b"), i(9)], vec![txt("c"), i(3)]])
    );
    p.assert_same();
}

/// ON DELETE CASCADE runs again at apply. A child inserted concurrently with
/// the parent's delete is deleted with it, as it would be if the two ran one
/// after the other; the captured cascade records do not count as conflicts.
#[test]
fn cascade_deletes_a_child_inserted_concurrently() {
    let mut p = Pair::new(&[
        "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1), (2)",
        "INSERT INTO child VALUES (10, 1), (11, 1), (20, 2)",
    ]);
    let new_child = p.capture(&["INSERT INTO child VALUES (12, 1)"]);
    let delete_parent = p.capture(&["DELETE FROM parent WHERE id = 1"]);
    assert_eq!(p.apply(vec![new_child, delete_parent]), applied_ok(2));
    assert_eq!(p.rows("SELECT id FROM child"), ints(&[&[20]]));
    p.assert_same();
}

#[test]
fn turning_the_check_off_brings_back_plain_writes() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
        "INSERT INTO t VALUES (1, 0)",
    ]);
    p.a.sm.set_conflict_check(false);
    p.b.sm.set_conflict_check(false);
    let x = p.capture(&["UPDATE t SET v = v + 1 WHERE id = 1"]);
    let y = p.capture(&["UPDATE t SET v = v + 1 WHERE id = 1"]);
    assert_eq!(p.apply(vec![x, y]), applied_ok(2));
    assert_eq!(p.rows("SELECT v FROM t"), ints(&[&[1]]), "a lost update");
    p.assert_same();
}

/// Groups captured on an [`AheadDb`] see the groups before them although the
/// node's own file does not have them yet, and apply in order with every
/// check passing. Applied, they give the node's file what the ahead file
/// holds.
#[test]
fn groups_captured_ahead_apply_in_order() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let dir = TempDir::new().unwrap();
    let ahead = AheadDb::open(dir.path().join("ahead.db")).unwrap();
    assert_eq!(ahead.reset(&p.a.db).unwrap(), 1);
    let group = |sqls: &[&[&str]]| -> Vec<Vec<u8>> {
        let batch: Vec<Vec<Statement>> = sqls
            .iter()
            .map(|t| t.iter().map(|s| (*s).into()).collect())
            .collect();
        ahead
            .capture_batch(&batch)
            .unwrap()
            .into_iter()
            .map(|r| r.unwrap().change_set)
            .collect()
    };
    let g1 = group(&[&["UPDATE t SET v = v || 'b' WHERE id = 1"]]);
    let g2 = group(&[
        &["UPDATE t SET v = v || 'c' WHERE id = 1"],
        &["ALTER TABLE t ADD COLUMN w"],
    ]);
    let g3 = group(&[&["INSERT INTO t SELECT 2, v, 'w' FROM t WHERE id = 1"]]);
    for cs in g1.iter().chain(&g2).chain(&g3) {
        let cs = ChangeSet::decode(cs).unwrap();
        assert_eq!(cs.base_index, 1);
        assert!(cs.reads_pending);
    }
    assert_eq!(p.rows("SELECT v FROM t"), values(vec![vec![txt("a")]]));
    assert_eq!(p.apply(g1), applied_ok(1));
    assert_eq!(p.apply(g2), applied_ok(2));
    assert_eq!(p.apply(g3), applied_ok(1));
    assert_eq!(
        p.rows("SELECT * FROM t"),
        values(vec![
            vec![i(1), txt("abc"), Value::Null],
            vec![i(2), txt("abc"), txt("w")]
        ])
    );
    let without_position = |d: String| -> String {
        d.lines()
            // The ahead file keeps the commit record of its last batch.
            .filter(|l| !l.starts_with("tursoraft_applied:") && !l.starts_with("turso_cdc:"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        without_position(dump(&ahead.connect().unwrap())),
        without_position(dump(&p.a.db.connect().unwrap()))
    );
    p.assert_same();
}

/// The ahead file's WAL stays short however many transactions it took, both
/// through capture and through change sets from elsewhere. Its commits are
/// not synced, and Turso then never starts the WAL over on its own; a WAL
/// that only grows makes every capture slower than the one before.
#[test]
fn the_ahead_file_wal_stays_short() {
    let p = Pair::new(&["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"]);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("ahead.db");
    let ahead = AheadDb::open(&path).unwrap();
    ahead.reset(&p.a.db).unwrap();
    let wal_bytes = || std::fs::metadata(path.with_extension("db-wal")).map_or(0, |m| m.len());
    // Without the restart, each loop adds about 5 MB.
    let limit = 4 << 20;
    for n in 0..600 {
        let insert = format!("INSERT INTO t VALUES ({n}, 'ahead')");
        let captured = ahead.capture_batch(&[vec![insert.as_str().into()]]);
        captured.unwrap().pop().unwrap().unwrap();
    }
    assert!(wal_bytes() < limit, "after captures: {} bytes", wal_bytes());
    for n in 600..1200 {
        let change_set = p.capture(&[&format!("INSERT INTO t VALUES ({n}, 'elsewhere')")]);
        assert_eq!(ahead.apply_incoming(&[change_set]).unwrap(), vec![APPLIED]);
    }
    assert!(wal_bytes() < limit, "after incoming: {} bytes", wal_bytes());
    let count = query(&ahead.connect().unwrap(), "SELECT count(*) FROM t");
    assert_eq!(count, vec![vec![i(1200)]]);
}

/// A change set that read pending change sets is rejected once one of them
/// is: here a change set from elsewhere comes first and makes the first
/// group lose a write conflict, so the groups captured on top of it read a
/// state that never existed. Every node decides alike from its file and the
/// log: one that restarted, and one built from a snapshot, between the
/// entries.
#[test]
fn a_rejection_rejects_every_later_change_set_that_read_pending_ones() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let mut b = node(&dir, "b.db");
    let mut c = node(&dir, "c.db");
    let setup = capture(
        &a,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE TABLE seen(v TEXT)",
            "CREATE TABLE other(v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
    );
    apply_all(
        &mut [&mut a, &mut b, &mut c],
        &entry(1, vec![setup]),
        &[APPLIED],
    );
    let ahead = AheadDb::open(dir.path().join("ahead.db")).unwrap();
    assert_eq!(ahead.reset(&a.db).unwrap(), 1);
    let on_ahead = |sql: &str| -> Vec<u8> {
        ahead.capture_batch(&[vec![sql.into()]]).unwrap()[0]
            .as_ref()
            .unwrap()
            .change_set
            .clone()
    };
    // Captured on the node's own file at index 1, as a former leader would.
    let foreign = capture(&a, &["UPDATE t SET v = 'foreign' WHERE id = 1"]);
    let g1 = on_ahead("UPDATE t SET v = v || '+g1' WHERE id = 1");
    let g2 = on_ahead("INSERT INTO seen SELECT v FROM t WHERE id = 1");
    let g3 = on_ahead("INSERT INTO other VALUES ('g3')");

    apply_all(
        &mut [&mut a, &mut b, &mut c],
        &entry(2, vec![foreign]),
        &[APPLIED],
    );
    let e3 = entry(3, vec![g1]);
    let o3 = a.sm.apply(&e3).unwrap();
    assert!(reason(&o3[0]).starts_with("write conflict on t rowid 1"));
    assert_eq!(b.sm.apply(&e3).unwrap(), o3);
    // c restarts before entry 3, d is built from a snapshot after it.
    drop(c);
    let mut c = node(&dir, "c.db");
    assert_eq!(c.sm.apply(&e3).unwrap(), o3);
    let mut d = node(&dir, "d.db");
    d.sm.install_snapshot(&a.sm.snapshot().unwrap()).unwrap();
    let e4 = entry(4, vec![g2, g3]);
    let mut outcomes = Vec::new();
    for n in [&mut a, &mut b, &mut c, &mut d] {
        outcomes.push(n.sm.apply(&e4).unwrap());
    }
    eprintln!("--- entry 4 on a, b, restarted c, snapshot d: {outcomes:?}");
    assert!(outcomes.iter().all(|o| o == &outcomes[0]));
    for o in &outcomes[0] {
        assert!(
            reason(o).starts_with("it read change sets that were not applied yet"),
            "{o:?}"
        );
    }
    assert_eq!(count(&a, "SELECT count(*) FROM seen"), 0);
    assert_eq!(count(&a, "SELECT count(*) FROM other"), 0);

    // After a reset the ahead file starts from the node's file again.
    assert_eq!(ahead.reset(&a.db).unwrap(), 4);
    let g5 = on_ahead("INSERT INTO seen SELECT v FROM t WHERE id = 1");
    for n in [&mut a, &mut b, &mut c, &mut d] {
        assert_eq!(n.sm.apply(&entry(5, vec![g5.clone()])).unwrap(), [APPLIED]);
    }
    assert_eq!(
        values(query(&a.db.connect().unwrap(), "SELECT v FROM seen")),
        values(vec![vec![txt("foreign")]])
    );
    for n in [&b, &c, &d] {
        assert_same(&a, n);
    }
}

/// Within one batch on the node's own file, a change set reads the earlier
/// ones that changed something, so a rejection of the first rejects it.
#[test]
fn a_batch_member_that_read_a_rejected_one_is_rejected() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE seen(v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let winner = p.capture(&["UPDATE t SET v = 'winner' WHERE id = 1"]);
    let batch: Vec<Vec<Statement>> = vec![
        vec!["SELECT 1".into()],
        vec!["UPDATE t SET v = 'loser' WHERE id = 1".into()],
        vec!["INSERT INTO seen SELECT v FROM t".into()],
    ];
    let batch: Vec<Vec<u8>> =
        p.a.db
            .capture_batch(&batch)
            .unwrap()
            .into_iter()
            .map(|r| r.unwrap().change_set)
            .collect();
    let pending: Vec<bool> = batch
        .iter()
        .map(|cs| ChangeSet::decode(cs).unwrap().reads_pending)
        .collect();
    assert_eq!(pending, [false, false, true]);
    assert_eq!(p.apply(vec![winner]), [APPLIED]);
    let outcomes = p.apply(batch);
    assert_eq!(outcomes[0], APPLIED);
    assert!(reason(&outcomes[1]).starts_with("write conflict"));
    assert!(reason(&outcomes[2]).starts_with("it read change sets"));
    assert_eq!(p.rows("SELECT count(*) FROM seen"), ints(&[&[0]]));
    p.assert_same();
}

/// `INSERT OR ROLLBACK` on the ahead file undoes the earlier transactions
/// of its batch; they report it instead of returning change sets that the
/// ahead file no longer holds.
#[test]
fn a_failure_that_ends_the_transaction_on_the_ahead_file() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
        "INSERT INTO t VALUES (1, 'a')",
    ]);
    let dir = TempDir::new().unwrap();
    let ahead = AheadDb::open(dir.path().join("ahead.db")).unwrap();
    ahead.reset(&p.a.db).unwrap();
    let earlier = ahead
        .capture_batch(&[vec!["INSERT INTO t VALUES (2, 'b')".into()]])
        .unwrap();
    let batch: Vec<Vec<Statement>> = vec![
        vec!["INSERT INTO t VALUES (3, 'c')".into()],
        vec!["INSERT OR ROLLBACK INTO t VALUES (4, 'a')".into()],
        vec![
            "SELECT count(*) FROM t".into(),
            "INSERT INTO t VALUES (5, 'e')".into(),
        ],
    ];
    let results = ahead.capture_batch(&batch).unwrap();
    assert!(matches!(results[0], Err(Error::Undone { by: 1 })));
    assert!(results[1]
        .as_ref()
        .unwrap_err()
        .to_string()
        .contains("UNIQUE"));
    let last = results[2].as_ref().unwrap();
    assert_eq!(
        last.results[0].rows,
        vec![vec![i(2)]],
        "sees row 2, not row 3"
    );
    let proposals = vec![
        earlier[0].as_ref().unwrap().change_set.clone(),
        last.change_set.clone(),
    ];
    assert_eq!(p.apply(proposals), applied_ok(2));
    assert_eq!(p.rows("SELECT id FROM t"), ints(&[&[1], &[2], &[5]]));
    assert_eq!(
        values(query(&ahead.connect().unwrap(), "SELECT id FROM t")),
        p.rows("SELECT id FROM t")
    );
    p.assert_same();
}
