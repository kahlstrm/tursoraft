//! SQL features that phase 0 did not test: DROP INDEX, unique indexes,
//! foreign keys, triggers, views, WITHOUT ROWID tables, AUTOINCREMENT. Node A
//! captures, A and B apply, and both must give the same outcomes and end
//! equal. Tests marked LIMIT assert what happens today where it differs from
//! what SQL would do.

mod common;

use common::*;
use tursoraft_changeset::ChangeSet;
use tursoraft_statemachine::{Outcome, StateMachine};

#[test]
fn drop_index_alone() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x)",
        "CREATE INDEX t_x ON t(x)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    ]);
    let drop = p.capture(&["DROP INDEX t_x"]);
    let set = ChangeSet::decode(&drop).unwrap();
    eprintln!(
        "--- DROP INDEX records: {:?}",
        set.written_rows().collect::<Vec<_>>()
    );
    assert_eq!(p.apply(vec![drop]), applied_ok(1));
    assert_eq!(
        p.rows("SELECT count(*) FROM sqlite_schema WHERE name = 't_x'"),
        ints(&[&[0]])
    );
    p.assert_same();
}

/// A unique index on a table with INTEGER PRIMARY KEY: the second of two
/// concurrent inserts of the same value is rejected.
#[test]
fn unique_index_rejects_duplicate() {
    let mut p = Pair::new(&[
        "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE UNIQUE INDEX users_email ON users(email)",
    ]);
    let first = p.capture(&["INSERT INTO users VALUES (1, 'a@x')"]);
    let second = p.capture(&["INSERT INTO users VALUES (2, 'a@x')"]);
    let outcomes = p.apply(vec![first, second]);
    assert_eq!(outcomes[0], Outcome::Applied);
    assert!(matches!(&outcomes[1], Outcome::Rejected { reason } if reason.contains("UNIQUE")));
    // Moving a unique value from one row to another inside one transaction.
    let moved = p.capture(&[
        "INSERT INTO users VALUES (3, 'b@x')",
        "UPDATE users SET email = 'tmp' WHERE id = 1",
        "UPDATE users SET email = 'a@x' WHERE id = 3",
        "UPDATE users SET email = 'b@x' WHERE id = 1",
    ]);
    assert_eq!(p.apply(vec![moved]), applied_ok(1));
    assert_eq!(
        p.rows("SELECT id FROM users ORDER BY email"),
        ints(&[&[3], &[1]])
    );
    p.assert_same();
}

/// Foreign keys are enforced where the transaction runs and where it is
/// applied: ON DELETE CASCADE replicates, and a proposal whose parent row
/// an earlier proposal of the same entry deleted is rejected.
#[test]
fn foreign_key() {
    let mut p = Pair::new(&[
        "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1, 'one'), (2, 'two')",
        "INSERT INTO child VALUES (10, 1), (11, 1), (12, 2)",
    ]);
    let err =
        p.a.db
            .capture(&["INSERT INTO child VALUES (13, 99)".into()])
            .unwrap_err();
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");

    let cascade = p.capture(&["DELETE FROM parent WHERE id = 1"]);
    let set = ChangeSet::decode(&cascade).unwrap();
    eprintln!(
        "--- cascade records: {:?}",
        set.written_rows().collect::<Vec<_>>()
    );
    assert_eq!(p.apply(vec![cascade]), applied_ok(1));
    assert_eq!(p.rows("SELECT id FROM child"), ints(&[&[12]]));

    let delete_two = p.capture(&["DELETE FROM parent WHERE id = 2"]);
    let child_of_two = p.capture(&["INSERT INTO child VALUES (14, 2)"]);
    let outcomes = p.apply(vec![delete_two, child_of_two]);
    assert_eq!(outcomes[0], Outcome::Applied);
    assert!(
        matches!(&outcomes[1], Outcome::Rejected { reason } if reason.contains("FOREIGN KEY")),
        "{outcomes:?}"
    );
    assert_eq!(p.rows("SELECT count(*) FROM child"), ints(&[&[0]]));
    p.assert_same();
}

/// A trigger's writes are captured as rows. Replay turns triggers off, so
/// they are applied once, not once from the record and again by the trigger.
#[test]
fn trigger() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x)",
        "CREATE TABLE audit(id INTEGER PRIMARY KEY, t_id, what)",
        "CREATE TABLE counter(id INTEGER PRIMARY KEY, n)",
        "INSERT INTO counter VALUES (1, 0)",
        "CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN \
           INSERT INTO audit(t_id, what) VALUES (new.id, 'insert'); \
           UPDATE counter SET n = n + 1 WHERE id = 1; END",
    ]);
    let insert = p.capture(&["INSERT INTO t VALUES (1, 'a'), (2, 'b')"]);
    let set = ChangeSet::decode(&insert).unwrap();
    eprintln!(
        "--- trigger records: {:?}",
        set.written_rows().collect::<Vec<_>>()
    );
    assert_eq!(p.apply(vec![insert]), applied_ok(1));
    assert_eq!(p.rows("SELECT count(*) FROM audit"), ints(&[&[2]]));
    assert_eq!(p.rows("SELECT n FROM counter"), ints(&[&[2]]));
    // The trigger reached B through the change set: it fires for
    // transactions captured on B.
    let on_b = capture(&p.b, &["INSERT INTO t VALUES (3, 'c')"]);
    assert_eq!(p.apply(vec![on_b]), applied_ok(1));
    assert_eq!(p.rows("SELECT n FROM counter"), ints(&[&[3]]));
    p.assert_same();

    let drop = p.capture(&["DROP TRIGGER t_ins", "INSERT INTO t VALUES (4, 'd')"]);
    assert_eq!(p.apply(vec![drop]), applied_ok(1));
    assert_eq!(p.rows("SELECT n FROM counter"), ints(&[&[3]]));
    p.assert_same();
}

/// Views, like triggers, get no CDC record; capture adds their schema rows.
#[test]
fn view() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x)",
        "INSERT INTO t VALUES (1, 5), (2, 7)",
        "CREATE VIEW big AS SELECT id FROM t WHERE x > 6",
    ]);
    assert_eq!(p.rows("SELECT id FROM big"), ints(&[&[2]]));
    let drop = p.capture(&[
        "DROP VIEW big",
        "CREATE VIEW small AS SELECT id FROM t WHERE x < 6",
    ]);
    assert_eq!(p.apply(vec![drop]), applied_ok(1));
    assert_eq!(p.rows("SELECT id FROM small"), ints(&[&[1]]));
    assert_eq!(
        p.rows("SELECT count(*) FROM sqlite_schema WHERE name = 'big'"),
        ints(&[&[0]])
    );
    p.assert_same();
}

/// LIMIT: Turso refuses WITHOUT ROWID tables unless the database is opened
/// with an experimental flag, and with the flag CDC refuses writes to them
/// ("CDC on WITHOUT ROWID tables is not supported"). NodeDb leaves the flag off.
#[test]
fn limit_without_rowid_is_refused() {
    let p = Pair::new(&["CREATE TABLE t(x)"]);
    let err =
        p.a.db
            .capture(&["CREATE TABLE kv(k TEXT PRIMARY KEY, v) WITHOUT ROWID".into()])
            .unwrap_err();
    eprintln!("--- {err}");
    assert!(
        err.to_string()
            .contains("WITHOUT ROWID tables are an experimental feature"),
        "{err}"
    );
}

/// AUTOINCREMENT keeps its counter in a table; a node that captures after
/// applying must not reuse an id.
#[test]
fn autoincrement() {
    let mut p = Pair::new(&[
        "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, x)",
        "INSERT INTO t(x) VALUES ('a'), ('b'), ('c')",
    ]);
    let delete = p.capture(&["DELETE FROM t WHERE id = 3"]);
    assert_eq!(p.apply(vec![delete]), applied_ok(1));
    let on_b = capture(&p.b, &["INSERT INTO t(x) VALUES ('d')"]);
    assert_eq!(p.apply(vec![on_b]), applied_ok(1));
    assert_eq!(
        p.rows("SELECT id FROM t ORDER BY id"),
        ints(&[&[1], &[2], &[4]])
    );
    p.assert_same();
}

/// Entries with no proposals, as Raft's blank and membership entries.
#[test]
fn empty_entry_advances_position() {
    let mut p = Pair::new(&["CREATE TABLE t(x)"]);
    assert_eq!(p.apply(vec![]), vec![]);
    assert_eq!(p.a.sm.applied().unwrap().unwrap().index, 2);
    p.assert_same();
}
