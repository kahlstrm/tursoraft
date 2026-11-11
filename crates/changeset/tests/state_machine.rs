//! Capture on one node, apply on every node, through the `StateMachine` trait.
//! Run with `cargo test -p tursoraft-changeset --test state_machine -- --nocapture`
//! to see the change-set sizes and the database contents compared.

use std::time::{Duration, Instant};

mod common;

use common::*;
use tempfile::TempDir;
use tursoraft_changeset::{turso_core::Value, ChangeKind, ChangeSet, NodeDb, Statement};
use tursoraft_statemachine::{Outcome, StateMachine};

/// The node that captures (A) and a node that only applies (B) end equal,
/// across inserts, updates, deletes and schema changes. A transaction that is
/// captured and never proposed leaves no trace.
#[test]
fn capturing_node_and_follower_end_equal() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let mut b = node(&dir, "b.db");
    let transactions: &[&[&str]] = &[
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)",
            "CREATE TABLE u(v TEXT)",
            "CREATE TABLE gone(k INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO u(rowid, v) VALUES (10, 'ten'), (20, 'twenty')",
            "INSERT INTO gone VALUES (1)",
        ],
        &[
            "ALTER TABLE t ADD COLUMN y",
            "UPDATE t SET x = 'B', y = 7 WHERE id = 2",
            "DELETE FROM t WHERE id = 3",
            "INSERT INTO t VALUES (4, hex(randomblob(8)), random())",
            "INSERT INTO t VALUES (5, NULL, 2.5), (6, 'blob', x'00ff')",
            "DELETE FROM u WHERE rowid = 10",
            "INSERT INTO u(v) VALUES ('next')",
        ],
        &[
            "CREATE INDEX t_x ON t(x)",
            "UPDATE u SET v = upper(v)",
            "DROP TABLE gone",
        ],
        &["DROP INDEX t_x", "UPDATE t SET x = x || '!' WHERE id < 5"],
    ];
    for (i, statements) in transactions.iter().enumerate() {
        let index = i as u64 + 1;
        let proposal = capture(&a, statements);
        let change_set = ChangeSet::decode(&proposal).unwrap();
        assert_eq!(change_set.base_index, index - 1);
        eprintln!(
            "--- change set {index}: {} rows, {} bytes, rows written {:?}",
            change_set.changes.len(),
            proposal.len(),
            change_set.written_rows().collect::<Vec<_>>()
        );
        apply_all(
            &mut [&mut a, &mut b],
            &entry(index, vec![proposal]),
            &applied_ok(1),
        );
    }
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 5);
    assert_same(&a, &b);

    // Captured, never proposed: A is unchanged, including after DDL.
    let before = dump(&a.db.connect().unwrap());
    let captured =
        a.db.capture(&[
            "INSERT INTO t VALUES (100, 'never', 0) RETURNING id, x".into(),
            "SELECT count(*) FROM t".into(),
            "CREATE TABLE never(z)".into(),
            "UPDATE u SET v = 'x'".into(),
        ])
        .unwrap();
    assert_eq!(
        captured.results[0].rows,
        vec![vec![Value::from_i64(100), Value::build_text("never")]]
    );
    assert_eq!(
        captured.results[1].rows,
        vec![vec![Value::from_i64(6)]],
        "the transaction reads its own writes"
    );
    assert_eq!(
        captured.results[3].rows_changed, 2,
        "u holds rowids 20 and 21"
    );
    // Insert into t, insert of never's schema row, two updates of u.
    assert_eq!(
        ChangeSet::decode(&captured.change_set)
            .unwrap()
            .changes
            .len(),
        4
    );
    assert_eq!(dump(&a.db.connect().unwrap()), before);
    assert_same(&a, &b);
}

/// One entry, several proposals: the ones that cannot be applied are
/// rejected, leave no trace, and both nodes give the same outcomes.
#[test]
fn entry_with_rejected_proposals() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let mut b = node(&dir, "b.db");
    let setup = capture(
        &a,
        &["CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT UNIQUE)"],
    );
    apply_all(
        &mut [&mut a, &mut b],
        &entry(1, vec![setup]),
        &applied_ok(1),
    );

    // All captured on A from the same state, as concurrent transactions.
    let alice = capture(&a, &["INSERT INTO users VALUES (1, 'alice@x')"]);
    let alice_again = capture(
        &a,
        &[
            "CREATE TABLE side_effect(s INTEGER PRIMARY KEY, t TEXT)",
            "INSERT INTO side_effect VALUES (1, 'must not survive')",
            "INSERT INTO users VALUES (2, 'alice@x')",
        ],
    );
    // Rejected because bob, before it in the entry, changed the schema.
    let carol = capture(&a, &["INSERT INTO users VALUES (3, 'carol@x')"]);
    let bob = capture(
        &a,
        &[
            "CREATE TABLE side_effect(s INTEGER PRIMARY KEY, a, b, c)",
            "INSERT INTO side_effect VALUES (7, 1, 2, 3)",
            "INSERT INTO users VALUES (4, 'bob@x')",
        ],
    );
    let garbage = vec![1, 2, 3];
    let proposals = vec![alice, alice_again, bob, carol, garbage];

    let outcomes_a = a.sm.apply(&entry(2, proposals.clone())).unwrap();
    let outcomes_b = b.sm.apply(&entry(2, proposals)).unwrap();
    eprintln!("--- outcomes {outcomes_a:#?}");
    assert_eq!(outcomes_a, outcomes_b);
    let shape: Vec<bool> = outcomes_a
        .iter()
        .map(|o| matches!(o, Outcome::Applied))
        .collect();
    assert_eq!(shape, [true, false, true, false, false]);
    let reason = |i: usize| match &outcomes_a[i] {
        Outcome::Rejected { reason } => reason.clone(),
        o => panic!("{o:?}"),
    };
    assert!(reason(1).contains("UNIQUE"), "{}", reason(1));
    assert!(reason(3).contains("schema changed"), "{}", reason(3));

    for n in [&a, &b] {
        let users = query(
            &n.db.connect().unwrap(),
            "SELECT id, email FROM users ORDER BY id",
        );
        assert_eq!(
            format!("{users:?}"),
            format!(
                "{:?}",
                vec![
                    vec![Value::from_i64(1), Value::build_text("alice@x")],
                    vec![Value::from_i64(4), Value::build_text("bob@x")],
                ]
            )
        );
        // side_effect has the shape of the applied proposal (bob), not the
        // rejected one, whose CREATE TABLE was rolled back.
        let side = query(&n.db.connect().unwrap(), "SELECT * FROM side_effect");
        assert_eq!(
            format!("{side:?}"),
            format!(
                "{:?}",
                vec![vec![
                    Value::from_i64(7),
                    Value::from_i64(1),
                    Value::from_i64(2),
                    Value::from_i64(3)
                ]]
            )
        );
    }
    assert_same(&a, &b);
    assert_eq!(a.sm.applied().unwrap().unwrap().index, 2);
}

/// Entries without proposals advance the applied position; an entry at or
/// below it is not applied again; the position survives closing the file.
#[test]
fn restart_keeps_applied_position_and_skips_old_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.db");
    {
        let db = NodeDb::open(&path).unwrap();
        let mut sm = db.state_machine();
        assert_eq!(sm.applied().unwrap(), None);
        let p = db
            .capture(&["CREATE TABLE t(id INTEGER PRIMARY KEY, n)".into()])
            .unwrap();
        assert_eq!(
            sm.apply(&entry(1, vec![p.change_set])).unwrap(),
            applied_ok(1)
        );
        assert_eq!(sm.apply(&entry(2, vec![])).unwrap(), vec![]);
        let p = db
            .capture(&[
                "INSERT INTO t VALUES (1, 1)".into(),
                "INSERT INTO t(n) VALUES (2)".into(),
            ])
            .unwrap();
        assert_eq!(
            sm.apply(&entry(3, vec![p.change_set])).unwrap(),
            applied_ok(1)
        );
    }
    let a = node(&dir, "a.db");
    let mut sm = a.sm;
    let applied = sm.applied().unwrap().unwrap();
    assert_eq!((applied.index, applied.raft_meta), (3, b"meta 3".to_vec()));
    assert_eq!(a.db.applied_index(), 3);

    // Replay entry 3 after the restart, as the Raft layer may: no effect.
    let before = dump(&a.db.connect().unwrap());
    let p =
        a.db.capture(&["INSERT INTO t(n) VALUES (3)".into()])
            .unwrap();
    assert_eq!(
        sm.apply(&entry(3, vec![p.change_set.clone()])).unwrap(),
        vec![]
    );
    assert_eq!(sm.apply(&entry(1, vec![p.change_set])).unwrap(), vec![]);
    assert_eq!(dump(&a.db.connect().unwrap()), before);
    assert_eq!(sm.applied().unwrap().unwrap().index, 3);
}

const CRASH_ROWS_PER_ENTRY: i64 = 2000;

/// Kills a process while it applies large entries, reopens the file, and
/// checks that it holds exactly the entries up to the applied index. The child
/// writes which entry it is applying to a progress file, so the test knows
/// whether a kill landed inside `apply`.
#[test]
fn crash_during_apply_keeps_entries_atomic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("crash.db");
    let progress = dir.path().join("progress");
    let mut seen_indexes = Vec::new();
    let mut killed_inside_apply = 0;
    for round in 0..8 {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--ignored", "--nocapture"])
            .env("TURSORAFT_CRASH_DB", &path)
            .env("TURSORAFT_CRASH_PROGRESS", &progress)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(400 + 150 * round));
        child.kill().unwrap();
        child.wait().unwrap();
        let last_progress = std::fs::read_to_string(&progress).unwrap_or_default();

        let db = NodeDb::open(&path).unwrap();
        let applied = db.state_machine().applied().unwrap().map_or(0, |a| a.index);
        let conn = db.connect().unwrap();
        if applied > 0 {
            let row = &query(&conn, "SELECT count(*), coalesce(max(entry), 0) FROM big")[0];
            eprintln!(
                "--- round {round}: killed at '{last_progress}', applied index {applied}, rows {}",
                int(&row[0])
            );
            assert_eq!(int(&row[0]), (applied as i64 - 1) * CRASH_ROWS_PER_ENTRY);
            assert_eq!(int(&row[1]), applied as i64 - 1);
        }
        assert_eq!(
            query(&conn, "PRAGMA integrity_check"),
            vec![vec![Value::build_text("ok")]]
        );
        if last_progress == format!("applying {}", applied + 1) {
            killed_inside_apply += 1;
        }
        seen_indexes.push(applied);
    }
    eprintln!("--- {killed_inside_apply} of 8 kills landed inside apply");
    assert!(killed_inside_apply > 0);
    assert!(
        seen_indexes.windows(2).all(|w| w[0] <= w[1]) && *seen_indexes.last().unwrap() > 1,
        "the child made progress: {seen_indexes:?}"
    );
}

/// Run by `crash_during_apply_keeps_entries_atomic` in a child process: applies
/// entry after entry until it is killed. Entry 1 creates the table; entry k > 1
/// inserts CRASH_ROWS_PER_ENTRY rows tagged k - 1.
#[test]
#[ignore]
fn crash_child() {
    let Ok(path) = std::env::var("TURSORAFT_CRASH_DB") else {
        return;
    };
    let db = NodeDb::open(path).unwrap();
    let mut sm = db.state_machine();
    let mut index = sm.applied().unwrap().map_or(0, |a| a.index);
    loop {
        index += 1;
        let statements: Vec<Statement> = if index == 1 {
            vec!["CREATE TABLE big(id INTEGER PRIMARY KEY, entry INTEGER, pad TEXT)".into()]
        } else {
            vec![Statement::new(
                "INSERT INTO big(entry, pad) SELECT ?, hex(randomblob(32)) FROM generate_series(1, ?)",
                vec![Value::from_i64(index as i64 - 1), Value::from_i64(CRASH_ROWS_PER_ENTRY)],
            )]
        };
        let p = db.capture(&statements).unwrap();
        let progress = std::env::var("TURSORAFT_CRASH_PROGRESS").unwrap();
        std::fs::write(&progress, format!("applying {index}")).unwrap();
        assert_eq!(
            sm.apply(&entry(index, vec![p.change_set])).unwrap(),
            applied_ok(1)
        );
        std::fs::write(&progress, format!("applied {index}")).unwrap();
    }
}

/// A node that installs a snapshot and then applies later entries equals the
/// source. Read connections opened before the install keep working.
#[test]
fn snapshot_install_then_later_entries() {
    let dir = TempDir::new().unwrap();
    let mut src = node(&dir, "src.db");
    let mut behind = node(&dir, "behind.db");
    let mut empty = node(&dir, "empty.db");
    let (src_db, empty_db) = (src.db.clone(), empty.db.clone());
    let mut index = 0;
    let mut step = |nodes: &mut [&mut Node], capturer: &NodeDb, statements: &[&str]| {
        index += 1;
        let statements: Vec<Statement> = statements.iter().map(|s| (*s).into()).collect();
        let p = capturer.capture(&statements).unwrap().change_set;
        apply_all(nodes, &entry(index, vec![p]), &applied_ok(1));
    };
    step(
        &mut [&mut src, &mut behind],
        &src_db,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)",
            "CREATE TABLE gone(g)",
            "INSERT INTO gone VALUES (1)",
        ],
    );
    step(
        &mut [&mut src],
        &src_db,
        &["INSERT INTO t SELECT value, hex(randomblob(500)) FROM generate_series(1, 300)"],
    );
    step(
        &mut [&mut src],
        &src_db,
        &[
            "DROP TABLE gone",
            "CREATE INDEX t_x ON t(x)",
            "DELETE FROM t WHERE id % 3 = 0",
        ],
    );

    let reader = empty.db.connect().unwrap();
    assert_eq!(
        query(
            &reader,
            "SELECT count(*) FROM sqlite_schema WHERE name = 't'"
        ),
        vec![vec![Value::from_i64(0)]]
    );
    let behind_reader = behind.db.connect().unwrap();
    assert_eq!(
        query(&behind_reader, "SELECT count(*) FROM gone"),
        vec![vec![Value::from_i64(1)]]
    );

    let snapshot = src.sm.snapshot().unwrap();
    eprintln!(
        "--- snapshot at index {}: {} bytes",
        snapshot.applied.index,
        snapshot.data.len()
    );
    assert_eq!(snapshot.applied.index, 3);
    for n in [&mut empty, &mut behind] {
        n.sm.install_snapshot(&snapshot).unwrap();
        assert_eq!(n.sm.applied().unwrap(), Some(snapshot.applied.clone()));
        assert_eq!(n.db.applied_index(), 3);
    }
    assert_eq!(
        query(&reader, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(200)]]
    );
    assert!(
        behind_reader.prepare("SELECT count(*) FROM gone").is_err(),
        "dropped in the snapshot"
    );

    step(
        &mut [&mut src, &mut behind, &mut empty],
        &empty_db,
        &[
            "UPDATE t SET x = 'after' WHERE id < 10",
            "ALTER TABLE t ADD COLUMN z",
            "INSERT INTO t VALUES (1000, 'new', 1)",
        ],
    );
    assert_same(&src, &empty);
    assert_same(&src, &behind);

    // The installed file survives reopening.
    drop((reader, behind_reader, empty, empty_db));
    let empty = node(&dir, "empty.db");
    assert_same(&src, &empty);
}

/// Change-set sizes of typical transactions.
#[test]
fn change_set_sizes() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let setup = capture(
        &a,
        &[
            "CREATE TABLE accounts(id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)",
            "CREATE TABLE events(id INTEGER PRIMARY KEY, at TEXT, kind TEXT, body TEXT)",
            "INSERT INTO accounts SELECT value, 'owner ' || value, 1000 FROM generate_series(1, 100)",
        ],
    );
    let setup_len = setup.len();
    a.sm.apply(&entry(1, vec![setup])).unwrap();
    let cases: &[(&str, &[&str])] = &[
        ("create 2 tables + insert 100 rows (setup)", &[]),
        ("insert 1 small row", &["INSERT INTO events VALUES (1, '2026-11-11', 'login', 'user 7')"]),
        ("insert 1 row with 1 KB text", &["INSERT INTO events VALUES (2, '2026-11-11', 'blob', hex(randomblob(512)))"]),
        ("update 1 column of 1 row", &["UPDATE accounts SET balance = balance - 10 WHERE id = 1"]),
        (
            "bank transfer: 2 updates + 1 insert",
            &[
                "UPDATE accounts SET balance = balance - 10 WHERE id = 1",
                "UPDATE accounts SET balance = balance + 10 WHERE id = 2",
                "INSERT INTO events VALUES (3, '2026-11-11', 'transfer', '1 -> 2: 10')",
            ],
        ),
        ("delete 1 row", &["DELETE FROM accounts WHERE id = 100"]),
        ("update 1 column of 100 rows", &["UPDATE accounts SET balance = balance + 1"]),
        ("insert 1000 small rows", &["INSERT INTO events SELECT 100 + value, '2026-11-11', 'bulk', 'row ' || value FROM generate_series(1, 1000)"]),
        ("CREATE INDEX", &["CREATE INDEX accounts_owner ON accounts(owner)"]),
        ("ALTER TABLE ADD COLUMN", &["ALTER TABLE accounts ADD COLUMN note TEXT"]),
    ];
    eprintln!(
        "--- change-set sizes\n| transaction | rows | bytes | bytes per row |\n|---|---|---|---|"
    );
    for (name, statements) in cases {
        let (rows, bytes) = if statements.is_empty() {
            (102, setup_len)
        } else {
            let p = capture(&a, statements);
            (ChangeSet::decode(&p).unwrap().changes.len(), p.len())
        };
        eprintln!("| {name} | {rows} | {bytes} | {} |", bytes / rows.max(1));
    }
}

/// The first capture of a change set reports the right kinds and rows.
#[test]
fn change_set_lists_written_rows() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let p = capture(
        &a,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, x)",
            "INSERT INTO t VALUES (5, 'a'), (9, 'b')",
        ],
    );
    a.sm.apply(&entry(7, vec![p])).unwrap();
    let p = capture(
        &a,
        &[
            "UPDATE t SET x = 'c' WHERE id = 9",
            "DELETE FROM t WHERE id = 5",
            "INSERT INTO t VALUES (6, 'd')",
        ],
    );
    let set = ChangeSet::decode(&p).unwrap();
    assert_eq!(set.base_index, 7);
    assert_eq!(
        set.written_rows().collect::<Vec<_>>(),
        [("t", 9), ("t", 5), ("t", 6)]
    );
    let kinds: Vec<_> = set.changes.iter().map(|c| c.kind).collect();
    assert_eq!(
        kinds,
        [ChangeKind::Update, ChangeKind::Delete, ChangeKind::Insert]
    );
    assert!(set
        .changes
        .iter()
        .all(|c| (c.kind == ChangeKind::Insert) == c.before.is_none()));
}

#[test]
fn capture_refuses_transaction_control() {
    let dir = TempDir::new().unwrap();
    let a = node(&dir, "a.db");
    for sql in [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT s",
        "BEGIN",
        "SELECT 1; COMMIT",
    ] {
        let err = a.db.capture(&["SELECT 1".into(), sql.into()]).unwrap_err();
        assert!(
            err.to_string().contains("statement 1 is not allowed"),
            "{sql}: {err}"
        );
    }
    let err =
        a.db.capture(&[
            "CREATE TABLE t(x)".into(),
            "INSERT INTO nope VALUES (1)".into(),
        ])
        .unwrap_err();
    assert!(err.to_string().contains("statement 1 failed"), "{err}");
    assert_eq!(
        query(
            &a.db.connect().unwrap(),
            "SELECT count(*) FROM sqlite_schema WHERE name = 't'"
        ),
        vec![vec![Value::from_i64(0)]]
    );
    // The capture connection is usable after a failure.
    a.db.capture(&["CREATE TABLE t(x)".into()]).unwrap();
}

/// Capture and apply both want the write lock. Each waits for the other.
#[test]
fn capture_and_apply_at_once() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let p = capture(
        &a,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, x)",
            "CREATE TABLE s(id INTEGER PRIMARY KEY, x)",
        ],
    );
    a.sm.apply(&entry(1, vec![p])).unwrap();
    let small = capture(&a, &["INSERT INTO s VALUES (1, 'small')"]);

    let db = a.db.clone();
    let slow = std::thread::spawn(move || {
        let start = Instant::now();
        let p = db
            .capture(&[
                "INSERT INTO t SELECT value, hex(randomblob(100)) FROM generate_series(1, 50000)"
                    .into(),
            ])
            .unwrap();
        (start.elapsed(), p.change_set.len())
    });
    std::thread::sleep(Duration::from_millis(50));
    let start = Instant::now();
    assert_eq!(a.sm.apply(&entry(2, vec![small])).unwrap(), applied_ok(1));
    let apply_took = start.elapsed();
    let (capture_took, bytes) = slow.join().unwrap();
    eprintln!("--- capture of 50000 rows took {capture_took:?} ({bytes} bytes); apply waited and took {apply_took:?}");
    assert_eq!(count(&a, "SELECT count(*) FROM s"), 1);
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 0);
}

/// A read connection that writes takes the write lock outside the NodeDb.
/// Apply then fails instead of waiting.
#[test]
fn writer_outside_node_db_blocks_apply() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let p = capture(&a, &["CREATE TABLE t(id INTEGER PRIMARY KEY)"]);
    a.sm.apply(&entry(1, vec![p])).unwrap();
    let p = capture(&a, &["INSERT INTO t VALUES (1)"]);
    let rogue = a.db.connect().unwrap();
    rogue.execute("BEGIN IMMEDIATE").unwrap();
    let err = a.sm.apply(&entry(2, vec![p.clone()])).unwrap_err();
    eprintln!("--- apply while another connection holds the write lock: {err}");
    let start = Instant::now();
    let capture_err =
        a.db.capture(&["INSERT INTO t VALUES (2)".into()])
            .unwrap_err();
    eprintln!(
        "--- capture while another connection holds the write lock, after {:?}: {capture_err}",
        start.elapsed()
    );
    rogue.execute("ROLLBACK").unwrap();
    assert_eq!(a.db.applied_index(), 1);
    assert_eq!(a.sm.apply(&entry(2, vec![p])).unwrap(), applied_ok(1));
    assert_eq!(count(&a, "SELECT count(*) FROM t"), 1);
}

#[test]
fn wait_for_applied_wakes_reader() {
    let dir = TempDir::new().unwrap();
    let mut a = node(&dir, "a.db");
    let p = capture(&a, &["CREATE TABLE t(x)", "INSERT INTO t VALUES (42)"]);
    let db = a.db.clone();
    let reader = std::thread::spawn(move || {
        assert!(!db.wait_for_applied(1, Duration::from_millis(10)));
        assert!(db.wait_for_applied(1, Duration::from_secs(10)));
        query(&db.connect().unwrap(), "SELECT x FROM t")
    });
    std::thread::sleep(Duration::from_millis(100));
    a.sm.apply(&entry(1, vec![p])).unwrap();
    assert_eq!(reader.join().unwrap(), vec![vec![Value::from_i64(42)]]);
}
