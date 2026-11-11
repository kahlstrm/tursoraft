//! Phase 0, question 5: an outside crate can apply captured change sets with
//! the sync engine's replay code, through public API:
//! `DatabaseTape::start_replay_session` -> `DatabaseReplaySession::replay`, fed
//! with `DatabaseChange { .. }.into_apply()`.
//!
//! The sync engine's async API is driven by a genawaiter coroutine; the loop in
//! `apply` steps Turso's IO whenever the coroutine yields.
//! Run with `cargo test -p tursoraft-changeset --test replay -- --nocapture`.

use std::sync::Arc;

use tempfile::TempDir;
use turso_core::{ConnectOpts, Connection, Database, Numeric, PlatformIO, Value, IO};
use turso_sqlite::SqliteDialect;
use turso_sync_engine::{
    database_tape::{DatabaseReplaySessionOpts, DatabaseTape, DatabaseTapeOpts},
    types::{Coro, DatabaseChange, DatabaseChangeType, DatabaseTapeOperation},
};

const CHANGE_SETS: &[&[&str]] = &[
    &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)",
        "CREATE TABLE u(v TEXT)",
        "CREATE TABLE gone(k INTEGER PRIMARY KEY)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        // Rowid table: rowids must survive, including gaps.
        "INSERT INTO u(rowid, v) VALUES (10, 'ten'), (20, 'twenty')",
        "INSERT INTO gone VALUES (1)",
    ],
    &[
        "ALTER TABLE t ADD COLUMN y",
        "UPDATE t SET x = 'B', y = 7 WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
        // Row images carry the values, so non-deterministic SQL is safe.
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
];

/// The leader commits each transaction; the follower applies the change set
/// read from the leader's open transaction before COMMIT.
#[test]
fn follower_replays_leader_change_sets() {
    let dir = TempDir::new().unwrap();
    let (_, leader_db) = open(&dir, "leader.db");
    let (follower_io, follower_db) = open(&dir, "follower.db");
    let follower_tape = replay_tape(&follower_db);
    let leader = cdc_connection(&leader_db);

    for (i, statements) in CHANGE_SETS.iter().enumerate() {
        eprintln!("--- change set {}", i + 1);
        leader.execute("BEGIN").unwrap();
        for sql in *statements {
            leader.execute(sql).unwrap();
        }
        let changes = read_open_transaction(&leader);
        leader.execute("COMMIT").unwrap();
        apply(&follower_io, &follower_tape, changes);
    }

    let follower = follower_db.connect(ConnectOpts::new()).unwrap();
    assert_same_rows(&leader, &follower);
}

/// The leader reads the change set, rolls back, and then applies the change
/// set to its own database the same way the follower does.
#[test]
fn leader_rolls_back_then_applies_its_own_change_sets() {
    let dir = TempDir::new().unwrap();
    let (leader_io, leader_db) = open(&dir, "leader.db");
    let (follower_io, follower_db) = open(&dir, "follower.db");
    let leader_tape = replay_tape(&leader_db);
    let follower_tape = replay_tape(&follower_db);
    let leader = cdc_connection(&leader_db);

    for (i, statements) in CHANGE_SETS.iter().enumerate() {
        eprintln!("--- change set {}", i + 1);
        leader.execute("BEGIN").unwrap();
        for sql in *statements {
            leader.execute(sql).unwrap();
        }
        let changes = read_open_transaction(&leader);
        leader.execute("ROLLBACK").unwrap();
        apply(&leader_io, &leader_tape, changes.clone());
        apply(&follower_io, &follower_tape, changes);
    }

    let follower = follower_db.connect(ConnectOpts::new()).unwrap();
    assert_same_rows(&leader, &follower);
    assert_eq!(
        query(&leader, "SELECT count(*) FROM t")[0][0],
        Value::from_i64(5),
        "the leader holds the applied rows, not an empty database"
    );
}

fn open(dir: &TempDir, name: &str) -> (Arc<dyn IO>, Arc<Database>) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let path = dir.path().join(name);
    let db =
        Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    (io, db)
}

/// A tape whose replay connections have CDC off, so applying does not capture.
fn replay_tape(db: &Arc<Database>) -> Arc<DatabaseTape> {
    Arc::new(DatabaseTape::new_with_opts(
        db.clone(),
        DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("off".to_string()),
            disable_auto_checkpoint: false,
        },
    ))
}

fn cdc_connection(db: &Arc<Database>) -> Arc<Connection> {
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn
}

/// The open transaction's own `turso_cdc` rows, plus a COMMIT marker: the
/// COMMIT record is written by COMMIT itself, so it is not there yet.
fn read_open_transaction(conn: &Arc<Connection>) -> Vec<DatabaseChange> {
    let txn_id = conn.get_cdc_transaction_id();
    assert_ne!(txn_id, -1, "the transaction has captured changes");
    let rows = query(
        conn,
        &format!(
            "SELECT change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates \
             FROM turso_cdc WHERE change_txn_id = {txn_id} ORDER BY change_id"
        ),
    );
    let mut changes: Vec<DatabaseChange> = rows
        .iter()
        .map(|r| DatabaseChange {
            change_id: int(&r[0]).unwrap(),
            change_time: int(&r[1]).unwrap() as u64,
            change_txn_id: int(&r[2]),
            change_type: match int(&r[3]).unwrap() {
                1 => DatabaseChangeType::Insert,
                0 => DatabaseChangeType::Update,
                -1 => DatabaseChangeType::Delete,
                t => panic!("unexpected change_type {t}"),
            },
            table_name: match &r[4] {
                Value::Text(t) => t.as_str().to_string(),
                v => panic!("expected text, got {v:?}"),
            },
            id: int(&r[5]).unwrap(),
            before: blob(&r[6]),
            after: blob(&r[7]),
            updates: blob(&r[8]),
        })
        .collect();
    changes.push(DatabaseChange {
        change_id: -1,
        change_time: 0,
        change_txn_id: Some(txn_id),
        change_type: DatabaseChangeType::Commit,
        table_name: String::new(),
        id: 0,
        before: None,
        after: None,
        updates: None,
    });
    let bytes: usize = changes
        .iter()
        .map(|c| {
            c.before.as_ref().map_or(0, Vec::len)
                + c.after.as_ref().map_or(0, Vec::len)
                + c.updates.as_ref().map_or(0, Vec::len)
        })
        .sum();
    eprintln!("    {} records, {bytes} bytes of row images", changes.len());
    changes
}

/// Applies one change set in one transaction.
fn apply(io: &Arc<dyn IO>, tape: &Arc<DatabaseTape>, changes: Vec<DatabaseChange>) {
    let tape = tape.clone();
    let mut gen = genawaiter::sync::Gen::new(|coro| async move {
        let coro: Coro<()> = coro.into();
        let opts = DatabaseReplaySessionOpts {
            use_implicit_rowid: true,
        };
        let mut session = tape.start_replay_session(&coro, opts).await?;
        for change in changes {
            let op = if change.change_type == DatabaseChangeType::Commit {
                DatabaseTapeOperation::Commit
            } else {
                DatabaseTapeOperation::RowChange(change.into_apply()?)
            };
            session.replay(&coro, op).await?;
        }
        Ok::<(), turso_sync_engine::errors::Error>(())
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(_) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }
}

/// Compares the schema (without root pages) and every row of every user table.
/// Turso's CDC tables exist only where CDC was turned on.
fn assert_same_rows(leader: &Arc<Connection>, follower: &Arc<Connection>) {
    let schema = "SELECT type, name, tbl_name, sql FROM sqlite_schema \
                  WHERE name NOT GLOB 'turso_*' AND name NOT GLOB '__turso_internal*' \
                  AND name NOT GLOB 'sqlite_*' ORDER BY name";
    let leader_schema = query(leader, schema);
    let follower_schema = query(follower, schema);
    eprintln!("--- schema\n    leader:   {leader_schema:?}\n    follower: {follower_schema:?}");
    assert_eq!(format!("{leader_schema:?}"), format!("{follower_schema:?}"));
    assert!(!leader_schema.is_empty());
    for row in &leader_schema {
        if row[0] != Value::build_text("table") {
            continue;
        }
        let Value::Text(name) = &row[1] else {
            unreachable!()
        };
        let sql = format!("SELECT rowid, * FROM {} ORDER BY rowid", name.as_str());
        let l = query(leader, &sql);
        let f = query(follower, &sql);
        eprintln!("--- {sql}\n    leader:   {l:?}\n    follower: {f:?}");
        assert!(!l.is_empty());
        assert_eq!(format!("{l:?}"), format!("{f:?}"));
    }
}

fn query(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Numeric(Numeric::Integer(n)) => Some(*n),
        Value::Null => None,
        v => panic!("expected integer, got {v:?}"),
    }
}

fn blob(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Blob(b) => Some(b.to_vec()),
        Value::Null => None,
        v => panic!("expected blob, got {v:?}"),
    }
}
