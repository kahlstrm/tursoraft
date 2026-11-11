//! Phase 0: what `PRAGMA capture_data_changes_conn('full')` gives a leader.
//! Run with `cargo test -p tursoraft-changeset --test cdc_capture -- --nocapture`
//! to see the captured rows.
//!
//! `turso_cdc` (CDC v2) columns: change_id, change_time, change_txn_id,
//! change_type (1 insert, 0 update, -1 delete, 2 COMMIT), table_name, id,
//! before, after, updates. `before`/`after`/`updates` are SQLite binary records.

use std::sync::Arc;

use tempfile::TempDir;
use turso_core::{ConnectOpts, Connection, Database, Numeric, PlatformIO, Value};
use turso_sqlite::SqliteDialect;
use turso_sync_engine::types::parse_bin_record;

#[test]
fn own_open_transaction_rows_visible_before_commit_wal() {
    own_open_transaction(false);
}

#[test]
fn own_open_transaction_rows_visible_before_commit_mvcc() {
    own_open_transaction(true);
}

/// Question 1: inside an open transaction, the connection that runs it can
/// read the transaction's own change records; other connections cannot.
fn own_open_transaction(mvcc: bool) {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, mvcc);
    let leader = db.connect(ConnectOpts::new()).unwrap();
    leader
        .execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    leader
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)")
        .unwrap();
    leader.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let before_txn = max_change_id(&leader);

    leader.execute("BEGIN").unwrap();
    leader.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    leader.execute("UPDATE t SET x = 'A' WHERE id = 1").unwrap();
    leader.execute("DELETE FROM t WHERE id = 2").unwrap();
    let txn_id = leader.get_cdc_transaction_id();

    let open_rows = cdc_rows(&leader, before_txn);
    print(
        &format!("mvcc={mvcc}: own open txn before COMMIT, cdc_transaction_id={txn_id}"),
        &open_rows,
    );
    assert_eq!(
        open_rows.len(),
        3,
        "insert, update, delete visible before COMMIT"
    );
    let txn = format!("txn={:?}", Value::from_i64(txn_id));
    assert!(open_rows.iter().all(|r| r.contains(&txn)));

    let other = db.connect(ConnectOpts::new()).unwrap();
    let other_rows = cdc_rows(&other, before_txn);
    print(
        &format!("mvcc={mvcc}: other connection before COMMIT"),
        &other_rows,
    );
    assert!(other_rows.is_empty());

    leader.execute("COMMIT").unwrap();
    let committed = cdc_rows(&leader, before_txn);
    print(&format!("mvcc={mvcc}: after COMMIT"), &committed);
    assert_eq!(committed.len(), 4, "3 row changes + 1 COMMIT record");
    assert_eq!(committed[..3], open_rows[..]);
    assert!(committed[3].contains("type=Numeric(Integer(2))"));
    assert_eq!(cdc_rows(&other, before_txn), committed);
    assert_eq!(leader.get_cdc_transaction_id(), -1, "reset after COMMIT");
}

/// Question 2: ROLLBACK removes the transaction's change records.
#[test]
fn rollback_removes_captured_rows() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, false);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)")
        .unwrap();
    let before_txn = max_change_id(&conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let open_rows = cdc_rows(&conn, before_txn);
    print("before ROLLBACK", &open_rows);
    assert_eq!(open_rows.len(), 1);
    conn.execute("ROLLBACK").unwrap();
    let after = cdc_rows(&conn, before_txn);
    print("after ROLLBACK", &after);
    assert!(after.is_empty());
    assert_eq!(conn.get_cdc_transaction_id(), -1);
}

/// Question 3: `change_txn_id` separates transactions, autocommit statements
/// included, and each transaction ends with one COMMIT record.
#[test]
fn transactions_are_grouped_by_change_txn_id() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, false);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)")
        .unwrap();
    let start = max_change_id(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    // Two autocommit statements in a row: one transaction each.
    conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'd')").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET x = 'z'").unwrap();
    conn.execute("COMMIT").unwrap();
    print("four transactions", &cdc_rows(&conn, start));

    let groups = query(
        &conn,
        &format!(
            "SELECT change_txn_id, count(*), sum(change_type = 2), max(change_type = 2 AND change_id = (SELECT max(change_id) FROM turso_cdc c WHERE c.change_txn_id = turso_cdc.change_txn_id)) \
             FROM turso_cdc WHERE change_id > {start} GROUP BY change_txn_id ORDER BY min(change_id)"
        ),
    );
    eprintln!("--- (change_txn_id, rows, commit records, commit is last): {groups:?}");
    let shape: Vec<(i64, i64, i64)> = groups
        .iter()
        .map(|g| (int(&g[1]), int(&g[2]), int(&g[3])))
        .collect();
    assert_eq!(
        shape,
        vec![
            (3, 1, 1), // 2 inserts + COMMIT
            (2, 1, 1), // autocommit insert + COMMIT
            (2, 1, 1), // autocommit insert + COMMIT
            (5, 1, 1), // 4 updates + COMMIT
        ]
    );
}

/// Question 4: DDL is captured as row changes on `sqlite_schema`.
#[test]
fn schema_changes_are_captured() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, false);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    let start = max_change_id(&conn);
    let statements = [
        "CREATE TABLE s(id INTEGER PRIMARY KEY, x TEXT)",
        "INSERT INTO s VALUES (1, 'one')",
        "ALTER TABLE s ADD COLUMN y INTEGER",
        "CREATE INDEX s_x ON s(x)",
        "DROP TABLE s",
    ];
    let mut per_statement = Vec::new();
    conn.execute("BEGIN").unwrap();
    for sql in statements {
        let before = max_change_id(&conn);
        conn.execute(sql).unwrap();
        print(sql, &cdc_rows(&conn, before));
        per_statement.push(query(
            &conn,
            &format!(
                "SELECT change_type, table_name, id FROM turso_cdc \
                 WHERE change_id > {before} ORDER BY change_id"
            ),
        ));
    }
    conn.execute("COMMIT").unwrap();
    print("whole transaction after COMMIT", &cdc_rows(&conn, start));

    // (change_type, table_name) of each record, per statement.
    let kinds: Vec<Vec<(i64, String)>> = per_statement
        .iter()
        .map(|rows| rows.iter().map(|r| (int(&r[0]), text(&r[1]))).collect())
        .collect();
    let schema = || "sqlite_schema".to_string();
    assert_eq!(
        kinds,
        vec![
            vec![(1, schema())],        // CREATE TABLE: insert of its schema row
            vec![(1, "s".to_string())], // INSERT
            vec![(0, schema())],        // ALTER TABLE: update of the schema row
            vec![(1, schema())],        // CREATE INDEX: insert of its schema row
            vec![(-1, schema())],       // DROP TABLE: one delete, see below
        ]
    );
    // `id` is the sqlite_schema rowid. DROP TABLE deletes the table's schema
    // row only: no record for the index s_x dropped with it, and no delete
    // records for the table's rows.
    let id = |i: usize| int(&per_statement[i][0][2]);
    assert_eq!(id(4), id(0), "DROP TABLE record is table s");
    assert_ne!(id(4), id(3), "not index s_x");
}

fn open(dir: &TempDir, mvcc: bool) -> Arc<Database> {
    let path = dir.path().join("db.sqlite");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    if mvcc {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    db
}

/// `turso_cdc` rows after `after_change_id`, as readable strings.
fn cdc_rows(conn: &Arc<Connection>, after_change_id: i64) -> Vec<String> {
    query(
        conn,
        &format!(
            "SELECT change_id, change_txn_id, change_type, table_name, id, before, after, updates \
             FROM turso_cdc WHERE change_id > {after_change_id} ORDER BY change_id"
        ),
    )
    .into_iter()
    .map(|row| {
        let show = |v: &Value| match v {
            Value::Blob(b) => format!("{:?}", parse_bin_record(b).unwrap()),
            v => format!("{v:?}"),
        };
        let cols: Vec<String> = row.iter().map(show).collect();
        format!(
            "change_id={} txn={} type={} table={} id={} before={} after={} updates={}",
            cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7]
        )
    })
    .collect()
}

fn max_change_id(conn: &Arc<Connection>) -> i64 {
    int(&query(conn, "SELECT coalesce(max(change_id), 0) FROM turso_cdc")[0][0])
}

fn query(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Numeric(Numeric::Integer(n)) => *n,
        v => panic!("expected integer, got {v:?}"),
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.as_str().to_string(),
        v => panic!("expected text, got {v:?}"),
    }
}

fn print(label: &str, rows: &[String]) {
    eprintln!("--- {label} ({} rows)", rows.len());
    for r in rows {
        eprintln!("    {r}");
    }
}
