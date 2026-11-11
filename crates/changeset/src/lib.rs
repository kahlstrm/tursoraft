//! Change sets on Turso: capture one transaction's row changes, and apply
//! committed change sets to a database file as the replicated state machine.
//!
//! A [`NodeDb`] owns one database file in WAL mode. Its file changes only
//! through [`TursoStateMachine::apply`] and
//! [`TursoStateMachine::install_snapshot`]:
//!
//! - [`NodeDb::capture`] runs statements as one transaction, reads the
//!   transaction's change records from `turso_cdc`, and rolls the transaction
//!   back. The encoded [`ChangeSet`] is one Raft proposal.
//! - [`NodeDb::capture_batch`] does the same for several transactions in a
//!   row, each seeing the ones before it.
//! - [`TursoStateMachine::apply`] applies an entry's change sets, each inside
//!   its own savepoint, and writes the applied position in the same
//!   transaction. A change set that another one, applied after its base,
//!   conflicts with is rejected (snapshot isolation by rows). A change set
//!   that read pending change sets is rejected when one of them was.
//! - [`AheadDb`] is a second file for a leader that captures the next group
//!   before the previous one is applied: groups run on it for real, so each
//!   sees the ones before it.
//! - [`NodeDb::begin`] opens a local transaction ([`LocalTx`]) whose
//!   statements run one at a time on this file; its commit returns the
//!   change set and rolls the transaction back.
//! - [`NodeDb::connect`] gives connections for local reads, and
//!   [`NodeDb::wait_for_applied`] waits until an entry has been applied.
//!
//! Capture, apply and local transactions take one write lock per [`NodeDb`],
//! so they run one at a time; none sees Turso's "database is busy" from
//! another. See the `local` module for how a local transaction holds it.
//!
//! Foreign keys are enforced both when a transaction is captured and when it
//! is applied.

mod apply;
mod changeset;
mod local;

use std::{
    future::Future,
    num::NonZero,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use turso_core::{ConnectOpts, Connection, Database, LimboError, Numeric, OpenOptions, Value, IO};
use turso_sqlite::SqliteDialect;
use turso_sync_engine::{
    database_tape::{DatabaseTape, DatabaseTapeOpts},
    types::Coro,
};
use tursoraft_statemachine::{Applied, Entry, Outcome, Snapshot, StateMachine};

pub use changeset::{ChangeKind, ChangeSet, RowChange};
pub use local::LocalTx;
pub use turso_core;

/// The table that holds the applied position, the schema epoch (the number of
/// applied change sets that changed the schema) and `last_rejected` (the
/// index of the last entry in which a change set was rejected, 0 if none).
/// One row, `id = 0`.
pub const APPLIED_TABLE: &str = "tursoraft_applied";

/// How long capture waits for a writer outside this [`NodeDb`], such as a
/// read connection that wrote.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] LimboError),
    #[error("replay error: {0}")]
    Replay(#[from] turso_sync_engine::errors::Error),
    #[error("statement {index} failed: {source}")]
    Statement { index: usize, source: LimboError },
    #[error("statement {index} is not allowed in a captured transaction: {reason}")]
    NotAllowed { index: usize, reason: String },
    #[error("invalid change set: {0}")]
    Decode(String),
    #[error("invalid snapshot: {0}")]
    Snapshot(String),
    /// A proposal that cannot be applied to the current state, such as one
    /// that lost a write conflict. `apply` turns it into a rejection.
    #[error("{0}")]
    Rejected(String),
    /// [`AheadDb::capture_batch`]: a later transaction of the batch ended the
    /// database transaction and undid this one. It had no effect.
    #[error("undone by the failure of transaction {by} of the same batch")]
    Undone { by: usize },
    /// A local transaction ended without effect: it held the write lock
    /// longer than the hold limit while an apply or another transaction
    /// waited, or a failing statement ended it (`INSERT OR ROLLBACK`).
    #[error("the local transaction was ended: {0}")]
    Aborted(String),
    /// A local transaction could not begin: another one held the write lock
    /// for the whole wait.
    #[error("busy: {0}")]
    Busy(String),
}

/// One SQL statement and its parameters, bound by position.
#[derive(Debug, Clone)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

impl From<&str> for Statement {
    fn from(sql: &str) -> Self {
        Self::new(sql, Vec::new())
    }
}

/// What one statement returned inside the captured transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// Rows the statement inserted, updated or deleted (`changes()`).
    pub rows_changed: u64,
}

/// The result of [`NodeDb::capture`].
#[derive(Debug, Clone)]
pub struct Captured {
    pub results: Vec<StatementResult>,
    /// The encoded [`ChangeSet`]: one proposal. A transaction that wrote
    /// nothing gives a change set without changes.
    pub change_set: Vec<u8>,
}

/// One result per transaction of a batch.
type BatchResults = Vec<Result<Captured, Error>>;

/// One node's database file. Cheap to clone; clones share the file and the
/// write lock.
#[derive(Clone)]
pub struct NodeDb {
    shared: Arc<Shared>,
}

struct Shared {
    io: Arc<dyn IO>,
    db: Arc<Database>,
    /// Held by capture, apply, snapshot and install_snapshot. Holds the
    /// connection that captures, with CDC on. Capture, apply and install also
    /// claim `gate`.
    write: Mutex<Arc<Connection>>,
    /// Who holds the database's write lock across calls; see `local`.
    gate: local::Gate,
    /// The connection of the open local transaction, with CDC on. One is
    /// enough: one local transaction is open at a time.
    local: Mutex<Arc<Connection>>,
    /// Replay connections have CDC off, so applying records nothing.
    tape: Arc<DatabaseTape>,
    applied_index: Mutex<u64>,
    applied_changed: Condvar,
}

impl NodeDb {
    /// Opens or creates the database file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<NodeDb, Error> {
        let path = path.as_ref().to_str().expect("database path is UTF-8");
        let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new()?);
        let db = Database::open(io.clone(), path, OpenOptions::new(Arc::new(SqliteDialect)))?;
        let conn = db.connect(ConnectOpts::new())?;
        let mode = query(&conn, "PRAGMA journal_mode", &[])?;
        assert_eq!(
            mode[0][0],
            Value::build_text("wal"),
            "change sets are applied to databases in WAL mode only"
        );
        conn.execute(format!(
            "CREATE TABLE IF NOT EXISTS {APPLIED_TABLE}(\
             id INTEGER PRIMARY KEY CHECK (id = 0), log_index INTEGER NOT NULL, raft_meta BLOB NOT NULL, \
             schema_epoch INTEGER NOT NULL, last_rejected INTEGER NOT NULL)"
        ))?;
        // Turning CDC on creates `turso_cdc` and `turso_cdc_version` in a
        // transaction of its own. Doing it at open gives every node the same
        // tables; captured rows are always rolled back, so they stay empty.
        let capture = db.connect(ConnectOpts::new())?;
        capture.execute("PRAGMA capture_data_changes_conn('full')")?;
        capture.set_busy_timeout(BUSY_TIMEOUT);
        capture.set_foreign_keys_enabled(true);
        let local = db.connect(ConnectOpts::new())?;
        local.execute("PRAGMA capture_data_changes_conn('full')")?;
        local.set_busy_timeout(BUSY_TIMEOUT);
        local.set_foreign_keys_enabled(true);
        let tape = Arc::new(DatabaseTape::new_with_opts(
            db.clone(),
            DatabaseTapeOpts {
                cdc_table: None,
                cdc_mode: Some("off".to_string()),
                disable_auto_checkpoint: false,
            },
        ));
        let applied_index = read_applied(&conn)?.map_or(0, |a| a.index);
        Ok(NodeDb {
            shared: Arc::new(Shared {
                io,
                db,
                write: Mutex::new(capture),
                gate: local::Gate::new(),
                local: Mutex::new(local),
                tape,
                applied_index: Mutex::new(applied_index),
                applied_changed: Condvar::new(),
            }),
        })
    }

    /// Runs `statements` as one transaction, returns their results and the
    /// encoded change set, and rolls the transaction back, so the database is
    /// unchanged. The change set records the applied index and the schema
    /// epoch the transaction read. CDC records nothing for CREATE and DROP of
    /// triggers and views; capture adds their `sqlite_schema` changes itself.
    ///
    /// A failing statement rolls the transaction back and returns
    /// [`Error::Statement`]. Statements that control the transaction (BEGIN,
    /// COMMIT, ROLLBACK, SAVEPOINT, RELEASE) or contain more than one statement
    /// are refused before anything runs.
    pub fn capture(&self, statements: &[Statement]) -> Result<Captured, Error> {
        self.capture_batch(&[statements.to_vec()])?
            .pop()
            .expect("one result per transaction")
    }

    /// Runs `transactions` one after another as [`NodeDb::capture`] does, but
    /// each sees the writes of the ones before it, and rolls all of them back
    /// at the end, so the database is unchanged. Returns one result per
    /// transaction. Proposed in order, the change sets apply with every
    /// conflict check passing, as long as no other change set comes between.
    /// A change set that read an earlier one of the batch has
    /// [`ChangeSet::reads_pending`] set, so it is rejected if that one is.
    ///
    /// A transaction that fails is undone alone and returns its error. A
    /// failure that ends the whole database transaction, as `INSERT OR
    /// ROLLBACK` and `RAISE(ROLLBACK)` do, also undoes the transactions before
    /// it: they keep their results and change sets, but the transactions after
    /// it run on the state at the start of the batch.
    ///
    /// The outer error is a failure of the batch as a whole, such as a busy
    /// database.
    pub fn capture_batch(
        &self,
        transactions: &[Vec<Statement>],
    ) -> Result<Vec<Result<Captured, Error>>, Error> {
        let conn = self.shared.lock_writer()?;
        conn.execute("BEGIN IMMEDIATE")?;
        let result = capture_in_transaction(&conn, transactions, false);
        if !conn.get_auto_commit() {
            conn.execute("ROLLBACK")?;
        }
        result.map(|(results, _)| results)
    }

    /// The state machine that applies committed entries to this file. Use one
    /// per [`NodeDb`].
    pub fn state_machine(&self) -> TursoStateMachine {
        TursoStateMachine {
            shared: self.shared.clone(),
            check: true,
        }
    }

    /// A new connection for local reads. It must not write: a write would
    /// change this node's file outside the log. Each read transaction sees the
    /// entries applied when it started. After a snapshot is installed, the
    /// connection stays usable; a read transaction open across the install
    /// keeps its old view until it ends.
    pub fn connect(&self) -> Result<Arc<Connection>, Error> {
        Ok(self.shared.db.connect(ConnectOpts::new())?)
    }

    /// The index of the last applied entry or installed snapshot, 0 before any.
    pub fn applied_index(&self) -> u64 {
        *self.shared.applied_index.lock().unwrap()
    }

    /// Waits until the entry at `index` is applied, at most `timeout`. Returns
    /// whether it was. A read transaction started after `true` sees the entry.
    pub fn wait_for_applied(&self, index: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut applied = self.shared.applied_index.lock().unwrap();
        while *applied < index {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            applied = self
                .shared
                .applied_changed
                .wait_timeout(applied, left)
                .unwrap()
                .0;
        }
        true
    }
}

/// A second database file for a leader that captures the next group of
/// transactions before the previous groups are applied.
///
/// [`AheadDb::reset`] makes it a copy of the node's own file. Each
/// [`AheadDb::capture_batch`] then runs its transactions for real and keeps
/// their effects here, so the next group sees them although the node's own
/// file does not have them yet. Every change set captured here records the
/// copied position as its base and sets [`ChangeSet::reads_pending`]. As long
/// as every change set captured since the reset is proposed in capture order,
/// with no other change set between, and each one is applied, this file holds
/// what the node's own file will hold. When one is rejected or lost, the
/// change sets captured after it are rejected too, and the caller resets this
/// file once its proposals are all decided.
///
/// Nothing reads this file but the capture, and a crash loses nothing that a
/// reset does not rebuild, so its commits are not synced to disk.
#[derive(Clone)]
pub struct AheadDb {
    db: NodeDb,
}

impl AheadDb {
    /// Opens or creates the file at `path`. Call [`AheadDb::reset`] before
    /// the first capture.
    pub fn open(path: impl AsRef<Path>) -> Result<AheadDb, Error> {
        let db = NodeDb::open(path)?;
        db.shared
            .write
            .lock()
            .unwrap()
            .set_sync_mode(turso_core::SyncMode::Off);
        Ok(AheadDb { db })
    }

    /// Replaces this file with a copy of `from`. Returns `from`'s applied
    /// index, the base of every change set captured here until the next reset.
    pub fn reset(&self, from: &NodeDb) -> Result<u64, Error> {
        let snapshot = from.state_machine().snapshot()?;
        self.db.state_machine().install_snapshot(&snapshot)?;
        Ok(snapshot.applied.index)
    }

    /// Runs `transactions` as [`NodeDb::capture_batch`] does, on top of every
    /// batch captured here since the last reset, and keeps their effects.
    ///
    /// A failure that ends the whole database transaction (`INSERT OR
    /// ROLLBACK`) undoes the transactions of this batch before it. Unlike
    /// [`NodeDb::capture_batch`], they then return [`Error::Undone`], so that
    /// this file keeps the effect of every change set it returned.
    pub fn capture_batch(
        &self,
        transactions: &[Vec<Statement>],
    ) -> Result<Vec<Result<Captured, Error>>, Error> {
        let conn = self.db.shared.lock_writer()?;
        conn.execute("BEGIN IMMEDIATE")?;
        let result = capture_in_transaction(&conn, transactions, true).and_then(|r| {
            conn.execute("COMMIT")?;
            Ok(r)
        });
        if result.is_err() && !conn.get_auto_commit() {
            conn.execute("ROLLBACK")?;
        }
        let (mut results, ended) = result?;
        if let Some(by) = ended {
            for r in &mut results[..by] {
                if r.is_ok() {
                    *r = Err(Error::Undone { by });
                }
            }
        }
        Ok(results)
    }

    /// Checks change sets captured elsewhere against this file, in order,
    /// as apply does, and keeps the effect of each one that passes, so later
    /// captures here see it. Returns one outcome per change set. The applied
    /// position of this file, the base of the change sets captured here,
    /// stays as it is.
    pub fn apply_incoming(&self, change_sets: &[Vec<u8>]) -> Result<Vec<Outcome>, Error> {
        let write = self.db.shared.lock_writer()?;
        let applied = read_applied(&write)?.unwrap_or(Applied {
            index: 0,
            raft_meta: Vec::new(),
        });
        let entry = Entry {
            index: applied.index,
            raft_meta: applied.raft_meta,
            proposals: change_sets.to_vec(),
        };
        self.db.shared.apply_locked(&entry, true, false)
    }

    /// A connection to this file, for checks. It must not write.
    pub fn connect(&self) -> Result<Arc<Connection>, Error> {
        self.db.connect()
    }
}

fn check_allowed(index: usize, sql: &str) -> Result<(), Error> {
    use turso_parser::ast::{Cmd, Stmt};
    let not_allowed = |reason: String| Error::NotAllowed { index, reason };
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let cmd = match parser.next() {
        Some(Ok(cmd)) => cmd,
        Some(Err(e)) => {
            return Err(Error::Statement {
                index,
                source: LimboError::ParseError(e.to_string()),
            })
        }
        None => return Err(not_allowed("no statement".to_string())),
    };
    if parser.next().is_some() {
        return Err(not_allowed("more than one statement".to_string()));
    }
    if let Cmd::Stmt(
        Stmt::Begin { .. }
        | Stmt::Commit { .. }
        | Stmt::Rollback { .. }
        | Stmt::Savepoint { .. }
        | Stmt::Release { .. },
    ) = cmd
    {
        return Err(not_allowed("it controls the transaction".to_string()));
    }
    Ok(())
}

/// Runs `transactions` inside the open database transaction. With `ahead`,
/// every change set reads pending ones (the file holds them); otherwise only
/// those after an earlier change set of the batch that changed something.
/// Also returns the last transaction whose failure ended the database
/// transaction, which undid the transactions before it.
fn capture_in_transaction(
    conn: &Arc<Connection>,
    transactions: &[Vec<Statement>],
    ahead: bool,
) -> Result<(BatchResults, Option<usize>), Error> {
    let base_index = read_applied(conn)?.map_or(0, |a| a.index);
    let base_epoch = read_schema_epoch(conn)?;
    let mut epoch = base_epoch;
    let mut read_earlier = false;
    let mut ended = None;
    let mut results = Vec::with_capacity(transactions.len());
    for statements in transactions {
        let allowed = statements
            .iter()
            .enumerate()
            .try_for_each(|(index, s)| check_allowed(index, &s.sql));
        if let Err(e) = allowed {
            results.push(Err(e));
            continue;
        }
        conn.execute("SAVEPOINT capture")?;
        match run_and_read_changes(conn, statements) {
            Ok((statement_results, changes)) => {
                conn.execute("RELEASE capture")?;
                let change_set = ChangeSet {
                    base_index,
                    schema_epoch: epoch,
                    reads_pending: ahead || read_earlier,
                    changes,
                };
                if change_set.changes_schema() {
                    epoch += 1;
                }
                read_earlier |= !change_set.changes.is_empty();
                results.push(Ok(Captured {
                    results: statement_results,
                    change_set: change_set.encode(),
                }));
            }
            Err(e) => {
                if conn.get_auto_commit() {
                    // The failure ended the database transaction.
                    conn.execute("BEGIN IMMEDIATE")?;
                    epoch = base_epoch;
                    read_earlier = false;
                    ended = Some(results.len());
                } else {
                    conn.execute("ROLLBACK TO capture")?;
                    conn.execute("RELEASE capture")?;
                }
                results.push(Err(e));
            }
        }
    }
    if ahead {
        // Keep the effects, but not CDC's records of them, and record the
        // schema changes as apply would.
        if epoch != base_epoch {
            conn.execute(format!(
                "UPDATE {APPLIED_TABLE} SET schema_epoch = {epoch} WHERE id = 0"
            ))?;
        }
        conn.execute("DELETE FROM turso_cdc")?;
    }
    Ok((results, ended))
}

/// Runs one transaction's statements inside the open database transaction
/// and returns their results and the row changes they made.
fn run_and_read_changes(
    conn: &Arc<Connection>,
    statements: &[Statement],
) -> Result<(Vec<StatementResult>, Vec<RowChange>), Error> {
    let start_change_id = max_change_id(conn)?;
    let mut results = Vec::with_capacity(statements.len());
    // (change_id it follows, record) for schema changes CDC does not record.
    let mut schema_records = Vec::new();
    for (index, statement) in statements.iter().enumerate() {
        results.push(run_statement(conn, index, statement, &mut schema_records)?);
    }
    let changes = read_changes(conn, start_change_id, schema_records)?;
    Ok((results, changes))
}

/// Runs one statement inside the open database transaction. Adds the
/// schema changes CDC does not record to `schema_records`, each with the
/// change_id it follows.
fn run_statement(
    conn: &Arc<Connection>,
    index: usize,
    statement: &Statement,
    schema_records: &mut Vec<(i64, RowChange)>,
) -> Result<StatementResult, Error> {
    let failed = |source| Error::Statement { index, source };
    let schema_before = if changes_trigger_or_view(&statement.sql) {
        Some(triggers_and_views(conn)?)
    } else {
        None
    };
    let mut stmt = conn.prepare(&statement.sql).map_err(failed)?;
    for (i, value) in statement.params.iter().enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), value.clone())
            .map_err(failed)?;
    }
    let columns = (0..stmt.num_columns())
        .map(|i| stmt.get_column_name(i).into_owned())
        .collect();
    let rows = stmt.run_collect_rows().map_err(failed)?;
    let result = StatementResult {
        columns,
        rows,
        rows_changed: stmt.n_change() as u64,
    };
    drop(stmt);
    if let Some(before) = schema_before {
        let after = triggers_and_views(conn)?;
        let last_change_id = max_change_id(conn)?;
        for change in schema_diff(&before, &after)? {
            schema_records.push((last_change_id, change));
        }
    }
    Ok(result)
}

/// The row changes the open database transaction made after
/// `start_change_id`, with `schema_records` in their places.
fn read_changes(
    conn: &Arc<Connection>,
    start_change_id: i64,
    schema_records: Vec<(i64, RowChange)>,
) -> Result<Vec<RowChange>, Error> {
    let cdc_rows = query(
        conn,
        "SELECT change_id, change_type, table_name, id, before, after, updates FROM turso_cdc \
         WHERE change_id > ? ORDER BY change_id",
        &[Value::from_i64(start_change_id)],
    )?;
    let mut changes: Vec<(i64, RowChange)> = cdc_rows
        .into_iter()
        .map(|row| {
            let kind = match int(&row[1]) {
                1 => ChangeKind::Insert,
                0 => ChangeKind::Update,
                -1 => ChangeKind::Delete,
                t => unreachable!("change_type {t} before COMMIT"),
            };
            let Value::Text(table) = &row[2] else {
                unreachable!("table_name is text: {:?}", row[2])
            };
            let mut change = RowChange {
                kind,
                table: table.as_str().to_string(),
                id: int(&row[3]),
                before: blob(&row[4]),
                after: blob(&row[5]),
                updates: blob(&row[6]),
            };
            // An UPDATE that changes an INTEGER PRIMARY KEY is recorded as a
            // delete and an insert, and the insert's row image lands in
            // `before` (`core/translate/emitter/update.rs` passes it in the
            // before-record position).
            if kind == ChangeKind::Insert && change.after.is_none() {
                change.after = change.before.take();
            }
            (int(&row[0]), change)
        })
        .collect();
    // A schema record goes after the CDC records of its own statement; the
    // sort is stable, so records of one statement keep their order.
    changes.extend(schema_records);
    changes.sort_by_key(|(id, _)| *id);
    Ok(changes.into_iter().map(|(_, c)| c).collect())
}

fn max_change_id(conn: &Arc<Connection>) -> Result<i64, Error> {
    Ok(int(&query(
        conn,
        "SELECT coalesce(max(change_id), 0) FROM turso_cdc",
        &[],
    )?[0][0]))
}

/// CDC records no change of `sqlite_schema` for triggers and views
/// (`core/translate/trigger.rs`, `core/translate/view.rs`), so capture
/// compares their schema rows around statements that may change them.
fn changes_trigger_or_view(sql: &str) -> bool {
    use turso_parser::ast::{Cmd, Stmt};
    matches!(
        turso_parser::parser::Parser::new(sql.as_bytes()).next(),
        Some(Ok(Cmd::Stmt(
            Stmt::CreateTrigger { .. }
                | Stmt::DropTrigger { .. }
                | Stmt::CreateView { .. }
                | Stmt::DropView { .. }
        )))
    )
}

/// The `sqlite_schema` rows of triggers and views: rowid, then the five columns.
fn triggers_and_views(conn: &Arc<Connection>) -> Result<Vec<Vec<Value>>, Error> {
    query(
        conn,
        "SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema \
         WHERE type IN ('trigger', 'view') ORDER BY rowid",
        &[],
    )
}

/// The `sqlite_schema` inserts and deletes that turn `before` into `after`.
fn schema_diff(before: &[Vec<Value>], after: &[Vec<Value>]) -> Result<Vec<RowChange>, Error> {
    let record = |row: &Vec<Value>| -> Result<Vec<u8>, Error> {
        Ok(
            turso_core::types::ImmutableRecord::from_values(&row[1..], 5)?
                .into_payload()
                .to_vec(),
        )
    };
    let mut changes = Vec::new();
    for row in before.iter().filter(|r| !after.contains(r)) {
        changes.push(RowChange {
            kind: ChangeKind::Delete,
            table: "sqlite_schema".to_string(),
            id: int(&row[0]),
            before: Some(record(row)?),
            after: None,
            updates: None,
        });
    }
    for row in after.iter().filter(|r| !before.contains(r)) {
        changes.push(RowChange {
            kind: ChangeKind::Insert,
            table: "sqlite_schema".to_string(),
            id: int(&row[0]),
            before: None,
            after: Some(record(row)?),
            updates: None,
        });
    }
    Ok(changes)
}

/// [`StateMachine`] over a [`NodeDb`]. `apply` rejects a change set that
/// lost a write conflict; see the `apply` module for the rule.
pub struct TursoStateMachine {
    shared: Arc<Shared>,
    check: bool,
}

impl StateMachine for TursoStateMachine {
    type Error = Error;

    fn applied(&mut self) -> Result<Option<Applied>, Error> {
        read_applied(&self.shared.db.connect(ConnectOpts::new())?)
    }

    fn apply(&mut self, entry: &Entry) -> Result<Vec<Outcome>, Error> {
        let _write = self.shared.lock_writer()?;
        if entry.index <= *self.shared.applied_index.lock().unwrap() {
            return Ok(Vec::new());
        }
        let outcomes = self.shared.apply_locked(entry, self.check, true)?;
        self.set_applied_index(entry.index);
        Ok(outcomes)
    }

    fn snapshot(&mut self) -> Result<Snapshot, Error> {
        let _write = self.shared.write.lock().unwrap();
        let conn = self.shared.db.connect(ConnectOpts::new())?;
        conn.execute("BEGIN")?;
        // The first read starts the read transaction; the pages below are
        // read at the same WAL position.
        let applied = read_applied(&conn)?.unwrap_or(Applied {
            index: 0,
            raft_meta: Vec::new(),
        });
        let page_size = int(&query(&conn, "PRAGMA page_size", &[])?[0][0]) as usize;
        let page_count = int(&query(&conn, "PRAGMA page_count", &[])?[0][0]) as u32;
        let mut data = vec![0u8; page_size * page_count as usize];
        for (i, page) in data.chunks_mut(page_size).enumerate() {
            let found = conn.try_wal_watermark_read_page(i as u32 + 1, page, None)?;
            assert!(found, "page {} of {page_count} exists", i + 1);
        }
        conn.execute("COMMIT")?;
        Ok(Snapshot { applied, data })
    }

    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        let _write = self.shared.lock_writer()?;
        let data = &snapshot.data;
        if data.len() < 100 {
            return Err(Error::Snapshot(
                "shorter than a database header".to_string(),
            ));
        }
        let page_size = match u16::from_be_bytes([data[16], data[17]]) {
            1 => 65536,
            n => n as usize,
        };
        if data.len() % page_size != 0 {
            return Err(Error::Snapshot(format!(
                "{} bytes is not a whole number of {page_size}-byte pages",
                data.len()
            )));
        }
        // Connections notice a new schema by a changed schema cookie. Give the
        // installed file a cookie no connection has seen, so every connection
        // re-reads the schema even if the two files had equal cookies.
        let conn = self.shared.db.connect(ConnectOpts::new())?;
        let local_cookie = int(&query(&conn, "PRAGMA schema_version", &[])?[0][0]) as u32;
        let snapshot_cookie = u32::from_be_bytes(data[40..44].try_into().unwrap());
        let mut page_1 = data[..page_size].to_vec();
        page_1[40..44].copy_from_slice(&(local_cookie.max(snapshot_cookie) + 1).to_be_bytes());

        let tape = self.shared.tape.clone();
        drive(&self.shared.io, |coro| async move {
            // All pages go into the WAL as one transaction; its commit frame
            // sets the database size, which also drops any later local pages.
            let mut session = tape.start_wal_session(&coro).await?;
            for (i, page) in data.chunks(page_size).enumerate() {
                session.append_page(i as u32 + 1, if i == 0 { &page_1 } else { page })?;
            }
            session.commit((data.len() / page_size) as u32)?;
            session.wal_session.end(true)?;
            Ok(())
        })?;

        let installed = read_applied(&conn)?;
        assert_eq!(
            installed.as_ref().map_or(0, |a| a.index),
            snapshot.applied.index,
            "the snapshot's data holds its applied position"
        );
        self.set_applied_index(snapshot.applied.index);
        Ok(())
    }
}

impl Shared {
    /// Applies `entry` with the caller holding the write lock. Without
    /// `durable`, the commit is not synced to disk.
    fn apply_locked(
        &self,
        entry: &Entry,
        check: bool,
        durable: bool,
    ) -> Result<Vec<Outcome>, Error> {
        let tape = self.tape.clone();
        let mut session_conn = None;
        let result = drive(&self.io, |coro| {
            apply::apply_entry(coro, tape, entry, check, durable, &mut session_conn)
        });
        if result.is_err() {
            if let Some(conn) = session_conn.filter(|c| !c.get_auto_commit()) {
                conn.execute("ROLLBACK")?;
            }
        }
        result
    }
}

impl TursoStateMachine {
    /// Turns the write-conflict check off or on (it is on by default), for
    /// measuring what it costs. Every node of a cluster must use the same
    /// setting, or they reach different outcomes.
    pub fn set_conflict_check(&mut self, on: bool) {
        self.check = on;
    }

    fn set_applied_index(&self, index: u64) {
        *self.shared.applied_index.lock().unwrap() = index;
        self.shared.applied_changed.notify_all();
    }
}

/// Runs a sync-engine coroutine to completion, stepping Turso's IO whenever it
/// waits.
fn drive<T, F, Fut>(io: &Arc<dyn IO>, body: F) -> Result<T, Error>
where
    F: FnOnce(Coro<()>) -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut gen = genawaiter::sync::Gen::new(|co| body(co.into()));
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(_) => io.step()?,
            genawaiter::GeneratorState::Complete(result) => return result,
        }
    }
}

fn read_applied(conn: &Arc<Connection>) -> Result<Option<Applied>, Error> {
    let rows = query(
        conn,
        &format!("SELECT log_index, raft_meta FROM {APPLIED_TABLE} WHERE id = 0"),
        &[],
    )?;
    Ok(rows.first().map(|row| Applied {
        index: int(&row[0]) as u64,
        raft_meta: blob(&row[1]).unwrap_or_default(),
    }))
}

fn read_schema_epoch(conn: &Arc<Connection>) -> Result<u64, Error> {
    Ok(read_counters(conn)?.0)
}

/// The schema epoch and `last_rejected` of [`APPLIED_TABLE`].
fn read_counters(conn: &Arc<Connection>) -> Result<(u64, u64), Error> {
    let rows = query(
        conn,
        &format!("SELECT schema_epoch, last_rejected FROM {APPLIED_TABLE} WHERE id = 0"),
        &[],
    )?;
    Ok(rows
        .first()
        .map_or((0, 0), |row| (int(&row[0]) as u64, int(&row[1]) as u64)))
}

fn query(conn: &Arc<Connection>, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error> {
    let mut stmt = conn.prepare(sql)?;
    for (i, value) in params.iter().enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), value.clone())?;
    }
    Ok(stmt.run_collect_rows()?)
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Numeric(Numeric::Integer(n)) => *n,
        v => unreachable!("expected an integer, got {v:?}"),
    }
}

fn blob(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Blob(b) => Some(b.to_vec()),
        Value::Null => None,
        v => unreachable!("expected a blob, got {v:?}"),
    }
}
