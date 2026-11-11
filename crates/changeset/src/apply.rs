//! Applies committed entries: every change set of an entry in one
//! transaction, each in a savepoint, checked for write conflicts first.
//!
//! The check gives snapshot isolation by rows. A change set is rejected when
//! a change set applied after its base wrote a row it also wrote:
//!
//! - an update or delete finds the row changed or gone (its before-image no
//!   longer equals the current row);
//! - an insert finds a row with the same key already there;
//! - any change set at an older schema epoch, because rows captured before a
//!   schema change may not fit the current tables.
//!
//! Each row is checked once per change set, the first time the change set
//! touches it; later records of the same row follow from the earlier ones.
//! The rows of a run of row records between two schema records are all
//! checked before any of them is written, so that the writes a foreign-key
//! action makes (ON DELETE CASCADE and so on) do not look like conflicts to
//! the records that captured those same writes.
//!
//! Row records are written with plain INSERT, UPDATE and DELETE statements,
//! not with the sync engine's replay, whose upserts and `INSERT OR REPLACE`
//! would overwrite a conflicting row instead of failing. Schema records go
//! through the sync engine's `DatabaseReplaySession`.

use std::{
    collections::{hash_map, HashMap, HashSet},
    num::NonZero,
    sync::Arc,
};

use turso_core::{Connection, LimboError, Numeric, Value};
use turso_sync_engine::{
    database_tape::{DatabaseReplaySession, DatabaseReplaySessionOpts, DatabaseTape},
    types::{parse_bin_record, Coro, DatabaseChange, DatabaseChangeType, DatabaseTapeOperation},
};
use tursoraft_statemachine::{Entry, Outcome};

use crate::{query, ChangeKind, ChangeSet, Error, RowChange, APPLIED_TABLE};

const SQLITE_SCHEMA: &str = "sqlite_schema";

/// Applies `entry` and records it as applied, with the new schema epoch, in
/// the same transaction. `session_conn` receives the transaction's connection
/// so that the caller can roll back after an error.
pub(crate) async fn apply_entry(
    coro: Coro<()>,
    tape: Arc<DatabaseTape>,
    entry: &Entry,
    check: bool,
    session_conn: &mut Option<Arc<Connection>>,
) -> Result<Vec<Outcome>, Error> {
    let opts = DatabaseReplaySessionOpts {
        use_implicit_rowid: true,
    };
    let mut session = tape.start_replay_session(&coro, opts).await?;
    let conn = session.conn();
    *session_conn = Some(conn.clone());
    // Foreign keys are checked as they were at capture, so a proposal whose
    // parent row an earlier proposal deleted is rejected.
    conn.set_foreign_keys_enabled(true);
    let mut epoch = crate::read_schema_epoch(&conn)?;
    let mut applier = Applier {
        conn: conn.clone(),
        check,
        tables: HashMap::new(),
        statements: HashMap::new(),
    };
    let mut outcomes = Vec::with_capacity(entry.proposals.len());
    for proposal in &entry.proposals {
        let change_set = match ChangeSet::decode(proposal) {
            Ok(change_set) => change_set,
            Err(e) => {
                outcomes.push(Outcome::Rejected {
                    reason: e.to_string(),
                });
                continue;
            }
        };
        if check && change_set.schema_epoch != epoch {
            outcomes.push(Outcome::Rejected {
                reason: format!(
                    "schema changed after the transaction read it: captured at schema epoch {}, \
                     the database is at schema epoch {epoch}",
                    change_set.schema_epoch
                ),
            });
            continue;
        }
        conn.execute("SAVEPOINT proposal")?;
        match applier
            .apply_change_set(&coro, &mut session, &change_set)
            .await
        {
            Ok(()) => {
                if change_set.changes_schema() {
                    epoch += 1;
                }
                outcomes.push(Outcome::Applied);
            }
            Err(e) if is_rejection(&e) => {
                applier.forget();
                conn.execute("ROLLBACK TO proposal")?;
                outcomes.push(Outcome::Rejected {
                    reason: e.to_string(),
                });
            }
            Err(e) => return Err(e),
        }
        conn.execute("RELEASE proposal")?;
    }
    applier.forget();
    let mut stmt = conn.prepare(format!(
        "INSERT OR REPLACE INTO {APPLIED_TABLE}(id, log_index, raft_meta, schema_epoch) \
         VALUES (0, ?, ?, ?)"
    ))?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::from_i64(entry.index as i64),
    )?;
    stmt.bind_at(
        NonZero::new(2).unwrap(),
        Value::from_slice(&entry.raft_meta).expect("allocate raft_meta"),
    )?;
    stmt.bind_at(NonZero::new(3).unwrap(), Value::from_i64(epoch as i64))?;
    stmt.run_ignore_rows()?;
    drop(stmt);
    session.replay(&coro, DatabaseTapeOperation::Commit).await?;
    Ok(outcomes)
}

/// Whether an error means the proposal cannot be applied to this state, which
/// every node decides alike. Other errors, such as I/O errors, stop the node
/// instead: a node that rejected what the others applied would diverge.
fn is_rejection(e: &Error) -> bool {
    use turso_sync_engine::errors::Error as E;
    let limbo = |e: &LimboError| {
        matches!(
            e,
            LimboError::Constraint(_)
                | LimboError::ConstraintViolation(_)
                | LimboError::ForeignKeyConstraint(_)
                | LimboError::Raise(..)
                | LimboError::SqlError(_)
                | LimboError::ParseError(_)
                | LimboError::LexerError(_)
                | LimboError::PlanningError(_)
                | LimboError::ConversionError(_)
                | LimboError::InvalidArgument(_)
                | LimboError::TooBig
                | LimboError::IntegerOverflow
        )
    };
    match e {
        Error::Rejected(_) => true,
        Error::Database(e) => limbo(e),
        Error::Replay(E::TursoError(e)) => limbo(e),
        // The replay code's own checks, and DDL failures it wraps as text.
        Error::Replay(E::DatabaseTapeError(message)) => {
            !["database is busy", "interrupted", "I/O", "Corrupt"]
                .iter()
                .any(|s| message.contains(s))
        }
        _ => false,
    }
}

/// Writes row records on the apply transaction's connection.
struct Applier {
    conn: Arc<Connection>,
    check: bool,
    tables: HashMap<String, Arc<Table>>,
    statements: HashMap<String, turso_core::Statement>,
}

/// What the applier needs to know about a table.
struct Table {
    name: String,
    columns: Vec<String>,
    /// The columns of a PRIMARY KEY that is not the rowid, in key order.
    /// Empty when the rowid identifies the row.
    key: Vec<usize>,
    /// The INTEGER PRIMARY KEY column, which is the rowid.
    rowid_column: Option<usize>,
    /// The name of the rowid in SQL: `rowid`, `_rowid_` or `oid`, whichever
    /// no column shadows.
    rowid_name: &'static str,
}

/// One row record with its images parsed.
struct Row<'a> {
    change: &'a RowChange,
    before: Option<Vec<Value>>,
    after: Option<Vec<Value>>,
    updates: Option<Vec<Value>>,
}

impl Applier {
    async fn apply_change_set(
        &mut self,
        coro: &Coro<()>,
        session: &mut DatabaseReplaySession,
        change_set: &ChangeSet,
    ) -> Result<(), Error> {
        let changes = &change_set.changes;
        // (table, row identity) of the rows already checked.
        let mut touched = HashSet::new();
        let mut start = 0;
        while start < changes.len() {
            if changes[start].table == SQLITE_SCHEMA {
                self.forget();
                let change = schema_change(&changes[start]).into_apply()?;
                session
                    .replay(coro, DatabaseTapeOperation::RowChange(change))
                    .await?;
                self.forget();
                start += 1;
                continue;
            }
            let end = changes[start..]
                .iter()
                .position(|c| c.table == SQLITE_SCHEMA)
                .map_or(changes.len(), |n| start + n);
            let rows = changes[start..end]
                .iter()
                .map(Row::parse)
                .collect::<Result<Vec<_>, Error>>()?;
            if self.check {
                for row in &rows {
                    self.check_row(row, &mut touched)?;
                }
            }
            for row in &rows {
                self.write_row(row)?;
            }
            start = end;
        }
        Ok(())
    }

    /// Fails with a write conflict when the row this record changes is not
    /// in the state the transaction read.
    fn check_row(
        &mut self,
        row: &Row,
        touched: &mut HashSet<(String, String)>,
    ) -> Result<(), Error> {
        let table = self.table(&row.change.table)?;
        let image = match row.change.kind {
            ChangeKind::Insert => row.after()?,
            ChangeKind::Update | ChangeKind::Delete => row.before()?,
        };
        let (key_sql, key_values) = table.identity(row.change.id, image)?;
        let key_text = table.describe_key(row.change.id, image);
        let first_touch = touched.insert((table.name.clone(), key_text.clone()));
        if row.change.kind == ChangeKind::Update {
            // An update that moves the key: later records of the row under its
            // new key follow from this one.
            touched.insert((
                table.name.clone(),
                table.describe_key(row.change.id, row.after()?),
            ));
        }
        if !first_touch {
            return Ok(());
        }
        let columns = table.quoted_columns(table.columns.len());
        let current = self.run(
            format!(
                "SELECT {} FROM {} WHERE {key_sql}",
                columns.join(", "),
                quote(&table.name)
            ),
            key_values,
        )?;
        let conflict = |what: &str| {
            Err(Error::Rejected(format!(
                "write conflict on {} {key_text}: {what}",
                table.name
            )))
        };
        match (row.change.kind, current.first()) {
            (ChangeKind::Insert, None) => Ok(()),
            (ChangeKind::Insert, Some(_)) => conflict(
                "another transaction inserted a row with this key after this one read the table",
            ),
            (_, None) => conflict("another transaction deleted the row after this one read it"),
            (_, Some(current)) if !same_row(current, image) => {
                conflict("another transaction changed the row after this one read it")
            }
            _ => Ok(()),
        }
    }

    fn write_row(&mut self, row: &Row) -> Result<(), Error> {
        let table = self.table(&row.change.table)?;
        let name = quote(&table.name);
        match row.change.kind {
            ChangeKind::Insert => {
                let after = row.after()?;
                let mut columns = table.quoted_columns(after.len());
                let mut values = after.to_vec();
                if table.key.is_empty() && table.rowid_column.is_none() {
                    columns.push(table.rowid_name.to_string());
                    values.push(Value::from_i64(row.change.id));
                }
                let marks = vec!["?"; values.len()].join(", ");
                self.run(
                    format!(
                        "INSERT INTO {name}({}) VALUES ({marks})",
                        columns.join(", ")
                    ),
                    values,
                )?;
            }
            ChangeKind::Update => {
                let after = row.after()?;
                let changed: Vec<usize> = match &row.updates {
                    // One changed flag per column, then the new values.
                    Some(updates) => updates[..updates.len() / 2]
                        .iter()
                        .enumerate()
                        .filter(|(_, flag)| matches!(flag, Value::Numeric(Numeric::Integer(1))))
                        .map(|(i, _)| i)
                        .collect(),
                    None => (0..after.len()).collect(),
                };
                if changed.is_empty() {
                    return Ok(());
                }
                let columns = table.quoted_columns(after.len());
                let set: Vec<String> = changed
                    .iter()
                    .map(|&i| format!("{} = ?", columns[i]))
                    .collect();
                let (key_sql, key_values) = table.identity(row.change.id, row.before()?)?;
                let mut values: Vec<Value> = changed.iter().map(|&i| after[i].clone()).collect();
                values.extend(key_values);
                self.run(
                    format!("UPDATE {name} SET {} WHERE {key_sql}", set.join(", ")),
                    values,
                )?;
            }
            ChangeKind::Delete => {
                let (key_sql, key_values) = table.identity(row.change.id, row.before()?)?;
                self.run(format!("DELETE FROM {name} WHERE {key_sql}"), key_values)?;
            }
        }
        Ok(())
    }

    fn table(&mut self, name: &str) -> Result<Arc<Table>, Error> {
        if let Some(table) = self.tables.get(name) {
            return Ok(table.clone());
        }
        let info = query(
            &self.conn,
            &format!(
                "SELECT name, type, pk FROM pragma_table_info('{}') ORDER BY cid",
                name.replace('\'', "''")
            ),
            &[],
        )?;
        if info.is_empty() {
            return Err(Error::Rejected(format!("no such table: {name}")));
        }
        let columns: Vec<String> = info.iter().map(|r| text(&r[0])).collect();
        let mut pk: Vec<(i64, usize)> = info
            .iter()
            .enumerate()
            .filter_map(|(i, r)| match r[2] {
                Value::Numeric(Numeric::Integer(n)) if n > 0 => Some((n, i)),
                _ => None,
            })
            .collect();
        pk.sort_unstable();
        let pk: Vec<usize> = pk.into_iter().map(|(_, i)| i).collect();
        // The same rule as the sync engine's replay: a single INTEGER PRIMARY
        // KEY column is the rowid.
        let rowid_column =
            (pk.len() == 1 && text(&info[pk[0]][1]).eq_ignore_ascii_case("INTEGER")).then(|| pk[0]);
        let rowid_name = ["rowid", "_rowid_", "oid"]
            .into_iter()
            .find(|alias| !columns.iter().any(|c| c.eq_ignore_ascii_case(alias)))
            .ok_or_else(|| {
                Error::Rejected(format!("table {name} shadows every name of the rowid"))
            })?;
        let table = Arc::new(Table {
            name: name.to_string(),
            columns,
            key: if rowid_column.is_some() {
                Vec::new()
            } else {
                pk
            },
            rowid_column,
            rowid_name,
        });
        self.tables.insert(name.to_string(), table.clone());
        Ok(table)
    }

    /// Runs `sql` with a cached prepared statement.
    fn run(&mut self, sql: String, values: Vec<Value>) -> Result<Vec<Vec<Value>>, Error> {
        let stmt = match self.statements.entry(sql) {
            hash_map::Entry::Occupied(e) => e.into_mut(),
            hash_map::Entry::Vacant(e) => {
                let stmt = self.conn.prepare(e.key())?;
                e.insert(stmt)
            }
        };
        for (i, value) in values.into_iter().enumerate() {
            stmt.bind_at(NonZero::new(i + 1).unwrap(), value)?;
        }
        let rows = stmt.run_collect_rows();
        stmt.reset_best_effort();
        Ok(rows?)
    }

    /// Drops cached statements and table shapes, which a schema change or a
    /// rollback makes stale.
    fn forget(&mut self) {
        self.statements.clear();
        self.tables.clear();
    }
}

impl Table {
    /// The WHERE clause that finds the row, and its parameters: the key
    /// columns of `image`, or the rowid.
    fn identity(&self, id: i64, image: &[Value]) -> Result<(String, Vec<Value>), Error> {
        if self.key.is_empty() {
            return Ok((
                format!("{} = ?", self.rowid_name),
                vec![Value::from_i64(id)],
            ));
        }
        let mut predicates = Vec::with_capacity(self.key.len());
        let mut values = Vec::with_capacity(self.key.len());
        for &k in &self.key {
            let value = image.get(k).ok_or_else(|| {
                Error::Rejected(format!("row image of {} lacks key column {k}", self.name))
            })?;
            predicates.push(format!("{} IS ?", quote(&self.columns[k])));
            values.push(value.clone());
        }
        Ok((predicates.join(" AND "), values))
    }

    /// The row's identity for messages and for telling rows apart, such as
    /// `rowid 5` or `(email) = ('a@x')`.
    fn describe_key(&self, id: i64, image: &[Value]) -> String {
        if self.key.is_empty() {
            return format!("rowid {id}");
        }
        let names: Vec<&str> = self.key.iter().map(|&k| self.columns[k].as_str()).collect();
        let values: Vec<String> = self
            .key
            .iter()
            .map(|&k| image.get(k).map_or("?".to_string(), literal))
            .collect();
        format!("({}) = ({})", names.join(", "), values.join(", "))
    }

    fn quoted_columns(&self, n: usize) -> Vec<String> {
        self.columns.iter().take(n).map(|c| quote(c)).collect()
    }
}

impl<'a> Row<'a> {
    fn parse(change: &'a RowChange) -> Result<Row<'a>, Error> {
        let parse = |image: &Option<Vec<u8>>| -> Result<Option<Vec<Value>>, Error> {
            image
                .as_ref()
                .map(|bytes| Ok(parse_bin_record(bytes)?.into_iter().collect()))
                .transpose()
        };
        Ok(Row {
            change,
            before: parse(&change.before)?,
            after: parse(&change.after)?,
            updates: parse(&change.updates)?,
        })
    }

    fn before(&self) -> Result<&[Value], Error> {
        self.before.as_deref().ok_or_else(|| self.missing("before"))
    }

    fn after(&self) -> Result<&[Value], Error> {
        self.after.as_deref().ok_or_else(|| self.missing("after"))
    }

    fn missing(&self, image: &str) -> Error {
        Error::Rejected(format!(
            "{:?} of {} rowid {} has no {image} image",
            self.change.kind, self.change.table, self.change.id
        ))
    }
}

/// A `sqlite_schema` record in the sync engine's form, for its replay code.
fn schema_change(change: &RowChange) -> DatabaseChange {
    DatabaseChange {
        change_id: 0,
        change_time: 0,
        change_txn_id: None,
        change_type: match change.kind {
            ChangeKind::Insert => DatabaseChangeType::Insert,
            ChangeKind::Update => DatabaseChangeType::Update,
            ChangeKind::Delete => DatabaseChangeType::Delete,
        },
        table_name: change.table.clone(),
        id: change.id,
        before: change.before.clone(),
        after: change.after.clone(),
        updates: change.updates.clone(),
    }
}

/// Whether the current row equals a row image, value by value and type by
/// type: an integer 2 does not equal a real 2.0.
fn same_row(current: &[Value], image: &[Value]) -> bool {
    current.len() == image.len()
        && current.iter().zip(image).all(|(a, b)| match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Numeric(Numeric::Integer(x)), Value::Numeric(Numeric::Integer(y))) => x == y,
            (Value::Numeric(Numeric::Float(_)), Value::Numeric(Numeric::Float(_)))
            | (Value::Text(_), Value::Text(_))
            | (Value::Blob(_), Value::Blob(_)) => a == b,
            _ => false,
        })
}

fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(n)) => n.to_string(),
        Value::Numeric(Numeric::Float(f)) => format!("{:?}", f64::from(*f)),
        Value::Text(t) => format!("'{}'", t.as_str().replace('\'', "''")),
        Value::Blob(b) => format!(
            "x'{}'",
            b.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
    }
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn text(value: &Value) -> String {
    match value {
        Value::Text(t) => t.as_str().to_string(),
        v => unreachable!("expected text, got {v:?}"),
    }
}
