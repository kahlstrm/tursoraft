//! Local transactions: statements run one at a time on this node's file,
//! with change capture, and end by returning their change set and rolling
//! back, so the file still changes only by applying committed entries.
//!
//! A local transaction holds the database's write lock from its first
//! statement to its end, so that it reads one state, the one at its base
//! index, and so that it can write. Apply, capture and snapshot install
//! write the same file and need the lock too; while a local transaction holds
//! it, they wait. The [`Gate`] decides who holds it: one writer (apply,
//! capture, install) or one local transaction. A waiting writer goes before
//! a local transaction that wants to begin. Once someone has waited for the
//! hold limit while the local transaction held the lock for at least as
//! long, the transaction is rolled back and ends with [`Error::Aborted`].
//! So an apply waits at most the hold limit, and several local transactions
//! on one node run one after another.

use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use turso_core::Connection;

use crate::{
    check_allowed, max_change_id, read_applied, read_changes, read_schema_epoch, run_statement,
    ChangeSet, Error, NodeDb, RowChange, Shared, Statement, StatementResult,
};

const DEFAULT_HOLD_LIMIT: Duration = Duration::from_secs(1);

pub(crate) struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}

struct State {
    owner: Owner,
    /// Writers waiting for the lock. A local transaction does not begin
    /// while one waits.
    writers_waiting: usize,
    hold_limit: Duration,
    last_id: u64,
    /// The last local transaction that was ended, and why.
    ended: Option<(u64, String)>,
}

#[derive(Clone, Copy, PartialEq)]
enum Owner {
    Free,
    Writer,
    Local { id: u64, since: Instant },
}

impl Gate {
    pub(crate) fn new() -> Gate {
        Gate {
            state: Mutex::new(State {
                owner: Owner::Free,
                writers_waiting: 0,
                hold_limit: DEFAULT_HOLD_LIMIT,
                last_id: 0,
                ended: None,
            }),
            changed: Condvar::new(),
        }
    }
}

/// The write connection, held with the database's write lock. Releases the
/// lock when dropped.
pub(crate) struct WriterGuard<'a> {
    conn: MutexGuard<'a, Arc<Connection>>,
    gate: &'a Gate,
}

impl Deref for WriterGuard<'_> {
    type Target = Arc<Connection>;

    fn deref(&self) -> &Arc<Connection> {
        &self.conn
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap();
        assert!(state.owner == Owner::Writer, "the writer holds the gate");
        state.owner = Owner::Free;
        self.gate.changed.notify_all();
    }
}

impl Shared {
    /// The write connection and the write lock, for apply, capture and
    /// install. Waits for an open local transaction, and ends it once it has
    /// made this caller wait for the hold limit.
    pub(crate) fn lock_writer(&self) -> Result<WriterGuard<'_>, Error> {
        let conn = self.write.lock().unwrap();
        self.acquire(None)?;
        Ok(WriterGuard {
            conn,
            gate: &self.gate,
        })
    }

    /// Takes the lock for a writer (`local_until` is `None`), or for a new
    /// local transaction that waits until `local_until` at most. Returns the
    /// local transaction's id.
    fn acquire(&self, local_until: Option<Instant>) -> Result<u64, Error> {
        let local = local_until.is_some();
        let started = Instant::now();
        let mut state = self.gate.state.lock().unwrap();
        if !local {
            state.writers_waiting += 1;
        }
        loop {
            if state.owner == Owner::Free && (!local || state.writers_waiting == 0) {
                if !local {
                    state.writers_waiting -= 1;
                    state.owner = Owner::Writer;
                    return Ok(0);
                }
                state.last_id += 1;
                state.owner = Owner::Local {
                    id: state.last_id,
                    since: Instant::now(),
                };
                return Ok(state.last_id);
            }
            let now = Instant::now();
            let mut wake = local_until;
            if let Owner::Local { id, since } = state.owner {
                let end_at = since.max(started) + state.hold_limit;
                if now >= end_at {
                    let reason = format!(
                        "it held the write lock for {} ms while another writer waited",
                        state.hold_limit.as_millis()
                    );
                    drop(state);
                    let ended = self.end_local(id, reason);
                    state = self.gate.state.lock().unwrap();
                    if let Err(e) = ended {
                        if !local {
                            state.writers_waiting -= 1;
                        }
                        return Err(e);
                    }
                    continue;
                }
                wake = Some(wake.map_or(end_at, |w| w.min(end_at)));
            }
            if local_until.is_some_and(|until| now >= until) {
                return Err(Error::Busy(
                    "another local transaction held the write lock for the whole wait".into(),
                ));
            }
            state = match wake {
                Some(wake) => {
                    self.gate
                        .changed
                        .wait_timeout(state, wake.saturating_duration_since(now))
                        .unwrap()
                        .0
                }
                None => self.gate.changed.wait(state).unwrap(),
            };
        }
    }

    /// Rolls back local transaction `id` if it still holds the lock.
    fn end_local(&self, id: u64, reason: String) -> Result<(), Error> {
        let conn = self.local.lock().unwrap();
        let mut state = self.gate.state.lock().unwrap();
        if !matches!(state.owner, Owner::Local { id: held, .. } if held == id) {
            return Ok(());
        }
        let rolled_back = if conn.get_auto_commit() {
            Ok(())
        } else {
            conn.execute("ROLLBACK")
        };
        state.owner = Owner::Free;
        state.ended = Some((id, reason));
        self.gate.changed.notify_all();
        Ok(rolled_back?)
    }
}

impl NodeDb {
    /// Begins a local transaction on this file. Waits at most `wait` for
    /// another local transaction to end; see the `local` module.
    pub fn begin(&self, wait: Duration) -> Result<LocalTx, Error> {
        let shared = &self.shared;
        let id = shared.acquire(Some(Instant::now() + wait))?;
        let conn = shared.local.lock().unwrap();
        let started = (|| {
            conn.execute("BEGIN IMMEDIATE")?;
            let base_index = read_applied(&conn)?.map_or(0, |a| a.index);
            Ok::<_, Error>((base_index, read_schema_epoch(&conn)?, max_change_id(&conn)?))
        })();
        match started {
            Ok((base_index, schema_epoch, start_change_id)) => Ok(LocalTx {
                shared: shared.clone(),
                id,
                base_index,
                schema_epoch,
                start_change_id,
                schema_records: Vec::new(),
                statements: 0,
                ended: false,
            }),
            Err(e) => {
                if !conn.get_auto_commit() {
                    let _ = conn.execute("ROLLBACK");
                }
                release(shared, id);
                Err(e)
            }
        }
    }

    /// How long a local transaction may hold the write lock while another
    /// writer waits; 1 s by default.
    pub fn set_local_hold_limit(&self, limit: Duration) {
        self.shared.gate.state.lock().unwrap().hold_limit = limit;
    }
}

/// An open local transaction; see [`NodeDb::begin`]. Dropping it rolls it
/// back.
pub struct LocalTx {
    shared: Arc<Shared>,
    id: u64,
    base_index: u64,
    schema_epoch: u64,
    start_change_id: i64,
    /// Schema changes CDC does not record, each with the change_id it follows.
    schema_records: Vec<(i64, RowChange)>,
    statements: usize,
    ended: bool,
}

impl LocalTx {
    /// The applied log index of the state this transaction reads.
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Runs one statement and returns what it returned. A statement that
    /// fails is undone alone and returns its error; the transaction stays
    /// open. A failure that ends the whole transaction (`INSERT OR
    /// ROLLBACK`) ends this one with [`Error::Aborted`]. Statements that
    /// control the transaction are refused.
    pub fn execute(&mut self, statement: &Statement) -> Result<StatementResult, Error> {
        let shared = self.shared.clone();
        let conn = shared.local.lock().unwrap();
        self.check_held()?;
        let index = self.statements;
        check_allowed(index, &statement.sql)?;
        self.statements += 1;
        conn.execute("SAVEPOINT local_statement")?;
        match run_statement(&conn, index, statement, &mut self.schema_records) {
            Ok(result) => {
                conn.execute("RELEASE local_statement")?;
                Ok(result)
            }
            Err(e) if conn.get_auto_commit() => {
                self.ended = true;
                release(&self.shared, self.id);
                Err(Error::Aborted(format!("statement {index} ended it: {e}")))
            }
            Err(e) => {
                conn.execute("ROLLBACK TO local_statement")?;
                conn.execute("RELEASE local_statement")?;
                Err(e)
            }
        }
    }

    /// Ends the transaction: returns its encoded change set, read before the
    /// transaction is rolled back. The change set records the base index and
    /// the schema epoch the transaction read, and does not read pending
    /// change sets: this file holds committed entries only. A transaction
    /// that wrote nothing gives a change set without changes.
    pub fn commit(mut self) -> Result<Vec<u8>, Error> {
        let shared = self.shared.clone();
        let conn = shared.local.lock().unwrap();
        self.check_held()?;
        let schema_records = std::mem::take(&mut self.schema_records);
        let changes = read_changes(&conn, self.start_change_id, schema_records);
        let rolled_back = conn.execute("ROLLBACK");
        self.ended = true;
        release(&self.shared, self.id);
        let changes = changes?;
        rolled_back?;
        Ok(ChangeSet {
            base_index: self.base_index,
            schema_epoch: self.schema_epoch,
            reads_pending: false,
            changes,
        }
        .encode())
    }

    /// Fails with [`Error::Aborted`] if the transaction no longer holds the
    /// lock. The caller holds the local connection.
    fn check_held(&mut self) -> Result<(), Error> {
        let state = self.shared.gate.state.lock().unwrap();
        if matches!(state.owner, Owner::Local { id, .. } if id == self.id) && !self.ended {
            return Ok(());
        }
        self.ended = true;
        let reason = match &state.ended {
            Some((id, reason)) if *id == self.id => reason.clone(),
            _ => "it already ended".to_string(),
        };
        Err(Error::Aborted(reason))
    }
}

impl Drop for LocalTx {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let conn = self.shared.local.lock().unwrap();
        let held = matches!(
            self.shared.gate.state.lock().unwrap().owner,
            Owner::Local { id, .. } if id == self.id
        );
        if held {
            if !conn.get_auto_commit() {
                let _ = conn.execute("ROLLBACK");
            }
            release(&self.shared, self.id);
        }
    }
}

/// Frees the lock if local transaction `id` holds it.
fn release(shared: &Shared, id: u64) {
    let mut state = shared.gate.state.lock().unwrap();
    if matches!(state.owner, Owner::Local { id: held, .. } if held == id) {
        state.owner = Owner::Free;
        shared.gate.changed.notify_all();
    }
}
