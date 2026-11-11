//! Local reads of this node's file.
//!
//! A read runs on a connection in query-only mode and must parse as one
//! `SELECT`. Query-only mode alone is not enough: it lets `BEGIN`, `PRAGMA
//! journal_mode = ...`, `PRAGMA wal_checkpoint` and `PRAGMA query_only = 0`
//! through.

use std::num::NonZero;
use std::sync::{Arc, Mutex};

use tursoraft_changeset::turso_core::Connection;
use tursoraft_changeset::NodeDb;

use crate::{Error, Rows, Statement, Value};

#[derive(Clone)]
pub(crate) struct Reads {
    db: NodeDb,
    idle: Arc<Mutex<Vec<Arc<Connection>>>>,
}

impl Reads {
    pub(crate) fn new(db: NodeDb) -> Reads {
        Reads {
            db,
            idle: Arc::default(),
        }
    }

    pub(crate) fn query(&self, statement: &Statement) -> Result<Rows, Error> {
        check_select(&statement.sql)?;
        let conn = match self.idle.lock().unwrap().pop() {
            Some(conn) => conn,
            None => {
                let conn = self.db.connect().map_err(|e| Error::Read(e.to_string()))?;
                conn.set_query_only(true);
                conn
            }
        };
        let result = run(&conn, statement);
        if conn.get_auto_commit() && conn.get_query_only() {
            self.idle.lock().unwrap().push(conn);
        }
        result
    }
}

fn run(conn: &Arc<Connection>, statement: &Statement) -> Result<Rows, Error> {
    let err = |e: tursoraft_changeset::turso_core::LimboError| Error::Read(e.to_string());
    let mut stmt = conn.prepare(&statement.sql).map_err(err)?;
    for (i, value) in statement.params.iter().enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), value.to_turso())
            .map_err(err)?;
    }
    let columns = (0..stmt.num_columns())
        .map(|i| stmt.get_column_name(i).into_owned())
        .collect();
    let rows = stmt.run_collect_rows().map_err(err)?;
    Ok(Rows {
        columns,
        rows: rows
            .iter()
            .map(|row| row.iter().map(Value::from_turso).collect())
            .collect(),
        rows_changed: 0,
    })
}

fn check_select(sql: &str) -> Result<(), Error> {
    use turso_parser::ast::{Cmd, Stmt};
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let cmd = match parser.next() {
        Some(Ok(cmd)) => cmd,
        Some(Err(e)) => return Err(Error::Read(e.to_string())),
        None => return Err(Error::Read("no statement".into())),
    };
    if parser.next().is_some() {
        return Err(Error::Read("more than one statement".into()));
    }
    match cmd {
        Cmd::Stmt(Stmt::Select(_)) => Ok(()),
        _ => Err(Error::Read(
            "only a SELECT can be read; write through execute".into(),
        )),
    }
}
