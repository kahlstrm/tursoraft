//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::sync::Arc;

use tempfile::TempDir;
use tursoraft_changeset::{
    turso_core::{Connection, Numeric, Value},
    NodeDb, Statement, TursoStateMachine,
};
use tursoraft_statemachine::{Entry, Outcome, StateMachine};

pub struct Node {
    pub db: NodeDb,
    pub sm: TursoStateMachine,
}

pub fn node(dir: &TempDir, name: &str) -> Node {
    let db = NodeDb::open(dir.path().join(name)).unwrap();
    let sm = db.state_machine();
    Node { db, sm }
}

pub fn entry(index: u64, proposals: Vec<Vec<u8>>) -> Entry {
    Entry {
        index,
        raft_meta: format!("meta {index}").into_bytes(),
        proposals,
    }
}

pub fn capture(node: &Node, statements: &[&str]) -> Vec<u8> {
    let statements: Vec<Statement> = statements.iter().map(|s| (*s).into()).collect();
    node.db.capture(&statements).unwrap().change_set
}

/// Applies `entry` on every node and checks that each gives `expected`.
pub fn apply_all(nodes: &mut [&mut Node], entry: &Entry, expected: &[Outcome]) {
    for node in nodes.iter_mut() {
        let outcomes = node.sm.apply(entry).unwrap();
        assert_eq!(outcomes, expected, "entry {}", entry.index);
    }
}

pub fn applied_ok(n: usize) -> Vec<Outcome> {
    vec![Outcome::Applied; n]
}

/// Schema (without root pages) and every row of every table, CDC tables
/// included, plus the result of `PRAGMA integrity_check`.
pub fn dump(conn: &Arc<Connection>) -> String {
    let mut out = String::new();
    let schema = query(
        conn,
        "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name",
    );
    for row in &schema {
        out += &format!("{row:?}\n");
    }
    for row in &schema {
        if row[0] != Value::build_text("table") {
            continue;
        }
        let name = text(&row[1]);
        let sql = text(&row[3]);
        let select = if sql.to_uppercase().contains("WITHOUT ROWID") {
            format!("SELECT * FROM \"{name}\"")
        } else {
            format!("SELECT rowid, * FROM \"{name}\"")
        };
        let mut rows: Vec<String> = query(conn, &select)
            .iter()
            .map(|r| format!("{r:?}"))
            .collect();
        rows.sort();
        out += &format!("{name}: {rows:?}\n");
    }
    out += &format!("integrity: {:?}\n", query(conn, "PRAGMA integrity_check"));
    out
}

pub fn assert_same(a: &Node, b: &Node) {
    let a = dump(&a.db.connect().unwrap());
    let b = dump(&b.db.connect().unwrap());
    eprintln!("--- dump\n{a}");
    assert_eq!(a, b);
}

pub fn query(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

pub fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.as_str().to_string(),
        v => panic!("expected text, got {v:?}"),
    }
}

pub fn count(node: &Node, sql: &str) -> i64 {
    int(&query(&node.db.connect().unwrap(), sql)[0][0])
}

pub fn int(v: &Value) -> i64 {
    match v {
        Value::Numeric(Numeric::Integer(n)) => *n,
        v => panic!("expected integer, got {v:?}"),
    }
}

/// Node A captures; A and B apply every entry and must agree.
pub struct Pair {
    _dir: TempDir,
    pub a: Node,
    pub b: Node,
    pub index: u64,
}

impl Pair {
    pub fn new(setup: &[&str]) -> Pair {
        let dir = TempDir::new().unwrap();
        let a = node(&dir, "a.db");
        let b = node(&dir, "b.db");
        let mut pair = Pair {
            _dir: dir,
            a,
            b,
            index: 0,
        };
        let p = pair.capture(setup);
        assert_eq!(pair.apply(vec![p]), applied_ok(1));
        pair
    }

    pub fn capture(&self, statements: &[&str]) -> Vec<u8> {
        capture(&self.a, statements)
    }

    /// Applies one entry on both nodes; returns the outcomes, equal on both.
    pub fn apply(&mut self, proposals: Vec<Vec<u8>>) -> Vec<Outcome> {
        self.index += 1;
        let e = entry(self.index, proposals);
        let a = self.a.sm.apply(&e).unwrap();
        let b = self.b.sm.apply(&e).unwrap();
        assert_eq!(a, b);
        eprintln!("--- entry {}: {a:?}", self.index);
        a
    }

    pub fn rows(&self, sql: &str) -> String {
        let a = format!("{:?}", query(&self.a.db.connect().unwrap(), sql));
        let b = format!("{:?}", query(&self.b.db.connect().unwrap(), sql));
        assert_eq!(a, b, "{sql}");
        a
    }

    pub fn assert_same(&self) {
        assert_same(&self.a, &self.b);
    }
}

pub fn ints(rows: &[&[i64]]) -> String {
    let rows: Vec<Vec<Value>> = rows
        .iter()
        .map(|r| r.iter().map(|v| Value::from_i64(*v)).collect())
        .collect();
    format!("{rows:?}")
}

/// The reason of a rejection; panics on `Applied`.
pub fn reason(outcome: &Outcome) -> &str {
    match outcome {
        Outcome::Rejected { reason } => reason,
        o => panic!("expected a rejection, got {o:?}"),
    }
}
