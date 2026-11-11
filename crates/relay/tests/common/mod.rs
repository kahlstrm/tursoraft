//! The Worker and follower under Miniflare, a cluster in this process, a
//! store-like schema, and comparison of a follower with a node.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tursoraft_node::{CommitMode, Error, Node, ReadLevel, Statement, Value};
use tursoraft_raft::NodeId;
use tursoraft_relay::protocol::decode_value;
use tursoraft_relay::{Config, Relay};

#[path = "../../../node/tests/in_process/mod.rs"]
pub mod in_process;

pub use in_process::Cluster;

pub const SECRET: &str = "test-secret";
/// The follower that pages read from: the Worker's `FOLLOWER_NAME`.
pub const FOLLOWER: &str = "default";

/// The Worker and follower Durable Object, run by `edge/test/serve.mjs`.
pub struct Edge {
    pub url: String,
    _child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    http: reqwest::Client,
}

impl Edge {
    pub async fn start() -> Edge {
        let edge = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../edge");
        assert!(
            edge.join("node_modules/miniflare").exists(),
            "Miniflare is not installed: run `npm ci` in {}",
            edge.display()
        );
        let mut child = Command::new("node")
            .arg(edge.join("test/serve.mjs"))
            .env("RELAY_SECRET", SECRET)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("run node");
        let stdin = child.stdin.take().unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let first = tokio::time::timeout(Duration::from_secs(60), stdout.next_line())
            .await
            .expect("Miniflare did not start in time")
            .unwrap()
            .expect("serve.mjs exited");
        let url = serde_json::from_str::<serde_json::Value>(&first).unwrap()["url"]
            .as_str()
            .unwrap()
            .to_string();
        Edge {
            url,
            _child: child,
            stdin,
            stdout,
            http: reqwest::Client::new(),
        }
    }

    pub fn relay_config(&self, follower: &str) -> Config {
        let mut config = Config::new(
            format!(
                "{}/relay?follower={follower}",
                self.url.replace("http://", "ws://")
            ),
            SECRET,
        );
        config.retry = Duration::from_millis(100);
        config
    }

    pub fn relay(&self, node: &Node, follower: &str) -> Relay {
        Relay::start(node.clone(), self.relay_config(follower))
    }

    async fn admin(&self, path: &str) -> reqwest::Response {
        self.http
            .get(format!("{}{path}", self.url))
            .bearer_auth(SECRET)
            .send()
            .await
            .unwrap()
    }

    pub async fn status(&self, follower: &str) -> Status {
        let v: serde_json::Value = self
            .admin(&format!("/admin/status?follower={follower}"))
            .await
            .json()
            .await
            .unwrap();
        Status {
            through: v["through"].as_u64().unwrap(),
            instance: v["instance"].as_str().unwrap().to_string(),
            constructed: v["constructed"].as_u64().unwrap(),
            relays: v["relays"].as_u64().unwrap(),
            copying: v["copying"].as_bool().unwrap(),
        }
    }

    /// The follower's tables once it has applied `at`; `None` if it did not
    /// within `wait_ms`.
    pub async fn dump(&self, follower: &str, at: u64, wait_ms: u64) -> Option<Tables> {
        let response = self
            .admin(&format!(
                "/admin/dump?follower={follower}&at={at}&wait_ms={wait_ms}"
            ))
            .await;
        if response.status() == 504 {
            return None;
        }
        let status = response.status();
        assert!(
            status.is_success(),
            "{status}: {}",
            response.text().await.unwrap()
        );
        let v: serde_json::Value = response.json().await.unwrap();
        let mut tables = BTreeMap::new();
        for (name, t) in v["tables"].as_object().unwrap() {
            let rows = t["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap()
                        .iter()
                        .map(|v| decode_value(v).unwrap_or_else(|| panic!("value {v}")))
                        .collect()
                })
                .collect();
            tables.insert(name.clone(), sorted(t["rowid"].as_bool().unwrap(), rows));
        }
        let schema = v["schema"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                (
                    o["type"].as_str().unwrap().to_string(),
                    o["name"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        Some(Tables {
            index: v["through"].as_u64().unwrap(),
            schema: sorted_schema(schema),
            tables,
        })
    }

    pub async fn close_relays(&self, follower: &str) -> u64 {
        let v: serde_json::Value = self
            .http
            .post(format!(
                "{}/admin/close-relays?follower={follower}",
                self.url
            ))
            .bearer_auth(SECRET)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        v["closed"].as_u64().unwrap()
    }

    /// GET `path`: status, body, and the follower position it was read at.
    pub async fn page(&self, path: &str) -> (u16, String, Option<u64>) {
        let response = self
            .http
            .get(format!("{}{path}", self.url))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let through = response
            .headers()
            .get("x-follower-through")
            .map(|v| v.to_str().unwrap().parse().unwrap());
        (status, response.text().await.unwrap(), through)
    }

    /// Removes the follower from memory, keeping its WebSockets open.
    pub async fn evict(&mut self, follower: &str) {
        self.stdin
            .write_all(format!("evict {follower}\n").as_bytes())
            .await
            .unwrap();
        let line = self.stdout.next_line().await.unwrap().unwrap();
        assert_eq!(line, "evicted");
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    pub through: u64,
    pub instance: String,
    pub constructed: u64,
    pub relays: u64,
    pub copying: bool,
}

/// The copied tables of a node or a follower at one log index: for each
/// table, whether rows carry their rowid first (tables without a PRIMARY
/// KEY), and its rows in a fixed order.
#[derive(Debug, PartialEq)]
pub struct Tables {
    pub index: u64,
    pub schema: Vec<(String, String)>,
    pub tables: BTreeMap<String, (bool, Vec<Vec<Value>>)>,
}

/// The tables of `node` at its applied index, read with SQL independent of
/// the relay's: one SELECT per table, each returning the index it read at.
pub async fn read_node(node: &Node) -> Tables {
    'again: loop {
        let schema = query(
            node,
            "SELECT type, name, sql FROM sqlite_schema WHERE type IN ('table', 'index', 'view')",
        )
        .await;
        let schema: Vec<(String, String)> = schema
            .into_iter()
            .filter(|r| r[2] != Value::Null)
            .map(|r| (text(&r[0]), text(&r[1])))
            .filter(|(_, name)| !internal(name))
            .collect();
        let mut index = None;
        let mut tables = BTreeMap::new();
        for (_, name) in schema.iter().filter(|(kind, _)| kind == "table") {
            let keyed = query(
                node,
                &format!("SELECT count(*) FROM pragma_table_info('{name}') WHERE pk > 0"),
            )
            .await;
            let rowid = keyed[0][0] == Value::Integer(0);
            let rows = query(
                node,
                &format!("SELECT a.log_index, t.rowid, t.* FROM tursoraft_applied a LEFT JOIN \"{name}\" t"),
            )
            .await;
            let Value::Integer(at) = rows[0][0] else {
                panic!("no applied index")
            };
            if index.is_some_and(|i| i != at as u64) {
                continue 'again;
            }
            index = Some(at as u64);
            let rows = rows
                .into_iter()
                .filter(|r| r[1] != Value::Null)
                .map(|r| r[if rowid { 1 } else { 2 }..].to_vec())
                .collect();
            tables.insert(name.clone(), sorted(rowid, rows));
        }
        let index = match index {
            Some(i) => i,
            None => applied(node).await,
        };
        return Tables {
            index,
            schema: sorted_schema(schema),
            tables,
        };
    }
}

/// Waits until `follower` has applied what `node` has, and asserts that
/// their tables are equal at that log index. Returns the index.
pub async fn assert_same(edge: &Edge, follower: &str, node: &Node) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let expected = read_node(node).await;
        let got = edge.dump(follower, expected.index, 2_000).await;
        if let Some(got) = got {
            if got.index == expected.index {
                assert_eq!(got.schema, expected.schema, "schema at {}", got.index);
                for (name, table) in &expected.tables {
                    assert_eq!(
                        got.tables.get(name),
                        Some(table),
                        "table {name} at log index {}",
                        got.index
                    );
                }
                assert_eq!(got.tables.len(), expected.tables.len());
                return got.index;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follower {follower} did not reach node {} at {}: {:?}",
            node.id(),
            expected.index,
            edge.status(follower).await
        );
    }
}

fn sorted(rowid: bool, mut rows: Vec<Vec<Value>>) -> (bool, Vec<Vec<Value>>) {
    rows.sort_by_cached_key(|r| format!("{r:?}"));
    (rowid, rows)
}

fn sorted_schema(mut schema: Vec<(String, String)>) -> Vec<(String, String)> {
    schema.sort();
    schema
}

fn internal(name: &str) -> bool {
    ["sqlite_", "turso_cdc", "tursoraft_"]
        .iter()
        .any(|p| name.starts_with(p))
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        v => panic!("expected text, got {v:?}"),
    }
}

pub async fn query(node: &Node, sql: &str) -> Vec<Vec<Value>> {
    node.query(Statement::from(sql), ReadLevel::Local)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows
}

pub async fn applied(node: &Node) -> u64 {
    let rows = query(node, "SELECT log_index FROM tursoraft_applied").await;
    let Value::Integer(i) = rows[0][0] else {
        panic!("no applied index")
    };
    i as u64
}

/// Runs `statements` as one transaction on `node` and returns its index.
pub async fn write(node: &Node, statements: Vec<Statement>) -> Result<u64, Error> {
    node.execute(statements, CommitMode::Quorum)
        .await
        .map(|w| w.index)
}

pub fn st(sql: &str, params: Vec<Value>) -> Statement {
    Statement::new(sql, params)
}

pub fn int(i: i64) -> Value {
    Value::Integer(i)
}

pub fn txt(s: &str) -> Value {
    Value::Text(s.into())
}

/// A store: products with stock, orders that take stock through a trigger,
/// notes that an order's deletion removes through ON DELETE CASCADE, an
/// index and a view.
pub const STORE: &[&str] = &[
    "CREATE TABLE products(product_id INTEGER PRIMARY KEY, title TEXT NOT NULL, \
     price_cents INTEGER NOT NULL, stock INTEGER NOT NULL)",
    "CREATE TABLE orders(order_id TEXT PRIMARY KEY, \
     product_id INTEGER NOT NULL REFERENCES products(product_id), quantity INTEGER NOT NULL)",
    "CREATE INDEX orders_by_product ON orders(product_id)",
    // No PRIMARY KEY: rows are identified by rowid.
    "CREATE TABLE order_notes(order_id TEXT REFERENCES orders(order_id) ON DELETE CASCADE, \
     note TEXT)",
    "CREATE TRIGGER take_stock AFTER INSERT ON orders BEGIN \
     UPDATE products SET stock = stock - new.quantity WHERE product_id = new.product_id; END",
    "CREATE VIEW low_stock AS SELECT product_id, title FROM products WHERE stock < 5",
];

pub const PRODUCTS: i64 = 8;

pub async fn create_store(node: &Node) {
    for sql in STORE {
        write(node, vec![Statement::from(*sql)]).await.unwrap();
    }
    for id in 1..=PRODUCTS {
        write(
            node,
            vec![st(
                "INSERT INTO products VALUES (?, ?, ?, 100)",
                vec![int(id), txt(&format!("Product {id}")), int(100 * id)],
            )],
        )
        .await
        .unwrap();
    }
}

/// Places an order with a note; the trigger takes the stock.
pub async fn place_order(node: &Node, order: &str, product: i64, quantity: i64) -> u64 {
    write(
        node,
        vec![
            st(
                "INSERT INTO orders VALUES (?, ?, ?)",
                vec![txt(order), int(product), int(quantity)],
            ),
            st(
                "INSERT INTO order_notes VALUES (?, ?)",
                vec![txt(order), txt(&format!("note of {order}"))],
            ),
        ],
    )
    .await
    .unwrap()
}

/// `n` transactions of every kind the store has: orders, cancellations
/// (which cascade to notes), price changes, new products, deleted products.
pub async fn store_writes(node: &Node, seed: u64, n: usize) -> u64 {
    let mut rng = Rng(seed);
    let mut index = 0;
    for i in 0..n {
        let order = format!("o{seed}-{i}");
        index = match rng.below(5) {
            0 | 1 => place_order(node, &order, 1 + rng.below(PRODUCTS as u64) as i64, 1).await,
            2 => write(
                node,
                vec![st(
                    "DELETE FROM orders WHERE order_id = (SELECT max(order_id) FROM orders)",
                    vec![],
                )],
            )
            .await
            .unwrap(),
            3 => write(
                node,
                vec![st(
                    "UPDATE products SET price_cents = price_cents + 1 WHERE product_id = ?",
                    vec![int(1 + rng.below(PRODUCTS as u64) as i64)],
                )],
            )
            .await
            .unwrap(),
            _ => write(
                node,
                vec![
                    st(
                        "INSERT INTO products(title, price_cents, stock) VALUES (?, 5, 3)",
                        vec![txt(&format!("New {order}"))],
                    ),
                    st(
                        "DELETE FROM products WHERE product_id = \
                         (SELECT min(product_id) FROM products WHERE product_id > ? \
                          AND product_id NOT IN (SELECT product_id FROM orders))",
                        vec![int(PRODUCTS + 3)],
                    ),
                ],
            )
            .await
            .unwrap(),
        };
    }
    index
}

/// Write-conflict outcomes of [`interactive_orders`].
#[derive(Default)]
pub struct Tally {
    pub committed: AtomicU64,
    pub rejected: AtomicU64,
    pub aborted: AtomicU64,
}

/// `n` interactive transactions on `node`, each reading a product's stock
/// and writing it back lower with an order, so that concurrent ones on the
/// same product conflict.
pub async fn interactive_orders(node: Node, seed: u64, n: usize, tally: Arc<Tally>) {
    let mut rng = Rng(seed);
    for i in 0..n {
        let product = 1 + rng.below(3) as i64;
        let order = format!("i{seed}-{i}");
        let result = async {
            let mut tx = node.begin().await?;
            let rows = tx
                .execute(st(
                    "SELECT stock FROM products WHERE product_id = ?",
                    vec![int(product)],
                ))
                .await?
                .rows;
            let Value::Integer(stock) = rows[0][0] else {
                unreachable!()
            };
            tokio::time::sleep(Duration::from_millis(rng.below(3))).await;
            tx.execute(st(
                "UPDATE products SET stock = ? WHERE product_id = ?",
                vec![int(stock - 1), int(product)],
            ))
            .await?;
            tx.execute(st(
                "INSERT INTO order_notes VALUES (NULL, ?)",
                vec![txt(&order)],
            ))
            .await?;
            tx.commit(CommitMode::Quorum).await.map(|w| w.index)
        }
        .await;
        match result {
            Ok(_) => tally.committed.fetch_add(1, Relaxed),
            Err(Error::Rejected(_)) => tally.rejected.fetch_add(1, Relaxed),
            Err(Error::Aborted(_)) => tally.aborted.fetch_add(1, Relaxed),
            Err(e) => panic!("unexpected error: {e}"),
        };
    }
}

/// Runs `writers` store writers and `interactive` interactive writers per
/// node in `on`, all at once, and returns their tally.
pub async fn concurrent_writes(c: &Cluster, on: &[NodeId], n: usize) -> Arc<Tally> {
    let tally = Arc::new(Tally::default());
    let mut tasks = Vec::new();
    for (i, id) in on.iter().enumerate() {
        let node = c.node(*id).clone();
        tasks.push(tokio::spawn(async move {
            store_writes(&node, 100 + i as u64, n).await;
        }));
        for k in 0..2 {
            tasks.push(tokio::spawn(interactive_orders(
                c.node(*id).clone(),
                200 + 10 * i as u64 + k,
                n,
                tally.clone(),
            )));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }
    println!(
        "interactive: committed {}, rejected {}, aborted {}",
        tally.committed.load(Relaxed),
        tally.rejected.load(Relaxed),
        tally.aborted.load(Relaxed)
    );
    tally
}

/// Voters 1, 2, 3 with the store. Returns the leader and another voter.
pub async fn three_nodes(change: impl FnOnce(&mut Cluster)) -> (Cluster, NodeId, NodeId) {
    let mut c = Cluster::new(&[1, 2, 3], |_| {});
    change(&mut c);
    c.start_all().await;
    let leader = c.initialize(&[1, 2, 3]).await;
    create_store(c.node(leader)).await;
    let other = [1, 2, 3].into_iter().find(|n| *n != leader).unwrap();
    c.converge(leader, &[1, 2, 3]).await;
    (c, leader, other)
}

/// Waits until `relay` has made `copies` full copies.
pub async fn wait_copies(relay: &Relay, copies: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while relay.stats().copies < copies {
        assert!(
            tokio::time::Instant::now() < deadline,
            "relay made no copy {copies}: {:?}",
            relay.stats()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A small deterministic random number generator.
pub struct Rng(pub u64);

impl Rng {
    pub fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}
