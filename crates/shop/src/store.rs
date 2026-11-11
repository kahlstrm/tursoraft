//! The store's schema and every database operation of the HTTP API.
//!
//! Every key is chosen by the store or its clients, never by the database:
//! products and units have integer ids derived from the product id, carts and
//! orders have text ids the client sends (or the store draws at random). Rows
//! whose key the database picks would collide between instances, and the
//! commit-time check would reject one of them.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::Rng;
use tursoraft_node::{CommitMode, Error, Node, ReadLevel, Rows, Statement, Transaction, Value};

use crate::api::{
    Cart, CartLine, CheckoutRequest, Export, Order, OrderLine, Outcome, Product, Reserve,
    SetupRequest, StockRow, WriteAnswer,
};

/// Deliberate defects, for showing that the checker catches them. Never set
/// outside such a test.
#[derive(Debug, Clone, Copy, Default)]
pub struct Breaks {
    /// Answer a checkout whose commit was rejected as if it had succeeded.
    pub ack_rejected: bool,
    /// Design A: take stock without checking that enough is left.
    pub oversell: bool,
}

pub struct Store {
    pub node: Node,
    pub name: String,
    /// The commit mode of add to cart.
    pub cart_mode: CommitMode,
    /// The simulated call to the payment provider, outside any transaction:
    /// a delay drawn from this range, in milliseconds.
    pub payment_ms: (u64, u64),
    /// Share of payments the provider declines when the request does not say.
    pub decline: f64,
    pub breaks: Breaks,
    reserve: OnceLock<Reserve>,
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE shop_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE products(product_id INTEGER PRIMARY KEY, title TEXT NOT NULL, \
     price_cents INTEGER NOT NULL, stock INTEGER NOT NULL)",
    // Designs A and C: one counter row per product.
    "CREATE TABLE stock(product_id INTEGER PRIMARY KEY, available INTEGER NOT NULL)",
    // Design B: one row per unit for sale, unit_id = product_id * 1000000 + n.
    "CREATE TABLE units(unit_id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL)",
    "CREATE INDEX units_by_product ON units(product_id, unit_id)",
    "CREATE TABLE cart_lines(cart_id TEXT NOT NULL, product_id INTEGER NOT NULL, \
     quantity INTEGER NOT NULL, PRIMARY KEY(cart_id, product_id))",
    "CREATE TABLE orders(order_id TEXT PRIMARY KEY, node_id INTEGER NOT NULL, \
     cart_id TEXT NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL, \
     created_ms INTEGER NOT NULL)",
    "CREATE TABLE order_lines(order_id TEXT NOT NULL, line_no INTEGER NOT NULL, \
     product_id INTEGER NOT NULL, quantity INTEGER NOT NULL, price_cents INTEGER NOT NULL, \
     PRIMARY KEY(order_id, line_no))",
    // Design B: the units each order took.
    "CREATE TABLE order_units(order_id TEXT NOT NULL, unit_id INTEGER NOT NULL, \
     product_id INTEGER NOT NULL, PRIMARY KEY(order_id, unit_id))",
    // Design C: stock taken at the leader, then claimed by the order.
    "CREATE TABLE reservations(order_id TEXT NOT NULL, product_id INTEGER NOT NULL, \
     quantity INTEGER NOT NULL, status TEXT NOT NULL, PRIMARY KEY(order_id, product_id))",
];

/// The product on flash sale.
pub const FLASH_PRODUCT: i64 = 0;
pub const UNIT_ID_BASE: i64 = 1_000_000;
const MAX_QUANTITY: i64 = 10;

impl Store {
    pub fn new(node: Node, name: String) -> Store {
        Store {
            node,
            name,
            cart_mode: CommitMode::Async,
            payment_ms: (200, 800),
            decline: 0.05,
            breaks: Breaks::default(),
            reserve: OnceLock::new(),
        }
    }

    /// The design the cluster was set up with, or `None` before setup.
    pub async fn reserve(&self) -> Option<Reserve> {
        if let Some(r) = self.reserve.get() {
            return Some(*r);
        }
        let rows = self
            .node
            .query(
                st("SELECT value FROM shop_meta WHERE key = 'reserve'", vec![]),
                ReadLevel::Local,
            )
            .await
            .ok()?;
        let r = Reserve::parse(&text(rows.rows.first()?.first()?))?;
        Some(*self.reserve.get_or_init(|| r))
    }

    /// Creates the schema and the catalog. Fails if the store is set up.
    pub async fn setup(&self, req: &SetupRequest) -> Result<u64, String> {
        if self.reserve().await.is_some() {
            return Err("the store is already set up".into());
        }
        if req.products < 1 || req.stock < 0 || req.flash_stock < 0 {
            return Err("products must be at least 1 and stock at least 0".into());
        }
        if req.stock >= UNIT_ID_BASE || req.flash_stock >= UNIT_ID_BASE {
            return Err(format!("stock must be below {UNIT_ID_BASE}"));
        }
        let mut statements: Vec<Statement> = SCHEMA.iter().map(|s| Statement::from(*s)).collect();
        statements.push(st(
            "INSERT INTO shop_meta VALUES ('reserve', ?)",
            vec![Value::Text(req.reserve.name().into())],
        ));
        let mut products = vec![(
            FLASH_PRODUCT,
            "Flash sale: limited sneaker".to_string(),
            4900,
            req.flash_stock,
        )];
        for id in 1..=req.products {
            products.push((
                id,
                format!("Product {id}"),
                500 + (id * 137) % 5000,
                req.stock,
            ));
        }
        for (id, title, price, stock) in &products {
            statements.push(st(
                "INSERT INTO products VALUES (?, ?, ?, ?)",
                vec![
                    int(*id),
                    Value::Text(title.clone()),
                    int(*price),
                    int(*stock),
                ],
            ));
            let available = if req.reserve == Reserve::Units {
                0
            } else {
                *stock
            };
            statements.push(st(
                "INSERT INTO stock VALUES (?, ?)",
                vec![int(*id), int(available)],
            ));
        }
        let mut last = self
            .node
            .execute(statements, CommitMode::Quorum)
            .await
            .map_err(|e| e.to_string())?
            .index;
        if req.reserve == Reserve::Units {
            // Inserted as literals in transactions of a few thousand rows,
            // so no change set grows large.
            let mut units = Vec::new();
            for (id, _, _, stock) in &products {
                units.extend((0..*stock).map(|n| (id * UNIT_ID_BASE + n, *id)));
            }
            for chunk in units.chunks(5000) {
                let values: Vec<String> = chunk.iter().map(|(u, p)| format!("({u},{p})")).collect();
                let sql = format!("INSERT INTO units VALUES {}", values.join(","));
                last = self
                    .node
                    .execute(vec![Statement::from(sql.as_str())], CommitMode::Quorum)
                    .await
                    .map_err(|e| e.to_string())?
                    .index;
            }
        }
        Ok(last)
    }

    /// Products by id with their stock, read locally.
    pub async fn catalog(&self, offset: i64, limit: i64) -> Result<Vec<Product>, Error> {
        let rows = self
            .read(
                "SELECT p.product_id, p.title, p.price_cents, s.available FROM products p \
                 JOIN stock s ON s.product_id = p.product_id \
                 ORDER BY p.product_id LIMIT ? OFFSET ?",
                vec![int(limit), int(offset)],
                ReadLevel::Local,
            )
            .await?;
        let mut products: Vec<Product> = rows.rows.iter().map(|r| product(r)).collect();
        if self.reserve().await == Some(Reserve::Units) && !products.is_empty() {
            let first = products[0].product_id;
            let last = products[products.len() - 1].product_id;
            let counts = self
                .read(
                    "SELECT product_id, COUNT(*) FROM units \
                     WHERE product_id BETWEEN ? AND ? GROUP BY product_id",
                    vec![int(first), int(last)],
                    ReadLevel::Local,
                )
                .await?;
            let counts: BTreeMap<i64, i64> = counts
                .rows
                .iter()
                .map(|r| (integer(&r[0]), integer(&r[1])))
                .collect();
            for p in &mut products {
                p.available = counts.get(&p.product_id).copied().unwrap_or(0);
            }
        }
        Ok(products)
    }

    /// One product, read locally.
    pub async fn product(&self, id: i64) -> Result<Option<Product>, Error> {
        let rows = self
            .read(
                "SELECT p.product_id, p.title, p.price_cents, s.available FROM products p \
                 JOIN stock s ON s.product_id = p.product_id WHERE p.product_id = ?",
                vec![int(id)],
                ReadLevel::Local,
            )
            .await?;
        let Some(mut p) = rows.rows.first().map(|r| product(r)) else {
            return Ok(None);
        };
        if self.reserve().await == Some(Reserve::Units) {
            p.available = self.units_left(id).await?;
        }
        Ok(Some(p))
    }

    /// Units of `id` still for sale, read locally.
    pub async fn available(&self, id: i64) -> Result<i64, Error> {
        if self.reserve().await == Some(Reserve::Units) {
            return self.units_left(id).await;
        }
        let rows = self
            .read(
                "SELECT available FROM stock WHERE product_id = ?",
                vec![int(id)],
                ReadLevel::Local,
            )
            .await?;
        Ok(rows.rows.first().map_or(0, |r| integer(&r[0])))
    }

    async fn units_left(&self, id: i64) -> Result<i64, Error> {
        let rows = self
            .read(
                "SELECT COUNT(*) FROM units WHERE product_id = ?",
                vec![int(id)],
                ReadLevel::Local,
            )
            .await?;
        Ok(integer(&rows.rows[0][0]))
    }

    /// The cart, read once this instance has applied log index `after`.
    pub async fn cart(&self, cart_id: &str, after: u64) -> Result<Cart, Error> {
        let rows = self
            .read(CART_SQL, vec![txt(cart_id)], ReadLevel::AtIndex(after))
            .await?;
        let lines: Vec<CartLine> = rows.rows.iter().map(|r| cart_line(r)).collect();
        Ok(Cart {
            cart_id: cart_id.to_string(),
            total_cents: lines.iter().map(|l| l.quantity * l.price_cents).sum(),
            lines,
        })
    }

    /// Adds `quantity` of a product to the cart, in the cart's commit mode.
    pub async fn add_to_cart(&self, cart_id: &str, product_id: i64, quantity: i64) -> WriteAnswer {
        if !(1..=MAX_QUANTITY).contains(&quantity) {
            return invalid(format!("quantity must be 1 to {MAX_QUANTITY}"));
        }
        let exists = self
            .read(
                "SELECT 1 FROM products WHERE product_id = ?",
                vec![int(product_id)],
                ReadLevel::Local,
            )
            .await;
        match exists {
            Ok(rows) if rows.rows.is_empty() => return invalid(format!("no product {product_id}")),
            Ok(_) => {}
            Err(e) => return failed(&e),
        }
        let upsert = st(
            "INSERT INTO cart_lines VALUES (?, ?, ?) ON CONFLICT(cart_id, product_id) \
             DO UPDATE SET quantity = quantity + excluded.quantity",
            vec![txt(cart_id), int(product_id), int(quantity)],
        );
        match self.node.execute(vec![upsert], self.cart_mode).await {
            Ok(w) => ok(w.index),
            Err(e) => failed(&e),
        }
    }

    /// Turns the cart into a pending order, reserving its stock the way the
    /// store was set up with. The order is created in an interactive
    /// transaction on this instance, committed in `quorum` mode.
    pub async fn checkout(&self, req: &CheckoutRequest) -> WriteAnswer {
        let order_id = match &req.order_id {
            Some(id) => id.clone(),
            None => format!(
                "o{}-{:016x}",
                self.node.id(),
                rand::thread_rng().gen::<u64>()
            ),
        };
        if !valid_id(&order_id) {
            return invalid("an order id is 1 to 64 letters, digits, '-' or '_'".into());
        }
        let Some(reserve) = self.reserve().await else {
            return invalid("the store is not set up".into());
        };
        if let Err(e) = self.node.raft().wait_applied(req.after, TIMEOUT).await {
            return answer(Outcome::Unavailable, e.to_string());
        }
        let mut a = match reserve {
            Reserve::Counter | Reserve::Units => {
                self.checkout_locally(reserve, &req.cart_id, &order_id)
                    .await
            }
            Reserve::Leader => self.checkout_at_leader(&req.cart_id, &order_id).await,
        };
        if a.outcome == Outcome::Rejected && self.breaks.ack_rejected {
            a.outcome = Outcome::Ok;
            a.error = None;
        }
        a.order_id = Some(order_id);
        a
    }

    /// Designs A and B: the stock is taken inside the interactive transaction.
    async fn checkout_locally(
        &self,
        reserve: Reserve,
        cart_id: &str,
        order_id: &str,
    ) -> WriteAnswer {
        let mut tx = match self.node.begin().await {
            Ok(tx) => tx,
            Err(e) => return failed(&e),
        };
        let lines = match tx.execute(st(CART_SQL, vec![txt(cart_id)])).await {
            Ok(rows) => rows.rows.iter().map(|r| cart_line(r)).collect::<Vec<_>>(),
            Err(e) => return failed(&e),
        };
        if lines.is_empty() {
            return invalid("the cart is empty".into());
        }
        for line in &lines {
            let taken = match reserve {
                Reserve::Counter => self.take_counter(&mut tx, line).await,
                _ => take_units(&mut tx, order_id, line).await,
            };
            match taken {
                Ok(true) => {}
                Ok(false) => {
                    return answer(Outcome::SoldOut, format!("product {}", line.product_id))
                }
                Err(e) => return failed(&e),
            }
        }
        let total = match create_order(&mut tx, self.node.id(), cart_id, order_id, &lines).await {
            Ok(total) => total,
            Err(e) => return failed(&e),
        };
        match tx.commit(CommitMode::Quorum).await {
            Ok(w) => order_answer(w.index, "pending", total),
            Err(e) => failed(&e),
        }
    }

    async fn take_counter(&self, tx: &mut Transaction, line: &CartLine) -> Result<bool, Error> {
        let sql = if self.breaks.oversell {
            "UPDATE stock SET available = available - ? WHERE product_id = ? AND ? > 0"
        } else {
            "UPDATE stock SET available = available - ? WHERE product_id = ? AND available >= ?"
        };
        let q = line.quantity;
        let rows = tx
            .execute(st(sql, vec![int(q), int(line.product_id), int(q)]))
            .await?;
        Ok(rows.rows_changed == 1)
    }

    /// Design C: the leader takes the stock with one `execute`, then the
    /// interactive transaction claims it. Stock that was taken and not
    /// claimed is given back.
    async fn checkout_at_leader(&self, cart_id: &str, order_id: &str) -> WriteAnswer {
        let lines = match self
            .read(CART_SQL, vec![txt(cart_id)], ReadLevel::Local)
            .await
        {
            Ok(rows) => rows.rows.iter().map(|r| cart_line(r)).collect::<Vec<_>>(),
            Err(e) => return failed(&e),
        };
        if lines.is_empty() {
            return invalid("the cart is empty".into());
        }
        let mut reserve = Vec::new();
        for l in &lines {
            let (p, q) = (int(l.product_id), int(l.quantity));
            reserve.push(st(
                "INSERT INTO reservations SELECT ?, product_id, ?, 'held' FROM stock \
                 WHERE product_id = ? AND available >= ?",
                vec![txt(order_id), q.clone(), p.clone(), q.clone()],
            ));
            reserve.push(st(
                "UPDATE stock SET available = available - ? WHERE product_id = ? AND available >= ?",
                vec![q.clone(), p, q],
            ));
        }
        let reserved = match self.node.execute(reserve, CommitMode::Quorum).await {
            Ok(w) => w,
            // Nothing to give back if the outcome is unknown: the stock may
            // stay held, which the checker counts as reserved.
            Err(e) => return failed_before_order(&e),
        };
        let held = reserved
            .results
            .iter()
            .step_by(2)
            .filter(|r| r.rows_changed == 1)
            .count();
        if held < lines.len() {
            if held > 0 {
                self.release(order_id, reserved.index).await;
            }
            return answer(Outcome::SoldOut, "not enough stock".into());
        }
        let a = self.claim(cart_id, order_id, reserved.index).await;
        if a.outcome.had_no_effect() {
            self.release(order_id, reserved.index).await;
        }
        a
    }

    async fn claim(&self, cart_id: &str, order_id: &str, reserved_at: u64) -> WriteAnswer {
        if let Err(e) = self.node.raft().wait_applied(reserved_at, TIMEOUT).await {
            return answer(Outcome::Unavailable, e.to_string());
        }
        let mut tx = match self.node.begin().await {
            Ok(tx) => tx,
            Err(e) => return failed(&e),
        };
        let lines = match tx
            .execute(st(
                "SELECT r.product_id, p.title, r.quantity, p.price_cents FROM reservations r \
                 JOIN products p ON p.product_id = r.product_id \
                 WHERE r.order_id = ? AND r.status = 'held' ORDER BY r.product_id",
                vec![txt(order_id)],
            ))
            .await
        {
            Ok(rows) => rows.rows.iter().map(|r| cart_line(r)).collect::<Vec<_>>(),
            Err(e) => return failed(&e),
        };
        if lines.is_empty() {
            return answer(Outcome::Failed, "the reservation is gone".into());
        }
        let claim = st(
            "UPDATE reservations SET status = 'claimed' WHERE order_id = ? AND status = 'held'",
            vec![txt(order_id)],
        );
        if let Err(e) = tx.execute(claim).await {
            return failed(&e);
        }
        let total = match create_order(&mut tx, self.node.id(), cart_id, order_id, &lines).await {
            Ok(total) => total,
            Err(e) => return failed(&e),
        };
        match tx.commit(CommitMode::Quorum).await {
            Ok(w) => order_answer(w.index, "pending", total),
            Err(e) => failed(&e),
        }
    }

    /// Gives back the stock of the reservations of `order_id` still held.
    async fn release(&self, order_id: &str, after: u64) {
        let held = match self
            .read(
                "SELECT product_id, quantity FROM reservations \
                 WHERE order_id = ? AND status = 'held'",
                vec![txt(order_id)],
                ReadLevel::AtIndex(after),
            )
            .await
        {
            Ok(rows) => rows.rows,
            Err(e) => {
                tracing::warn!("release {order_id}: {e}");
                return;
            }
        };
        let mut statements = Vec::new();
        for r in &held {
            statements.push(st(
                "UPDATE stock SET available = available + ? WHERE product_id = ? AND EXISTS \
                 (SELECT 1 FROM reservations WHERE order_id = ? AND product_id = ? AND status = 'held')",
                vec![r[1].clone(), r[0].clone(), txt(order_id), r[0].clone()],
            ));
        }
        statements.push(st(
            "DELETE FROM reservations WHERE order_id = ? AND status = 'held'",
            vec![txt(order_id)],
        ));
        if let Err(e) = self.node.execute(statements, CommitMode::Quorum).await {
            tracing::warn!("release {order_id}: {e}");
        }
    }

    /// Calls the simulated payment provider, outside any transaction, then
    /// marks the order paid, or cancels it and gives its stock back.
    pub async fn pay(&self, order_id: &str, approved: Option<bool>, after: u64) -> WriteAnswer {
        let Some(reserve) = self.reserve().await else {
            return invalid("the store is not set up".into());
        };
        let order = match self.order(order_id, after).await {
            Ok(Some(o)) => o,
            Ok(None) => return invalid(format!("no order {order_id}")),
            Err(e) => return failed(&e),
        };
        if order.status != "pending" {
            return invalid(format!("order {order_id} is {}", order.status));
        }
        let (approved, delay) = {
            let mut rng = rand::thread_rng();
            let (lo, hi) = self.payment_ms;
            let approved = approved.unwrap_or_else(|| !rng.gen_bool(self.decline));
            (approved, rng.gen_range(lo..=hi.max(lo)))
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
        let pending = "EXISTS (SELECT 1 FROM orders WHERE order_id = ? AND status = 'pending')";
        let mut statements = Vec::new();
        let status = if approved {
            "paid"
        } else {
            if reserve == Reserve::Units {
                statements.push(st(
                    &format!(
                        "INSERT INTO units SELECT unit_id, product_id FROM order_units \
                         WHERE order_id = ? AND {pending}"
                    ),
                    vec![txt(order_id), txt(order_id)],
                ));
            } else {
                for l in &order.lines {
                    statements.push(st(
                        &format!(
                            "UPDATE stock SET available = available + ? WHERE product_id = ? AND {pending}"
                        ),
                        vec![int(l.quantity), int(l.product_id), txt(order_id)],
                    ));
                }
            }
            "cancelled"
        };
        statements.push(st(
            "UPDATE orders SET status = ? WHERE order_id = ? AND status = 'pending'",
            vec![txt(status), txt(order_id)],
        ));
        match self.node.execute(statements, CommitMode::Quorum).await {
            Ok(w) if w.results.last().is_some_and(|r| r.rows_changed == 1) => {
                let mut a = order_answer(w.index, status, order.total_cents);
                a.order_id = Some(order_id.to_string());
                a
            }
            Ok(_) => invalid(format!("order {order_id} is no longer pending")),
            Err(e) => failed(&e),
        }
    }

    /// The order, read once this instance has applied log index `after`.
    pub async fn order(&self, order_id: &str, after: u64) -> Result<Option<Order>, Error> {
        let level = ReadLevel::AtIndex(after);
        let rows = self
            .read(
                "SELECT order_id, node_id, status, total_cents FROM orders WHERE order_id = ?",
                vec![txt(order_id)],
                level,
            )
            .await?;
        let Some(r) = rows.rows.first() else {
            return Ok(None);
        };
        let lines = self
            .read(
                "SELECT line_no, product_id, quantity, price_cents FROM order_lines \
                 WHERE order_id = ? ORDER BY line_no",
                vec![txt(order_id)],
                level,
            )
            .await?;
        Ok(Some(Order {
            order_id: text(&r[0]),
            node_id: integer(&r[1]),
            status: text(&r[2]),
            total_cents: integer(&r[3]),
            lines: lines.rows.iter().map(|r| order_line(r)).collect(),
        }))
    }

    /// Everything the checker needs, read once this instance has applied
    /// log index `after`. Read again when a write was applied in between.
    pub async fn export(&self, after: u64) -> Result<Export, Error> {
        self.node
            .raft()
            .wait_applied(after, TIMEOUT)
            .await
            .map_err(|e| Error::ReadTimeout(e.to_string()))?;
        let reserve = self
            .reserve()
            .await
            .ok_or_else(|| Error::Read("the store is not set up".into()))?;
        loop {
            let index_before = self.node.stats().applied_index;
            let stock = self
                .read(
                    "SELECT p.product_id, p.price_cents, p.stock, s.available, \
                     (SELECT COUNT(*) FROM units u WHERE u.product_id = p.product_id), \
                     (SELECT COALESCE(SUM(quantity), 0) FROM reservations r \
                      WHERE r.product_id = p.product_id AND r.status = 'held') \
                     FROM products p JOIN stock s ON s.product_id = p.product_id \
                     ORDER BY p.product_id",
                    vec![],
                    ReadLevel::Local,
                )
                .await?;
            let orders = self
                .read(
                    "SELECT order_id, node_id, status, total_cents FROM orders ORDER BY order_id",
                    vec![],
                    ReadLevel::Local,
                )
                .await?;
            let lines = self
                .read(
                    "SELECT order_id, line_no, product_id, quantity, price_cents FROM order_lines \
                     ORDER BY order_id, line_no",
                    vec![],
                    ReadLevel::Local,
                )
                .await?;
            let units = self
                .read(
                    "SELECT order_id, product_id, COUNT(*) FROM order_units \
                     GROUP BY order_id, product_id ORDER BY order_id, product_id",
                    vec![],
                    ReadLevel::Local,
                )
                .await?;
            let index_after = self.node.stats().applied_index;
            if index_before != index_after {
                continue;
            }
            let mut by_order: BTreeMap<String, Vec<OrderLine>> = BTreeMap::new();
            for r in &lines.rows {
                by_order
                    .entry(text(&r[0]))
                    .or_default()
                    .push(order_line(&r[1..]));
            }
            return Ok(Export {
                node_id: self.node.id(),
                reserve,
                index_before,
                index_after,
                products: stock
                    .rows
                    .iter()
                    .map(|r| StockRow {
                        product_id: integer(&r[0]),
                        price_cents: integer(&r[1]),
                        stock: integer(&r[2]),
                        available: if reserve == Reserve::Units {
                            integer(&r[4])
                        } else {
                            integer(&r[3])
                        },
                        reserved: integer(&r[5]),
                    })
                    .collect(),
                orders: orders
                    .rows
                    .iter()
                    .map(|r| Order {
                        order_id: text(&r[0]),
                        node_id: integer(&r[1]),
                        status: text(&r[2]),
                        total_cents: integer(&r[3]),
                        lines: by_order.remove(&text(&r[0])).unwrap_or_default(),
                    })
                    .collect(),
                order_units: units
                    .rows
                    .iter()
                    .map(|r| (text(&r[0]), integer(&r[1]), integer(&r[2])))
                    .collect(),
            });
        }
    }

    async fn read(&self, sql: &str, params: Vec<Value>, level: ReadLevel) -> Result<Rows, Error> {
        self.node.query(st(sql, params), level).await
    }
}

/// How long a request waits for this instance to reach a log position.
const TIMEOUT: Duration = Duration::from_secs(10);

const CART_SQL: &str = "SELECT c.product_id, p.title, c.quantity, p.price_cents FROM cart_lines c \
     JOIN products p ON p.product_id = c.product_id WHERE c.cart_id = ? ORDER BY c.product_id";

/// Design B: deletes `line.quantity` units of the product, picked at
/// random among those this instance sees, so that concurrent checkouts on
/// other instances rarely pick the same units.
async fn take_units(tx: &mut Transaction, order_id: &str, line: &CartLine) -> Result<bool, Error> {
    let rows = tx
        .execute(st(
            "SELECT unit_id FROM units WHERE product_id = ? ORDER BY random() LIMIT ?",
            vec![int(line.product_id), int(line.quantity)],
        ))
        .await?;
    let units: Vec<i64> = rows.rows.iter().map(|r| integer(&r[0])).collect();
    if (units.len() as i64) < line.quantity {
        return Ok(false);
    }
    for unit in units {
        tx.execute(st("DELETE FROM units WHERE unit_id = ?", vec![int(unit)]))
            .await?;
        tx.execute(st(
            "INSERT INTO order_units VALUES (?, ?, ?)",
            vec![txt(order_id), int(unit), int(line.product_id)],
        ))
        .await?;
    }
    Ok(true)
}

/// Inserts the pending order and its lines and empties the cart. Returns the
/// order total.
async fn create_order(
    tx: &mut Transaction,
    node_id: u64,
    cart_id: &str,
    order_id: &str,
    lines: &[CartLine],
) -> Result<i64, Error> {
    let total: i64 = lines.iter().map(|l| l.quantity * l.price_cents).sum();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tx.execute(st(
        "INSERT INTO orders VALUES (?, ?, ?, 'pending', ?, ?)",
        vec![
            txt(order_id),
            int(node_id as i64),
            txt(cart_id),
            int(total),
            int(now),
        ],
    ))
    .await?;
    for (n, l) in lines.iter().enumerate() {
        tx.execute(st(
            "INSERT INTO order_lines VALUES (?, ?, ?, ?, ?)",
            vec![
                txt(order_id),
                int(n as i64 + 1),
                int(l.product_id),
                int(l.quantity),
                int(l.price_cents),
            ],
        ))
        .await?;
    }
    tx.execute(st(
        "DELETE FROM cart_lines WHERE cart_id = ?",
        vec![txt(cart_id)],
    ))
    .await?;
    Ok(total)
}

pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The outcome of a failed write. A statement that fails on a duplicate
/// order id is the client's mistake.
pub fn outcome_of(e: &Error) -> Outcome {
    match e {
        Error::Transaction(m) if m.contains("UNIQUE") || m.contains("PRIMARY KEY") => {
            Outcome::Invalid
        }
        Error::Transaction(_) => Outcome::Failed,
        Error::Rejected(_) => Outcome::Rejected,
        Error::Unavailable(_) | Error::Read(_) | Error::ReadTimeout(_) => Outcome::Unavailable,
        Error::Aborted(_) => Outcome::Aborted,
        Error::OutcomeUnknown(_) | Error::Internal(_) => Outcome::Unknown,
    }
}

fn failed(e: &Error) -> WriteAnswer {
    answer(outcome_of(e), e.to_string())
}

/// Design C's reservation failed: no order was created, whatever happened
/// to the reservation.
fn failed_before_order(e: &Error) -> WriteAnswer {
    let outcome = match outcome_of(e) {
        Outcome::Unknown => Outcome::Unavailable,
        o => o,
    };
    answer(outcome, format!("reserving stock: {e}"))
}

fn answer(outcome: Outcome, error: String) -> WriteAnswer {
    WriteAnswer {
        outcome,
        index: None,
        order_id: None,
        status: None,
        total_cents: None,
        error: Some(error),
    }
}

fn invalid(error: String) -> WriteAnswer {
    answer(Outcome::Invalid, error)
}

fn ok(index: u64) -> WriteAnswer {
    WriteAnswer {
        outcome: Outcome::Ok,
        index: Some(index),
        order_id: None,
        status: None,
        total_cents: None,
        error: None,
    }
}

fn order_answer(index: u64, status: &str, total: i64) -> WriteAnswer {
    WriteAnswer {
        status: Some(status.to_string()),
        total_cents: Some(total),
        ..ok(index)
    }
}

fn st(sql: &str, params: Vec<Value>) -> Statement {
    Statement::new(sql, params)
}

fn int(i: i64) -> Value {
    Value::Integer(i)
}

fn txt(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn integer(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Real(f) => *f as i64,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn product(r: &[Value]) -> Product {
    Product {
        product_id: integer(&r[0]),
        title: text(&r[1]),
        price_cents: integer(&r[2]),
        available: integer(&r[3]),
    }
}

fn cart_line(r: &[Value]) -> CartLine {
    CartLine {
        product_id: integer(&r[0]),
        title: text(&r[1]),
        quantity: integer(&r[2]),
        price_cents: integer(&r[3]),
    }
}

fn order_line(r: &[Value]) -> OrderLine {
    OrderLine {
        line_no: integer(&r[0]),
        product_id: integer(&r[1]),
        quantity: integer(&r[2]),
        price_cents: integer(&r[3]),
    }
}
