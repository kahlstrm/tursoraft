//! The checker: after the load, reads every node's data through the admin
//! API and reports each violation of these rules.
//!
//! 1. Every node holds the same products, stock and orders.
//! 2. No product sold more than its stock: for each product, stock at setup
//!    = units for sale + units reserved and not yet claimed + units in
//!    orders that are not cancelled, and none of these is negative. With
//!    one row per unit, each order holds as many units as its lines say.
//! 3. Every order total equals the sum of its lines, and every order has a
//!    line.
//! 4. Every checkout that was acknowledged has its order on every node, with
//!    the total it was told.
//! 5. No order exists for a checkout that was told it had no effect.
//! 6. Every payment that was acknowledged left its order in the status it
//!    was told (`paid` or `cancelled`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, Instant};

use crate::api::{Export, Outcome, Reserve, Status};
use crate::load::Results;

pub struct Report {
    pub nodes: Vec<(u64, u64)>,
    pub orders: usize,
    pub acknowledged: usize,
    pub failed: usize,
    pub unknown: usize,
    pub sold: BTreeMap<i64, (i64, i64)>,
    pub violations: Vec<String>,
}

pub async fn run(admins: &[String], results: &[Results]) -> Report {
    let exports = converged_exports(admins).await;
    let mut violations = Vec::new();
    let first = &exports[0];
    for e in &exports[1..] {
        if e.products != first.products {
            violations.push(format!(
                "node {} and node {} differ in products or stock",
                first.node_id, e.node_id
            ));
        }
        if e.orders != first.orders {
            let a: BTreeSet<_> = first.orders.iter().map(|o| &o.order_id).collect();
            let b: BTreeSet<_> = e.orders.iter().map(|o| &o.order_id).collect();
            violations.push(format!(
                "node {} and node {} differ in orders: {} only on the first, {} only on the second",
                first.node_id,
                e.node_id,
                a.difference(&b).count(),
                b.difference(&a).count()
            ));
        }
        if e.order_units != first.order_units {
            violations.push(format!(
                "node {} and node {} differ in the units of orders",
                first.node_id, e.node_id
            ));
        }
    }
    let mut sold = BTreeMap::new();
    for e in &exports {
        sold = check_node(e, &mut violations);
    }
    let mut acknowledged = 0;
    let mut failed = 0;
    let mut unknown = 0;
    for e in &exports {
        let orders: BTreeMap<&str, _> = e.orders.iter().map(|o| (o.order_id.as_str(), o)).collect();
        for r in results {
            for c in &r.checkouts {
                let order = orders.get(c.order_id.as_str());
                match c.outcome {
                    Outcome::Ok => match order {
                        None => violations.push(format!(
                            "node {}: checkout {} on {} was acknowledged and its order is missing",
                            e.node_id, c.order_id, r.name
                        )),
                        Some(o) if c.total_cents.is_some_and(|t| t != o.total_cents) => violations
                            .push(format!(
                                "node {}: order {} has total {} and its checkout was told {:?}",
                                e.node_id, c.order_id, o.total_cents, c.total_cents
                            )),
                        Some(_) => {}
                    },
                    Outcome::Unknown => {}
                    outcome => {
                        if order.is_some() {
                            violations.push(format!(
                                "node {}: order {} exists and its checkout on {} was told {outcome:?}",
                                e.node_id, c.order_id, r.name
                            ));
                        }
                    }
                }
            }
            for p in &r.payments {
                let (Outcome::Ok, Some(status)) = (p.outcome, &p.status) else {
                    continue;
                };
                match orders.get(p.order_id.as_str()) {
                    Some(o) if &o.status == status => {}
                    other => violations.push(format!(
                        "node {}: order {} was told {status} and is {:?}",
                        e.node_id,
                        p.order_id,
                        other.map(|o| &o.status)
                    )),
                }
            }
        }
    }
    for r in results {
        for c in &r.checkouts {
            match c.outcome {
                Outcome::Ok => acknowledged += 1,
                Outcome::Unknown => unknown += 1,
                _ => failed += 1,
            }
        }
    }
    Report {
        nodes: exports.iter().map(|e| (e.node_id, e.index_after)).collect(),
        orders: first.orders.len(),
        acknowledged,
        failed,
        unknown,
        sold,
        violations,
    }
}

/// Rules 2 and 3 on one node. Returns units sold and stock per product, for
/// products that sold any.
fn check_node(e: &Export, violations: &mut Vec<String>) -> BTreeMap<i64, (i64, i64)> {
    let mut sold: BTreeMap<i64, i64> = BTreeMap::new();
    let mut units: BTreeMap<(&str, i64), i64> = BTreeMap::new();
    for (order_id, product_id, n) in &e.order_units {
        units.insert((order_id.as_str(), *product_id), *n);
    }
    for o in &e.orders {
        let total: i64 = o.lines.iter().map(|l| l.quantity * l.price_cents).sum();
        if o.lines.is_empty() {
            violations.push(format!(
                "node {}: order {} has no lines",
                e.node_id, o.order_id
            ));
        }
        if total != o.total_cents {
            violations.push(format!(
                "node {}: order {} has total {} and its lines sum to {total}",
                e.node_id, o.order_id, o.total_cents
            ));
        }
        if o.status == "cancelled" {
            continue;
        }
        for l in &o.lines {
            *sold.entry(l.product_id).or_default() += l.quantity;
            let taken = units.get(&(o.order_id.as_str(), l.product_id)).copied();
            if e.reserve == Reserve::Units && taken != Some(l.quantity) {
                violations.push(format!(
                    "node {}: order {} has {} of product {} and took {:?} units",
                    e.node_id, o.order_id, l.quantity, l.product_id, taken
                ));
            }
        }
    }
    let mut out = BTreeMap::new();
    for p in &e.products {
        let s = sold.get(&p.product_id).copied().unwrap_or(0);
        if p.available < 0 || p.reserved < 0 || s > p.stock {
            violations.push(format!(
                "node {}: product {} oversold: stock {}, sold {s}, available {}, reserved {}",
                e.node_id, p.product_id, p.stock, p.available, p.reserved
            ));
        }
        if p.available + p.reserved + s != p.stock {
            violations.push(format!(
                "node {}: product {}: available {} + reserved {} + sold {s} is not the stock {}",
                e.node_id, p.product_id, p.available, p.reserved, p.stock
            ));
        }
        if s > 0 {
            out.insert(p.product_id, (s, p.stock));
        }
    }
    out
}

/// The export of every node, once all have applied the same log index.
async fn converged_exports(admins: &[String]) -> Vec<Export> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let mut target = 0;
        for a in admins {
            let s: Status = get(&http, &format!("{a}/admin/status")).await;
            target = target.max(s.applied.unwrap_or(0));
        }
        let mut exports = Vec::new();
        for a in admins {
            exports.push(get::<Export>(&http, &format!("{a}/admin/export?after={target}")).await);
        }
        let indexes: BTreeSet<u64> = exports.iter().map(|e| e.index_after).collect();
        if indexes.len() == 1 {
            return exports;
        }
        assert!(
            Instant::now() < deadline,
            "the nodes did not reach the same log index: {indexes:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn get<T: serde::de::DeserializeOwned>(http: &reqwest::Client, url: &str) -> T {
    let r = http
        .get(url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {url}: {e}"));
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    assert!(status.is_success(), "GET {url}: {status}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("GET {url}: {e}: {body}"))
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\ncheck")?;
        for (id, index) in &self.nodes {
            writeln!(f, "  node {id}: applied index {index}")?;
        }
        writeln!(
            f,
            "  checkouts: {} acknowledged, {} told they failed, {} outcome unknown; {} orders on each node",
            self.acknowledged, self.failed, self.unknown, self.orders
        )?;
        for (product, (sold, stock)) in self.sold.iter().filter(|(p, _)| **p == 0) {
            writeln!(f, "  flash-sale product {product}: {sold} of {stock} sold")?;
        }
        if self.violations.is_empty() {
            writeln!(f, "  PASS: no violations")
        } else {
            for v in self.violations.iter().take(50) {
                writeln!(f, "  VIOLATION {v}")?;
            }
            writeln!(f, "  FAIL: {} violations", self.violations.len())
        }
    }
}
