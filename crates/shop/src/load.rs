//! The load generator: shoppers with a mostly-read mix on one instance, and
//! a flash sale of one product with little stock.
//!
//! Each shopper waits a random think time (0 to 2 × `think_ms`) and then
//! sends one request, chosen by these shares:
//!
//! | Request | Share | Read level or commit mode |
//! | --- | --- | --- |
//! | catalog page, 24 products | 34% | local |
//! | product page | 26% | local |
//! | cart page | 10% | at the shopper's last write |
//! | order status | 6% | at the order's checkout |
//! | add to cart | 16% | the instance's cart mode (`async` by default) |
//! | checkout, then pay | 8% | interactive, `quorum`; pay is one `execute` in `quorum` |
//!
//! From `flash_at` on, flash shoppers buy the flash-sale product one unit at
//! a time: add it to a new cart, check out (up to 5 tries when the commit
//! is rejected or aborted) and pay. A flash shopper stops once it is told the
//! product is sold out.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::api::{AddToCart, Cart, CheckoutRequest, Outcome, PayRequest, Product, WriteAnswer};
use crate::store::FLASH_PRODUCT;

#[derive(Debug, Clone)]
pub struct Config {
    /// The instance's HTTP address, such as `http://127.0.0.1:8080`.
    pub url: String,
    /// Names this instance in the results, and prefixes every id this load
    /// creates, so loads on several instances never choose the same ids.
    pub name: String,
    pub duration: Duration,
    pub shoppers: usize,
    pub think_ms: u64,
    pub flash_at: Duration,
    pub flash_shoppers: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            url: String::new(),
            name: String::new(),
            duration: Duration::from_secs(60),
            shoppers: 16,
            think_ms: 100,
            flash_at: Duration::from_secs(20),
            flash_shoppers: 8,
        }
    }
}

/// What one load run saw.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Results {
    pub name: String,
    pub url: String,
    pub started_unix_ms: u64,
    pub seconds: f64,
    pub kinds: BTreeMap<String, KindStats>,
    /// Every checkout, with the order id the shopper chose and what it was
    /// told.
    pub checkouts: Vec<CheckoutRecord>,
    /// Every payment of an order that was acknowledged.
    pub payments: Vec<PayRecord>,
    /// Cart pages read at the shopper's last write that did not show it.
    pub stale_cart_reads: u64,
    /// Units of the flash-sale product this instance's shoppers bought and
    /// paid for.
    pub flash_units_paid: u64,
    /// Seconds from the flash sale's start until a shopper here was first
    /// told it was sold out.
    pub flash_sold_out_after: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KindStats {
    pub count: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Requests per outcome: `ok`, an [`Outcome`] for writes, or `http NNN`
    /// and `error` for failed reads.
    pub outcomes: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutRecord {
    pub order_id: String,
    pub outcome: Outcome,
    pub flash: bool,
    #[serde(default)]
    pub total_cents: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayRecord {
    pub order_id: String,
    pub outcome: Outcome,
    /// `paid` or `cancelled` when the outcome is `ok`.
    pub status: Option<String>,
}

#[derive(Default)]
struct Recorder {
    samples: BTreeMap<&'static str, Vec<(f64, String)>>,
    checkouts: Vec<CheckoutRecord>,
    payments: Vec<PayRecord>,
    stale_cart_reads: u64,
    flash_units_paid: u64,
    flash_sold_out: Option<Instant>,
}

struct Shared {
    config: Config,
    http: reqwest::Client,
    products: i64,
    rec: Mutex<Recorder>,
    start: Instant,
    deadline: Instant,
}

pub async fn run(config: &Config) -> Results {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(config.shoppers + config.flash_shoppers)
        .build()
        .unwrap();
    let catalog: Vec<Product> = http
        .get(format!("{}/api/products?limit=100000", config.url))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .unwrap_or_else(|e| panic!("read the catalog of {}: {e}", config.url))
        .json()
        .await
        .expect("parse the catalog");
    let products = catalog.iter().map(|p| p.product_id).max().unwrap_or(0);
    assert!(products >= 1, "the store at {} has no products", config.url);
    let started_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start = Instant::now();
    let shared = Arc::new(Shared {
        config: config.clone(),
        http,
        products,
        rec: Mutex::default(),
        start,
        deadline: start + config.duration,
    });
    let mut tasks = Vec::new();
    for i in 0..config.shoppers {
        tasks.push(tokio::spawn(shopper(shared.clone(), i)));
    }
    for i in 0..config.flash_shoppers {
        tasks.push(tokio::spawn(flash_shopper(shared.clone(), i)));
    }
    for t in tasks {
        t.await.expect("a shopper panicked");
    }
    let rec = std::mem::take(&mut *shared.rec.lock().unwrap());
    let flash_start = start + config.flash_at;
    Results {
        name: config.name.clone(),
        url: config.url.clone(),
        started_unix_ms,
        seconds: start.elapsed().as_secs_f64(),
        kinds: rec
            .samples
            .into_iter()
            .map(|(k, s)| (k.to_string(), stats(s)))
            .collect(),
        checkouts: rec.checkouts,
        payments: rec.payments,
        stale_cart_reads: rec.stale_cart_reads,
        flash_units_paid: rec.flash_units_paid,
        flash_sold_out_after: rec
            .flash_sold_out
            .map(|t| t.saturating_duration_since(flash_start).as_secs_f64()),
    }
}

/// One shopper's session, repeated until the deadline.
async fn shopper(s: Arc<Shared>, i: usize) {
    let mut rng = StdRng::from_entropy();
    let name = &s.config.name;
    let mut carts = 0;
    let mut cart_id = format!("{name}-s{i}-c{carts}");
    let mut cart: BTreeMap<i64, i64> = BTreeMap::new();
    // The log position of this shopper's last write.
    let mut after = 0;
    let mut orders: Vec<(String, u64)> = Vec::new();
    let mut attempts = 0;
    while Instant::now() < s.deadline {
        let think = rng.gen_range(0..=2 * s.config.think_ms);
        tokio::time::sleep(Duration::from_millis(think)).await;
        let roll = rng.gen_range(0..100);
        if roll < 34 {
            let offset = rng.gen_range(0..s.products.max(24) - 23);
            s.read(
                "catalog",
                &format!("/api/products?offset={offset}&limit=24"),
            )
            .await;
        } else if roll < 60 {
            let id = rng.gen_range(1..=s.products);
            s.read("product", &format!("/api/products/{id}")).await;
        } else if roll < 70 {
            let path = format!("/api/carts/{cart_id}?after={after}");
            if let Some(body) = s.read("cart", &path).await {
                let seen: Option<Cart> = serde_json::from_str(&body).ok();
                let seen: Option<BTreeMap<i64, i64>> =
                    seen.map(|c| c.lines.iter().map(|l| (l.product_id, l.quantity)).collect());
                if seen.as_ref() != Some(&cart) {
                    s.rec.lock().unwrap().stale_cart_reads += 1;
                }
            }
        } else if roll < 76 && !orders.is_empty() {
            let (order_id, index) = &orders[rng.gen_range(0..orders.len())];
            s.read("order", &format!("/api/orders/{order_id}?after={index}"))
                .await;
        } else if roll < 92 || cart.is_empty() {
            let product_id = rng.gen_range(1..=s.products);
            let quantity = rng.gen_range(1..=3);
            let a = s
                .write(
                    "add_to_cart",
                    &format!("/api/carts/{cart_id}/lines"),
                    &AddToCart {
                        product_id,
                        quantity,
                    },
                )
                .await;
            match a.outcome {
                Outcome::Ok => {
                    *cart.entry(product_id).or_default() += quantity;
                    after = after.max(a.index.unwrap_or(0));
                }
                // The cart may or may not hold the line: start a new one.
                Outcome::Unknown => {
                    carts += 1;
                    cart_id = format!("{name}-s{i}-c{carts}");
                    cart.clear();
                }
                _ => {}
            }
        } else {
            let order_id = format!("{name}-s{i}-o{attempts}");
            attempts += 1;
            let a = s.checkout(&cart_id, &order_id, after, false).await;
            match a.outcome {
                Outcome::Ok => {
                    let index = a.index.unwrap_or(0);
                    after = after.max(index);
                    orders.push((order_id.clone(), index));
                    cart.clear();
                    s.pay(&order_id, index, false).await;
                }
                Outcome::Unknown => {
                    carts += 1;
                    cart_id = format!("{name}-s{i}-c{carts}");
                    cart.clear();
                }
                _ => {}
            }
        }
    }
}

/// Buys the flash-sale product one unit at a time until it is sold out.
async fn flash_shopper(s: Arc<Shared>, i: usize) {
    let mut rng = StdRng::from_entropy();
    let name = &s.config.name;
    let start = s.start + s.config.flash_at;
    tokio::time::sleep_until(start.into()).await;
    let mut n = 0;
    'buy: while Instant::now() < s.deadline {
        n += 1;
        let cart_id = format!("{name}-f{i}-c{n}");
        let a = s
            .write(
                "flash_add",
                &format!("/api/carts/{cart_id}/lines"),
                &AddToCart {
                    product_id: FLASH_PRODUCT,
                    quantity: 1,
                },
            )
            .await;
        if a.outcome != Outcome::Ok {
            continue;
        }
        let after = a.index.unwrap_or(0);
        for attempt in 0..5 {
            if Instant::now() >= s.deadline {
                break 'buy;
            }
            let order_id = format!("{name}-f{i}-c{n}-a{attempt}");
            let a = s.checkout(&cart_id, &order_id, after, true).await;
            match a.outcome {
                Outcome::Ok => {
                    let paid = s.pay(&order_id, a.index.unwrap_or(0), true).await;
                    if paid {
                        s.rec.lock().unwrap().flash_units_paid += 1;
                    }
                    break;
                }
                Outcome::SoldOut => {
                    let mut rec = s.rec.lock().unwrap();
                    rec.flash_sold_out.get_or_insert_with(Instant::now);
                    break 'buy;
                }
                Outcome::Rejected | Outcome::Aborted => {
                    let backoff = rng.gen_range(0..=20);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
                _ => break,
            }
        }
    }
}

impl Shared {
    /// A GET; returns the body when the answer is 200.
    async fn read(&self, kind: &'static str, path: &str) -> Option<String> {
        let t = Instant::now();
        let res = self
            .http
            .get(format!("{}{path}", self.config.url))
            .send()
            .await;
        let (outcome, body) = match res {
            Ok(r) if r.status().is_success() => match r.text().await {
                Ok(body) => ("ok".to_string(), Some(body)),
                Err(_) => ("error".to_string(), None),
            },
            Ok(r) => (format!("http {}", r.status().as_u16()), None),
            Err(_) => ("error".to_string(), None),
        };
        self.record(kind, t, outcome);
        body
    }

    /// A POST of a write request. A request that got no answer counts as
    /// [`Outcome::Unknown`]: it may have taken effect.
    async fn write<T: Serialize>(&self, kind: &'static str, path: &str, body: &T) -> WriteAnswer {
        let t = Instant::now();
        let res = self
            .http
            .post(format!("{}{path}", self.config.url))
            .json(body)
            .send()
            .await;
        let answer = match res {
            Ok(r) => r.json::<WriteAnswer>().await.ok(),
            Err(_) => None,
        };
        let answer = answer.unwrap_or(WriteAnswer {
            outcome: Outcome::Unknown,
            index: None,
            order_id: None,
            status: None,
            total_cents: None,
            error: Some("no answer".into()),
        });
        let outcome = serde_json::to_value(answer.outcome).unwrap();
        self.record(kind, t, outcome.as_str().unwrap().to_string());
        answer
    }

    async fn checkout(
        &self,
        cart_id: &str,
        order_id: &str,
        after: u64,
        flash: bool,
    ) -> WriteAnswer {
        let kind = if flash { "flash_checkout" } else { "checkout" };
        let req = CheckoutRequest {
            cart_id: cart_id.to_string(),
            order_id: Some(order_id.to_string()),
            after,
        };
        let a = self.write(kind, "/api/checkout", &req).await;
        self.rec.lock().unwrap().checkouts.push(CheckoutRecord {
            order_id: order_id.to_string(),
            outcome: a.outcome,
            flash,
            total_cents: a.total_cents,
        });
        a
    }

    /// Pays the order; true when it was paid. The provider's answer is left
    /// to the store, which declines a share of payments.
    async fn pay(&self, order_id: &str, after: u64, flash: bool) -> bool {
        let kind = if flash { "flash_pay" } else { "pay" };
        let req = PayRequest {
            approved: None,
            after,
        };
        let a = self
            .write(kind, &format!("/api/orders/{order_id}/pay"), &req)
            .await;
        let paid = a.outcome == Outcome::Ok && a.status.as_deref() == Some("paid");
        self.rec.lock().unwrap().payments.push(PayRecord {
            order_id: order_id.to_string(),
            outcome: a.outcome,
            status: a.status,
        });
        paid
    }

    fn record(&self, kind: &'static str, t: Instant, outcome: String) {
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let mut rec = self.rec.lock().unwrap();
        rec.samples.entry(kind).or_default().push((ms, outcome));
    }
}

fn stats(mut samples: Vec<(f64, String)>) -> KindStats {
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    let pct = |p: f64| {
        let i = ((samples.len() as f64 * p).ceil() as usize).clamp(1, samples.len()) - 1;
        samples[i].0
    };
    let mut outcomes = BTreeMap::new();
    for (_, o) in &samples {
        *outcomes.entry(o.clone()).or_default() += 1;
    }
    KindStats {
        count: samples.len() as u64,
        p50_ms: pct(0.5),
        p99_ms: pct(0.99),
        max_ms: samples.last().unwrap().0,
        outcomes,
    }
}

/// One table of every instance's results: per request kind, the count, the
/// median and 99th-percentile latency, and the outcomes.
pub fn table(results: &[Results]) -> String {
    let mut out = String::new();
    for r in results {
        let _ = writeln!(
            out,
            "\n{} ({}), {:.0} s, stale cart reads {}, flash units paid {}, flash sold out after {}",
            r.name,
            r.url,
            r.seconds,
            r.stale_cart_reads,
            r.flash_units_paid,
            r.flash_sold_out_after
                .map_or("-".to_string(), |s| format!("{s:.1} s")),
        );
        let _ = writeln!(
            out,
            "  {:<15} {:>7} {:>9} {:>9}  outcomes",
            "request", "count", "p50 ms", "p99 ms"
        );
        for (kind, k) in &r.kinds {
            let outcomes: Vec<String> =
                k.outcomes.iter().map(|(o, n)| format!("{o} {n}")).collect();
            let _ = writeln!(
                out,
                "  {:<15} {:>7} {:>9.1} {:>9.1}  {}",
                kind,
                k.count,
                k.p50_ms,
                k.p99_ms,
                outcomes.join(", ")
            );
        }
    }
    out
}
