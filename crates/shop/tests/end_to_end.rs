//! The store on three node processes over real TCP: a short load with a
//! flash sale on every instance, then the checker. With each way of
//! reserving stock the checker passes; with a store that oversells on
//! purpose it fails.
//!
//! And the store with the edge tier: one instance relays to a follower
//! Durable Object, run with the Worker under Miniflare (needs `npm ci` in
//! `edge/`), and the Worker's product page shows what the store sold.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;
use tursoraft_raft::{ClusterConfig, DelayTable, LogConfig};
use tursoraft_shop::api::Reserve;
use tursoraft_shop::{check, load};

const FLASH_STOCK: i64 = 20;

#[tokio::test(flavor = "multi_thread")]
async fn checker_passes_for_every_design_and_catches_oversell() {
    for reserve in [Reserve::Counter, Reserve::Units, Reserve::Leader] {
        let shop = Shop::start(reserve, &[]).await;
        let report = shop.load_and_check().await;
        println!("{} {report}", reserve.name());
        assert!(report.violations.is_empty(), "{:?}", report.violations);
        assert!(report.acknowledged > 0);
        assert_eq!(report.sold.get(&0), Some(&(FLASH_STOCK, FLASH_STOCK)));
    }

    let shop = Shop::start(Reserve::Counter, &["--break", "oversell"]).await;
    let report = shop.load_and_check().await;
    println!("oversell {report}");
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("product 0 oversold")),
        "{:?}",
        report.violations
    );
}

struct Shop {
    dir: TempDir,
    procs: Vec<Child>,
    http: Vec<String>,
    admin: Vec<String>,
}

impl Shop {
    /// Two voters and one non-voting follower, 10 ms apart, set up with
    /// `reserve`.
    async fn start(reserve: Reserve, args: &[&str]) -> Shop {
        Shop::start_with(reserve, args, &[]).await
    }

    /// [`Shop::start`], with `node_3_args` given to node 3 only.
    async fn start_with(reserve: Reserve, args: &[&str], node_3_args: &[&str]) -> Shop {
        let dir = TempDir::new().unwrap();
        let config = ClusterConfig {
            nodes: (1..=3)
                .map(|id| (id, format!("127.0.0.1:{}", free_port())))
                .collect(),
            delays: DelayTable::uniform(10.0),
            timeouts: None,
            batching: None,
            log: LogConfig::default(),
            propose_timeout_ms: 10_000,
        };
        let config_path = dir.path().join("cluster.json");
        std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut shop = Shop {
            procs: Vec::new(),
            http: Vec::new(),
            admin: Vec::new(),
            dir,
        };
        for id in 1..=3 {
            let http = format!("127.0.0.1:{}", free_port());
            let admin = format!("127.0.0.1:{}", free_port());
            let mut child = Command::new(env!("CARGO_BIN_EXE_tursoraft-shop"))
                .arg("serve")
                .arg("--config")
                .arg(&config_path)
                .args(["--id", &id.to_string(), "--http", &http, "--admin", &admin])
                .arg("--dir")
                .arg(shop.dir.path().join(format!("node-{id}")))
                .args(["--payment-ms", "0,20", "--decline", "0.2"])
                .args(args)
                .args(if id == 3 { node_3_args } else { &[] })
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut line = String::new();
            BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert!(line.starts_with("listening"), "node {id}: {line:?}");
            shop.procs.push(child);
            shop.http.push(format!("http://{http}"));
            shop.admin.push(format!("http://{admin}"));
        }
        let client = reqwest::Client::new();
        let post = |path: &str, body: serde_json::Value| {
            client
                .post(format!("{}{path}", shop.admin[0]))
                .json(&body)
                .send()
        };
        let r = post(
            "/admin/init",
            serde_json::json!({"voters": [1, 2], "followers": [3]}),
        )
        .await
        .unwrap();
        assert!(r.status().is_success(), "{}", r.text().await.unwrap());
        let r = post(
            "/admin/setup",
            serde_json::json!({
                "reserve": reserve, "products": 30, "stock": 50, "flash_stock": FLASH_STOCK,
            }),
        )
        .await
        .unwrap();
        assert!(r.status().is_success(), "{}", r.text().await.unwrap());
        let index = r.json::<serde_json::Value>().await.unwrap()["index"]
            .as_u64()
            .unwrap();
        for url in &shop.http {
            let r = client
                .get(format!("{url}/api/carts/warmup?after={index}"))
                .send()
                .await
                .unwrap();
            assert!(r.status().is_success());
        }
        shop
    }

    /// Runs the load on every instance at once, then the checker.
    async fn load_and_check(&self) -> check::Report {
        let runs = self.http.iter().enumerate().map(|(i, url)| {
            let config = load::Config {
                url: url.clone(),
                name: format!("n{}", i + 1),
                duration: Duration::from_secs(6),
                shoppers: 4,
                think_ms: 20,
                flash_at: Duration::from_secs(1),
                flash_shoppers: 4,
            };
            async move { load::run(&config).await }
        });
        let results = futures_util::future::join_all(runs).await;
        check::run(&self.admin, &results).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_edge_page_shows_the_stock_the_store_reports_at_the_log_index_of_a_purchase() {
    let edge = Edge::start();
    let secret_dir = TempDir::new().unwrap();
    let secret_file = secret_dir.path().join("relay-secret");
    std::fs::write(&secret_file, format!("{EDGE_SECRET}\n")).unwrap();
    let client = reqwest::Client::new();
    for reserve in [Reserve::Counter, Reserve::Units, Reserve::Leader] {
        // The relay runs on the non-voting follower; purchases go to node 1.
        let follower = format!("shop-{}", reserve.name());
        let relay = format!("{}/relay?follower={follower}", edge.ws_url());
        let secret = secret_file.to_str().unwrap();
        let shop = Shop::start_with(
            reserve,
            &[],
            &["--relay", &relay, "--relay-secret-file", secret],
        )
        .await;
        let store = &shop.http[0];
        let post = |path: String, body: serde_json::Value| {
            let request = client.post(format!("{store}{path}")).json(&body).send();
            async move {
                let answer: serde_json::Value = request.await.unwrap().json().await.unwrap();
                assert_eq!(answer["outcome"], "ok", "{path}: {answer}");
                answer
            }
        };
        // Buy 3 of the flash-sale product and pay, then buy 2 and have the
        // payment declined, which gives them back.
        let mut index = 0;
        for (order, quantity, approved) in [("bought", 3, true), ("declined", 2, false)] {
            let cart = format!("cart-{order}");
            let a = post(
                format!("/api/carts/{cart}/lines"),
                serde_json::json!({"product_id": 0, "quantity": quantity}),
            )
            .await;
            let a = post(
                "/api/checkout".into(),
                serde_json::json!({"cart_id": cart, "order_id": order, "after": a["index"]}),
            )
            .await;
            // The stock is taken while the order is pending.
            let pending = a["index"].as_u64().unwrap();
            let expected = FLASH_STOCK - 3 - if approved { 0 } else { quantity };
            let (store_stock, edge_stock) = shop.stock_both_ways(&edge, &follower, pending).await;
            println!(
                "{} {order} pending: store {store_stock}, edge {edge_stock}",
                reserve.name()
            );
            assert_eq!(
                store_stock,
                expected,
                "{} {order}: the store's stock",
                reserve.name()
            );
            assert_eq!(
                edge_stock,
                store_stock,
                "{} {order} pending",
                reserve.name()
            );
            let a = post(
                format!("/api/orders/{order}/pay"),
                serde_json::json!({"approved": approved, "after": pending}),
            )
            .await;
            index = a["index"].as_u64().unwrap();
        }
        let (store_stock, edge_stock) = shop.stock_both_ways(&edge, &follower, index).await;
        println!(
            "{} at the end: store {store_stock}, edge {edge_stock}",
            reserve.name()
        );
        assert_eq!(
            store_stock,
            FLASH_STOCK - 3,
            "{}: the store's stock",
            reserve.name()
        );
        assert_eq!(
            edge_stock,
            store_stock,
            "{}: the edge's stock",
            reserve.name()
        );
    }
}

const EDGE_SECRET: &str = "edge-test-secret";

impl Shop {
    /// The stock of the flash-sale product once log index `index` is
    /// applied: on node 1's product page, and on the Worker's page with
    /// `?at=index`, read from `follower`.
    async fn stock_both_ways(&self, edge: &Edge, follower: &str, index: u64) -> (i64, i64) {
        let client = reqwest::Client::new();
        // A read at `index` waits until node 1 has applied it.
        let r = client
            .get(format!("{}/api/carts/sync?after={index}", self.http[0]))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
        let store: serde_json::Value = client
            .get(format!("{}/api/products/0", self.http[0]))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let r = client
            .get(format!(
                "{}/products/0?follower={follower}&at={index}&wait_ms=20000",
                edge.url
            ))
            .send()
            .await
            .unwrap();
        let status = r.status();
        let through: u64 = r.headers()["x-follower-through"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let page = r.text().await.unwrap();
        assert_eq!(status, 200, "{page}");
        assert!(through >= index, "read at {through}, asked for {index}");
        assert!(page.contains("Flash sale: limited sneaker"), "{page}");
        let shown = page
            .split(r#"<b id="stock">"#)
            .nth(1)
            .and_then(|rest| rest.split("</b>").next())
            .unwrap_or_else(|| panic!("no stock on the page: {page}"));
        (store["available"].as_i64().unwrap(), shown.parse().unwrap())
    }
}

/// The Worker and follower Durable Objects under Miniflare, started by
/// `edge/test/serve.mjs`, which runs until its stdin closes.
struct Edge {
    url: String,
    child: Child,
    stdin: Option<ChildStdin>,
}

impl Edge {
    fn start() -> Edge {
        let edge = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../edge");
        assert!(
            Path::new(&edge.join("node_modules/miniflare")).exists(),
            "Miniflare is not installed: run `npm ci` in {}",
            edge.display()
        );
        let mut child = Command::new("node")
            .arg(edge.join("test/serve.mjs"))
            .env("RELAY_SECRET", EDGE_SECRET)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Its own group, so that Drop can stop Miniflare's workerd too.
            .process_group(0)
            .spawn()
            .expect("run node");
        let stdin = child.stdin.take().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let url = serde_json::from_str::<serde_json::Value>(&line)
            .unwrap_or_else(|e| panic!("serve.mjs printed {line:?}: {e}"))["url"]
            .as_str()
            .unwrap()
            .to_string();
        Edge {
            url,
            child,
            stdin: Some(stdin),
        }
    }

    fn ws_url(&self) -> String {
        self.url.replacen("http://", "ws://", 1)
    }
}

/// Closes serve.mjs's stdin, which stops Miniflare and its workerd, waits
/// for it a little, then kills its whole process group. Killing node alone
/// leaves workerd running.
impl Drop for Edge {
    fn drop(&mut self) {
        drop(self.stdin.take());
        for _ in 0..100 {
            if !matches!(self.child.try_wait(), Ok(None)) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.child.id())])
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
    }
}

impl Drop for Shop {
    fn drop(&mut self) {
        for p in &mut self.procs {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
