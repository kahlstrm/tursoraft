//! The store on three node processes over real TCP: a short load with a
//! flash sale on every instance, then the checker. With each way of
//! reserving stock the checker passes; with a store that oversells on
//! purpose it fails.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
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
