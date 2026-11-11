//! ```text
//! tursoraft-shop serve --config cluster.json --id N --dir DIR --http ADDR --admin ADDR
//!                      [--name NAME] [--cart-mode async|quorum] [--payment-ms LO,HI]
//!                      [--decline P] [--local-hold-ms MS] [--break ack-rejected|oversell]
//!                      [--relay URL --relay-secret-file FILE]
//! tursoraft-shop load  --url URL --name NAME --out FILE [--duration S] [--shoppers N]
//!                      [--think-ms MS] [--flash-at S] [--flash-shoppers N]
//! tursoraft-shop check --admin URL [--admin URL ...] RESULTS.json ...
//! ```
//!
//! `serve` runs one store instance: a tursoraft node (Raft on the address
//! the cluster config gives node N), the shop on `--http`, and the admin API
//! on `--admin`. `cluster.json` is a [`tursoraft_raft::ClusterConfig`]. The
//! node keeps its Raft log in `DIR/raft` and its database in `DIR/data.db`.
//!
//! `--relay` also runs a relay ([`tursoraft_relay`]) in the instance: it sends
//! the changes this node applies to a reads-only follower Durable Object, at
//! the Worker URL `URL` (`wss://HOST/relay?follower=NAME`, see `edge/`).
//! The relay's secret is read from `FILE`, never from the command line, so
//! that it does not show in the process list or a service's unit file. Run
//! one relay per follower name: a second one takes the follower over.
//!
//! `load` drives one instance's HTTP API and writes what every write request
//! was told to `--out`. `check` reads every node's data through the admin API,
//! compares it with the load results and exits with status 1 on a violation.
//!
//! `--break` makes the instance misbehave on purpose, to show that the
//! checker catches it. Never use it otherwise.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tursoraft_node::CommitMode;
use tursoraft_raft::ClusterConfig;
use tursoraft_shop::server::{self, App};
use tursoraft_shop::store::Store;
use tursoraft_shop::{check, load};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rest = args.get(1..).unwrap_or_default();
    let runtime = tokio::runtime::Runtime::new().expect("start the async runtime");
    match args.first().map(String::as_str) {
        Some("serve") => runtime.block_on(serve(rest)),
        Some("load") => runtime.block_on(load_main(rest)),
        Some("check") => {
            let ok = runtime.block_on(check_main(rest));
            std::process::exit(if ok { 0 } else { 1 });
        }
        _ => usage("expected a command: serve, load or check"),
    }
}

async fn serve(args: &[String]) {
    let mut config_path = None;
    let mut id = None;
    let mut dir = None;
    let mut http = None;
    let mut admin = None;
    let mut name = None;
    let mut cart_mode = CommitMode::Async;
    let mut payment_ms = (200, 800);
    let mut decline = 0.05;
    let mut options = tursoraft_node::Options::default();
    let mut breaks = tursoraft_shop::store::Breaks::default();
    let mut relay_url = None;
    let mut relay_secret_file = None;
    for (arg, value) in pairs(args) {
        match arg {
            "--config" => config_path = Some(PathBuf::from(value)),
            "--id" => id = Some(number(arg, value)),
            "--dir" => dir = Some(PathBuf::from(value)),
            "--http" => http = Some(value.to_string()),
            "--admin" => admin = Some(value.to_string()),
            "--name" => name = Some(value.to_string()),
            "--cart-mode" => {
                cart_mode = match value {
                    "async" => CommitMode::Async,
                    "quorum" => CommitMode::Quorum,
                    _ => usage("--cart-mode is async or quorum"),
                }
            }
            "--payment-ms" => {
                let (lo, hi) = value
                    .split_once(',')
                    .unwrap_or_else(|| usage("--payment-ms is LO,HI"));
                payment_ms = (number(arg, lo), number(arg, hi));
            }
            "--decline" => decline = number(arg, value),
            "--local-hold-ms" => options.local_hold_ms = number(arg, value),
            "--relay" => relay_url = Some(value.to_string()),
            "--relay-secret-file" => relay_secret_file = Some(PathBuf::from(value)),
            "--break" => match value {
                "ack-rejected" => breaks.ack_rejected = true,
                "oversell" => breaks.oversell = true,
                _ => usage("--break is ack-rejected or oversell"),
            },
            _ => usage(&format!("unknown argument {arg}")),
        }
    }
    let config_path = config_path.unwrap_or_else(|| usage("--config is required"));
    let id: u64 = id.unwrap_or_else(|| usage("--id is required"));
    let dir = dir.unwrap_or_else(|| usage("--dir is required"));
    let http = http.unwrap_or_else(|| usage("--http is required"));
    let admin = admin.unwrap_or_else(|| usage("--admin is required"));
    let relay = match (relay_url, relay_secret_file) {
        (Some(url), Some(file)) => Some(tursoraft_relay::Config::new(url, read_secret(&file))),
        (None, None) => None,
        _ => usage("--relay and --relay-secret-file go together"),
    };

    let text = std::fs::read_to_string(&config_path).expect("read the cluster config");
    let cluster: ClusterConfig = serde_json::from_str(&text).expect("parse the cluster config");
    let config = cluster
        .node_config(id, dir.join("raft"))
        .unwrap_or_else(|| usage(&format!("node {id} is not in the cluster config")));
    std::fs::create_dir_all(&dir).expect("create the node directory");
    let node = tursoraft_node::Node::start_with(config, dir.join("data.db"), options)
        .await
        .expect("start the node");
    let mut store = Store::new(node.clone(), name.unwrap_or_else(|| format!("node-{id}")));
    store.cart_mode = cart_mode;
    store.payment_ms = payment_ms;
    store.decline = decline;
    store.breaks = breaks;
    let app = Arc::new(App { store, cluster });
    // Runs until this function returns.
    let _relay = relay.map(|config| {
        tracing::info!("relaying this node's changes to {}", config.url);
        tursoraft_relay::Relay::start(node.clone(), config)
    });

    let http = tokio::net::TcpListener::bind(&http)
        .await
        .unwrap_or_else(|e| panic!("bind {http}: {e}"));
    let admin = tokio::net::TcpListener::bind(&admin)
        .await
        .unwrap_or_else(|e| panic!("bind {admin}: {e}"));
    println!(
        "listening raft {} http {} admin {}",
        node.raft().local_addr(),
        http.local_addr().unwrap(),
        admin.local_addr().unwrap()
    );
    std::io::stdout().flush().unwrap();
    let public = axum::serve(http, server::public(app.clone()));
    let admin = axum::serve(admin, server::admin(app));
    let (a, b) = tokio::join!(public, admin);
    a.expect("serve http");
    b.expect("serve admin");
}

async fn load_main(args: &[String]) {
    let mut config = load::Config::default();
    let mut out = None;
    for (arg, value) in pairs(args) {
        match arg {
            "--url" => config.url = value.trim_end_matches('/').to_string(),
            "--name" => config.name = value.to_string(),
            "--out" => out = Some(PathBuf::from(value)),
            "--duration" => config.duration = Duration::from_secs_f64(number(arg, value)),
            "--shoppers" => config.shoppers = number(arg, value),
            "--think-ms" => config.think_ms = number(arg, value),
            "--flash-at" => config.flash_at = Duration::from_secs_f64(number(arg, value)),
            "--flash-shoppers" => config.flash_shoppers = number(arg, value),
            _ => usage(&format!("unknown argument {arg}")),
        }
    }
    if config.url.is_empty() || config.name.is_empty() {
        usage("--url and --name are required");
    }
    let out = out.unwrap_or_else(|| usage("--out is required"));
    let results = load::run(&config).await;
    std::fs::write(&out, serde_json::to_vec_pretty(&results).unwrap())
        .unwrap_or_else(|e| panic!("write {}: {e}", out.display()));
    print!("{}", load::table(std::slice::from_ref(&results)));
}

async fn check_main(args: &[String]) -> bool {
    let mut admins = Vec::new();
    let mut files = Vec::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == "--admin" {
            let url = it.next().unwrap_or_else(|| usage("--admin needs a value"));
            admins.push(url.trim_end_matches('/').to_string());
        } else if arg.starts_with("--") {
            usage(&format!("unknown argument {arg}"));
        } else {
            files.push(PathBuf::from(arg));
        }
    }
    if admins.is_empty() {
        usage("give at least one --admin URL");
    }
    let results: Vec<load::Results> = files
        .iter()
        .map(|f| {
            let bytes = std::fs::read(f).unwrap_or_else(|e| panic!("read {}: {e}", f.display()));
            serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", f.display()))
        })
        .collect();
    print!("{}", load::table(&results));
    let report = check::run(&admins, &results).await;
    print!("{report}");
    report.violations.is_empty()
}

/// The relay's secret: the whole file, without trailing white space. An
/// error names the file, never its contents.
fn read_secret(file: &std::path::Path) -> String {
    let text = std::fs::read_to_string(file)
        .unwrap_or_else(|e| usage(&format!("read --relay-secret-file {}: {e}", file.display())));
    let secret = text.trim_end().to_string();
    if secret.is_empty() {
        usage(&format!("--relay-secret-file {} is empty", file.display()));
    }
    secret
}

fn pairs(args: &[String]) -> impl Iterator<Item = (&str, &str)> {
    args.chunks(2).map(|c| match c {
        [arg, value] => (arg.as_str(), value.as_str()),
        [arg] => usage(&format!("{arg} needs a value")),
        _ => unreachable!(),
    })
}

fn number<T: std::str::FromStr>(arg: &str, value: &str) -> T {
    value
        .parse()
        .unwrap_or_else(|_| usage(&format!("{arg} must be a number, not {value:?}")))
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}");
    eprintln!(
        "usage: tursoraft-shop serve --config FILE --id N --dir DIR --http ADDR --admin ADDR \
         [--name NAME] [--cart-mode async|quorum] [--payment-ms LO,HI] [--decline P] \
         [--local-hold-ms MS] [--break ack-rejected|oversell] \
         [--relay URL --relay-secret-file FILE]"
    );
    eprintln!(
        "       tursoraft-shop load --url URL --name NAME --out FILE [--duration S] [--shoppers N] \
         [--think-ms MS] [--flash-at S] [--flash-shoppers N]"
    );
    eprintln!("       tursoraft-shop check --admin URL [--admin URL ...] RESULTS.json ...");
    std::process::exit(2);
}
