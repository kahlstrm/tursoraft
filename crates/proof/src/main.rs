//! ```text
//! tursoraft-proof node --config cluster.json --id N --dir DIR [--max-pending-groups N]
//! tursoraft-proof bench [--quick] [--only SECTION,...] [--pending N,...]
//! ```
//!
//! `node` runs one tursoraft node, as the `tursoraft-node` binary does. The
//! tests and the benchmark start one process per node with it.
//!
//! `bench` runs the latency and throughput benchmark and prints its tables.
//! Sections: `uniform`, `followers`, `geo`, `failover`, `move`. `--quick` runs a
//! small part of each, for checking that the benchmark works. `--pending`
//! lists the values of `max_pending_groups` each table is measured with
//! (default `1,256`: pipelining off, and the node's default).

use std::io::Write;
use std::path::PathBuf;

use tursoraft_raft::ClusterConfig;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("node") => node(&args[1..]),
        Some("bench") => tursoraft_proof::bench::main(&args[1..]),
        _ => usage("expected a command: node or bench"),
    }
}

#[tokio::main]
async fn node(args: &[String]) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let mut config_path = None;
    let mut id = None;
    let mut dir = None;
    let mut options = tursoraft_node::Options::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| usage(&format!("{arg} needs a value")));
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(value)),
            "--id" => id = Some(value.parse().unwrap_or_else(|_| usage("bad --id"))),
            "--dir" => dir = Some(PathBuf::from(value)),
            "--max-pending-groups" => {
                options.max_pending_groups = value
                    .parse()
                    .ok()
                    .filter(|n| *n >= 1)
                    .unwrap_or_else(|| usage("bad --max-pending-groups"))
            }
            other => usage(&format!("unknown argument {other}")),
        }
    }
    let config_path = config_path.unwrap_or_else(|| usage("--config is required"));
    let id = id.unwrap_or_else(|| usage("--id is required"));
    let dir = dir.unwrap_or_else(|| usage("--dir is required"));
    let text = std::fs::read_to_string(&config_path).expect("read the cluster config");
    let cluster: ClusterConfig = serde_json::from_str(&text).expect("parse the cluster config");
    let config = cluster
        .node_config(id, dir.join("raft"))
        .unwrap_or_else(|| usage(&format!("node {id} is not in the cluster config")));
    std::fs::create_dir_all(&dir).expect("create the node directory");
    let node = tursoraft_node::Node::start_with(config, dir.join("data.db"), options)
        .await
        .expect("start the node");
    println!("listening {}", node.raft().local_addr());
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}");
    eprintln!(
        "usage: tursoraft-proof node --config FILE --id N --dir DIR [--max-pending-groups N]"
    );
    eprintln!(
        "       tursoraft-proof bench [--quick] [--only uniform,followers,geo,failover,move] [--pending 1,256]"
    );
    std::process::exit(2);
}
