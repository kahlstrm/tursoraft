//! One tursoraft node in its own process.
//!
//! ```text
//! tursoraft-node --config cluster.json --id 1 --dir /tmp/n1
//! ```
//!
//! `cluster.json` is a [`tursoraft_raft::ClusterConfig`]. The node keeps its
//! Raft log in `DIR/raft` and its database in `DIR/data.db`, and prints
//! `listening <addr>` once it accepts connections. Reach it with
//! [`tursoraft_node::Client`].

use std::io::Write;
use std::path::PathBuf;

use tursoraft_raft::ClusterConfig;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let mut config_path = None;
    let mut id = None;
    let mut dir = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| usage(&format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(value())),
            "--id" => {
                id = Some(
                    value()
                        .parse()
                        .unwrap_or_else(|_| usage("--id must be a number")),
                )
            }
            "--dir" => dir = Some(PathBuf::from(value())),
            other => usage(&format!("unknown argument {other}")),
        }
    }
    let config_path = config_path.unwrap_or_else(|| usage("--config is required"));
    let id = id.unwrap_or_else(|| usage("--id is required"));
    let dir: PathBuf = dir.unwrap_or_else(|| usage("--dir is required"));

    let text = std::fs::read_to_string(&config_path).expect("read the cluster config");
    let cluster: ClusterConfig = serde_json::from_str(&text).expect("parse the cluster config");
    let config = cluster
        .node_config(id, dir.join("raft"))
        .unwrap_or_else(|| usage(&format!("node {id} is not in the cluster config")));
    std::fs::create_dir_all(&dir).expect("create the node directory");
    let node = tursoraft_node::Node::start(config, dir.join("data.db"))
        .await
        .expect("start the node");

    println!("listening {}", node.raft().local_addr());
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}");
    eprintln!("usage: tursoraft-node --config FILE --id N --dir DIR");
    std::process::exit(2);
}
