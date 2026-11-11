//! One Raft node with a [`CountingStateMachine`], in its own process.
//!
//! ```text
//! tursoraft-raft-node --config cluster.json --id 1 --dir /tmp/n1 [--record] [--reject-prefix BYTES]
//! ```
//!
//! `cluster.json` is a [`ClusterConfig`]. The node prints `listening <addr>`
//! once it accepts connections. A new cluster is formed with
//! `Client::initialize` on one node; non-voting followers are added with
//! `Client::add_follower`. `Client::app` returns the state machine's
//! [`tursoraft_raft::CountingStats`] as JSON.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use tursoraft_raft::{ClusterConfig, CountingOptions, CountingStateMachine, Node};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let mut config_path = None;
    let mut id = None;
    let mut dir = None;
    let mut opts = CountingOptions::default();
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
            "--record" => opts.record = true,
            "--reject-prefix" => opts.reject_prefix = Some(value().into_bytes()),
            other => usage(&format!("unknown argument {other}")),
        }
    }
    let config_path = config_path.unwrap_or_else(|| usage("--config is required"));
    let id = id.unwrap_or_else(|| usage("--id is required"));
    let dir = dir.unwrap_or_else(|| usage("--dir is required"));

    let text = std::fs::read_to_string(&config_path).expect("read the cluster config");
    let cluster: ClusterConfig = serde_json::from_str(&text).expect("parse the cluster config");
    let config = cluster
        .node_config(id, dir.clone())
        .unwrap_or_else(|| usage(&format!("node {id} is not in the cluster config")));

    std::fs::create_dir_all(&dir).expect("create the node directory");
    opts.path = Some(dir.join("state-machine"));
    let sm = CountingStateMachine::new(opts).expect("open the state machine");
    let stats = sm.clone();
    let node = Node::start(config, sm).await.expect("start the node");
    node.set_app_handler(Arc::new(move |_node, _request| {
        let mut v = serde_json::to_value(stats.stats()).expect("stats serialize");
        v["snapshots_installed"] = stats.snapshots_installed().into();
        let answer = serde_json::to_vec(&v).expect("stats serialize");
        Box::pin(async move { Ok(answer) })
    }));

    println!("listening {}", node.local_addr());
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}");
    eprintln!("usage: tursoraft-raft-node --config FILE --id N --dir DIR [--record] [--reject-prefix BYTES]");
    std::process::exit(2);
}
