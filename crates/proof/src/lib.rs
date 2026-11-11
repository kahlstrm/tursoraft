//! Failure tests and a benchmark of tursoraft clusters. Everything here goes
//! through the public API of the other crates and runs one node process per
//! node (`tursoraft-proof node`).
//!
//! - [`cluster`] starts, kills, stops and resumes node processes.
//! - [`bank`] is the workload: transfers between accounts, each recorded
//!   under a key the client chose, and the checker of what the cluster kept.
//! - [`bench`] measures write latency and throughput against network delay.
//! - [`moves`] moves a running cluster's voters and leader to another region.
//!
//! ```text
//! cargo test -p tursoraft-proof -- --nocapture                    # the failure and move tests (about 4 min)
//! cargo test --release -p tursoraft-proof --test failures -- --ignored --nocapture
//!                                                                  # async loss, and the stale-answer defect
//! cargo run --release -p tursoraft-proof -- bench                 # the benchmark tables (about 25 min)
//! cargo run --release -p tursoraft-proof -- bench --quick         # a small part of each table (about 1 min)
//! cargo run --release -p tursoraft-proof -- bench --only move     # moving the voters between regions
//! ```

pub mod bank;
pub mod bench;
pub mod cluster;
pub mod moves;
