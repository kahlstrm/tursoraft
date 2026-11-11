//! A small store on a tursoraft cluster: each instance is one process that
//! embeds a [`tursoraft_node::Node`] and serves HTTP. The same binary also
//! drives load against instances and checks what the cluster kept.
//!
//! - [`store`]: the schema and every database operation.
//! - [`server`]: the HTTP API on top of it.
//! - [`load`]: the load generator, a shopping mix plus a flash sale.
//! - [`check`]: the checker that runs after the load.

pub mod api;
pub mod check;
pub mod load;
pub mod server;
pub mod store;
