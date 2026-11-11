# tursoraft

Research prototype: every application instance embeds a Turso database and a
Raft node. The cluster replicates the rows each transaction changed, read from
Turso's change data capture (CDC), through Raft (openraft 0.10.0).

| Crate | Path | Role |
|---|---|---|
| `tursoraft-statemachine` | `crates/statemachine` | The `StateMachine` trait and types between Raft and the database. Change only by agreement of both sides. |
| `tursoraft-changeset` | `crates/changeset` | Capture and apply change sets on Turso. No openraft. |
| `tursoraft-raft` | `crates/raft` | Raft on openraft. No Turso. |
| `tursoraft-node` | `crates/node` | Joins the two. |
| `tursoraft-shop` | `crates/shop` | A small store on a cluster, its load generator and its checker. |

Turso comes from its Git repository at commit
`0c9fe0dd94c14a4f00106bffa9c9433d5965fd3a` (0.9.0-pre.5), as Git
dependencies. This workspace is not a member of Turso's workspace. It uses
resolver 3, so the lockfile only picks dependency versions that support Rust
1.88.

## Build

```bash
git clone https://github.com/kahlstrm/tursoraft
cd tursoraft
cargo test --workspace
```

The first build fetches the Turso repository into Cargo's Git cache.

`cargo test -p tursoraft-changeset -- --nocapture --test-threads=1` prints
what CDC captures (`tests/cdc_capture.rs`) and replays a leader's change sets
onto another database with the sync engine's `DatabaseReplaySession`
(`tests/replay.rs`).

## The store

`tursoraft-shop serve` runs one store instance: a tursoraft node and an
HTTP server in one process. `tursoraft-shop load` drives an instance with
shoppers and a flash sale, and `tursoraft-shop check` checks afterwards that
nothing was lost or oversold. `deploy/` runs it on one machine or on real
machines; see `deploy/README.md`.

Public port (`--http`):

| Endpoint | What it does | Read level or commit mode |
| --- | --- | --- |
| `GET /` | Product list | local |
| `GET /products/{id}` | Product page; its stock updates live, with buttons to buy | local |
| `GET /api/products?offset=&limit=` | Products with stock, as JSON | local |
| `GET /api/products/{id}` | One product | local |
| `GET /api/products/{id}/stock` | Server-sent events: the stock now, then after every change, from `Node::subscribe()` | local |
| `GET /api/carts/{cart}?after=N` | The cart | at log index N |
| `POST /api/carts/{cart}/lines` `{"product_id", "quantity"}` | Add to cart | `async` (`--cart-mode quorum` to change) |
| `POST /api/checkout` `{"cart_id", "order_id", "after"}` | Reserve stock and create a pending order | interactive transaction, `quorum` |
| `GET /api/orders/{order}?after=N` | The order | at log index N |
| `POST /api/orders/{order}/pay` `{"approved", "after"}` | Simulated payment (200–800 ms, outside any transaction), then paid, or cancelled with the stock given back | one `execute`, `quorum` |
| `GET /api/status` | This node's view of the cluster | |

Every write answers `{"outcome", "index", ...}`: `outcome` is `ok`,
`sold_out`, `rejected` (lost a conflict at the commit-time check), `aborted`,
`unavailable`, `invalid`, `failed`, or `unknown` (may still take effect);
`index` is the log index to pass as `after` to a later read.

Admin port (`--admin`, keep it private): `POST /admin/init`
`{"voters": [...], "followers": [...]}` forms the cluster, `POST /admin/setup`
`{"reserve", "products", "stock", "flash_stock"}` creates the schema and the
catalog, `GET /admin/export?after=N` returns what the checker compares, and
`GET /admin/status`.

`reserve` picks how a checkout reserves stock, for the whole cluster:

- `counter`: one counter row per product, decremented inside the
  interactive checkout. Concurrent checkouts of one product conflict.
- `units`: one row per unit for sale; the checkout deletes randomly chosen
  units. Checkouts conflict only when they pick the same unit.
- `leader`: the leader decrements the counter with one `execute`, then the
  interactive checkout claims the reservation. No conflicts, one more round
  trip to the leader.
