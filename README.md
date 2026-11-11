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

Turso comes from the enclosing checkout as path dependencies (commit
`0c9fe0dd94c14a4f00106bffa9c9433d5965fd3a`, 0.9.0-pre.5). This workspace is
not a member of Turso's workspace. It uses resolver 3, so the lockfile only
picks dependency versions that support Rust 1.88.

## Build

This repository holds only the `tursoraft/` directory, so it builds inside a
Turso checkout at the pinned commit:

```bash
git clone https://github.com/tursodatabase/turso
cd turso
git checkout 0c9fe0dd94c14a4f00106bffa9c9433d5965fd3a
git clone https://github.com/kahlstrm/tursoraft
cd tursoraft
cargo test --workspace
```

`cargo test -p tursoraft-changeset -- --nocapture --test-threads=1` prints
what CDC captures (`tests/cdc_capture.rs`) and replays a leader's change sets
onto another database with the sync engine's `DatabaseReplaySession`
(`tests/replay.rs`).
