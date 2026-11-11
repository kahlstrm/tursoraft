# tursoraft-raft

The Raft layer of tursoraft, on openraft 0.10.0. It orders opaque proposals and
applies them through the `StateMachine` trait of `tursoraft-statemachine`. It
knows nothing about Turso.

## Pieces

| File | What it does |
|---|---|
| `src/node.rs` | `Node`: starts openraft, serves other nodes and clients over TCP, forwards requests from followers to the leader, batches proposals |
| `src/log_store.rs` | Durable Raft log and vote: append-only segment files with CRC-checked records, `fdatasync` before an append is reported flushed |
| `src/state_machine.rs` | Adaptor from openraft's state machine API to `StateMachine`, on blocking threads |
| `src/network.rs` | TCP transport with a configured delay per pair of nodes |
| `src/client.rs` | `Client` for other processes: tests, benchmarks, tools |
| `src/counting.rs` | `CountingStateMachine`: counts entries and proposals, can reject proposals by prefix |
| `src/bin/tursoraft-raft-node.rs` | One node with a `CountingStateMachine` per process |

## Running a cluster by hand

```sh
cat > cluster.json <<'EOF'
{
  "nodes": {"1": "127.0.0.1:7001", "2": "127.0.0.1:7002", "3": "127.0.0.1:7003"},
  "delays": {"default_rtt_ms": 10.0, "pairs": [{"a": 1, "b": 3, "rtt_ms": 80.0}]},
  "batching": {"max_proposals": 4096, "max_bytes": 8388608, "max_entries_in_flight": 2}
}
EOF
for i in 1 2 3; do cargo run -p tursoraft-raft --release --bin tursoraft-raft-node -- \
  --config cluster.json --id $i --dir /tmp/raft-$i & done
```

Then form the cluster with `Client::new("127.0.0.1:7001").initialize(...)`. Leave
out `batching` to turn batching off. `timeouts` defaults to
`RaftTimeouts::for_max_rtt` of the delay table; `log` sets segment size and
snapshot policy.

## Behavior of openraft that matters for latency

openraft 0.10 pipelines replication: the leader sends each AppendEntries to a
follower without waiting for the answer to the one before
(`RaftNetworkV2::stream_append`), so one caller writing back-to-back pays one
round trip per write. openraft 0.9.25, which phase 1b used, kept one request in
flight per follower and paid two round trips per back-to-back write.

A heartbeat must come back within `heartbeat_ms`. Replication to a node may
take `heartbeat_ms` plus twice that node's round trip in the delay table, so a
far non-voting follower needs no longer heartbeat or election timeout.

## API for the node crate

- `Node::propose_many(Vec<Vec<u8>>)` puts a group of proposals into one log
  entry, in order, with nothing between them.
- `Node::app_request(to, bytes, timeout)` sends bytes to node `to`'s
  `AppHandler` over the delayed node-to-node connection. The handler is async
  and gets the `Node`, so it can propose.
- `Node::transfer_leader(to)` hands leadership to a voter.

## Tests

```sh
cargo test -p tursoraft-raft                     # all
cargo test -p tursoraft-raft --release --test cluster -- --nocapture --test-threads=1   # with timings
```
