# Phase 1c benchmark of the Raft layer

`src/bin/tursoraft-raft-bench.rs` starts one `tursoraft-raft-node` process
per node on this machine (release build, `CountingStateMachine`, real
`fdatasync` in the log store), runs one job, and appends one JSON line per
job to `bench-results/phase1c.jsonl`. `report.py` turns that file into the
tables in `bench-results/phase1c.md`.

## Rerun

From `tursoraft/`:

```sh
cargo build --release -p tursoraft-raft --bins
target/release/tursoraft-raft-bench run                      # the whole plan, about 1.7 h
target/release/tursoraft-raft-bench run --list               # the jobs, one JSON line each
target/release/tursoraft-raft-bench run --only rtt=100/ --only writers=16   # a subset
target/release/tursoraft-raft-bench run --again --only proxy_rtt            # repeat jobs
python3 -I crates/raft/bench/report.py crates/raft/bench-results/phase1c.jsonl
```

`run` skips jobs whose name is already in the result file, so an
interrupted run continues where it stopped. `--out FILE` writes elsewhere;
`--jobs FILE` runs jobs from a file of JSON lines in the format `--list`
prints. Each row holds the job as run, `started_unix`, `wall_s`, the git
commit of the code (`code`), and the result or the error. `report.py`
reports a job from its last row.

## What a cell measures

- Writers are closed loops: each proposes one proposal and waits for its
  acknowledgement before the next. Every writer has its own TCP connection
  to the node where the write is issued; that client has no injected delay.
- A cell forms the cluster, moves the leader if asked, adds the non-voting
  followers, waits until all nodes applied everything, measures the round
  trip from the leader to every node (5 pings), runs the writers for a
  warmup of 2 s + 5 × the expected latency, and then for the measured
  window. One writer: window = 1000 × the expected latency in ms, as
  seconds (10 s to 200 s). More writers: a fifth of that (10 s to 60 s).
- Latency: every write started in the window, from the call to the
  acknowledgement. Writes per second: acknowledgements inside the window.
- Entries per write and AppendEntries per write: from the leader's
  `Status` before and after the window. "Voter AppendEntries with entries
  per write" counts requests that carried entries, to all voters together.
- Follower lag: every 5 ms the leader's and each follower's `Status`. In
  entries: leader's last log index minus the follower's applied index (and
  minus what the leader knows the follower stored). In time: how long ago
  the leader applied the first entry the follower has not applied.
- After the window, every node must have applied the same entries
  (`all_nodes_applied_same_entries`) and the leader must not have changed
  (`leader_changed`).
- Timeouts: `RaftTimeouts::for_max_rtt` of the largest round trip between
  voters.
- `fsync` jobs append a record (8 header bytes + size) to a file and call
  `fdatasync`, 2000 times per thread, with 1, 3 or 7 threads at once.
- `failover` jobs: 3 voters, writers on both followers, SIGKILL of the
  leader, time to the first acknowledgement of a write started after the
  kill; then the killed node restarts and catches up. 10 trials.
- `transfer` jobs: 3 voters, 8 writers at the old leader,
  `Client::transfer_leader` back and forth, 6 trials.

## The plan

Base case: 3 voters, no non-voting followers, the same round trip between
all pairs, 200-byte proposals, batching on, writes issued at the leader.

- fdatasync of the orb: size 200 B, 4 KiB, 64 KiB × 1, 3, 7 threads.
- Voters × round trip in full: 3, 5, 7 voters × 0, 1, 10, 50, 100, 250 ms,
  one writer, batching off.
- Writers × batching × round trip in full: 1, 16, 128 writers × off, on ×
  the six round trips, 3 voters.
- Proposal size: 200 B, 4 KiB, 64 KiB × 1 and 16 writers × 0 and 100 ms.
- Non-voting followers: 0, 2, 4, 10 × 1 and 16 writers × 10 and 100 ms.
- Where the write is issued: leader or farthest node × 1 and 16 writers ×
  10 and 100 ms.
- AWS placements a, b, c × 1 and 16 writers × leader or farthest node.
- Delay by proxy instead of the network layer: 10 and 100 ms × 1 and 16
  writers, each next to the same cell with the network layer's delay.
- Failover at each round trip.
- Added after the first run: the batcher's `max_entries_in_flight` (1, 8,
  64; the default is 2) × 16 and 128 writers × 10 and 100 ms, and leader
  transfer at 0, 10, 100 ms with in flight 2 and 64.

## AWS table

`aws-7-regions.json` is the P50 matrix of round trips between AWS regions
over one day (`timeframe 1D`), as embedded in https://www.cloudping.co/,
fetched 2026-11-11 16:40 UTC. The benchmark uses the mean of both
directions, rounded to 0.1 ms, and 1 ms between two nodes in the same
region:

| | Frankfurt | Ireland | London | N. Virginia | Oregon | Singapore | Sydney |
|---|---|---|---|---|---|---|---|
| Frankfurt | 1.0 | 21.6 | 14.4 | 91.5 | 146.3 | 164.8 | 255.7 |
| Ireland | 21.6 | 1.0 | 11.6 | 68.6 | 121.8 | 176.7 | 255.0 |
| London | 14.4 | 11.6 | 1.0 | 77.3 | 134.2 | 164.1 | 266.0 |
| N. Virginia | 91.5 | 68.6 | 77.3 | 1.0 | 64.1 | 220.2 | 207.5 |
| Oregon | 146.3 | 121.8 | 134.2 | 64.1 | 1.0 | 177.8 | 144.8 |
| Singapore | 164.8 | 176.7 | 164.1 | 220.2 | 177.8 | 1.0 | 94.0 |
| Sydney | 255.7 | 255.0 | 266.0 | 207.5 | 144.8 | 94.0 | 1.0 |

Nodes 1–7 are the regions in this order; nodes 8 and 9 are two more nodes
in Frankfurt. Placement a: voters 1, 2, 3; b: voters 1, 4, 6; c: voters
1, 8, 9. The other regions hold non-voting followers, and the leader is
moved to Frankfurt (node 1) before measuring.

## Notes on the recorded run

- All node processes share one machine: 8 CPUs, one disk. Concurrent
  `fdatasync` calls of several nodes contend for that disk.
- Three single-writer cells (`voters_x_rtt`, 3 voters, 50/100/250 ms) ran
  while other work used the machine and were repeated; both rows are in the
  file. The four proxy cells failed the first time (the benchmark binary was
  rebuilt during the run, and the proxy was started from the binary's old
  path) and were repeated after the fix.
- `flip-log-group-commit.jsonl` is not from committed code: it is the two
  unbatched cells at 0 ms with 16 and 128 writers, rerun with a temporary
  change to the log writer that groups queued appends under one
  `fdatasync`. It shows how much of the unbatched ceiling is the per-append
  flush.
