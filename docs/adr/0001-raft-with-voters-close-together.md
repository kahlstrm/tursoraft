# 0001: Raft, with the voters close together and far instances that do not vote

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating agent, after Kalle Ahlström questioned Raft
across continents; Kalle can veto

## Context

The architecture this prototype aims for has a database inside every
application instance, and instances that elect a leader and synchronize data
among themselves. That needs an ordered, replicated log and leader election.

A Raft leader answers a write when a majority of voters, itself included, has
stored the entry. So a write waits for the round trip to the nearest majority.
The Raft benchmark (`crates/raft/bench-results/phase1c.md`) measured this in
21 placements with 3, 5 and 7 voters and up to 10 non-voting followers:

- A write at the leader costs the round trip to the nearest majority plus
  1.1–2.8 ms.
- The number of voters and of non-voting followers does not change that.
- After a killed leader, a new one comes after about 24 round trips with the
  timeouts used: 0.5–0.65 s for voters up to 10 ms apart, about 6 s at 250 ms.

Voters on different continents would therefore cost 85–300 ms per write
(calculated from published round-trip times), and seconds of failover.

## Decision

- Raft, through `openraft` 0.10.0.
- Three voters by default, one in each of three nearby regions.
- Every other instance is a non-voting follower. It receives the same log,
  reads its own file, and sends its writes to the leader. The leader never
  waits for it.
- The voters and the leader can be moved to another set of nodes while the
  cluster runs (`change_voters` and `transfer_leader` of the Raft crate), so a
  database whose writers moved can follow them.
- Pre-vote is on.

## Alternatives rejected

- **Voters worldwide.** 85–300 ms per write and failover in seconds. Kept
  only as a baseline to measure against.
- **Leaderless replication with CRDTs** (cr-sqlite, Corrosion). Writes are
  local everywhere, but transactions across nodes, unique and foreign-key
  constraints and rules such as "a balance never goes below zero" are lost.
- **Leaderless consensus** (the EPaxos family). Fast only when transactions
  do not conflict, and all writes to one SQLite database conflict.
- **Making the Turso Sync hub highly available.** A pull asks for the pages
  changed since a revision, and the revision is one server's WAL frame
  number, so another server cannot take over. The result would still be one
  hub where the last push wins.
- **`openraft` 0.9.** A lone writer paid two round trips per write, and 0.9
  has no call to hand leadership to a chosen node.

## Consequences

- The cluster keeps accepting writes when one voter, or the region it is in,
  is lost. It stops accepting writes when the whole area that holds the
  voters is lost.
- A write from a far instance costs one round trip to the leader on top of
  the leader's own wait. ADR 0004 makes that one round trip per transaction
  and not one per statement.
- With voters in one area this is the same shape as libSQL (one primary,
  replicas that read locally), plus failover without a person and no lost
  write that was acknowledged in `quorum` mode. The larger difference is that
  the voters can be spread over nearby regions and moved.
- Each answer to an AppendEntries call is given the heartbeat interval plus
  twice the configured round trip. On real machines the delay table is empty,
  so the heartbeat must be longer than the round trip to the farthest
  follower. `deploy/regions.json` uses a 400 ms heartbeat and an election
  timeout of 2–4 s. Failover there takes several seconds. This was read in
  the code and is not measured on real machines.
- A voter whose log is behind no longer holds up an election. Pre-vote adds
  one round trip before every election. With it, a leader came 4.7–5.6 s
  after a kill in the middle of a move of the voters, where it took
  4.5–13.1 s without it (12 kills each).
- No voter starts an election until `openraft`'s leader lease and its own
  election timeout have passed. The lease equals the largest election
  timeout and cannot be set separately in 0.10.0.
- A move of the voters that is cut short is not finished by the node. The
  application or an operator calls the move again.
