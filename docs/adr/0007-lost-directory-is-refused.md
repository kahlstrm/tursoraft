# 0007: A node that lost its Raft directory is refused under its old id

Status: Accepted
Date: 2026-11-11
Decided by: the coordinating and implementing agents; Kalle Ahlström can veto

## Context

A voter that comes back with an empty directory has no vote and no log. Raft
assumes that a voter never forgets either. Such a node can vote for a
candidate whose log is behind, and that candidate then removes committed
entries on the other nodes. A test produced exactly this: two nodes each
missed 138 acknowledged transfers
(`a_voter_that_forgets_its_log_loses_acknowledged_writes_and_the_checker_sees_it`
in `crates/proof/tests/moves.rs`).

On rented machines, losing a disk is a normal event.

## Decision

- Every Raft directory gets a file `incarnation` the first time a node starts
  there. It holds a random non-zero number and is written with a flush, a
  rename and a flush of the directory.
- The membership records each member's incarnation
  (`tursoraft_raft::Member { addr, incarnation }`).
- Every connection names the incarnation the sender expects of the receiver.
  A receiver whose incarnation differs logs an error and refuses every
  request on that connection. It cannot vote, take entries or install a
  snapshot as its old member.
- `add_follower` refuses an id that is already a member with another
  incarnation.

## Alternatives rejected

- **Let the node rejoin under its old id after catching up.** That needs a
  rule for when the node may vote again, and a way to remove the old member
  first. `change_voters` keeps removed voters as followers, so the old id
  stays taken.
- **Exit the process when refused.** The application in the same process
  would go down with it. The node logs an error and keeps running.

## Consequences

- A node that lost its directory comes back under a new id.
- A node must be running when it is added to a cluster, because adding it
  asks for its incarnation.
- Only the Raft directory is checked. Losing the database file while keeping
  the Raft directory is not detected.
- No status field reports the refusal. It shows as an error in the refused
  node's log and as an unreachable peer on the others.
- The test that removes a voter's log still produces the loss when it keeps
  the `incarnation` file. The check detects a lost directory, not a damaged
  one.
- Logs, snapshots and the connection handshake changed with this decision.
  Directories written before it cannot be read, and no migration exists.
